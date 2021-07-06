#!/usr/local/bin/python3
#coding: utf-8

"""
Script making the pairings for FIDE binance tournament
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import logging.handlers
import requests
import os
import time
import re
import sqlite3
import sys

from argparse import RawTextHelpFormatter
from dataclasses import dataclass
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

#############
# Constants #
#############

load_dotenv()

PLAYER_REGEX = re.compile(r"^(\d+) +(\w+) +\d+.+ +(\w+) +\d+") # re.compile(r"(\w+) +\d+")

G_DOC_PATH = "sample.txt"
LOG_PATH = "pair.log"

PAIRING_API = "TODO"

API_KEY = f"Authorization: Bearer {os.getenv('TOKEN')}"

RETRY_STRAT = Retry(
    total=5,
    backoff_factor=200,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["GET"]
)
ADAPTER = HTTPAdapter(max_retries=RETRY_STRAT)


########
# Logs #
########

log = logging.getLogger("pair")
log.setLevel(logging.DEBUG)
format_string = "%(asctime)s | %(levelname)-8s | %(message)s"

# 125000000 bytes = 125Mb
handler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=125000000, backupCount=3, encoding="utf8")
handler.setFormatter(logging.Formatter(format_string))
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

handler_2 = logging.StreamHandler(sys.stdout)
handler_2.setFormatter(logging.Formatter(format_string))
handler_2.setLevel(logging.INFO)
log.addHandler(handler_2)

###########
# Classes #
###########

class Db:

    def __init__(self: Db) -> None:
        self.con = sqlite3.connect('FIDE_binance.db', isolation_level=None)
        self.cur = self.con.cursor()

    def create_db(self: Db) -> None:
        # Since the event is divided in two parts, `round_nb` will first indicate the round number in the round-robin then advancement in the knockdown event
        # `result` 0 = black wins, 1 = white wins, 2 = draw, 3 = unknown (everything else)
        # `rowId` is the primary key and is create silently
        self.cur.execute('''CREATE TABLE rounds
               (
               white_player description VARCHAR(30) NOT NULL, 
               black_player description VARCHAR(30) NOT NULL, 
               lichess_game_id CHAR(8), 
               result INT,
               round_nb INT)''')

    def show(self: Db) -> None:
        tables = self.cur.execute("""SELECT name 
            FROM sqlite_master 
            WHERE type ='table' AND 
            name NOT LIKE 'sqlite_%';""")
        clean_tables = [t[0] for t in tables]
        log.info(f"List of table's names: {clean_tables}")
        for table in clean_tables:
            struct = self.cur.execute(f'PRAGMA table_info({table})') # discouraged but qmark does not work here for some reason
            log.info(f"{table} structure: {[t for t in struct]}")
            rows = self.cur.execute(f'SELECT * from {table}')
            log.info(f"{table} rows: {[t for t in rows]}")


    def add_players(self: Db, pair: Pair) -> None:
        self.cur.execute('''INSERT INTO rounds
            (
            white_player,
            black_player
            ) VALUES (?, ?)
            ''', (pair.white_player, pair.black_player))

    def get_unpaired_players(self: Db) -> Iterator[Tuple[str, str, str]]:
        return self.cur.execute('''SELECT 
            rowId, 
            white_player,
            black_player
            FROM rounds
            WHERE lichess_game_id IS NULL
            ''')

    def add_lichess_game_id(self: Db, rowId: int, game_id: str) -> None:
        self.cur.execute('''UPDATE rounds
            SET lichess_game_id = ?
            WHERE
            rowId = ?''', (game_id, rowId))

class FileHandler:

    def __init__(self: FileHandler, db: Db) -> None:
        self.db = db

    def get_pairing(self: FileHandler) -> List[Pair]:
        l: List[str] = []
        with open(G_DOC_PATH) as input_:
            for line in input_:
                match = PLAYER_REGEX.match(line)
                if match is None:
                    continue
                log.info(match.groups())
                (table_number, player_1, player_2) = match.groups()
                if int(table_number) % 2: # odd numbers have white player on left 
                    pair = Pair(white_player=player_1, black_player=player_2)
                else:
                    pair = Pair(white_player=player_2, black_player=player_1)
                log.info(pair)
                l.append(pair)
        return l

    def fetch(self: FileHandler) -> None:
        for pair in self.get_pairing():
            self.db.add_players(pair)

@dataclass
class Pair:
    white_player: str
    black_player: str

class Pairing:

    def __init__(self: Pairing) -> None:
        http = requests.Session()
        http.mount("https://", ADAPTER)
        http.mount("http://", ADAPTER)
        self.http = http
        self.dep = time.time()

    def tl(self: Pairing) -> float:
        """time elapsed"""
        return time.time() - self.dep

#############
# Functions #
#############

def create_db() -> None:
    """Setup the sqlite database, should be run once first when getting the script"""
    db = Db()
    db.create_db()

def show() -> None:
    """Show the current state of the database. For debug purpose only"""
    db = Db()
    db.show()

def test() -> None:
    db = Db()
    f = FileHandler(db)
    log.info([x for x in db.get_unpaired_players()])
    db.add_lichess_game_id(rowId=2, game_id="12345678")

def fetch() -> None:
    """Takes the raw dump from the `G_DOC_PATH` copied document and store the pairings in the db, without launching the challenges"""
    f = FileHandler(Db())
    f.fetch()

def pair() -> None:
    pass

def doc(dic: Dict[str, Callable[..., Any]]) -> str:
    """Produce documentation for every command based on doc of each function"""
    doc_string = ""
    for name_cmd, func in dic.items():
        doc_string += f"{name_cmd}: {func.__doc__}\n\n"
    return doc_string

def main() -> None:
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    commands = {
    "create_db": create_db,
    "show": show,
    "test": test,
    "fetch": fetch,
    "pair": pair,
    }
    parser.add_argument("command", choices=commands.keys(), help=doc(commands))
    args = parser.parse_args()
    commands[args.command]()

########
# Main #
########

if __name__ == "__main__":
    print('#'*80)
    main()