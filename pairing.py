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
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

#############
# Constants #
#############

load_dotenv()

PLAYER_REGEX = re.compile(r"(\w+) +\d+")

G_DOC_PATH = "sample.txt"
LOG_PATH = "pair.log"

PAIRING_API = "TODO"

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
        log.info(tables)
        for table in clean_tables:
            struct = self.cur.execute(f'PRAGMA table_info({table})') # discouraged but qmark does not work here for some reason
            log.info(f"{table} structure: {[t for t in struct]}")
            rows = self.cur.execute(f'SELECT * from {table}')
            log.info(f"{table} rows: {[t for t in rows]}")


    def add_players(self: Db, white_player: str, black_player: str) -> None:
        self.cur.execute('''INSERT INTO rounds
            (
            white_player,
            black_player
            ) VALUES (?, ?)
            ''', (white_player, black_player))

class FileHandler:

    def get_pairing(self: FileHandler) -> List[List[str]]:
        l: List[str] = []
        with open(G_DOC_PATH) as input_:
            for line in input_:
                raw_res: List[str] = PLAYER_REGEX.findall(line)
                if len(raw_res) != 2:
                    log.error(f"Player usernames not fetched properly, result: {raw_res}")
                else:
                    l.append(raw_res)
        return l

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

def test() -> None:
    db = Db()
    db.show()

def doc(dic: Dict[str, Callable[..., Any]]) -> str:
    """Produce documentation for every command based on doc of each function"""
    doc_string = ""
    for name_cmd, func in dic.items():
        doc_string += f"{name_cmd}: {func.__doc__}\n\n"
    return doc_string

def main() -> None:
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    commands = {
    "create": create_db,
    "test": test,
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