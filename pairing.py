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

G_DOC_PATH = "round_{}.txt"
LOG_PATH = "pair.log"

BASE = "https://lichess.org"
if __debug__: 
    BASE = "http://localhost:9663"  
PAIRING_API = BASE + "/api/challenge/admin/{}/{}"
LOOKUP_API = BASE + "/games/export/_ids"

API_KEY = {"Authorization": f"Bearer {os.getenv('TOKEN')}", "Accept": "application/x-ndjson"}

RETRY_STRAT = Retry(
    total=5,
    backoff_factor=5,
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
        # Since the event is divided in two parts, `round_nb` will first indicate the round_nb number in the round-robin then advancement in the knockdown event
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


    def add_players(self: Db, pair: Pair, round_nb: int) -> None:
        self.cur.execute('''INSERT INTO rounds
            (
            white_player,
            black_player,
            round_nb
            ) VALUES (?, ?, ?)
            ''', (pair.white_player, pair.black_player, round_nb))

    def get_unpaired_players(self: Db, round_nb: int) -> List[Tuple[int, Pair]]:
        raw_data = list(self.cur.execute('''SELECT 
            rowId, 
            white_player,
            black_player
            FROM rounds
            WHERE lichess_game_id IS NULL AND round_nb = ?
            ''', (round_nb,)))
        log.info(f"Round {round_nb}, {len(raw_data)} games to be created")
        return [(int(row_id), Pair(white_player, black_player))for row_id, white_player, black_player in raw_data]

    def add_lichess_game_id(self: Db, row_id: int, game_id: str) -> None:
        self.cur.execute('''UPDATE rounds
            SET lichess_game_id = ?
            WHERE
            rowId = ?''', (game_id, row_id))

    def get_unfinished_games(self: Db, round_nb: int) -> Dict[str, int]:
        raw_data = list(self.cur.execute('''SELECT 
            rowId, 
            lichess_game_id,
            FROM rounds
            WHERE lichess_game_id IS NOT NULL AND result IS NULL AND round_nb = ?
            ''', (round_nb,)))
        log.info(f"Round {round_nb}, {len(raw_data)} games unfinished")
        return {game_id: int(row_id) for row_id, game_id in raw_data}

    def get_game_ids(self: Db, round_nb: int) -> str:
        raw_data = list(self.cur.execute('''SELECT 
            lichess_game_id
            FROM rounds
            WHERE lichess_game_id IS NOT NULL AND round_nb = ?
            ''', (round_nb,)))
        log.info(f"Round {round_nb}, {len(raw_data)} games started")
        return " ".join(raw_data)

    def add_game_result(self: Db, row_id: int, result: int) -> None:
        self.cur.execute('''UPDATE rounds
            SET result = ?
            WHERE
            rowId = ?''', (result, row_id))

class FileHandler:

    def __init__(self: FileHandler, db: Db) -> None:
        self.db = db

    def get_pairing(self: FileHandler, round_nb: int) -> List[Pair]:
        l: List[Pair] = []
        with open(G_DOC_PATH.format(round_nb)) as input_:
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

    def fetch(self: FileHandler, round_nb: int) -> None:
        for pair in self.get_pairing(round_nb):
            self.db.add_players(pair, round_nb)

@dataclass
class Pair:
    white_player: str
    black_player: str

class Pairing:

    def __init__(self: Pairing, db: Db) -> None:
        self.db = db
        http = requests.Session()
        http.mount("https://", ADAPTER)
        http.mount("http://", ADAPTER)
        self.http = http
        self.dep = time.time()

    def tl(self: Pairing) -> float:
        """time elapsed"""
        return time.time() - self.dep

    def pair_all_players(self: Pairing, round_nb: int) -> None:
        for row_id, pair in self.db.get_unpaired_players(round_nb):
            game_id = self.create_game(pair)
            self.db.add_lichess_game_id(row_id, game_id)

    def create_game(self: Pairing, pair: Pair) -> str:
        """Return the lichess game id of the game created"""
        url = PAIRING_API.format(pair.white_player, pair.black_player)
        payload = {
            "rated": "true",
            "clock.limit": 600,
            "clock.increment": 2,
            "color": "white"
        }
        r = self.http.post(url, data=payload, headers=API_KEY)
        rep = r.json()
        log.debug(rep)
        return rep["game"]["id"]

    def check_all_results(self: Pairing, round_nb: int) -> None:
        games_dic = self.db.get_unfinished_games(round_nb)
        r = self.http.post(LOOKUP_API, data=",".join(games_dic.keys()), headers=API_KEY, params={"moves": "false"})
        rep = r.json()
        for game in rep:
            res = game["status"]
            id_ = game["id"]
            log.info(f"Game {id_}, result: {res}")



#############
# Functions #
#############

def create_db(*args) -> None:
    """Setup the sqlite database, should be run once first when getting the script"""
    db = Db()
    db.create_db()

def show(*args) -> None:
    """Show the current state of the database. For debug purpose only"""
    db = Db()
    db.show()

def test(*args) -> None:
    db = Db()
    f = FileHandler(db)
    p = Pairing(db)
    p.create_game(Pair("test", "test2"))

def fetch(round_nb: int) -> None:
    """Takes the raw dump from the `G_DOC_PATH` copied document and store the pairings in the db, without launching the challenges"""
    f = FileHandler(Db())
    f.fetch(round_nb)

def pair(round_nb: int) -> None:
    """Create a challenge for every couple of players that has not been already paired"""
    db = Db()
    p = Pairing(db)
    p.pair_all_players(round_nb)

def result(round_nb: int) -> None:
    """Fetch all games from that round_nb, check if they are finished, and print the results"""
    pass

def broadcast(round_nb: int) -> None:
    """Return game ids of the round `round_nb` separated by a space"""
    db = Db()
    print(db.get_game_ids(round_nb))

def doc(dic: Dict[str, function]) -> str:
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
    "result": result,
    "broadcast": broadcast,
    }
    parser.add_argument("command", choices=commands.keys(), help=doc(commands))
    parser.add_argument("round_nb", type=int, help="The round number related to the action you want to do. Only used for `fetch`, `pair`, `result`")
    args = parser.parse_args()
    commands[args.command](args.round_nb)

########
# Main #
########

if __name__ == "__main__":
    print('#'*80)
    main()