import json
import random
import time
from ctypes import *
from typing import List

import requests


slib = "/home/blong14/Developer/git/gache/bin/gache.so"
gache = CDLL(slib)
gache.Get.argtypes = [c_char_p]
gache.Get.restype = c_char_p
gache.Execute.argtypes = [c_char_p]
gache.Set.argtypes = [c_char_p]


class Gache:

    @staticmethod
    def get(table: str, key: str) -> str:
        value = gache.Get(json.dumps(dict(table=table, key=key)).encode("utf-8"))
        return value.decode("utf-8")

    @staticmethod
    def execute(sql: str):
        gache.Execute(sql.encode("utf-8"))

    @staticmethod
    def set(table: str, key: str, value: str):
        gache.Set(json.dumps(dict(table=table, key=key, value=value)).encode("utf-8"))


def init():
    start = time.perf_counter()
    word_site = "https://www.mit.edu/~ecprice/wordlist.10000"
    response = requests.get(word_site)
    words = response.content.splitlines()
    gache.Init()
    stop = time.perf_counter()
    print(f"init::{stop - start:04f}")
    return words


def run(word_list: List[str], count: int):
    start = time.perf_counter()
    keys = set()
    table = "default"
    for i in range(count):
        key = random.choice(word_list).decode()
        value = random.choice(word_list).decode()
        Gache.execute(f"insert into default set key = {key}, value = {value};")
        keys.add((key, value))
    stop = time.perf_counter()
    print(f"run::{stop - start:04f}")

    start = time.perf_counter()
    values = []
    for row in keys:
        key, value = row
        v = Gache.get(table, key)
        try:
            assert v == value
        except Exception:
            values.append((key, value, v))
    stop = time.perf_counter()
    print(values)
    print(f"run::{stop - start:04f}")


def close():
    start = time.perf_counter()
    gache.Stop()
    stop = time.perf_counter()
    print(f"close::{stop - start:04f}")


if __name__ == "__main__":
    run(init(), 10)
    close()
