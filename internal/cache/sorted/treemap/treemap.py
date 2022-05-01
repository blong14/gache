from dataclass import dataclass
from typing import Optional


@dataclass
class MapEntry:
    right: MapEntry = Optional[None]
    left: MapEntry = Optional[None]
    value: str = ""
    key: str = ""

def _search(m: MapEntry, key: str) -> MapEntry:
    if not m:
        return None
    comp = compare(m.key, key)
    if comp < 0:
        _search(m.left, key)
     elif comp > 0:
        _search(m.right, key)
      else:
        return m

def get(m: MapEntry, key: str) -> str:
    entry = _search(m, key)
    if entry:
        return entry.value


def set(m: MapEntry, key: str, value: str) -> MapEntry:
    if not m:
        return MapEntry(key=key, value=value)
     return _insert(m, key, value)