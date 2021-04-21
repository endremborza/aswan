from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple, Union


@dataclass
class DictValues:
    keys: Optional[Iterable] = None
    unstack: bool = False
    level_name: str = ""


@dataclass
class ListElements:
    max_index: Optional[int] = None
    index_naming: Optional[callable] = None
    unstack: bool = False
    level_name: str = ""


_T_PARSING_KEYBRANCH_ELEMENT = Union[str, type(...), DictValues, ListElements]
_T_PARSING_KEYBRANCH = Tuple[_T_PARSING_KEYBRANCH_ELEMENT, ...]

_T_POSSIBLE_KEY = Union[_T_PARSING_KEYBRANCH, _T_PARSING_KEYBRANCH_ELEMENT]

_T_LEAF = Union[None, bool, int, float, str]
_T_NONLEAF = Union[dict, list]
_T_NODE = Union[_T_LEAF, _T_NONLEAF]

_T_RECORD_KEYBRANCH_ELEMENT = Union[int, str]
_T_RECORD_KEYBRANCH = Tuple[_T_RECORD_KEYBRANCH_ELEMENT, ...]
_T_RECORD = List[Tuple[_T_RECORD_KEYBRANCH, _T_NODE]]
