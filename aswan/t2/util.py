from typing import List

from .types import _T_PARSING_KEYBRANCH, _T_POSSIBLE_KEY


class NamingSchemes:

    joiner = "join"
    last = "last"


NAMING_SHCEME_DICT = {
    NamingSchemes.joiner: lambda t: "_".join(map(str, t)),
    NamingSchemes.last: lambda t: t[-1],
}


def _parse_to_keybranches(
    possible_keybranches: List[_T_POSSIBLE_KEY],
) -> List[_T_PARSING_KEYBRANCH]:
    out = []
    for kt in possible_keybranches:
        if not isinstance(kt, tuple):
            kt = (kt,)
        out.append(kt)
    return out
