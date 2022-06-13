from typing import List, Optional, Union

from structlog import get_logger

from .types import (
    _T_NODE,
    _T_PARSING_KEYBRANCH,
    _T_POSSIBLE_KEY,
    _T_RECORD,
    _T_RECORD_KEYBRANCH,
    DictValues,
    ListElements,
)
from .util import NAMING_SHCEME_DICT, NamingSchemes, _parse_to_keybranches

logger = get_logger()


class RecordExtractor:
    def __init__(
        self,
        key_branches: List[_T_POSSIBLE_KEY],
        id_key: Optional[str] = None,
    ):
        self._key_branches = _parse_to_keybranches(key_branches)
        self.id_key = id_key

    def parse(self, obj: Union[dict, list]) -> Union[dict, list]:

        parsed_object = ParsedObject()
        for key_branch in self._key_branches:
            parsed_object.integrate_key_branch_of_object(key_branch, obj)

        return parsed_object.export_full()

    def add_to_object(
        self,
        object_to_parse: Union[list, dict],
        output_object: Union[list, dict],
    ):
        _out = self.parse(object_to_parse)
        if self.id_key is None:
            output_object.extend(_out)
        elif isinstance(_out, list):
            output_object.update({o[self.id_key]: o for o in _out})
        else:
            # FIXME here it may extend with dict keys
            # ???
            output_object[_out[self.id_key]] = _out


class ParsedObject:
    """
    wtf does this do?

    """

    def __init__(
        self,
        records: Optional[List["ParsedObject"]] = None,
        key_creator: callable = NAMING_SHCEME_DICT[NamingSchemes.joiner],
        extension: Optional[dict] = None,
    ):
        self._records = records
        self._branch_value_pairs: _T_RECORD = []
        self._key_creator = key_creator
        self._extension_based_on_keys = extension or {}

    # FIXME: shorten, and better missing handling
    def integrate_key_branch_of_object(
        self,
        keybranch_tuple: _T_PARSING_KEYBRANCH,
        obj: _T_NODE,
        record_keybranch_so_far: _T_RECORD_KEYBRANCH = (),
    ) -> "ParsedObject":
        try:
            self._integrate_key_branch_of_object(
                keybranch_tuple, obj, record_keybranch_so_far
            )
        except KeyError as e:
            logger.warning(f"KeyError: {e} key not in object {type(obj)}")
        return self

    def _integrate_key_branch_of_object(
        self,
        keybranch_tuple: _T_PARSING_KEYBRANCH,
        obj: _T_NODE,
        record_keybranch_so_far: _T_RECORD_KEYBRANCH = (),
    ) -> "ParsedObject":

        if len(keybranch_tuple) == 0:
            self._branch_value_pairs.append((record_keybranch_so_far, obj))
            return self

        current_key = keybranch_tuple[0]
        remaining_keys = keybranch_tuple[1:]

        if isinstance(current_key, RecordExtractor):
            ...  # TODO

        if current_key is ...:
            current_key = ListElements(unstack=True)

        unstack, keys_to_parse = _get_unstack_and_keys(current_key, obj)

        if not unstack:
            for k in keys_to_parse:
                self.integrate_key_branch_of_object(
                    remaining_keys, obj[k], (*record_keybranch_so_far, k)
                )
            return self

        self._init_records()
        for ind in keys_to_parse:
            new_po = ParsedObject(
                self._records,
                key_creator=NAMING_SHCEME_DICT[NamingSchemes.last],
                extension={
                    **self._extension_based_on_keys,
                    **({current_key.level_name: ind} if current_key.level_name else {}),
                },
            ).integrate_key_branch_of_object(
                remaining_keys, obj[ind], record_keybranch_so_far
            )
            self._add_full_record(new_po)
        return self

    def export_full(self):
        parsed_dict = self.export_base()
        if self._records is None:
            return parsed_dict
        else:
            out = []
            for record_po in self._records:
                _record_dict = record_po.export_base()
                out.append({**parsed_dict, **_record_dict})
            return out

    def export_base(self):
        final_values = dict(
            map(
                lambda tv: (self._key_creator(tv[0]), tv[1]),
                self._branch_value_pairs,
            )
        )
        return {**final_values, **self._extension_based_on_keys}

    def _add_full_record(self, po: "ParsedObject"):
        if po._branch_value_pairs:
            self._records.append(po)

    def _init_records(self):
        if self._records is None:
            self._records = []


def _get_unstack_and_keys(current_key, obj):

    unstack = False
    if isinstance(current_key, DictValues):
        unstack = current_key.unstack
        if current_key.keys is None:
            keys_to_parse = obj.keys()
        else:
            keys_to_parse = current_key.keys
    elif isinstance(current_key, ListElements):
        unstack = current_key.unstack
        keys_to_parse = range(
            current_key.max_index if current_key.max_index is not None else len(obj)
        )
    else:
        keys_to_parse = [current_key]
    return unstack, keys_to_parse
