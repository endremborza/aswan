import pytest

from aswan.t2 import DictValues, ListElements, RecordExtractor

test_record1 = {
    "a": 10,
    "b": {"x": 100, "y": 200, "z": 300},
    "c": [1, 2, 3],
    "d": [{"xx": 1, "yy": 2, "zz": 3}, {"xx": 3, "yy": 4, "zz": 5}],
    "side": {
        "s1": [{"sa": 1, "sb": 2}, {"sa": 10, "sb": 20}],
        "s2": [{"sa": 12, "sb": 22}],
    },
    "difftype": {"dta": 1, "dtl": [1, 2, 3]},
    "diffemb": [
        {"dea": 10, "del": [2, 3, 4]},
        {"dea": 20, "del": [10, 11, 12, 13]},
    ],
    "difftypelist": ["a", {"v": 5, "vv": 50}, 10],
}

test_record2 = {
    "a": 20,
    "b": {"x": 200},
    "c": [7, 8],
    "side": {"s1": [{"sa": 10, "sb": 20}], "s3": [{"sa": 12, "sb": 22}]},
}


@pytest.mark.parametrize(
    ["key_branches", "expected_dict"],
    [
        (["a", ("b", "y")], {"a": 10, "b_y": 200}),
        (
            ["a", ("b", DictValues(["x", "y"]))],
            {"a": 10, "b_x": 100, "b_y": 200},
        ),
        (
            ["a", ("b", DictValues())],
            {"a": 10, "b_x": 100, "b_y": 200, "b_z": 300},
        ),
        ([("b", "x"), ("c", ListElements(1))], {"b_x": 100, "c_0": 1}),
        (
            [("b", "x"), ("c", ListElements())],
            {"b_x": 100, "c_0": 1, "c_1": 2, "c_2": 3},
        ),
        (
            ["a", ("d", ListElements(), DictValues(["xx", "yy"]))],
            {"a": 10, "d_0_xx": 1, "d_1_xx": 3, "d_0_yy": 2, "d_1_yy": 4},
        ),
        (
            ["a", ("side", DictValues(), ListElements(1), DictValues(["sa"]))],
            {"a": 10, "side_s1_0_sa": 1, "side_s2_0_sa": 12},
        ),
    ],
)
def test_single_record_extractor(key_branches, expected_dict):
    extractor = RecordExtractor(key_branches=key_branches)
    assert extractor.parse(test_record1) == expected_dict


@pytest.mark.parametrize(
    ["key_branches", "expected_list"],
    [
        (
            [("d", ..., "xx"), "a", ("b", "x")],
            [{"a": 10, "b_x": 100, "xx": 1}, {"a": 10, "b_x": 100, "xx": 3}],
        ),
        (
            [("c", ...), ("b", "y")],
            [
                {"b_y": 200, "c": 1},
                {"b_y": 200, "c": 2},
                {"b_y": 200, "c": 3},
            ],
        ),
        (
            [("d", ..., DictValues()), "a"],
            [
                {"a": 10, "xx": 1, "yy": 2, "zz": 3},
                {"a": 10, "xx": 3, "yy": 4, "zz": 5},
            ],
        ),
        (
            [
                (
                    "side",
                    DictValues(level_name="side", unstack=True),
                    ...,
                    DictValues(),
                )
            ],
            [
                {"side": "s1", "sa": 1, "sb": 2},
                {"side": "s1", "sa": 10, "sb": 20},
                {"side": "s2", "sa": 12, "sb": 22},
            ],
        ),
        (
            [
                (
                    "side",
                    DictValues(unstack=True),
                    ListElements(1, unstack=True),
                    DictValues(),
                )
            ],
            [{"sa": 1, "sb": 2}, {"sa": 12, "sb": 22}],
        ),
        (
            [("difftype", "dta"), ("difftype", "dtl", ...)],
            [
                {"difftype_dta": 1, "dtl": 1},
                {"difftype_dta": 1, "dtl": 2},
                {"difftype_dta": 1, "dtl": 3},
            ],
        ),
    ],
)
def test_set_extractor(key_branches, expected_list):
    extractor = RecordExtractor(key_branches)
    assert extractor.parse(test_record1) == expected_list


@pytest.mark.parametrize(
    ["key_branches", "expected_list"],
    [
        (
            [
                (
                    "diffemb",
                    ...,
                    RecordExtractor(["dea", ("del", ListElements())]),
                )
            ],
            [
                {"dea": 10, "del_0": 2, "del_1": 3, "del_2": 4},
                {
                    "dea": 20,
                    "del_0": 10,
                    "del_1": 11,
                    "del_2": 12,
                    "del_3": 13,
                },
            ],
        ),
    ],
)
def later_test_recursed_extractor(key_branches, expected_list):
    # TODO
    extractor = RecordExtractor(key_branches)
    assert extractor.parse(test_record1) == expected_list


@pytest.mark.parametrize(
    ["key_branches", "expected_list"],
    [
        (
            [("d", ..., "xx"), "a", ("b", "x")],
            [{"a": 10, "b_x": 100, "xx": 1}, {"a": 10, "b_x": 100, "xx": 3}],
        ),
    ],
)
def test_individual_extractor(key_branches, expected_list):
    extractor = RecordExtractor(key_branches)
    assert extractor.parse(test_record1) == expected_list
