import pytest

from aswan.scheduler import Resource
from aswan.scheduler.resource import (
    ResourceBundle,
    ResourceLimit,
    ResourceLimitSet,
    get_resource_name,
)


class BigNumberCapability(Resource):
    pass


class PrimeBaseCapability(Resource):
    actor_resource = False

    def __init__(self, base: int):
        self.base = base


class BasicResource(Resource):
    pass


class NumberResource(Resource):
    actor_resource = False

    def __init__(self, base: int):
        self.base = base


@pytest.fixture
def empty_bundle():
    return ResourceBundle()


@pytest.fixture
def basic1_bundle():
    return ResourceBundle([BasicResource()])


@pytest.fixture
def basic2_bundle():
    return ResourceBundle([BasicResource(), BasicResource()])


@pytest.fixture
def single_n10():
    return ResourceBundle([NumberResource(10)])


@pytest.fixture
def basic_and_n10():
    return ResourceBundle([BasicResource(), NumberResource(10)])


@pytest.fixture
def basic_and_n5():
    return ResourceBundle([BasicResource(), NumberResource(5)])


@pytest.mark.parametrize(
    ["resource1", "resource2"],
    [
        (NumberResource(10), BasicResource()),
        (NumberResource(1), NumberResource(2)),
    ],
)
def test_resource_name_neq(resource1, resource2):
    assert get_resource_name(resource1) != get_resource_name(resource2)


@pytest.mark.parametrize(
    ["resource1", "resource2"],
    [
        (BasicResource(), BasicResource()),
        (NumberResource(2), NumberResource(2)),
    ],
)
def test_resource_name_eq(resource1, resource2):
    assert get_resource_name(resource1) == get_resource_name(resource2)


def test_resource_bundles(
    empty_bundle,
    basic1_bundle,
    basic2_bundle,
    single_n10,
    basic_and_n10,
    basic_and_n5,
):

    for _bundle in [basic1_bundle, basic2_bundle, single_n10, basic_and_n10]:
        assert empty_bundle < _bundle
        assert _bundle >= empty_bundle
        assert _bundle != empty_bundle

    assert (basic1_bundle * 2) == basic2_bundle
    assert basic1_bundle == ResourceBundle([BasicResource()])
    assert (basic1_bundle + basic1_bundle) == basic2_bundle

    assert single_n10 <= basic_and_n10
    assert basic_and_n10 > single_n10
    assert not (basic_and_n5 <= basic_and_n10)
    assert not (basic_and_n10 >= basic_and_n5)

    new_single_n10 = basic_and_n10 - basic1_bundle

    assert new_single_n10 == single_n10
    assert (
        basic_and_n10.key
        == ResourceBundle([NumberResource(10), BasicResource()]).key
    )
    assert (basic_and_n5 + basic2_bundle).key == ResourceBundle(
        [BasicResource(), BasicResource(), NumberResource(5), BasicResource()]
    ).key
    assert (
        basic_and_n10.key
        != ResourceBundle([NumberResource(5), BasicResource()]).key
    )


def test_resource_limits(
    empty_bundle,
    basic1_bundle,
    basic2_bundle,
    single_n10,
    basic_and_n10,
    basic_and_n5,
):
    no_limits = ResourceLimitSet()

    for _bundle in [empty_bundle, basic1_bundle, basic_and_n10]:
        assert no_limits.satisfied(_bundle)

    basic1_limit = ResourceLimit(BasicResource, global_limit=1)
    basic1_limit_set = ResourceLimitSet([basic1_limit])
    assert basic1_limit_set.satisfied(basic1_bundle)
    assert basic1_limit_set.satisfied(basic_and_n10)
    assert not basic1_limit_set.satisfied(basic2_bundle)

    n_sum7_limit = ResourceLimit(
        NumberResource,
        global_limit=8,
        target_attribute="base",
        limit_kind="sum",
    )
    n_nunique_limit = ResourceLimit(
        NumberResource,
        global_limit=1,
        target_attribute="base",
        limit_kind="nunique",
    )
    n_maxc_limit = ResourceLimit(
        NumberResource,
        global_limit=1,
        target_attribute="base",
        limit_kind="max_value_count",
    )

    basic1_nsum_limit_set = ResourceLimitSet([basic1_limit, n_sum7_limit])

    assert basic1_nsum_limit_set.satisfied(basic_and_n5)
    assert not basic1_nsum_limit_set.satisfied(basic_and_n10)
    assert not basic1_nsum_limit_set.satisfied(single_n10)

    basic1_num_nunique_limit_set = ResourceLimitSet(
        [basic1_limit, n_nunique_limit]
    )

    assert basic1_num_nunique_limit_set.satisfied(basic_and_n10)
    assert basic1_num_nunique_limit_set.satisfied(single_n10 * 4)
    assert not basic1_num_nunique_limit_set.satisfied(basic_and_n10 * 2)
    assert not basic1_num_nunique_limit_set.satisfied(
        basic_and_n5 + single_n10
    )
    assert not basic1_num_nunique_limit_set.satisfied(
        basic_and_n5 + basic_and_n10
    )

    basic1_num_maxc_limit_set = ResourceLimitSet([basic1_limit, n_maxc_limit])
    assert basic1_num_maxc_limit_set.satisfied(single_n10 + basic_and_n5)
    assert not basic1_num_maxc_limit_set.satisfied(single_n10 + basic_and_n10)
