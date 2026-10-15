"""End-to-end depot flows: multi-run history, past_runs iterable, merge dedup."""

import aswan

from ..helpers import get_cev, seed_run


def test_past_runs_iterable_filters_runs(tmp_path):
    depot = aswan.AswanDepot("PR", tmp_path).setup(init=True)
    seed_run(depot, output_file="of-1")
    seed_run(depot, output_file="of-2")
    seed_run(depot, output_file="of-3")

    runs = sorted(depot.get_all_run_ids())
    assert len(runs) == 3

    middle_only = list(
        depot.get_handler_events(past_runs=[runs[1]], only_successful=False)
    )
    assert {e.cev.output_file for e in middle_only} == {"of-2"}


def test_past_runs_int_limit(tmp_path):
    depot = aswan.AswanDepot("PRI", tmp_path).setup(init=True)
    seed_run(depot, output_file="of-1")
    seed_run(depot, output_file="of-2")
    seed_run(depot, output_file="of-3")

    latest_two = list(
        depot.get_handler_events(past_runs=2, only_latest=False, only_successful=False)
    )
    files = {e.cev.output_file for e in latest_two}
    assert "of-3" in files
    assert len(files) == 2


def test_empty_depot_has_no_runs(tmp_path):
    depot = aswan.AswanDepot("E", tmp_path).setup()
    assert depot.get_all_run_ids() == set()
    assert list(depot.get_handler_events()) == []


def test_merge_preserves_object_store_files(tmp_path):
    a = aswan.AswanDepot("MA", tmp_path / "a").setup(init=True)
    b = aswan.AswanDepot("MA", tmp_path / "b").setup(init=True)

    seed_run(a, url="u-a", output_file=a.object_store.dump_str("from-a"))
    b_blob = b.object_store.dump_str("from-b")
    seed_run(b, url="u-b", output_file=b_blob)

    a.merge(b)

    a.init_w_complete()
    urls = {e.url for e in a.get_handler_events(only_successful=False)}
    assert urls == {"u-a", "u-b"}
    # the object stored by b must be readable via the merged depot
    assert a.object_store.read(b_blob) == "from-b"


def test_merge_is_idempotent(tmp_path):
    a = aswan.AswanDepot("MI", tmp_path / "a").setup(init=True)
    b = aswan.AswanDepot("MI", tmp_path / "b").setup(init=True)
    seed_run(b, url="ub", output_file=b.object_store.dump_str("x"))

    a.merge(b)
    runs1 = set(a.get_all_run_ids())
    a.merge(b)  # no-op
    assert set(a.get_all_run_ids()) == runs1


def test_parsed_event_repr_includes_status_and_url(tmp_path):
    depot = aswan.AswanDepot("R", tmp_path).setup(init=True)
    blob = depot.object_store.dump_str("hi")
    depot.current.integrate_events([get_cev(url="u-r", output_file=blob)])
    ev = next(depot.get_handler_events(from_current=True))
    s = repr(ev)
    assert "u-r" in s and ev.status in s
