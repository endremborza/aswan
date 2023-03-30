from functools import partial
from pathlib import Path

import aswan
from aswan.depot import AswanDepot
from aswan.depot.remote import get_remote
from aswan.models import CollEvent, RegEvent

_rev = partial(RegEvent, url="url1", handler="H1")
_cev = partial(
    CollEvent,
    url="url1",
    handler="H1",
    status=aswan.Statuses.PROCESSED,
    timestamp=0,
    output_file="",
)


def test_push(env_auth_id: str, tmp_path: Path):
    # if os.name == "nt": maybe not
    #    return  # SSH needs to point to linux

    depot = AswanDepot("test", local_root=tmp_path / "depot1").setup(True)
    depot.current.integrate_events([_rev()])
    depot.save_current()
    depot.push(env_auth_id)
    depot.push(env_auth_id)

    depot2 = AswanDepot("test", local_root=tmp_path / "depot2").setup(True)
    depot2.current.integrate_events([_rev(url="url2")])
    depot2.save_current()
    depot2.push(env_auth_id)

    depot.pull(env_auth_id)
    depot.current.purge()
    depot.set_as_current(depot.get_complete_status())

    assert sorted(
        [su.url for su in depot.current.next_batch(3, to_processing=False)]
    ) == ["url1", "url2"]


def test_pull(env_auth_id: str):
    depot = AswanDepot("puller").setup(True)
    _of = depot.object_store.dump_bytes(b"XYZ")
    depot.current.integrate_events([_rev(), _rev(url="url2"), _cev(output_file=_of)])
    first_status = depot.save_current().name
    depot.current.purge()
    depot.init_w_complete()
    depot.current.integrate_events(
        [_rev(url="url3"), _rev(url="url4"), _cev(url="url2")]
    )
    second_status = depot.save_current().name
    depot.push()
    depot.purge().setup(True)

    def _get_cev_urls():
        return sorted([pcev.url for pcev in depot.get_handler_events(past_runs=2)])

    depot.pull(post_status=first_status)
    assert _get_cev_urls() == ["url2"]
    assert not [*depot.object_store_path.iterdir()]
    depot.purge()
    depot.pull(post_status=second_status)
    assert _get_cev_urls() == []
    depot.purge()
    depot.pull(complete=True)
    assert _get_cev_urls() == ["url1", "url2"]
    assert (
        next(depot.object_store_path.iterdir()).name
        == _of[: depot.object_store.prefix_chars]
    )


def test_pull_nothing(env_auth_id: str):
    depot = AswanDepot("empty-pull").setup()
    depot.pull()


def test_cache_overwrite_error(env_auth_id: str):
    depot = AswanDepot("cache-err").setup(True)
    depot.current.integrate_events([_rev(url="url1")])
    depot.save_current()
    depot.push()
    depot.current.purge()
    depot.init_w_complete()
    depot.current.integrate_events([_rev(url="url2")])
    depot.save_current()

    with get_remote(env_auth_id) as conn:
        conn.run(f"chmod 400 {conn.cwd}/{depot.name}/{depot._cache_path.name}")

    depot.push()


def test_continue(env_auth_id: str):
    depot = AswanDepot("test-continue").setup(True)
    depot.current.integrate_events([_rev()])
    half = depot.save_current()
    depot.current.purge()
    depot.init_w_complete()
    depot.current.integrate_events([_rev(url="url2")])
    depot.save_current()
    depot.push()
    depot.purge()

    depot.setup(True)
    depot.current.integrate_events([_rev(url="url3")])
    depot.save_current()
    depot.push()
    depot.purge()

    depot.pull(post_status=half.name)
