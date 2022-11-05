import aswan
from aswan.tests.godel_src.app import test_app_default_address


def test_load(godel_test_app, env_auth_id):
    # project = aswan.Project("godel-1")

    name = "godel-1"
    depot = aswan.AswanDepot(name)

    _it = list(range(10))
    base_url = f"{test_app_default_address}/test_param"
    urls = [aswan.add_url_params(base_url, {"param": i}) for i in _it]
    aswan.run_simple_project({aswan.RequestHandler: urls}, name, remote=True)

    depot.purge()
    depot.pull(complete=True)
    content = [int(pv.content.decode()) for pv in depot.get_handler_events(past_runs=2)]
    assert sorted(content) == _it

    # TODO: processed upto Status
    # pull necessary runs, and only necessary objects
