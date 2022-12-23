import aswan
from aswan.tests.godel_src.app import test_app_default_address

base_url = f"{test_app_default_address}/test_param"


def _range_dic(n):
    urls = [aswan.add_url_params(base_url, {"param": i}) for i in range(n)]
    return {aswan.RequestHandler: urls}


def test_simple(godel_env):

    name = "godel-simple"
    depot = aswan.AswanDepot(name)

    _it = list(range(10))
    aswan.run_simple_project(_range_dic(10), name, remote=True)

    depot.purge()
    depot.pull(complete=True)
    assert _get_nums(depot, past_runs=2) == _it


def test_load(godel_env):
    project = aswan.Project("godel-load")
    project.max_cpu_use = 1
    depot = project.depot

    def _pp():
        project.commit_current_run()
        depot.push()
        depot.purge()

    project.run(_range_dic(2))
    _pp()

    project.run(_range_dic(3))
    _pp()

    depot.pull()
    complete = depot.get_complete_status()
    assert len(depot._get_full_run_tree(complete)) == 2
    project.run(_range_dic(4))
    _pp()

    depot.pull(post_status=complete.name)
    assert len(depot.get_all_run_ids()) == 1
    assert depot.get_complete_status().parent == complete.name


class PG(aswan.RequestHandler):
    def parse(self, blob: bytes):

        i = int(blob.decode("utf-8"))
        next_page = {}
        if i < 2:
            next_page["param"] = i + 1
        self.register_url_with_params(next_page)

        return blob


def test_paginate(godel_env):
    project = aswan.Project("godel-pages")
    project.run({PG: [aswan.add_url_params(base_url, {"param": 1})]}, force_sync=True)
    assert _get_nums(project.depot, from_current=True) == [1, 2]


def _get_nums(depot: aswan.AswanDepot, **kwargs):
    return sorted(int(pv.content.decode()) for pv in depot.get_handler_events(**kwargs))
