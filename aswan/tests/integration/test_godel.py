from collections import Counter

import pandas as pd
from parquetranger import TableRepo

import aswan
import aswan.tests.godel_src.handlers as ghandlers
import aswan.tests.godel_src.registrands as gregs


def test_godel(tmp_path, godel_test_app):
    """
    missing to test:
    - proxies
    - url params
    - cookies
    """

    godel_project = aswan.project_from_dir(
        tmp_path, prod_distributed_api=aswan.config_class.DEFAULT_PROD_DIST_API
    )
    conf = godel_project.config

    json_trepo = TableRepo(conf.prod.t2_path / "js", env_parents=conf.t2_root_dic)
    click_trepo = conf.get_prod_table("click")

    class JsConc(aswan.RecordsToT2):
        handlers = [ghandlers.JS]

        def get_t2_table(self):
            return json_trepo

    class T2IntJS(aswan.T2Integrator):
        def parse_pcevlist(self, pcevs):
            for pc in pcevs:
                if pc.handler_name == ghandlers.Clicker().name:
                    click_trepo.replace_records(
                        pd.DataFrame([pc.content]).set_index("field2")
                    )

    godel_project.register_module(ghandlers)
    godel_project.register_module(gregs)

    for t2i in [JsConc, T2IntJS]:
        godel_project.register_t2_integrator(t2i)

    for trepo in [json_trepo, click_trepo]:
        godel_project.register_t2_table(trepo)

    godel_project.set_env(aswan.Envs.TEST)
    godel_project.run()

    assert (
        next(godel_project.handler_events(ghandlers.LinkRoot)).status
        == aswan.Statuses.PROCESSED
    )

    jsdf = json_trepo.get_full_df()
    assert jsdf.shape[0] == 1
    assert jsdf["A"].iloc[0] == 10
    assert (
        next(godel_project.handler_events(ghandlers.GetMain, limit=1)).content["main"]
        == "Alonzo Church"
    )

    cldf = click_trepo.get_full_df()
    assert cldf.shape[0] == 1
    assert cldf["field4"].iloc[0] == 1000

    godel_project.set_env(aswan.Envs.PROD)
    godel_project.run(with_monitor_process=True)

    found = [
        pcev.content["main"] for pcev in godel_project.handler_events(ghandlers.GetMain)
    ]
    assert sorted(found) == ["Alonzo Church", "Entscheidungsproblem"]
    for pcev in godel_project.handler_events(ghandlers.GetMain, only_successful=False):
        if pcev.status == aswan.Statuses.CONNECTION_ERROR:
            assert pcev.url.split("/")[-1] == "Alan_Turing"

    pre_exp_df = click_trepo.get_full_df()
    godel_project.push()

    godel_project.run()
    post_exp_df = click_trepo.get_full_df()

    comp_dict = (
        pre_exp_df["field4"] == post_exp_df.reindex(pre_exp_df.index)["field4"]
    ).to_dict()
    assert comp_dict == {"Good": False, "Bad": True, "Static": True}

    urlcount = Counter(
        [
            pcev.url.split("/")[-1]
            for pcev in godel_project.handler_events(
                ghandlers.Clicker, only_latest=False
            )
        ]
    )

    for k, v in urlcount.items():
        if k == "jstest.html":
            assert v == 2
        else:
            assert v == 1
    assert len(urlcount) == 3

    godel_project.pull()
    assert click_trepo.get_full_df().equals(pre_exp_df)
