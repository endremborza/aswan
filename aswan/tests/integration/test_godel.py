from collections import Counter

import pandas as pd

import aswan
import aswan.tests.godel_src.handlers as ghandlers
import aswan.tests.godel_src.registrands as gregs

# import pytest


# @pytest.mark.parametrize("dist_api", [("sync"), ("mp")])
def test_godel(godel_test_app, test_project: aswan.Project):
    """
    missing to test:
    - proxies
    - url params
    - cookies
    """

    conf = test_project.config
    json_trepo = conf.get_prod_table("js")
    click_trepo = conf.get_prod_table("click")

    class JsConc(aswan.FlexibleDfParser):
        handlers = [ghandlers.JS]

        def write_df(self, df):
            return json_trepo.extend(df)

    class T2IntJS(aswan.FlexibleDfParser):

        handlers = [ghandlers.Clicker]

        def write_df(self, df):
            return click_trepo.replace_records(df)

        @staticmethod
        def proc_df(df: pd.DataFrame):
            return df.set_index("field2")

    test_project.register_module(ghandlers)
    test_project.register_module(gregs)

    for t2i in [JsConc, T2IntJS]:
        test_project.register_t2_integrator(t2i)

    for trepo in [json_trepo, click_trepo]:
        test_project.register_t2_table(trepo)

    test_project.set_env(aswan.AswanConfig.test_name)
    test_project.run()

    assert (
        next(test_project.handler_events(ghandlers.LinkRoot)).status
        == aswan.Statuses.PROCESSED
    )

    jsdf = json_trepo.get_full_df()
    assert jsdf.shape[0] == 1
    assert jsdf["A"].iloc[0] == 10
    assert (
        next(test_project.handler_events(ghandlers.GetMain, limit=1)).content["main"]
        == "Alonzo Church"
    )

    cldf = click_trepo.get_full_df()
    assert cldf.shape[0] == 1
    assert cldf["field4"].iloc[0] > 1000

    test_project.set_env(aswan.AswanConfig.prod_name)
    test_project.run(with_monitor_process=True)

    found = [
        pcev.content["main"] for pcev in test_project.handler_events(ghandlers.GetMain)
    ]
    assert sorted(found) == ["Alonzo Church", "Entscheidungsproblem"]
    for pcev in test_project.handler_events(ghandlers.GetMain, only_successful=False):
        if pcev.status == aswan.Statuses.CONNECTION_ERROR:
            assert pcev.url.split("/")[-1] == "Alan_Turing"

    pre_exp_df = click_trepo.get_full_df()
    test_project.push()

    test_project.run()
    post_exp_df = click_trepo.get_full_df()

    comp_dict = (
        pre_exp_df["field4"] == post_exp_df.reindex(pre_exp_df.index)["field4"]
    ).to_dict()
    assert comp_dict == {"Good": False, "Bad": True, "Static": True}

    urlcount = Counter(
        [
            pcev.url.split("/")[-1]
            for pcev in test_project.handler_events(
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

    test_project.pull(force=True)
    assert click_trepo.get_full_df().equals(pre_exp_df)

    test_project.add_urls_to_handler(ghandlers.GetMain, ["/test_page/Axiom.html"], True)
    test_project.set_env(aswan.AswanConfig.exp_name)
    test_project.run()
    assert click_trepo.get_full_df().shape[0] == 0
