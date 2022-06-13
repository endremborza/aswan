import pandas as pd

from aswan import AswanConfig


def test_push_pull(test_config: AswanConfig):
    # TODO: figure this out
    df1 = pd.DataFrame([{"A": 10}])
    tab = test_config.get_prod_table("tab")
    tab.extend(df1)
    test_config.push()
    tabfp = test_config.remote.t2_root / "tab.parquet"
    assert pd.read_parquet(tabfp).equals(df1)


def test_proper_validation():
    pass
