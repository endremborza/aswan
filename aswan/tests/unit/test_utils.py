import pytest

from aswan.utils import add_url_params, get_url_root, run_and_log_functions


def test_fun_logger(capsys):

    d = {}

    def fing():
        d["k"] = 10

    run_and_log_functions([fing], kw=15)

    captured = capsys.readouterr()

    assert "fing" in captured.out
    assert "15" in captured.out
    assert "kw" in captured.out
    assert d["k"] == 10


@pytest.mark.parametrize(
    ("params", "suffix"),
    [({"k": "v"}, "?k=v"), ({"k1": 1, "k2": "v2"}, "?k1=1&k2=v2")],
)
def test_url_param_add(params, suffix):
    root = "http://test.com"
    assert add_url_params(root, params) == f"{root}{suffix}"


def test_extend_url_params():
    assert (
        add_url_params("http://test.com?k1=v1", {"k2": "v2", "k3": "v3"})
        == "http://test.com?k1=v1&k2=v2&k3=v3"
    )


@pytest.mark.parametrize(
    "full_url",
    [
        "http://test.com",
        "http://test.com/a/b",
        "http://test.com/a/b/k.html",
        "http://test.com/a/b/k.html",
        "http://test.com?k1=v1&k2=v2&k3=v3",
    ],
)
def test_url_root(full_url):
    assert get_url_root(full_url) == "http://test.com"


def test_url_root_weird():

    assert get_url_root("http://te-st.com/a/b") == "http://te-st.com"
    assert get_url_root("https://xx.re.te-st.com/a/b") == "https://xx.re.te-st.com"
    assert get_url_root("nothing") is None
