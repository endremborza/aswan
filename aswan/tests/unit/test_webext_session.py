import multiprocessing as mp
import time

import requests

import aswan
from aswan.constants import WE_REG_K, WE_SOURCE_K, WE_URL_K, WE_URL_ROUTE, WEBEXT_PORT
from aswan.url_handler import WebExtHandler

_URL = "url1"
_URL_SRC = "resp-txt"

_URL2 = "url2b"
_URL2_SRC = "resp-txt-b"


_REG_URL = "url2"
_REG_SRC = "reg-txt"


def test_simple(test_project2: aswan.Project):
    @test_project2.register_handler
    class WH(WebExtHandler):
        pass

    proc = mp.Process(target=_send_src_txt)
    proc.start()

    test_project2.run({WH: [_URL]}, force_sync=True)
    proc.join()

    def _get_res():
        return sorted(
            (c.url, c.content)
            for c in test_project2.depot.get_handler_events(WH, from_current=True)
        )

    assert _get_res() == [(_URL, _URL_SRC), (_REG_URL, _REG_SRC)]

    proc2 = mp.Process(target=_send_src2)
    proc2.start()

    test_project2.continue_run(urls_to_register={WH: [_URL2]}, force_sync=True)

    proc2.join()

    assert _get_res() == [(_URL, _URL_SRC), (_REG_URL, _REG_SRC), (_URL2, _URL2_SRC)]


def _send_src_txt():
    for _ in range(4):
        time.sleep(0.3)
        try:
            requests.post(
                f"http://localhost:{WEBEXT_PORT}",
                json={WE_SOURCE_K: _URL_SRC, WE_URL_K: _URL, WE_REG_K: [_REG_URL]},
            )
            requests.post(
                f"http://localhost:{WEBEXT_PORT}",
                json={WE_SOURCE_K: _REG_SRC, WE_URL_K: _REG_URL},
            )
            print("sent requests!!")
            return
        except Exception as e:
            print(f"failed sending with {e}")


def _send_src2():
    for _ in range(4):
        time.sleep(0.3)
        try:
            url_resp = requests.get(f"http://localhost:{WEBEXT_PORT}/{WE_URL_ROUTE}")
            assert url_resp.ok
            url = url_resp.text
            assert url
            resp = requests.post(
                f"http://localhost:{WEBEXT_PORT}",
                json={WE_SOURCE_K: _URL2_SRC, WE_URL_K: url},
            )
            print(f"sent request2 to {url} - {resp.text}")
            return
        except Exception as e:
            print(f"failed sending2 with {e}")
