import json
from functools import partial
from urllib.parse import ParseResult, parse_qsl, unquote, urlencode, urlparse


def is_subclass(kls, base):
    try:
        return base in kls.mro()
    except (AttributeError, TypeError):
        return False


def browser_wait(
    browser,
    wait_for_id: str | None = None,
    wait_for_xpath: str | None = None,
    wait_for_tag: str | None = None,
    wait_for_class: str | None = None,
    timeout: int = 20,
    click: bool = False,
):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions
    from selenium.webdriver.support.ui import WebDriverWait

    by_map = {
        By.ID: wait_for_id,
        By.TAG_NAME: wait_for_tag,
        By.XPATH: wait_for_xpath,
        By.CLASS_NAME: wait_for_class,
    }

    for element_tuple in by_map.items():
        if element_tuple[1] is None:
            continue
        element_present = expected_conditions.presence_of_element_located(element_tuple)
        WebDriverWait(browser, timeout).until(element_present)
        if click:
            browser.find_element(*element_tuple).click()


def run_and_log_functions(function_list: list, **kwargs):
    for fun in function_list:
        fun_name = fun.func.__name__ if isinstance(fun, partial) else fun.__name__
        print(f"running function {fun_name}", kwargs)
        res = fun()
        print(f"function {fun_name} returned {res}", kwargs)


def add_url_params(url, params):
    url = unquote(url)
    parsed_url = urlparse(url)
    get_args = parsed_url.query
    parsed_get_args = dict(parse_qsl(get_args))
    parsed_get_args.update(params)
    parsed_get_args.update(
        {
            k: json.dumps(v)
            for k, v in parsed_get_args.items()
            if isinstance(v, (bool, dict))
        }
    )

    encoded_get_args = urlencode(parsed_get_args, doseq=True)
    return ParseResult(
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        encoded_get_args,
        parsed_url.fragment,
    ).geturl()
