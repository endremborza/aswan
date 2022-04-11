from typing import Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait


def browser_wait(
    browser: Chrome,
    wait_for_id: Optional[str] = None,
    wait_for_xpath: Optional[str] = None,
    wait_for_tag: Optional[str] = None,
    wait_for_class: Optional[str] = None,
    timeout: int = 20,
    click: bool = False,
):

    element_tuples = []
    for elem_id, by_descriptor in zip(
        [wait_for_id, wait_for_tag, wait_for_xpath, wait_for_class],
        [By.ID, By.TAG_NAME, By.XPATH, By.CLASS_NAME],
    ):
        if elem_id:
            element_tuples.append((by_descriptor, elem_id))

    for element_tuple in element_tuples:
        element_present = expected_conditions.presence_of_element_located(element_tuple)
        try:
            WebDriverWait(browser, timeout).until(element_present)
        except TimeoutException:
            raise TimeoutException
        if click:
            browser.find_element(*element_tuple)
