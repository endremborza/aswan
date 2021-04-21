import re
from typing import List

from bs4 import BeautifulSoup


def get_matching_links(reg_str: str, soup: BeautifulSoup) -> List[str]:
    link_re = re.compile(reg_str)
    return list(
        set(
            [
                link["href"]
                for link in soup.find_all("a")
                if link_re.findall(link.get("href", ""))
            ]
        )
    )
