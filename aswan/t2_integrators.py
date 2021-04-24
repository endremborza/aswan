from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

import pandas as pd
from parquetranger import TableRepo

from .url_handler import UrlHandler

if TYPE_CHECKING:
    from aswan.project.core import ParsedCollectionEvent


class T2Integrator(ABC):
    @abstractmethod
    def parse_pcevlist(self, cevs: List["ParsedCollectionEvent"]):
        pass  # pragma: nocover


class FlexibleDfParser(T2Integrator):
    """parses all content in cls.handlers to dataframes

    converts dicts to lists of dicts by default

    set class prop 'method' to extend/replace_inds/replace_all
    """

    method = "extend"
    by_groups = False
    handlers: Optional[List] = None

    _fix = "flex"  # "felx/conc/rec"

    def parse_pcevlist(self, cevs: List["ParsedCollectionEvent"]):
        out = []
        handler_names = _handlers_to_name(self.handlers)
        for cev in cevs:
            if (self.handlers is None) or (cev.handler_name in handler_names):
                cev_content = cev.content
                df_base = cev_content
                if (self._fix == "rec") or (
                    self._fix == "flex" and isinstance(cev_content, dict)
                ):
                    df_base = [cev_content]
                out.append(
                    pd.DataFrame(df_base).assign(**self.url_parser(cev.url))
                )
        if not out:
            return
        new_df = pd.concat(out).pipe(self.proc_df)
        trepo = self.get_t2_table()
        if self.method == "extend":
            trepo.extend(new_df)
        elif self.method == "replace_inds":
            trepo.replace_records(new_df, by_groups=self.by_groups)
        elif self.method == "replace_all":
            trepo.replace_all(new_df)

    def url_parser(self, url):
        return {}

    def proc_df(self, df):
        return df

    @abstractmethod
    def get_t2_table(self) -> TableRepo:
        pass  # pragma: nocover


class ConcatToT2(FlexibleDfParser):
    _fix = "conc"  # "felx/conc/rec"


class RecordsToT2(FlexibleDfParser):
    _fix = "rec"  # "felx/conc/rec"


def _handlers_to_name(handlers):
    if handlers is None:
        return []
    out = []
    for h in handlers:
        if isinstance(h, str):
            hname = h
        elif isinstance(h, UrlHandler):
            hname = h.name
        else:
            hname = h().name

        out.append(hname)
    return out
