from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, List, Optional

import pandas as pd
from parquetranger import TableRepo

if TYPE_CHECKING:
    from .project import ParsedCollectionEvent


class T2Integrator(ABC):
    @abstractmethod
    def parse_pcevlist(self, cevs: List["ParsedCollectionEvent"]):
        pass  # pragma: nocover


class FlexibleDfParser(T2Integrator):
    """parses all content in cls.handlers to dataframes

    converts dicts to lists of dicts by default
    """

    handlers: Optional[List] = None

    def parse_pcevlist(self, pcevs: Iterable["ParsedCollectionEvent"]):
        out = []
        handler_names = _handlers_to_name(self.handlers)
        for pcev in pcevs:
            if (self.handlers is None) or (pcev.handler_name in handler_names):
                df_base = self.wrap_content(pcev.content)
                url_dic = self.url_parser(pcev.url)
                if url_dic:
                    df_base = [{**d, **url_dic} for d in df_base]
                out += df_base
        if not out:
            return
        self.write_df(pd.DataFrame(out).pipe(self.proc_df))

    @staticmethod
    def url_parser(url: str):
        return {}

    @staticmethod
    def proc_df(df: pd.DataFrame):
        return df

    @staticmethod
    def wrap_content(content):
        if isinstance(content, dict):
            return [content]
        return content

    @abstractmethod
    def write_df(self, df: pd.DataFrame) -> TableRepo:
        pass  # pragma: nocover


def _handlers_to_name(handlers):
    if handlers is None:
        return []
    out = []
    for h in handlers:
        if isinstance(h, str):
            hname = h
        elif isinstance(h, type):
            hname = h.__name__
        out.append(hname)
    return out
