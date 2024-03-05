from __future__ import annotations
from gscraper.base.spider import Spider, AsyncSpider
from gscraper.base.types import Keyword, Data, TabularData
from gscraper.utils.map import convert_data

from base.abstract import GET, POST, OPTIONS, HEAD, PUT, PATCH, DELETE, API, URL
from base.abstract import ALPHA, NAVER, SQUARE, YAHOO

from abc import ABCMeta
from typing import List, Sequence, Union
import functools
import pandas as pd


EST = "US/Eastern"
KST = "Asia/Seoul"

Code = Union[str, Sequence[str]]
Symbol = Union[str, Sequence[str]]


###################################################################
########################## Price Function #########################
###################################################################

PRICE_COLUMNS = ["open", "high", "low", "close"]
PRICE_CHANGES = ["gap", "highPct", "lowPct", "change"]


def change(__from: float, __to: float) -> float:
    return (__to - __from) / __from


def assure_dataframe(func):
    @functools.wraps(func)
    def wrapper(data: TabularData, *args, **kwargs):
        if not isinstance(data, pd.DataFrame):
            data = func(convert_data(data, return_type="dataframe"), *args, **kwargs)
            return convert_data(data, return_type="records")
        else: return func(data, *args, **kwargs)
    return wrapper


@assure_dataframe
def groupby_symbols(data: pd.DataFrame, column="symbol") -> List[pd.DataFrame]:
    if column not in data: return [data]
    has_date, has_datetime = "date" in data, "datetime" in data
    keys = [column]+(["datetime"] if has_datetime else (["date"] if has_date else list()))
    data = data.sort_values(keys)
    return [data[data[column]==symbol] for symbol in data[column].unique()]


@assure_dataframe
def set_change(data: pd.DataFrame, trunc=4) -> pd.DataFrame:
    if "close" not in data: return data
    if "preivousClose" not in data: data = set_previous_close(data).copy()
    return calc_change(data, trunc=trunc)


def set_previous_close(data: pd.DataFrame) -> pd.DataFrame:
    if "datetime" in data:
        if "date" not in data: data["date"] = data["datetime"].apply(lambda x: x.date())
        daily = (data[data["volume"]>0].groupby("date").agg({"close":"last"}).
                reset_index().rename(columns={"close":"previousClose"}))
        daily["date"] = daily["date"].shift(-1)
        return data.merge(daily[daily["date"].notna()], how="left", on="date")
    data["previousClose"] = data["close"].shift(1)
    return data


def calc_change(data: pd.DataFrame, trunc=4) -> pd.DataFrame:
    for column, price in zip(PRICE_CHANGES, PRICE_COLUMNS):
        if (price == "open") and ("datetime" in data): continue
        if price in data:
            change = (data[price] - data["previousClose"]) / data["previousClose"]
            data[column] = change.apply(lambda x: round(x, trunc))
    return data


###################################################################
########################### Fiannce Base ##########################
###################################################################

class FinanceSpider(Spider):
    __metaclass__ = ABCMeta
    operation = "finance"
    iterateArgs = ["symbol"]
    iterateUnit = 1

    @Spider.init_session
    def crawl(self, symbol: Keyword, **context) -> Data:
        args, context = self.validate_params(symbol=symbol, **context)
        return self.gather(*args, **context)


class FinanceKrSpider(Spider):
    __metaclass__ = ABCMeta
    operation = "finance"
    iterateArgs = ["code"]
    iterateUnit = 1

    @Spider.init_session
    def crawl(self, code: Keyword, **context) -> Data:
        args, context = self.validate_params(code=code, **context)
        return self.gather(*args, **context)


class FinanceAsyncSpider(AsyncSpider):
    __metaclass__ = ABCMeta
    operation = "finance"
    iterateArgs = ["symbol"]
    iterateUnit = 1

    @AsyncSpider.init_session
    async def crawl(self, symbol: Keyword, **context) -> Data:
        args, context = self.validate_params(symbol=symbol, **context)
        return await self.gather(*args, **context)


class FinanceKrAsyncSpider(AsyncSpider):
    __metaclass__ = ABCMeta
    operation = "finance"
    iterateArgs = ["code"]
    iterateUnit = 1

    @AsyncSpider.init_session
    async def crawl(self, code: Keyword, **context) -> Data:
        args, context = self.validate_params(code=code, **context)
        return await self.gather(*args, **context)
