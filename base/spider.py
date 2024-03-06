from __future__ import annotations
from gscraper.base.spider import Spider, AsyncSpider
from gscraper.base.types import Keyword, DateFormat, Data, TabularData

from gscraper.utils.cast import cast_date
from gscraper.utils.map import convert_data

from base.abstract import GET, POST, OPTIONS, HEAD, PUT, PATCH, DELETE, API, URL
from base.abstract import ALPHA, NAVER, SQUARE, YAHOO

from typing import List, Literal, Optional, Sequence, Union
from numbers import Real
from abc import ABCMeta
import functools
import datetime as dt
import pandas as pd

from holidays.countries import united_states
from workalendar.usa import UnitedStates

from holidays.countries import south_korea
from workalendar.asia import SouthKorea


EST = "US/Eastern"
KST = "Asia/Seoul"

Code = Union[str, Sequence[str]]
Symbol = Union[str, Sequence[str]]


###################################################################
########################## Date Function ##########################
###################################################################

def assure_date(func):
    @functools.wraps(func)
    def wrapper(__date: DateFormat, *args, **kwargs):
        if not isinstance(__date, dt.date):
            __date = cast_date(__date)
            if not isinstance(__date, dt.date):
                raise TypeError("Date type input is required.")
        return func(__date, *args, **kwargs)
    return wrapper


@assure_date
def is_us_working_day(__date: DateFormat) -> bool:
    calendar, holidays = UnitedStates(), united_states.US()
    return calendar.is_working_day(__date) & (__date not in holidays)


@assure_date
def is_kr_working_day(__date: DateFormat) -> bool:
    calendar, holidays = SouthKorea(), south_korea.KR()
    return calendar.is_working_day(__date) & (__date not in holidays)


@assure_date
def is_working_day(__date: DateFormat, tzinfo="US/Eastern") -> bool:
    if not isinstance(__date, dt.date): raise TypeError("Date type input is required.")
    if str(tzinfo) == EST: return is_us_working_day(__date)
    elif str(tzinfo) == KST: return is_kr_working_day(__date)
    elif tzinfo is None: return is_us_working_day(__date)
    else: raise ValueError(f"Invalid timezone entered: '{tzinfo}'")


@assure_date
def get_busday(__date: DateFormat, tzinfo="US/Eastern", how: Literal["previous","next"]="previous") -> dt.date:
    while not is_working_day(__date, tzinfo):
        if how == "previous": __date = __date - dt.timedelta(days=1)
        elif how == "next": __date = __date + dt.timedelta(days=1)
        else: raise ValueError(f"Invalid progress method entered: '{tzinfo}'")
    return __date


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
    has_date, has_datetime = ("date" in data), ("datetime" in data)
    keys = [column]+(["datetime"] if has_datetime else (["date"] if has_date else list()))
    data = data.sort_values(keys)
    return [data[data[column]==symbol] for symbol in data[column].unique()]


@assure_dataframe
def set_change(data: pd.DataFrame, trunc: Optional[int]=4) -> pd.DataFrame:
    if "close" not in data:
        return data
    if "preivousClose" not in data:
        data = set_previous_close(data)
    return _calc_change_by_price(data, trunc=trunc)


@assure_dataframe
def set_previous_close(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    if "datetime" in data:
        if "date" not in data:
            data["date"] = data["datetime"].apply(lambda x: x.date())
        daily = (data[data["volume"]>0].groupby("date").agg({"close":"last"}).
                reset_index().rename(columns={"close":"previousClose"}))
        daily["date"] = daily["date"].shift(-1)
        return data.merge(daily[daily["date"].notna()], how="left", on="date")
    data["previousClose"] = data["close"].shift(1)
    return data


@assure_dataframe
def _calc_change_by_price(data: pd.DataFrame, trunc: Optional[int]=4) -> pd.DataFrame:
    data = data.copy()
    for column, price in zip(PRICE_CHANGES, PRICE_COLUMNS):
        if (price == "open") and ("datetime" in data): continue
        elif price in data:
            change = (data[price] - data["previousClose"]) / data["previousClose"]
            if isinstance(trunc, int):
                data[column] = change.apply(lambda x: round(x, trunc) if pd.notna(x) and isinstance(x, float) else x)
    return data


@assure_dataframe
def set_draw_down(data: pd.DataFrame, maxPrice: Optional[Real]=None) -> pd.DataFrame:
    if ("high" not in data) or ("close" not in data): return data
    elif isinstance(maxPrice, (float,int)):
        data = pd.concat([pd.DataFrame([{"high":maxPrice}], columns=data.columns), data])
    else: data = data.copy()
    data["maxPrice"] = data["high"].cummax()
    if isinstance(maxPrice, (float,int)):
        data = data.iloc[1:].copy()
    data["drawDown"] = (data["close"] - data["maxPrice"]) / data["maxPrice"]
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
