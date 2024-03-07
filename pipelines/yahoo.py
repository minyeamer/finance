from pipelines import Pipeline, Dag, Task, EST, KST
from base.spider import get_busday, set_change, set_draw_down

from spiders.yahoo import YahooPriceSpider
from data.yahoo import DAILY_NASDAQ_INFO, DAILY_KOSPI_INFO, DAILY_KOSDAQ_INFO

from gscraper.base.types import TypeHint, IndexLabel, DateFormat, Timezone, Data
from gscraper.utils.map import cloc

from typing import Dict, Literal, Optional
from numbers import Real
from abc import ABCMeta

import datetime as dt
import pandas as pd


###################################################################
########################### Yahoo Price ###########################
###################################################################

PRICE_FIELDS = ["date", "open", "high", "low", "close", "volume"]
CHANGE_FIELDS = ["gap", "highPct", "lowPct", "previousClose", "change"]
PRICE_CHANGE_FIELDS = ["date", "open", "gap", "high", "highPct", "low", "lowPct", "close", "previousClose", "change"]

lower_first = lambda __s: __s[0].lower() + __s[1:] if isinstance(__s, str) and __s else str(__s)

PRE_PRICE_FIELDS = ["preHigh", "preHighPct", "preLow", "preLowPct", "preChange"]
PRE_PRICE_RENAME = {lower_first(__field.replace("pre", '')): __field for __field in PRE_PRICE_FIELDS}

POST_PRICE_FIELDS = ["postHigh", "postHighPct", "postLow", "postLowPct", "postChange"]
POST_PRICE_RENAME = {lower_first(__field.replace("post", '')): __field for __field in POST_PRICE_FIELDS}

PREPOST_FIELDS = PRE_PRICE_FIELDS + POST_PRICE_FIELDS

DRAWDOWN_FIELDS = ["maxPrice", "drawDown"]

class DailyPipeline(Pipeline):
    __metaclass__ = ABCMeta
    operation = "dailyPipeline"

    @Pipeline.init_task
    def crawl(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                maxPrice: Optional[Real]=None, trunc: Optional[int]=2, tzinfo: Optional[Timezone]=None, **context) -> Data:
        startDate, endDate = self.get_date_pair(startDate, endDate, if_null=(1,0))
        startDate, endDate = get_busday(startDate, tzinfo), get_busday(endDate, tzinfo)
        context = self.from_locals(locals(), ranges=[dict(field="date", left=startDate, right=endDate)], drop=["symbol"])
        return self.gather(**context)

    def gather(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, trunc: Optional[int]=2, **context) -> Data:
        data = dict()
        for task in self.dags:
            data[task["dataName"]] = self.run_task(task, symbol=[task["dataName"]], trunc=trunc, **context)
        trunc = trunc+2 if isinstance(trunc, int) else None
        return self.map_reduce(data=data, fields=fields, returnType=returnType, **context)


US_INDEX_SYMBOLS = ["^IXIC", "^VIX", "DX-Y.NYB", "^IRX", "^TNX", "CL=F", "BTC-USD"]
US_INDEX_FIELDS = ["VIX", "USDX", "IRX", "TNX", "CL=F"]

BTC_FIELDS = ["BTC-USD", "BTC-Change"]
BTC_RENAME = {"close":"BTC-USD", "change":"BTC-Change"}

DAILY_NASDAQ_FIELDS = PRICE_CHANGE_FIELDS + PREPOST_FIELDS + DRAWDOWN_FIELDS + US_INDEX_FIELDS + BTC_FIELDS + ["volume"]

class DailyNasdaqPipeline(DailyPipeline):
    operation = "dailyNasdaq"
    fields = DAILY_NASDAQ_FIELDS
    tzinfo = EST
    returnType = "dataframe"
    info = DAILY_NASDAQ_INFO()
    dags = Dag(
        Task(operator=YahooPriceSpider, name="nasdaq", task="request_crawl", dataName="^IXIC", dataType="dataframe",
            fields=tuple(PRICE_FIELDS), freq="1d", which="NASDAQ Value"),
        Task(operator=YahooPriceSpider, name="nasdaqFutures", task="request_crawl", dataName="QQQ", dataType="dataframe",
            fields=tuple(["datetime"]+PRICE_FIELDS), freq="1h", prepost=True, which="QQQ Price"),
        Task(operator=YahooPriceSpider, name="vix", task="request_crawl", dataName="^VIX", dataType="dataframe",
            fields=("date","close"), freq="1d", which="Volatility Index"),
        Task(operator=YahooPriceSpider, name="dollarIndex", task="request_crawl", dataName="DX-Y.NYB", dataType="dataframe",
            fields=("date","close"), freq="1d", which="US Dollar Index"),
        Task(operator=YahooPriceSpider, name="13wTreasury", task="request_crawl", dataName="^IRX", dataType="dataframe",
            fields=("date","close"), freq="1d", which="13-week Treasury"),
        Task(operator=YahooPriceSpider, name="10yTreasury", task="request_crawl", dataName="^TNX", dataType="dataframe",
            fields=("date","close"), freq="1d", which="10-year Treasury"),
        Task(operator=YahooPriceSpider, name="crudeOil", task="request_crawl", dataName="CL=F", dataType="dataframe",
            fields=("date","close"), freq="1d", which="Crude Oil Price"),
        Task(operator=YahooPriceSpider, name="bitcoin", task="request_crawl", dataName="BTC-USD", dataType="dataframe",
            fields=("date","close"), freq="1d", which="Bitcoin Price"),
    )

    @Pipeline.arrange_data
    def map_reduce(self, data: Dict[str,pd.DataFrame], maxPrice: Optional[Real]=None, trunc: Optional[int]=4, **context) -> Data:
        return (set_draw_down(set_change(data["^IXIC"], trunc), maxPrice, trunc).
            merge(self.map_prepost_price(set_change(data["QQQ"], trunc), data["^IXIC"]), how="left", on="date").
            merge(data["^VIX"].rename(columns={"close":"VIX"}), how="left", on="date").
            merge(data["DX-Y.NYB"].rename(columns={"close":"USDX"}), how="left", on="date").
            merge(data["^IRX"].rename(columns={"close":"IRX"}), how="left", on="date").
            merge(data["^TNX"].rename(columns={"close":"TNX"}), how="left", on="date").
            merge(data["CL=F"].rename(columns={"close":"CL=F"}), how="left", on="date").
            merge(set_change(data["BTC-USD"], trunc).rename(columns=BTC_RENAME)[["date"]+BTC_FIELDS], how="left", on="date"))

    def map_prepost_price(self, prepost: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
        if prepost.empty: return pd.DataFrame(columns=["date"]+PRE_PRICE_FIELDS+POST_PRICE_FIELDS)
        pre, post = self.agg_daily_prepost(prepost, daily, type="pre"), self.agg_daily_prepost(prepost, daily, type="post")
        pre, post = pre.rename(columns=PRE_PRICE_RENAME), post.rename(columns=POST_PRICE_RENAME)
        return cloc(pre.merge(post, how="outer", on="date"), columns=["date"]+PREPOST_FIELDS, if_null="drop")

    def agg_daily_prepost(self, prepost: pd.DataFrame, daily: pd.DataFrame, type: Literal["pre","post"]) -> pd.DataFrame:
        data = self.filter_prepost_time(prepost, type)
        if "change" in data:
            data = data.groupby("date").agg({"high":"max","highPct":"max","low":"min","lowPct":"min"}).reset_index()
            data = data.merge(daily[["date","open"]], how="left", on="date")
            data["change"] = data.apply(lambda row: self.calc_prepost_change(row), axis=1)
            return data.drop(columns="open")
        else: return data.groupby("date").agg({"high":"max","low":"min"}).reset_index()

    def filter_prepost_time(self, data: pd.DataFrame, type: Literal["pre","post"]) -> pd.DataFrame:
        if type == "pre":
            return data[(data["volume"]==0)&data["datetime"].apply(lambda x: x.time()<=dt.time(9,30))]
        elif type == "post":
            return data[(data["volume"]==0)&data["datetime"].apply(lambda x: x.time()>=dt.time(16))]
        else: return data

    def calc_prepost_change(self, row: pd.Series) -> float:
        high_lt_low = abs(row["open"]-row["high"]) > abs(row["open"]-row["low"])
        return row["highPct"] if high_lt_low else row["lowPct"]


KS_TOP_SYMBOL = "005930.KS"

KS_INDEX_SYMBOLS = ["KS200", "USD/KRW", "NASDAQ", "HSI", KS_TOP_SYMBOL]
KS_INDEX_FIELDS = ["KS200", "USD/KRW", "NASDAQ", "HSI"]

INVESTOR_FIELDS = ["individual", "institutional", "pensionFund", "foreign"]

TOP_FIELDS = ["topSymbol", "topClose", "topChange"]
TOP_RENAME = {"symbol":"topSymbol", "close":"topClose", "change":"topChange"}

DAILY_KOSPI_FIELDS = PRICE_CHANGE_FIELDS + KS_INDEX_FIELDS + TOP_FIELDS + DRAWDOWN_FIELDS + ["volume"]

class DailyKospiPipeline(DailyPipeline):
    operation = "dailyKospi"
    fields = DAILY_KOSPI_FIELDS
    tzinfo = KST
    returnType = "dataframe"
    info = DAILY_KOSPI_INFO()
    dags = Dag(
        Task(operator=YahooPriceSpider, name="kospi", task="request_crawl", dataName="^KS11", dataType="dataframe",
            fields=tuple(PRICE_FIELDS), freq="1d", which="KOSPI Value"),
        Task(operator=YahooPriceSpider, name="kospi200", task="request_crawl", dataName="^KS200", dataType="dataframe",
            fields=("date","close"), freq="1d", which="KOSPI200 Value"),
        Task(operator=YahooPriceSpider, name="usdKrw", task="request_crawl", dataName="KRW=X", dataType="dataframe",
            fields=("date","close"), freq="1d", which="USD/KRW"),
        Task(operator=YahooPriceSpider, name="nasdaq", task="request_crawl", dataName="^IXIC", dataType="dataframe",
            fields=("date","close"), freq="1d", which="NASDAQ Value"),
        Task(operator=YahooPriceSpider, name="hsIndex", task="request_crawl", dataName="^HSI", dataType="dataframe",
            fields=("date","close"), freq="1d", which="HSI Value"),
        Task(operator=YahooPriceSpider, name="kospiTopPrice", task="request_crawl", dataName=KS_TOP_SYMBOL, dataType="dataframe",
            fields=("symbol","date","close"), freq="1d", which=f"{KS_TOP_SYMBOL} Price"),
    )

    @Pipeline.arrange_data
    def map_reduce(self, data: Dict[str,pd.DataFrame], maxPrice: Optional[Real]=None, trunc: Optional[int]=4, **context) -> Data:
        return (set_draw_down(set_change(data["^KS11"], trunc), maxPrice, trunc).
            merge(data["^KS200"].rename(columns={"close":"KS200"}), how="left", on="date").
            merge(data["KRW=X"].rename(columns={"close":"USD/KRW"}), how="left", on="date").
            merge(data["^IXIC"].rename(columns={"close":"NASDAQ"}), how="left", on="date").
            merge(data["^HSI"].rename(columns={"close":"HSI"}), how="left", on="date").
            merge(set_change(data[KS_TOP_SYMBOL], trunc).rename(columns=TOP_RENAME)[["date"]+TOP_FIELDS], how="left", on="date"))


KQ_TOP_SYMBOL = "086520.KQ"

KQ_INDEX_SYMBOLS = ["^KQ11", "^KQ100", "^KQ47", "^KQ26", "^KQ15", "KRW=X", "^IXIC", "^HSI", KQ_TOP_SYMBOL]
KQ_INDEX_FIELDS = ["KQ100", "KQ15", "KQ26", "KQ47", "USD/KRW", "NASDAQ", "HSI"]

DAILY_KOSDAQ_FIELDS = PRICE_CHANGE_FIELDS + KS_INDEX_FIELDS + TOP_FIELDS + DRAWDOWN_FIELDS + ["volume"]

class DailyKosdaqPipeline(DailyPipeline):
    operation = "dailyKosdaq"
    fields = DAILY_KOSDAQ_FIELDS
    tzinfo = KST
    returnType = "dataframe"
    info = DAILY_KOSDAQ_INFO()
    dags = Dag(
        Task(operator=YahooPriceSpider, name="kosdaq", task="request_crawl", dataName="^KQ11", dataType="dataframe",
            fields=tuple(PRICE_FIELDS), freq="1d", which="KOSDAQ Value"),
        Task(operator=YahooPriceSpider, name="kosdaq100", task="request_crawl", dataName="^KQ100", dataType="dataframe",
            fields=("date","close"), freq="1d", which="KOSDAQ100 Value"),
        Task(operator=YahooPriceSpider, name="kq47", task="request_crawl", dataName="^KQ47", dataType="dataframe",
            fields=("date","close"), freq="1d", which="KOSDAQ Semiconductors Value"),
        Task(operator=YahooPriceSpider, name="kq26", task="request_crawl", dataName="^KQ26", dataType="dataframe",
            fields=("date","close"), freq="1d", which="KOSDAQ Pharmaceuticals Value"),
        Task(operator=YahooPriceSpider, name="kq15", task="request_crawl", dataName="^KQ15", dataType="dataframe",
            fields=("date","close"), freq="1d", which="KOSDAQ Financials Value"),
        Task(operator=YahooPriceSpider, name="usdKrw", task="request_crawl", dataName="KRW=X", dataType="dataframe",
            fields=("date","close"), freq="1d", which="USD/KRW"),
        Task(operator=YahooPriceSpider, name="nasdaq", task="request_crawl", dataName="^IXIC", dataType="dataframe",
            fields=("date","close"), freq="1d", which="NASDAQ Value"),
        Task(operator=YahooPriceSpider, name="hsIndex", task="request_crawl", dataName="^HSI", dataType="dataframe",
            fields=("date","close"), freq="1d", which="HSI Value"),
        Task(operator=YahooPriceSpider, name="kosdaqTopPrice", task="request_crawl", dataName=KQ_TOP_SYMBOL, dataType="dataframe",
            fields=("symbol","date","close"), freq="1d", which=f"{KQ_TOP_SYMBOL} Price"),
    )

    @Pipeline.arrange_data
    def map_reduce(self, data: Dict[str,pd.DataFrame], maxPrice: Optional[Real]=None, trunc: Optional[int]=4, **context) -> Data:
        return (set_draw_down(set_change(data["^KQ11"], trunc), maxPrice, trunc).
            merge(data["^KQ100"].rename(columns={"close":"KQ100"}), how="left", on="date").
            merge(data["^KQ47"].rename(columns={"close":"KQ47"}), how="left", on="date").
            merge(data["^KQ26"].rename(columns={"close":"KQ26"}), how="left", on="date").
            merge(data["^KQ15"].rename(columns={"close":"KQ15"}), how="left", on="date").
            merge(data["KRW=X"].rename(columns={"close":"USD/KRW"}), how="left", on="date").
            merge(data["^IXIC"].rename(columns={"close":"NASDAQ"}), how="left", on="date").
            merge(data["^HSI"].rename(columns={"close":"HSI"}), how="left", on="date").
            merge(set_change(data[KQ_TOP_SYMBOL], trunc).rename(columns=TOP_RENAME)[["date"]+TOP_FIELDS], how="left", on="date"))
