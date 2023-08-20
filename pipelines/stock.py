from gscraper.base import Pipeline
from gscraper.date import get_date

from base.parsers import calc_change
from spiders.yahoo import YahooPriceSpider

from typing import Dict
from pandas.tseries.offsets import BDay
import datetime as dt
import pandas as pd
import yfinance

YAHOO = "yahoo"

NASDAQ_SYMBOLS = ["^IXIC","^VIX","DX-Y.NYB","^IRX","^TNX","CL=F","BTC-USD"]
BASE_COLUMNS = ["date","open","gap","high","highPct","low","lowPct","close","previousClose","change"]
PREPOST_COLUMNS = ["preHigh","preLow","preChange","postHigh","postLow","postChange"]
INDEX_COLUMNS = ["VIX","USDX","IRX","TNX","CL=F","BTC-USD"]
NASDAQ_COLUMNS = BASE_COLUMNS+PREPOST_COLUMNS+["maxPrice","drawDown"]+INDEX_COLUMNS+["volume"]


class DailyNasdaqPipeline(Pipeline, YahooPriceSpider):
    operation = "dailyNasdaq"

    @Pipeline.requests_task
    def crawl(self, startDate=None, endDate=None, maxPrice=None, **kwargs) -> pd.DataFrame:
        startDate, endDate = (get_date(startDate,1)-BDay(1)).date(), get_date(endDate,0)
        return self.gather(startDate, endDate, maxPrice, **kwargs)

    def gather(self, startDate: dt.date, endDate: dt.date, maxPrice=None,
                interval="1d", prepost=False, **kwargs) -> pd.DataFrame:
        results = dict()
        for symbol in NASDAQ_SYMBOLS:
            results[symbol] = self.fetch(symbol, startDate, endDate, interval="1d", prepost=False, **kwargs)
        results["QQQ"] = self.fetch("QQQ", startDate, endDate, interval="1h", prepost=True, **kwargs)
        return self.map_reduce(results, maxPrice=maxPrice)

    def fetch(self, symbol: str, startDate: dt.date, endDate: dt.date,
                interval="1d", prepost=False, trunc=2, tzinfo=None, **kwargs) -> pd.DataFrame:
        startDate, endDate = self.set_date(startDate, endDate, interval)
        response = yfinance.download(symbol, startDate, endDate, interval=interval, prepost=prepost, progress=False)
        data = self.parse(response, symbol, trunc=trunc, tzinfo=tzinfo)
        return data[(data["date"]>=(startDate+BDay(1)).date())] if "date" in data else pd.DataFrame()

    def map_reduce(self, data: Dict[str,pd.DataFrame], maxPrice=None, **kwargs) -> pd.DataFrame:
        results = (data["^IXIC"][BASE_COLUMNS].
            merge(self.calc_prepost(data["QQQ"]), how="left", on="date").
            merge(data["^VIX"][["date","close"]].rename(columns={"close":"VIX"}), how="left", on="date").
            merge(data["DX-Y.NYB"][["date","close"]].rename(columns={"close":"USDX"}), how="left", on="date").
            merge(data["^IRX"][["date","close"]].rename(columns={"close":"IRX"}), how="left", on="date").
            merge(data["^TNX"][["date","close"]].rename(columns={"close":"TNX"}), how="left", on="date").
            merge(data["CL=F"][["date","close"]].rename(columns={"close":"CL=F"}), how="left", on="date").
            merge(data["BTC-USD"][["date","change"]].rename(columns={"change":"BTC-USD"}), how="left", on="date").
            merge(data["^IXIC"][["date","volume"]], how="left", on="date"))
        results["preChange"] = results.apply(lambda row: self.minmax_prepost(row, prefix="pre"), axis=1)
        results["postChange"] = results.apply(lambda row: self.minmax_prepost(row, prefix="post"), axis=1)
        return self.calc_draw_down(results, maxPrice)[NASDAQ_COLUMNS]

    def calc_prepost(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        prepost = dict()
        prepost["pre"] = data[(data["volume"]==0)&data["datetime"].apply(lambda x: x.time()<=dt.time(9,30))]
        prepost["post"] = data[(data["volume"]==0)&data["datetime"].apply(lambda x: x.time()>=dt.time(16))]
        rename = lambda name: {
            price+pct: name+price.capitalize()+pct for price in ["high","low"] for pct in [str(),"Pct"]}
        pre, post = [df.groupby("date").agg({"high":"max","highPct":"max","low":"min","lowPct":"min"}).
                        reset_index().rename(columns=rename(name)) for name, df in prepost.items()]
        return pre.merge(post, how="outer", on="date")

    def minmax_prepost(self, row: pd.Series, prefix: str, **kwargs) -> pd.DataFrame:
        high_lt_low = abs(row["open"]-row[f"{prefix}High"]) > abs(row["open"]-row[f"{prefix}Low"])
        return row[f"{prefix}HighPct"] if high_lt_low else row[f"{prefix}LowPct"]

    def calc_draw_down(self, data: pd.DataFrame, maxPrice=None, **kwargs) -> pd.DataFrame:
        if isinstance(maxPrice, (float,int)):
            data = pd.concat([pd.DataFrame([{"high":maxPrice}], columns=BASE_COLUMNS), data])
        else: data = data.copy()
        data["maxPrice"] = data["high"].cummax()
        if isinstance(maxPrice, (float,int)): data = data.iloc[1:].copy()
        data["drawDown"] = calc_change(data["maxPrice"], data["close"])
        return data
