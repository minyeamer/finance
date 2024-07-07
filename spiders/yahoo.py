from spiders import FinanceSpider, FinanceAsyncSpider, Flow, EST, get_headers
from spiders import GET, API, YAHOO, URL, Symbol

from data.yahoo import YAHOO_TICKER_INFO, YAHOO_TICKER_SECTION
from data.yahoo import YAHOO_QUERY_INFO, YAHOO_QUERY_FIELDS, YAHOO_SUMMARY_INFO
from data.yahoo import get_yahoo_params, get_yahoo_cookies

from data import US_STOCK_PRICE_SCHEMA
from data.yahoo import YAHOO_PRICE_INFO, YAHOO_DATE_LIMIT

from gscraper.base.types import IndexLabel, Keyword, DateFormat, Records, Data
from gscraper.utils.map import inter, endswith

from typing import Dict, Optional, Tuple, Union
from abc import ABCMeta
import datetime as dt
import pandas as pd
import yfinance


def fmt(symbol: str) -> str:
    if isinstance(symbol, str):
        if endswith(symbol, [".KS", ".KQ"]): return symbol
        else: return symbol.replace('.','-')
    else: return str()


def get_yahoo_headers(url: str, symbol=str(), referer=str(), **kwargs) -> Dict[str,str]:
    referer = referer if referer else URL(GET, YAHOO, "main", fmt(symbol))
    return get_headers(url, referer=referer, origin=True, cookies=get_yahoo_cookies(), **kwargs)


class YahooSpider(FinanceSpider):
    __metaclass__ = ABCMeta
    operation = "yahooSpider"
    host = YAHOO
    where = "Yahoo Finance"
    tzinfo = EST


class YahooAsyncSpider(FinanceAsyncSpider):
    __metaclass__ = ABCMeta
    operation = "yahooSpider"
    host = YAHOO
    where = "Yahoo Finance"
    tzinfo = EST


###################################################################
######################## Yahoo Information ########################
###################################################################

class YahooTickerSpider(YahooSpider):
    operation = "yahooTicker"
    which = "stock information"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    info = YAHOO_TICKER_INFO()
    flow = Flow()

    @YahooSpider.init_task
    def crawl(self, symbol: Symbol, section: Keyword=["summary"], prepost=False, trunc: Optional[int]=2, **context) -> Data:
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    @YahooSpider.retry_request
    @YahooSpider.limit_request
    def fetch(self, symbol: str, section: Keyword=["summary"], prepost=False, **context) -> Dict:
        response = yfinance.Ticker(fmt(symbol))
        return self.parse(response.info, symbol=symbol, section=section, prepost=prepost, **context)

    def get_flow(self, section: Keyword=["summary"], prepost=False, **context) -> Flow:
        return super().get_flow(Flow(*self.validate_section(section, prepost)))

    def validate_section(self, section: Keyword=["summary"], prepost=False) -> Keyword:
        section = inter(YAHOO_TICKER_SECTION, section) if section else YAHOO_TICKER_SECTION
        if section[0] == "summary":
            section.insert(1, ("prepost" if prepost else "regular"))
        return section


###################################################################
########################### Yahoo Query ###########################
###################################################################

class YahooQuerySpider(YahooAsyncSpider):
    operation = "yahooQuery"
    which = "stock names"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    root = ["quoteResponse","result",0]
    info = YAHOO_QUERY_INFO()
    flow = Flow("info")

    @YahooAsyncSpider.retry_request
    @YahooAsyncSpider.limit_request
    async def fetch(self, symbol: str, **context) -> Dict:
        url = URL(API, YAHOO, "query", fmt(symbol))
        params = get_yahoo_params(symbols=fmt(symbol), fields=','.join(YAHOO_QUERY_FIELDS))
        headers = get_yahoo_headers(url, symbol)
        response = await self.request_json(GET, url, params=params, encode=True, headers=headers, **context)
        return self.parse(response, symbol=symbol, **context)


class YahooSummarySpider(YahooAsyncSpider):
    operation = "yahooSummary"
    which = "company profiles"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    root = ["quoteSummary","result",0]
    info = YAHOO_SUMMARY_INFO()
    flow = Flow("summary")

    @YahooAsyncSpider.retry_request
    @YahooAsyncSpider.limit_request
    async def fetch(self, symbol: str, **context) -> Dict:
        url = URL(API, YAHOO, "summary", fmt(symbol))
        params = get_yahoo_params(modules="assetProfile,secFilings")
        headers = get_yahoo_headers(url, symbol)
        response = await self.request_json(GET, url, params=params, encode=True, headers=headers, **context)
        return self.parse(response, symbol=symbol, **context)


###################################################################
########################### Yahoo Price ###########################
###################################################################

class YahooPriceSpider(YahooSpider):
    operation = "yahooPrice"
    which = "stock prices"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    responseType = "records"
    returnType = "records"
    info = YAHOO_PRICE_INFO()
    flow = Flow("price")

    @YahooSpider.init_task
    def crawl(self, symbol: Symbol, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
            period: Optional[str]=None, freq: Union[str,int]="1d", prepost=False, trunc: Optional[int]=2, **context) -> Data:
        if period is None:
            startDate, endDate, interval = self.set_date(startDate, endDate, freq)
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    def set_date(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                period: Optional[str]=None, freq: Union[str,int]="1d") -> Tuple[dt.date,dt.date,str]:
        startDate, endDate = self.get_date_pair(startDate, endDate)
        today, limit = self.today(), YAHOO_DATE_LIMIT[freq]
        hasEnd, endDate = bool(endDate), (endDate if endDate else today)
        if freq != "1m": return startDate, endDate, str()
        elif not startDate: return (endDate-dt.timedelta(limit-1)), endDate, "7D"
        elif (today-startDate).days < limit: return startDate, endDate, "7D"
        elif hasEnd: return (endDate-dt.timedelta(limit-1)), endDate, "7D"
        else: return startDate, (startDate+dt.timedelta(limit-1)), "7D"

    @YahooSpider.retry_request
    @YahooSpider.limit_request
    def fetch(self, symbol: str, startDate: Optional[dt.date]=None, endDate: Optional[dt.date]=None,
            period: Optional[str]=None, freq: Union[str,int]="1d", prepost=False, **context) -> Records:
        response = yfinance.download(fmt(symbol), startDate, endDate, period=period, interval=freq, prepost=prepost, progress=False)
        return self.parse(response, symbol=symbol, **context)

    @YahooSpider.validate_response
    def parse(self, response: pd.DataFrame, **context) -> Records:
        return self.map(response[::-1].reset_index().to_dict("records"), **context)[::-1]

    def is_valid_response(self, response: pd.DataFrame) -> bool:
        if response.empty: raise ValueError("Failed download")
        else: return True

    def get_upload_columns(self, interval="1d", name=str(), **context) -> IndexLabel:
        if name == "sheet":
            dateType = "datetime" if YAHOO_DATE_LIMIT.get(interval) else "date"
            return US_STOCK_PRICE_SCHEMA(dateType).get("name")
        elif name == "ohlc": return ["symbol", "open", "high", "low", "close", "date"]
        elif name == "close": return ["date", "symbol", "close"]
        else: return list()

    def map_upload_data(self, data: pd.DataFrame, name=str(), **context) -> pd.DataFrame:
        if name == "ohlc":
            return data[data["date"]==data["date"].max()]
        else: return data
