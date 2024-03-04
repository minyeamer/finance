from spiders import FinanceSpider, FinanceAsyncSpider, Flow, EST, get_headers
from spiders import GET, API, YAHOO, URL, Symbol

from data.yahoo import YAHOO_TICKER_INFO, YAHOO_TICKER_SECTION
from data.yahoo import YAHOO_QUERY_INFO, YAHOO_QUERY_FIELDS, YAHOO_SUMMARY_INFO
from data.yahoo import get_yahoo_params, get_yahoo_cookies

from data import US_STOCK_PRICE_SCHEMA
from data.yahoo import YAHOO_PRICE_INFO, YAHOO_DATE_LIMIT

from gscraper.base.types import IndexLabel, Keyword, DateFormat, Timezone, Data
from gscraper.utils.date import get_timezone
from gscraper.utils.map import inter
from base.spider import set_change

from typing import Dict, Optional, Tuple
from abc import ABCMeta
import datetime as dt
import pandas as pd
import yfinance


def fmt(symbol: str) -> str:
    return symbol.replace('.','-') if isinstance(symbol, str) else str()


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
        self.flow = Flow(*self.map_section(section, prepost))
        args, context = self.validate_params(symbol=symbol, trunc=trunc, **context)
        return self.gather(*args, **context)

    @YahooSpider.catch_exception
    @YahooSpider.limit_request
    def fetch(self, symbol: str, **context) -> Dict:
        response = yfinance.Ticker(fmt(symbol))
        return self.parse(response.info, symbol=symbol, **context)

    def map_section(self, section: Keyword=["summary"], prepost=False) -> Keyword:
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

    @YahooAsyncSpider.catch_exception
    @YahooAsyncSpider.limit_request
    async def fetch(self, symbol: str, **context) -> Dict:
        url = URL(API, YAHOO, "query", fmt(symbol))
        params = get_yahoo_params(symbols=fmt(symbol), fields=','.join(YAHOO_QUERY_FIELDS))
        headers = get_yahoo_headers(url, symbol)
        response = await self.request_json(GET, encode=True, **self.local_request(locals()))
        return self.parse(**self.local_response(locals()))


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

    @YahooAsyncSpider.catch_exception
    @YahooAsyncSpider.limit_request
    async def fetch(self, symbol: str, **context) -> Dict:
        url = URL(API, YAHOO, "summary", fmt(symbol))
        params = get_yahoo_params(modules="assetProfile,secFilings")
        headers = get_yahoo_headers(url, symbol)
        response = await self.request_json(GET, encode=True, **self.local_request(locals()))
        return self.parse(**self.local_response(locals()))


###################################################################
########################### Yahoo Price ###########################
###################################################################

class YahooPriceSpider(YahooSpider):
    operation = "yahooPrice"
    which = "stock prices"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    responseType = "dataframe"
    returnType = "dataframe"
    info = YAHOO_PRICE_INFO()
    flow = Flow("price")

    @YahooSpider.init_task
    def crawl(self, symbol: Symbol, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                freq="1d", prepost=False, trunc: Optional[int]=2, **context) -> Data:
        startDate, endDate, interval = self.set_date(startDate, endDate, freq)
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    def set_date(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                freq="1d") -> Tuple[dt.date,dt.date,str]:
        startDate, endDate = self.get_date_pair(startDate, endDate)
        today, limit = self.today(), YAHOO_DATE_LIMIT[freq]
        interval = "7D" if freq == "1m" else str()
        if not limit: return startDate, endDate, interval
        hasEnd, endDate = bool(endDate), (endDate if endDate else today)
        if not startDate: return (endDate-dt.timedelta(limit-1)), endDate, interval
        elif (today-startDate).days < limit: return startDate, endDate, interval
        elif hasEnd: return (endDate-dt.timedelta(limit-1)), endDate, interval
        else: return startDate, (startDate+dt.timedelta(limit-1))

    @YahooSpider.catch_exception
    @YahooSpider.limit_request
    def fetch(self, symbol: str, startDate: Optional[dt.date]=None, endDate: Optional[dt.date]=None,
                freq="1d", prepost=False, **context) -> pd.DataFrame:
        response = yfinance.download(symbol, startDate, endDate, interval=freq, prepost=prepost, progress=False)
        return self.parse(response, symbol=symbol, **context)

    @YahooSpider.validate_response
    def parse(self, response: pd.DataFrame, tzinfo: Optional[Timezone]=None, trunc=2, **context) -> pd.DataFrame:
        data = response.reset_index()
        data.columns = [column.lower() for column in data.columns]
        data = self.map_date(data, tzinfo)
        data = set_change(data, (trunc+2 if isinstance(trunc, int) else 4))
        return self.map(data, tzinfo=tzinfo, trunc=trunc, **context)

    def map_date(self, data: pd.DataFrame, tzinfo: Optional[Timezone]=None) -> pd.DataFrame:
        data["date"] = data[("date" if "date" in data else "datetime")].apply(lambda x: x.date())
        if "datetime" in data:
            tzinfo = get_timezone(tzinfo)
            if tzinfo: data["datetime"] = data["datetime"].apply(lambda x: x.astimezone(tzinfo))
            else: data["datetime"] = data["datetime"].apply(lambda x: x.replace(tzinfo=None))
        return data

    def get_upload_columns(self, interval="1d", name=str(), **context) -> IndexLabel:
        dateType = "time" if YAHOO_DATE_LIMIT.get(interval) else "date"
        return US_STOCK_PRICE_SCHEMA(dateType).get("name")
