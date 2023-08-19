from gscraper.base import AsyncSpider, get_headers, log_messages, log_client
from gscraper.date import now, get_date
from gscraper.map import unique

from base.spiders import FinanceAsyncSpider
from base.urls import API_URL, GET_URL
from data.models import US_STOCK_PRICE_SCHEMA
from data.yahoo import *
from parsers.yahoo import *

from typing import Dict, List, Tuple
from aiohttp import ClientSession
import datetime as dt
import yfinance

YAHOO = "yahoo"

fmt = lambda symbol: str(symbol).replace('.','-')


class YahooTickerSpider(FinanceAsyncSpider, YahooTickerParser):
    operation = "yahooTicker"
    message = "Collecting stock information from yahoo finance"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, **kwargs) -> Dict:
        symbol = fmt(symbol)
        response = yfinance.Ticker(fmt(symbol))
        return self.parse(response.info, symbol, **kwargs)


class YahooQuerySpider(FinanceAsyncSpider, YahooQueryParser):
    operation = "yahooQuery"
    message = "Collecting stock names from yahoo finance"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, session: ClientSession=None, **kwargs) -> Dict:
        symbol = fmt(symbol)
        params = get_params(symbols=symbol, fields=','.join(QUERY_FIELDS))
        api_url = API_URL(YAHOO, "query", symbol)+'?'+params
        headers = get_headers(api_url, referer=GET_URL(YAHOO, "main", symbol), origin=True, cookies=get_cookies())
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(api_url, headers=headers) as response:
            self.logger.info(await log_client(response, url=api_url, symbol=symbol))
            return self.parse(await response.text(), symbol, **kwargs)


class YahooSummarySpider(FinanceAsyncSpider, YahooSummaryParser):
    operation = "yahooSummary"
    message = "Collecting company profiles from yahoo finance"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, session: ClientSession=None, **kwargs) -> Dict:
        symbol = fmt(symbol)
        params = get_params(modules="assetProfile,secFilings")
        api_url = API_URL(YAHOO, "summary", symbol)+'?'+params
        headers = get_headers(api_url, referer=GET_URL(YAHOO, "main", symbol), origin=True, cookies=get_cookies())
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(api_url, headers=headers) as response:
            self.logger.info(await log_client(response, url=api_url, symbol=symbol))
            return self.parse(await response.text(), symbol, **kwargs)


DATE_LIMIT = {"1m":30, "2m":60, "5m":60, "15m":60, "30m":60, "60m":730, "90m":60,
                "1h":730, "1d":None, "5d":None, "1wk":None, "1mo":None, "3mo":None}

class YahooPriceSpider(AsyncSpider, YahooPriceParser):
    operation = "yahooPrice"
    returnType = "dataframe"
    message = "Collecting yahoo stock prices"

    @AsyncSpider.asyncio_session
    async def crawl(self, query: List[str], startDate=None, endDate=None, interval="1d",
                    prepost=False, trunc=2, tzinfo="default", **kwargs) -> pd.DataFrame:
        start, end = self.set_date(start, end, interval)
        return await self.gather(list(map(fmt, unique(*query))), startDate, endDate,
                                interval, prepost, trunc, tzinfo, **kwargs)

    @AsyncSpider.asyncio_filter
    async def gather(self, query: List[str], startDate: dt.date=None, endDate: dt.date=None,
                    interval="1d", prepost=False, trunc=2, tzinfo="default", message=str(), **kwargs) -> pd.DataFrame:
        message = message if message else self.message
        return pd.concat(await self.tqdm.gather(
            *[self.fetch(symbol, start, end, interval, prepost, trunc, tzinfo, **kwargs)
                for symbol in query for start, end in self.set_period(startDate, endDate, interval)], desc=message))

    @AsyncSpider.asyncio_errors
    @AsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, startDate: dt.date=None, endDate: dt.date=None,
                    interval="1d", prepost=False, trunc=2, tzinfo="default", **kwargs) -> pd.DataFrame:
        response = yfinance.download(symbol, startDate, endDate, interval=interval, prepost=prepost, progress=False)
        return self.parse(response, symbol, trunc, tzinfo, **kwargs)

    def set_date(self, startDate=None, endDate=None, interval="1d", **kwargs) -> Tuple[dt.date,dt.date]:
        startDate, endDate, today = get_date(startDate, default=None), get_date(endDate, default=None), now().date()
        limit = DATE_LIMIT[interval]
        if not limit: return startDate, endDate
        is_end, endDate = bool(endDate), (endDate if endDate else today)
        if not startDate: return (endDate-dt.timedelta(limit-1)), endDate
        days = ((endDate if endDate else today)-startDate).days
        if days < limit: return startDate, endDate
        elif is_end: return (endDate-dt.timedelta(limit-1)), endDate
        else: return startDate, (startDate+dt.timedelta(limit-1))

    def set_period(self, startDate: dt.date=None, endDate: dt.date=None,
                    interval="1d", **kwargs) -> List[Tuple[dt.date,dt.date]]:
        if interval != "1m": return [(startDate, endDate)]
        dates = list(map(lambda x: x.date(), pd.date_range(startDate, endDate, interval="7D")))
        return [(x, (y-dt.timedelta(days=1)))
                for x, y in zip(dates, dates[1:]+[endDate+dt.timedelta(days=1)])]

    def get_gbq_schema(self, interval="1d", **kwargs) -> List[Dict[str, str]]:
        return US_STOCK_PRICE_SCHEMA("time" if DATE_LIMIT.get(interval) else "date")
