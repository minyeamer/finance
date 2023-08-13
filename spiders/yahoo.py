from gscraper.base import AsyncSpider, get_headers, log_messages, log_client
from gscraper.cast import cast_tuple
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


class YahooQuerySpider(FinanceAsyncSpider, YahooQueryParser):
    operation = "yahooQuery"
    message = "Collecting stock information from yahoo finance"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, session: ClientSession=None, **kwargs) -> Dict:
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
    async def crawl(self, query: List[str], start: dt.date=None, end: dt.date=None,
                    freq="1d", **kwargs) -> pd.DataFrame:
        start, end = self.set_date(start, end, freq)
        return await self.gather(unique(*query), start, end, freq, **kwargs)

    @AsyncSpider.asyncio_filter
    async def gather(self, query: List[str], start: dt.date=None, end: dt.date=None,
                    freq="1d", message=str(), **kwargs) -> pd.DataFrame:
        message = message if message else self.message
        return pd.concat(await self.tqdm.gather(
            *[self.fetch(symbol, __start, __to, freq, **kwargs)
                for symbol in query for __start, __to in self.set_period(start, end, freq)], desc=message))

    @AsyncSpider.asyncio_errors
    @AsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, start: dt.date=None, end: dt.date=None,
                    freq="1d", prepost=False, trunc=2, **kwargs) -> pd.DataFrame:
        response = yfinance.download(symbol, start, end, interval=freq, prepost=prepost, progress=False)
        return self.parse(response, symbol, trunc, **kwargs)

    def set_date(self, start=None, end=None, freq="1d", **kwargs) -> Tuple[dt.date,dt.date]:
        start, end, today = get_date(start, default=None), get_date(end, default=None), now().date()
        limit = DATE_LIMIT[freq]
        if not limit: return start, end
        is_end, end = bool(end), (end if end else today)
        if not start: return (end-dt.timedelta(limit)), end
        days = ((end if end else today)-start).days
        if days <= limit: return start, end
        elif is_end: return (end-dt.timedelta(limit)), end
        else: return start, (start+dt.timedelta(limit))

    def set_period(self, start=None, end=None, freq="1d", **kwargs) -> List[Tuple[dt.date,dt.date]]:
        if freq != "1m": return [(start, end)]
        starts = list(map(lambda x: x.date(), pd.date_range(start, end, freq="7D")))
        return [(x, (y-dt.timedelta(days=1)))
                for x, y in zip(starts, starts[1:]+[end+dt.timedelta(days=1)])]

    def get_gbq_schema(self, freq="day", **kwargs) -> List[Dict[str, str]]:
        return US_STOCK_PRICE_SCHEMA("time" if isinstance(freq, int) or "minute" in str(freq) else "date")
