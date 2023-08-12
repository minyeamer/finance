from gscraper.base import get_headers, log_messages, log_client

from base.spiders import FinanceAsyncSpider
from base.urls import API_URL, GET_URL
from data.yahoo import *
from parsers.yahoo import *

from typing import Dict
from aiohttp import ClientSession

YAHOO = "yahoo"


class YahooQuerySpider(FinanceAsyncSpider, YahooQueryParser):
    operation = "yahooQuery"
    message = "Collecting stock information"

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
    message = "Collecting company profiles"

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
