from __future__ import annotations
from gscraper.base import Spider, AsyncSpider, REDIRECT_MSG
from gscraper.base import get_headers, log_messages, log_response, log_client, log_results
from gscraper.map import unique, filter_exists, chain_exists

from typing import Dict, List, Union
from abc import ABCMeta, abstractmethod
from aiohttp import ClientSession
from requests import Session
import json


class FinanceSpider(Spider):
    __metaclass__ = ABCMeta
    operation = str()
    message = "example tqdm message"

    @Spider.requests_session
    def crawl(self, symbols: List[str], start=1, pageSize=1, **kwargs) -> List[Dict]:
        return self.gather(symbols=unique(*symbols), start=start, pageSize=pageSize, **kwargs)

    @Spider.response_filter
    def gather(self, symbols: List[str], start=1, pageSize=1, message=str(), **kwargs) -> List[Dict]:
        message = message if message else self.message
        iterable = [(symbol,page) for symbol in symbols for page in range(start,start+pageSize)]
        return filter_exists(
            [self.fetch(symbol, page=page, **kwargs) for symbol, page in self.tqdm(iterable, desc=message)])

    @abstractmethod
    @Spider.log_errors
    @Spider.requests_limit
    def fetch(self, symbol: str, page=1, session: Session=None, **kwargs) -> Dict:
        url = f"https://example.com?query={symbol}"
        headers = get_headers(url)
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        with session.get(url, headers=headers) as response:
            self.logger.info(log_response(response, url=url, symbol=symbol))
            results = json.loads(response.text, symbol, page=page, **kwargs)
        self.logger.info(log_results(results, symbol=symbol))
        return results


class FinanceAsyncSpider(AsyncSpider):
    __metaclass__ = ABCMeta
    operation = str()
    message = "example tqdm message"

    @AsyncSpider.asyncio_session
    async def crawl(self, symbols: List[str], start=1, pageSize=1, apiRedirect=False, **kwargs) -> List[Dict]:
        context = dict(symbols=unique(*symbols), start=start, pageSize=pageSize, **kwargs)
        return await (self.redirect(**context) if apiRedirect else self.gather(**context))

    @AsyncSpider.asyncio_filter
    async def gather(self, symbols: List[str], start=1, pageSize=1, message=str(), **kwargs) -> List[Dict]:
        message = message if message else self.message
        return filter_exists(await self.tqdm.gather(
            *[self.fetch(symbol, page=page, **kwargs)
                for symbol in symbols for page in range(start,start+pageSize)], desc=message))

    @AsyncSpider.gcloud_authorized
    async def redirect(self, symbols: List[str], message=str(), **kwargs) -> List[Dict]:
        message = message if message else REDIRECT_MSG(self.operation)
        return chain_exists(await self.tqdm.gather(
            *[self.fetch_redirect(symbol=symbol, **kwargs) for symbol in self.redirect_range(symbols)], desc=message))

    @abstractmethod
    @AsyncSpider.asyncio_errors
    @AsyncSpider.asyncio_limit
    async def fetch(self, symbol: str, page=1, session: ClientSession=None, **kwargs) -> Dict:
        url = f"https://example.com?query={symbol}"
        headers = get_headers(url)
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(url, headers=headers) as response:
            self.logger.info(await log_client(response, url=url, symbol=symbol))
            results = json.loads(await response.text(), symbol, page=page, **kwargs)
        self.logger.info(log_results(results, symbol=symbol))
        return results
