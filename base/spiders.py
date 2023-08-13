from __future__ import annotations
from gscraper.base import Spider, AsyncSpider, REDIRECT_MSG
from gscraper.base import get_headers, log_messages, log_response, log_client, log_results
from gscraper.cast import cast_tuple
from gscraper.map import unique, chain_exists, is_2darray

from typing import Any, Dict, List, Tuple
from abc import ABCMeta, abstractmethod
from aiohttp import ClientSession
from requests import Session
import json


class FinanceSpider(Spider):
    __metaclass__ = ABCMeta
    operation = str()
    message = "example tqdm message"

    @Spider.requests_session
    def crawl(self, query: List[Any], start=1, pageSize=1, **kwargs) -> List[Dict]:
        query = unique(*query) if is_2darray(query) else list(map(cast_tuple, unique(*query)))
        return self.gather(query=query, start=start, pageSize=pageSize, **kwargs)

    @Spider.response_filter
    def gather(self, query: List[Tuple[Any]], start=1, pageSize=1, message=str(), **kwargs) -> List[Dict]:
        message = message if message else self.message
        iterable = [(*args, page) for args in query for page in range(start,start+pageSize)]
        return chain_exists(
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
    async def crawl(self, query: List[Any], startPage=1, pageSize=1, apiRedirect=False, **kwargs) -> List[Dict]:
        query = unique(*query) if is_2darray(query) or apiRedirect else list(map(cast_tuple, unique(*query)))
        context = dict(query=query, startPage=startPage, pageSize=pageSize, **kwargs)
        return await (self.redirect(**context) if apiRedirect else self.gather(**context))

    @AsyncSpider.asyncio_filter
    async def gather(self, query: List[Tuple[Any]], startPage=1, pageSize=1, message=str(), **kwargs) -> List[Dict]:
        message = message if message else self.message
        return chain_exists(await self.tqdm.gather(
            *[self.fetch(*args, page=page, **kwargs)
                for args in query for page in range(startPage, startPage+pageSize)], desc=message))

    @AsyncSpider.gcloud_authorized
    async def redirect(self, query: List[Tuple[Any]], message=str(), **kwargs) -> List[Dict]:
        message = message if message else REDIRECT_MSG(self.operation)
        return chain_exists(await self.tqdm.gather(
            *[self.fetch_redirect(query=args, **kwargs) for args in self.redirect_range(query)], desc=message))

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
