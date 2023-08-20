from gscraper.base import AsyncSpider, REDIRECT_MSG
from gscraper.base import get_headers, log_messages, log_client
from gscraper.date import get_date, get_busdate
from gscraper.map import chain_exists

from base.spiders import FinanceAsyncSpider
from base.urls import API_URL, GET_URL
from parsers.naver import *

from typing import Dict, Iterable, List, Tuple
from aiohttp import ClientSession
from math import ceil
import datetime as dt
import numpy as np

NAVER = "naver"

INDEX_TYPES = {"KOSPI":"01", "KOSDAQ":"02", "FUTURES":"03"}


class NaverReportSpider(FinanceAsyncSpider, NaverReportParser):
    operation = "naverReport"
    message = "Collecting naver finance reports"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, code: str, stockType: str, session: ClientSession=None, **kwargs) -> Dict:
        api_url = API_URL(NAVER, stockType, code)
        headers = get_headers(host=api_url, referer=GET_URL(NAVER, "main", code))
        headers["Upgrade-Insecure-Requests"] = '1'
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(api_url, headers=headers) as response:
            self.logger.info(await log_client(response, url=api_url, code=code, stockType=stockType))
            return self.parse(await response.text(), code, stockType, **kwargs)


class NaverInvestorSpider(AsyncSpider, NaverInvestorParser):
    operation = "naverInvestor"
    message = "Collecting investor deal trands from Naver Finance"

    @AsyncSpider.asyncio_session
    async def crawl(self, startDate=None, endDate=None, indexType="KOSPI", pages=None,
                    apiRedirect=False, **kwargs) -> List[Dict]:
        bizdate, pages = self.set_query(startDate, endDate, pages)
        context = dict(bizdate=bizdate, indexType=indexType, pages=pages, startDate=startDate, **kwargs)
        results = await (self.redirect(**context) if apiRedirect else self.gather(**context))
        return results

    @AsyncSpider.asyncio_filter
    async def gather(self, bizdate: str, indexType="KOSPI", pages=1, message=str(), **kwargs) -> List[Dict]:
        pages = range(1,pages+1) if isinstance(pages, int) else pages
        message = message if message else self.message
        return chain_exists(await self.tqdm.gather(
            *[self.fetch(bizdate, indexType, page=page, **kwargs) for page in pages], desc=message))

    @AsyncSpider.gcloud_authorized
    async def redirect(self, bizdate: str, pages=1, message=str(), **kwargs) -> List[Dict]:
        pages = range(1,pages+1) if isinstance(pages, int) else pages
        message = message if message else REDIRECT_MSG(self.operation)
        return chain_exists(await self.tqdm.gather(
            *[self.fetch_redirect(endDate=bizdate, pages=args, **kwargs)
                for args in self.redirect_range(pages)], desc=message))

    @AsyncSpider.asyncio_errors
    @AsyncSpider.asyncio_limit
    async def fetch(self, bizdate: str, indexType="KOSPI", page=1, startDate=None,
                    session: ClientSession=None, **kwargs) -> List[Dict]:
        get_url = GET_URL(NAVER, "investor")
        params = {"bizdate":bizdate, "sosok":INDEX_TYPES.get(indexType,"01"), "page":page}
        headers = get_headers(host=get_url, referer=get_url)
        headers["Upgrade-Insecure-Requests"] = '1'
        self.logger.debug(log_messages(params=params, headers=headers, json=self.logJson))
        async with session.get(get_url, params=params, headers=headers) as response:
            self.logger.info(await log_client(response,
                url=get_url, bizdate=bizdate, indexType=indexType, page=page))
            return self.parse(await response.text(), indexType, startDate, **kwargs)

    def set_query(self, startDate=None, endDate=None, pages=None, pageUnit=10, **kwargs) -> Tuple[str,Iterable[int]]:
        bizdate = get_busdate(endDate, default=0)
        if not pages:
            startDate = get_busdate(startDate, default=None)
            pages = ceil(np.busday_count(startDate, bizdate)/pageUnit) if startDate else 1
        bizdate = bizdate.strftime("%Y%m%d") if isinstance(bizdate, dt.date) else str(bizdate)
        return bizdate, (pages if isinstance(pages, Iterable) else range(1,pages+1))
