from gscraper.base import AsyncSpider, REDIRECT_MSG
from gscraper.base import get_headers, log_messages, log_client
from gscraper.map import unique, filter_exists, chain_exists

from base.spiders import FinanceAsyncSpider
from base.urls import API_URL, GET_URL
from parsers.naver import *

from typing import Dict, List, Tuple
from aiohttp import ClientSession

NAVER = "naver"


class NaverReportSpider(FinanceAsyncSpider, NaverReportParser):
    operation = "naverReport"
    message = "Collecting naver reports"

    @AsyncSpider.asyncio_session
    async def crawl(self, codes: List[Tuple[str,str]], apiRedirect=False, **kwargs) -> List[Dict]:
        context = dict(codes=unique(*codes), **kwargs)
        return await (self.redirect(**context) if apiRedirect else self.gather(**context))

    @AsyncSpider.asyncio_filter
    async def gather(self, codes: List[Tuple[str,str]], message=str(), **kwargs) -> List[Dict]:
        message = message if message else self.message
        return filter_exists(await self.tqdm.gather(
            *[self.fetch(code, stockType, **kwargs) for code, stockType in codes], desc=message))

    @AsyncSpider.gcloud_authorized
    async def redirect(self, codes: List[Tuple[str,str]], message=str(), **kwargs) -> List[Dict]:
        message = message if message else REDIRECT_MSG(self.operation)
        return chain_exists(await self.tqdm.gather(
            *[self.fetch_redirect(codes=info, **kwargs) for info in self.redirect_range(codes)], desc=message))

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
