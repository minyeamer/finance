from gscraper.base import AsyncSpider
from gscraper.base import get_headers, log_messages, log_client

from base.spiders import FinanceAsyncSpider
from base.urls import API_URL, GET_URL
from parsers.naver import *

from typing import Dict
from aiohttp import ClientSession

NAVER = "naver"


class NaverReportSpider(FinanceAsyncSpider, NaverReportParser):
    operation = "naverReport"
    message = "Collecting naver reports"

    @AsyncSpider.asyncio_errors
    @AsyncSpider.asyncio_limit
    async def fetch(self, code: str, stockType: str, session: ClientSession=None, **kwargs) -> Dict:
        api_url = API_URL(NAVER, stockType, code)
        headers = get_headers(host=api_url, referer=GET_URL(NAVER, "main", code))
        headers["Upgrade-Insecure-Requests"] = '1'
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(api_url, headers=headers) as response:
            self.logger.info(await log_client(response, url=api_url, code=code, stockType=stockType))
            return self.parse(await response.text(), code, stockType, **kwargs)
