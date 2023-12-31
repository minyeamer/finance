from gscraper.base import get_headers, log_messages, log_client

from base.spiders import FinanceAsyncSpider
from base.urls import API_URL, GET_URL
from data.models import KR_STOCK_PRICE_SCHEMA
from parsers.square import *

from typing import Dict, List, Optional, Union
from aiohttp import ClientSession

SQUARE = "square"

MAX_LIMIT = 1000
TOKEN = "eiVlUSkhrkIYaJ11nVFqHmDdHK8m1Hcy7p28i2dOcLaISw5fFVtyDP2GMWICQGDn"


class SquareDetailSpider(FinanceAsyncSpider, SquareDetailParser):
    operation = "squareDetail"
    message = "Collecting stock details from Alpha Square"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, code: str, session: ClientSession=None, **kwargs) -> Dict:
        api_url = API_URL(SQUARE, "details", code)
        headers = get_headers(api_url, referer=GET_URL(SQUARE, "main"), origin=True)
        headers["X-Csrftoken"] = TOKEN
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(api_url, headers=headers) as response:
            self.logger.info(await log_client(response, url=api_url, code=code))
            return self.parse(await response.text(), code, **kwargs)


class SquarePriceSpider(FinanceAsyncSpider, SquarePriceParser):
    operation = "squarePrice"
    message = "Collecting stock prices from Alpha Square"

    @FinanceAsyncSpider.asyncio_errors
    @FinanceAsyncSpider.asyncio_limit
    async def fetch(self, id: str, code: str, freq: Optional[Union[str,int]]="day", limit=600,
                    startDate=None, endDate=None, trunc=2, session: ClientSession=None, **kwargs) -> Dict:
        api_url = API_URL(SQUARE, "prices", id)
        freq = (f"minute-{freq}" if isinstance(freq, int) else freq)
        params = {"freq":freq, "limit":min(limit,MAX_LIMIT), "include_current_candle":"false"}
        headers = get_headers(host=api_url, referer=GET_URL(SQUARE, "main"), origin=True)
        headers["X-Csrftoken"] = TOKEN
        self.logger.debug(log_messages(params=params, headers=headers, json=self.logJson))
        async with session.get(api_url, params=params, headers=headers) as response:
            self.logger.info(await log_client(response, url=api_url, id=id, code=code))
            return self.parse(await response.text(), id, code, freq, startDate, endDate, trunc, **kwargs)

    def get_gbq_schema(self, freq="day", **kwargs) -> List[Dict[str, str]]:
        return KR_STOCK_PRICE_SCHEMA("time" if isinstance(freq, int) or "minute" in str(freq) else "date")
