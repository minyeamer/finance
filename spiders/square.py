from spiders import FinanceKrAsyncSpider, Flow, KST, get_headers
from spiders import GET, API, SQUARE, URL, Code

from data import KR_STOCK_PRICE_SCHEMA
from data.square import SQUARE_DETAIL_INFO
from data.square import SQUARE_PRICE_PARAMS, SQUARE_PRICE_INFO, SQUARE_PRICE_FIELDS

from gscraper.base.types import IndexLabel, Id, DateFormat, Records, Data, JsonData
from gscraper.utils.cast import cast_timestamp
from gscraper.utils.map import between

from typing import Dict, Optional, Union
from abc import ABCMeta
import re


class SquareAsyncSpider(FinanceKrAsyncSpider):
    __metaclass__ = ABCMeta
    operation = "squareSpider"
    host = SQUARE
    where = "Alpha Square"
    tzinfo = KST
    token = "eiVlUSkhrkIYaJ11nVFqHmDdHK8m1Hcy7p28i2dOcLaISw5fFVtyDP2GMWICQGDn"


###################################################################
####################### Alpha Square Detail #######################
###################################################################

class SquareDetailSpider(SquareAsyncSpider):
    operation = "squareDetail"
    which = "stock details"
    iterateArgs = ["code"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    root = ["code"]
    info = SQUARE_DETAIL_INFO()
    flow = Flow("detail")

    @SquareAsyncSpider.retry_request
    @SquareAsyncSpider.limit_request
    async def fetch(self, code: str, **context) -> Dict:
        url = URL(API, SQUARE, "details", code)
        headers = get_headers(url, referer=URL(GET, SQUARE, "main"), origin=True)
        headers["X-Csrftoken"] = self.token
        response = await self.request_json(GET, url, headers=headers, **context)
        return self.parse(response, code=code, **context)


###################################################################
######################## Alpha Square Price #######################
###################################################################

class SquarePriceSpider(SquareAsyncSpider):
    operation = "squarePrice"
    which = "stock prices"
    iterateArgs = ["id", "code"]
    iterateUnit = 1
    responseType = "records"
    returnType = "records"
    dateType = "date"
    info = SQUARE_PRICE_INFO()
    flow = Flow()

    @SquareAsyncSpider.init_session
    async def crawl(self, id: Id, code: Code=list(), freq: Union[str,int]="day", limit=600,
                    startTime: Optional[DateFormat]=None, endTime: Optional[DateFormat]=None,
                    trunc: Optional[int]=2, **context) -> Data:
        self.dateType = "datetime" if isinstance(freq, int) or ("minute" in str(freq)) else "date"
        startTime, endTime = cast_timestamp(startTime), cast_timestamp(endTime)
        args, context = self.validate_params(locals())
        return await self.gather(*args, **context)

    @SquareAsyncSpider.retry_request
    @SquareAsyncSpider.limit_request
    async def fetch(self, id: str, code=str(), freq: Union[str,int]="day", limit=600,
                    startTime: Optional[int]=None, endTime: Optional[int]=None, trunc=2, **context) -> Records:
        url = URL(API, SQUARE, "prices", id)
        params = SQUARE_PRICE_PARAMS(freq, limit)
        headers = get_headers(host=url, referer=URL(GET, SQUARE, "main"), origin=True)
        headers["X-Csrftoken"] = self.token
        response = await self.request_json(GET, url, params=params, headers=headers, **context)
        return self.parse(response, locals=locals())

    @SquareAsyncSpider.validate_response
    def parse(self, response: JsonData, startTime: Optional[int]=None, endTime: Optional[int]=None, **context) -> Records:
        data = [dict(zip(SQUARE_PRICE_FIELDS, row)) for row in response["data"]
                if isinstance(row[0], (float,int)) and between(row[0], startTime, endTime)]
        return self.map(data, **context)

    def get_flow(self, code=str(), **context) -> Flow:
        is_kr_price = (not code) or re.match("\d{6}", str(code))
        return super().get_flow(Flow("id", ("kr" if is_kr_price else "us")))

    def get_upload_columns(self, name=str(), **context) -> IndexLabel:
        return KR_STOCK_PRICE_SCHEMA(self.dateType).get("name")
