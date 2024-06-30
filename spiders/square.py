from spiders import FinanceKrAsyncSpider, EncryptedSpider, Flow, KST, get_headers
from spiders import GET, POST, DELETE, API, SQUARE, URL, Code

from data import KR_STOCK_PRICE_SCHEMA
from data.square import SQUARE_DETAIL_INFO
from data.square import SQUARE_PRICE_PARAMS, SQUARE_PRICE_INFO, SQUARE_PRICE_FIELDS

from data.square import SQUARE_WATCHLIST_INFO, SQUARE_WATCHLIST_UPLOAD_INFO, SQUARE_WATCHLIST_DELETE_INFO

from gscraper.base.types import IndexLabel, Keyword, Id, DateFormat, Records, Data, JsonData
from gscraper.utils.cast import cast_timestamp
from gscraper.utils.map import chain_exists, between

from typing import Dict, List, Optional, Union
from abc import ABCMeta
import re


class SquareAsyncSpider(FinanceKrAsyncSpider):
    __metaclass__ = ABCMeta
    operation = "squareSpider"
    host = SQUARE
    where = "Alpha Square"
    tzinfo = KST
    token = "eiVlUSkhrkIYaJ11nVFqHmDdHK8m1Hcy7p28i2dOcLaISw5fFVtyDP2GMWICQGDn"

    def get_headers(self, url: str, referer=str(), origin=True, **kwargs) -> Dict[str,str]:
        referer = referer if referer else URL(GET, SQUARE, "main")
        return get_headers(host=url, referer=referer, origin=origin, **{"X-Csrftoken":self.token}, **kwargs)


class SquareEncSpider(EncryptedSpider):
    __metaclass__ = ABCMeta
    operation = "squareSpider"
    host = SQUARE
    where = "Alpha Square"
    tzinfo = KST
    token = "eiVlUSkhrkIYaJ11nVFqHmDdHK8m1Hcy7p28i2dOcLaISw5fFVtyDP2GMWICQGDn"

    def get_headers(self, url: str, referer=str(), origin=True, **kwargs) -> Dict[str,str]:
        referer = referer if referer else URL(GET, SQUARE, "main")
        return get_headers(host=url, referer=referer, origin=origin, **{"X-Csrftoken":self.token}, **kwargs)


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
    info = SQUARE_DETAIL_INFO()
    flow = Flow("detail")

    @SquareAsyncSpider.retry_request
    @SquareAsyncSpider.limit_request
    async def fetch(self, code: str, **context) -> Dict:
        url = URL(API, SQUARE, "details", code)
        response = await self.request_json(GET, url, headers=self.get_headers(url), **context)
        return self.parse(response[code], code=code, **context)


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
        response = await self.request_json(GET, url, params=params, headers=self.get_headers(url), **context)
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


###################################################################
###################### Alpha Square Watchlist #####################
###################################################################

class SquareWatchlistSpider(SquareEncSpider):
    operation = "squareWatchlist"
    responseType = "records"
    returnType = "records"
    root = ["data"]
    info = SQUARE_WATCHLIST_INFO()
    flow = Flow()

    @SquareEncSpider.init_session
    def crawl(self, cookies: str, id=str(), **context) -> Data:
        return self.fetch(cookies=cookies, id=id, **context)

    @SquareEncSpider.arrange_data
    @SquareEncSpider.retry_request
    def fetch(self, cookies: str, id=str(), **context) -> Records:
        url = URL(API, SQUARE, "watchlist", id)
        response = self.request_json(GET, url, headers=self.get_headers(url, cookies=cookies), **context)
        return self.parse(response, id=id, **context)

    @SquareEncSpider.validate_response
    def parse(self, response: JsonData, **context) -> Data:
        data = self.map(response, **context)
        data = sorted(data, key=(lambda x: x["order"]), reverse=True)
        return [dict(__row, order=__i) for __i, __row in enumerate(data)]

    def get_flow(self, id=str(), **context) -> Flow:
        return super().get_flow(Flow("item" if id else "watchlist"))


class SquareWatchlistUpload(SquareEncSpider):
    operation = "squareWatchlistUpload"
    iterateArgs = ["value", "stockType"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    info = SQUARE_WATCHLIST_UPLOAD_INFO()
    flow = Flow()

    @SquareEncSpider.init_session
    def crawl(self, value: Keyword, stockType: Keyword, watchlistId: str, cookies: str, **context) -> Data:
        args, context = self.validate_params(locals(), unique=False)
        return self.gather(*(__arg[::-1] for __arg in args), **context)

    def get_gather_message(self, watchlistId: str, **context) -> str:
        return f"Uploading items to watchlist '{watchlistId}' of Alpha Square"

    @SquareEncSpider.arrange_data
    def reduce(self, data: List[Data], **context) -> Data:
        return chain_exists(data)[::-1]

    @SquareEncSpider.retry_request
    @SquareEncSpider.limit_request
    def fetch(self, value: str, stockType: str, watchlistId: str, cookies: str, **context) -> Dict:
        url = URL(API, SQUARE, "upload", watchlistId, stockType=stockType)
        data = {"stock_id":int(value)} if stockType == "stock" else {"name":str(value)}
        status = self.request_status(POST, url, json=data, headers=self.get_headers(url, cookies=cookies), **context)
        return dict(value=value, stockType=stockType, status=status)


class SquareWatchlistDelete(SquareEncSpider):
    operation = "squareWatchlistDelete"
    iterateArgs = ["stockType", "id"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    info = SQUARE_WATCHLIST_DELETE_INFO()
    flow = Flow()

    @SquareEncSpider.init_session
    def crawl(self, id: Id, stockType: Keyword, watchlistId: str, cookies: str, **context) -> Data:
        args, context = self.validate_params(locals(), unique=False)
        return self.gather(*args, **context)

    def get_gather_message(self, watchlistId: str, **context) -> str:
        return f"Deleting items from watchlist '{watchlistId}' of Alpha Square"

    @SquareEncSpider.retry_request
    @SquareEncSpider.limit_request
    def fetch(self, id: str, stockType: str, watchlistId: str, cookies: str, **context) -> Dict:
        url = URL(API, SQUARE, "delete", watchlistId, stockType=stockType, id=id)
        response = self.request_json(DELETE, url, headers=self.get_headers(url, cookies=cookies), **context)
        return dict(id=id, stockType=stockType, message=response.get("message"))
