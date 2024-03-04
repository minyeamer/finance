from spiders import FinanceKrAsyncSpider, Flow, KST, get_headers
from spiders import GET, API, NAVER, URL, Code

from data.naver import NAVER_STOCK_INFO_INFO
from data.naver import NAVER_INVESTOR_INFO, INDEX_CATEGORY, INVESTOR_COLUMNS

from gscraper.base.types import Context, DateFormat, Records, Data
from gscraper.utils.map import re_get, select_text, fill_array

from typing import Dict, Literal, Optional, Sequence
from abc import ABCMeta
from math import ceil
import numpy as np

from bs4 import BeautifulSoup
from io import StringIO
import json
import pandas as pd


class NaverAsyncSpider(FinanceKrAsyncSpider):
    __metaclass__ = ABCMeta
    operation = "naverSpider"
    host = NAVER
    where = "Naver Finance"
    maxLimit = 3
    tzinfo = KST


###################################################################
########################### Naver Stock ###########################
###################################################################

class NaverStockInfoSpider(NaverAsyncSpider):
    operation = "naverReport"
    which = "company info"
    iterateArgs = ["code", "stockType"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    info = NAVER_STOCK_INFO_INFO()
    flow = Flow()

    @NaverAsyncSpider.init_session
    async def crawl(self, code: Code, stockType: Sequence[Literal["company","etf"]], **context) -> Data:
        return await self.gather(*self.validate_args(code, stockType, how="first"), **context)

    @NaverAsyncSpider.catch_exception
    @NaverAsyncSpider.limit_request
    async def fetch(self, code: str, stockType: Literal["company","etf"], **context) -> Records:
        url = URL(API, NAVER, stockType, code)
        headers = get_headers(host=url, referer=URL(GET, NAVER, "main", code), secure=True)
        response = await self.request_text(GET, **self.local_request(locals()))
        return self.parse(**self.local_response(locals()))

    @NaverAsyncSpider.validate_response
    def parse(self, response: str, code: str, stockType: Literal["company","etf"], **context) -> Dict:
        if stockType == "company":
            source = BeautifulSoup(response, "html.parser")
            values = select_text(source, "td.cmp-table-cell > dl > dt.line-left")
            data = dict(zip(["name","sector","industry","bps","per","sectorPer","pbr","dividentYield"], fill_array(values, 8)))
        else: data = json.loads(re_get("var summary_data = (\{[^}]*\});", response))
        return self.map(data, code=code, stockType=stockType, **context)

    def get_flow(self, stockType: Literal["company","etf"], **context) -> Flow:
        return super().get_flow(Flow(stockType))


###################################################################
########################## Naver Investor #########################
###################################################################

class NaverInvestorSpider(NaverAsyncSpider):
    operation = "naverInvestor"
    which = "investor deal trands"
    iterateArgs = []
    iterateUnit = 1
    pagination = True
    pageLimit = 10
    responseType = "dataframe"
    returnType = "dataframe"
    info = NAVER_INVESTOR_INFO()
    flow = Flow("investor")

    @NaverAsyncSpider.init_session
    async def crawl(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                    indexType: Literal["KOSPI","KOSDAQ","FUTURES"]="KOSPI", size=None, pageStart=1, **context) -> Data:
        context = self.validate_context(startDate, endDate, size, indexType=indexType, pageStart=pageStart, **context)
        return await self.gather(**context)

    def validate_context(self, startDate=None, endDate=None, size=None, **context) -> Context:
        byDate = "date" if (startDate is not None) or (endDate is not None) else None
        startDate, endDate = self.get_date_pair(startDate, endDate, if_null=(None,0), busdate=True)
        dateFilter = dict(context, byDate=byDate, fromDate=startDate, toDate=endDate) if byDate else dict()
        if not isinstance(size, (Sequence,int)):
            size = (ceil(np.busday_count(startDate, endDate)/self.pageLimit) if startDate else 1) * self.pageLimit
        return dict(context, bizdate=endDate.strftime("%Y%m%d"), size=size, pageSize=self.pageLimit, **dateFilter)

    @NaverAsyncSpider.catch_exception
    @NaverAsyncSpider.limit_request
    async def fetch(self, bizdate: str, indexType="KOSPI", page=1, **context) -> pd.DataFrame:
        url = URL(GET, NAVER, "investor")
        params = dict(bizdate=bizdate, sosok=INDEX_CATEGORY.get(indexType,"01"), page=page)
        headers = get_headers(host=url, referer=url, secure=True)
        response = await self.request_text(GET, **self.local_request(locals()))
        return self.parse(**self.local_response(locals()))

    @NaverAsyncSpider.validate_response
    def parse(self, response: str, indexType=str(), **context) -> pd.DataFrame:
        data = pd.read_html(StringIO(response))[0]
        data.columns = ["date"]+INVESTOR_COLUMNS
        return self.map(data[data['date'].notna()], indexType=indexType, **context)
