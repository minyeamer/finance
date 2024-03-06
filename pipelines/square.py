from pipelines import AsyncPipeline, Dag, Task

from spiders.square import SquarePriceSpider
from data.square import STOCK_PRICE_KR_INFO
from data import KR_STOCK_PRICE_SCHEMA

from gscraper.base.types import Context, IndexLabel, DateFormat, Records, MappedData

from typing import Dict, List, Optional


###################################################################
######################## Alpha Square Price #######################
###################################################################

PRICE_FIELDS = ["open", "high", "low", "close", "volume"]
DAILY_PRICE_FIELDS = ["code", "date"] + PRICE_FIELDS
HOURLY_PRICE_FIELDS = ["code", "datetime"] + PRICE_FIELDS

QUERY_TYPE = ["stock_1d", "stock_1h", "stock_1m", "etf_1d"]
QUERY_FREQ = {"stock_1d":"day", "stock_1h":60, "stock_1m":1, "etf_1d":"day"}

def add_id_params(queryMap: Dict[str,Dict[str,List[str]]], __type: str, id: str, code: str, **kwargs):
    queryMap[__type]["id"].append(id)
    queryMap[__type]["code"].append(code)

class StockPriceKrPipeline(AsyncPipeline):
    operation = "stockPriceKr"
    fields = {"stock_1d":DAILY_PRICE_FIELDS, "stock_1h":HOURLY_PRICE_FIELDS, "stock_1m":HOURLY_PRICE_FIELDS, "etf_1d":DAILY_PRICE_FIELDS}
    returnType = "dataframe"
    mappedReturn = True
    info = STOCK_PRICE_KR_INFO()
    dags = Dag(
        Task(operator=SquarePriceSpider, name="square1dPrice", task="crawl_price", dataName="stock_1d", dataType="records",
        key="stock_1d", by="by day"),
        Task(operator=SquarePriceSpider, name="square1hPrice", task="crawl_price", dataName="stock_1h", dataType="records",
        key="stock_1h", by="by hour"),
        Task(operator=SquarePriceSpider, name="square1mPrice", task="crawl_price", dataName="stock_1m", dataType="records",
        key="stock_1m", by="by minute"),
        Task(operator=SquarePriceSpider, name="squareEtfPrice", task="crawl_price", dataName="etf_1d", dataType="records",
        key="etf_1d", which="etf prices"),
    )

    @AsyncPipeline.init_task
    async def crawl(self, query: Records, limit=600, startTime: Optional[DateFormat]=None, endTime: Optional[DateFormat]=None,
                    trunc: Optional[int]=2, **context) -> MappedData:
        context = self.validate_context(**self.from_locals(locals()))
        return await self.gather(**context)

    def validate_context(self, query: Records, **context) -> Context:
        queryMap = {__type: dict(id=list(), code=list()) for __type in QUERY_TYPE}
        for __q in query:
            freq = __q.get("freq", "day")
            if __q.get("etf"): add_id_params(queryMap, "etf_1d", **__q)
            elif isinstance(freq, (float,int)):
                if freq <= 390: add_id_params(queryMap, "stock_1d", **__q)
                if freq <= 60: add_id_params(queryMap, "stock_1h", **__q)
                if freq <= 1: add_id_params(queryMap, "stock_1m", **__q)
            elif freq == "day": add_id_params(queryMap, "stock_1d", **__q)
            elif freq == "minute-60": add_id_params(queryMap, "stock_1h", **__q)
            elif freq == "minute-1": add_id_params(queryMap, "stock_1m", **__q)
            else: continue
        return dict(context, queryMap=queryMap)

    @AsyncPipeline.validate_data
    @AsyncPipeline.limit_request
    async def crawl_price(self, worker: SquarePriceSpider, queryMap: Dict[str,Dict[str,List[str]]], key: str, **params) -> Records:
        params.update(queryMap[key])
        if not (params.get("id") and params.get("code")): return list()
        else: return await worker.crawl(**params)

    @AsyncPipeline.arrange_data
    def map_reduce(self, stock_1d: Records=list(), stock_1h: Records=list(), stock_1m: Records=list(),
                    etf_1d: Records=list(), **context) -> MappedData:
        return dict(stock_1d=stock_1d, stock_1h=stock_1h, stock_1m=stock_1m, etf_1d=etf_1d)

    def get_upload_columns(self, name=str(), **context) -> IndexLabel:
        dateType = "date" if name.endswith("1d") else "datetime"
        return KR_STOCK_PRICE_SCHEMA(dateType).get("name")
