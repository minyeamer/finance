from pipelines import Pipeline, AsyncPipeline, Dag, Task

from spiders.square import SquarePriceSpider
from data.square import STOCK_PRICE_KR_INFO
from data import KR_STOCK_PRICE_SCHEMA

from spiders.square import SquareWatchlistSpider, SquareWatchlistUpload, SquareWatchlistDelete
from data.square import SQUARE_WATCHLIST_PLUS_INFO, SQUARE_WATCHLIST_CLEAR_INFO

from gscraper.base.types import _KT, _VT, Arguments, Context, IndexLabel, Keyword, DateFormat
from gscraper.base.types import Records, Data, MappedData
from gscraper.utils.map import kloc

from typing import Dict, List, Optional, Tuple, Union


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


###################################################################
###################### Alpha Square Watchlist #####################
###################################################################

class SquareWatchlistPipeline(Pipeline):
    operation = "squareWatchlistPlus"
    returnType = None
    mappedReturn = True
    info = SQUARE_WATCHLIST_PLUS_INFO()
    dags = Dag(
        Task(operator=SquareWatchlistSpider, task="request_crawl", dataName="watchlist", dataType="records", params=["cookies"]),
        Task(operator=SquareWatchlistSpider, task="crawl_items", dataName="items", dataType=None, derivData=["watchlist"],
            params=["cookies"], updateTime=False),
    )

    @Pipeline.init_task
    def crawl(self, cookies: str, key: _KT=str(), **context) -> Union[Data,MappedData]:
        return self.gather(cookies=cookies, key=key, **context)

    @Pipeline.validate_data
    @Pipeline.limit_request
    def crawl_items(self, worker: SquareWatchlistSpider, watchlist: Records, **params) -> Dict[str,Records]:
        return {__watchlist["id"]: worker.crawl(id=__watchlist["id"], **params) for __watchlist in watchlist}

    @Pipeline.arrange_data
    def map_reduce(self, watchlist: Records, items: Dict[str,Records], key: _KT=str(), **context) -> Union[Data,MappedData]:
        def make_key(__m: Dict, key: _KT) -> Union[_VT,Tuple[_VT]]:
            __values = kloc(__m, key, if_null="pass", values_only=True)
            return tuple(__values) if isinstance(__values, List) else __values
        data = [dict(__watchlist, items=items.get(__watchlist["id"], list())) for __watchlist in watchlist]
        return {make_key(__watchlist, key): __watchlist["items"] for __watchlist in data} if key else data


class SquareWatchlistBulkUpload(Pipeline):
    operation = "squareWatchlistBulkUpload"
    returnType = "records"
    mappedReturn = True
    info = SQUARE_WATCHLIST_CLEAR_INFO()
    dags = Dag(
        Task(operator=SquareWatchlistPipeline, task="request_crawl", dataName="watchlist", dataType=None,
            params=["cookies"], key=["name","id"]),
        Task(operator=SquareWatchlistUpload, task="upload_items", dataName="response", dataType=None,
            derivData=["watchlist"], params=["query","cookies"]),
    )

    @Pipeline.init_task
    def crawl(self, query: Dict[str,Keyword], cookies: str, **context) -> MappedData:
        return self.gather(**self.validate_context(locals()))

    @Pipeline.validate_data
    @Pipeline.limit_request
    def upload_items(self, worker: SquareWatchlistDelete, query: Dict[str,Keyword],
                    watchlist: Dict[Tuple[str,str],Records], **params) -> Dict[str,Records]:
        id, idmap = [x[0] for x in watchlist.keys()], dict(watchlist.keys())
        def validate_args(values: Keyword) -> Arguments:
            return values, [("stock" if str(value).isdigit() else "splitter") for value in values]
        return {idmap.get(key,key): worker.crawl(*validate_args(values), watchlistId=idmap.get(key,key), **params)
                for key, values in query.items() if key in id or key in idmap}

    @Pipeline.arrange_data
    def map_reduce(self, response: Dict[str,Records], **context) -> MappedData:
        return response


class SquareWatchlistClear(Pipeline):
    operation = "squareWatchlistClear"
    returnType = "records"
    mappedReturn = True
    info = SQUARE_WATCHLIST_CLEAR_INFO()
    dags = Dag(
        Task(operator=SquareWatchlistPipeline, task="request_crawl", dataName="watchlist", dataType=None,
            params=["cookies"], key=["id","name"]),
        Task(operator=SquareWatchlistDelete, task="clear_items", dataName="response", dataType=None,
            derivData=["watchlist"], params=["query","cookies"]),
    )

    @Pipeline.init_task
    def crawl(self, query: Keyword, cookies: str, **context) -> MappedData:
        return self.gather(**self.validate_context(locals()))

    @Pipeline.validate_data
    @Pipeline.limit_request
    def clear_items(self, worker: SquareWatchlistDelete, query: Keyword,
                    watchlist: Dict[Tuple[str,str],Records], **params) -> Dict[str,Records]:
        def validate_args(items: Records) -> Arguments:
            id = [(item["id"] if item["stockType"] == "stock" else item["itemId"]) for item in items]
            return id, [item["stockType"] for item in items]
        return {id: worker.crawl(*validate_args(items), watchlistId=id, **params)
                for (id, name), items in watchlist.items() if (id in query) or (name in query)}

    @Pipeline.arrange_data
    def map_reduce(self, response: Dict[str,Records], **context) -> MappedData:
        return response
