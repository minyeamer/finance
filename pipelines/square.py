from spiders.square import SquarePriceSpider

from typing import Dict, List, Optional, Tuple, Union
import pandas as pd

SQUARE = "square"


price_message = lambda __type, __freq: f"Collecting {__type} prices by {__freq} from Alpha Square"

DEFAULT_QUERY = ["stock_1d", "stock_1h", "stock_1m", "etf_1d"]
QUERY_CONTEXT = {
    "stock_1d": {"freq":"day", "message":price_message("stock","day")},
    "stock_1h": {"freq":60, "message":price_message("stock","1hour")},
    "stock_1m": {"freq":1, "message":price_message("stock","1min")},
    "etf_1d": {"freq":"day", "message":price_message("etf","day")},
}

class SquarePricePipeline(SquarePriceSpider):
    operation = "squarePrices"

    @SquarePriceSpider.asyncio_session
    async def crawl(self, id: List[str], code: List[str], etf: List[Union[bool,str]], freq: List[int],
                queryTypes: Optional[List[str]]=list(), limit=600, startDate=None, endDate=None, trunc=2,
                **kwargs) -> Dict[str,List[Dict]]:
        queries = self.split_query(id, code, etf, freq)
        results = {queryType:list() for queryType in DEFAULT_QUERY}
        context = dict(limit=limit, startDate=startDate, endDate=endDate, trunc=trunc, **kwargs)
        for queryType in (queryTypes if queryTypes else DEFAULT_QUERY):
            results[queryType] = await self.gather(queries[queryType], **QUERY_CONTEXT[queryType], **context)
        self.upload_price(**results, **kwargs)
        return results

    def split_query(self, id: List[str], code: List[str], etf: List[Union[bool,str]], freq: List[int],
                    **kwargs) -> Dict[str,List[Tuple[str,str]]]:
        query = pd.DataFrame({"id":id, "code":code, "etf":etf, "freq":freq})
        query["etf"] = query["etf"].apply(lambda x: x=="TRUE" if isinstance(x, str) else x)
        stock, etf = query[~query["etf"]], query[query["etf"]]
        stock_1d, stock_1h, stock_1m = stock[stock["freq"]<=390], stock[stock["freq"]<=60], stock[stock["freq"]==1]
        to_query = lambda data: [(x,y) for x,y in zip(data["id"],data["code"])]
        return {"stock_1d":to_query(stock_1d), "stock_1h":to_query(stock_1h),
                "stock_1m":to_query(stock_1m), "etf_1d":to_query(etf)}

    def upload_price(self, stock_1d: List[Dict], stock_1h: List[Dict], stock_1m: List[Dict], etf_1d: List[Dict],
                    gsSheet=str(), gbqTable=str(), gbqMode="append", gbqSchema=None, gbqIndex=str(), **kwargs):
        daySchema, timeSchema = self.get_gbq_schema(freq="day"), self.get_gbq_schema(freq="minute")
        self.upload_data(stock_1d, gbqTable="kr.stock_1d", gbqMode=gbqMode, gbqSchema=daySchema, **kwargs)
        self.upload_data(stock_1h, gbqTable="kr.stock_1h", gbqMode=gbqMode, gbqSchema=timeSchema, **kwargs)
        self.upload_data(stock_1m, gbqTable="kr.stock_1m", gbqMode=gbqMode, gbqSchema=timeSchema, **kwargs)
        self.upload_data(etf_1d, gbqTable="kr.etf_1d", gbqMode=gbqMode, gbqSchema=daySchema, **kwargs)
