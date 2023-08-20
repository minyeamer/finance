from gscraper.base import Parser, log_results
from gscraper.cast import cast_str, cast_timestamp
from gscraper.date import now, get_timestamp
from gscraper.map import filter_map

from typing import Dict, List
import json
import re

SQUARE = "square"
KST = "Asia/Seoul"

PRICE_FIELDS = ["date", "open", "high", "low", "close", "volume"]
PRICE_INDEX = [1,2,3,4]


class SquareDetailParser(Parser):
    operation = "squareDetail"

    def parse(self, response: str, code=str(), filter=list(), **kwargs) -> Dict:
        data = json.loads(response)[code]
        data["id"] = cast_str(data["id"])
        data["code"] = code
        data["updateDate"], data["upadteTime"] = now().date(), now()
        return filter_map(data, filter=filter)


class SquarePriceParser(Parser):
    operation = "squarePrice"

    def parse(self, response: str, id=str(), code=str(), freq="day", startDate=None, endDate=None,
                trunc=2, filter=list(), **kwargs) -> List[Dict]:
        data = json.loads(response)["data"]
        results = [self.map_price(price, id, code, freq, trunc, filter=filter, **kwargs)
                    for price in data if self.validate_data(price, startDate, endDate)]
        log_results(results, id=id, code=code)
        return results

    def map_price(self, data: List[int], id=str(), code=str(), freq="day", trunc=2, filter=list(), **kwargs) -> Dict:
        not_kr = re.match("[^\d]", str(code))
        price = dict(id=id, code=code,
            **{key:(round(float(value), trunc) if idx in PRICE_INDEX and not_kr else int(value))
                for idx,(key,value) in enumerate(zip(PRICE_FIELDS,data))})
        datetime = cast_timestamp(price["date"], tzinfo=KST, tsUnit="ms")
        if isinstance(freq, int) or "minute" in str(freq):
            price["datetime"] = datetime
        price["date"] = datetime.date()
        return filter_map(price, filter=filter)

    def validate_data(self, data: List[int], startDate=None, endDate=None, time=True, **kwargs) -> bool:
        if not data: return False
        valid = (len(data) == len(PRICE_FIELDS))
        if startDate: valid = valid & (data[0] >= get_timestamp(startDate, time=time, tsUnit="ms"))
        if endDate: valid = valid & (data[0] <= get_timestamp(endDate, time=time, tsUnit="ms"))
        return valid
