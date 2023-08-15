from gscraper.base import Parser, log_results
from gscraper.cast import cast_str, cast_timestamp
from gscraper.date import now, get_timestamp
from gscraper.map import filter_map

from typing import Dict, List
import json
import re

ALPHA = "alpha"
KST = "Asia/Seoul"

KR_CODE_PATTERN = "\d{6}"
PRICE_FIELDS = ["date", "open", "high", "low", "close", "volume"]
PRICE_INDEX = [1,2,3,4]


class AlphaDetailParser(Parser):
    operation = "alphaDetail"

    def parse(self, response: str, code=str(), filter=list(), **kwargs) -> Dict:
        data = json.loads(response)[code]
        data["id"] = cast_str(data["id"])
        data["code"] = code
        data["updateDate"], data["upadteTime"] = now().date(), now()
        return filter_map(data, filter=filter)


class AlphaPriceParser(Parser):
    operation = "alphaPrice"

    def parse(self, response: str, id=str(), code=str(), start=None, end=None,
                trunc=2, filter=list(), **kwargs) -> List[Dict]:
        data = json.loads(response)["data"]
        results = [self.map_price(price, id, code, trunc, filter=filter, **kwargs)
                    for price in data if self.validate_data(price, start, end)]
        log_results(results, id=id, code=code)
        return results

    def map_price(self, data: List[int], id=str(), code=str(), trunc=2, filter=list(), **kwargs) -> Dict:
        is_kr = re.match(KR_CODE_PATTERN, code)
        price = dict(id=id, code=code,
            **{key:(round(float(value), trunc) if idx in PRICE_INDEX and not is_kr else int(value))
                for idx,(key,value) in enumerate(zip(PRICE_FIELDS,data))})
        datetime = cast_timestamp(price["date"], tzinfo=KST, tsUnit="ms")
        price["date"], price["datetime"] = datetime.date(), datetime
        return filter_map(price, filter=filter)

    def validate_data(self, data: List[int], start=None, end=None, time=True, **kwargs) -> bool:
        if not data: return False
        valid = (len(data) == len(PRICE_FIELDS))
        if start: valid = valid & (data[0] >= get_timestamp(start, time=time, tsUnit="ms"))
        if end: valid = valid & (data[0] <= get_timestamp(end, time=time, tsUnit="ms"))
        return valid
