from gscraper.base import log_results
from gscraper.cast import cast_datetime, cast_date, get_timezone
from gscraper.map import filter_data

from base.parsers import StockPriceParser

from io import StringIO
import pandas as pd

ALPHA = "alpha"
EST = "US/Eastern"
UTC = "UTC"


class AlphaPriceParser(StockPriceParser):
    operation = "alphaPrice"

    def parse(self, response: str, symbol=str(), month=str(), interval="1d", trunc=2, tzinfo=EST,
                filter=list(), **kwargs) -> pd.DataFrame:
        raw = StringIO(response)
        data = pd.read_csv(raw, sep=',')
        data = self.map_price(data, symbol, interval, trunc, tzinfo, filter=filter, **kwargs)
        log_results(data, symbol=symbol, month=month)
        return data

    def map_price(self, data: pd.DataFrame, symbol=str(), interval="1d", trunc=2, tzinfo=EST,
                    filter=list(), **kwargs) -> pd.DataFrame:
        data["symbol"] = symbol
        if isinstance(interval, int) or "min" in str(interval):
            data["datetime"] = data["timestamp"].apply(lambda x: cast_datetime(x, tzinfo=tzinfo))
        data["date"] = data["timestamp"].apply(cast_date)
        data = self.trunc_price(data, trunc, **kwargs)
        return filter_data(data, filter=filter, return_type="dataframe")
