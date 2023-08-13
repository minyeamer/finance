from gscraper.base import Parser, log_results
from gscraper.date import now, get_date
from gscraper.map import hier_get, filter_map, filter_data

from typing import Dict
import json
import pandas as pd

YAHOO = "yahoo"
EST = "US/Eastern"

PRICE_RENAME = {"Date":"date", "Datetime":"datetime",
                "Open":"open", "High":"high", "Low":"low", "Close":"close", "Volume":"volume"}
PRICE_COLUMNS = ["open", "high", "low", "close"]


class YahooQueryParser(Parser):
    operation = "yahooQuery"

    def parse(self, response: str, symbol=str(), filter=list(), **kwargs) -> Dict:
        data = hier_get(json.loads(response), ["quoteResponse","result",0])
        info = {"symbol": symbol,
                "name": data["longName"],
                "market": data["fullExchangeName"],
                "updateDate": now(tzinfo=EST).date(), "updateTime": now(tzinfo=EST)}
        return filter_map(info, filter=filter)


class YahooSummaryParser(Parser):
    operation = "yahooSummary"

    def parse(self, response: str, symbol=str(), filter=list(), **kwargs) -> Dict:
        data = hier_get(json.loads(response), ["quoteSummary","result",0,"fullExchangeName"])
        profile = {"symbol": symbol,
                    "sector": data["sector"],
                    "industry": data["industry"],
                    "description": data["longBusinessSummary"],
                    "updateDate": now(tzinfo=EST).date(), "updateTime": now(tzinfo=EST)}
        return filter_map(profile, filter=filter)


class YahooPriceParser(Parser):
    operation = "yahooPrice"

    def parse(self, response: pd.DataFrame, symbol=str(), trunc=2, filter=list(), **kwargs) -> pd.DataFrame:
        if response.empty:
            log_results(list(), symbol=symbol)
            return pd.DataFrame()
        data = response.reset_index().rename(columns=PRICE_RENAME)
        data["symbol"] = symbol
        if isinstance(trunc, int):
            for column in PRICE_COLUMNS:
                if column in data: data[column] = data[column].apply(lambda x: round(x, trunc))
        log_results(data, symbol=symbol)
        return filter_data(data, filter=filter, return_type="dataframe")
