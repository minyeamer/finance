from gscraper.base import Parser, log_results
from gscraper.cast import cast_timestamp
from gscraper.date import now
from gscraper.map import hier_get, filter_map, filter_data

from typing import Dict
import json
import pandas as pd

YAHOO = "yahoo"
EST = "US/Eastern"

PRICE_RENAME = {"Date":"date", "Datetime":"datetime",
                "Open":"open", "High":"high", "Low":"low", "Close":"close", "Volume":"volume"}
PRICE_COLUMNS = ["open", "high", "low", "close"]


class YahooTickerParser(Parser):
    operation = "yahooTicker"

    def parse(self, response: Dict, symbol=str(), prepost=False, filter=list(), **kwargs) -> Dict:
        info = dict(response, **{"symbol": symbol, "updateDate":now().date(), "updateTime":now()})
        for key, value in info.items():
            if ("Date" in key or "FiscalYear" in key) and isinstance(value, int):
                info[key] = cast_timestamp(value, tsUnit="s")
        info["name"] = info.get("longName")
        info["description"] = info.get("longBusinessSummary")
        for column in ["high","low"]:
            info[column] = info.get(("day" if prepost else "regularMarketDay")+column.capitalize())
        for column in ["previousClose","volume"]:
            info[column] = info.get(column if prepost else "regularMarket"+column.capitalize())
        info["high52"], info["low52"] = info.get("fiftyTwoWeekHigh"), info.get("fiftyTwoWeekLow")
        info["ma50"], info["ma200"] = info.get("fiftyDayAverage"), info.get("twoHundredDayAverage")
        info["per"], info["eps"] = info.get("trailingPE"), info.get("trailingEps")
        info["dividendDate"], info["dividendValue"] = info.get("lastDividendDate"), info.get("lastDividendValue")
        info["roa"], info["roe"] = info.get("returnOnAssets"), info.get("returnOnEquity")
        return filter_map(info, filter=filter)


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
