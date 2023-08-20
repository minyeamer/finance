from gscraper.base import Parser, log_results
from gscraper.cast import cast_timestamp, get_timezone
from gscraper.date import now
from gscraper.map import hier_get, filter_map, filter_data

from base.parsers import StockPriceParser

from typing import Dict
import json
import pandas as pd

YAHOO = "yahoo"
EST = "US/Eastern"

PRICE_RENAME = {"Date":"date", "Datetime":"datetime",
                "Open":"open", "High":"high", "Low":"low", "Close":"close", "Volume":"volume"}


class YahooTickerParser(Parser):
    operation = "yahooTicker"

    def parse(self, response: Dict, symbol=str(), prepost=False, filter=list(), **kwargs) -> Dict:
        info = dict(response, **{"symbol": symbol, "updateDate":now().date(), "updateTime":now()})
        return self.map_info(info, prepost, filter=filter, **kwargs)

    def map_info(self, info: Dict, prepost=False, filter=list(), **kwargs) -> Dict:
        info["name"] = info.get("longName")
        info["description"] = info.get("longBusinessSummary")
        info = self.map_date(info)
        info = self.map_price(info, prepost)
        info = self.map_finance(info)
        return filter_map(info, filter=filter)

    def map_date(self, info: Dict, **kwargs) -> Dict:
        for key, value in info.items():
            if ("Date" in key or "FiscalYear" in key) and isinstance(value, int):
                info[key] = cast_timestamp(value, tsUnit="s")
        return info

    def map_price(self, info: Dict, prepost=False, **kwargs) -> Dict:
        for column in ["high","low"]:
            info[column] = info.get(("day" if prepost else "regularMarketDay")+column.capitalize())
        for column in ["previousClose","volume"]:
            info[column] = info.get(column if prepost else "regularMarket"+column.capitalize())
        info["high52"], info["low52"] = info.get("fiftyTwoWeekHigh"), info.get("fiftyTwoWeekLow")
        info["ma50"], info["ma200"] = info.get("fiftyDayAverage"), info.get("twoHundredDayAverage")
        return info

    def map_finance(self, info: Dict, **kwargs) -> Dict:
        info["per"], info["eps"] = info.get("trailingPE"), info.get("trailingEps")
        info["dividendDate"], info["dividendValue"] = info.get("lastDividendDate"), info.get("lastDividendValue")
        info["roa"], info["roe"] = info.get("returnOnAssets"), info.get("returnOnEquity")
        return info


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


class YahooPriceParser(StockPriceParser):
    operation = "yahooPrice"

    def parse(self, response: pd.DataFrame, symbol=str(), trunc=2, tzinfo="default",
                filter=list(), **kwargs) -> pd.DataFrame:
        data = response.reset_index().rename(columns=PRICE_RENAME)
        data = self.map_price(data, symbol, trunc, tzinfo, filter=filter, **kwargs)
        log_results(data, symbol=symbol)
        return data

    def map_price(self, data: pd.DataFrame, symbol=str(), trunc=2, tzinfo="default",
                    filter=list(), **kwargs) -> pd.DataFrame:
        data["symbol"] = symbol
        data = self.map_date(data, tzinfo, **kwargs)
        data = self.trunc_price(data, trunc, **kwargs)
        data = self.calc_change(data, trunc+2, **kwargs)
        return filter_data(data, filter=filter, return_type="dataframe")

    def map_date(self, data: pd.DataFrame, tzinfo="default", **kwargs) -> pd.DataFrame:
        data["date"] = data[("date" if "date" in data else "datetime")].apply(lambda x: x.date())
        if (tzinfo != "default") and ("datetime" in data):
            tzinfo = get_timezone(tzinfo)
            if tzinfo: data["datetime"] = data["datetime"].apply(lambda x: x.astimezone(tzinfo))
            else: data["datetime"] = data["datetime"].apply(lambda x: x.replace(tzinfo=None))
        return data
