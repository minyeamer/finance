from gscraper.base import Parser

from abc import ABCMeta
import pandas as pd

PRICE_COLUMNS = ["open", "high", "low", "close"]

calc_change = lambda __from, __to: (__to - __from) / __from


class StockPriceParser(Parser):
    __metaclass__ = ABCMeta
    operation = str()

    def trunc_price(self, data: pd.DataFrame, trunc=2,
                    columns=["open", "high", "low", "close"], **kwargs) -> pd.DataFrame:
        if isinstance(trunc, int):
            for column in columns:
                if column in data: data[column] = data[column].apply(lambda x: round(x, trunc))
        return data

    def calc_change(self, data: pd.DataFrame, trunc=4, **kwargs) -> pd.DataFrame:
        data = self.set_previous_close(data).copy()
        for column, price in zip(["gap", "highPct", "lowPct", "change"], PRICE_COLUMNS):
            if (price == "open") and ("datetime" in data): continue
            if price in data:
                change = calc_change(data["previousClose"], data[price])
                data[column] = change.apply(lambda x: round(x, trunc))
        return data

    def set_previous_close(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if "preivousClose" in data: pass
        elif ("date" not in data) or ("close" not in data): data["preivousClose"] = None
        elif "datetime" in data:
            data["date"] = data["datetime"].apply(lambda x: x.date())
            daily = (data[data["volume"]>0].groupby("date").agg({"close":"last"}).
                    reset_index().rename(columns={"close":"previousClose"}))
            daily["date"] = daily["date"].shift(-1)
            data = data.merge(daily[daily["date"].notna()], how="left", on="date")
        else: data["previousClose"] = data["close"].shift(1)
        return data
