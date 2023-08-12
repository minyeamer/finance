
from gscraper.base import parse_cookies
from gscraper.date import now

from urllib.parse import urlencode


def get_params(**kwargs) -> str:
    return urlencode(dict(
        {"formatted": "true",
        "crumb": "3y1.e/IRjsL",
        "lang": "en-US", "region": "US",
        "corsDomain": "finance.yahoo.com",}, **kwargs))


def get_cookies(**kwargs) -> str:
    d = "AQABBKZhumQCEBPgrUpD3p_PGNFU-Qn0otcFEgEBCAErxmTvZGdkb2UB_eMBAAcIpmG6ZAn0otc&S=AQAAAjKELA27052TeQmZ10WKg8E"
    cookies = dict({"cmp": f"t={int(now(minutes=20).timestamp())}&j=0&u=1---",
                    "A1": f"d={d}", "A3": f"d={d}", "A1S": f"d={d}&j=WORLD"}, **kwargs)
    return parse_cookies(cookies)


QUERY_FIELDS = [
    "messageBoardId",
    "longName",
    "shortName",
    "marketCap",
    "underlyingSymbol",
    "underlyingExchangeSymbol",
    "headSymbolAsString",
    "regularMarketPrice",
    "regularMarketChange",
    "regularMarketChangePercent",
    "regularMarketVolume",
    "uuid",
    "regularMarketOpen",
    "fiftyTwoWeekLow",
    "fiftyTwoWeekHigh",
    "toCurrency",
    "fromCurrency",
    "toExchange",
    "fromExchange",
    "corporateActions",]
