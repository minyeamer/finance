from typing import Literal


GET = "GET"
POST = "POST"
OPTIONS = "OPTIONS"
HEAD = "HEAD"
PUT = "PUT"
PATCH = "PATCH"
DELETE = "DELETE"
API = "API"


ALPHA = "alpha"
NAVER = "naver"
SAMSUNG = "samsung"
SQUARE = "square"
YAHOO = "yahoo"


def URL(method: Literal["GET","POST","API"], host: str, uri: str, query=str(), **params) -> str:
    if host == ALPHA: return ALPHA_URL(method, uri, query, **params)
    elif host == NAVER: return NAVER_URL(method, uri, query, **params)
    elif host == SAMSUNG: return SAMSUNG_URL(method, uri, query, **params)
    elif host == SQUARE: return SQUARE_URL(method, uri, query, **params)
    elif host == YAHOO: return YAHOO_URL(method, uri, query, **params)
    else: return str()


def ALPHA_URL(method: Literal["GET","POST","API"], uri: str, query=str(), **params) -> str:
    if method == API:
        if uri == "query": return "https://www.alphavantage.co/query"
    else: return str()


def NAVER_URL(method: Literal["GET","POST","API"], uri: str, query=str(), **params) -> str:
    if method == GET:
        if uri == "main": return f"https://finance.naver.com/item/main.naver?code={query}" # code
        elif uri == "info": return f"https://finance.naver.com/item/coinfo.naver?code={query}" # code
        elif uri == "investor": return "https://finance.naver.com/sise/investorDealTrendDay.nhn"
    elif method == API:
        if uri == "company": return f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={query}" # code
        elif uri == "etf": return f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={query}" # code
    else: return str()


def SAMSUNG_URL(method: Literal["GET","POST","API"], uri: str, query=str(), **params) -> str:
    if method == GET:
        if uri == "main": return "https://www.samsungpop.com"
        elif uri == "login": return "https://www.samsungpop.com/login/login.do?cmd=notWindowOS&RETURN_MENU_CODE=$INDEX&isCertMode=Y"
        elif uri == "order": return "https://www.samsungpop.com/wts_new/index.pop?screenid=3500&amp"
    elif method == POST:
        if uri == "login": return "https://www.samsungpop.com/login/login.do"
        elif uri == "order": return "https://www.samsungpop.com/wts/fidForeign.do"
    else: return str()


def SQUARE_URL(method: Literal["GET","POST","API"], uri: str, query=str(), **params) -> str:
    if method == GET:
        if uri == "main": return "https://alphasquare.co.kr"
    elif method == API:
        if uri == "details": return f"https://api.alphasquare.co.kr/data/v2/stock/details?code={query}" # code
        elif uri == "prices": return f"https://api.alphasquare.co.kr/data/v3/prices/candles/{query}" # id
    else: return str()


def YAHOO_URL(method: Literal["GET","POST","API"], uri: str, query=str(), **params) -> str:
    if method == GET:
        if uri == "main": f"https://finance.yahoo.com/quote/{query}/profile" # symbol
    elif method == API:
        if uri == "query": return "https://query2.finance.yahoo.com/v7/finance/quote"
        elif uri == "summary": return f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{query}" # symbol
    else: return str()
