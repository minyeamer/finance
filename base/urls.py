import re


API_URL = lambda host, uri, query=str(), **params: {
    "alpha": {
        "query": "https://www.alphavantage.co/query",
    }.get(uri, str()),
    "naver": {
        "company": f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={query}", # code
        "etf": f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={query}", # code
    }.get(uri, str()),
    "square": {
        "details": f"https://api.alphasquare.co.kr/data/v2/stock/details?code={query}", # code
        "prices": f"https://api.alphasquare.co.kr/data/v3/prices/candles/{query}", # id
    }.get(uri, str()),
    "yahoo": {
        "query": "https://query2.finance.yahoo.com/v7/finance/quote",
        "summary": f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{query}", # symbol
    }.get(uri, str()),
}.get(host, str())


GET_URL = lambda host, uri, query=str(), **params: {
    "naver": {
        "main": f"https://finance.naver.com/item/main.naver?code={query}", # code
        "info": f"https://finance.naver.com/item/coinfo.naver?code={query}", # code
        "investor": "https://finance.naver.com/sise/investorDealTrendDay.nhn",
    }.get(uri, str()),
    "square": {
        "main": "https://alphasquare.co.kr",
    }.get(uri, str()),
    "yahoo": {
        "main": f"https://finance.yahoo.com/quote/{query}/profile", # symbol
    }.get(uri, str()),
}.get(host, str())


POST_URL = lambda host, uri, query=str(), **params: {
    "": {

    }.get(uri, dict()),
}.get(host, str())


def url2id(url: str, uri: str) -> str:
    pattern = re.compile(f"(?<={uri})\d+")
    if pattern.search(url):
        return pattern.search(url).group()
