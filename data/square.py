from data import Info, Query, Variable, Schema, Field, Match


###################################################################
####################### Alpha Square Detail #######################
###################################################################

SQAURE_DETAIL_SCHEMA = lambda: Schema(
    Field(name="id", type="STRING", desc="ID", mode="REQUIRED", path=["id"]),
    Field(name="code", type="STRING", desc="종목코드", mode="QUERY", path=["code"]),
    Field(name="isin", type="STRING", desc="12자리코드", mode="NULLABLE", path=["isin"]),
    Field(name="logo", type="STRING", desc="로고", mode="NULLABLE", path=["logo"]),
    Field(name="name", type="STRING", desc="종목명", mode="NULLABLE", path=["ko_name"]),
    Field(name="enName", type="STRING", desc="영문명", mode="NULLABLE", path=["en_name"]),
    Field(name="market", type="STRING", desc="거래소", mode="NULLABLE", path=["market"]),
    Field(name="isAlive", type="BOOLEAN", desc="상장여부", mode="NULLABLE", path=["is_alive"]),
    Field(name="stockType", type="STRING", desc="종목종류", mode="NULLABLE", path=("stock","etf"), match=Match(path=["sector"])),
    Field(name="description", type="STRING", desc="설명", mode="NULLABLE", path=["description"]),
    Field(name="sector", type="STRING", desc="섹터", mode="NULLABLE", path=["sector"]),
    Field(name="industry", type="STRING", desc="산업", mode="NULLABLE", path=["industry"]),
    Field(name="industryCode", type="STRING", desc="산업코드", mode="NULLABLE", path=["industry_code"]),
    Field(name="currency", type="STRING", desc="통화", mode="NULLABLE", path=["currency"]),
    Field(name="countryCode", type="STRING", desc="국가코드", mode="NULLABLE", path=["country_code"]),
    Field(name="timezone", type="STRING", desc="시간대", mode="NULLABLE", path=["timezone"]),
    Field(name="listDate", type="DATE", desc="상장일", mode="NULLABLE", path=["list_date"]),
)


SQAURE_DETAIL_INFO = lambda: Info(
    query = Query(Variable(name="code", type="STRING", desc="종목코드", iterable=True)),
    detail = SQAURE_DETAIL_SCHEMA(),
)


###################################################################
######################## Alpha Square Price #######################
###################################################################

CANDLE_LIMIT = 1000

SQUARE_PRICE_FIELDS = ["date", "open", "high", "low", "close", "volume"]

SQUARE_PRICE_PARAMS = lambda freq="day", limit=600: {
    "freq": f"minute-{freq}" if isinstance(freq, int) else freq,
    "limit": min(limit, CANDLE_LIMIT),
    "include_current_candle": "false"
}


SQUARE_PRICE_QUERY = lambda: Query(
    Variable(name="id", type="STRING", desc="ID", iterable=True),
    Variable(name="code", type="STRING", desc="종목코드", iterable=True, default=list()),
    Variable(name="freq", type=None, desc="주기", iterable=False, default="day"),
    Variable(name="limit", type="INTEGER", desc="표시수", iterable=False, default=600),
    Variable(name="startTime", type="DATETIME", desc="시작일시", iterable=False, default=None),
    Variable(name="endTime", type="DATETIME", desc="종료일시", iterable=False, default=None),
    Variable(name="trunc", type="INTEGER", desc="반올림위치", iterable=False, default=2),
)


SQAURE_PRICE_ID_SCHEMA = lambda: Schema(
    Field(name="id", type="STRING", desc="ID", mode="QUERY", path=["id"]),
    Field(name="code", type="STRING", desc="종목코드", mode="QUERY", path=["code"]),
    Field(name="datetime", type="DATETIME", desc="일시", mode="NULLABLE", path=["date"]),
    Field(name="date", type="DATE", desc="일자", mode="NULLABLE", path=["datetime"]),
)

SQUARE_PRICE_KR_VALUE_SCHEMA = lambda: Schema(
    Field(name="open", type="INTEGER", desc="시가", mode="NULLABLE", path=["open"]),
    Field(name="high", type="INTEGER", desc="고가", mode="NULLABLE", path=["high"]),
    Field(name="low", type="INTEGER", desc="저가", mode="NULLABLE", path=["low"]),
    Field(name="close", type="INTEGER", desc="종가", mode="NULLABLE", path=["close"]),
    Field(name="volume", type="INTEGER", desc="거래량", mode="NULLABLE", path=["volume"]),
)

SQUARE_PRICE_US_VALUE_SCHEMA = lambda: Schema(
    Field(name="open", type="FLOAT", desc="시가", mode="NULLABLE", path=["open"]),
    Field(name="high", type="FLOAT", desc="고가", mode="NULLABLE", path=["high"]),
    Field(name="low", type="FLOAT", desc="저가", mode="NULLABLE", path=["low"]),
    Field(name="close", type="FLOAT", desc="종가", mode="NULLABLE", path=["close"]),
    Field(name="volume", type="INTEGER", desc="거래량", mode="NULLABLE", path=["volume"]),
)


SQAURE_PRICE_INFO = lambda: Info(
    query = Query(Variable(name="code", type="STRING", desc="종목코드", iterable=True)),
    id = SQAURE_PRICE_ID_SCHEMA(),
    kr = SQUARE_PRICE_KR_VALUE_SCHEMA(),
    us = SQUARE_PRICE_US_VALUE_SCHEMA(),
)
