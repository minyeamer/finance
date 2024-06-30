from data import Info, Query, Variable, Schema, Field, Match
from data import PipelineInfo, PipelineQuery, PipelineSchema, PipelineField

from typing import Dict
import datetime as dt


###################################################################
####################### Alpha Square Detail #######################
###################################################################

SQUARE_DETAIL_SCHEMA = lambda: Schema(
    Field(name="id", type="STRING", desc="ID", mode="NOTZERO", path=["id"]),
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


SQUARE_DETAIL_INFO = lambda: Info(
    query = Query(Variable(name="code", type="STRING", desc="종목코드", iterable=True)),
    detail = SQUARE_DETAIL_SCHEMA(),
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

STOCK_PRICE_KR_QUERY = lambda: PipelineQuery(
    Variable(name="query", type="DICT", desc="쿼리", iterable=True),
    Variable(name="limit", type="INTEGER", desc="표시수", iterable=False, default=600),
    Variable(name="startTime", type="DATETIME", desc="시작일시", iterable=False, default=None),
    Variable(name="endTime", type="DATETIME", desc="종료일시", iterable=False, default=None),
    Variable(name="trunc", type="INTEGER", desc="반올림위치", iterable=False, default=2),
)


SQUARE_PRICE_ID_SCHEMA = lambda: Schema(
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


SQUARE_PRICE_INFO = lambda: Info(
    query = SQUARE_PRICE_QUERY(),
    id = SQUARE_PRICE_ID_SCHEMA(),
    kr = SQUARE_PRICE_KR_VALUE_SCHEMA(),
    us = SQUARE_PRICE_US_VALUE_SCHEMA(),
)

STOCK_PRICE_KR_INFO = lambda: PipelineInfo(
    query = STOCK_PRICE_KR_QUERY(),
    id = SQUARE_PRICE_ID_SCHEMA(),
    price = SQUARE_PRICE_KR_VALUE_SCHEMA(),
)


###################################################################
###################### Alpha Square Watchlist #####################
###################################################################

def _drop_ms(__date_string: str) -> str:
    return __date_string.split('.')[0]

def _get_item_id(item: Dict) -> int:
    info = item.get(item.get("type"))
    return info.get("id") if isinstance(info, Dict) else None

def _get_item_name(item: Dict) -> int:
    info = item.get(item.get("type"))
    key = "ko_name" if item.get("type") == "stock" else "name"
    return info.get(key) if isinstance(info, Dict) else None


SQUARE_WATCHLIST_PLUS_QUERY = lambda: PipelineQuery(
    Variable(name="cookies", type="STRING", desc="쿠키", iterable=False),
    Variable(name="key", type="STRING", desc="키", iterable=False, default=str()),
)

SQUARE_WATCHLIST_UPLOAD_QUERY = lambda: Query(
    Variable(name="value", type="STRING", desc="값", iterable=True),
    Variable(name="stockType", type="STRING", desc="종목종류", iterable=True),
    Variable(name="watchlistId", type="STRING", desc="관심목록ID", iterable=False),
    Variable(name="cookies", type="STRING", desc="쿠키", iterable=False),
)

SQUARE_WATCHLIST_BULK_UPLOAD_QUERY = lambda: Query(
    Variable(name="query", type="DICT", desc="쿼리", iterable=False),
    Variable(name="cookies", type="STRING", desc="쿠키", iterable=False),
)

SQUARE_WATCHLIST_DELETE_QUERY = lambda: Query(
    Variable(name="id", type="STRING", desc="ID", iterable=True),
    Variable(name="stockType", type="STRING", desc="종목종류", iterable=True),
    Variable(name="watchlistId", type="STRING", desc="관심목록ID", iterable=False),
    Variable(name="cookies", type="STRING", desc="쿠키", iterable=False),
)

SQUARE_WATCHLIST_CLEAR_QUERY = lambda: Query(
    Variable(name="query", type="STRING", desc="쿼리", iterable=True),
    Variable(name="cookies", type="STRING", desc="쿠키", iterable=False),
)


SQUARE_WATCHLIST_SCHEMA = lambda: Schema(
    Field(name="id", type="STRING", desc="ID", mode="NOTZERO", path=["id"]),
    Field(name="order", type="INTEGER", desc="순번", mode="NULLABLE", path=["order"]),
    Field(name="name", type="STRING", desc="이름", mode="NULLABLE", path=["name"]),
    Field(name="ownerId", type="STRING", desc="소유자ID", mode="NULLABLE", path=["owner_id"], cast=True),
    Field(name="createTime", type="DATETIME", desc="생성일시", mode="NULLABLE", path=["created_at"], apply=_drop_ms),
    Field(name="modifyTime", type="DATETIME", desc="수정일시", mode="NULLABLE", path=["updated_at"], apply=_drop_ms),
    Field(name="stockCount", type="INTEGER", desc="종목수", mode="NULLABLE", path=["stock_count"]),
)

SQUARE_WATCHLIST_ITEM_SCHEMA = lambda: Schema(
    Field(name="itemId", type="STRING", desc="관심종목ID", mode="NOTZERO", path=["id"]),
    Field(name="watchlistId", type="STRING", desc="관심목록ID", mode="QUERY", path=["id"]),
    Field(name="order", type="INTEGER", desc="순번", mode="NULLABLE", path=["order"]),
    Field(name="stockType", type="STRING", desc="종목종류", mode="NULLABLE", path=["type"]),
    Field(name="id", type="STRING", desc="ID", mode="NOTZERO", path=_get_item_id),
    Field(name="code", type="STRING", desc="종목코드", mode="OPTIONAL", path=["stock","code"], cast=True),
    Field(name="name", type="STRING", desc="종목코드", mode="NOTZERO", path=_get_item_name),
    Field(name="createTime", type="DATETIME", desc="생성일시", mode="NULLABLE", path=["created_at"], apply=_drop_ms),
    Field(name="modifyTime", type="DATETIME", desc="수정일시", mode="NULLABLE", path=["updated_at"], apply=_drop_ms),
)

SQUARE_WATCHLIST_UPLOAD_SCHEMA = lambda: Schema(
    Field(name="value", type="STRING", desc="값", mode="QUERY", path=["value"]),
    Field(name="stockType", type="STRING", desc="종목종류", mode="QUERY", path=["stockType"]),
    Field(name="status", type="INTEGER", desc="응답상태", mode="NULLABLE", path=["status"]),
)

SQUARE_WATCHLIST_DELETE_SCHEMA = lambda: Schema(
    Field(name="id", type="STRING", desc="값", mode="QUERY", path=["id"]),
    Field(name="stockType", type="STRING", desc="종목종류", mode="QUERY", path=["stockType"]),
    Field(name="message", type="STRING", desc="응답상태", mode="NULLABLE", path=["message"]),
)


SQUARE_WATCHLIST_INFO = lambda: Info(
    query = Query(Variable(name="cookies", type="STRING", desc="쿠키", iterable=False)),
    watchlist = SQUARE_WATCHLIST_SCHEMA(),
    item = SQUARE_WATCHLIST_ITEM_SCHEMA(),
)

SQUARE_WATCHLIST_PLUS_INFO = lambda: PipelineInfo(
    query = SQUARE_WATCHLIST_PLUS_QUERY(),
    watchlist = SQUARE_WATCHLIST_SCHEMA(),
    item = SQUARE_WATCHLIST_ITEM_SCHEMA(),
)

SQUARE_WATCHLIST_UPLOAD_INFO = lambda: Info(
    query = SQUARE_WATCHLIST_UPLOAD_QUERY(),
    upload = SQUARE_WATCHLIST_UPLOAD_SCHEMA(),
)

SQUARE_WATCHLIST_BULK_UPLOAD_INFO = lambda: PipelineInfo(
    query = SQUARE_WATCHLIST_BULK_UPLOAD_QUERY(),
    upload = SQUARE_WATCHLIST_UPLOAD_SCHEMA(),
)

SQUARE_WATCHLIST_DELETE_INFO = lambda: Info(
    query = SQUARE_WATCHLIST_DELETE_QUERY(),
    delete = SQUARE_WATCHLIST_DELETE_SCHEMA(),
)

SQUARE_WATCHLIST_CLEAR_INFO = lambda: PipelineInfo(
    query = SQUARE_WATCHLIST_CLEAR_QUERY(),
    delete = SQUARE_WATCHLIST_DELETE_SCHEMA(),
)
