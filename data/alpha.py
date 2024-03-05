from data import Info, Query, Variable, Schema, Field

from gscraper.base.types import Timezone
from gscraper.utils.cast import cast_datetime, get_timezone

from typing import Dict, Optional
import datetime as dt


###################################################################
####################### Alpha Vantage Price #######################
###################################################################

def _get_datetime(__m: Dict, dateType: str, tzinfo: Optional[Timezone]=None, **kwargs) -> dt.datetime:
    if dateType != "INTRADAY": return None
    datetime = cast_datetime(__m.get("timestamp"))
    if isinstance(datetime, dt.datetime):
        tzinfo = get_timezone(tzinfo)
        if tzinfo: return datetime.astimezone(tzinfo)
        else: return datetime.replace(tzinfo=None)
    else: return None


ALPHA_INTRADAY_EXTRA_PARAMS = lambda freq=str(), month=str(), adjusted=False, prepost=False: dict({
    "interval": freq,
    "adjusted": str(adjusted).lower(),
    "extended_hours": str(prepost).lower(),
}, **({"month":month} if month else dict()))


ALPHA_PRICE_PARAMS = lambda symbol, apiKey, dateType, freq=str(), month=str(), adjusted=False, prepost=False, dataType="csv": dict({
    "function": f"TIME_SERIES_{dateType}",
    "symbol": symbol,
    "apikey": apiKey,
    "datatype": dataType,
    "outputsize": "full",
}, **(ALPHA_INTRADAY_EXTRA_PARAMS(freq, month, adjusted, prepost) if dateType == "INTRADAY" else dict()))


ALPHA_PRICE_QUERY = lambda: Query(
    Variable(name="symbol", type="STRING", desc="티커", iterable=True),
    Variable(name="apiKey", type="STRING", desc="API키", iterable=False),
    Variable(name="freq", type=None, desc="주기", iterable=False, default="1d"),
    Variable(name="month", type="STRING", desc="연도월", iterable=True, default=list()),
    Variable(name="startDate", type="DATE", desc="시작일자", iterable=False, default=None),
    Variable(name="endDate", type="DATE", desc="종료일자", iterable=False, default=None),
    Variable(name="adjusted", type="BOOLEAN", desc="수정주가", iterable=False, default=False),
    Variable(name="prepost", type="BOOLEAN", desc="시간외주가", iterable=False, default=False),
    Variable(name="trunc", type="INTEGER", desc="반올림위치", iterable=False, default=2),
)


ALPHA_PRICE_SCHEMA = lambda: Schema(
    Field(name="symbol", type="STRING", desc="티커", mode="QUERY", path=["symbol"]),
    Field(name="date", type="DATE", desc="일자", mode="NULLABLE", path=["timestamp"]),
    Field(name="datetime", type="DATETIME", desc="일시", mode="OPTIONAL", path=_get_datetime),
    Field(name="open", type="FLOAT", desc="시가", mode="NULLABLE", path=["open"]),
    Field(name="high", type="FLOAT", desc="고가", mode="NULLABLE", path=["high"]),
    Field(name="low", type="FLOAT", desc="저가", mode="NULLABLE", path=["low"]),
    Field(name="close", type="FLOAT", desc="종가", mode="NULLABLE", path=["close"]),
    Field(name="adjClose", type="FLOAT", desc="수정종가", mode="NULLABLE", path=["adjusted_close"]),
    Field(name="dividendAmount", type="FLOAT", desc="배당금", mode="NULLABLE", path=["dividend_amount"]),
    Field(name="splitCoefficient", type="FLOAT", desc="액면분할계수", mode="NULLABLE", path=["split_coefficient"]),
)


ALPHA_PRICE_INFO = lambda: Info(
    query = ALPHA_PRICE_QUERY(),
    price = ALPHA_PRICE_SCHEMA(),
)
