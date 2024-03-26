from data import Info, Query, Variable, Schema, Field

import datetime as dt


###################################################################
######################## Samsung POP Login ########################
###################################################################

SAMSUNG_LOGIN_DATA = lambda userid, passwd, ticket: '&'.join([f"{__k}={__v}" for __k, __v in {
    "ticket": ticket,
    "customerUseridText": '*' * len(userid),
    "customerPasswd": passwd,
    "idsave": "",
    "customerUserid": userid,
    "XuseYN": "Q",
    "mac": "N/A",
    "nkey": "N/A",
    "hdsn": "N/A",
    "hkey": "N/A",
    "UserGubun": "Customer",
    "customer_session_timeout": "60",
    "isMobileYN": "Y",
    "af_auto_index": "start",
    "cmd": "loginCustomer",
    "ajaxQuery": "1",
}.items()])


###################################################################
#################### Samsung Foreign Order Book ###################
###################################################################

SAMSUNG_ORDER_DATA = lambda symbol, size=30, saveBufLen='1', saveBuf='1': [
    {
        "idx": "fid9000", # Order Book
        "gid": "9000",
        "fidCodeBean": {"9104": "a", "9244": "1", "9904": f"NQQUSA{symbol}", "9913" :"D", "9922": "1"},
        "outFid": "1,3,4,5,6,7,8,9,19,22,23,24,25,28,29,39,41,42,43,45,46,47,48,49,50,51,60,71,72,74,75,76,77,78,91,92,93,173,182,189,243,242,246,247,605,2703,1362,1547,2161,2897,2898,300,31,377,361,363,310,330,350,309,329,349,308,328,348,307,327,347,306,326,346,305,325,345,304,324,344,303,323,343,302,322,342,301,321,341,311,331,351,312,332,352,313,333,353,314,334,354,315,335,355,316,336,356,317,337,357,318,338,358,319,339,359,320,340,360,362,364,378,83,2838,2923,2924,2917,104,103,2866,226,191,2867,380",
        "isList": "0",
        "order": "ASC",
        "reqCnt": "30",
        "actionKey": "0",
        "saveBufLen": saveBufLen,
        "saveBuf": "1",
    },
    {
        "idx": "fid9003", # Order History
        "gid": "9003",
        "fidCodeBean": {"9007": "0", "9101": "0", "9104": "a", "9904": f"NQQUSA{symbol}", "9913": "D", "9922": "1"},
        "outFid": "300,4,5,6,7,8,*407,2703,420,2799,2797,2798,2800,419",
        "isList": "1",
        "order": "ASC",
        "reqCnt": int(size),
        "actionKey": "0",
        "saveBufLen": saveBufLen,
        "saveBuf": saveBuf,
    },
    {
        "idx": "fid9004", # Daily OHLCV
        "gid": "9004",
        "fidCodeBean": {"9101": "0", "9104": "a", "9244": "0", "9904": f"NQQUSA{symbol}", "9913": "D"},
        "outFid": "500,22,23,24,4,8",
        "isList": "1",
        "order": "ASC",
        "reqCnt": int(size),
        "actionKey": "0",
        "saveBufLen": saveBufLen,
        "saveBuf": saveBuf,
    }
]

EST_TO_KST = 13

def cast_time(__object: str) -> dt.time:
    if (isinstance(__object, str) and (len(__object) == 6) and __object.isdigit()):
        return dt.time((int(__object[:2])+EST_TO_KST)%24, int(__object[2:4]), int(__object[4:]))


SAMSUNG_ORDER_QUERY = lambda: Query(
    Variable(name="symbol", type="STRING", desc="티커", iterable=True),
    Variable(name="size", type="INTEGER", desc="주문수", iterable=False, default=30),
)


SAMSUNG_ORDER_BOOK_SCHEMA = lambda: Schema(
    Field(name="symbol", type="STRING", desc="티커", mode="QUERY", path=["symbol"]),
    Field(name="ask10", type="LIST", desc="매도10호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["310"]), int(data["330"])])),
    Field(name="ask09", type="LIST", desc="매도9호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["309"]), int(data["329"])])),
    Field(name="ask08", type="LIST", desc="매도8호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["308"]), int(data["328"])])),
    Field(name="ask07", type="LIST", desc="매도7호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["307"]), int(data["327"])])),
    Field(name="ask06", type="LIST", desc="매도6호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["306"]), int(data["326"])])),
    Field(name="ask05", type="LIST", desc="매도5호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["305"]), int(data["325"])])),
    Field(name="ask04", type="LIST", desc="매도4호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["304"]), int(data["324"])])),
    Field(name="ask03", type="LIST", desc="매도3호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["303"]), int(data["323"])])),
    Field(name="ask02", type="LIST", desc="매도2호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["302"]), int(data["322"])])),
    Field(name="ask01", type="LIST", desc="매도1호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["301"]), int(data["321"])])),
    Field(name="bid01", type="LIST", desc="매수1호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["311"]), int(data["331"])])),
    Field(name="bid02", type="LIST", desc="매수2호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["312"]), int(data["332"])])),
    Field(name="bid03", type="LIST", desc="매수3호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["313"]), int(data["333"])])),
    Field(name="bid04", type="LIST", desc="매수4호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["314"]), int(data["334"])])),
    Field(name="bid05", type="LIST", desc="매수5호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["315"]), int(data["335"])])),
    Field(name="bid06", type="LIST", desc="매수6호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["316"]), int(data["336"])])),
    Field(name="bid07", type="LIST", desc="매수7호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["317"]), int(data["337"])])),
    Field(name="bid08", type="LIST", desc="매수8호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["318"]), int(data["338"])])),
    Field(name="bid09", type="LIST", desc="매수9호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["319"]), int(data["339"])])),
    Field(name="bid10", type="LIST", desc="매수10호가", mode="NULLABLE", path=[], apply=(lambda data: [float(data["320"]), int(data["340"])])),
)

SAMSUNG_ORDER_HISTORY_SCHEMA = lambda: Schema(
    Field(name="symbol", type="STRING", desc="티커", mode="QUERY", path=["symbol"]),
    Field(name="time", type="TIME", desc="시간", mode="NULLABLE", path=["300"], apply=cast_time),
    Field(name="price", type="FLOAT", desc="주가", mode="NULLABLE", path=["4"]),
    Field(name="changePr", type="FLOAT", desc="등락폭", mode="NULLABLE", path=["6"]),
    Field(name="changePct", type="FLOAT", desc="등락률", mode="NULLABLE", path=["7"]),
    Field(name="volume", type="INTEGER", desc="거래량", mode="NULLABLE", path=["8"]),
    Field(name="amount", type="INTEGER", desc="체결량", mode="NULLABLE", path=["407"]),
    Field(name="volumePower", type="FLOAT", desc="체결강도", mode="NULLABLE", path=["2703"]),
    Field(name="volumeRatio", type="FLOAT", desc="매수비율", mode="NULLABLE", path=["420"]),
    Field(name="askPrice", type="STRING", desc="매도1호가", mode="NULLABLE", path=["2797"]),
    Field(name="bidPrice", type="STRING", desc="매수1호가", mode="NULLABLE", path=["2798"]),
)


SAMSUNG_ORDER_INFO = lambda: Info(
    query = SAMSUNG_ORDER_QUERY(),
    book = SAMSUNG_ORDER_BOOK_SCHEMA(),
    history = SAMSUNG_ORDER_HISTORY_SCHEMA(),
)
