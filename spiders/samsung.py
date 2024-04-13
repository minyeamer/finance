from spiders import FinanceEncSpider, Flow, KST, get_headers
from spiders import GET, POST, SAMSUNG, URL

from data.samsung import SAMSUNG_LOGIN_DATA
from data.samsung import SAMSUNG_ORDER_INFO, SAMSUNG_ORDER_DATA

from gscraper.base.spider import LoginSpider
from gscraper.base.types import LogLevel, Keyword, MappedData, JsonData

from typing import List, Optional
from abc import ABCMeta


###################################################################
######################## Samsung POP Login ########################
###################################################################

class SamsungLogin(LoginSpider):
    operation = "samsungLogin"
    host = SAMSUNG
    where = "Samsung POP"

    def __init__(self, userid: str, passwd: str, logLevel: LogLevel="WARN", logFile: Optional[str]=None,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None, **context):
        super().__init__(self.operation, logLevel, logFile, debug, extraSave, interrupt, **context)
        self.userid = userid
        self.passwd = passwd
        self.ticket = str()

    def login(self):
        self.fetch_ticket()
        url = URL(POST, SAMSUNG, "login")
        data = SAMSUNG_LOGIN_DATA(self.userid, self.passwd, self.ticket)
        headers = get_headers(host=url, refere=URL(GET, SAMSUNG, "login"), origin=True, urlencoded=True, utf8=True, xml=True)
        self.request_url(POST, url, origin="login", data=data, headers=headers)

    def fetch_ticket(self):
        url = URL(GET, SAMSUNG, "login")
        headers = get_headers(host=url, referer=URL(POST, SAMSUNG, "login"), secure=True)
        source = self.request_source(GET, url, origin="fetch_ticket", headers=headers)
        self.ticket = str(source.select_one('input[name="ticket"]').attrs.get("value")).replace('+', '/')


class SamsungSpider(FinanceEncSpider):
    __metaclass__ = ABCMeta
    operation = "samsungSpider"
    host = SAMSUNG
    where = "Samsung POP"
    tzinfo = KST
    auth = SamsungLogin
    authKey = ["userid", "passwd"]


###################################################################
#################### Samsung Foreign Order Book ###################
###################################################################

class SamsungOrderSpider(SamsungSpider):
    operation = "samsungOrder"
    which = "order info"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    returnType = "records"
    mappedReturn = True
    info = SAMSUNG_ORDER_INFO()
    flow = Flow()

    @SamsungSpider.login_session
    def crawl(self, symbol: Keyword, size=30, **context) -> MappedData:
        return self.gather(*self.validate_args(symbol), size=size, **context)

    @SamsungSpider.arrange_data
    @SamsungSpider.validate_range
    def reduce(self, data: List[MappedData], **context) -> MappedData:
        __m = dict(book=list(), history=list())
        for __data in data:
            __m["book"].append(__data["book"])
            __m["history"] += __data["history"]
        return __m

    @SamsungSpider.retry_request
    @SamsungSpider.limit_request
    def fetch(self, symbol: str, size=30, saveBufLen='1', saveBuf='1', **context) -> MappedData:
        url = URL(POST, SAMSUNG, "order")
        data = SAMSUNG_ORDER_DATA(symbol, size, saveBufLen, saveBuf)
        headers = get_headers(url, referer=URL(GET, SAMSUNG, "order"))
        response = self.request_json(POST, url, json=data, headers=headers, **context)
        return self.parse(response, symbol=symbol, **context)

    @SamsungSpider.validate_response
    def parse(self, response: JsonData, **context) -> MappedData:
        data = dict()
        data["book"] = self.map(response, flow=Flow("book"), responseType="dict", root=["fid9000","data",0], **context)
        data["history"] = self.map(response, flow=Flow("history"), responseType="records", root=["fid9003","data"], **context)
        return data
