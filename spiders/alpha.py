from spiders import FinanceSpider, Flow, EST
from spiders import GET, API, ALPHA, URL, Symbol

from data import US_STOCK_PRICE_SCHEMA
from data.alpha import ALPHA_PRICE_INFO, ALPHA_PRICE_PARAMS

from gscraper.base.types import IndexLabel, DateFormat, Records, Data

from typing import Optional, Sequence, Tuple, Union
from abc import ABCMeta
from io import StringIO
import pandas as pd


class AlphaSpider(FinanceSpider):
    __metaclass__ = ABCMeta
    operation = "alphaSpider"
    host = ALPHA
    where = "Alpha Vantage"
    tzinfo = EST


###################################################################
####################### Alpha Vantage Price #######################
###################################################################

class AlphaPriceSpider(AlphaSpider):
    operation = "alphaPrice"
    which = "stock prices"
    iterateArgs = ["symbol"]
    iterateUnit = 1
    responseType = "records"
    returnType = "records"
    info = ALPHA_PRICE_INFO()
    flow = Flow("price")

    @AlphaSpider.init_session
    def crawl(self, symbol: Symbol, apiKey: str, freq: Union[str,int]="1d", month: Optional[Sequence[str]]=list(),
            startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
            adjusted=False, prepost=False, trunc: Optional[int]=2, **context) -> Data:
        dateType, freq = self.set_date_type(freq, adjusted)
        month = self.set_month(dateType, month, startDate, endDate)
        self.iterateProduct = ["month"] if month else list()
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    def set_date_type(self, freq: Union[str,int]="1d", adjusted=False) -> Tuple[str,str]:
        _adjusted = "_ADJUSTED" if adjusted else str()
        freq = freq.lower() if isinstance(freq, str) else freq
        if isinstance(freq, int): return "INTRADAY", f"{freq}min"
        elif "min" in freq: return "INTRADAY", freq
        elif "d" in freq: return ("DAILY"+_adjusted), str()
        elif "w" in freq: return ("WEEKLY"+_adjusted), str()
        elif "m" in freq: return ("MONTHLY"+_adjusted), str()
        else: return ("DAILY"+_adjusted), str()

    def set_month(self, dateType: str, month: Optional[Sequence[str]]=list(),
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None) -> Sequence[str]:
        if dateType != "INTRADAY": return list()
        elif month or (startDate is None): return month
        else: startDate, endDate = self.get_date_pair(startDate, endDate, if_null=(None,0))
        date_range = pd.date_range(startDate.replace(day=1), endDate.replace(day=1), freq="MS")
        return [__m.strftime("%Y-%m") for __m in date_range]

    @AlphaSpider.catch_exception
    @AlphaSpider.limit_request
    def fetch(self, symbol: str, apiKey: str, dateType: str, freq=str(), month=str(),
            adjusted=False, prepost=False, **context) -> Records:
        url = URL(API, ALPHA, "query")
        params = ALPHA_PRICE_PARAMS(symbol, apiKey, dateType, freq, month, adjusted, prepost, dataType="csv")
        response = self.request_text(GET, **self.local_request(locals()))
        return self.parse(**self.local_response(locals()))

    @AlphaSpider.validate_response
    def parse(self, response: str, **context) -> Records:
        data = pd.read_csv(StringIO(response), sep=',')
        return self.map(data.to_dict("records"), **context)

    def get_upload_columns(self, freq: Union[str,int]="1d", name=str(), **context) -> IndexLabel:
        dateType = "time" if isinstance(freq, int) or ("min" in str(freq)) else "date"
        return US_STOCK_PRICE_SCHEMA(dateType).get("name")
