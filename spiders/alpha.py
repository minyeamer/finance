from gscraper.base import Spider, log_messages, log_response
from gscraper.date import get_date
from gscraper.map import unique, exist

from base.urls import API_URL
from data.models import US_STOCK_PRICE_SCHEMA
from data.alpha import *
from parsers.alpha import *

from typing import Dict, List
from requests import Session
import datetime as dt

ALPHA = "alpha"
EST = "US/Eastern"


class AlphaPriceSpider(Spider, AlphaPriceParser):
    operation = "alphaPrice"
    returnType = "dataframe"
    message = "Collecting stock prices from Alpha Vantage"

    @Spider.requests_session
    async def crawl(self, query: List[str], apiKey: str, startDate=None, endDate=None, interval="1d",
                    adjusted=False, prepost=False, trunc=2, tzinfo=EST, **kwargs) -> pd.DataFrame:
        startDate, endDate = get_date(startDate,1), get_date(endDate,1)
        return self.gather(
            unique(*query), apiKey, startDate, endDate, interval, adjusted, prepost, trunc, tzinfo, **kwargs)

    @Spider.response_filter
    def gather(self, query: List[str], apiKey: str, startDate: dt.date=None, endDate: dt.date=None, interval="1d",
                    adjusted=False, prepost=False, trunc=2, tzinfo=EST, message=str(), **kwargs) -> pd.DataFrame:
        message = message if message else self.message
        iterable = [(symbol,month) for symbol in query for month in self.set_period(startDate, endDate, interval)]
        results = [self.fetch(symbol, apiKey, month, interval, adjusted, prepost, trunc, tzinfo, **kwargs)
                    for symbol, month in self.tqdm(iterable, desc=message)]
        results = pd.concat([result for result in results if exist(result)])
        return self.map_reduce(results, query, startDate, endDate, trunc+2, **kwargs)

    @Spider.log_errors
    @Spider.requests_limit
    def fetch(self, symbol: str, apiKey: str, month=str(), interval="1d", adjusted=False,
                    prepost=False, trunc=2, tzinfo=EST, session: Session=None, **kwargs) -> pd.DataFrame:
        api_url = API_URL(ALPHA, "query")
        params = self.get_params(symbol, apiKey, month, interval, adjusted, prepost)
        self.logger.debug(log_messages(params=params, json=self.logJson))
        with session.get(api_url, params=params) as response:
            self.logger.info(log_response(response, url=api_url, symbol=symbol, month=month))
            return self.parse(response.text, symbol, month, interval, trunc, tzinfo, **kwargs)

    def map_reduce(self, data: pd.DataFrame, query: List[str], startDate: dt.date=None, endDate: dt.date=None,
                    trunc=4, **kwargs) -> pd.DataFrame:
        if startDate: data = data[data["date"]>=startDate]
        if endDate: data = data[data["date"]<=endDate]
        data = data.sort_values(["symbol",("datetime" if "datetime" in data else "date")])
        return pd.concat([self.calc_change(data[data["symbol"]==symbol], trunc, **kwargs) for symbol in query])

    def set_period(self, startDate: dt.date=None, endDate: dt.date=None, interval="1d", **kwargs) -> List[str]:
        if not (isinstance(interval, int) or "min" in str(interval) and startDate and endDate):
            return [endDate.strftime("%Y-%m")]
        date_range = pd.date_range(startDate.replace(day=1), endDate.replace(day=1), freq="M")
        return list(map(lambda x: x.strftime("%Y-%m"), date_range))+[endDate.strftime("%Y-%m")]

    def get_params(self, symbol: str, apiKey: str, month=str(), interval="1d", adjusted=False,
                    prepost=False, **kwargs) -> Dict:
        if isinstance(interval, int) or "min" in str(interval):
            return INTRADAY_PARAMS(symbol, apiKey, interval, adjusted, prepost, month)
        if interval == "1d": return DAILY_PARAMS(symbol, apiKey, adjusted)
        elif interval == "1w": return WEEKLY_PARAMS(symbol, apiKey, adjusted)
        elif interval == "1mo": return MONTHLY_PARAMS(symbol, apiKey, adjusted)
        else: return dict()

    def get_gbq_schema(self, interval="1d", **kwargs) -> List[Dict[str, str]]:
        return US_STOCK_PRICE_SCHEMA("time" if isinstance(interval, int) or "min" in str(interval) else "date")
