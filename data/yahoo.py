from data import Info, Query, Variable, Schema, Field, Map
from data import PipelineInfo, PipelineQuery, PipelineSchema, PipelineField

from gscraper.base.spider import parse_cookies
from gscraper.base.types import Timezone
from gscraper.utils.cast import get_timezone
from gscraper.utils.date import now

from typing import Dict, Optional
import datetime as dt


###################################################################
######################## Yahoo Information ########################
###################################################################

YAHOO_TICKER_SECTION = [
    "summary", "valuation", "trading", "share", "dividend", "fiscal", "profitability",
    "management", "income", "sheet", "cash", "profile", "governance", "analysis"
]

YAHOO_PRICE_HINT = ["Strong Buy", "Buy", "Hold", "Underperform", "Sell"]

YAHOO_TICKER_QUERY = lambda: Query(
    Variable(name="symbol", type="STRING", desc="Symbol", iterable=True),
    Variable(name="section", type="STRING", desc="Section", iterable=True, default=["summary"]),
    Variable(name="prepost", type="BOOLEAN", desc="Pre/Post", iterable=False, default=False),
    Variable(name="trunc", type="INTEGER", desc="Round Place", iterable=False, default=2),
)


YAHOO_TICKER_SUMMARY_SCHEMA = lambda: Schema( # Summary
    Field(name="symbol", type="STRING", desc="Symbol", mode="QUERY", path=["symbol"]),
    Field(name="name", type="STRING", desc="Long Name", mode="NULLABLE", path=["longName"]),
    Field(name="currency", type="STRING", desc="Currency", mode="NULLABLE", path=["currency"]),
    # Field(name="bid", type="FLOAT", desc="Bid Price", mode="NULLABLE", path=["bid"]),
    Field(name="bidSize", type="INTEGER", desc="Bid Size", mode="NULLABLE", path=["bidSize"]),
    # Field(name="ask", type="FLOAT", desc="Ask Price", mode="NULLABLE", path=["ask"]),
    Field(name="askSize", type="INTEGER", desc="Ask Size", mode="NULLABLE", path=["askSize"]),
    Field(name="targetMeanPrice", type="FLOAT", desc="1y Target Est", mode="NULLABLE", path=["targetMeanPrice"]),
    # Field(name="firstTradeDate", type="DATETIME", desc="First Trade Date", mode="NULLABLE", path=["firstTradeDateEpochUtc"]),
    Field(name="timezone", type="STRING", desc="Time Zone", mode="NULLABLE", path=["timeZoneFullName"]),
    Field(name="tzinfo", type="STRING", desc="Time Zone Short Name", mode="NULLABLE", path=["timeZoneShortName"]),
)

YAHOO_REGULAR_PRICE_SCHEMA = lambda: Schema( # Summary
    Field(name="open", type="FLOAT", desc="Open", mode="NULLABLE", path=["open"]),
    Field(name="high", type="FLOAT", desc="High", mode="NULLABLE", path=["regularMarketDayHigh"]),
    Field(name="low", type="FLOAT", desc="Low", mode="NULLABLE", path=["regularMarketDayLow"]),
    Field(name="close", type="FLOAT", desc="Close", mode="NULLABLE", path=["regularMarketPreviousClose"]),
    Field(name="volume", type="INTEGER", desc="Volume", mode="NULLABLE", path=["regularMarketVolume"]),
)

YAHOO_PREPOST_PRICE_SCHEMA = lambda: Schema( # Summary
    Field(name="open", type="FLOAT", desc="Open", mode="NULLABLE", path=["open"]),
    Field(name="high", type="FLOAT", desc="High", mode="NULLABLE", path=["dayHigh"]),
    Field(name="low", type="FLOAT", desc="Low", mode="NULLABLE", path=["dayLow"]),
    Field(name="close", type="FLOAT", desc="Close", mode="NULLABLE", path=["previousClose"]),
    Field(name="volume", type="INTEGER", desc="Volume", mode="NULLABLE", path=["volume"]),
)

YAHOO_TICKER_VALUATION_SCHEMA = lambda: Schema( # Valuation Measures
    Field(name="marketCap", type="INTEGER", desc="Market Cap", mode="NULLABLE", path=["marketCap"]),
    Field(name="enterpriseValue", type="INTEGER", desc="Enterprise Value", mode="NULLABLE", path=["enterpriseValue"]),
    Field(name="trailingPE", type="FLOAT", desc="Trailing P/E", mode="NULLABLE", path=["trailingPE"]), # PER/EPS (-12M)
    Field(name="forwardPE", type="FLOAT", desc="Forward P/E", mode="NULLABLE", path=["forwardPE"]), # PER/EPS (+12M)
    Field(name="trailingPegRatio", type="FLOAT", desc="PEG Ratio (5 yr expected)", mode="NULLABLE", path=["trailingPegRatio"]), # P/E + Growth
    Field(name="pegRatio", type="FLOAT", desc="PEG Ratio", mode="NULLABLE", path=["pegRatio"]),
    Field(name="trailingEps", type="FLOAT", desc="Trailing EPS", mode="NULLABLE", path=["trailingEps"]),
    Field(name="forwardEps", type="FLOAT", desc="Forward EPS", mode="NULLABLE", path=["forwardEps"]),
    Field(name="priceToSalesTrailing12Months", type="FLOAT", desc="Price/Sales (ttm)", mode="NULLABLE", path=["priceToSalesTrailing12Months"]),
    Field(name="priceToBook", type="FLOAT", desc="Price/Book (mrq)", mode="NULLABLE", path=["priceToBook"]),
    Field(name="enterpriseToRevenue", type="FLOAT", desc="Enterprise Value/Revenue", mode="NULLABLE", path=["enterpriseToRevenue"]),
    Field(name="enterpriseToEbitda", type="FLOAT", desc="Enterprise Value/EBITDA", mode="NULLABLE", path=["enterpriseToEbitda"]),
)

YAHOO_TICKER_TRADING_SCHEMA = lambda: Schema( # Trading Information
    Field(name="beta", type="FLOAT", desc="Beta (5Y Monthly)", mode="NULLABLE", path=["beta"]),
    Field(name="change52", type="FLOAT", desc="52-Week Change", mode="NULLABLE", path=["52WeekChange"]),
    Field(name="SnpChange52", type="FLOAT", desc="S&P500 52-Week Change", mode="NULLABLE", path=["SandP52WeekChange"]),
    Field(name="high52", type="FLOAT", desc="52 Week Low", mode="NULLABLE", path=["fiftyTwoWeekLow"]),
    Field(name="low52", type="FLOAT", desc="52 Week High", mode="NULLABLE", path=["fiftyTwoWeekHigh"]),
    Field(name="ma50", type="FLOAT", desc="50-Day Moving Average", mode="NULLABLE", path=["fiftyDayAverage"]),
    Field(name="ma200", type="FLOAT", desc="200-Day Moving Average", mode="NULLABLE", path=["twoHundredDayAverage"]),
)

YAHOO_TICKER_SHARE_SCHEMA = lambda: Schema( # Share Statistics
    Field(name="averageVolume", type="INTEGER", desc="Avg Vol (3 month)", mode="NULLABLE", path=["averageVolume"]),
    Field(name="averageVolume10days", type="INTEGER", desc="Avg Vol (10 day)", mode="NULLABLE", path=["averageVolume10days"]),
    Field(name="sharesOutstanding", type="INTEGER", desc="Shares Outstanding", mode="NULLABLE", path=["sharesOutstanding"]),
    Field(name="impliedSharesOutstanding", type="INTEGER", desc="Implied Shares Outstanding", mode="NULLABLE", path=["impliedSharesOutstanding"]), # Include CB
    Field(name="floatShares", type="FLOAT", desc="INTEGER", mode="NULLABLE", path=["floatShares"]),
    Field(name="heldPercentInsiders", type="FLOAT", desc="% Held by Insiders", mode="NULLABLE", path=["heldPercentInsiders"]),
    Field(name="heldPercentInstitutions", type="FLOAT", desc="% Held by Institutions", mode="NULLABLE", path=["heldPercentInstitutions"]),
    Field(name="sharesShort", type="INTEGER", desc="Shares Short", mode="NULLABLE", path=["sharesShort"]),
    Field(name="shortRatio", type="FLOAT", desc="Short Ratio", mode="NULLABLE", path=["shortRatio"]), # Sares Short / Volume 1D
    Field(name="shortPercentOfFloat", type="FLOAT", desc="Short % of Float", mode="NULLABLE", path=["shortPercentOfFloat"]), # Short Ratio * Shares Short / Float Shares
    Field(name="impliedSharesOutstanding", type="INTEGER", desc="Short % of Shares Outstanding", mode="NULLABLE", path=["impliedSharesOutstanding"]), # Short Ratio * Shares Short / Shares Outstanding
    Field(name="sharesShortPriorMonth", type="INTEGER", desc="Shares Short (prior month)", mode="NULLABLE", path=["sharesShortPriorMonth"]),
)

YAHOO_TICKER_DIVIDEND_SCHEMA = lambda: Schema( # Dividends & Splits
    Field(name="dividendRate", type="FLOAT", desc="Forward Annual Dividend Rate", mode="NULLABLE", path=["dividendRate"]),
    Field(name="dividendYield", type="FLOAT", desc="Forward Annual Dividend Yield", mode="NULLABLE", path=["dividendYield"]),
    Field(name="trailingAnnualDividendRate", type="FLOAT", desc="Trailing Annual Dividend Rate", mode="NULLABLE", path=["trailingAnnualDividendRate"]),
    Field(name="trailingAnnualDividendYield", type="FLOAT", desc="Trailing Annual Dividend Yield", mode="NULLABLE", path=["trailingAnnualDividendYield"]),
    Field(name="fiveYearAvgDividendYield", type="FLOAT", desc="5 Year Average Dividend Yield", mode="NULLABLE", path=["fiveYearAvgDividendYield"]),
    Field(name="payoutRatio", type="FLOAT", desc="Payout Ratio", mode="NULLABLE", path=["payoutRatio"]),
    Field(name="dividendValue", type="FLOAT", desc="Dividend Value", mode="NULLABLE", path=["lastDividendValue"]),
    Field(name="dividendDate", type="DATETIME", desc="Dividend Date", mode="NULLABLE", path=["lastDividendDate"]),
    Field(name="exDividendDate", type="DATETIME", desc="Ex-Dividend Date", mode="NULLABLE", path=["exDividendDate"]),
    Field(name="lastSplitFactor", type="STRING", desc="Last Split Factor", mode="NULLABLE", path=["lastSplitFactor"]),
    Field(name="lastSplitDate", type="DATETIME", desc="Last Split Date", mode="NULLABLE", path=["lastSplitDate"]),
)

YAHOO_TICKER_FISCAL_SCHEMA = lambda: Schema( # Fiscal Year
    Field(name="lastFiscalYearEnd", type="DATETIME", desc="Fiscal Year Ends", mode="NULLABLE", path=["lastFiscalYearEnd"]),
    Field(name="nextFiscalYearEnd", type="DATETIME", desc="Next Fiscal Year Ends", mode="NULLABLE", path=["nextFiscalYearEnd"]),
    Field(name="mostRecentQuarter", type="DATETIME", desc="Most Recent Quarter (mrq)", mode="NULLABLE", path=["mostRecentQuarter"]),
)

YAHOO_TICKER_PROFITABILITY_SCHEMA = lambda: Schema( # Profitability
    Field(name="profitMargins", type="FLOAT", desc="Profit Margin", mode="NULLABLE", path=["profitMargins"]),
    Field(name="operatingMargins", type="FLOAT", desc="Operating Margin (ttm)", mode="NULLABLE", path=["operatingMargins"]),
)

YAHOO_TICKER_MANAGEMENT_SCHEMA = lambda: Schema( # Management Effectiveness
    Field(name="roa", type="FLOAT", desc="Return on Assets (ttm)", mode="NULLABLE", path=["returnOnAssets"]),
    Field(name="roe", type="FLOAT", desc="Return on Equity (ttm)", mode="NULLABLE", path=["returnOnEquity"]),
)

YAHOO_TICKER_INCOME_SCHEMA = lambda: Schema( # Income Statement
    Field(name="totalRevenue", type="INTEGER", desc="Revenue (ttm)", mode="NULLABLE", path=["totalRevenue"]),
    Field(name="revenuePerShare", type="FLOAT", desc="Revenue Per Share (ttm)", mode="NULLABLE", path=["revenuePerShare"]),
    Field(name="revenueGrowth", type="FLOAT", desc="Quarterly Revenue Growth (yoy)", mode="NULLABLE", path=["revenueGrowth"]),
    # Field(name="grossProfits", type="FLOAT", desc="Gross Profit (ttm)", mode="NULLABLE", path=["grossProfits"]),
    Field(name="grossMargins", type="FLOAT", desc="Gross Margin", mode="NULLABLE", path=["grossMargins"]),
    Field(name="ebitda", type="INTEGER", desc="EBITDA", mode="NULLABLE", path=["ebitda"]),
    Field(name="ebitdaMargins", type="FLOAT", desc="EBITDA Margin", mode="NULLABLE", path=["ebitdaMargins"]),
    Field(name="netIncomeToCommon", type="INTEGER", desc="Net Income Avi to Common (ttm)", mode="NULLABLE", path=["netIncomeToCommon"]),
    Field(name="earningsGrowth", type="FLOAT", desc="Earnings Growth", mode="NULLABLE", path=["earningsGrowth"]),
    Field(name="earningsQuarterlyGrowth", type="FLOAT", desc="Quarterly Earnings Growth (yoy)", mode="NULLABLE", path=["earningsQuarterlyGrowth"]),
)

YAHOO_TICKER_SHEET_SCHEMA = lambda: Schema( # Balance Sheet
    Field(name="totalCash", type="INTEGER", desc="Total Cash (mrq)", mode="NULLABLE", path=["totalCash"]),
    Field(name="totalCashPerShare", type="FLOAT", desc="Total Cash Per Share (mrq)", mode="NULLABLE", path=["totalCashPerShare"]),
    Field(name="totalDebt", type="INTEGER", desc="Total Debt (mrq)", mode="NULLABLE", path=["totalDebt"]),
    Field(name="debtToEquity", type="FLOAT", desc="Total Debt/Equity (mrq)", mode="NULLABLE", path=["debtToEquity"]),
    Field(name="quickRatio", type="FLOAT", desc="Quick Ratio", mode="NULLABLE", path=["quickRatio"]),
    Field(name="currentRatio", type="FLOAT", desc="Current Ratio (mrq)", mode="NULLABLE", path=["currentRatio"]),
    Field(name="bookValue", type="FLOAT", desc="Book Value Per Share (mrq)", mode="NULLABLE", path=["bookValue"]),
)

YAHOO_TICKER_CASH_SCHEMA = lambda: Schema( # Cash Flow Statement
    Field(name="operatingCashflow", type="INTEGER", desc="Operating Cash Flow (ttm)", mode="NULLABLE", path=["operatingCashflow"]),
    Field(name="freeCashflow", type="INTEGER", desc="Levered Free Cash Flow (ttm)", mode="NULLABLE", path=["freeCashflow"]),
)

YAHOO_TICKER_PROFILE_SCHEMA = lambda: Schema( # Profile
    Field(name="address1", type="STRING", desc="Address", mode="NULLABLE", path=["address1"]),
    Field(name="city", type="STRING", desc="City", mode="NULLABLE", path=["city"]),
    Field(name="state", type="STRING", desc="State", mode="NULLABLE", path=["state"]),
    Field(name="zip", type="STRING", desc="ZIP Code", mode="NULLABLE", path=["zip"]),
    Field(name="country", type="STRING", desc="Country", mode="NULLABLE", path=["country"]),
    Field(name="phone", type="STRING", desc="Phone", mode="NULLABLE", path=["phone"]),
    Field(name="website", type="STRING", desc="Website", mode="NULLABLE", path=["website"]),
    Field(name="sector", type="STRING", desc="Sector", mode="NULLABLE", path=["sector"]),
    Field(name="industry", type="STRING", desc="Industry", mode="NULLABLE", path=["industry"]),
    Field(name="fullTimeEmployees", type="INTEGER", desc="Full Time Employees", mode="NULLABLE", path=["fullTimeEmployees"]),
    Field(name="companyOfficers", type="RECORDS", desc="Key Executives", mode="NULLABLE", path=["companyOfficers"], apply=Map(schema=YAHOO_TICKER_OFFICER_SCHEMA(), type="records")),
    Field(name="description", type="STRING", desc="Business Summary", mode="NULLABLE", path=["longBusinessSummary"]),
)

YAHOO_TICKER_OFFICER_SCHEMA = lambda: Schema( # companyOfficers
    Field(name="maxAge", type="INTEGER", desc="maxAge", mode="NULLABLE", path=["maxAge"]),
    Field(name="name", type="STRING", desc="name", mode="NULLABLE", path=["name"]),
    Field(name="age", type="INTEGER", desc="age", mode="NULLABLE", path=["age"]),
    Field(name="title", type="STRING", desc="title", mode="NULLABLE", path=["title"]),
    Field(name="yearBorn", type="INTEGER", desc="yearBorn", mode="NULLABLE", path=["yearBorn"]),
    Field(name="fiscalYear", type="INTEGER", desc="fiscalYear", mode="NULLABLE", path=["fiscalYear"]),
    Field(name="totalPay", type="INTEGER", desc="totalPay", mode="NULLABLE", path=["totalPay"]),
    Field(name="exercisedValue", type="INTEGER", desc="exercisedValue", mode="NULLABLE", path=["exercisedValue"]),
    Field(name="unexercisedValue", type="INTEGER", desc="unexercisedValue", mode="NULLABLE", path=["unexercisedValue"]),
)

YAHOO_TICKER_GOVERNANCE_SCHEMA = lambda: Schema( # Corporate Governance
    Field(name="auditRisk", type="FLOAT", desc="Audit", mode="NULLABLE", path=["auditRisk"]),
    Field(name="boardRisk", type="FLOAT", desc="Board", mode="NULLABLE", path=["boardRisk"]),
    Field(name="shareHolderRightsRisk", type="FLOAT", desc="Shareholder Rights", mode="NULLABLE", path=["shareHolderRightsRisk"]),
    Field(name="compensationRisk", type="FLOAT", desc="Compensation", mode="NULLABLE", path=["compensationRisk"]),
    Field(name="overallRisk", type="FLOAT", desc="ISS Governance QualityScore", mode="NULLABLE", path=["overallRisk"]),
)

YAHOO_TICKER_ANALYSIS_SCHEMA = lambda: Schema( # Analysis
    Field(name="recommendationMean", type="FLOAT", desc="Recommendation Rating", mode="NULLABLE", path=["recommendationMean"]),
    Field(name="recommendationKey", type="STRING", desc="Recommentdation Key", mode="NULLABLE", path=["recommendationKey"]),
    Field(name="priceHint", type="STRING", desc="Price hint", mode="NULLABLE", path=["priceHint"], apply=(lambda x: YAHOO_PRICE_HINT[x-1])),
    Field(name="currentPrice", type="FLOAT", desc="Current Price", mode="NULLABLE", path=["currentPrice"]),
    Field(name="targetHighPrice", type="FLOAT", desc="Target High Price", mode="NULLABLE", path=["targetHighPrice"]),
    Field(name="targetLowPrice", type="FLOAT", desc="Target Low Price", mode="NULLABLE", path=["targetLowPrice"]),
    Field(name="targetMeanPrice", type="FLOAT", desc="Target Mean Price", mode="NULLABLE", path=["targetMeanPrice"]),
    Field(name="targetMedianPrice", type="FLOAT", desc="Target Median Price", mode="NULLABLE", path=["targetMedianPrice"]),
    Field(name="numberOfAnalystOpinions", type="FLOAT", desc="Number Of Analyst Opinions", mode="NULLABLE", path=["numberOfAnalystOpinions"]),
)


YAHOO_TICKER_INFO = lambda: Info(
    query = YAHOO_TICKER_QUERY(),
    summary = YAHOO_TICKER_SUMMARY_SCHEMA(),
    regular = YAHOO_REGULAR_PRICE_SCHEMA(),
    prepost = YAHOO_PREPOST_PRICE_SCHEMA(),
    valuation = YAHOO_TICKER_VALUATION_SCHEMA(),
    trading = YAHOO_TICKER_TRADING_SCHEMA(),
    share = YAHOO_TICKER_SHARE_SCHEMA(),
    dividend = YAHOO_TICKER_DIVIDEND_SCHEMA(),
    fiscal = YAHOO_TICKER_FISCAL_SCHEMA(),
    profitability = YAHOO_TICKER_PROFITABILITY_SCHEMA(),
    management = YAHOO_TICKER_MANAGEMENT_SCHEMA(),
    income = YAHOO_TICKER_INCOME_SCHEMA(),
    sheet = YAHOO_TICKER_SHEET_SCHEMA(),
    cash = YAHOO_TICKER_CASH_SCHEMA(),
    profile = YAHOO_TICKER_PROFILE_SCHEMA(),
    governance = YAHOO_TICKER_GOVERNANCE_SCHEMA(),
    analysis = YAHOO_TICKER_ANALYSIS_SCHEMA(),
)


###################################################################
########################### Yahoo Query ###########################
###################################################################

YAHOO_QUERY_FIELDS = [
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
    "corporateActions",
]

def get_yahoo_params(**kwargs) -> Dict:
    return dict({
        "formatted": "true",
        "crumb": "3y1.e/IRjsL",
        "lang": "en-US",
        "region": "US",
        "corsDomain": "finance.yahoo.com",
    }, **kwargs)

def get_yahoo_cookies(**kwargs) -> str:
    d = "AQABBKZhumQCEBPgrUpD3p_PGNFU-Qn0otcFEgEBCAErxmTvZGdkb2UB_eMBAAcIpmG6ZAn0otc&S=AQAAAjKELA27052TeQmZ10WKg8E"
    cookies = dict({"cmp": f"t={int(now(minutes=20).timestamp())}&j=0&u=1---",
                    "A1": f"d={d}", "A3": f"d={d}", "A1S": f"d={d}&j=WORLD"}, **kwargs)
    return parse_cookies(cookies)


YAHOO_QUERY_SCHEMA = lambda: Schema( # Analysis
    Field(name="symbol", type="STRING", desc="티커", mode="QUERY", path=["symbol"]),
    Field(name="name", type="STRING", desc="종목명", mode="NULLABLE", path=["longName"]),
    Field(name="market", type="STRING", desc="거래소", mode="NULLABLE", path=["fullExchangeName"]),
)

YAHOO_SUMMARY_SCHEMA = lambda: Schema( # Analysis
    Field(name="symbol", type="STRING", desc="티커", mode="QUERY", path=["symbol"]),
    Field(name="sector", type="STRING", desc="섹터", mode="NULLABLE", path=["sector"]),
    Field(name="industry", type="STRING", desc="산업", mode="NULLABLE", path=["industry"]),
    Field(name="description", type="STRING", desc="설명", mode="NULLABLE", path=["longBusinessSummary"]),
)


YAHOO_QUERY_INFO = lambda: Info(
    query = Query(Variable(name="symbol", type="STRING", desc="티커", iterable=True)),
    info = YAHOO_QUERY_SCHEMA(),
)

YAHOO_SUMMARY_INFO = lambda: Info(
    query = Query(Variable(name="symbol", type="STRING", desc="티커", iterable=True)),
    summary = YAHOO_SUMMARY_SCHEMA(),
)


###################################################################
########################### Yahoo Price ###########################
###################################################################

YAHOO_DATE_LIMIT = {
    "1m":30, "2m":60, "5m":60, "15m":60, "30m":60, "60m":730, "90m":60,
    "1h":730, "1d":None, "5d":None, "1wk":None, "1mo":None, "3mo":None}

def _get_date(__m: Dict) -> dt.date:
    return (__m["Date"] if "Date" in __m else __m["Datetime"])

def _get_datetime(__m: Dict, tzinfo: Optional[Timezone]=None, **kwargs) -> dt.datetime:
    datetime = __m.get("Datetime")
    if isinstance(datetime, dt.datetime):
        tzinfo = get_timezone(tzinfo)
        if tzinfo: return datetime.astimezone(tzinfo)
        else: return datetime.replace(tzinfo=None)
    else: return None


YAHOO_PRICE_QUERY = lambda: Query(
    Variable(name="symbol", type="STRING", desc="티커", iterable=True),
    Variable(name="startDate", type="DATE", desc="시작일자", iterable=False, default=None),
    Variable(name="endDate", type="DATE", desc="종료일자", iterable=False, default=None),
    Variable(name="period", type="STRING", desc="기간", iterable=False, default=None),
    Variable(name="freq", type="STRING", desc="주기", iterable=False, default="1d"),
    Variable(name="prepost", type="BOOLEAN", desc="Pre/Post", iterable=False, default=False),
    Variable(name="trunc", type="INTEGER", desc="반올림위치", iterable=False, default=2),
)

DAILY_INDEX_QUERY = lambda: PipelineQuery(
    Variable(name="startDate", type="DATE", desc="시작일자", iterable=False, default=None),
    Variable(name="endDate", type="DATE", desc="종료일자", iterable=False, default=None),
    Variable(name="maxPrice", type="FLOAT", desc="최고가", iterable=False, default=None),
    Variable(name="trunc", type="INTEGER", desc="반올림위치", iterable=False, default=2),
    Variable(name="tzinfo", type=None, desc="시간대", iterable=False, default=None),
)


YAHOO_PRICE_SCHEMA = lambda: Schema(
    Field(name="symbol", type="STRING", desc="티커", mode="QUERY", path=["symbol"]),
    Field(name="datetime", type="DATETIME", desc="일시", mode="OPTIONAL", path=_get_datetime),
    Field(name="date", type="DATE", desc="일자", mode="NULLABLE", path=_get_date),
    Field(name="open", type="FLOAT", desc="시가", mode="NULLABLE", path=["Open"]),
    Field(name="high", type="FLOAT", desc="고가", mode="NULLABLE", path=["High"]),
    Field(name="low", type="FLOAT", desc="저가", mode="NULLABLE", path=["Low"]),
    Field(name="close", type="FLOAT", desc="종가", mode="NULLABLE", path=["Close"]),
    Field(name="adjClose", type="FLOAT", desc="수정종가", mode="NULLABLE", path=["Adj Close"]),
    Field(name="volume", type="INTEGER", desc="거래량", mode="NULLABLE", path=["Volume"]),
)

PRICE_CHANGE_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="gap", type="FLOAT", desc=""),
    PipelineField(name="highPct", type="FLOAT", desc=""),
    PipelineField(name="lowPct", type="FLOAT", desc=""),
    PipelineField(name="previousClose", type="FLOAT", desc=""),
    PipelineField(name="change", type="FLOAT", desc=""),
)

PREPOST_PRICE_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="preHigh", type="FLOAT", desc=""),
    PipelineField(name="preHighPct", type="FLOAT", desc=""),
    PipelineField(name="preLow", type="FLOAT", desc=""),
    PipelineField(name="preLowPct", type="FLOAT", desc=""),
    PipelineField(name="preChange", type="FLOAT", desc=""),
    PipelineField(name="postHigh", type="FLOAT", desc=""),
    PipelineField(name="postHighPct", type="FLOAT", desc=""),
    PipelineField(name="postLow", type="FLOAT", desc=""),
    PipelineField(name="postLowPct", type="FLOAT", desc=""),
    PipelineField(name="postChange", type="FLOAT", desc=""),
)

TOP_STOCK_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="topSymbol", type="STRING", desc=""),
    PipelineField(name="topClose", type="FLOAT", desc=""),
    PipelineField(name="topChange", type="FLOAT", desc=""),
)

DRAW_DOWN_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="maxPrice", type="FLOAT", desc=""),
    PipelineField(name="drawDown", type="FLOAT", desc=""),
)

US_INDEX_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="VIX", type="FLOAT"),
    PipelineField(name="USDX", type="FLOAT"),
    PipelineField(name="IRX", type="FLOAT"),
    PipelineField(name="TNX", type="FLOAT"),
    PipelineField(name="CL=F", type="FLOAT"),
    PipelineField(name="BTC-USD", type="FLOAT"),
    PipelineField(name="BTC-Change", type="FLOAT"),
)

KS_INDEX_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="KS200", type="FLOAT"),
    PipelineField(name="USD/KRW", type="FLOAT"),
    PipelineField(name="NASDAQ", type="FLOAT"),
    PipelineField(name="HSI", type="FLOAT"),
)

KQ_INDEX_SCHEMA = lambda: PipelineSchema(
    PipelineField(name="KQ100", type="FLOAT"),
    PipelineField(name="KQ15", type="FLOAT"),
    PipelineField(name="KQ26", type="FLOAT"),
    PipelineField(name="KQ47", type="FLOAT"),
    PipelineField(name="USD/KRW", type="FLOAT"),
    PipelineField(name="NASDAQ", type="FLOAT"),
    PipelineField(name="HSI", type="FLOAT"),
)


YAHOO_PRICE_INFO = lambda: Info(
    query = YAHOO_PRICE_QUERY(),
    price = YAHOO_PRICE_SCHEMA(),
)

DAILY_NASDAQ_INFO = lambda: PipelineInfo(
    query = DAILY_INDEX_QUERY(),
    price = YAHOO_PRICE_SCHEMA(),
    change = PRICE_CHANGE_SCHEMA(),
    prepost = PREPOST_PRICE_SCHEMA(),
    drawdown = DRAW_DOWN_SCHEMA(),
    index = US_INDEX_SCHEMA(),
)

DAILY_KOSPI_INFO = lambda: PipelineInfo(
    query = DAILY_INDEX_QUERY(),
    price = YAHOO_PRICE_SCHEMA(),
    change = PRICE_CHANGE_SCHEMA(),
    top = TOP_STOCK_SCHEMA(),
    drawdown = DRAW_DOWN_SCHEMA(),
    index = KS_INDEX_SCHEMA(),
)

DAILY_KOSDAQ_INFO = lambda: PipelineInfo(
    query = DAILY_INDEX_QUERY(),
    price = YAHOO_PRICE_SCHEMA(),
    change = PRICE_CHANGE_SCHEMA(),
    top = TOP_STOCK_SCHEMA(),
    drawdown = DRAW_DOWN_SCHEMA(),
    index = KQ_INDEX_SCHEMA(),
)
