
from gscraper.base import parse_cookies
from gscraper.date import now

from urllib.parse import urlencode


def get_params(**kwargs) -> str:
    return urlencode(dict(
        {"formatted": "true",
        "crumb": "3y1.e/IRjsL",
        "lang": "en-US", "region": "US",
        "corsDomain": "finance.yahoo.com",}, **kwargs))


def get_cookies(**kwargs) -> str:
    d = "AQABBKZhumQCEBPgrUpD3p_PGNFU-Qn0otcFEgEBCAErxmTvZGdkb2UB_eMBAAcIpmG6ZAn0otc&S=AQAAAjKELA27052TeQmZ10WKg8E"
    cookies = dict({"cmp": f"t={int(now(minutes=20).timestamp())}&j=0&u=1---",
                    "A1": f"d={d}", "A3": f"d={d}", "A1S": f"d={d}&j=WORLD"}, **kwargs)
    return parse_cookies(cookies)


QUERY_FIELDS = [
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
    "corporateActions",]

PRICE_HINT = ["Strong Buy", "Buy", "Hold", "Underperform", "Sell"]

QUERY_COLUMS = {
    # Summary
    "symbol": "Symbol",
    "longName": "Long Name", # name
    "currency": "Currency",
    "previousClose": "Previous Close", # previousClose | regularMarketPreviousClose
    "open": "Open",
    "bid": "Bid",
    "bidSize": "Bid Size",
    "ask": "Ask",
    "askSize": "Ask Size",
    "low": "Low", # dayLow | regularMarketDayLow
    "high": "High", # dayHigh | regularMarketDayHigh
    "volume": "Volume", # volume | regularMarketVolume
    "avgVolume": "Avg. Volume",
    "per": "PE Ratio", # trailingPE
    "eps": "EPS", # trailingEps
    "targetMeanPrice": "1y Target Est",
    "firstTradeDateEpochUtc": "First Trade Date", # timestamp
    "timeZoneFullName": "Time Zone",
    "timeZoneShortName": "Time Zone Short Name",
    # Valuation Measures
    "marketCap": "Market Cap",
    "enterpriseValue": "Enterprise Value",
    "trailingPE": "Trailing P/E", # PER/EPS (-12M)
    "forwardPE": "Forward P/E", # PER/EPS (+12M)
    "trailingPegRatio": "PEG Ratio (5 yr expected)", # P/E + Growth,
    "pegRatio": "PEG Ratio",
    "trailingEps": "Trailing EPS",
    "forwardEps": "Forward EPS",
    "priceToSalesTrailing12Months": "Price/Sales (ttm)",
    "priceToBook": "Price/Book (mrq)",
    "enterpriseToRevenue": "Enterprise Value/Revenue",
    "enterpriseToEbitda": "Enterprise Value/EBITDA",
    # Trading Information
    "beta": "Beta (5Y Monthly)",
    "52WeekChange": "52-Week Change",
    "SandP52WeekChange": "S&P500 52-Week Change",
    "fiftyTwoWeekLow": "52 Week Low", # high52
    "fiftyTwoWeekHigh": "52 Week High", # low52
    "fiftyDayAverage": "50-Day Moving Average", # ma50
    "twoHundredDayAverage": "200-Day Moving Average", # ma200
    # Share Statistics
    "averageVolume": "Avg Vol (3 month)",
    "averageVolume10days": "Avg Vol (10 day)",
    "sharesOutstanding": "Shares Outstanding",
    "impliedSharesOutstanding": "Implied Shares Outstanding", # Include CB
    "floatShares": "Float",
    "heldPercentInsiders": "% Held by Insiders",
    "heldPercentInstitutions": "% Held by Institutions",
    "sharesShort": "Shares Short",
    "shortRatio": "Short Ratio", # Sares Short / Volume 1D
    "shortPercentOfFloat": "Short % of Float", # Short Ratio * Shares Short / Float Shares
    "impliedSharesOutstanding": "Short % of Shares Outstanding", # Short Ratio * Shares Short / Shares Outstanding
    "sharesShortPriorMonth": "Shares Short (prior month)",
    # Dividends & Splits
    "dividendRate": "Forward Annual Dividend Rate",
    "dividendYield": "Forward Annual Dividend Yield",
    "trailingAnnualDividendRate": "Trailing Annual Dividend Rate",
    "trailingAnnualDividendYield": "Trailing Annual Dividend Yield",
    "fiveYearAvgDividendYield": "5 Year Average Dividend Yield",
    "payoutRatio": "Payout Ratio",
    "lastDividendValue": "Dividend Value", # dividendValue
    "lastDividendDate": "Dividend Date", # timestamp
    "exDividendDate": "Ex-Dividend Date", # timestamp
    "lastSplitFactor": "Last Split Factor",
    "lastSplitDate": "Last Split Date", # timestamp
    # Fiscal Year
    "lastFiscalYearEnd": "Fiscal Year Ends", # timestamp
    "nextFiscalYearEnd": "Next Fiscal Year Ends", # timestamp
    "mostRecentQuarter": "Most Recent Quarter (mrq)", # timestamp
    # Profitability
    "profitMargins": "Profit Margin",
    "operatingMargins": "Operating Margin (ttm)",
    # Management Effectiveness
    "returnOnAssets": "Return on Assets (ttm)", # roa
    "returnOnEquity": "Return on Equity (ttm)", # roe
    # Income Statement
    "totalRevenue": "Revenue (ttm)",
    "revenuePerShare": "Revenue Per Share (ttm)",
    "revenueGrowth": "Quarterly Revenue Growth (yoy)",
    "grossProfits": "Gross Profit (ttm)",
    "grossMargins": "Gross Margin",
    "ebitda": "EBITDA",
    "ebitdaMargins": "EBITDA Margin",
    "netIncomeToCommon": "Net Income Avi to Common (ttm)",
    "earningsGrowth": "Earnings Growth",
    "earningsQuarterlyGrowth": "Quarterly Earnings Growth (yoy)",
    # Balance Sheet
    "totalCash": "Total Cash (mrq)",
    "totalCashPerShare": "Total Cash Per Share (mrq)",
    "totalDebt": "Total Debt (mrq)",
    "debtToEquity": "Total Debt/Equity (mrq)",
    "quickRatio": "Quick Ratio",
    "currentRatio": "Current Ratio (mrq)",
    "bookValue": "Book Value Per Share (mrq)",
    # Cash Flow Statement
    "operatingCashflow": "Operating Cash Flow (ttm)",
    "freeCashflow": "Levered Free Cash Flow (ttm)",
    # Profile
    "address1": "Address",
    "city": "City",
    "state": "State",
    "zip": "ZIP Code",
    "country": "Country",
    "phone": "Phone",
    "website": "Website",
    "sector": "Sector",
    "industry": "Industry",
    "fullTimeEmployees": "Full Time Employees",
    "companyOfficers": "Key Executives", # [maxAge, name, age, title, yearBorn, fiscalYear, totalPay, exercisedValue, unexercisedValue]
    "longBusinessSummary": "Business Summary", # description
    # Corporate Governance
    "auditRisk": "Audit",
    "boardRisk": "Board",
    "shareHolderRightsRisk": "Shareholder Rights",
    "compensationRisk": "Compensation",
    "overallRisk": "ISS Governance QualityScore",
    # Analysis
    "recommendationMean": "Recommendation Rating",
    "recommendationKey": "Recommentdation Key",
    "priceHint": "Price hint", # index
    "currentPrice": "Current Price",
    "targetHighPrice": "Target High Price",
    "targetLowPrice": "Target Low Price",
    "targetMeanPrice": "Target Mean Price",
    "targetMedianPrice": "Target Median Price",
    "numberOfAnalystOpinions": "Number Of Analyst Opinions",
}
