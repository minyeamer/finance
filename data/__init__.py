from gscraper.base.abstract import Query, Variable
from gscraper.base.session import Info, Schema, Field
from gscraper.base.session import Apply, Match, Cast, Exists, Join, Regex, Rename, Split, Stat, Sum, Map
from gscraper.base.spider import PipelineInfo, PipelineQuery, PipelineSchema, PipelineField
from gscraper.base.gcloud import BigQuerySchema

from gscraper.utils.map import flip_dict


###################################################################
######################## BigQuery KR Schema #######################
###################################################################

# PARTITION BY date
# PRIMARY KEY (date, code)
KR_STOCK_DATE_SCHEMA = lambda: BigQuerySchema(*[
    {"name":"code", "type":"STRING", "description":"종목코드", "mode":"REQUIRED"},
    {"name":"open", "type":"INTEGER", "description":"시가"},
    {"name":"high", "type":"INTEGER", "description":"고가"},
    {"name":"low", "type":"INTEGER", "description":"저가"},
    {"name":"close", "type":"INTEGER", "description":"종가"},
    {"name":"volume", "type":"INTEGER", "description":"거래량"},
    {"name":"date", "type":"DATE", "description":"일자", "mode":"REQUIRED"}
])

# PARTITION BY HOUR(datetime)
# PRIMARY KEY (datetime, code)
KR_STOCK_TIME_SCHEMA = lambda: BigQuerySchema(*[
    {"name":"code", "type":"STRING", "description":"종목코드", "mode":"REQUIRED"},
    {"name":"open", "type":"INTEGER", "description":"시가"},
    {"name":"high", "type":"INTEGER", "description":"고가"},
    {"name":"low", "type":"INTEGER", "description":"저가"},
    {"name":"close", "type":"INTEGER", "description":"종가"},
    {"name":"volume", "type":"INTEGER", "description":"거래량"},
    {"name":"datetime", "type":"TIMESTAMP", "description":"일시", "mode":"REQUIRED"}
])

KR_STOCK_PRICE_SCHEMA = lambda dateType="date": (
    KR_STOCK_DATE_SCHEMA() if dateType == "date" else KR_STOCK_TIME_SCHEMA())


###################################################################
######################## BigQuery US Schema #######################
###################################################################

# PARTITION BY date
# PRIMARY KEY (date, symbol)
US_STOCK_DATE_SCHEMA = lambda: BigQuerySchema(*[
    {"name":"symbol", "type":"STRING", "description":"티커", "mode":"REQUIRED"},
    {"name":"open", "type":"FLOAT", "description":"시가"},
    {"name":"high", "type":"FLOAT", "description":"고가"},
    {"name":"low", "type":"FLOAT", "description":"저가"},
    {"name":"close", "type":"FLOAT", "description":"종가"},
    {"name":"volume", "type":"FLOAT", "description":"거래량"},
    {"name":"date", "type":"DATE", "description":"일자", "mode":"REQUIRED"}
])

# PARTITION BY HOUR(datetime)
# PRIMARY KEY (datetime, symbol)
US_STOCK_TIME_SCHEMA = lambda: BigQuerySchema(*[
    {"name":"symbol", "type":"STRING", "description":"티커", "mode":"REQUIRED"},
    {"name":"open", "type":"FLOAT", "description":"시가"},
    {"name":"high", "type":"FLOAT", "description":"고가"},
    {"name":"low", "type":"FLOAT", "description":"저가"},
    {"name":"close", "type":"FLOAT", "description":"종가"},
    {"name":"volume", "type":"FLOAT", "description":"거래량"},
    {"name":"datetime", "type":"TIMESTAMP", "description":"일시", "mode":"REQUIRED"}
])

US_STOCK_PRICE_SCHEMA = lambda dateType="date": (
    US_STOCK_DATE_SCHEMA() if dateType == "date" else US_STOCK_TIME_SCHEMA())
