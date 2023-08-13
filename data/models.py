###################################################################
############################ US Schema ############################
###################################################################

# Clustering by date
# Partition by date
US_STOCK_DATE_SCHEMA = [
    {"name":"symbol", "type":"STRING", "mode":"REQUIRED"},
    {"name":"open", "type":"FLOAT"},
    {"name":"high", "type":"FLOAT"},
    {"name":"low", "type":"FLOAT"},
    {"name":"close", "type":"FLOAT"},
    {"name":"volume", "type":"INTEGER"},
    {"name":"date", "type":"DATE", "mode":"REQUIRED"}
]

# [HOUR] Clustering by datetime
# [MINUTE] Clustering by symbol, datetime
# Partition by datetime
US_STOCK_TIME_SCHEMA = [
    {"name":"symbol", "type":"STRING", "mode":"REQUIRED"},
    {"name":"open", "type":"FLOAT"},
    {"name":"high", "type":"FLOAT"},
    {"name":"low", "type":"FLOAT"},
    {"name":"close", "type":"FLOAT"},
    {"name":"volume", "type":"INTEGER"},
    {"name":"datetime", "type":"TIMESTAMP", "mode":"REQUIRED"}
]

US_STOCK_PRICE_SCHEMA = lambda dateType="date": (
    US_STOCK_DATE_SCHEMA if dateType == "date" else US_STOCK_TIME_SCHEMA)


###################################################################
############################ KR Schema ############################
###################################################################

# Clustering by date
# Partition by date
KR_STOCK_DATE_SCHEMA = [
    {"name":"code", "type":"STRING", "mode":"REQUIRED"},
    {"name":"open", "type":"INTEGER"},
    {"name":"high", "type":"INTEGER"},
    {"name":"low", "type":"INTEGER"},
    {"name":"close", "type":"INTEGER"},
    {"name":"volume", "type":"INTEGER"},
    {"name":"date", "type":"DATE", "mode":"REQUIRED"}
]

# [HOUR] Clustering by datetime
# [MINUTE] Clustering by symbol, datetime
# Partition by datetime
KR_STOCK_TIME_SCHEMA = [
    {"name":"code", "type":"STRING", "mode":"REQUIRED"},
    {"name":"open", "type":"INTEGER"},
    {"name":"high", "type":"INTEGER"},
    {"name":"low", "type":"INTEGER"},
    {"name":"close", "type":"INTEGER"},
    {"name":"volume", "type":"INTEGER"},
    {"name":"datetime", "type":"TIMESTAMP", "mode":"REQUIRED"}
]

KR_STOCK_PRICE_SCHEMA = lambda dateType="date": (
    KR_STOCK_DATE_SCHEMA if dateType == "date" else KR_STOCK_TIME_SCHEMA)
