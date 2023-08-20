INTRADAY_PARAMS = lambda symbol, apiKey, interaval=60, adjusted=False, prepost=False, month=str(), datatype="csv", **kwargs: dict({
    "function": "TIME_SERIES_INTRADAY",
    "symbol": symbol,
    "apikey": apiKey,
    "interval": f"{interaval}min" if isinstance(interaval, int) else interaval,
    "adjusted": str(adjusted).lower(),
    "extended_hours": str(prepost).lower(),
    "datatype": datatype,
    "outputsize": "full",
}, **({"month":month} if month else dict()))

DAILY_PARAMS = lambda symbol, apiKey, adjusted=False, datatype="csv", **kwargs: {
    "function": "TIME_SERIES_DAILY"+("_ADJUSTED" if adjusted else str()),
    "symbol": symbol,
    "apikey": apiKey,
    "datatype": datatype,
    "outputsize": "full",
}

WEEKLY_PARAMS = lambda symbol, apiKey, adjusted=False, datatype="csv", **kwargs: {
    "function": "TIME_SERIES_WEEKLY"+("_ADJUSTED" if adjusted else str()),
    "symbol": symbol,
    "apikey": apiKey,
    "datatype": datatype,
    "outputsize": "full",
}

MONTHLY_PARAMS = lambda symbol, apiKey, adjusted=False, datatype="csv", **kwargs: {
    "function": "TIME_SERIES_MONTHLY"+("_ADJUSTED" if adjusted else str()),
    "symbol": symbol,
    "apikey": apiKey,
    "datatype": datatype,
    "outputsize": "full",
}
