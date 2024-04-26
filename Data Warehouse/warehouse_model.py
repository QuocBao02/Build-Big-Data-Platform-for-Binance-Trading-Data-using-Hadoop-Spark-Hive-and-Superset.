def Trades():
    return "CREATE TABLE IF NOT EXISTS Trades(trade_id BIGINT, \
isBuyerMaker BOOLEAN, \
isBestMatch BOOLEAN, \
price DECIMAL(20, 10), \
qty DECIMAL(20, 10), \
quoteQty DECIMAL(20, 10), \
time STRING) \
PARTITIONED BY(symbol_id INT);"

def Symbol_Infor():
    return "CREATE TABLE IF NOT EXISTS Symbol_Infor( \
symbol_name STRING, \
status STRING, \
baseAsset STRING, \
baseAssetPrecision INT, \
quoteAsset STRING, \
quotePrecision INT, \
quoteAssetPrecision INT, \
baseCommissionPrecision INT, \
quoteCommissionPrecision INT, \
filterstype_id INT , \
icebergAllowed BOOLEAN, \
ocoAllowed BOOLEAN, \
quoteOrderQtyMarketAllowed BOOLEAN, \
allowTrailingStop BOOLEAN, \
cancelReplaceAllowed BOOLEAN, \
isSpotTradingAllowed BOOLEAN, \
isMarginTradingAllowed BOOLEAN, \
defaultSelfTradePreventionMode STRING) \
PARTITIONED BY(symbol_id INT);"

# def Filters():
#     return "CREATE TABLE IF NOT EXISTS Filters( filtertype_id BIGINT,\
# price_id INT, \
# lot_size_filter_id INT, \
# iceberg_parts_filter_id INT, \
# market_lot_size_filter_id INT, \
# trailing_delta_filter_id INT, \
# percent_price_by_side_filter_id INT, \
# notional_filter_id INT, \
# max_num_orders_filter_id INT, \
# max_num_algo_orders_filter_id INT) \
# PARTITIONED BY(symbol_id INT);"

def Price_filter_detail():
    return "CREATE TABLE IF NOT EXISTS  Price_filter_detail(\
maxPrice DECIMAL(20, 10), \
minPrice DECIMAL(20, 10), \
tickSize DECIMAL(20, 10)) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Lot_size_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Lot_size_filter_detail(\
stepSize DECIMAL(20, 10), \
maxQty DECIMAL(20, 10), \
minQty DECIMAL(20, 10)) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Iceberg_parts_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Iceberg_parts_filter_detail(\
limit INT) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Market_lot_size_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Market_lot_size_filter_detail(\
stepSize DECIMAL(20, 10), \
maxQty DECIMAL(20, 10), \
minQty DECIMAL(20, 10)) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Trailing_delta_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Trailing_delta_filter_detail(\
minTrailingBelowDelta DECIMAL(20, 10), \
maxTrailingBelowDelta DECIMAL(20, 10), \
maxTrailingAboveDelta DECIMAL(20, 10), \
minTrailingAboveDelta DECIMAL(20, 10)) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Percent_price_by_side_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Percent_price_by_side_filter_detail(\
bidMultiplierUp DECIMAL(20, 10), \
askMultiplierUp DECIMAL(20, 10), \
bidMultiplierDown DECIMAL(20, 10), \
avgPriceMins DECIMAL(20, 10), \
askMultiplierDown DECIMAL(20, 10)) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Notional_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Notional_filter_detail(\
maxNotional DECIMAL(20, 10), \
minNotional DECIMAL(20, 10), \
avgPriceMins DECIMAL(20, 10), \
applyMinToMarket BOOLEAN, \
applyMaxToMarket BOOLEAN) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Max_num_orders_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Max_num_orders_filter_detail(\
maxNumOrders INT) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Max_num_algo_orders_filter_detail():
    return "CREATE TABLE IF NOT EXISTS Max_num_algo_orders_filter_detail(\
maxNumAlgoOrders INT) \
PARTITIONED BY(symbol_id INT, filter_type_id BIGINT);"

def Ticker_24h():
    return "CREATE TABLE IF NOT EXISTS Ticker_24h(ticker_id BIGINT, \
priceChange DECIMAL(20, 10), \
priceChangePercent DECIMAL(20, 10), \
weightedAvgPrice DECIMAL(20, 10), \
prevClosePrice DECIMAL(20, 10), \
lastPrice DECIMAL(20, 10), \
bidPrice DECIMAL(20, 10), \
askPrice DECIMAL(20, 10), \
openPrice DECIMAL(20, 10), \
highPrice DECIMAL(20, 10), \
lowPrice DECIMAL(20, 10), \
volume DECIMAL(20, 10), \
openTime STRING, \
closeTime STRING, \
fristTradeId BIGINT, \
lastTradeId BIGINT, \
count BIGINT) \
PARTITIONED BY(symbol_id INT);"

def Klines():
    return "CREATE TABLE IF NOT EXISTS Klines(kline_id INT, \
opentime STRING, \
openPrice DECIMAL(20, 10), \
highPrice DECIMAL(20, 10), \
lowPrice DECIMAL(20, 10), \
closePrice DECIMAL(20, 10), \
volume DECIMAL(20, 10), \
closetime STRING, \
quoteassetvolume DECIMAL(20, 10), \
numberoftrades INT, \
takerbaseassetvolume DECIMAL(20, 10), \
takerbuyquoteassetvolume DECIMAL(20, 10), \
ignored BOOLEAN) \
PARTITIONED BY(symbol_id INT);"
