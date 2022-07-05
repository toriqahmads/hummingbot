# A single source of truth for constant variables related to the exchange

from hummingbot.core.api_throttler.data_types import RateLimit


EXCHANGE_NAME = "huobi"

REST_URL = "https://api.huobi.pro"
WS_PUBLIC_URL = "wss://api.huobi.pro/ws"
WS_PRIVATE_URL = "wss://api.huobi.pro/ws/v2"

WS_HEARTBEAT_TIME_INTERVAL = 30  # seconds

# Websocket event types
TRADE_CHANNEL_SUFFIX = "trade.detail"
ORDERBOOK_CHANNEL_SUFFIX = "depth.step0"

SYMBOLS_URL = "/settings/common/symbols"
TRADE_RULES_URL="/settings/common/market-symbols"
TICKER_URL = "/market/tickers"
DEPTH_URL = "/market/depth"
LAST_TRADE_URL = "/market/trade"

API_VERSION_OLD = "/v1"
API_VERSION_NEW = "/v2"


SERVER_TIME_URL = "/common/timestamp"
ACCOUNT_ID_URL = "/account/accounts"
ACCOUNT_BALANCE_URL = "/account/accounts/{}/balance"
ORDER_DETAIL_URL = "/order/orders/{}"
PLACE_ORDER_URL = "/order/orders/place"
CANCEL_ORDER_URL = "/order/orders/{}/submitcancel"
BATCH_CANCEL_URL = "/order/orders/batchcancel"

HUOBI_ACCOUNT_UPDATE_TOPIC = "accounts.update#2"
HUOBI_ORDER_UPDATE_TOPIC = "orders#*"
HUOBI_TRADE_DETAILS_TOPIC = "trade.clearing#*"

HUOBI_SUBSCRIBE_TOPICS = {HUOBI_ORDER_UPDATE_TOPIC, HUOBI_ACCOUNT_UPDATE_TOPIC, HUOBI_TRADE_DETAILS_TOPIC}

