# A single source of truth for constant variables related to the exchange

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "huobi"
BROKER_ID = "AAc484720a"


REST_URL = "https://api.huobi.pro"
WS_PUBLIC_URL = "wss://api.huobi.pro/ws"
WS_PRIVATE_URL = "wss://api.huobi.pro/ws/v2"

WS_HEARTBEAT_TIME_INTERVAL = 30  # seconds

# Websocket event types
TRADE_CHANNEL_SUFFIX = "trade.detail"
ORDERBOOK_CHANNEL_SUFFIX = "depth.step0"

SYMBOLS_URL = "/v2/settings/common/symbols"
TRADE_RULES_URL = "/v1/settings/common/market-symbols"
TICKER_URL = "/market/tickers"
DEPTH_URL = "/market/depth"
LAST_TRADE_URL = "/market/trade"

API_VERSION_OLD = "/v1"
API_VERSION_NEW = "/v2"


SERVER_TIME_URL = "/v1/common/timestamp"
ACCOUNT_ID_URL = "/v1/account/accounts"
ACCOUNT_BALANCE_URL = "/v1/account/accounts/{}/balance"
OPEN_ORDERS_URL = "/v1/order/openOrders"
ORDER_DETAIL_URL = "/v1/order/orders/{}"
PLACE_ORDER_URL = "/v1/order/orders/place"
CANCEL_ORDER_URL = "/v1/order/orders/{}/submitcancel"
BATCH_CANCEL_URL = "/v1/order/orders/batchcancel"

HUOBI_ACCOUNT_UPDATE_TOPIC = "accounts.update#2"
HUOBI_ORDER_UPDATE_TOPIC = "orders#{}"
HUOBI_TRADE_DETAILS_TOPIC = "trade.clearing#{}#1"

HUOBI_SUBSCRIBE_TOPICS = {HUOBI_ORDER_UPDATE_TOPIC, HUOBI_ACCOUNT_UPDATE_TOPIC, HUOBI_TRADE_DETAILS_TOPIC}

WS_CONNECTION_LIMIT_ID = "WSConnection"
WS_REQUEST_LIMIT_ID = "WSRequest"
CANCEL_URL_LIMIT_ID = "cancelRequest"
ACCOUNT_BALANCE_LIMIT_ID = "accountBalance"
ORDER_DETAIL_LIMIT_ID = "orderDetail"

RATE_LIMITS = [
    RateLimit(WS_CONNECTION_LIMIT_ID, limit=50, time_interval=1),
    RateLimit(WS_REQUEST_LIMIT_ID, limit=10, time_interval=1),
    RateLimit(limit_id=SYMBOLS_URL, limit=10, time_interval=1),
    RateLimit(limit_id=TRADE_RULES_URL, limit=10, time_interval=1),
    RateLimit(limit_id=TICKER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=DEPTH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=LAST_TRADE_URL, limit=10, time_interval=1),
    RateLimit(limit_id=SERVER_TIME_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ACCOUNT_ID_URL, limit=100, time_interval=2),
    RateLimit(limit_id=ACCOUNT_BALANCE_LIMIT_ID, limit=100, time_interval=2),
    RateLimit(limit_id=ORDER_DETAIL_LIMIT_ID, limit=50, time_interval=2),
    RateLimit(limit_id=PLACE_ORDER_URL, limit=100, time_interval=2),
    RateLimit(limit_id=CANCEL_URL_LIMIT_ID, limit=100, time_interval=2),
    RateLimit(limit_id=BATCH_CANCEL_URL, limit=50, time_interval=2),

]

# Order States
ORDER_STATE = {
    "rejected": OrderState.FAILED,
    "canceled": OrderState.CANCELED,
    "submitted": OrderState.OPEN,
    "partial-filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "partial-canceled": OrderState.CANCELED,
    "created": OrderState.PENDING_CREATE
}
