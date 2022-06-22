from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import OrderState

# API Domains
DEFAULT_DOMAIN = "api"
HFT_DOMAIN = "hft-api"
PUBLIC_DOMAIN = "public"

# Base API & WS URLs
REST_URL = "https://{}.coindcx.com"
WSS_URL = "https://stream.coindcx.com"

# Public URL endpoints
CHECK_NETWORK_REQUEST_PATH_URL = ""
TICKER_PRICE_PATH_URL = "/exchange/ticker"
MARKETS_PATH_URL = "/exchange/v1/markets_details"
ORDER_BOOK_PATH_URL = "/market_data/orderbook"
TRADE_HISTORY_PATH_URL = "/market_data/trade_history"

# Private URL endpoints
PLACE_ORDER_PATH_URL = "/exchange/v1/orders/create"
CANCEL_ORDER_PATH_URL = "/exchange/v1/orders/cancel"
BALANCE_PATH_URL = "/exchange/v1/users/balances"
ORDER_STATUS_PATH_URL = "/exchange/v1/orders/status"
MULTIPLE_ORDER_STATUS_PATH_URL = "/exchange/v1/orders/status_multiple"
USER_TRADE_HISTORY_PATH_URL = "/exchange/v1/orders/trade_history"

# Websocket Channels
ORDER_BOOK_DEPTH_EVENT_TYPE = "depth-update"
ORDER_BOOK_TRADE_EVENT_TYPE = "new-trade"
USER_TRADE_EVENT_TYPE = "trade-update"
USER_ORDER_EVENT_TYPE = "order-update"
USER_BALANCE_EVENT_TYPE = "balance-update"

# Order States
ORDER_STATE = {
    "init": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "partially_cancelled": OrderState.PENDING_CANCEL,
    "cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED
}

ORDER_TYPE_MAPPING = {
    OrderType.LIMIT: "limit_order",
    OrderType.MARKET: "market_order"
}

# Misc Exchange Information
MAX_CLIENT_ID_LEN = 36
CLIENT_ID_PREFIX = "HBOT-"


COINDCX_GLOBAL_RATE_LIMIT = 15  # 15 request/second

# Arbitrary Max request
MAX_REQUEST = 5000
ONE_SECOND = 1

RATE_LIMITS = [
    # POOL
    RateLimit(limit_id=COINDCX_GLOBAL_RATE_LIMIT, limit=COINDCX_GLOBAL_RATE_LIMIT, time_interval=1),
    # Individual endpoints
    RateLimit(limit_id=CHECK_NETWORK_REQUEST_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=TICKER_PRICE_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=MARKETS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=ORDER_BOOK_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=TRADE_HISTORY_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=PLACE_ORDER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=BALANCE_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=MULTIPLE_ORDER_STATUS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
    RateLimit(limit_id=USER_TRADE_HISTORY_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(COINDCX_GLOBAL_RATE_LIMIT)]),
]
