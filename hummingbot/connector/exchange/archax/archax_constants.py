from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "archax_main"

HBOT_ORDER_ID_PREFIX = "ARCHAX-"
MAX_ORDER_ID_LEN = 32
HBOT_BROKER_ID = "Hummingbot"

SIDE_BUY = 0
SIDE_SELL = 1

ORDER_TYPE_LIMIT = 0
ORDER_TYPE_MARKET = 1

BUY = "buy"
SELL = "sell"

TIME_IN_FORCE_GTC = "GTC"
# Base URL
REST_URLS = {"archax_main": "https://ace-auth.archax.com/api/2.0",
             "archax_testnet": "https://ace-auth.qat.archax.com/api/2.0"}

WSS_V2_PUBLIC_URL = {"archax_main": "wss://ace-auth.archax.com/api/2.0/gateway",
                     "archax_testnet": "wss://ace-auth.qat.archax.com/api/2.0/gateway"}

WSS_PRIVATE_URL = {"archax_main": "wss://ace-auth.archax.com/api/2.0/gateway",
                   "archax_testnet": "wss://ace-auth.qat.archax.com/api/2.0/gateway"}

# Websocket event types
ORDER_SUBMIT_ACTION = "order-submit"
LOGIN_EVENT_TYPE = "user-login"
BALANCE_EVENT_TYPE = "balances"
INSTRUMENT_EVENT_TYPE = "instruments"
DIFF_EVENT_TYPE = "market-depths"
TRADE_EVENT_TYPE = "trade-histories"
SNAPSHOT_EVENT_TYPE = "market-depths"
NOTIFICATIONS_EVENT_TYPE = "notifications"
ORDERS_EVENT_TYPE = "orders"
ORDER_SUBMITTED_EVENT_TYPE = "order-submitted"
ORDER_UPDATED_EVENT_TYPE = "order-updated"
ORDER_CANCELLED_EVENT_TYPE = "order-cancelled"
ORDER_CANCEL_REJECTED_EVENT_TYPE = "cancel-rejected"
ORDER_CANCEL_FAILED_EVENT_TYPE = "cancel-failed"
QUOTES_EVENT_TYPE = "market-quotes"

# Public API endpoints
LOGIN_PATH_URL = "/login"
EXCHANGE_INFO_PATH_URL = "/rest-gateway/reporting/instruments"
SERVER_TIME_PATH_URL = "/health-check"

# Private API endpoints
ORDER_PATH_URL = "/open-orders"

# Order States
ORDER_STATE = {
    "pending": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "completed": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
    "expired": OrderState.FAILED
}

FILL_STATE = {
    "partiallyFilled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FAILED
}

WS_HEARTBEAT_TIME_INTERVAL = 3000

# Rate Limit Type
REQUEST_GET = "GET"
REQUEST_POST = "POST"

ONE_MINUTE = 60

RATE_LIMITS = [
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=60, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=60, time_interval=ONE_MINUTE),
    RateLimit(limit_id=LOGIN_PATH_URL, limit=60, time_interval=ONE_MINUTE),
]
