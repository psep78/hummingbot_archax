from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "archax_main"

HBOT_ORDER_ID_PREFIX = "ARCHAX-"
MAX_ORDER_ID_LEN = 32
HBOT_BROKER_ID = "Hummingbot"

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_GTC = "GTC"
# Base URL
REST_URLS = {"archax_main": "https://ace-auth.qat.archax.com/api/1.0",
             "archax_testnet": "https://ace-auth.qat.archax.com/api/1.0"}

WSS_V1_PUBLIC_URL = {"archax_main": "wss://ace-auth.qat.archax.com/api/1.0/gateway",
                     "archax_testnet": "wss://ace-auth.qat.archax.com/api/1.0/gateway"}

WSS_PRIVATE_URL = {"archax_main": "wss://ace-auth.qat.archax.com/api/1.0/gateway",
                   "archax_testnet": "wss://ace-auth.qat.archax.com/api/1.0/gateway"}

# Websocket event types
LOGIN_EVENT_TYPE = "user-login"
INSTRUMENT_EVENT_TYPE = "instruments"
DIFF_EVENT_TYPE = "market-depths"
TRADE_EVENT_TYPE = "transactions"
SNAPSHOT_EVENT_TYPE = "market-depths"

# Public API endpoints
LOGIN_PATH_URL = "/login"
EXCHANGE_INFO_PATH_URL = "/rest-gateway/reporting/instruments"
SERVER_TIME_PATH_URL = "/rest-gateway/reporting/instruments"

# Private API endpoints
ORDER_PATH_URL = "/open-orders"

# Order States
ORDER_STATE = {
    "pending": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "completed": OrderState.FILLED,
    # "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
    "expired": OrderState.FAILED
}

WS_HEARTBEAT_TIME_INTERVAL = 30

# Rate Limit Type
REQUEST_GET = "GET"
REQUEST_POST = "POST"

ONE_MINUTE = 60

RATE_LIMITS = [
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=60, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=60, time_interval=ONE_MINUTE),
    RateLimit(limit_id=LOGIN_PATH_URL, limit=60, time_interval=ONE_MINUTE),
]
