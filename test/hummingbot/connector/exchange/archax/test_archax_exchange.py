import asyncio
import json
import re
import unittest
from decimal import Decimal
from typing import Awaitable, Dict, NamedTuple, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch

from aioresponses import aioresponses
from bidict import bidict
from dateutil.parser import isoparse

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.archax import archax_constants as CONSTANTS, archax_web_utils as web_utils
from hummingbot.connector.exchange.archax.archax_api_order_book_data_source import ArchaxAPIOrderBookDataSource
from hummingbot.connector.exchange.archax.archax_exchange import ArchaxExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import BuyOrderCompletedEvent, MarketEvent, OrderCancelledEvent, OrderFilledEvent
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


class TestArchaxExchange(unittest.TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.api_key = "someKey"
        cls.api_passphrase = "somePassPhrase"
        cls.api_secret_key = "someSecretKey"

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.test_task: Optional[asyncio.Task] = None
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())

        self.exchange = ArchaxExchange(
            self.client_config_map,
            self.api_key,
            self.api_secret_key,
            trading_pairs=[self.trading_pair]
        )

        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(0)
        self.exchange._time_synchronizer.logger().setLevel(1)
        self.exchange._time_synchronizer.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

        self._initialize_event_loggers()

        ArchaxAPIOrderBookDataSource._trading_pair_symbol_map = {
            CONSTANTS.DEFAULT_DOMAIN: bidict(
                {self.ex_trading_pair: self.trading_pair})
        }

    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        ArchaxAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.buy_order_created_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_failure_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.sell_order_created_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.BuyOrderCreated, self.buy_order_created_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFailure, self.order_failure_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.SellOrderCreated, self.sell_order_created_logger)]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_exchange_rules_mock(self) -> Dict:
        exchange_rules = {
            "status": "OK",
            "data": [
                {
                    "id": 6,
                    "name": "COINALPHA",
                    "symbol": "COINALPHA",
                    "currency": "USDT",
                    "description": "COINALPHA-USDT trading pair",
                    "status": "Trading",
                    "type": "crypto",
                    "quantityDecimalPlaces": 6,
                    "priceDecimalPlaces": 2,
                    "initialPrice": "1000",
                    "minimumOrderValue": "10",
                    "tickSize": "0.01"
                }, {
                    "id": 11,
                    "name": "APP",
                    "symbol": "APP",
                    "currency": "USD",
                    "description": "APP-USD trading pair",
                    "status": "Trading",
                    "type": "security",
                    "quantityDecimalPlaces": 6,
                    "priceDecimalPlaces": 2,
                    "initialPrice": "1000",
                    "minimumOrderValue": "10",
                    "tickSize": "0.01"
                }, {
                    "id": 11,
                    "name": "USD",
                    "symbol": "USD",
                    "currency": "USD",
                    "description": "USD",
                    "status": "Trading",
                    "type": "security",
                    "quantityDecimalPlaces": 6,
                    "priceDecimalPlaces": 2,
                    "initialPrice": "1000",
                    "minimumOrderValue": "10",
                    "tickSize": "0.01"
                }
            ],
            "timestamp": "2023-10-24T08:47:37.221Z"
        }

        return exchange_rules

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal(str(0.01)),
                min_price_increment=Decimal(str(0.0001)),
                min_base_amount_increment=Decimal(str(0.000001)),
            )
        }

    def _validate_auth_credentials_present(self, request_call_tuple: NamedTuple):
        request_headers = request_call_tuple.kwargs["headers"]
        request_params = request_call_tuple.kwargs["params"]
        self.assertIn("Content-Type", request_headers)
        self.assertEqual("application/x-www-form-urlencoded", request_headers["Content-Type"])
        self.assertIn("api_key", request_params)
        self.assertIn("sign", request_params)

    def test_supported_order_types(self):
        supported_types = self.exchange.supported_order_types()
        self.assertNotIn(OrderType.MARKET, supported_types)
        self.assertIn(OrderType.LIMIT, supported_types)
        self.assertNotIn(OrderType.LIMIT_MAKER, supported_types)

    @aioresponses()
    def test_check_network_success(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        resp = {
            "status": "OK",
            "data": {
                "message": "Health check: up and running!"
            },
            "timestamp": "2023-11-05T16:50:34.142Z"
        }
        mock_api.get(url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(NetworkStatus.CONNECTED, ret)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        mock_api.get(url, status=500)

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(ret, NetworkStatus.NOT_CONNECTED)

    @aioresponses()
    def test_check_network_raises_cancel_exception(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)

        mock_api.get(url, exception=asyncio.CancelledError)

        self.assertRaises(asyncio.CancelledError, self.async_run_with_timeout, self.exchange.check_network())

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = web_utils.rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)

        resp = self.get_exchange_rules_mock()
        mock_api.get(url, body=json.dumps(resp))
        mock_api.get(url, body=json.dumps(resp))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertTrue(self.trading_pair in self.exchange._trading_rules)

    @aioresponses()
    def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = web_utils.rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        exchange_rules = {
            "status": "OK",
            "data": [
                {
                    "id": 6,
                    "name": "COINALPHA",
                    "symbol": "COINALPHA",
                    "currency": "USDT",
                    "description": "COINALPHA-USDT trading pair",
                    "status": "Trading",
                    "type": "crypto",
                    "quantityDecimalPlaces": 0,
                    "priceDecimalPlaces": 0,
                    "initialPrice": "1000",
                },
            ],
            "timestamp": "2023-10-24T08:47:37.221Z"
        }
        mock_api.get(url, body=json.dumps(exchange_rules))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertEqual(0, len(self.exchange._trading_rules))
        self.assertTrue(
            self._is_logged("ERROR", f"Error parsing the trading pair rule {self.trading_pair}. Skipping.")
        )

    def test_initial_status_dict(self):
        ArchaxAPIOrderBookDataSource._trading_pair_symbol_map = {}

        status_dict = self.exchange.status_dict

        expected_initial_dict = {
            "symbols_mapping_initialized": False,
            "order_books_initialized": False,
            "account_balance": False,
            "trading_rule_initialized": False,
            "user_stream_initialized": False,
        }

        self.assertEqual(expected_initial_dict, status_dict)
        self.assertFalse(self.exchange.ready)

    def test_get_fee_returns_fee_from_exchange_if_available_and_default_if_not(self):
        fee = self.exchange.get_fee(
            base_currency="SOME",
            quote_currency="OTHER",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("10"),
            price=Decimal("20"),
        )

        self.assertEqual(Decimal("0.001"), fee.percent)  # default fee

    @patch("hummingbot.connector.utils.get_tracking_nonce")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 9

        result = self.exchange.buy(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=True, trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False, trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN
        )

        self.assertEqual(result, expected_client_order_id)

    def test_restore_tracking_states_only_registers_open_orders(self):
        orders = []
        orders.append(InFlightOrder(
            client_order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
        ))
        orders.append(InFlightOrder(
            client_order_id="OID2",
            exchange_order_id="EOID2",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.CANCELED
        ))
        orders.append(InFlightOrder(
            client_order_id="OID3",
            exchange_order_id="EOID3",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        ))
        orders.append(InFlightOrder(
            client_order_id="OID4",
            exchange_order_id="EOID4",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FAILED
        ))

        tracking_states = {order.client_order_id: order.to_json() for order in orders}

        self.exchange.restore_tracking_states(tracking_states)

        self.assertIn("OID1", self.exchange.in_flight_orders)
        self.assertNotIn("OID2", self.exchange.in_flight_orders)
        self.assertNotIn("OID3", self.exchange.in_flight_orders)
        self.assertNotIn("OID4", self.exchange.in_flight_orders)

    @aioresponses()
    def test_create_limit_order_successfully(self, mock_api):
        self._simulate_trading_rules_initialized()

        self.exchange._user_stream = AsyncMock()
        self.exchange._user_stream._get_ws_assistant = AsyncMock()
        self.exchange._user_stream._get_ws_assistant.return_value = mock_api
        mock_api.send = AsyncMock()

        self.exchange._mapping[1] = combine_to_hb_trading_pair(base="COINALPHA", quote="USDT")
        self.exchange._set_trading_pair_symbol_map(self.exchange._mapping)

        self.async_run_with_timeout(
            self.exchange._create_order(trade_type=TradeType.BUY,
                                        order_id="OID1",
                                        trading_pair=self.trading_pair,
                                        amount=Decimal("100"),
                                        order_type=OrderType.LIMIT,
                                        price=Decimal("10000")))

        mock_api.send.assert_awaited_once()
        calls = [call(WSJSONRequest(payload={'action': 'order-submit', 'data': {'actionRef': 'OID1', 'instrumentId': '1', 'limitPrice': '10000.0000', 'orderType': 0, 'quantity': '100.000000', 'side': 0, 'organisationId': 0}}))]
        mock_api.send.assert_has_calls(calls, any_order=True)
        self.assertTrue(
            self._is_logged(
                "INFO",
                f"Created LIMIT BUY order OID1 for {Decimal('100.000000')} {self.trading_pair}."
            )
        )

    @aioresponses()
    def test_create_order_fails_when_trading_rule_error_and_raises_failure_event(self, mock_api):
        self._simulate_trading_rules_initialized()

        self.exchange._user_stream = AsyncMock()
        self.exchange._user_stream._get_ws_assistant = AsyncMock()
        self.exchange._user_stream._get_ws_assistant.return_value = mock_api
        mock_api.send = AsyncMock()

        self.exchange._mapping[1] = combine_to_hb_trading_pair(base="COINALPHA", quote="USDT")
        self.exchange._set_trading_pair_symbol_map(self.exchange._mapping)

        self.async_run_with_timeout(
            self.exchange._create_order(trade_type=TradeType.BUY,
                                        order_id="OID1",
                                        trading_pair=self.trading_pair,
                                        amount=Decimal("0.0001"),
                                        order_type=OrderType.LIMIT,
                                        price=Decimal("10000")))

        mock_api.send.assert_not_awaited()

        self.assertTrue(
            self._is_logged(
                "WARNING",
                "Buy order amount 0.0001 is lower than the minimum order "
                "size 0.01. The order will not be created, increase the "
                "amount to be higher than the minimum order size."
            )
        )
        self.assertTrue(
            self._is_logged(
                "INFO",
                f"Order OID1 has failed. Order Update: OrderUpdate(trading_pair='{self.trading_pair}', "
                f"update_timestamp={self.exchange.current_timestamp}, new_state={repr(OrderState.FAILED)}, "
                "client_order_id='OID1', exchange_order_id=None, misc_updates=None)"
            )
        )

    @aioresponses()
    def test_cancel_order_successfully(self, mock_api):
        self.exchange._user_stream = AsyncMock()
        self.exchange._user_stream._get_ws_assistant = AsyncMock()
        self.exchange._user_stream._get_ws_assistant.return_value = mock_api
        mock_api._connection = AsyncMock()
        mock_api._connection.connected = True
        mock_api.send = AsyncMock()

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="4",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn("OID1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders["OID1"]

        result = self.async_run_with_timeout(
            self.exchange._place_cancel(order_id="OID1",
                                        tracked_order=order))

        self.assertTrue(result)
        mock_api.send.assert_awaited_once()
        calls = [call(WSJSONRequest(payload={'action': 'order-cancel', 'data': {'actionRef': 'OID1c', 'orderId': '4', 'organisationId': 0}}, throttler_limit_id=None, is_auth_required=False))]
        mock_api.send.assert_has_calls(calls, any_order=True)

    @aioresponses()
    def test_cancel_order_raises_failure_event_when_not_connected(self, mock_api):
        self.exchange._user_stream = AsyncMock()
        self.exchange._user_stream._get_ws_assistant = AsyncMock()
        self.exchange._user_stream._get_ws_assistant.return_value = mock_api
        mock_api._connection = AsyncMock()
        mock_api._connection.connected = False
        mock_api.send = AsyncMock()

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="4",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn("OID1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders["OID1"]

        result = self.async_run_with_timeout(
            self.exchange._place_cancel(order_id="OID1",
                                        tracked_order=order))

        self.assertFalse(result)
        mock_api.send.assert_not_awaited()

    @aioresponses()
    @patch("hummingbot.connector.time_synchronizer.TimeSynchronizer._current_seconds_counter")
    def test_update_time_synchronizer_successfully(self, mock_api, seconds_counter_mock):
        seconds_counter_mock.side_effect = [0, 0, 0]

        self.exchange._time_synchronizer.clear_time_offset_ms_samples()
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
            "status": "OK",
            "data": {
                "message": "Health check: up and running!"
            },
            "timestamp": "2023-11-06T15:09:23.735Z"
        }

        mock_api.get(regex_url, body=json.dumps(response))

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())
        server_time = int(isoparse(response["timestamp"]).timestamp() * 1e3)
        self.assertEqual(server_time, int(self.exchange._time_synchronizer.time() * 1e3))

    @aioresponses()
    def test_update_time_synchronizer_failure_is_logged(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
            "status": "ERROR",
            "data": {
            }
        }

        mock_api.get(regex_url, body=json.dumps(response))

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertTrue(self._is_logged("NETWORK", "Error getting server time."))

    @aioresponses()
    def test_update_time_synchronizer_raises_cancelled_error(self, mock_api):
        url = web_utils.rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout, self.exchange._update_time_synchronizer())

    @aioresponses()
    def test_update_balances(self, mock_api):

        self.exchange._mapping[1] = combine_to_hb_trading_pair(base="COINALPHA", quote="USDT")
        self.exchange._mapping[2] = combine_to_hb_trading_pair(base="BTC", quote="USD")

        balance_message = {
            "status": "OK",
            "action": "set-balances",
            "data": {
                "2786123094": {
                    "available": "5555",
                    "instrumentId": 1,
                    "total": "6666"
                },
                "2786123095": {
                    "available": "7777",
                    "instrumentId": 2,
                    "total": "8888"
                },
                "2786122842": {
                    "available": "1000180131",
                    "instrumentId": 3,
                    "total": "1000180207"
                }
            },
            "type": "balances",
            "timestamp": "2021-07-01T04:30:06.408Z"
        }

        async def async_generator_side_effect():
            yield balance_message

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("5555"), available_balances["COINALPHA"])
        self.assertEqual(Decimal("7777"), available_balances["BTC"])
        self.assertEqual(Decimal("6666"), total_balances["COINALPHA"])
        self.assertEqual(Decimal("8888"), total_balances["BTC"])

    @aioresponses()
    def test_update_balances_failure(self, mock_api):

        self.exchange._mapping[1] = combine_to_hb_trading_pair(base="COINALPHA", quote="USDT")
        self.exchange._mapping[2] = combine_to_hb_trading_pair(base="BTC", quote="USD")

        balance_message = {
            "status": "ERROR",
            "action": "set-balances",
            "data": {
            },
            "type": "balances",
            "timestamp": "2021-07-01T04:30:06.408Z"
        }

        async def async_generator_side_effect():
            yield balance_message

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertNotIn("COINALPHA-USDT", available_balances)
        self.assertNotIn("BTC-USD", available_balances)
        self.assertNotIn("COINALPHA-USDT", total_balances)
        self.assertNotIn("BTC-USD", total_balances)

    @aioresponses()
    def test_user_stream_order_submit(self, mock_api):

        submit_message = {
            "action": "order-submit",
            "data": {
                "actionRef": "aassssasss",
                "instrumentId": "6",
                "limitPrice": "3359.00",
                "orderType": 0,
                "quantity": "0.01",
                "side": 1,
                "organisationId": 198
            }
        }

        async def async_generator_side_effect():
            yield submit_message

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertTrue(
            self._is_logged(
                "INFO",
                "Order submitted: {'action': 'order-submit', 'data': {'actionRef': 'aassssasss', 'instrumentId': '6', 'limitPrice': '3359.00', 'orderType': 0, 'quantity': '0.01', 'side': 1, 'organisationId': 198}}"
            )
        )

    @aioresponses()
    def test_user_stream_order_submitted_failure(self, mock_api):

        submit_message = {
            "status": "ERROR",
            "action": "order-submitted",
            "data": {
                "error": "No instrument selected",
                "actionRef": "user_reference_1234"
            },
            "type": "notifications",
            "timestamp": "2021-07-07T07:45:04.231Z"
        }

        async def async_generator_side_effect():
            yield submit_message

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertIn("user_reference_1234", self.exchange._order_cache)

        self.assertTrue(
            self._is_logged(
                "WARNING",
                "Order user_reference_1234 submit error: No instrument selected"
            )
        )

    @aioresponses()
    def test_user_stream_order_updated(self, mock_api):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )

        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        submit_message = {
            "status": "OK",
            "action": "order-updated",
            "data": {
                "actionRef": "OID1",
                "orderId": "EOID1",
                "organisationId": 1,
                "status": "open"
            },
            "type": "notifications",
            "timestamp": "2021-07-07T07:26:45.140Z"
        }

        async def async_generator_side_effect():
            yield submit_message

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.assertNotEqual(OrderState.OPEN, order.current_state)

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertEqual(OrderState.OPEN, order.current_state)

    @aioresponses()
    def test_user_stream_order_rejected(self, mock_api):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )

        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        submit_message = {
            "status": "OK",
            "action": "order-updated",
            "data": {
                "actionRef": "OID1",
                "error": "Dynamic Price Collar",
                "orderId": "EOID1",
                "organisationId": 1,
                "status": "rejected"
            },
            "type": "notifications",
            "timestamp": "2021-07-07T07:26:45.140Z"
        }

        async def async_generator_side_effect():
            yield submit_message

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.assertNotEqual(OrderState.FAILED, order.current_state)

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertEqual(OrderState.FAILED, order.current_state)

    @aioresponses()
    def test_user_stream_market_quotes(self, mock_api):
        market_quotes = {
            "status": "OK",
            "action": "set-market-quotes",
            "data": {
                "3": {
                    "lastPrice": "4327"
                },
                "4": {
                    "avgPrice": "1489"
                },
                "5": {
                    "lastPrice": "22145"
                }
            },
            "type": "market-quotes"
        }

        async def async_generator_side_effect():
            yield market_quotes

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.exchange._mapping[3] = self.trading_pair
        self.exchange._decimal_map[self.trading_pair] = (1, 1)

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertEqual(4327, self.exchange._last_prices[self.trading_pair])

    @aioresponses()
    def test_user_stream_market_quotes_fail(self, mock_api):
        market_quotes = {
            "status": "ERROR",
            "action": "set-market-quotes",
            "data": {
                "3": {
                    "lastPrice": "4327"
                }
            },
            "type": "market-quotes"
        }

        async def async_generator_side_effect():
            yield market_quotes

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.exchange._mapping[3] = self.trading_pair
        self.exchange._decimal_map[self.trading_pair] = (1, 1)

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertNotIn(self.trading_pair, self.exchange._last_prices)

    @aioresponses()
    def test_user_stream_market_quotes_failure(self, mock_api):
        market_quotes = {
            "status": "OK",
            "action": "set-market-quotes",
            "data": {
                "3": {
                    "lastPrice": "4327"
                }
            }
        }

        async def async_generator_side_effect():
            yield market_quotes

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.exchange._mapping[3] = self.trading_pair
        self.exchange._decimal_map[self.trading_pair] = (1, 1)

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertNotIn(self.trading_pair, self.exchange._last_prices)

    @aioresponses()
    def test_update_order_status_when_filled(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("1000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        order_update = {
            "status": "OK",
            "action": "set-orders",
            "data": {
                "EOID1": {
                    "averagePrice": "1000",
                    "created": "1623217157444",
                    "commission": "10",
                    "currency": "EUR",
                    "decimals": 2,
                    "executions": {
                        "1623217158966156": {
                            "created": "1623217158966",
                            "commission": "10",
                            "grossExecutionAmount": "17000",
                            "price": "1000",
                            "id": "1623217158966156",
                            "quantity": "1",
                            "tax": "0"
                        }
                    },
                    "filledPercent": "100.00",
                    "filledQty": "1",
                    "filledStatus": "filled",
                    "id": "EOID1",
                    "instrumentId": 24,
                    "limitPrice": "1000",
                    "orderStatus": "completed",
                    "quantity": "1",
                    "side": "buy",
                    "symbol": "OASa",
                    "type": "limit",
                    "tax": "0",
                    "userId": 1
                }
            },
            "type": "orders",
            "timestamp": "2021-07-06T08:04:20.837Z"
        }

        async def async_generator_side_effect():
            yield order_update

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.exchange._decimal_map[self.trading_pair] = (1, 1)
        self.exchange._order_id_map["EOID1"] = "OID1"

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        # Simulate the order has been filled with a TradeUpdate
        order.completely_filled_event.set()
        self.async_run_with_timeout(self.exchange._update_order_status())
        self.async_run_with_timeout(order.wait_until_completely_filled())

        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_done)

        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(Decimal(1), buy_event.base_asset_amount)
        self.assertEqual(Decimal(1000), buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(
            self._is_logged(
                "INFO",
                f"BUY order {order.client_order_id} completely filled."
            )
        )

    @aioresponses()
    def test_update_order_status_when_filled_with_missing_fields(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        order_update = {
            "status": "OK",
            "action": "set-orders",
            "data": {
                "EOID1": {
                    "averagePrice": "1000",
                    "created": "1623217157444",
                    "commission": "10",
                    "currency": "EUR",
                    "decimals": 2,
                    "executions": {
                        "1623217158966156": {
                            "grossExecutionAmount": "17000",
                            "id": "1623217158966156",
                        }
                    },
                    "filledPercent": "100.00",
                    "filledQty": "17",
                    "filledStatus": "filled",
                    "id": "EOID1",
                    "instrumentId": 24,
                    "limitPrice": "1000",
                    "orderStatus": "completed",
                    "quantity": "17",
                    "side": "buy",
                    "symbol": "OASa",
                    "type": "limit",
                    "tax": "0",
                    "userId": 1
                }
            },
            "type": "orders",
            "timestamp": "2021-07-06T08:04:20.837Z"
        }

        async def async_generator_side_effect():
            yield order_update

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.exchange._decimal_map[self.trading_pair] = (1, 1)
        self.exchange._order_id_map["EOID1"] = "OID1"

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        # Simulate the order has been filled with a TradeUpdate
        order.completely_filled_event.set()
        self.async_run_with_timeout(self.exchange._update_order_status())
        self.async_run_with_timeout(order.wait_until_completely_filled())

        self.assertFalse(order.is_filled)
        self.assertTrue(order.is_done)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

    @aioresponses()
    def test_update_order_status_when_cancelled(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="100234",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        order_status = {
            "status": "OK",
            "action": "order-cancelled",
            "data": {
                "actionRef": "OID1",
                "orderId": "100234",
                "organisationId": 1,
                "status": "cancelled"
            },
            "type": "notifications",
            "timestamp": "2021-07-07T18:59:04.231Z"
        }

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order.client_order_id, cancel_event.order_id)
        self.assertEqual(order.exchange_order_id, cancel_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(
            self._is_logged("INFO", f"Successfully canceled order {order.client_order_id}.")
        )

    @aioresponses()
    def test_not_update_order_status_when_cancel_fail(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)
        order_status = {
            "status": "ERROR",
            "action": "order-cancelled",
            "data": {
            },
            "type": "notifications",
            "timestamp": "2021-07-07T18:57:04.231Z"
        }

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertEqual(0, len(self.order_cancelled_logger.event_log))

    @aioresponses()
    def test_not_update_order_status_when_cancel_rejected(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)
        order_status = {
            "status": "ERROR",
            "action": "cancel-rejected",
            "data": {
            },
            "type": "notifications",
            "timestamp": "2021-07-07T18:57:04.231Z"
        }

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertEqual(0, len(self.order_cancelled_logger.event_log))

    @aioresponses()
    def test_update_order_status_when_order_has_not_changed(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        order_status = {
            "status": "OK",
            "action": "set-orders",
            "data": {
                "EOID1": {
                    "averagePrice": "1000",
                    "created": "1623217157444",
                    "commission": "10",
                    "currency": "EUR",
                    "decimals": 2,
                    "executions": {
                    },
                    "filledPercent": "100.00",
                    "filledQty": "17",
                    "filledStatus": "filled",
                    "id": "EOID1",
                    "instrumentId": 24,
                    "limitPrice": "1000",
                    "orderStatus": "open",
                    "quantity": "17",
                    "side": "buy",
                    "symbol": "OASa",
                    "type": "limit",
                    "tax": "0",
                    "userId": 1
                }
            },
            "type": "orders",
            "timestamp": "2021-07-01T08:04:40.837Z"
        }

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertTrue(order.is_open)

        self.async_run_with_timeout(self.exchange._update_order_status())

        self.assertTrue(order.is_open)
        self.assertFalse(order.is_filled)
        self.assertFalse(order.is_done)

    @aioresponses()
    def test_update_order_status_when_request_fails_marks_order_as_not_found(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              10 - 1)

        self.exchange._order_cache_updated = True

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        self.async_run_with_timeout(self.exchange._update_order_status())

        self.assertFalse(order.is_open)
        self.assertFalse(order.is_filled)
        self.assertTrue(order.is_done)

        self.assertEqual(0, self.exchange._order_tracker._order_not_found_records[order.client_order_id])

    def test_user_stream_update_for_order_partial_fill(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        order_status = {
            "status": "OK",
            "action": "set-orders",
            "data": {
                "EOID1": {
                    "averagePrice": "1000",
                    "created": "1623217157444",
                    "commission": "10",
                    "currency": "EUR",
                    "decimals": 2,
                    "executions": {
                        "2311070010000000968": {
                            "commission": "1",
                            "created": "1699361661658",
                            "grossExecutionAmount": "1100022570",
                            "id": "2311070010000000968",
                            "orderId": "1699361661659511",
                            "price": "10000",
                            "quantity": "0.5",
                            "referenceId": "1699361661660377",
                            "tax": "0"
                        }
                    },
                    "id": "EOID1",
                    "instrumentId": 24,
                    "limitPrice": "1000",
                    "orderStatus": "open",
                    "filledStatus": "partiallyFilled",
                    "quantity": "1",
                    "side": "buy",
                    "symbol": "OASa",
                    "type": "limit",
                    "tax": "0",
                    "userId": 1
                }
            },
            "type": "orders",
            "timestamp": "2021-07-06T18:04:40.837Z"
        }

        self.exchange._order_id_map["EOID1"] = "OID1"
        self.exchange._decimal_map[self.trading_pair] = (100, 1000000)

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertTrue(order.is_open)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(Decimal(100), fill_event.price)

        self.assertEqual([TokenAmount(amount=Decimal("0.01"), token=("USDT"))],
                         fill_event.trade_fee.flat_fees)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

        self.assertTrue(
            self._is_logged("INFO", f"The {order.trade_type.name} order {order.client_order_id} amounting to "
                                    f"{fill_event.amount}/{order.amount} {order.base_asset} has been filled.")
        )

    def test_user_stream_update_for_order_fill(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("0.000001"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        order_status = {
            "status": "OK",
            "action": "set-orders",
            "data": {
                "EOID1": {
                    "averagePrice": "1000",
                    "created": "1623217157444",
                    "commission": "10",
                    "currency": "EUR",
                    "decimals": 2,
                    "executions": {
                        "2311070010000000968": {
                            "commission": "1",
                            "created": "1699361661658",
                            "grossExecutionAmount": "1100022570",
                            "id": "2311070010000000968",
                            "orderId": "1699361661659511",
                            "price": "10000",
                            "quantity": "1",
                            "referenceId": "1699361661660377",
                            "tax": "0"
                        }
                    },
                    "id": "EOID1",
                    "instrumentId": 24,
                    "limitPrice": "1000",
                    "orderStatus": "completed",
                    "filledStatus": "filled",
                    "quantity": "1",
                    "side": "buy",
                    "symbol": "OASa",
                    "type": "limit",
                    "tax": "0",
                    "userId": 1
                }
            },
            "type": "orders",
            "timestamp": "2021-07-06T08:04:40.837Z"
        }

        self.exchange._order_cache_updated = True
        self.exchange._order_id_map["EOID1"] = "OID1"
        self.exchange._decimal_map[self.trading_pair] = (100, 1000000)

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        match_price = Decimal(100)
        match_size = Decimal("0.000001")
        self.assertEqual(match_price, fill_event.price)
        self.assertEqual(match_size, fill_event.amount)
        self.assertEqual([TokenAmount(amount=Decimal("0.01"), token=("USDT"))],
                         fill_event.trade_fee.flat_fees)

        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(order.amount, buy_event.base_asset_amount)
        self.assertEqual(order.amount * match_price, buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_done)

        self.assertTrue(
            self._is_logged(
                "INFO",
                f"BUY order {order.client_order_id} completely filled."
            )
        )

    def test_user_stream_update_for_order_commission(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("0.000001"),
        )

        order_status = {
            "status": "OK",
            "action": "set-orders",
            "data": {
                "EOID1": {
                    "commission": "10",
                    "currency": "EUR",
                    "id": "EOID1",
                    "instrumentId": 24,
                    "actionRef": "RefNumber"
                }
            },
            "type": "orders",
            "timestamp": "2021-07-06T08:04:40.837Z"
        }

        self.exchange._order_cache_updated = True
        self.exchange._order_id_map["EOID1"] = "OID1"
        self.exchange._decimal_map[self.trading_pair] = (100, 1000000)

        async def async_generator_side_effect():
            yield order_status

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.async_run_with_timeout(self.exchange._user_stream_event_listener())

        self.assertEqual(0, len(self.order_filled_logger.event_log))
        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

    def test_user_stream_raises_cancel_exception(self):
        self.exchange._set_current_timestamp(1640780000)

        data = MagicMock()
        data.get.side_effect = asyncio.CancelledError

        async def async_generator_side_effect():
            yield data

        self.exchange._iter_user_event_queue = async_generator_side_effect

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout,
            self.exchange._user_stream_event_listener())
