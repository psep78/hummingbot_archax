import asyncio
import json
import unittest
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.archax import archax_constants as CONSTANTS
from hummingbot.connector.exchange.archax.archax_api_order_book_data_source import ArchaxAPIOrderBookDataSource
from hummingbot.connector.exchange.archax.archax_exchange import ArchaxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class TestArchaxAPIOrderBookDataSource(unittest.TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.async_task = None
        self.mocking_assistant = NetworkMockingAssistant()
        self.auth = MagicMock()

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = ArchaxExchange(
            client_config_map=client_config_map,
            archax_email="",
            archax_password="",
            trading_pairs=[self.trading_pair])

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.time_synchronnizer = TimeSynchronizer()
        self.time_synchronnizer.add_time_offset_ms_sample(1000)
        self.ob_data_source = ArchaxAPIOrderBookDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            throttler=self.throttler,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            time_synchronizer=self.time_synchronnizer)

        self._original_full_order_book_reset_time = self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.ob_data_source.logger().setLevel(1)
        self.ob_data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({1: self.trading_pair}))

    def tearDown(self) -> None:
        self.async_task and self.async_task.cancel()
        self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @aioresponses()
    def test_request_order_book_snapshot(self, mock_api):
        ret = self.async_run_with_timeout(
            coroutine=self.ob_data_source._request_order_book_snapshot(self.trading_pair)
        )

        self.assertEqual(ret, {})

    def async_return(self, result):
        f = asyncio.Future()
        f.set_result(result)
        return f

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_get_last_traded_prices(self, ws_connect_mock):
        self.connector._last_prices[self.trading_pair] = 1234.56
        ltp = self.async_run_with_timeout(coroutine=self.ob_data_source.get_last_traded_prices([self.trading_pair]))
        self.assertEqual({self.trading_pair: 1234.56}, ltp)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_order_book_snapshot(self, ws_connect_mock):
        snapshot = self.async_run_with_timeout(coroutine=self.ob_data_source._order_book_snapshot(self.trading_pair))
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot.type)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_subscribes_to_trades_and_depth(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        auth_response = {
            "action": "user-login-success",
            "data": [],
            "description": "Authorisation successful",
            "status": "OK",
            "type": "user-login"
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(auth_response))

        auth_message = {
            "action": "login",
            "service": "core",
            "token": "jwt"
        }

        self.auth.generate_ws_authentication_message = MagicMock(return_value=self.async_return(auth_message))

        self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(1, len(sent_subscription_messages))
        self.assertEqual(auth_message, sent_subscription_messages[0])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_subscribes_to_channels(self, ws_connect_mock):
        self.connector._mapping[1] = self.trading_pair

        ws_connect_mock.send = AsyncMock()

        self.async_run_with_timeout(coroutine=self.ob_data_source._subscribe_channels(ws_connect_mock))

        self.assertEqual(3, ws_connect_mock.send.await_count)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_process_diff_message(self, ws_connect_mock):
        self.connector._mapping[1] = self.trading_pair

        market_depth = {
            "status": "OK",
            "action": "set-market-depths",
            "data": {
                "1": {
                    "instrumentId": 1,
                    "buy": {
                        "1200": 120
                    },
                    "sell": {}
                }
            },
            "type": "market-depths",
            "timestamp": "2021-07-01T05:17:10.691Z"
        }

        async def async_generator_side_effect():
            yield WSResponse(data = market_depth)

        ws_connect_mock.iter_messages = MagicMock()
        ws_connect_mock.iter_messages.side_effect = async_generator_side_effect

        mock_diff_queue = MagicMock()

        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_diff_queue

        mock_diff_queue.put_nowait = MagicMock()

        self.async_run_with_timeout(coroutine=self.ob_data_source._process_ws_messages(ws_connect_mock))

        mock_diff_queue.put_nowait.assert_called()

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_process_trade_message(self, ws_connect_mock):
        self.connector._mapping[1] = self.trading_pair

        trade = {
            "status": "OK",
            "action": "append-trade-histories/1",
            "data": {
                "1": [
                    {
                        "amount": "45",
                        "created": "1619080206756725534",
                        "price": "9127",
                        "side": "sell",
                        "tradeRef": "4"
                    }
                ]
            },
            "type": "trade-histories",
            "timestamp": "2021-08-27T15:35:59.147Z"
        }

        async def async_generator_side_effect():
            yield WSResponse(data = trade)

        ws_connect_mock.iter_messages = MagicMock()
        ws_connect_mock.iter_messages.side_effect = async_generator_side_effect

        mock_trade_queue = MagicMock()

        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_trade_queue

        mock_trade_queue.put_nowait = MagicMock()

        self.async_run_with_timeout(coroutine=self.ob_data_source._process_ws_messages(ws_connect_mock))

        mock_trade_queue.put_nowait.assert_called()

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_process_instruments_message(self, ws_connect_mock):
        self.connector._mapping[1] = self.trading_pair

        instrument_msg = {
            "status": "OK",
            "action": "set-instruments",
            "data": {
                "4": {
                    "currency": "USD",
                    "decimals": 2,
                    "id": 4,
                    "priceDecimalPlaces": 2,
                    "quantityDecimalPlaces": 0,
                    "symbol": "ETH",
                    "type": "crypto",
                }
            },
            "type": "instruments",
            "timestamp": "2021-08-28T15:35:59.147Z"
        }

        async def async_generator_side_effect():
            yield WSResponse(data = instrument_msg)

        ws_connect_mock.iter_messages = MagicMock()
        ws_connect_mock.iter_messages.side_effect = async_generator_side_effect

        self.async_run_with_timeout(coroutine=self.ob_data_source._process_ws_messages(ws_connect_mock))

        self.assertEqual(1, len(self.ob_data_source._id_to_pair_mapping))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.archax.archax_api_order_book_data_source.ArchaxAPIOrderBookDataSource._time")
    def test_listen_for_subscriptions_sends_ping_message_before_ping_interval_finishes(
            self,
            time_mock,
            ws_connect_mock):

        time_mock.side_effect = [1000, 1000 + CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL, 1001 + CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL, 1002 + CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL]  # Simulate first ping interval is already due

        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        auth_response = {
            "action": "user-login-success",
            "data": [],
            "description": "Authorisation successful",
            "status": "OK",
            "type": "user-login"
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(auth_response))

        auth_message = {
            "action": "login",
            "service": "core",
            "token": "jwt"
        }

        self.auth.generate_ws_authentication_message = MagicMock(return_value=self.async_return(auth_message))

        self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)
        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        expected_ping_message = {
            "action": "ping"
        }
        self.assertEqual(expected_ping_message, sent_messages[-1])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    def test_listen_for_subscriptions_raises_cancel_exception(self, _, ws_connect_mock):
        ws_connect_mock.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    def test_listen_for_subscriptions_logs_exception_details(self, sleep_mock, ws_connect_mock):
        sleep_mock.side_effect = asyncio.CancelledError
        ws_connect_mock.side_effect = Exception("TEST ERROR.")

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds..."))

    def test_listen_for_trades_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_trades_logs_exception(self):
        incomplete_resp = {
            "status": "OK",
            "action": "set-trade-histories/1",
            "timestamp": "2021-08-27T14:39:46.820Z"
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public trade updates from exchange"))

    def test_listen_for_trades_instrument_not_mapped(self):
        mock_queue = AsyncMock()

        trade_id = "1"
        timestamp = 1619080206756725504

        trade_event = {
            "status": "OK",
            "action": "set-trade-histories/1",
            "data": {
                "1": {
                    "amount": "81",
                    "created": timestamp,
                    "price": "9124",
                    "side": "sell",
                    "tradeRef": trade_id
                }
            },
            "type": "trade-histories",
            "timestamp": "2021-08-27T14:39:46.820Z"
        }

        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        timedout = False
        try:
            self.async_run_with_timeout(msg_queue.get())
        except asyncio.exceptions.TimeoutError:
            timedout = True

        self.assertTrue(timedout)

    def test_listen_for_trades_instrument_not_subscribed(self):
        mock_queue = AsyncMock()

        trade_id = "1"
        timestamp = 1619080206756725504

        trade_event = {
            "status": "OK",
            "action": "set-trade-histories/2",
            "data": {
                "2": {
                    "amount": "81",
                    "created": timestamp,
                    "price": "9124",
                    "side": "sell",
                    "tradeRef": trade_id
                }
            },
            "type": "trade-histories",
            "timestamp": "2021-08-27T14:39:46.820Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                },
                "2": {
                    "currency": "HBOT",
                    "id": 2,
                    "symbol": "APP",
                    "type": "crypto"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        timedout = False
        try:
            self.async_run_with_timeout(msg_queue.get())
        except asyncio.exceptions.TimeoutError:
            timedout = True

        self.assertTrue(timedout)

    def test_listen_for_trades_no_trading_rule(self):
        mock_queue = AsyncMock()

        trade_id = "1"
        timestamp = 1619080206756725504

        trade_event = {
            "status": "OK",
            "action": "set-trade-histories/1",
            "data": {
                "1": {
                    "amount": "81",
                    "created": timestamp,
                    "price": "9124",
                    "side": "sell",
                    "tradeRef": trade_id
                }
            },
            "type": "trade-histories",
            "timestamp": "2021-08-27T14:39:46.820Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        self.connector._decimal_map = {}

        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        timedout = False
        try:
            self.async_run_with_timeout(msg_queue.get())
        except asyncio.exceptions.TimeoutError:
            timedout = True

        self.assertTrue(timedout)

    def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()

        trade_id = "1"
        timestamp = 1619080206756725504

        trade_event = {
            "status": "OK",
            "action": "set-trade-histories/1",
            "data": {
                "1": {
                    "amount": "81",
                    "created": timestamp,
                    "price": "9124",
                    "side": "sell",
                    "tradeRef": trade_id
                }
            },
            "type": "trade-histories",
            "timestamp": "2021-08-27T14:39:46.820Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        self.connector._decimal_map = {
            self.trading_pair: (100, 1000000)
        }

        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)

        self.assertTrue(trade_id, msg.trade_id)
        self.assertTrue(timestamp, msg.timestamp)
        self.assertTrue(self.trading_pair, msg.trading_pair)

    def test_listen_for_trades_successful_list(self):
        mock_queue = AsyncMock()

        trade_id = "1"
        timestamp = 1619080206756725504

        trade_event = {
            "status": "OK",
            "action": "set-trade-histories/1",
            "data": {
                "1": [{
                    "amount": "81",
                    "created": timestamp,
                    "price": "9124",
                    "side": "sell",
                    "tradeRef": trade_id
                }]
            },
            "type": "trade-histories",
            "timestamp": "2021-08-27T14:39:46.820Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        self.connector._decimal_map = {
            self.trading_pair: (100, 1000000)
        }

        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)

        self.assertTrue(trade_id, msg.trade_id)
        self.assertTrue(timestamp, msg.timestamp)
        self.assertTrue(self.trading_pair, msg.trading_pair)

    def test_listen_for_order_book_diffs_cancelled(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_order_book_diffs_logs_exception(self):
        incomplete_resp = {
            "status": "OK",
            "action": "set-market-depths",
            "dat": {
                "1": {
                    "buy": {
                        "1200": 100,
                        "1300": 50
                    },
                    "indicativePrice": "",
                    "indicativeVolume": "",
                    "instrumentId": 1,
                    "sell": {
                        "1310": 20,
                        "1320": 50
                    }
                }
            },
            "timestamp": "2021-07-01T05:17:00.691Z"
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.ob_data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public order book updates from exchange"))

    def test_listen_for_order_book_diffs_successful(self):
        mock_queue = AsyncMock()
        diff_event = {
            "status": "OK",
            "action": "set-market-depths",
            "data": {
                "1": {
                    "buy": {
                        "1200": 100,
                        "1300": 50
                    },
                    "indicativePrice": "",
                    "indicativeVolume": "",
                    "instrumentId": 1,
                    "sell": {
                        "1310": 20,
                        "1320": 50
                    }
                }
            },
            "type": "market-depths",
            "timestamp": "2021-07-01T05:17:00.691Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        self.connector._decimal_map = {
            self.trading_pair: (100, 1000000)
        }

        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(self.trading_pair, msg.trading_pair)
        self.assertEqual(OrderBookMessageType.DIFF, msg.type)

        for bid_zip in zip(msg.bids, [[12.00, 0.0001], [13.00, 0.00005]]):
            self.assertEqual([bid_zip[0].price, bid_zip[0].amount], bid_zip[1])

        for bid_zip in zip(msg.asks, [[13.10, 0.00002], [13.20, 0.00005]]):
            self.assertEqual([bid_zip[0].price, bid_zip[0].amount], bid_zip[1])

    def test_listen_for_order_book_diffs_instrument_not_mapped(self):
        mock_queue = AsyncMock()
        diff_event = {
            "status": "OK",
            "action": "set-market-depths",
            "data": {
                "1": {
                    "buy": {
                        "1200": 100,
                        "1300": 50
                    },
                    "indicativePrice": "",
                    "indicativeVolume": "",
                    "instrumentId": 1,
                    "sell": {
                        "1310": 20,
                        "1320": 50
                    }
                }
            },
            "type": "market-depths",
            "timestamp": "2021-07-01T05:17:00.691Z"
        }

        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        timedout = False
        try:
            self.async_run_with_timeout(msg_queue.get())
        except asyncio.exceptions.TimeoutError:
            timedout = True

        self.assertTrue(timedout)

    def test_listen_for_order_book_diffs_instrument_not_subscribed(self):
        mock_queue = AsyncMock()
        diff_event = {
            "status": "OK",
            "action": "set-market-depths",
            "data": {
                "2": {
                    "buy": {
                        "1200": 100,
                        "1300": 50
                    },
                    "indicativePrice": "",
                    "indicativeVolume": "",
                    "instrumentId": 2,
                    "sell": {
                        "1310": 20,
                        "1320": 50
                    }
                }
            },
            "type": "market-depths",
            "timestamp": "2021-07-01T05:17:00.691Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                },
                "2": {
                    "currency": "HBOT",
                    "id": 2,
                    "symbol": "ETH",
                    "type": "crypto"
                },
                "3": {
                    "currency": "HBOT",
                    "id": 3,
                    "symbol": "APP",
                    "type": "security"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        timedout = False
        try:
            self.async_run_with_timeout(msg_queue.get())
        except asyncio.exceptions.TimeoutError:
            timedout = True

        self.assertTrue(timedout)

    def test_listen_for_order_book_diffs_no_trading_rule(self):
        mock_queue = AsyncMock()
        diff_event = {
            "status": "OK",
            "action": "set-market-depths",
            "data": {
                "1": {
                    "buy": {
                        "1200": 100,
                        "1300": 50
                    },
                    "indicativePrice": "",
                    "indicativeVolume": "",
                    "instrumentId": 1,
                    "sell": {
                        "1310": 20,
                        "1320": 50
                    }
                }
            },
            "type": "market-depths",
            "timestamp": "2021-07-01T05:17:00.691Z"
        }

        instrument_mapping = {
            "data": {
                "1": {
                    "currency": "HBOT",
                    "id": 1,
                    "symbol": "COINALPHA",
                    "type": "crypto"
                }
            }
        }
        self.ob_data_source._updateInstrumentMapping(instrument_mapping)

        self.connector._decimal_map = {}

        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.ob_data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.ob_data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        timedout = False
        try:
            self.async_run_with_timeout(msg_queue.get())
        except asyncio.exceptions.TimeoutError:
            timedout = True

        self.assertTrue(timedout)
