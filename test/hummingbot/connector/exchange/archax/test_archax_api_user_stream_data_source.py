import asyncio
import json
import unittest
from typing import Awaitable, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.archax import archax_constants as CONSTANTS, archax_web_utils as web_utils
from hummingbot.connector.exchange.archax.archax_api_user_stream_data_source import ArchaxAPIUserStreamDataSource
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class TestArchaxAPIUserStreamDataSource(unittest.TestCase):
    # the level is required to receive logs from the data source logger
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
        cls.api_key = "someKey"
        cls.api_passphrase = "somePassPhrase"
        cls.api_secret_key = "someSecretKey"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()
        self.auth = MagicMock()

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        self.api_factory = web_utils.build_api_factory(
            throttler=self.throttler,
            time_synchronizer=self.mock_time_provider,
            auth=self.auth)

        self.data_source = ArchaxAPIUserStreamDataSource(
            auth=self.auth,
            domain=self.domain,
            api_factory=self.api_factory,
            throttler=self.throttler,
            time_synchronizer=self.mock_time_provider)

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def async_return(self, result):
        f = asyncio.Future()
        f.set_result(result)
        return f

    def test_last_recv_time(self):
        # Initial last_recv_time
        self.assertEqual(0, self.data_source.last_recv_time)

        ws_assistant = self.async_run_with_timeout(self.data_source._get_ws_assistant())
        ws_assistant._connection._last_recv_time = 1000
        self.assertEqual(1000, self.data_source.last_recv_time)

    @patch("hummingbot.connector.exchange.archax.archax_auth.ArchaxAuth._time")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_auth(self, ws_connect_mock, auth_time_mock):
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

        output_queue = asyncio.Queue()

        self.auth.generate_ws_authentication_message = MagicMock(return_value=self.async_return(auth_message))

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(1, len(sent_subscription_messages))
        self.assertEqual(auth_message, sent_subscription_messages[0])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_does_not_queue_pong_payload(self, mock_ws):
        mock_pong = {
            "pong": "1545910590801"
        }

        auth_message = {
            "action": "login",
            "service": "core",
            "token": "jwt"
        }

        self.auth.generate_ws_authentication_message = MagicMock(return_value=self.async_return(auth_message))

        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, json.dumps(mock_pong))

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("hummingbot.connector.exchange.archax.archax_auth.ArchaxAuth._time")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_auth_failed_throws_exception(self, ws_connect_mock, auth_time_mock):
        auth_time_mock.side_effect = [100]
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        result_auth = {'auth': 'fail', 'userId': 24068148}
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_auth))

        output_queue = asyncio.Queue()

        auth_message = {
            "action": "login",
            "service": "core",
            "token": "jwt"
        }

        self.auth.generate_ws_authentication_message = MagicMock(return_value=self.async_return(auth_message))

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=output_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(1, len(sent_subscription_messages))
        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    def test_listen_for_user_stream_iter_message_throws_exception(self, sleep_mock, mock_ws):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.return_value.receive.side_effect = Exception("TEST ERROR")
        sleep_mock.side_effect = asyncio.CancelledError  # to finish the task execution

        try:
            self.async_run_with_timeout(self.data_source.listen_for_user_stream(msg_queue))
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.archax.archax_api_user_stream_data_source.ArchaxAPIUserStreamDataSource"
           "._time")
    def test_listen_for_user_stream_sends_ping_message_before_ping_interval_finishes(
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

        output_queue = asyncio.Queue()

        self.auth.generate_ws_authentication_message = MagicMock(return_value=self.async_return(auth_message))

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=output_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        expected_ping_message = {
            "action": "ping",
        }
        self.assertEqual(expected_ping_message, sent_messages[-1])
