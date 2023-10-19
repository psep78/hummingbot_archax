import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.exchange.archax.archax_constants as CONSTANTS
from hummingbot.connector.exchange.archax import archax_web_utils as web_utils
from hummingbot.connector.exchange.archax.archax_auth import ArchaxAuth
from hummingbot.connector.exchange.archax.archax_order_book import ArchaxOrderBook
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.archax.archax_exchange import ArchaxExchange


class ArchaxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(self,
                 auth: ArchaxAuth,
                 trading_pairs: List[str],
                 connector: 'ArchaxExchange',
                 api_factory: Optional[WebAssistantsFactory] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 throttler: Optional[AsyncThrottler] = None,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__(trading_pairs)
        self._auth: ArchaxAuth = auth
        self._connector = connector
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._time_synchronizer = time_synchronizer
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
        )
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._last_ws_message_sent_timestamp = 0
        self._id_to_pair_mapping = {}

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        return {}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {"trading_pair": [trading_pair], "update_id": 1, "bids": [], "asks": []}, timestamp=1)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["symbol"])
        for trades in raw_message["data"]:
            trade_message: OrderBookMessage = ArchaxOrderBook.trade_message_from_exchange(
                trades, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        for item in raw_message["data"]:
            instrument_data = raw_message["data"][item]
            instrument_id = instrument_data["instrumentId"]
            if instrument_id not in self._id_to_pair_mapping:
                continue

            trading_pair = self._id_to_pair_mapping[instrument_id]
            if self._trading_pairs.count(trading_pair) == 0:
                continue

            order_book_message: OrderBookMessage = ArchaxOrderBook.diff_message_from_exchange(instrument_data, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        This method runs continuously and request the full order book content from the exchange every hour.
        The method uses the REST API from the exchange because it does not provide an endpoint to get the full order
        book through websocket. With the information creates a snapshot messages that is added to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        pass

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=CONSTANTS.WSS_V1_PUBLIC_URL[self._domain])
                await self._subscribe_channels(ws)
                self._last_ws_message_sent_timestamp = self._time()

                while True:
                    try:
                        seconds_until_next_ping = (CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL - (
                            self._time() - self._last_ws_message_sent_timestamp))
                        await asyncio.wait_for(self._process_ws_messages(ws=ws), timeout=seconds_until_next_ping)
                    except asyncio.TimeoutError:
                        ping_time = self._time()
                        payload = {
                            "ping": int(ping_time * 1e3)
                        }
                        ping_request = WSJSONRequest(payload=payload)
                        await ws.send(request=ping_request)
                        self._last_ws_message_sent_timestamp = ping_time
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            payload = await self._auth.generate_ws_authentication_message()
            auth_message: WSJSONRequest = WSJSONRequest(payload=payload)
            await ws.send(auth_message)

            instruments_payload = {
                "action": "subscribe-instruments"
            }
            instruments_payload_request: WSJSONRequest = WSJSONRequest(payload=instruments_payload)

            mq_payload = {
                "action": "subscribe-transactions"
            }
            mq_payload_request: WSJSONRequest = WSJSONRequest(payload=mq_payload)

            md_payload = {
                "action": "subscribe-market-depths"
            }
            md_payload_request: WSJSONRequest = WSJSONRequest(payload=md_payload)

            await ws.send(instruments_payload_request)
            await ws.send(mq_payload_request)
            await ws.send(md_payload_request)

            self.logger().info("Subscribed to public order book and trade channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _process_ws_messages(self, ws: WSAssistant):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            # print(data)
            if data.get("msg") == "Success":
                continue
            event_type = data.get("type")
            if event_type == CONSTANTS.LOGIN_EVENT_TYPE:
                pass  # self._subscribe_channels()
            elif event_type == CONSTANTS.INSTRUMENT_EVENT_TYPE:
                self._updateInstrumentMapping(data)
            elif event_type == CONSTANTS.DIFF_EVENT_TYPE:
                if data.get("data"):
                    self._message_queue[CONSTANTS.DIFF_EVENT_TYPE].put_nowait(data)  # SNAPSHOT_EVENT_TYPE
                else:
                    self._message_queue[CONSTANTS.DIFF_EVENT_TYPE].put_nowait(data)
            elif event_type == CONSTANTS.TRADE_EVENT_TYPE:
                self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(data)

    # async def _process_ob_snapshot(self, snapshot_queue: asyncio.Queue):
    #     message_queue = self._message_queue[CONSTANTS.SNAPSHOT_EVENT_TYPE]
    #     while True:
    #         try:
    #             json_msg = await message_queue.get()
    #             trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
    #                 symbol=json_msg["symbol"])
    #             order_book_message: OrderBookMessage = ArchaxOrderBook.snapshot_message_from_exchange_websocket(
    #                 json_msg["data"][0], json_msg["data"][0], {"trading_pair": trading_pair})
    #             snapshot_queue.put_nowait(order_book_message)
    #         except asyncio.CancelledError:
    #             raise
    #         except Exception:
    #             self.logger().error("Unexpected error when processing public order book updates from exchange")
    #             raise

    def _time(self):
        return time.time()

    def _updateInstrumentMapping(self, data: Dict[str, Any]):
        for instrument_id in data["data"]:
            instrument = data["data"][instrument_id]
            if instrument["type"] != "crypto":
                continue
            self._id_to_pair_mapping[instrument["id"]] = f'{instrument["symbol"]}-{instrument["currency"]}'
            # print(instrument)
