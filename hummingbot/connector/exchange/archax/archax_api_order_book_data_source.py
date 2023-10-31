import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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

if TYPE_CHECKING:
    from hummingbot.connector.exchange.archax.archax_exchange import ArchaxExchange


class ArchaxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0

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
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
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
        # not supported
        return {}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {"trading_pair": [trading_pair], "update_id": 1, "bids": [], "asks": []}, timestamp=1)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        for trade in raw_message["data"]:
            instrument_id = int(trade)

            trading_pair = self._id_to_pair_mapping.get(instrument_id)
            if trading_pair is None:
                continue

            if self._trading_pairs.count(trading_pair) == 0:
                continue

            px_decimal, qty_decimal = self._connector._decimal_map.get(trading_pair)
            if px_decimal is None or qty_decimal is None:
                self.logger().error(f"No trading rule for {trading_pair}")
                continue

            trade_object = raw_message["data"][trade]
            if isinstance(trade_object, list):
                for tx in trade_object:
                    trade_message: OrderBookMessage = ArchaxOrderBook.trade_message_from_exchange(tx, px_decimal, qty_decimal, {"trading_pair": trading_pair})
                    message_queue.put_nowait(trade_message)
            else:
                trade_message: OrderBookMessage = ArchaxOrderBook.trade_message_from_exchange(trade_object, px_decimal, qty_decimal, {"trading_pair": trading_pair})
                message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        for item in raw_message["data"]:
            instrument_data = raw_message["data"][item]
            instrument_id = int(item)
            if instrument_id not in self._id_to_pair_mapping:
                continue

            trading_pair = self._id_to_pair_mapping[instrument_id]
            if self._trading_pairs.count(trading_pair) == 0:
                continue

            px_decimal, qty_decimal = self._connector._decimal_map.get(trading_pair)
            if px_decimal is None or qty_decimal is None:
                self.logger().error(f"No trading rule for {trading_pair}")
                continue

            order_book_message: OrderBookMessage = ArchaxOrderBook.diff_message_from_exchange(instrument_data, px_decimal, qty_decimal, time.time() * 1e3, {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        This method runs continuously and request the full order book content from the exchange every hour.
        The method uses the REST API from the exchange because it does not provide an endpoint to get the full order
        book through websocket. With the information creates a snapshot messages that is added to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        # not supported
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
                await ws.connect(ws_url=CONSTANTS.WSS_V2_PUBLIC_URL[self._domain])
                await self._authenticate_connection(ws)
                self._last_ws_message_sent_timestamp = self._time()

                while True:
                    try:
                        seconds_until_next_ping = (CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL - (
                            self._time() - self._last_ws_message_sent_timestamp))
                        await asyncio.wait_for(self._process_ws_messages(ws=ws), timeout=seconds_until_next_ping)
                    except asyncio.TimeoutError:
                        ping_time = self._time()
                        payload = {
                            "action": "ping"
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

    async def _authenticate_connection(self, ws: WSAssistant):
        try:
            payload = await self._auth.generate_ws_authentication_message()
            auth_message: WSJSONRequest = WSJSONRequest(payload=payload)
            await ws.send(auth_message)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred authenticating wss connection...",
                exc_info=True
            )
            raise

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            actions = ["subscribe-instruments", "subscribe-market-depths"]

            while len(self._connector._mapping.keys()) == 0:
                await self._sleep(1.0)

            for trading_pair in self._trading_pairs:
                instrument_id = self._connector._mapping.inverse[trading_pair]
                actions.append(f'subscribe-trade-histories/{instrument_id}')

            for action in actions:
                payload = {
                    "action": action
                }
                request: WSJSONRequest = WSJSONRequest(payload=payload)
                await ws.send(request)

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
            self.logger().debug(f"MD data: {data}")
            event_type = data.get("type")
            if event_type == CONSTANTS.LOGIN_EVENT_TYPE:
                await self._subscribe_channels(ws)
            elif event_type == CONSTANTS.INSTRUMENT_EVENT_TYPE:
                self._updateInstrumentMapping(data)
            elif event_type == CONSTANTS.DIFF_EVENT_TYPE:
                self._message_queue[CONSTANTS.DIFF_EVENT_TYPE].put_nowait(data)
            elif event_type == CONSTANTS.TRADE_EVENT_TYPE:
                self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(data)

    def _time(self):
        return time.time()

    def _updateInstrumentMapping(self, data: Dict[str, Any]):
        for instrument_id in data["data"]:
            instrument = data["data"][instrument_id]
            if instrument["type"] != "crypto":
                continue
            self._id_to_pair_mapping[instrument["id"]] = f'{instrument["symbol"]}-{instrument["currency"]}'
