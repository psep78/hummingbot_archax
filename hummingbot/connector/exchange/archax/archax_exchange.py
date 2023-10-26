import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.exchange.archax.archax_constants as CONSTANTS
import hummingbot.connector.exchange.archax.archax_utils as archax_utils
import hummingbot.connector.exchange.archax.archax_web_utils as web_utils
from hummingbot.connector.exchange.archax.archax_api_order_book_data_source import ArchaxAPIOrderBookDataSource
from hummingbot.connector.exchange.archax.archax_api_user_stream_data_source import ArchaxAPIUserStreamDataSource
from hummingbot.connector.exchange.archax.archax_auth import ArchaxAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None
s_decimal_NaN = Decimal("nan")


class ArchaxExchange(ExchangePyBase):
    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 archax_email: str,
                 archax_password: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.archax_email = archax_email
        self.archax_password = archax_password
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._mapping = bidict()
        self._ws: Optional[WSAssistant] = None
        self._user_stream: ArchaxAPIUserStreamDataSource
        self._order_id_map: Dict[str, str] = dict()
        self._order_cancel_map: Dict[str, bool] = dict()
        self._decimal_map: Dict[str, tuple] = dict()
        self._order_cache: Dict[str, Any] = dict()
        super().__init__(client_config_map)

    @staticmethod
    def archax_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(archax_type: str) -> OrderType:
        return OrderType[archax_type]

    @property
    def authenticator(self):
        return ArchaxAuth(
            archax_email=self.archax_email,
            archax_password=self.archax_password,
            web_assistants_factory=self._create_web_assistants_factory(),
            domain=self._domain)

    @property
    def name(self) -> str:
        if self._domain == "archax_main":
            return "archax"
        else:
            return f"archax_{self._domain}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for the request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_cancel_order_not_found_in_the_exchange when replacing the
        # dummy implementation
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=None)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ArchaxAPIOrderBookDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        self._user_stream = ArchaxAPIUserStreamDataSource(
            auth=self._auth,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )
        return self._user_stream

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        trade_base_fee = build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            order_side=order_side,
            order_type=order_type,
            amount=amount,
            price=price,
            base_currency=base_currency,
            quote_currency=quote_currency
        )
        return trade_base_fee

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        instrument_id = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        px_decimal, qty_decimal = self._decimal_map.get(trading_pair)
        if px_decimal is None or qty_decimal is None:
            self.logger().error(f"No trading rule for {trading_pair}")
            raise ValueError(f"Place order failed: No trading rule for {trading_pair}")

        payload = {
            "action": "order-submit",
            "data": {
                "actionRef": f"{order_id}",
                "instrumentId": f"{instrument_id}",
                "limitPrice": int(round(price * px_decimal)),
                "orderType": CONSTANTS.ORDER_TYPE_LIMIT if order_type is OrderType.LIMIT else CONSTANTS.ORDER_TYPE_MARKET,
                "quantity": int(round(amount * qty_decimal, 0)),
                "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
                "organisationId": self._auth.primary_org
            }
        }

        ws = await self._user_stream._get_ws_assistant()
        request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(request)

        self.logger().warn(f"Sent place: {payload}")

        archax_order_id = await self.get_order_id(order_id, CONSTANTS.ORDER_PLACEMENT_TIMEOUT_MS)

        if archax_order_id == "":
            raise ValueError(f"Error submitting order {order_id}. Check logs for details")
        elif archax_order_id == "-1":
            raise ValueError(f"Timeout while waiting for order {order_id}. Check logs for details")

        return (archax_order_id, time.time())

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        payload = {
            "action": "order-cancel",
            "data": {
                "actionRef": f"{order_id}",
                "orderId": f"{ tracked_order.exchange_order_id}",
                "organisationId": self._auth.primary_org
            }
        }

        ws = await self._user_stream._get_ws_assistant()
        if ws._connection.connected is False:
            return False

        request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(request)

        self.logger().warn(f"Sent cancel: {payload}")

        return await self.get_order_cancel_status(order_id, CONSTANTS.ORDER_CANCEL_TIMEOUT_MS)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
                {
            "ret_code": 0,
            "ret_msg": "",
            "ext_code": null,
            "ext_info": null,
            "result": [
                {
                    "name": "BTCUSDT",
                    "alias": "BTCUSDT",
                    "baseCurrency": "BTC",
                    "quoteCurrency": "USDT",
                    "basePrecision": "0.000001",
                    "quotePrecision": "0.01",
                    "minTradeQuantity": "0.0001",
                    "minTradeAmount": "10",
                    "minPricePrecision": "0.01",
                    "maxTradeQuantity": "2",
                    "maxTradeAmount": "200",
                    "category": 1
                },
                {
                    "name": "ETHUSDT",
                    "alias": "ETHUSDT",
                    "baseCurrency": "ETH",
                    "quoteCurrency": "USDT",
                    "basePrecision": "0.0001",
                    "quotePrecision": "0.01",
                    "minTradeQuantity": "0.0001",
                    "minTradeAmount": "10",
                    "minPricePrecision": "0.01",
                    "maxTradeQuantity": "2",
                    "maxTradeAmount": "200",
                    "category": 1
                }
            ]
        }
        """
        trading_pair_rules = exchange_info_dict.get("data", [])
        retval = []
        for rule in trading_pair_rules:
            try:
                if archax_utils.is_exchange_information_valid(rule) is False:
                    continue

                trading_pair = f'{rule["symbol"]}-{rule["currency"]}'

                px_decimal = Decimal(pow(10, rule.get("priceDecimalPlaces")))
                qty_decimal = Decimal(pow(10, rule.get("quantityDecimalPlaces")))

                self._decimal_map[trading_pair] = (px_decimal, qty_decimal)

                min_order_size = Decimal(1.0) / Decimal(pow(10, rule.get("quantityDecimalPlaces")))
                min_price_increment = rule.get("tickSize")
                min_base_amount_increment = min_order_size
                min_notional_size = rule.get("minimumOrderValue")

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=Decimal(min_order_size),
                                min_price_increment=Decimal(min_price_increment),
                                min_base_amount_increment=Decimal(min_base_amount_increment),
                                min_notional_size=Decimal(min_notional_size)))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule.get('name')}. Skipping.")
        return retval

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for data in self._iter_user_event_queue():
            try:
                self.logger().warn(f"WSS msg: {data}")
                evt_type = data["type"]
                if evt_type == CONSTANTS.BALANCE_EVENT_TYPE:
                    self.process_balance_update(data)
                elif evt_type == CONSTANTS.NOTIFICATIONS_EVENT_TYPE:
                    action = data["action"]
                    if action == CONSTANTS.ORDER_SUBMITTED_EVENT_TYPE:
                        self.process_order_submit_notification(data)
                    elif action == CONSTANTS.ORDER_UPDATED_EVENT_TYPE:
                        self.process_order_update_notification(data)
                    elif action == CONSTANTS.ORDER_CANCELLED_EVENT_TYPE:
                        self.process_order_cancel_success_notification(data)
                    elif action == CONSTANTS.ORDER_CANCEL_REJECTED_EVENT_TYPE or action == CONSTANTS.ORDER_CANCEL_FAILED_EVENT_TYPE:
                        self.process_order_cancel_rejected_notification(data)
                elif evt_type == CONSTANTS.ORDERS_EVENT_TYPE:
                    self.process_orders(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    def process_orders(self, data: Dict[str, Any]):
        if data["status"] != "OK":
            return

        for order in data["data"]:
            self._order_cache[order] = data["data"][order]

    def process_balance_update(self, data: Dict[str, Any]):
        if data["status"] != "OK":
            return

        for balance in data["data"]:
            balance_item = data["data"][balance]
            instrument_id = balance_item["instrumentId"]
            if instrument_id not in self._mapping:
                continue
            asset_name = self._mapping[instrument_id]
            free_balance = Decimal(balance_item["available"])
            total_balance = Decimal(balance_item["total"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance

    def process_order_submit_notification(self, data: Dict[str, Any]):
        status = data["status"]
        hbt_order_id = data["data"]["actionRef"]
        self._order_cache[hbt_order_id] = data["data"]
        if status == "OK":
            self._order_id_map[hbt_order_id] = data["data"]["orderId"]
        else:
            self.logger().warn(f'Order submit error: {data["data"]["error"]}')
            self._order_id_map[hbt_order_id] = ""

    def process_order_update_notification(self, data: Dict[str, Any]):
        status = data["status"]
        hbt_order_id = data["data"]["actionRef"]
        self._order_id_map[hbt_order_id] = data["data"]["orderId"]
        self._order_cache[hbt_order_id] = data["data"]
        if status != "OK":
            self.logger().warn(f'Order {hbt_order_id} submit error: {data["data"]["error"]}')

    def process_order_cancel_success_notification(self, data: Dict[str, Any]):
        status = data["status"]
        hbt_order_id = data["data"]["actionRef"]
        self._order_cache[hbt_order_id] = data["data"]
        if status == "OK":
            self._order_cancel_map[hbt_order_id] = True
        else:
            self.logger().error(f"Unexpected status: {status}")

    def process_order_cancel_rejected_notification(self, data: Dict[str, Any]):
        status = data["status"]
        hbt_order_id = data["data"]["actionRef"]
        if status == "ERROR":
            self._order_cancel_map[hbt_order_id] = False
        else:
            self.logger().error(f"Unexpected status: {status}")

    async def get_order_id(self, hbt_order_id: str, timeout_ms: int) -> str:
        sleep_time = 100
        iterations = int(timeout_ms / sleep_time)
        for _ in range(iterations):
            archax_order_id = self._order_id_map.get(hbt_order_id)
            if archax_order_id is not None:
                return archax_order_id
            else:
                await asyncio.sleep(10 / sleep_time)

        self.logger().warn(f'Time out waiting for order: {hbt_order_id}')
        return "0"

    async def get_order_cancel_status(self, hbt_order_id: str, timeout_ms: int) -> bool:
        sleep_time = 100
        iterations = int(timeout_ms / sleep_time)
        for _ in range(iterations):
            result = self._order_cancel_map.get(hbt_order_id)
            if result is not None:
                return result
            else:
                await asyncio.sleep(10 / sleep_time)

        self.logger().warn(f'Time out waiting for cancellation: {hbt_order_id}')
        return False

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            self.logger().warn(f"Trade update req: {order.exchange_order_id}")

            # exchange_order_id = int(order.exchange_order_id)
            # trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            # all_fills_response = await self._api_get(
            #     path_url=CONSTANTS.MY_TRADES_PATH_URL,
            #     params={
            #         "symbol": trading_pair,
            #         "orderId": exchange_order_id
            #     },
            #     is_auth_required=True,
            #     limit_id=CONSTANTS.MY_TRADES_PATH_URL)
            # fills_data = all_fills_response.get("result", [])
            # if fills_data is not None:
            #     for trade in fills_data:
            #         exchange_order_id = str(trade["orderId"])
            #         fee = TradeFeeBase.new_spot_fee(
            #             fee_schema=self.trade_fee_schema(),
            #             trade_type=order.trade_type,
            #             percent_token=trade["commissionAsset"],
            #             flat_fees=[TokenAmount(amount=Decimal(trade["commission"]), token=trade["commissionAsset"])]
            #         )
            #         trade_update = TradeUpdate(
            #             trade_id=str(trade["ticketId"]),
            #             client_order_id=order.client_order_id,
            #             exchange_order_id=exchange_order_id,
            #             trading_pair=trading_pair,
            #             fee=fee,
            #             fill_base_amount=Decimal(trade["qty"]),
            #             fill_quote_amount=Decimal(trade["price"]) * Decimal(trade["qty"]),
            #             fill_price=Decimal(trade["price"]),
            #             fill_timestamp=int(trade["executionTime"]) * 1e-3,
            #         )
            #         trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order = self._order_cache.get(tracked_order.exchange_order_id)
        if order is None:
            order = self._order_cache.get(tracked_order.client_order_id)

        if order is None:
            return OrderUpdate()

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=time.time() * 1e3,
            new_state=CONSTANTS.ORDER_STATE[order["orderStatus"]],
        )

        return order_update

    async def _update_balances(self):
        # not supported
        pass

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        for symbol_data in filter(archax_utils.is_exchange_information_valid, exchange_info["data"]):
            self._mapping[symbol_data["id"]] = combine_to_hb_trading_pair(base=symbol_data["symbol"], quote=symbol_data["currency"])
        self._set_trading_pair_symbol_map(self._mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return 1.0

    def get_last_traded_price(self, trading_pair: str) -> float:
        return 3359.00

    async def _api_request(self,
                           path_url,
                           method: RESTMethod = RESTMethod.GET,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False,
                           return_err: bool = False,
                           limit_id: Optional[str] = None,
                           trading_pair: Optional[str] = None,
                           **kwargs) -> Dict[str, Any]:
        last_exception = None
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.rest_url(path_url, domain=self.domain)
        local_headers = {
            "Content-Type": "application/x-www-form-urlencoded"}
        for _ in range(2):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
                    data=data,
                    method=method,
                    is_auth_required=is_auth_required,
                    return_err=return_err,
                    headers=local_headers,
                    throttler_limit_id=limit_id if limit_id else path_url,
                )
                return request_result
            except IOError as request_exception:
                last_exception = request_exception
                if self._is_request_exception_related_to_time_synchronizer(request_exception=request_exception):
                    self._time_synchronizer.clear_time_offset_ms_samples()
                    await self._update_time_synchronizer()
                else:
                    raise

        # Failed even after the last retry
        raise last_exception
