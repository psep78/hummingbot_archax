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
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, MarketOrderFailureEvent
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

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
        self._archax_email = archax_email
        self._archax_password = archax_password
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._mapping = bidict()
        self._ws: Optional[WSAssistant] = None
        self._user_stream: ArchaxAPIUserStreamDataSource
        self._order_id_map: Dict[str, str] = dict()
        self._decimal_map: Dict[str, tuple] = dict()
        self._order_cache: Dict[str, Any] = dict()
        self._order_cache_updated = False
        self._execution_cache = {}
        self._last_prices: Dict[str, Decimal] = dict()
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
            archax_email=self._archax_email,
            archax_password=self._archax_password,
            domain=self._domain)

    @property
    def name(self) -> str:
        if self._domain == "archax_main":
            return "archax"
        else:
            return self._domain

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
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

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

        payload = {
            "action": "order-submit",
            "data": {
                "actionRef": f"{order_id}",
                "instrumentId": f"{instrument_id}",
                "limitPrice": f"{price:f}",
                "orderType": CONSTANTS.ORDER_TYPE_LIMIT if order_type is OrderType.LIMIT else CONSTANTS.ORDER_TYPE_MARKET,
                "quantity": f"{amount:f}",
                "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
                "organisationId": self._auth.primary_org
            }
        }

        ws = await self._user_stream._get_ws_assistant()
        request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(request)

        self.logger().debug(f"Sent order: {payload}")

        return ("", time.time() * 1e3)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        payload = {
            "action": "order-cancel",
            "data": {
                "actionRef": f"{order_id}c",
                "orderId": f"{ tracked_order.exchange_order_id}",
                "organisationId": self._auth.primary_org
            }
        }

        ws = await self._user_stream._get_ws_assistant()
        if ws._connection.connected is False:
            return False

        request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(request)

        self.logger().debug(f"Sent cancel: {payload}")

        return False

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
        # not supported
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for data in self._iter_user_event_queue():
            try:
                evt_type = data["type"]
                if evt_type == CONSTANTS.BALANCE_EVENT_TYPE:
                    self.process_balance_update(data)
                elif evt_type == CONSTANTS.NOTIFICATIONS_EVENT_TYPE:
                    self.process_order_notification(data)
                elif evt_type == CONSTANTS.ORDERS_EVENT_TYPE:
                    self.process_orders(data)
                elif evt_type == CONSTANTS.QUOTES_EVENT_TYPE:
                    self.process_quotes(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    def process_order_notification(self, data: Dict[str, Any]):
        action = data["action"]
        if action == CONSTANTS.ORDER_SUBMITTED_EVENT_TYPE:
            self.process_order_submit_notification(data)
        elif action == CONSTANTS.ORDER_UPDATED_EVENT_TYPE:
            self.process_order_update_notification(data)
        elif action == CONSTANTS.ORDER_CANCELLED_EVENT_TYPE:
            self.process_order_cancel_success_notification(data)
        elif action in [CONSTANTS.ORDER_CANCEL_REJECTED_EVENT_TYPE, CONSTANTS.ORDER_CANCEL_FAILED_EVENT_TYPE]:
            self.process_order_cancel_rejected_notification(data)

    def process_quotes(self, data: Dict[str, Any]):
        if "status" not in data or data["status"] != "OK":
            return
        for instrument in data["data"]:
            instrument_details = data["data"][instrument]
            if "lastPrice" not in instrument_details:
                continue
            trading_pair = self._mapping.get(int(instrument))
            if trading_pair is None:
                continue
            px_decimal, _ = self._decimal_map.get(trading_pair)
            self._last_prices[trading_pair] = Decimal(instrument_details["lastPrice"]) / px_decimal

    def process_orders(self, data: Dict[str, Any]):
        self.logger().debug(f"Order data: {data}")
        if "status" not in data or data["status"] != "OK":
            return

        for archax_order in data["data"]:
            order_details = data["data"][archax_order]
            self._order_cache[archax_order] = order_details
            if "actionRef" in order_details:
                self._order_cache[order_details["actionRef"]] = order_details
            hbt_order_id = self._order_id_map.get(archax_order)
            if hbt_order_id is None:
                continue
            tracked_order = self._order_tracker.active_orders.get(hbt_order_id)
            if tracked_order is None:
                self.logger().error(f"No tracked order found for: {hbt_order_id}")
                continue

            if "executions" in order_details:
                self.process_order_executions(order_details, tracked_order, lambda trade_update: self._order_tracker.process_trade_update(trade_update))

            if "orderStatus" in order_details:
                current_status = CONSTANTS.ORDER_STATE[order_details["orderStatus"]]
                if current_status == OrderState.PENDING_CREATE:
                    continue

                order_update = OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=current_status,
                    exchange_order_id=archax_order
                )
                self._order_tracker.process_order_update(order_update=order_update)
        self._order_cache_updated = True

    def fill_trade_data(self, exec_details: Dict[str, Any], cached_exec: Dict[str, Any], px_decimal: Decimal, qty_decimal: Decimal):
        if "price" in exec_details:
            cached_exec["price"] = Decimal(exec_details["price"]) / px_decimal
        if "quantity" in exec_details:
            cached_exec["quantity"] = Decimal(exec_details["quantity"]) / qty_decimal
        if "created" in exec_details:
            cached_exec["created"] = exec_details["created"]
        if "commission" in exec_details and int(exec_details["commission"]) > 0:
            cached_exec["commission"] = int(exec_details["commission"])
        if "tax" in exec_details:
            cached_exec["tax"] = int(exec_details["tax"])

    def process_order_executions(self, order_details: Dict[str, Any], tracked_order: InFlightOrder, on_trade_update):
        for execution in order_details["executions"]:
            px_decimal, qty_decimal = self._decimal_map.get(tracked_order.trading_pair)
            exec_details = order_details["executions"][execution]

            if execution not in self._execution_cache:
                self._execution_cache[execution] = {}

            cached_exec = self._execution_cache.get(execution)
            if "done" in cached_exec and cached_exec["done"] is True:
                continue

            self.fill_trade_data(exec_details, cached_exec, px_decimal, qty_decimal)

            if len(cached_exec) != 5:
                continue

            self.logger().debug(f"Trade info: {cached_exec}")
            _, quote = split_hb_trading_pair(tracked_order.trading_pair)
            total_fee = (Decimal(cached_exec["commission"]) + Decimal(cached_exec["tax"])) / px_decimal
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=tracked_order.trade_type,
                flat_fees=[TokenAmount(amount=total_fee, token=quote)],
            )
            trade_update = TradeUpdate(
                trade_id=str(execution),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fee=fee,
                fill_base_amount=cached_exec["quantity"],
                fill_quote_amount= cached_exec["price"] * cached_exec["quantity"],
                fill_price= cached_exec["price"],
                fill_timestamp=float(cached_exec["created"]),
            )
            on_trade_update(trade_update)
            cached_exec["done"] = True

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
        hbt_order_id = data["data"]["actionRef"]
        self._order_cache[hbt_order_id] = data["data"]
        self.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(self.current_timestamp, hbt_order_id, OrderType.LIMIT))
        self.logger().warn(f'Order {hbt_order_id} submit error: {data["data"]["error"]}')

    def process_order_update_notification(self, data: Dict[str, Any]):
        now = time.time() * 1e3
        status = data["status"]
        hbt_order_id = data["data"]["actionRef"]
        archax_order = data["data"]["orderId"]
        self._order_cache[hbt_order_id] = data["data"]
        self._order_id_map[archax_order] = hbt_order_id
        if status == "OK":
            self.logger().info(f'Order {hbt_order_id} submit OK: {data["data"]}')
            current_status = CONSTANTS.ORDER_STATE[data["data"]["status"]]
            if current_status == OrderState.PENDING_CREATE:
                return

            if current_status == OrderState.FAILED:
                self.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(self.current_timestamp, hbt_order_id, OrderType.LIMIT))
                return

            tracked_order = self._order_tracker.active_orders.get(hbt_order_id)
            if tracked_order is None:
                return

            tracked_order.update_exchange_order_id(archax_order)
            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=current_status,
                exchange_order_id=archax_order
            )
            self._order_tracker.process_order_update(order_update=order_update)
        else:
            self.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(now, hbt_order_id, OrderType.LIMIT))
            self.logger().warn(f'Order {hbt_order_id} submit error: {data["data"]["error"]}')

    def process_order_cancel_success_notification(self, data: Dict[str, Any]):
        status = data["status"]
        hbt_order_id = str(data["data"]["actionRef"]).removesuffix("c")
        self._order_cache[hbt_order_id] = data["data"]
        if status == "OK":
            current_status = CONSTANTS.ORDER_STATE[data["data"]["status"]]
            if current_status != OrderState.CANCELED:
                self.logger().error(f"Unexpected status {current_status} found in order-cancelled")
                return
            tracked_order = self._order_tracker.active_orders.get(hbt_order_id)
            if tracked_order is None:
                return

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=current_status,
                exchange_order_id=data["data"]["actionRef"]
            )
            self._order_tracker.process_order_update(order_update=order_update)
        else:
            self.logger().error(f"Unexpected status: {status} in {data}")

    def process_order_cancel_rejected_notification(self, data: Dict[str, Any]):
        status = data["status"]
        if status != "ERROR":
            self.logger().error(f"Unexpected status: {status} in {data}")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        while self._order_cache_updated is False:
            await self._sleep(0.1)

        self.logger().debug(f"Trade update order cache: {self._order_cache}")

        self.logger().debug(f"Trade update req: {order.client_order_id} / {order.exchange_order_id}")

        if order.exchange_order_id is not None:
            order_details = self._order_cache.get(order.exchange_order_id)
            if order_details is not None and "executions" in order_details:
                self.process_order_executions(order_details, order, on_trade_update=lambda trade_update: trade_updates.append(trade_update))

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        while self._order_cache_updated is False:
            await self._sleep(0.1)

        self.logger().debug(f"Req order: order cache: {self._order_cache}")
        self.logger().debug(f"Req order status: {tracked_order.client_order_id} / {tracked_order.exchange_order_id}")

        order = self._order_cache.get(tracked_order.exchange_order_id)
        if order is None:
            order = self._order_cache.get(tracked_order.client_order_id)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=time.time() * 1e3,
            new_state= OrderState.FAILED if order is None else CONSTANTS.ORDER_STATE[order["orderStatus"]],
        )

        return order_update

    async def _update_balances(self):
        # not supported
        pass

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        for symbol_data in filter(archax_utils.is_exchange_information_valid, exchange_info["data"]):
            self._mapping[symbol_data["id"]] = combine_to_hb_trading_pair(base=symbol_data["symbol"], quote=symbol_data["currency"])
        self._set_trading_pair_symbol_map(self._mapping)

    def get_last_traded_price(self, trading_pair: str) -> float:
        ltp = self._last_prices.get(trading_pair)
        return ltp if ltp is not None else 0.0

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return self.get_last_traded_price(trading_pair)

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
