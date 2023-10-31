from decimal import Decimal
from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class ArchaxOrderBook(OrderBook):
    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   px_decimal: Decimal,
                                   qty_decimal: Decimal,
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message with the changes in the order book received from the exchange
        :param msg: the changes in the order book
        :param timestamp: the timestamp of the difference
        :param metadata: a dictionary with extra information to add to the difference data
        :return: a diff message with the changes in the order book notified by the exchange
        """
        if metadata:
            msg.update(metadata)
        ts = timestamp

        bids = []
        asks = []

        if "buy" in msg:
            for item in msg["buy"]:
                bids.append([Decimal(item) / px_decimal, Decimal(msg["buy"][item]) / qty_decimal])

        if "sell" in msg:
            for item in msg["sell"]:
                asks.append([Decimal(item) / px_decimal, Decimal(msg["sell"][item]) / qty_decimal])

        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": ts,
            "bids": bids,
            "asks": asks
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, any],
                                    px_decimal: Decimal,
                                    qty_decimal: Decimal,
                                    metadata: Optional[Dict] = None):
        """
        Creates a trade message with the information from the trade event sent by the exchange
        :param msg: the trade event details sent by the exchange
        :param metadata: a dictionary with extra information to add to trade message
        :return: a trade message with the details of the trade as provided by the exchange
        """
        if metadata:
            msg.update(metadata)
        ts = msg["created"]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if msg["side"] == "buy" else float(TradeType.SELL.value),
            "trade_id": msg["tradeRef"],
            "update_id": ts,
            "price": Decimal(msg["price"]) / px_decimal,
            "amount": Decimal(msg["amount"]) / qty_decimal
        }, timestamp=ts)
