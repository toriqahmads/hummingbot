import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional

import signalr_aio

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS, bittrex_web_utils as web_utils
from hummingbot.connector.exchange.bittrex.bittrex_utils import _decode_message, _get_timestamp
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class BittrexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    LOCK = asyncio.Lock()
    INVOCATION_EVENT = None
    INVOCATION_RESPONSE = None

    def __init__(self, trading_pairs: List[str],
                 connector,
                 api_factory: WebAssistantsFactory,
                 ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self.hub = None

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {
            "marketSymbol": exchange_symbol,
            "depth": 500
        }
        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(exchange_symbol)),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDERBOOK_SNAPSHOT_LIMIT_ID,
        )
        return data

    async def listen_for_subscriptions(self):
        """
        Overriding method for Bittrex signalr
        """
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                forever = asyncio.Event()
                await forever.wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await self._sleep(5.0)
            finally:
                await self._on_order_stream_interruption(websocket_assistant=ws)

    async def _subscribe_channels(self, ws):
        try:
            self.hub.client.on('orderBook', self._on_orderbook)
            self.hub.client.on('trade', self._on_trade)
            subscription_names = []
            for trading_pair in self._trading_pairs:
                trading_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trade_channel = f"trade_{trading_symbol}"
                orderbook_channel = f"orderbook_{trading_symbol}_500"
                subscription_names.extend([trade_channel, orderbook_channel])
            response = await self._invoke("Subscribe", [subscription_names])
            flag = True
            for i in range(len(subscription_names)):
                if not response[i]['Success']:
                    flag = False
                    self.logger().error("Subscription to '" + subscription_names[i] + "' failed: " + response[i]["ErrorCode"])
            if flag:
                self.logger().info("Subscribed to public order book and trade channels...")
            else:
                raise Exception("Failed to subscribe to all public channels")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Failed to subscribe to public order book and trade channels...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self):
        ws_connection = signalr_aio.Connection(CONSTANTS.BITTREX_WS_URL, session=None)
        self.hub = ws_connection.register_hub("c3")
        ws_connection.received += self._handle_message
        ws_connection.error += self._handle_error
        ws_connection.start()
        return ws_connection

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = self._time()
        snapshot_msg: OrderBookMessage = self._snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _invoke(self, method, args):
        async with self.LOCK:
            self.INVOCATION_EVENT = asyncio.Event()
            self.hub.server.invoke(method, *args)
            await self.INVOCATION_EVENT.wait()
            return self.INVOCATION_RESPONSE

    async def _handle_message(self, **msg):
        if 'R' in msg:
            self.INVOCATION_RESPONSE = msg['R']
            self.INVOCATION_EVENT.set()

    async def _handle_error(self, msg):
        self.logger().exception(msg)
        raise Exception

    async def _on_orderbook(self, msg):
        decoded_msg = await _decode_message(msg[0])
        self._message_queue[self._diff_messages_queue_key].put_nowait(decoded_msg)

    async def _on_trade(self, msg):
        decoded_msg = await _decode_message(msg[0])
        self._message_queue[self._trade_messages_queue_key].put_nowait(decoded_msg)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["marketSymbol"])
        for data in raw_message["deltas"]:
            trade_message: OrderBookMessage = self._trade_message_from_exchange(
                msg=data,
                metadata={"trading_pair": trading_pair, "sequence": raw_message["sequence"]}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["marketSymbol"])
        order_book_message: OrderBookMessage = self._diff_message_from_exchange(msg=raw_message,
                                                                                timestamp=self._time(),
                                                                                metadata={"trading_pair": trading_pair}
                                                                                )
        message_queue.put_nowait(order_book_message)

    def _snapshot_message_from_exchange(self,
                                        msg: Dict[str, any],
                                        timestamp: float,
                                        metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        bids = [(Decimal(bid["rate"]), Decimal(bid["quantity"])) for bid in msg["bid"]]
        asks = [(Decimal(ask["rate"]), Decimal(ask["quantity"])) for ask in msg["ask"]]
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": int(timestamp),
            "bids": bids,
            "asks": asks
        }, timestamp=timestamp)

    def _trade_message_from_exchange(self,
                                     msg: Dict[str, Any],
                                     metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(
            OrderBookMessageType.TRADE, {
                "trading_pair": msg["trading_pair"],
                "trade_type": float(TradeType.BUY.value) if msg["takerSide"] == "BUY" else float(TradeType.SELL.value),
                "trade_id": msg["id"],
                "price": msg["rate"],
                "amount": msg["quantity"]
            }, timestamp=_get_timestamp(msg["executedAt"]))

    def _diff_message_from_exchange(self,
                                    msg: Dict[str, any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None):
        if metadata:
            msg.update(metadata)
        bids = [(Decimal(bid["rate"]), Decimal(bid["quantity"])) for bid in msg["bidDeltas"]]
        asks = [(Decimal(ask["rate"]), Decimal(ask["quantity"])) for ask in msg["askDeltas"]]
        return OrderBookMessage(
            OrderBookMessageType.DIFF, {
                "trading_pair": msg["trading_pair"],
                "update_id": int(timestamp),
                "bids": bids,
                "asks": asks
            }, timestamp=timestamp)

    async def _on_order_stream_interruption(self, websocket_assistant):
        websocket_assistant and websocket_assistant.close()

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Suppressing function as Bittrex uses signalr connection
        """
        pass
