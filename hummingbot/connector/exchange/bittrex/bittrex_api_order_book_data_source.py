import asyncio
import time
from base64 import b64decode
from typing import Any, AsyncIterable, Dict, List, Optional
from zlib import MAX_WBITS, decompress

import signalr_aio
import ujson
from async_timeout import timeout

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS, bittrex_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class BittrexAPIOrderBookDataSource(OrderBookTrackerDataSource):

    def __init__(self, trading_pairs: List[str], connector, api_factory: WebAssistantsFactory, ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_KEY
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_KEY
        self._api_factory = api_factory
        self.hub = None

    async def _connected_websocket_assistant(self):
        ws_connection = signalr_aio.Connection(CONSTANTS.BITTREX_WS_URL, session=None)
        self.hub = ws_connection.register_hub("c3")
        return ws_connection

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {
            "marketSymbol": exchange_symbol,
        }
        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(exchange_symbol)),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDERBOOK_SNAPSHOT_LIMIT_ID,
        )
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = self.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    # TODO Move utility functions to one single file after verification
    async def _checked_socket_stream(self, connection: signalr_aio.Connection) -> AsyncIterable[str]:
        try:
            while True:
                async with timeout(CONSTANTS.MESSAGE_TIMEOUT):  # Timeouts if not receiving any messages for 10 seconds(ping)
                    msg = await connection.msg_queue.get()
                    yield msg
        except asyncio.TimeoutError:
            self.logger().warning("Message queue get() timed out. Going to reconnect...")

    def _decode_message(self, raw_message) -> Dict[str, Any]:
        try:
            decoded_msg: bytes = decompress(b64decode(raw_message, validate=True), -MAX_WBITS)
        except SyntaxError:
            decoded_msg: bytes = decompress(b64decode(raw_message, validate=True))
        except Exception:
            return {}

        return ujson.loads(decoded_msg.decode())

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            trading_symbols = [await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                               for trading_pair in self._trading_pairs]
            subscription_names = [f"trade_{symbol}" for symbol in trading_symbols]
            subscription_names.extend([f"orderbook_{symbol}_25" for symbol in trading_symbols])
            self.hub.server.invoke("Subscribe", subscription_names)
            self.logger().info("Subscribed to public order book and trade channels...")

            ws.start()
            self.logger().info("Websocket connection started...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant):
        async for raw_message in self._checked_socket_stream(websocket_assistant):
            decoded_msg: Dict[str, Any] = self._decode_message(raw_message)
            channel: str = self._channel_originating_message(event_message=decoded_msg)
            if channel in [self._diff_messages_queue_key, self._trade_messages_queue_key]:
                self._message_queue[channel].put_nowait(decoded_msg)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = self._trade_messages_queue_key
        if "depth" in event_message:
            channel = self._diff_messages_queue_key
        return channel

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["marketSymbol"])
        for data in raw_message["deltas"]:
            trade_message: OrderBookMessage = self.trade_message_from_exchange(
                msg=data,
                metadata={"trading_pair": trading_pair, "sequence": raw_message["sequence"]}
            )
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["marketSymbol"])
        order_book_message: OrderBookMessage = self.diff_message_from_exchange(raw_message, time.time(), {"trading_pair": trading_pair})
        message_queue.put_nowait(order_book_message)

    def snapshot_message_from_exchange(self, msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        bids, asks = msg["bid"], msg["ask"]
        bids = [(bid["rate"], bid["quantity"]) for bid in bids]
        asks = [(ask["rate"], ask["quantity"]) for ask in asks]
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": int(timestamp),
            "bids": bids,
            "asks": asks
        }, timestamp=timestamp)

    def trade_message_from_exchange(self, msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(
            OrderBookMessageType.TRADE, {
                "trading_pair": msg["trading_pair"],
                "trade_type": float(TradeType.BUY.value) if msg["takerSide"] == "BUY" else float(TradeType.SELL.value),
                "trade_id": msg["id"],
                "update_id": msg["sequence"],
                "price": msg["rate"],
                "amount": msg["quantity"]
            }, timestamp=float(msg["executedAt"]))

    def diff_message_from_exchange(self, msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None):
        if metadata:
            msg.update(metadata)
        bids, asks = msg["bidDeltas"], msg["askDeltas"]
        bids = [(bid["rate"], bid["quantity"]) for bid in bids]
        asks = [(ask["rate"], ask["quantity"]) for ask in asks]
        return OrderBookMessage(
            OrderBookMessageType.DIFF, {
                "trading_pair": msg["trading_pair"],
                "update_id": int(msg["sequence"]),
                "bids": bids,
                "asks": asks
            }, timestamp=timestamp)

    async def _on_order_stream_interruption(self, websocket_assistant):
        websocket_assistant and await websocket_assistant.close()
