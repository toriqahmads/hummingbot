import asyncio
from typing import Any, Dict, List, Optional

import hummingbot.connector.exchange.huobi.huobi_constants as CONSTANTS
from hummingbot.connector.exchange.huobi.huobi_utils import (
    convert_from_exchange_trading_pair,
    convert_to_exchange_trading_pair,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, RESTResponse, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class HuobiAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _haobds_logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector,
                 api_factory: WebAssistantsFactory,
                 ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._diff_messages_queue_key = CONSTANTS.ORDERBOOK_CHANNEL_SUFFIX
        self._trade_messages_queue_key = CONSTANTS.TRADE_CHANNEL_SUFFIX
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_PUBLIC_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)

        return ws

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def request_new_orderbook_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        rest_assistant = await self._api_factory.get_rest_assistant()
        url = CONSTANTS.REST_URL + CONSTANTS.DEPTH_URL
        # when type is set to "step0", the default value of "depth" is 150
        params: Dict = {"symbol": convert_to_exchange_trading_pair(trading_pair), "type": "step0"}
        request = RESTRequest(method=RESTMethod.GET,
                              url=url,
                              params=params)
        response: RESTResponse = await rest_assistant.call(request=request)

        if response.status != 200:
            raise IOError(f"Error fetching Huobi market snapshot for {trading_pair}. "
                          f"HTTP status is {response.status}.")
        snapshot_data: Dict[str, Any] = await response.json()
        return snapshot_data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self.request_new_orderbook_snapshot(trading_pair)
        snapshot_msg: OrderBookMessage = self.snapshot_message_from_exchange(
            msg=snapshot,
            metadata={"trading_pair": trading_pair},
        )

        return snapshot_msg

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest({
                    "sub": f"market.{convert_to_exchange_trading_pair(trading_pair)}.depth.step0",
                    "id": convert_to_exchange_trading_pair(trading_pair)
                })
                subscribe_trade_request: WSJSONRequest = WSJSONRequest({
                    "sub": f"market.{convert_to_exchange_trading_pair(trading_pair)}.trade.detail",
                    "id": convert_to_exchange_trading_pair(trading_pair)
                })
                await ws.send(subscribe_orderbook_request)
                await ws.send(subscribe_trade_request)
            self.logger().info("Subscribed to public orderbook and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = event_message.get("ch", "")
        retval = ""
        if channel.endswith(self._trade_messages_queue_key):
            retval = self._trade_messages_queue_key
        if channel.endswith(self._diff_messages_queue_key):
            retval = self._diff_messages_queue_key

        return retval

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):

        trading_pair = raw_message["ch"].split(".")[1]
        for data in raw_message["tick"]["data"]:
            trade_message: OrderBookMessage = self.trade_message_from_exchange(
                msg=data,
                metadata={"trading_pair": convert_from_exchange_trading_pair(trading_pair)}
            )
            message_queue.put_nowait(trade_message)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Huobi connector sends snapshot messages instead of diff messages at 1-second intervals.
        Hence there is no need for requesting a new snapshot every 1 hour.

        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                msg: Dict[str, Any] = await message_queue.get()
                snapshot_msg: OrderBookMessage = self.snapshot_message_from_exchange(
                    msg=msg,
                )
                output.put_nowait(snapshot_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange", exc_info=True)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Suppressing call to this function as the orderbook snapshots are handled by
        listen_for_order_book_diffs() for Huobi
        """
        pass

    def snapshot_message_from_exchange(self,
                                       msg: Dict[str, Any],
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:

        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)
        msg_ts = int(round(msg["tick"]["ts"] / 1e3))
        msg_channel = msg["ch"]
        content = {
            "update_id": msg["tick"]["ts"],
            "bids": msg["tick"]["bids"],
            "asks": msg["tick"]["asks"]
        }
        if "trading_pair" in msg:
            content["trading_pair"] = msg["trading_pair"]
        else:
            content["trading_pair"] = convert_from_exchange_trading_pair(msg_channel.split(".")[1])

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=msg_ts)

    def trade_message_from_exchange(self,
                                    msg: Dict[str, Any],
                                    metadata: Dict[str, Any] = None) -> OrderBookMessage:
        """
        Creates a trade message with the information from the trade event sent by the exchange
        :param msg: the trade event details sent by the exchange
        :param metadata: a dictionary with extra information to add to trade message
        :return: a trade message with the details of the trade as provided by the exchange
        """
        if metadata:
            msg.update(metadata)

        msg_ts = int(round(msg["ts"] / 1e3))
        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if msg["direction"] == "buy" else float(TradeType.SELL.value),
            "trade_id": msg["id"],
            "update_id": msg["ts"],
            "amount": msg["amount"],
            "price": msg["price"]
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=msg_ts)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = ws_response.data
            if "ping" in data:
                pong_request = WSJSONRequest(payload={"pong": data["ping"]})
                await websocket_assistant.send(request=pong_request)
                continue
            channel: str = self._channel_originating_message(event_message=data)
            if channel in [self._diff_messages_queue_key, self._trade_messages_queue_key]:
                self._message_queue[channel].put_nowait(data)
