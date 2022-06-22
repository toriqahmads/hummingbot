import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import socketio

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from .coindcx_exchange import CoindcxExchange


class CoindcxAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoindcxExchange",
        api_factory: Optional[WebAssistantsFactory] = None,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory or web_utils.build_api_factory()
        self._ws_assistant: Optional[socketio.AsyncClient] = None

    @classmethod
    def _default_domain(cls):
        return CONSTANTS.DEFAULT_DOMAIN

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = int(self._time()) * 1e3
        update_id: int = int(snapshot_timestamp)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(price, amt) for price, amt in snapshot_response["bids"].items()],
            "asks": [(price, amt) for price, amt in snapshot_response["asks"].items()],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT, order_book_message_content, snapshot_timestamp
        )

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "pair": await self._connector.exchange_ecode_symbol_associated_to_pair(trading_pair=trading_pair),
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data: Dict[str, Any] = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=CONSTANTS.PUBLIC_DOMAIN),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.COINDCX_GLOBAL_RATE_LIMIT,
        )

        return data

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        timestamp: float = float(raw_message["T"])
        trading_pair: str = await self._connector.trading_pair_associated_to_exchange_ecode_symbol(
            symbol=raw_message["channel"]
        )
        message_content = {
            "trade_id": int(timestamp),
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.SELL.value) if raw_message["m"] else float(TradeType.BUY.value),
            "amount": float(raw_message["q"]),
            "price": float(raw_message["p"]),
        }
        trade_message: OrderBookMessage = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE, content=message_content, timestamp=timestamp * 1e-3
        )
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        timestamp: float = float(raw_message["E"])
        trading_pair: str = await self._connector.trading_pair_associated_to_exchange_ecode_symbol(
            symbol=raw_message["channel"]
        )
        message_content = {
            "trading_pair": trading_pair,
            "update_id": int(timestamp),
            "bids": [(price, amt) for price, amt in raw_message["b"]],
            "asks": [(price, amt) for price, amt in raw_message["a"]],
        }
        # NOTE: CoinDCX does not use delta updates, and instead use depth updates.
        #       Hence we use OrderBookMessageType.SNAPSHOT here.
        depth_message: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            message_content,
            timestamp * 1e-3)
        message_queue.put_nowait(depth_message)

    async def _subscribe_channels(self):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        """
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol: str = await self._connector.exchange_ecode_symbol_associated_to_pair(
                    trading_pair=trading_pair
                )
                await self._ws_assistant.emit("join", {"channelName": exchange_symbol})
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    async def _on_new_trade(self, response: Dict[str, Any]):
        data: Dict[str, Any] = json.loads(response["data"])
        self._message_queue[self._trade_messages_queue_key].put_nowait(data)

    async def _on_depth_update(self, response: Dict[str, Any]):
        data: Dict[str, Any] = json.loads(response["data"])
        self._message_queue[self._diff_messages_queue_key].put_nowait(data)

    async def _handle_error(self, error):
        raise ValueError(str(error))

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        while True:
            try:
                await self._connected_websocket_assistant()
                await self._ws_assistant.wait()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(
                    f"Unexpected error occurred when listening to order book streams. Retrying in 5 seconds... Error: {e}",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                self._ws_assistant and await self._ws_assistant.disconnect()
                self._ws_assistant = None

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        # SocketIO client uses event handlers to determine the various channels.
        # See `_on_new_trade` and `_on_depth_update`.
        pass

    async def _connected_websocket_assistant(self) -> socketio.AsyncClient:
        try:
            self._ws_assistant = self._create_async_client()

            # Initialise SocketIO client with appropriate event handlers
            self._ws_assistant.on("connect", self._subscribe_channels)
            self._ws_assistant.on(CONSTANTS.ORDER_BOOK_TRADE_EVENT_TYPE, self._on_new_trade)
            self._ws_assistant.on(CONSTANTS.ORDER_BOOK_DEPTH_EVENT_TYPE, self._on_depth_update)
            self._ws_assistant.on("connect_error", self._handle_error)

            await self._ws_assistant.connect(CONSTANTS.WSS_URL, transports="websocket")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Unexpected error occurred connecting to CoinDCX Websocket API. ({e})")
            raise

    def _create_async_client(self):
        return socketio.AsyncClient()
