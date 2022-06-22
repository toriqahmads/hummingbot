import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import socketio

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoindcxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


class CoindcxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoindcxAuth,
                 connector: "CoindcxExchange",
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: CoindcxAuth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._ws_assistant: Optional[socketio.AsyncClient] = None

        self._last_recv_time: float = -1.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    @last_recv_time.setter
    def last_recv_time(self, value: float) -> float:
        self._last_recv_time = value

    async def _connected_websocket_assistant(self):
        """
        Creates an instance of SocketIO.AsyncClient. Connects to the exchange SocketIO server
        and initialize the appropriate event handler functions for specific channels.
        """
        try:
            self._ws_assistant: socketio.AsyncClient = self._create_async_client()

            # Initialise SocketIO client with appropriate event handlers
            self._ws_assistant.on("connect", self._subscribe_channels)
            self._ws_assistant.on(CONSTANTS.USER_BALANCE_EVENT_TYPE, self._process_balance_update)
            self._ws_assistant.on(CONSTANTS.USER_TRADE_EVENT_TYPE, self._process_trade_update)
            self._ws_assistant.on(CONSTANTS.USER_ORDER_EVENT_TYPE, self._process_order_update)
            self._ws_assistant.on("connect_error", self._handle_error)

            await self._ws_assistant.connect(CONSTANTS.WSS_URL, transports="websocket")

        except Exception as e:
            self.logger().error(
                f"Unexpected error occurred connecting to CoinDCX Websocket API. ({e})"
            )

    async def _process_event_message(self, event_message: Dict[str, Any]):
        """
        Event handler function for balance, trades and order update messages.

        :param event_message: Raw messages received from CoinDCX SocketIO
        :type event_message: Union[Dict[str, Any], List[Dict[str, Any]]]
        """
        self.last_recv_time = self._time()
        self._queue.put_nowait(event_message)

    async def _process_balance_update(self, balance_update: Dict[str, Any]):
        await self._process_event_message({
            "channel": CONSTANTS.USER_BALANCE_EVENT_TYPE,
            "data": json.loads(balance_update["data"])
        })

    async def _process_trade_update(self, trade_update: List[Dict[str, Any]]):
        await self._process_event_message({
            "channel": CONSTANTS.USER_TRADE_EVENT_TYPE,
            "data": json.loads(trade_update["data"])
        })

    async def _process_order_update(self, order_update: Dict[str, Any]):
        await self._process_event_message({
            "channel": CONSTANTS.USER_ORDER_EVENT_TYPE,
            "data": json.loads(order_update["data"])
        })

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue

        :param output: the queue to use to store the received messages
        """
        while True:
            try:
                self._queue: asyncio.Queue = output

                await self._connected_websocket_assistant()
                await self._ws_assistant.wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
                await self._sleep(5.0)
            finally:
                self._ws_assistant and await self._ws_assistant.disconnect()
                self._ws_assistant = None
                self._last_recv_time = -1.0

    async def _subscribe_channels(self):
        """
        Subscribes to the user balance, trades and order events.
        """
        try:
            auth_payload: Dict[str, Any] = await self._auth.ws_authenticate()
            await self._ws_assistant.emit("join", auth_payload)
            self.last_recv_time = self._time()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Unexpected error occurred subscribing to CoinDCX Websocket API. ({e})"
            )
            raise

    async def _handle_error(self, error):
        raise ValueError(str(error))

    def _create_async_client(self):
        return socketio.AsyncClient()
