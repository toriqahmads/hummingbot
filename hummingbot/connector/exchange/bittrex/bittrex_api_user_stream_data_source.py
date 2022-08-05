import asyncio

import signalr_aio

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS
from hummingbot.connector.exchange.bittrex.bittrex_auth import BittrexAuth
from hummingbot.connector.exchange.bittrex.bittrex_utils import _socket_stream, _transform_raw_message
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class BittrexAPIUserStreamDataSource(UserStreamTrackerDataSource):

    def __init__(self, auth: BittrexAuth,
                 connector,
                 api_factory):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self.hub = None

    @property
    def last_recv_time(self) -> float:
        """
        Overriding method for Bittrex Signalr
        """
        if self._ws_assistant:
            return self._auth.time_provider.time()
        return 0

    async def _connected_websocket_assistant(self):
        websocket_connection = signalr_aio.Connection(CONSTANTS.BITTREX_WS_URL, session=None)
        self.hub = websocket_connection.register_hub("c3")
        websocket_connection.start()
        auth_params = self._auth.generate_WS_auth_params()
        self.hub.server.invoke("Authenticate", auth_params)
        self.logger().info("Successfully authenticated")
        return websocket_connection

    async def _subscribe_channels(self, websocket_assistant):
        try:
            self.hub.server.invoke("Subscribe", ["order", "balance", "execution"])
            self.logger().info("Successfully subscribed to private channels")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Cannot subscribe to private channels")
            raise

    async def _process_websocket_messages(self, websocket_assistant, queue: asyncio.Queue):
        """
        Overriding method for Bittrex signalr
        """
        async for raw_message in _socket_stream(caller_class=self, conn=websocket_assistant):
            decoded_msg = _transform_raw_message(raw_message)
            if not decoded_msg.get("type"):
                continue
            await self._process_event_message(event_message=decoded_msg["results"], queue=queue)

    async def _on_user_stream_interruption(self, websocket_assistant):
        websocket_assistant and websocket_assistant.close()
