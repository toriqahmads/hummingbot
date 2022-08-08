import asyncio

import signalr_aio

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS
from hummingbot.connector.exchange.bittrex.bittrex_auth import BittrexAuth
from hummingbot.connector.exchange.bittrex.bittrex_utils import _socket_stream, _transform_raw_message
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class BittrexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    LOCK = asyncio.Lock()
    INVOCATION_EVENT = None
    INVOCATION_RESPONSE = None

    def __init__(self,
                 auth: BittrexAuth,
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
        websocket_connection.received += self.handler
        websocket_connection.error += self.handler
        websocket_connection.start()
        await self.authenticate_client()
        return websocket_connection

    async def _subscribe_channels(self, websocket_assistant):
        try:
            channels = ["order", "balance", "execution"]
            response = await self.invoke('Subscribe', [channels])
            flag = True
            for i in range(len(channels)):
                if response[i]['Success']:
                    self.logger().info("Subscription to '" + channels[i] + "' successful")
                else:
                    flag = False
                    print("Subscription to '" + channels[i] + "' failed: " + response[i]["ErrorCode"])
            if not flag:
                raise
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Failed to subscribe to all private channels")
            raise

    async def _process_websocket_messages(self, websocket_assistant, queue: asyncio.Queue):
        """
        Overriding method for Bittrex signalr
        """
        async for raw_message in _socket_stream(conn=websocket_assistant):
            try:
                decoded_msg = _transform_raw_message(raw_message)
                if not decoded_msg.get("type"):
                    continue
                await self._process_event_message(event_message=decoded_msg["results"], queue=queue)
            except Exception:
                self.logger().exception("Unexpected error while decoding websocket message...")

    async def _on_user_stream_interruption(self, websocket_assistant):
        websocket_assistant and websocket_assistant.close()

    async def invoke(self, method, args):
        async with self.LOCK:
            self.INVOCATION_EVENT = asyncio.Event()
            self.hub.server.invoke(method, *args)
            await self.INVOCATION_EVENT.wait()
            return self.INVOCATION_RESPONSE

    async def handler(self, **msg):
        if 'R' in msg:
            self.INVOCATION_RESPONSE = msg['R']
            self.INVOCATION_EVENT.set()

    async def authenticate_client(self):
        auth_params = self._auth.generate_WS_auth_params()
        response = await self.invoke('Authenticate', auth_params)
        if response['Success']:
            self.logger().info("Successfully authenticated")
        else:
            self.logger().error('Authentication failed: ' + response['ErrorCode'])
            raise
