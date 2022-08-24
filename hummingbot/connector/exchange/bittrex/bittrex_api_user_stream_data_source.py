import asyncio

import signalr_aio

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS
from hummingbot.connector.exchange.bittrex.bittrex_auth import BittrexAuth
from hummingbot.connector.exchange.bittrex.bittrex_utils import _decode_message
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class BittrexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    LOCK = asyncio.Lock()
    INVOCATION_EVENT = None
    INVOCATION_RESPONSE = None

    def __init__(self,
                 auth: BittrexAuth,
                 ):
        super().__init__()
        self._auth = auth
        self.hub = None
        self._queue: asyncio.Queue = None
        self._last_recv_time: float = 0

    @property
    def last_recv_time(self) -> float:
        """
        Overriding method for Bittrex Signalr
        """
        if self._ws_assistant:
            return self._last_recv_time
        return 0

    async def authenticate_client(self):
        auth_params = self._auth.generate_WS_auth_params()
        response = await self._invoke('Authenticate', auth_params)
        if response['Success']:
            self.logger().info("Successfully authenticated")
        else:
            self.logger().error('Authentication failed: ' + response['ErrorCode'])
            raise Exception

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Overwriting method for Bittrex' signalr connection
        """
        while True:
            try:
                self._queue = output
                self._ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(websocket_assistant=self._ws_assistant)
                forever = asyncio.Event()
                await forever.wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
                await self._sleep(5.0)
            finally:
                await self._on_user_stream_interruption(websocket_assistant=self._ws_assistant)
                self._ws_assistant = None

    async def _subscribe_channels(self, websocket_assistant):
        try:
            self.hub.client.on('execution', self._on_execution)
            self.hub.client.on('order', self._on_order)
            self.hub.client.on('balance', self._on_balance)
            channels = ["order", "balance", "execution"]
            response = await self._invoke('Subscribe', [channels])
            flag = True
            for i in range(len(channels)):
                if response[i]['Success']:
                    self.logger().info("Subscription to '" + channels[i] + "' successful")
                else:
                    flag = False
                    self.logger().error("Subscription to '" + channels[i] + "' failed: " + response[i]["ErrorCode"])
            if flag:
                self.logger().info("Subscribed to private channels")
            else:
                raise Exception("Failed to subscribe to all private channels")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Failed to subscribe to all private channels")
            raise

    async def _connected_websocket_assistant(self):
        websocket_connection = signalr_aio.Connection(CONSTANTS.BITTREX_WS_URL, session=None)
        self.hub = websocket_connection.register_hub("c3")
        websocket_connection.received += self._handle_message
        websocket_connection.error += self._handle_error
        websocket_connection.start()
        await self.authenticate_client()
        return websocket_connection

    async def _invoke(self, method, args):
        async with self.LOCK:
            self.INVOCATION_EVENT = asyncio.Event()
            self.hub.server.invoke(method, *args)
            await self.INVOCATION_EVENT.wait()
            self._last_recv_time = self._time()
            return self.INVOCATION_RESPONSE

    async def _handle_message(self, **msg):
        if 'R' in msg:
            self.INVOCATION_RESPONSE = msg['R']
            self.INVOCATION_EVENT.set()

    async def _handle_error(self, msg):
        self.logger().exception(msg)
        raise Exception(msg)

    async def _on_execution(self, msg):
        await self._process_message("execution", msg)

    async def _on_balance(self, msg):
        await self._process_message("balance", msg)

    async def _on_order(self, msg):
        await self._process_message("order", msg)

    async def _process_message(self,
                               event: str, msg):
        decoded_msg = await _decode_message(msg[0])
        decoded_msg["type"] = event
        self._last_recv_time = self._time()
        await self._process_event_message(event_message=decoded_msg, queue=self._queue)

    async def _on_user_stream_interruption(self, websocket_assistant):
        websocket_assistant and websocket_assistant.close()
