import asyncio
from base64 import b64decode
from typing import Any, AsyncIterable, Dict, Optional
from zlib import MAX_WBITS, decompress

import signalr_aio
import ujson
from async_timeout import timeout

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS
from hummingbot.connector.exchange.bittrex.bittrex_auth import BittrexAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class BittrexAPIUserStreamDataSource(UserStreamTrackerDataSource):

    def __init__(self, auth: BittrexAuth,
                 connector,
                 api_factory):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._websocket_connection: Optional[signalr_aio.Connection] = None
        self.hub = None

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message

        :return: the timestamp of the last received message in seconds
        """
        if self._ws_assistant:
            return self._auth.time_provider.time()
        return 0

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self):
        websocket_connection = signalr_aio.Connection(CONSTANTS.BITTREX_WS_URL, session=None)
        self.hub = websocket_connection.register_hub("c3")
        auth_params = self._auth.generate_WS_auth_params()
        self.hub.server.invoke("Authenticate", auth_params)
        return websocket_connection

    # TODO Remove this code after verification
    '''async def _authenticate_client(self, ws: WSAssistant):
        try:
            ws_request: WSJSONRequest = WSJSONRequest(
                {
                    "H": "c3",
                    "M": "Authenticate",
                    "A": [],
                    "I": 1
                }
            )
            auth_params = self._auth.generate_WS_auth_params()
            ws_request.payload['A'] = auth_params
            await ws.send(ws_request)
            resp: WSResponse = await ws.receive()
            auth_response = resp.data["R"]
            if not auth_response["Success"]:
                raise ValueError(f"User Stream Authentication Fail! {auth_response['ErrorCode']}")
            self.logger().info("Successfully authenticated to user stream...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Error occurred authenticating websocket connection..")
            raise'''

    async def _subscribe_channels(self, websocket_assistant):
        try:
            self.hub.server.invoke("Subscribe", ["heartbeat", "order", "balance", "execution"])
            websocket_assistant.start()
            self.logger().info("Successfully subscribed to private channels")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Cannot subscribe to private channels")
            raise

    def _decode_message(self, raw_message) -> Dict[str, Any]:
        try:
            decode_msg: bytes = decompress(b64decode(raw_message, validate=True), -MAX_WBITS)
        except SyntaxError:
            decode_msg: bytes = decompress(b64decode(raw_message, validate=True))
        except Exception:
            self.logger().error("Error decoding message", exc_info=True)

        return ujson.loads(decode_msg.decode(), precise_float=True)

    async def _socket_user_stream(self, conn: signalr_aio.Connection) -> AsyncIterable[str]:
        try:
            while True:
                async with timeout(CONSTANTS.MESSAGE_TIMEOUT):
                    msg = await conn.msg_queue.get()
                    yield msg
        except asyncio.TimeoutError:
            self.logger().warning("Message recv() timed out. Reconnecting to Bittrex SignalR WebSocket... ")

    async def _process_websocket_messages(self, websocket_assistant, queue: asyncio.Queue):
        async for raw_message in self._socket_user_stream(websocket_assistant):
            decoded_msg = self._decode_message(raw_message)
            await self._process_event_message(event_message=decoded_msg, queue=queue)

    async def _on_user_stream_interruption(self, websocket_assistant):
        websocket_assistant and await websocket_assistant.close()
