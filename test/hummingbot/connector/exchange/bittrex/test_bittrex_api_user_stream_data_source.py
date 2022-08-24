import asyncio
import unittest
from typing import Awaitable, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source import BittrexAPIUserStreamDataSource
from hummingbot.connector.exchange.bittrex.bittrex_auth import BittrexAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer


class BittrexAPIUserStreamDataSourceTest(unittest.TestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.api_key = "someKey"
        cls.secret_key = "someSecret"

    def setUp(self) -> None:
        super().setUp()
        self.listening_task: Optional[asyncio.Task] = None
        self.log_records = []
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000
        self.time_synchronizer = TimeSynchronizer()
        self.time_synchronizer.add_time_offset_ms_sample(0)
        self.data_source = BittrexAPIUserStreamDataSource(
            auth=BittrexAuth(api_key=self.api_key,
                             secret_key= self.secret_key,
                             time_provider=self.mock_time_provider),
        )
        self.resume_test_event = asyncio.Event()
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _raise_exception(self, exception_class):
        raise exception_class

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _unlock_test_with_event(self):
        self.resume_test_event.set()
        return

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "authenticate_client", new_callable=AsyncMock)
    def test_for_succesful_connect_to_signalr_and_register_hub(self, auth_mock, ws_connect_mock):
        conn_mock = MagicMock()
        ws_connect_mock.return_value = conn_mock
        msgeventhook_mock, errorevent_mock = MagicMock(), MagicMock()
        msgeventhook_mock.__iadd__.return_value = msgeventhook_mock
        errorevent_mock.__iadd__.return_value = errorevent_mock
        conn_mock.received = msgeventhook_mock
        conn_mock.error = errorevent_mock
        auth_mock.side_effect = self._unlock_test_with_event()
        self.listening_task = self.ev_loop.create_task(self.data_source._connected_websocket_assistant())
        self.async_run_with_timeout(self.resume_test_event.wait())
        conn_mock.register_hub.assert_called_with("c3")
        msgeventhook_mock.__iadd__.assert_called_with(self.data_source._handle_message)
        msgeventhook_mock.__iadd__.assert_called_once()
        errorevent_mock.__iadd__.assert_called_with(self.data_source._handle_error)
        errorevent_mock.__iadd__.assert_called_once()

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "authenticate_client", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_for_authentication_failed(self, invoke_mock, auth_mock, ws_connect_mock):
        conn_mock = MagicMock()
        ws_connect_mock.return_value = conn_mock
        auth_mock.side_effect = lambda *arg, **kwars: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR."))
        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "authenticate_client", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_listen_for_user_stream_subscribes_to_orders_and_balances_events(self, invoke_mock, auth_mock, ws_connect_mock):
        conn_mock = MagicMock()
        conn_mock.register_hub.return_value = MagicMock()
        ws_connect_mock.return_value = conn_mock
        auth_mock.side_effect = self._unlock_test_with_event()
        sub_channels = ["order", "balance", "execution"]
        invoke_mock.return_value = [
            {"Success": True, "ErrorCode": None},
            {"Success": True, "ErrorCode": None},
            {"Success": True, "ErrorCode": None}
        ]
        output_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=output_queue))
        self.async_run_with_timeout(self.resume_test_event.wait())
        invoke_mock.assert_called_with("Subscribe", [sub_channels])
        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to private channels"
        ))

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "authenticate_client", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_listen_for_user_stream_fails_to_subscribe_to_channels(self, invoke_mock, auth_mock, ws_connect_mock):
        conn_mock = MagicMock()
        conn_mock.register_hub.return_value = MagicMock()
        ws_connect_mock.return_value = conn_mock
        auth_mock.side_effect = self._unlock_test_with_event()
        invoke_mock.return_value = [
            {"Success": False, "ErrorCode": "Test-error-code"},
            {"Success": True, "ErrorCode": None},
            {"Success": True, "ErrorCode": None}
        ]
        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )
        self.async_run_with_timeout(self.resume_test_event.wait())
        self.assertTrue(
            self._is_logged("ERROR",
                            "Subscription to 'order' failed: Test-error-code"))

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "authenticate_client", new_callable=AsyncMock)
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_user_stream_data_source.BittrexAPIUserStreamDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_listen_for_user_stream_raises_exception_while_subscribing(self, invoke_mock, auth_mock, ws_connect_mock):
        conn_mock = MagicMock()
        conn_mock.register_hub.return_value = MagicMock()
        ws_connect_mock.return_value = conn_mock
        auth_mock.side_effect = self._unlock_test_with_event()
        invoke_mock.side_effect = lambda *arg, **kwars: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR."))
        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )
        self.async_run_with_timeout(self.resume_test_event.wait())
        self.assertTrue(
            self._is_logged("ERROR",
                            "Failed to subscribe to all private channels"))
