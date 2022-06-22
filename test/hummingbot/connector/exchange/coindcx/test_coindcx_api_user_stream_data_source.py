import asyncio
import json
import unittest
from typing import Awaitable, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoindcxAPIUserStreamDataSource
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoindcxAuth
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class CoindcxUserStreamDataSourceUnitTests(unittest.TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = "com"

        cls.listen_key = "TEST_LISTEN_KEY"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        self.time_synchronizer = MagicMock()
        self.time_synchronizer.time.return_value = 1640001112.223

        self.auth = CoindcxAuth(
            api_key="TEST_API_KEY",
            secret_key="TEST_SECRET",
        )
        self.connector = CoindcxExchange(
            coindcx_api_key="",
            coindcx_secret_key="",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.connector._web_assistants_factory._auth = self.auth

        self.data_source = CoindcxAPIUserStreamDataSource(
            auth=self.auth,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

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

    def _create_return_value_and_unlock_test_with_event(self, value):
        self.resume_test_event.set()
        return value

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.CoindcxAPIUserStreamDataSource"
           "._create_async_client")
    def test_listen_for_user_stream_configures_event_listeners(self, mock_client_creation):
        configured_events = {}
        connection_url = []
        mock_socketio_client = MagicMock()
        mock_socketio_client.on.side_effect = lambda event, f: configured_events.update({event: f})
        mock_socketio_client.connect = AsyncMock()
        mock_socketio_client.connect.side_effect = lambda url, **kwargs: connection_url.append(url)
        mock_socketio_client.wait = AsyncMock()
        mock_socketio_client.wait.side_effect = asyncio.CancelledError
        mock_socketio_client.disconnect = AsyncMock()
        mock_socketio_client.disconnect.return_value = None
        mock_client_creation.return_value = mock_socketio_client

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=asyncio.Queue()))

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertIn("connect", configured_events)
        self.assertEqual(configured_events["connect"], self.data_source._subscribe_channels)
        self.assertIn(CONSTANTS.USER_ORDER_EVENT_TYPE, configured_events)
        self.assertEqual(configured_events[CONSTANTS.USER_ORDER_EVENT_TYPE], self.data_source._process_order_update)
        self.assertIn(CONSTANTS.USER_BALANCE_EVENT_TYPE, configured_events)
        self.assertEqual(configured_events[CONSTANTS.USER_BALANCE_EVENT_TYPE], self.data_source._process_balance_update)
        self.assertIn("connect_error", configured_events)
        self.assertEqual(configured_events["connect_error"], self.data_source._handle_error)

        self.assertEqual(CONSTANTS.WSS_URL, connection_url[0])

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.CoindcxAPIUserStreamDataSource"
           "._create_async_client")
    def test_listen_for_user_stream_raises_cancel_exception(self, mock_client_creation):
        mock_client_creation.side_effect = asyncio.CancelledError

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=asyncio.Queue()))
        with self.assertRaises(asyncio.CancelledError):
            self.async_run_with_timeout(self.listening_task)

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.CoindcxAPIUserStreamDataSource"
           "._create_async_client")
    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.CoindcxAPIUserStreamDataSource"
           "._sleep")
    def test_listen_for_user_stream_raises_and_logs_exception(self, _, mock_client_creation):
        mock_socketio_client = MagicMock()
        mock_client_creation.return_value = mock_socketio_client
        mock_client_creation.side_effect = [Exception("Test Error"), asyncio.CancelledError]
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=asyncio.Queue()))

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred connecting to CoinDCX Websocket API. (Test Error)")
        )
        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )

    def test_subscribe_channels_subscribes_to_user_channel(self):
        all_subscriptions = []
        mock_ws = AsyncMock()
        mock_ws.emit.side_effect = lambda event, payload: all_subscriptions.append((event, payload))

        self.data_source._ws_assistant = mock_ws

        self.listening_task = self.ev_loop.create_task(self.data_source._subscribe_channels())
        self.async_run_with_timeout(self.listening_task)

        expected_payload = ("join", self.async_run_with_timeout(self.auth.ws_authenticate()))

        self.assertEqual(expected_payload, all_subscriptions[0])

    def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = AsyncMock()
        mock_ws.emit.side_effect = asyncio.CancelledError

        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.data_source._subscribe_channels())
            self.async_run_with_timeout(self.listening_task)

    def test_subscribe_channels_raises_and_logs_exceptions(self):
        mock_ws = AsyncMock()
        mock_ws.emit.side_effect = Exception("Test Error")

        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(Exception) as context:
            self.listening_task = self.ev_loop.create_task(self.data_source._subscribe_channels())
            self.async_run_with_timeout(self.listening_task)

        self.assertEqual("Test Error", str(context.exception))
        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to CoinDCX Websocket API. (Test Error)")
        )

    def test_process_balance_update(self):
        event_data = {
            "event": CONSTANTS.USER_BALANCE_EVENT_TYPE,
            "data": json.dumps([
                {
                    "id": "94dfde3c-cc1f-11ec-8e29-0f323befe763",
                    "balance": "10.0",
                    "locked_balance": "5.0",
                    "address": "0x711a1Fd5737eb0c33fF21dF9876Ad37c66854520",
                    "tag": "None",
                    "currency": {
                        "short_name": "COINALPHA",
                        "name": "CoinAlpha",
                        "status": "active",
                        "deprecated": ["withdrawal_charge", "min_withdrawal", "confirmations"],
                        "withdrawal_charge": 20,
                        "min_withdrawal": 24,
                        "min_deposit": 0,
                        "confirmations": 12,
                        "decimal_factor": 3,
                        "quote_precision_inr": 2,
                        "category": "erc20",
                        "otc_max_order_limit_inr": 100000,
                        "otc_min_order_limit_inr": 0,
                        "networks": [
                            {
                                "network": "tron",
                                "name": "TRC20",
                                "status": "active",
                                "withdrawal_charge": 1,
                                "min_withdrawal": 10,
                                "confirmations": 1,
                            },
                            {
                                "network": "erc20",
                                "name": "ERC20",
                                "status": "active",
                                "withdrawal_charge": 20,
                                "min_withdrawal": 24,
                                "confirmations": 12,
                            },
                        ],
                        "limit_order_status": "None",
                        "limit_order_details": "None",
                        "ob_visibility": "None",
                    },
                }
            ])
        }

        output_queue = asyncio.Queue()

        self.data_source._queue = output_queue
        self.async_run_with_timeout(self.data_source._process_balance_update(event_data))

        self.assertEqual(1, output_queue.qsize())
        message = output_queue.get_nowait()
        self.assertIn("channel", message)
        self.assertEqual(CONSTANTS.USER_BALANCE_EVENT_TYPE, message["channel"])
        self.assertIn("data", message)
        self.assertEqual(json.loads(event_data["data"]), message["data"])

    def test_process_trade_update(self):
        event_data = {
            "event": CONSTANTS.USER_TRADE_EVENT_TYPE,
            "data": json.dumps([
                {
                    "o": "someOrderId",
                    "c": "someClientOrderId",
                    "t": "58008779",
                    "s": "COINALPHAHBOT",
                    "p": 1.0,
                    "q": 1.0,
                    "T": 1654778738830.03,
                    "m": True,
                    "f": 0.1,
                    "e": "B",
                    "x": "filled",
                }
            ])
        }

        output_queue = asyncio.Queue()

        self.data_source._queue = output_queue
        self.async_run_with_timeout(self.data_source._process_trade_update(event_data))

        self.assertEqual(1, output_queue.qsize())
        message = output_queue.get_nowait()
        self.assertIn("channel", message)
        self.assertEqual(CONSTANTS.USER_TRADE_EVENT_TYPE, message["channel"])
        self.assertIn("data", message)
        self.assertEqual(json.loads(event_data["data"]), message["data"])

    def test_process_order_update(self):
        event_data = {
            "event": CONSTANTS.USER_ORDER_EVENT_TYPE,
            "data": json.dumps({
                "id": "someOrderId",
                "client_order_id": "someClientOrderId",
                "order_type": "limit_order",
                "side": "buy",
                "status": "open",
                "fee_amount": 0,
                "fee": 0.1,
                "maker_fee": 0.1,
                "taker_fee": 0.1,
                "total_quantity": 1.0,
                "remaining_quantity": 1.0,
                "source": "None",
                "base_currency_name": "Hummingbot",
                "target_currency_name": "CoinAlpha",
                "base_currency_short_name": "HBOT",
                "target_currency_short_name": "COINALPHA",
                "base_currency_precision": 3,
                "target_currency_precision": 1,
                "avg_price": 0,
                "price_per_unit": 1.0,
                "stop_price": 0,
                "market": "COINALPHAHBOT",
                "time_in_force": "good_till_cancel",
                "created_at": 1654779949609,
                "updated_at": 1654779949609,
            })
        }

        output_queue = asyncio.Queue()

        self.data_source._queue = output_queue
        self.async_run_with_timeout(self.data_source._process_order_update(event_data))

        self.assertEqual(1, output_queue.qsize())
        message = output_queue.get_nowait()
        self.assertIn("channel", message)
        self.assertEqual(CONSTANTS.USER_ORDER_EVENT_TYPE, message["channel"])
        self.assertIn("data", message)
        self.assertEqual(json.loads(event_data["data"]), message["data"])
