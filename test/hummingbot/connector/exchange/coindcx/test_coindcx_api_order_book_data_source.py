import asyncio
import json
import re
import unittest
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoindcxAPIOrderBookDataSource
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook, OrderBookMessage, OrderBookRow
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class CoinDCXAPIOrderBookDataSourceUnitTests(unittest.TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ecode_ex_trading_pair = f"B-{cls.base_asset}_{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"

    @classmethod
    def tearDownClass(cls) -> None:
        for task in asyncio.all_tasks(loop=cls.ev_loop):
            task.cancel()

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.connector = CoindcxExchange(
            coindcx_api_key="", coindcx_secret_key="", trading_pairs=[self.trading_pair], trading_required=False
        )
        self.data_source = CoindcxAPIOrderBookDataSource(trading_pairs=[self.trading_pair], connector=self.connector)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))
        self.connector._set_trading_pair_ecode_symbol_map(bidict({self.ecode_ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def mock_sio_client_connected(self):
        self.data_source._ws_assistant.connected = True
        self.data_source._ws_assistant.eio.state = "connected"

    def simulate_receiving_updates(self, orderbook_depth_message, orderbook_trade_message):
        self.ev_loop.run_until_complete(self.data_source._on_depth_update(orderbook_depth_message))
        self.ev_loop.run_until_complete(self.data_source._on_new_trade(orderbook_trade_message))
        self.resume_test_event.set()

    @aioresponses()
    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.CoindcxAPIOrderBookDataSource"
           "._time")
    def test_get_new_order_book_successful(self, mock_api, mock_time):
        mock_time.return_value = 1
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=CONSTANTS.PUBLIC_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = {
            "bids": {
                "11570.67000000": "0.000871",
                "11570.58000000": "0.001974",
                "11570.02000000": "0.280293",
                "11570.00000000": "5.929216",
                "11569.91000000": "0.000871",
                "11569.89000000": "0.0016",
            },
            "asks": {
                "13900.00000000": "27.04094600",
                "13100.00000000": "15.48547100",
                "12800.00000000": "36.93142200",
                "12200.00000000": "92.04554800",
                "12000.00000000": "72.66595000",
                "11950.00000000": "17.16624600",
            },
        }

        mock_api.get(regex_url, body=json.dumps(resp))

        order_book: OrderBook = self.async_run_with_timeout(self.data_source.get_new_order_book(self.trading_pair))

        expected_update_id = 1 * 1e3

        self.assertEqual(expected_update_id, order_book.snapshot_uid)
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())
        self.assertEqual(6, len(bids))
        self.assertEqual(6, len(asks))
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(expected_update_id, asks[0].update_id)
        self.assertEqual(11570.67, bids[0].price)
        self.assertEqual(11950.00, asks[0].price)

    @aioresponses()
    def test_get_new_order_book_raises_exception(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=CONSTANTS.PUBLIC_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, status=400)
        with self.assertRaises(IOError):
            self.async_run_with_timeout(self.data_source.get_new_order_book(self.trading_pair))

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.CoindcxAPIOrderBookDataSource"
           "._create_async_client")
    def test_listen_for_subscriptions_configures_event_listeners(self, mock_client_creation):
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

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        try:
            self.ev_loop.run_until_complete(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertIn("connect", configured_events)
        self.assertEqual(configured_events["connect"], self.data_source._subscribe_channels)
        self.assertIn(CONSTANTS.ORDER_BOOK_TRADE_EVENT_TYPE, configured_events)
        self.assertEqual(configured_events[CONSTANTS.ORDER_BOOK_TRADE_EVENT_TYPE], self.data_source._on_new_trade)
        self.assertIn(CONSTANTS.ORDER_BOOK_DEPTH_EVENT_TYPE, configured_events)
        self.assertEqual(configured_events[CONSTANTS.ORDER_BOOK_DEPTH_EVENT_TYPE], self.data_source._on_depth_update)
        self.assertIn("connect_error", configured_events)
        self.assertEqual(configured_events["connect_error"], self.data_source._handle_error)

        self.assertEqual(CONSTANTS.WSS_URL, connection_url[0])

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.CoindcxAPIOrderBookDataSource"
           "._create_async_client")
    def test_listen_for_subscriptions_raises_cancel_exception(self, mock_client_creation):
        mock_client_creation.side_effect = asyncio.CancelledError

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        with self.assertRaises(asyncio.CancelledError):
            self.async_run_with_timeout(self.listening_task)

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.CoindcxAPIOrderBookDataSource"
           "._create_async_client")
    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.CoindcxAPIOrderBookDataSource"
           "._sleep")
    def test_listen_for_subscriptions_raises_and_logs_exceptions(self, _, mock_client_creation):
        mock_client_creation.side_effect = [Exception("Test Error"), asyncio.CancelledError]

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

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
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds... "
                "Error: Test Error")
        )

    def test_subscribe_channels_subscribes_to_trades_and_order_diffs(self):
        all_subscriptions = []
        mock_ws = AsyncMock()
        mock_ws.emit.side_effect = lambda event, payload: all_subscriptions.append((event, payload))

        self.data_source._ws_assistant = mock_ws

        self.listening_task = self.ev_loop.create_task(self.data_source._subscribe_channels())
        self.async_run_with_timeout(self.listening_task)

        self.assertEqual(("join", {"channelName": self.ecode_ex_trading_pair}), all_subscriptions[0])

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
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book trading and delta streams...")
        )

    def test_listen_for_trades_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_trades_logs_exception(self):
        incomplete_resp = {"e": "trade"}

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_trades(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public trade updates from exchange"))

    def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        trade_event = {
            "e": "trade",
            "E": 1654745605544,
            "s": self.ex_trading_pair,
            "t": 1396715338,
            "p": "30244.22000000",
            "q": "0.00660000",
            "b": 10909599812,
            "a": 10909599074,
            "T": 1654745605543,
            "m": False,
            "M": True,
            "channel": self.ecode_ex_trading_pair,
            "type": "new-trade"}

        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_trades(self.ev_loop, msg_queue)
        )

        trade_message: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.TRADE, trade_message.type)
        self.assertEqual(trade_event["T"], trade_message.trade_id)
        self.assertEqual(trade_event["T"] * 1e-3, trade_message.timestamp)
        self.assertEqual(self.trading_pair, trade_message.trading_pair)
        self.assertEqual(float(TradeType.BUY.value), trade_message.content["trade_type"])
        self.assertEqual(float(trade_event["q"]), trade_message.content["amount"])
        self.assertEqual(float(trade_event["p"]), trade_message.content["price"])

    def test_on_new_trade(self):
        event_data = {
            "e": "trade",
            "E": 1654745605544,
            "s": self.ex_trading_pair,
            "t": 1396715338,
            "p": "30244.22000000",
            "q": "0.00660000",
            "b": 10909599812,
            "a": 10909599074,
            "T": 1654745605543,
            "m": False,
            "M": True,
            "channel": self.ecode_ex_trading_pair,
            "type": "new-trade"}

        trade_event = {
            'event': 'new-trade',
            'data': json.dumps(event_data)
        }

        self.async_run_with_timeout(self.data_source._on_new_trade(trade_event))
        trade_message = self.async_run_with_timeout(
            self.data_source._message_queue[self.data_source._trade_messages_queue_key].get())

        self.assertEqual(event_data, trade_message)

    def test_listen_for_order_book_diffs_cancelled(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_order_book_diffs_logs_exception(self):
        incomplete_resp = {
            "E": 1654746988919,
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public order book updates from exchange"))

    def test_listen_for_order_book_diffs_successful(self):
        mock_queue = AsyncMock()
        diff_event = {
            "E": 1654746988919,
            "s": "BTCUSDT",
            "b": [["30281.55000000", "5.49561000"]],
            "a": [["30281.56000000", "0.53694000"]],
            "type": "depth-update",
            "replace": True,
            "channel": self.ecode_ex_trading_pair
        }
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(diff_event["E"], msg.update_id)
        self.assertEqual(diff_event["E"] * 1e-3, msg.timestamp)
        self.assertEqual(self.trading_pair, msg.trading_pair)
        bid: OrderBookRow = msg.bids[0]
        self.assertEqual(float(diff_event["b"][0][0]), bid.price)
        self.assertEqual(float(diff_event["b"][0][1]), bid.amount)
        ask: OrderBookRow = msg.asks[0]
        self.assertEqual(float(diff_event["a"][0][0]), ask.price)
        self.assertEqual(float(diff_event["a"][0][1]), ask.amount)

    def test_on_depth_update(self):
        event_data = {
            "E": 1654746988919,
            "s": "BTCUSDT",
            "b": [["30281.55000000", "5.49561000"]],
            "a": [["30281.56000000", "0.53694000"]],
            "type": "depth-update",
            "replace": True,
            "channel": self.ecode_ex_trading_pair
        }

        depth_event = {
            "event": "depth-update",
            "data": json.dumps(event_data)}

        self.async_run_with_timeout(self.data_source._on_depth_update(depth_event))
        snapshot_message = self.async_run_with_timeout(
            self.data_source._message_queue[self.data_source._diff_messages_queue_key].get())

        self.assertEqual(event_data, snapshot_message)
