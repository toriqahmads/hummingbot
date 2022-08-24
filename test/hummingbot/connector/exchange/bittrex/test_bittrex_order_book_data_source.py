import asyncio
import json
import re
import unittest
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS, bittrex_web_utils as web_utils
from hummingbot.connector.exchange.bittrex.bittrex_api_order_book_data_source import BittrexAPIOrderBookDataSource
from hummingbot.connector.exchange.bittrex.bittrex_utils import _get_timestamp
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BittrexOrderBookDataSourceTest(unittest.TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.symbol = f"{cls.base_asset}{cls.quote_asset}"
        cls.ev_loop = asyncio.get_event_loop()

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.resume_test_event = asyncio.Event()
        self.mocking_assistant = NetworkMockingAssistant()
        self.connector = AsyncMock()
        self.connector.exchange_symbol_associated_to_pair.return_value = self.symbol
        self.connector.trading_pair_associated_to_exchange_symbol.return_value = self.trading_pair
        self.data_source = BittrexAPIOrderBookDataSource(trading_pairs=[self.trading_pair],
                                                         connector=self.connector,
                                                         api_factory=web_utils.build_api_factory())
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

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _unlock_test_with_event(self):
        self.resume_test_event.set()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def _trade_update_event(self):
        resp = {
            "sequence": "int",
            "marketSymbol": self.symbol,
            "deltas": [
                {
                    "id": "string (uuid)",
                    "executedAt": "2018-06-29T08:15:27.243860Z",
                    "quantity": "number (double)",
                    "rate": "number (double)",
                    "takerSide": "string"
                }
            ]
        }
        return resp

    def _order_diff_event(self):
        resp = {
            "marketSymbol": self.symbol,
            "depth": 25,
            "sequence": "17789",
            "bidDeltas": [
                {
                    "quantity": 431.0,
                    "rate": 4.0
                }
            ],
            "askDeltas": [
                {
                    "quantity": 12.03,
                    "rate": 7.8
                }
            ]
        }
        return resp

    def _snapshot_response(self):
        resp = {
            "bid": [
                {
                    "quantity": 431.0,
                    "rate": 4.0
                }
            ],
            "ask": [
                {
                    "quantity": 12.2,
                    "rate": 4.002
                }
            ]
        }
        return resp

    @aioresponses()
    def test_get_new_order_book_successful(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(self.symbol))
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self._snapshot_response()
        mock_api.get(regex_url, body=json.dumps(resp))
        order_book: OrderBook = self.async_run_with_timeout(
            self.data_source.get_new_order_book(self.trading_pair)
        )
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())
        self.assertEqual(1, len(bids))
        self.assertEqual(4.0, bids[0].price)
        self.assertEqual(431, bids[0].amount)
        self.assertEqual(1, len(asks))
        self.assertEqual(4.002, asks[0].price)
        self.assertEqual(12.2, asks[0].amount)

    @aioresponses()
    def test_get_new_order_book_raises_exception(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(self.symbol))
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=400)
        with self.assertRaises(IOError):
            self.async_run_with_timeout(
                self.data_source.get_new_order_book(self.trading_pair)
            )

    @patch("signalr_aio.Connection")
    def test_for_succesful_connect_to_signalr_and_register_hub(self, ws_connect_mock):
        conn_mock = MagicMock()
        ws_connect_mock.return_value = conn_mock
        msgeventhook_mock, errorevent_mock = MagicMock(), MagicMock()
        msgeventhook_mock.__iadd__.return_value = msgeventhook_mock
        errorevent_mock.__iadd__.return_value = errorevent_mock
        conn_mock.received = msgeventhook_mock
        conn_mock.error = errorevent_mock
        conn_mock.start.side_effect = self._unlock_test_with_event()
        self.listening_task = self.ev_loop.create_task(
            self.data_source._connected_websocket_assistant()
        )
        self.async_run_with_timeout(self.resume_test_event.wait())
        conn_mock.register_hub.assert_called_with("c3")
        msgeventhook_mock.__iadd__.assert_called_with(self.data_source._handle_message)
        msgeventhook_mock.__iadd__.assert_called_once()
        errorevent_mock.__iadd__.assert_called_with(self.data_source._handle_error)
        errorevent_mock.__iadd__.assert_called_once()

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_order_book_data_source.BittrexAPIOrderBookDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs(self, invoke_mock, ws_connect_mock):
        conn_mock = MagicMock()
        conn_mock.register_hub.return_value = MagicMock()
        conn_mock.start.side_effect = self._unlock_test_with_event()
        ws_connect_mock.return_value = conn_mock
        sub_channels = [f"trade_{self.symbol}", f"orderbook_{self.symbol}_500"]
        invoke_mock.return_value = [
            {"Success": True, "ErrorCode": None},
            {"Success": True, "ErrorCode": None},
        ]
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())
        self.async_run_with_timeout(self.resume_test_event.wait())
        invoke_mock.assert_called_with("Subscribe", [sub_channels])
        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to public order book and trade channels..."
        ))

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_order_book_data_source.BittrexAPIOrderBookDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_listen_for_subscriptions_raises_cancel_exception(self, invoke_mock, ws_connect_mock):
        conn_mock = MagicMock()
        conn_mock.register_hub.return_value = MagicMock()
        conn_mock.start.side_effect = self._unlock_test_with_event()
        ws_connect_mock.return_value = conn_mock
        invoke_mock.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

    @patch("signalr_aio.Connection")
    @patch("hummingbot.connector.exchange.bittrex.bittrex_api_order_book_data_source.BittrexAPIOrderBookDataSource."
           "_invoke", new_callable=AsyncMock)
    def test_listen_for_subscriptions_logs_exception_details(self, invoke_mock, ws_connect_mock):
        conn_mock = MagicMock()
        conn_mock.register_hub.return_value = MagicMock()
        conn_mock.start.side_effect = self._unlock_test_with_event()
        ws_connect_mock.return_value = conn_mock
        invoke_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(Exception("TEST ERROR."))
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())
        self.async_run_with_timeout(self.resume_test_event.wait())
        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Failed to subscribe to public order book and trade channels..."))

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
        incomplete_resp = {
            "sequence": "int",
            "marketSymbol": self.symbol,
        }
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
        trade_event = self._trade_update_event()
        test_timestamp = _get_timestamp(trade_event["deltas"][0]["executedAt"])
        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue
        msg_queue: asyncio.Queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_trades(self.ev_loop, msg_queue))
        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(trade_event["deltas"][0]["id"], msg.trade_id)
        self.assertEqual(test_timestamp, msg.timestamp)

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
            "marketSymbol": self.symbol,
            "depth": 25,
            "sequence": "int",
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

    @patch("time.time")
    def test_listen_for_order_book_diffs_successful(self, time_mock):
        mock_queue = AsyncMock()
        time_mock.return_value = 1658733281.707259
        diff_event = self._order_diff_event()
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue
        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue))

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(1658733281.707259, msg.timestamp)
        self.assertEqual(1658733281, msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(1, len(bids))
        self.assertEqual(4.0, bids[0].price)
        self.assertEqual(431.0, bids[0].amount)
        self.assertEqual(1658733281, bids[0].update_id)
        self.assertEqual(1, len(asks))
        self.assertEqual(7.8, asks[0].price)
        self.assertEqual(12.03, asks[0].amount)
        self.assertEqual(1658733281, asks[0].update_id)

    @aioresponses()
    def test_listen_for_order_book_snapshots_cancelled_when_fetching_snapshot(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(self.symbol))
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        with self.assertRaises(asyncio.CancelledError):
            self.async_run_with_timeout(
                self.data_source.listen_for_order_book_snapshots(self.ev_loop, asyncio.Queue())
            )

    @aioresponses()
    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    def test_listen_for_order_book_snapshots_log_exception(self, mock_api, sleep_mock):
        msg_queue: asyncio.Queue = asyncio.Queue()
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(self.symbol))
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=Exception)

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )
        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged("ERROR", f"Unexpected error fetching order book snapshot for {self.trading_pair}."))

    @aioresponses()
    @patch("time.time")
    def test_listen_for_order_book_snapshots_successful(self, mock_api, time_mock):
        msg_queue: asyncio.Queue = asyncio.Queue()
        time_mock.return_value = 1658733281.707259
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(self.symbol))
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = self._snapshot_response()

        mock_api.get(regex_url, body=json.dumps(resp))

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(1658733281.707259, msg.timestamp)
        self.assertEqual(1658733281, msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(1, len(bids))
        self.assertEqual(4.0, bids[0].price)
        self.assertEqual(431.0, bids[0].amount)
        self.assertEqual(1658733281, bids[0].update_id)
        self.assertEqual(1, len(asks))
        self.assertEqual(4.002, asks[0].price)
        self.assertEqual(12.2, asks[0].amount)
        self.assertEqual(1658733281, asks[0].update_id)
