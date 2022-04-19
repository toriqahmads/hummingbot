import asyncio
import json
import logging
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd
from bidict import bidict

from hummingbot.connector.exchange.gate_io import gate_io_constants as CONSTANTS
from hummingbot.connector.exchange.gate_io.gate_io_active_order_tracker import GateIoActiveOrderTracker
from hummingbot.connector.exchange.gate_io.gate_io_order_book import GateIoOrderBook
from hummingbot.connector.exchange.gate_io.gate_io_utils import (
    api_call_with_retries,
    build_gate_io_api_factory,
    convert_from_exchange_trading_pair,
    convert_to_exchange_trading_pair,
    GateIoAPIError,
    GateIORESTRequest,
)
from hummingbot.connector.exchange.gate_io.gate_io_websocket import GateIoWebsocket
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger


class GateIoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 trading_pairs: List[str],
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__(trading_pairs)
        self._throttler = throttler or self._get_throttler_instance()
        self._api_factory = api_factory or build_gate_io_api_factory(throttler=self._throttler)
        self._rest_assistant: Optional[RESTAssistant] = None
        self._trading_pairs: List[str] = trading_pairs

        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    @classmethod
    def _get_throttler_instance(cls) -> AsyncThrottler:
        throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        return throttler

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, Decimal]:
        throttler = cls._get_throttler_instance()
        api_factory = build_gate_io_api_factory(throttler=throttler)
        rest_assistant = await api_factory.get_rest_assistant()
        results = {}
        ticker_param = None
        if len(trading_pairs) == 1:
            ticker_param = {'currency_pair': await cls.exchange_symbol_associated_to_pair(trading_pairs[0])}

        tickers = await web_utils.api_request(
            path=CONSTANTS.TICKER_PATH_URL,
            domain=domain,
            params=ticker_param,
            method=RESTMethod.GET,
            limit_id=CONSTANTS.TICKER_PATH_URL
        )
        for trading_pair in trading_pairs:
            ex_pair = await cls.exchange_symbol_associated_to_pair(trading_pair)
            ticker = list([tic for tic in tickers if tic['currency_pair'] == ex_pair])[0]
            results[trading_pair] = Decimal(str(ticker["last"]))
        return results

    @classmethod
    async def fetch_trading_pairs(cls) -> List[str]:
        throttler = cls._get_throttler_instance()
        api_factory = build_gate_io_api_factory(throttler=throttler)
        rest_assistant = await api_factory.get_rest_assistant()
        try:
            async with throttler.execute_task(CONSTANTS.SYMBOL_PATH_URL):
                endpoint = CONSTANTS.SYMBOL_PATH_URL
                request = GateIORESTRequest(
                    method=RESTMethod.GET,
                    endpoint=endpoint,
                    throttler_limit_id=endpoint,
                )
                symbols = await api_call_with_retries(
                    request, rest_assistant, throttler, logging.getLogger()
                )
            trading_pairs = list([convert_from_exchange_trading_pair(sym["id"]) for sym in symbols])
            # Filter out unmatched pairs so nothing breaks
            return [sym for sym in trading_pairs if sym is not None]
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for Gate.io trading pairs
            pass
        return []

    @classmethod
    async def get_order_book_data(
            cls,
            trading_pair: str,
            throttler: Optional[AsyncThrottler] = None,
            rest_assistant: Optional[RESTAssistant] = None,
            logger: Optional[logging.Logger] = None,
    ) -> Dict[str, any]:
        """
        Get whole orderbook
        """
        throttler = throttler or cls._get_throttler_instance()
        api_factory = build_gate_io_api_factory(throttler=throttler)
        rest_assistant = rest_assistant or await api_factory.get_rest_assistant()
        logger = logger or logging.getLogger()
        try:
            endpoint = CONSTANTS.ORDER_BOOK_PATH_URL
            params = {
                "currency_pair": await cls.exchange_symbol_associated_to_pair(trading_pair),
                "with_id": json.dumps(True)
            }
            if not logger:
                logger = cls.logger()
            return await web_utils.api_request(
                path=endpoint,
                method=RESTMethod.GET,
                params=params,
                limit_id=endpoint,
                throttler=throttler or web_utils.create_throttler(),
                retry=True,
                retry_logger=logger,
            )
        except Exception as e:
            raise IOError(
                f"Error fetching OrderBook for {trading_pair} at {CONSTANTS.EXCHANGE_NAME}."
                f" Exception is: {e}")

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        rest_assistant = await self._get_rest_assistant()
        snapshot: Dict[str, Any] = await self.get_order_book_data(
            trading_pair, self._throttler, rest_assistant, self.logger()
        )
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": snapshot["id"],
                "bids": snapshot["bids"],
                "asks": snapshot["asks"],
            },
            timestamp=snapshot_timestamp)
        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)
                await self._subscribe_channels(ws)

                await self._process_ws_messages(websocket_assistant=ws)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket trade channel
        """
        msg_queue = self._message_queue[CONSTANTS.TRADES_ENDPOINT_NAME]
        msg = None
        while True:
            try:
                msg = await msg_queue.get()
                trade_data: Dict[Any] = msg.get("result", None)

                pair: str = await self.trading_pair_associated_to_exchange_symbol(trade_data.get("currency_pair", None))

                if pair is None:
                    continue

                trade_timestamp: int = trade_data['create_time']
                trade_msg: OrderBookMessage = OrderBookMessage(
                    OrderBookMessageType.TRADE,
                    {
                        "trading_pair": pair,
                        "trade_type": (float(TradeType.SELL.value)
                                       if trade_data["side"] == "sell"
                                       else float(TradeType.BUY.value)),
                        "trade_id": trade_data["id"],
                        "update_id": trade_timestamp,
                        "price": trade_data["price"],
                        "amount": trade_data["amount"],
                    },
                    timestamp=trade_timestamp)
                output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    f"Unexpected error while parsing ws trades message {msg}.", exc_info=True
                )
                await self._sleep(5.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook diffs using websocket book channel
        """
        msg_queue = self._message_queue[CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME]
        msg = None
        while True:
            try:
                msg = await msg_queue.get()
                order_book_data: str = msg.get("result", None)

                timestamp: float = (order_book_data["t"]) * 1e-3
                pair: str = await self.trading_pair_associated_to_exchange_symbol(order_book_data["s"])

                orderbook_msg: OrderBookMessage = OrderBookMessage(
                    OrderBookMessageType.DIFF,
                    {
                        "trading_pair": pair,
                        "first_update_id": order_book_data["U"],
                        "update_id": order_book_data["u"],
                        "bids": order_book_data["b"],
                        "asks": order_book_data["a"]
                    },
                    timestamp=timestamp)
                output.put_nowait(orderbook_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    f"Unexpected error while parsing ws order book message {msg}.", exc_info=True
                )
                await self._sleep(5.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook snapshots by fetching orderbook
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_order_book_data(trading_pair, self._throttler)
                        snapshot_timestamp: int = int(time.time())
                        snapshot_msg: OrderBookMessage = OrderBookMessage(
                            OrderBookMessageType.SNAPSHOT,
                            {
                                "trading_pair": trading_pair,
                                "update_id": snapshot["id"],
                                "bids": snapshot["bids"],
                                "asks": snapshot["asks"],
                            },
                            timestamp=snapshot_timestamp)
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")

                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error with WebSocket connection.", exc_info=True,
                            app_warning_msg="Unexpected error with WebSocket connection. Retrying in 5 seconds. "
                                            "Check network connection.")
                        await self._sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await self._sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await self._sleep(5.0)

    async def _process_ws_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if data.get("error") is not None:
                err_msg = data.get("error", {}).get("message", data["error"])
                raise IOError(f"Error event received from the server ({err_msg})")
            elif data.get("event") == "update" and data.get("channel") in [
                CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME,
                CONSTANTS.TRADES_ENDPOINT_NAME,
            ]:
                self._message_queue[data.get("channel")].put_nowait(data)

    @classmethod
    async def _init_trading_pair_symbols(
            cls,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
            api_factory: Optional[WebAssistantsFactory] = None,
            throttler: Optional[AsyncThrottler] = None,
            time_synchronizer: Optional[TimeSynchronizer] = None):
        """
        Initialize mapping of trade symbols in exchange notation to trade symbols in client notation
        """
        mapping = bidict()

        try:
            data = await web_utils.api_request(
                path=CONSTANTS.SYMBOL_PATH_URL,
                api_factory=api_factory,
                throttler=throttler,
                time_synchronizer=time_synchronizer,
                domain=domain,
                method=RESTMethod.GET,
            )
            for sd in data:
                if not web_utils.is_exchange_information_valid(sd):
                    continue
                mapping[sd["id"]] = combine_to_hb_trading_pair(
                    base=sd["base"],
                    quote=sd["quote"])

        except Exception as ex:
            cls.logger().exception(f"There was an error requesting exchange info ({str(ex)})")

        cls._trading_pair_symbol_map[domain] = mapping

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self.exchange_symbol_associated_to_pair(
                    trading_pair=trading_pair,
                    api_factory=self._api_factory,
                    throttler=self._throttler,
                )

                trades_payload = {
                    "time": int(self._time()),
                    "channel": CONSTANTS.TRADES_ENDPOINT_NAME,
                    "event": "subscribe",
                    "payload": [symbol]
                }
                subscribe_trade_request: WSRequest = WSRequest(payload=trades_payload)

                order_book_payload = {
                    "time": int(self._time()),
                    "channel": CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME,
                    "event": "subscribe",
                    "payload": [symbol, "100ms"]
                }
                subscribe_orderbook_request: WSRequest = WSRequest(payload=order_book_payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)

                self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise
        return ws

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant

    def _time(self):
        return time.time()
