import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from async_timeout import timeout

from hummingbot.connector.exchange.gate_io import gate_io_constants as CONSTANTS
from hummingbot.connector.exchange.gate_io.gate_io_api_order_book_data_source import GateIoAPIOrderBookDataSource
from hummingbot.connector.exchange.gate_io.gate_io_auth import GateIoAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OpenOrder, OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.logger import HummingbotLogger

ctce_logger = None
s_decimal_NaN = Decimal("nan")


class GateIoExchange(ExchangeBase):
    """
    GateIoExchange connects with Gate.io exchange and provides order book pricing, user account tracking and
    trading functionality.
    """
    ORDER_NOT_EXIST_CONFIRMATION_COUNT = 3
    ORDER_NOT_EXIST_CANCEL_COUNT = 2

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ctce_logger
        if ctce_logger is None:
            ctce_logger = logging.getLogger(__name__)
        return ctce_logger

    def __init__(self,
                 gate_io_api_key: str,
                 gate_io_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = DEFAULT_DOMAIN):
        """
        :param gate_io_api_key: The API key to connect to private Gate.io APIs.
        :param gate_io_secret_key: The API secret.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        self._gate_io_api_key = gate_io_api_key
        self._gate_io_secret_key = gate_io_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._gate_io_auth = GateIoAuth(gate_io_api_key, gate_io_secret_key)
        self._throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self._api_factory = build_gate_io_api_factory(throttler=self._throttler)
        self._rest_assistant: Optional[RESTAssistant] = None
        self._set_order_book_tracker(GateIoOrderBookTracker(
            self._throttler, trading_pairs, self._api_factory
        ))
        self._user_stream_tracker = GateIoUserStreamTracker(
            self._gate_io_auth, trading_pairs, self._api_factory
        )
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._in_flight_orders = {}  # Dict[client_order_id:str, GateIoInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._real_time_balance_update = False
        self._update_balances_fetching = False
        self._update_balances_queued = False
        self._update_balances_finished = asyncio.Event()

        super().__init__()

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self.order_book_tracker.order_books

    @property
    def name(self) -> str:
        return "gate_io"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "user_stream_initialized":
                self._user_stream_tracker.data_source.last_recv_time > 0 if self._trading_required else True,
        }

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.SYMBOL_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.NETWORK_CHECK_PATH_URL

    def supported_order_types(self):
        return [OrderType.LIMIT]

    def get_taker_order_type(self):
        return OrderType.LIMIT

    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)

    async def start_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """
        self.order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def stop_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        """
        # Resets timestamps for status_polling_task
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._poll_notifier = asyncio.Event()
        # Reset balance queue
        self._update_balances_fetching = False
        self._update_balances_queued = False
        self._update_balances_finished = asyncio.Event()

        self.order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            # since there is no ping endpoint, the lowest rate call is to get BTC-USD symbol
            endpoint = CONSTANTS.NETWORK_CHECK_PATH_URL
            request = GateIORESTRequest(
                method=RESTMethod.GET, endpoint=endpoint, throttler_limit_id=endpoint
            )
            await self._api_request(request)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(CONSTANTS.INTERVAL_TRADING_RULES)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg=("Could not fetch new trading rules from "
                                                       f"{CONSTANTS.EXCHANGE_NAME}. Check network connection."))
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        endpoint = CONSTANTS.SYMBOL_PATH_URL
        request = GateIORESTRequest(
            method=RESTMethod.GET, endpoint=endpoint, throttler_limit_id=endpoint
        )

    async def _format_trading_rules(self, raw_trading_pair_info: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.

        :param raw_trading_pair_info: The json API response
        :return A dictionary of trading rules.

        Example raw_trading_pair_info:
        https://www.gate.io/docs/apiv4/en/#list-all-currency-pairs-supported
        """
        result = []
        for rule in raw_trading_pair_info:
            try:
                if not web_utils.is_exchange_information_valid(rule):
                    continue

                trading_pair = await self._orderbook_ds.trading_pair_associated_to_exchange_symbol(rule["id"])

                min_amount_inc = Decimal(f"1e-{rule['amount_precision']}")
                min_price_inc = Decimal(f"1e-{rule['precision']}")
                min_amount = Decimal(str(rule.get("min_base_amount", min_amount_inc)))
                min_notional = Decimal(str(rule.get("min_quote_amount", min_price_inc)))
                result.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_amount,
                        min_price_increment=min_price_inc,
                        min_base_amount_increment=min_amount_inc,
                        min_notional_size=min_notional,
                        min_order_value=min_notional,
                    )
                )
            except Exception:
                self.logger().error(
                    f"Error parsing the trading pair rule {rule}. Skipping.", exc_info=True)
        return result

    async def _api_request(self, request: GateIORESTRequest) -> Dict[str, Any]:
        rest_assistant: RESTAssistant = await self._get_rest_assistant()
        response = await api_call_with_retries(
            request, rest_assistant, self._throttler, self.logger(), self._gate_io_auth
        )
        return response

    def get_order_price_quantum(self, trading_pair: str, price: Decimal):
        """
        Returns a price step, a minimum price increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        """
        Returns an order amount step, a minimum amount increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def get_order_book(self, trading_pair: str) -> OrderBook:
        if trading_pair not in self.order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self.order_book_tracker.order_books[trading_pair]

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID,
            max_id_len=CONSTANTS.MAX_ID_LEN,
        )
        safe_ensure_future(self._create_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price))
        return order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID,
            max_id_len=CONSTANTS.MAX_ID_LEN,
        )
        if order_result.get("status") in {"cancelled"}:
            raise web_utils.APIError({"label": "ORDER_REJECTED", "message": "Order rejected."})
        exchange_order_id = str(order_result["id"])
        return exchange_order_id, time.time()

    async def _place_cancel(self, order_id, tracked_order):
        """
        This implementation-specific method is called by _cancel
        returns True if successful
        """
        canceled = False
        exchange_order_id = await tracked_order.get_exchange_order_id()
        params = {
            'currency_pair': await self._orderbook_ds.exchange_symbol_associated_to_pair(tracked_order.trading_pair)
        }
        resp = await self._api_delete(
            path_url=CONSTANTS.ORDER_DELETE_PATH_URL.format(order_id=exchange_order_id),
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_DELETE_LIMIT_ID,
        )
        canceled = resp.get("status") == "cancelled"
        return canceled

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        account_info = ""
        try:
            account_info = await self._api_get(
                path_url=CONSTANTS.USER_BALANCES_PATH_URL,
                is_auth_required=True,
                limit_id=CONSTANTS.USER_BALANCES_PATH_URL
            )
            self._process_balance_message(account_info)
        except Exception as e:
            self.logger().network(
                f"Unexpected error while fetching balance update - {str(e)}", exc_info=True,
                app_warning_msg=(f"Could not fetch balance update from {self.name_cap}"))
            raise e
        return account_info

    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        orders_tasks = []
        trades_tasks = []
        reviewed_orders = []
        tracked_orders = list(self.in_flight_orders.values())

        # Prepare requests to update trades and orders
        for tracked_order in tracked_orders:
            try:
                exchange_order_id = await tracked_order.get_exchange_order_id()
                reviewed_orders.append(tracked_order)
            except asyncio.TimeoutError:
                self.logger().network(
                    f"Skipped order status update for {tracked_order.client_order_id} "
                    "- waiting for exchange order id.")
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)
                continue

            trading_pair = await self._orderbook_ds.exchange_symbol_associated_to_pair(tracked_order.trading_pair)

            trades_tasks.append(self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                params={
                    "currency_pair": trading_pair,
                    "order_id": exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PATH_URL,
            ))
            orders_tasks.append(self._api_get(
                path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(order_id=exchange_order_id),
                params={
                    "currency_pair": trading_pair
                },
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_STATUS_LIMIT_ID,
            ))

        # Process order trades first before processing order statuses
        responses = await safe_gather(*trades_tasks, return_exceptions=True)
        self.logger().debug(f"Polled trade updates for {len(tracked_orders)} orders: {len(responses)}.")
        for response, tracked_order in zip(responses, reviewed_orders):
            if not isinstance(response, Exception):
                for trade_fills in response:
                    self._process_trade_message(trade_fills, tracked_order.client_order_id)
            else:
                self.logger().warning(
                    f"Failed to fetch trade updates for order {tracked_order.client_order_id}. "
                    f"Response: {response}")

        # Process order statuses
        responses = await safe_gather(*orders_tasks, return_exceptions=True)
        self.logger().debug(f"Polled order updates for {len(tracked_orders)} orders: {len(responses)}.")
        for response, tracked_order in zip(responses, tracked_orders):
            if not isinstance(response, Exception):
                self._process_order_message(response)
            else:
                self.logger().warning(
                    f"Failed to fetch order status updates for order {tracked_order.client_order_id}. "
                    f"Response: {response}")
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream_tracker.user_stream queue.
        Traders, Orders, and Balance updates from the WS.
        """
        user_channels = [
            CONSTANTS.USER_TRADES_ENDPOINT_NAME,
            CONSTANTS.USER_ORDERS_ENDPOINT_NAME,
            CONSTANTS.USER_BALANCE_ENDPOINT_NAME,
        ]
        async for event_message in self._iter_user_event_queue():
            channel: str = event_message.get("channel", None)
            results: str = event_message.get("result", None)
            try:
                if channel not in user_channels:
                    self.logger().error(
                        f"Unexpected message in user stream: {event_message}.", exc_info=True)
                    continue

                if channel == CONSTANTS.USER_TRADES_ENDPOINT_NAME:
                    for trade_msg in results:
                        self._process_trade_message(trade_msg)
                elif channel == CONSTANTS.USER_ORDERS_ENDPOINT_NAME:
                    for order_msg in results:
                        self._process_order_message(order_msg)
                elif channel == CONSTANTS.USER_BALANCE_ENDPOINT_NAME:
                    self._process_balance_message_ws(results)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    def _normalise_order_message_state(self, order_msg: Dict[str, Any], tracked_order):
        state = None
        # we do not handle:
        #   "failed" because it is handled by create order
        #   "put" as the exchange order id is returned in the create order response
        #   "open" for same reason

        # same field for both WS and REST
        amount_left = Decimal(order_msg.get("left"))

        # WS
        if "event" in order_msg:
            event_type = order_msg.get("event")
            if event_type == "update":
                state = OrderState.FILLED
                if amount_left > 0:
                    state = OrderState.PARTIALLY_FILLED
            if event_type == "finish":
                state = OrderState.FILLED
                if amount_left > 0:
                    state = OrderState.CANCELED
        else:
            status = order_msg.get("status")
            if status == "closed":
                state = OrderState.FILLED
                if amount_left > 0:
                    state = OrderState.PARTIALLY_FILLED
            if status == "cancelled":
                state = OrderState.CANCELED
        return state

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancelation or failure event if needed.

        :param order_msg: The order response from either REST or web socket API (they are of the same format)

        Example Order:
        https://www.gate.io/docs/apiv4/en/#list-orders
        """
        state = None
        client_order_id = str(order_msg.get("text", ""))
        tracked_order = self.in_flight_orders.get(client_order_id, None)
        if not tracked_order:
            self.logger().debug(f"Ignoring order message with id {client_order_id}: not in in_flight_orders.")
            return

        state = self._normalise_order_message_state(order_msg, tracked_order)
        if state:
            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=int(order_msg["update_time"]),
                new_state=state,
                client_order_id=client_order_id,
                exchange_order_id=str(order_msg["id"]),
            )
            self._order_tracker.process_order_update(order_update=order_update)
            self.logger().info(f"Successfully updated order {tracked_order.client_order_id}.")

    def _process_trade_message(self, trade: Dict[str, Any], client_order_id: Optional[str] = None):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        Example Trade:
        https://www.gate.io/docs/apiv4/en/#retrieve-market-trades
        """
        client_order_id = client_order_id or str(trade["text"])
        tracked_order = self.in_flight_orders.get(client_order_id, None)
        if not tracked_order:
            self.logger().debug(f"Ignoring trade message with id {client_order_id}: not in in_flight_orders.")
            return

        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=tracked_order.trade_type,
            percent_token=trade["fee_currency"],
            flat_fees=[TokenAmount(
                amount=Decimal(trade["fee"]),
                token=trade["fee_currency"]
            )]
        )
        trade_update = TradeUpdate(
            trade_id=str(trade["id"]),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fee=fee,
            fill_base_amount=Decimal(trade["amount"]),
            fill_quote_amount=Decimal(trade["amount"]) * Decimal(trade["price"]),
            fill_price=Decimal(trade["price"]),
            fill_timestamp=trade["create_time"],
        )
        self._order_tracker.process_trade_update(trade_update)

    def _process_balance_message(self, balance_update):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for account in balance_update:
            asset_name = account["currency"]
            self._account_available_balances[asset_name] = Decimal(str(account["available"]))
            self._account_balances[asset_name] = Decimal(str(account["locked"])) + Decimal(str(account["available"]))
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _process_balance_message_ws(self, balance_update):
        for account in balance_update:
            asset_name = account["currency"]
            self._account_available_balances[asset_name] = Decimal(str(account["available"]))
            self._account_balances[asset_name] = Decimal(str(account["total"]))

        self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self._in_flight_orders.items()}
        self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all in-flight orders and waits for cancellation results.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :param timeout_seconds: The timeout at which the operation will be canceled.
        :returns List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        if self._trading_pairs is None:
            raise Exception("cancel_all can only be used when trading_pairs are specified.")
        open_orders = [o for o in self._in_flight_orders.values() if not o.is_done]
        if len(open_orders) == 0:
            return []
        tasks = [self._execute_cancel(o.trading_pair, o.client_order_id) for o in open_orders]
        cancellation_results = []
        cancel_timeout = timeout_seconds * len(open_orders) if len(open_orders) else timeout_seconds
        try:
            async with timeout(cancel_timeout):
                cancellation_results = await safe_gather(*tasks, return_exceptions=False)
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.", exc_info=True,
                app_warning_msg=(f"Failed to cancel all orders on {CONSTANTS.EXCHANGE_NAME}. "
                                 "Check API key and network connection.")
            )
        return cancellation_results

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        # Using 120 seconds here as Gate.io websocket is quiet
        poll_interval = (CONSTANTS.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 120.0
                         else CONSTANTS.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.", exc_info=True,
                    app_warning_msg=(f"Could not fetch user events from {CONSTANTS.EXCHANGE_NAME}. "
                                     "Check API key and network connection."))
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue. The messages are put in by
        GateIoAPIUserStreamDataSource.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                user_channels = [
                    CONSTANTS.USER_TRADES_ENDPOINT_NAME,
                    CONSTANTS.USER_ORDERS_ENDPOINT_NAME,
                    CONSTANTS.USER_BALANCE_ENDPOINT_NAME,
                ]

                channel: str = event_message.get("channel", None)
                results: str = event_message.get("result", None)

                if channel not in user_channels:
                    self.logger().error(f"Unexpected message in user stream: {event_message}.", exc_info=True)
                    continue
                if channel == CONSTANTS.USER_TRADES_ENDPOINT_NAME:
                    for trade_msg in results:
                        self._process_trade_message(trade_msg)
                elif channel == CONSTANTS.USER_ORDERS_ENDPOINT_NAME:
                    for order_msg in results:
                        self._process_order_message(order_msg)
                elif channel == CONSTANTS.USER_BALANCE_ENDPOINT_NAME:
                    self._process_balance_message_ws(results)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    # This is currently unused, but looks like a future addition.
    async def get_open_orders(self) -> List[OpenOrder]:
        endpoint = CONSTANTS.USER_ORDERS_PATH_URL
        request = GateIORESTRequest(
            method=RESTMethod.GET,
            endpoint=endpoint,
            is_auth_required=True,
            throttler_limit_id=endpoint,
        )
        result = await self._api_request(request)
        ret_val = []
        for pair_orders in result:
            for order in pair_orders["orders"]:
                if CONSTANTS.HBOT_ORDER_ID not in order["text"]:
                    continue
                if order["type"] != OrderType.LIMIT.name.lower():
                    self.logger().info(f"Unsupported order type found: {order['type']}")
                    continue
                ret_val.append(
                    OpenOrder(
                        client_order_id=order["text"],
                        trading_pair=convert_from_exchange_trading_pair(order["currency_pair"]),
                        price=Decimal(str(order["price"])),
                        amount=Decimal(str(order["amount"])),
                        executed_amount=Decimal(str(order["filled_total"])),
                        status=order["status"],
                        order_type=OrderType.LIMIT,
                        is_buy=True if order["side"].lower() == TradeType.BUY.name.lower() else False,
                        time=int(order["create_time"]),
                        exchange_order_id=order["id"]
                    )
                )
        return ret_val

    async def all_trading_pairs(self) -> List[str]:
        # This method should be removed and instead we should implement _initialize_trading_pair_symbol_map
        return await GateIoAPIOrderBookDataSource.fetch_trading_pairs()

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        # This method should be removed and instead we should implement _get_last_traded_price
        return await GateIoAPIOrderBookDataSource.get_last_traded_prices(trading_pairs=trading_pairs)
