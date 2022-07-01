import asyncio
import logging
import time
from decimal import Decimal
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)

import ujson

from bidict import bidict

import hummingbot.connector.exchange.huobi.huobi_constants as CONSTANTS
from hummingbot.connector.exchange.huobi.huobi_api_user_stream_data_source import HuobiAPIUserStreamDataSource
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.connector.exchange.huobi.huobi_auth import HuobiAuth
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.connector.exchange.huobi.huobi_order_book_tracker import HuobiOrderBookTracker
from hummingbot.connector.exchange.huobi.huobi_user_stream_tracker import HuobiUserStreamTracker
from hummingbot.connector.exchange.huobi.huobi_utils import (
    BROKER_ID,
    build_api_factory,
    convert_to_exchange_trading_pair,
    is_exchange_information_valid
)
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    MarketTransactionFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.logger import HummingbotLogger
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


hm_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("NaN")
HUOBI_ROOT_API = "https://api.huobi.pro/v1"


class HuobiExchange(ExchangePyBase):
    
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_ORDER_CANCELED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value
    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDERS_INTERVAL = 10.0
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0


    def __init__(self,
                 huobi_api_key: str,
                 huobi_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):
        self.huobi_api_key = huobi_api_key
        self. huobi_secret_key = huobi_secret_key
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        self._account_id = ""
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._ev_loop = asyncio.get_event_loop()
        self._in_flight_orders = {}
        super().__init__()
        self._set_order_book_tracker(HuobiOrderBookTracker(
            trading_pairs=trading_pairs,
            api_factory=self._web_assistants_factory,
        ))

    @property
    def name(self) -> str:
        return "huobi"
    
    @property
    def authenticator(self):
        HuobiAuth(api_key=self.huobi_api_key, secret_key=self.huobi_secret_key)
    
    @property
    def rate_limits_rules(self):
        pass
    @property
    def domain(self):
        pass

    @property
    def client_order_id_max_length(self):
        pass

    @property
    def client_order_id_prefix(self):
        pass

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.SYMBOLS_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.SYMBOLS_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs
    
    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required
    
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return build_api_factory()
    
    def _create_order_book_data_source(self):
        pass

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return HuobiAPIUserStreamDataSource(huobi_auth=self._auth,
                                                             api_factory=self._web_assistants_factory)



    async def _update_account_id(self) -> str:
        accounts = await self._api_request("get", path_url=CONSTANTS.ACCOUNT_ID_URL, is_auth_required=True)
        try:
            for account in accounts:
                if account["state"] == "working" and account["type"] == "spot":
                    self._account_id = str(account["id"])
        except Exception as e:
            raise ValueError(f"Unable to retrieve account id: {e}")

    async def _update_balances(self):

        new_available_balances = {}
        new_balances = {}
        if not self._account_id:
            await self._update_account_id()
        data = await self._api_get(path_url=CONSTANTS.ACCOUNT_BALANCE_URL.format(self._account_id),
                                       is_auth_required=True)
        balances = data.get("list", [])
        if len(balances) > 0:
            for balance_entry in balances:
                asset_name = balance_entry["currency"].upper()
                balance = Decimal(balance_entry["balance"])
                if balance == s_decimal_0:
                    continue
                if asset_name not in new_available_balances:
                    new_available_balances[asset_name] = s_decimal_0
                if asset_name not in new_balances:
                    new_balances[asset_name] = s_decimal_0

                new_balances[asset_name] += balance
                if balance_entry["type"] == "trade":
                    new_available_balances[asset_name] = balance

            self._account_available_balances.clear()
            self._account_available_balances = new_available_balances
            self._account_balances.clear()
            self._account_balances = new_balances
            
    
    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None):
            
            is_maker = order_type is OrderType.LIMIT_MAKER
            return estimate_fee("huobi", is_maker)

            

    def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        trading_rules = []

        for info in raw_trading_pair_info:
            try:
                base_asset = info["base-currency"]
                quote_asset = info["quote-currency"]
                trading_rules.append(
                    TradingRule(trading_pair=f"{base_asset}-{quote_asset}".upper(),
                                min_order_size=Decimal(info["min-order-amt"]),
                                max_order_size=Decimal(info["max-order-amt"]),
                                min_price_increment=Decimal(f"1e-{info['price-precision']}"),
                                min_base_amount_increment=Decimal(f"1e-{info['amount-precision']}"),
                                min_quote_amount_increment=Decimal(f"1e-{info['value-precision']}"),
                                min_notional_size=Decimal(info["min-order-value"]))
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {info}. Skipping.", exc_info=True)
        return trading_rules

    async def get_order_status(self, exchange_order_id: str) -> Dict[str, Any]:
        """
        Example:
        {
            "id": 59378,
            "symbol": "ethusdt",
            "account-id": 100009,
            "amount": "10.1000000000",
            "price": "100.1000000000",
            "created-at": 1494901162595,
            "type": "buy-limit",
            "field-amount": "10.1000000000",
            "field-cash-amount": "1011.0100000000",
            "field-fees": "0.0202000000",
            "finished-at": 1494901400468,
            "user-id": 1000,
            "source": "api",
            "state": "filled",
            "canceled-at": 0,
            "exchange": "huobi",
            "batch": ""
        }
        """
        path_url = CONSTANTS.ORDER_DETAIL_URL.format(exchange_order_id)
        params = {
            "order_id": exchange_order_id
        }
        return await self._api_get(path_url=path_url, params=params, is_auth_required=True)

    async def _update_order_status(self):

        tracked_orders = list(self._in_flight_orders.values())
        for tracked_order in tracked_orders:
                exchange_order_id = await tracked_order.get_exchange_order_id()
                order_update = await self.get_order_status(exchange_order_id)
                if order_update is None:
                    self.logger().network(
                        f"Error fetching status update for the order {tracked_order.client_order_id}: "
                        f"{order_update}.",
                        app_warning_msg=f"Could not fetch updates for the order {tracked_order.client_order_id}. "
                                        f"The order has either been filled or canceled."
                    )
                    continue

                order_state = order_update["state"]
                # possible order states are "submitted", "partial-filled", "filled", "canceled"

                if order_state not in ["submitted", "filled", "canceled"]:
                    self.logger().debug(f"Unrecognized order update response - {order_update}")

                # Calculate the newly executed amount for this update.
                tracked_order.last_state = order_state
                new_confirmed_amount = Decimal(order_update["field-amount"])  # probably typo in API (filled)
                execute_amount_diff = new_confirmed_amount - tracked_order.executed_amount_base

                if execute_amount_diff > s_decimal_0:
                    tracked_order.executed_amount_base = new_confirmed_amount
                    tracked_order.executed_amount_quote = Decimal(order_update["field-cash-amount"])
                    tracked_order.fee_paid = Decimal(order_update["field-fees"])
                    execute_price = Decimal(order_update["field-cash-amount"]) / new_confirmed_amount
                    order_filled_event = OrderFilledEvent(
                        self._current_timestamp,
                        tracked_order.client_order_id,
                        tracked_order.trading_pair,
                        tracked_order.trade_type,
                        tracked_order.order_type,
                        execute_price,
                        execute_amount_diff,
                        self._get_fee(
                            tracked_order.base_asset,
                            tracked_order.quote_asset,
                            tracked_order.order_type,
                            tracked_order.trade_type,
                            execute_price,
                            execute_amount_diff,
                        ),
                        # Unique exchange trade ID not available in client order status
                        # But can use validate an order using exchange order ID:
                        # https://huobiapi.github.io/docs/spot/v1/en/#query-order-by-order-id
                        exchange_trade_id=str(int(self._time() * 1e6))
                    )
                    self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                                       f"order {tracked_order.client_order_id}.")

                if tracked_order.is_open:
                    continue

                if tracked_order.is_done:
                    if not tracked_order.is_cancelled:  # Handles "filled" order
                        self.stop_tracking_order(tracked_order.client_order_id)
                        if tracked_order.trade_type is TradeType.BUY:
                            self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                        else:
                            self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                    else:  # Handles "canceled" or "partial-canceled" order
                        self.stop_tracking_order(tracked_order.client_order_id)
                        self.logger().info(f"The market order {tracked_order.client_order_id} "
                                           f"has been canceled according to order status API.")
                        

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        """
        Called by _user_stream_event_listener.
        """
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unknown error. Retrying after 1 second. {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for stream_message in self._iter_user_event_queue():
            try:
                channel = stream_message.get("ch", None)
                if channel not in CONSTANTS.HUOBI_SUBSCRIBE_TOPICS:
                    continue

                data = stream_message["data"]
                if len(data) == 0 and stream_message["code"] == 200:
                    # This is a subcribtion confirmation.
                    self.logger().info(f"Successfully subscribed to {channel}")
                    continue

                if channel == CONSTANTS.HUOBI_ACCOUNT_UPDATE_TOPIC:
                    asset_name = data["currency"].upper()
                    balance = data["balance"]
                    available_balance = data["available"]

                    self._account_balances.update({asset_name: Decimal(balance)})
                    self._account_available_balances.update({asset_name: Decimal(available_balance)})
                elif channel == CONSTANTS.HUOBI_ORDER_UPDATE_TOPIC:
                    safe_ensure_future(self._process_order_update(data))
                elif channel == CONSTANTS.HUOBI_TRADE_DETAILS_TOPIC:
                    safe_ensure_future(self._process_trade_event(data))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop. {e}", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_order_update(self, order_update: Dict[str, Any]):
        order_id = order_update["orderId"]
        client_order_id = order_update["clientOrderId"]
        trading_pair = order_update["symbol"]
        order_status = order_update["orderStatus"]

        if order_status not in ["submitted", "partial-filled", "filled", "partially-canceled", "canceled", "canceling"]:
            self.logger().debug(f"Unrecognized order update response - {order_update}")

        tracked_order = self._in_flight_orders.get(client_order_id, None)

        if tracked_order is None:
            return

        if order_status == "filled":
            tracked_order.last_state = order_status

            event = (self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG
                     if tracked_order.trade_type == TradeType.BUY
                     else self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG)
            event_class = (BuyOrderCompletedEvent
                           if tracked_order.trade_type == TradeType.BUY
                           else SellOrderCompletedEvent)

            try:
                await asyncio.wait_for(tracked_order.wait_until_completely_filled(), timeout=1)
            except asyncio.TimeoutError:
                self.logger().warning(
                    f"The order fill updates did not arrive on time for {tracked_order.client_order_id}. "
                    f"The complete update will be processed with incorrect information.")

            self.logger().info(f"The {tracked_order.trade_type.name} order {tracked_order.client_order_id} "
                               f"has completed according to order delta websocket API.")
            self.stop_tracking_order(tracked_order.client_order_id)

        if order_status == "canceled":
            tracked_order.last_state = order_status
            self.logger().info(f"The order {tracked_order.client_order_id} has been canceled "
                               f"according to order delta websocket API.")
            self.stop_tracking_order(tracked_order.client_order_id)

    async def _process_trade_event(self, trade_event: Dict[str, Any]):
        order_id = trade_event["orderId"]
        client_order_id = trade_event["clientOrderId"]
        execute_price = Decimal(trade_event["tradePrice"])
        execute_amount_diff = Decimal(trade_event["tradeVolume"])

        tracked_order = self._in_flight_orders.get(client_order_id, None)

        if tracked_order:
            updated = tracked_order.update_with_trade_update(trade_event)
        if updated:
            self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of order "
                               f"{tracked_order.order_type.name}-{tracked_order.client_order_id}")
    async def _update_trading_fees(self):
        pass


    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def _place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          trade_type: TradeType,
                          order_type: OrderType,
                          price: Decimal) -> str:
        path_url = CONSTANTS.PLACE_ORDER_URL
        side = trade_type.name.lower()
        order_type_str = "limit" if order_type is OrderType.LIMIT else "limit-maker"
        
        if not self._account_id:
            await self._update_account_id()

        params = {
            "account-id": self._account_id,
            "amount": f"{amount:f}",
            "client-order-id": order_id,
            "symbol": convert_to_exchange_trading_pair(trading_pair),
            "type": f"{side}-{order_type_str}",
        }
        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            params["price"] = f"{price:f}"
        exchange_order_id = await self._api_post(
            path_url=path_url,
            params=params,
            data=params,
            is_auth_required=True
        )
        return str(exchange_order_id["data"]), time.time()

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0):
        
        trading_rule = self._trading_rules[trading_pair]

        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            decimal_amount = self.quantize_order_amount(trading_pair, amount)
            decimal_price = self.quantize_order_price(trading_pair, price)
            if decimal_amount < trading_rule.min_order_size:
                raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                                 f"{trading_rule.min_order_size}.")
        try:
            exchange_order_id = await self._place_order(order_id, trading_pair, decimal_amount, True, order_type, decimal_price)
            self.start_tracking_order(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.BUY,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {order_id} for {decimal_amount} {trading_pair}.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.stop_tracking_order(order_id)
            order_type_str = order_type.name.lower()
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Huobi for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to Huobi. Check API key and network connection."
            )

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = s_decimal_0):
       
        trading_rule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = self.quantize_order_price(trading_pair, price)

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            exchange_order_id = await self._place_order(order_id, trading_pair, decimal_amount, False, order_type, decimal_price)
            self.start_tracking_order(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.SELL,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} sell order {order_id} for {decimal_amount} {trading_pair}.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.stop_tracking_order(order_id)
            order_type_str = order_type.name.lower()
            self.logger().network(
                f"Error submitting sell {order_type_str} order to Huobi for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to Huobi. Check API key and network connection."
            )

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder ):
        
        if tracked_order is None:
            raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
        path_url = CONSTANTS.CANCEL_ORDER_URL.format(tracked_order.exchange_order_id)
        response = await self._api_delete(path_url=path_url, is_auth_required=True)
        if tracked_order.exchange_order_id in response["data"].get("cancelledOrderIds", []):
            return True
        else:
            return False
        


    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(is_exchange_information_valid, exchange_info.get("data", [])):
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base=symbol_data["bc"],
                                                                        quote=symbol_data["qc"])
        self._set_trading_pair_symbol_map(mapping)




    async def _get_last_traded_price(self, trading_pair: str) -> float:
        
        url = CONSTANTS.REST_URL + CONSTANTS.TICKER_URL
        resp_json = await self._api_request(
            path_url=url,
            method=RESTMethod.GET,
        )
        resp_record = [o for o in resp_json["data"] if o["symbol"] == convert_to_exchange_trading_pair(trading_pair)][0]
        return float(resp_record["close"])
        