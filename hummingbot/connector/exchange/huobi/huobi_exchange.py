import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncIterable, Dict, List, Optional

from bidict import bidict

import hummingbot.connector.exchange.huobi.huobi_constants as CONSTANTS
from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.exchange.huobi import huobi_web_utils as web_utils
from hummingbot.connector.exchange.huobi.huobi_api_order_book_data_source import HuobiAPIOrderBookDataSource
from hummingbot.connector.exchange.huobi.huobi_api_user_stream_data_source import HuobiAPIUserStreamDataSource
from hummingbot.connector.exchange.huobi.huobi_auth import HuobiAuth
from hummingbot.connector.exchange.huobi.huobi_utils import is_exchange_information_valid
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class HuobiExchange(ExchangePyBase):

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 huobi_api_key: str,
                 huobi_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):
        self.huobi_api_key = huobi_api_key
        self. huobi_secret_key = huobi_secret_key
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        self._account_id = ""
        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return "huobi"

    @property
    def authenticator(self):
        return HuobiAuth(api_key=self.huobi_api_key,
                         secret_key=self.huobi_secret_key,
                         time_provider=self._time_synchronizer)

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return CONSTANTS.DOMAIN

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_CLIENT_ORDER_ID_LENGTH

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.TRADE_INFO_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.TRADE_INFO_URL

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
        return web_utils.build_api_factory(throttler=self._throttler,
                                           time_synchronizer=self._time_synchronizer,
                                           auth=self._auth)

    def _create_order_book_data_source(self):
        return HuobiAPIOrderBookDataSource(trading_pairs=self.trading_pairs,
                                           connector=self,
                                           api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return HuobiAPIUserStreamDataSource(huobi_auth=self._auth,
                                            trading_pairs=self._trading_pairs,
                                            connector=self,
                                            api_factory=self._web_assistants_factory)

    async def _update_account_id(self) -> str:
        accounts = await self._api_get(path_url=CONSTANTS.ACCOUNT_ID_URL, is_auth_required=True)
        try:
            for account in accounts["data"]:
                if account["state"] == "working" and account["type"] == "spot":
                    self._account_id = str(account["id"])
        except Exception:
            raise ValueError(f"Unable to retrieve account id.\n{accounts['err-msg']}")

    async def _update_balances(self):

        new_available_balances = {}
        new_balances = {}
        if not self._account_id:
            await self._update_account_id()
        data = await self._api_get(path_url=CONSTANTS.ACCOUNT_BALANCE_URL.format(self._account_id), is_auth_required=True, limit_id=CONSTANTS.ACCOUNT_BALANCE_LIMIT_ID)
        balances = data.get("data").get("list", [])
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

            self._account_available_balances = new_available_balances
            self._account_balances = new_balances

    async def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        trading_rules = []
        supported_symbols = await self.trading_pair_symbol_map()
        for info in raw_trading_pair_info["data"]:
            try:
                if info["symbol"] not in supported_symbols:
                    continue
                base_asset = info["bc"]
                quote_asset = info["qc"]
                trading_rules.append(
                    TradingRule(trading_pair=f"{base_asset}-{quote_asset}".upper(),
                                min_order_size=Decimal(info["minoa"]),
                                max_order_size=Decimal(info["maxoa"]),
                                min_price_increment=Decimal(info['pp']),
                                min_base_amount_increment=Decimal(info['ap']),
                                min_quote_amount_increment=Decimal(info['vp']),
                                min_notional_size=Decimal(info["minov"]))
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {info}. Skipping.", exc_info=True)
        return trading_rules

    async def _get_order_status(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        detail_url = CONSTANTS.ORDER_DETAIL_URL.format(exchange_order_id)
        trades_url = CONSTANTS.ORDER_MATCHES_URL.format(exchange_order_id)
        params = {
            "order-id": exchange_order_id
        }
        update = await self._api_get(path_url=detail_url, params=params, is_auth_required=True,
                                     limit_id=CONSTANTS.ORDER_DETAIL_LIMIT_ID)
        trades = None
        try:
            trades = await self._api_get(path_url=trades_url, params=params, is_auth_required=True,
                                         limit_id=CONSTANTS.ORDER_MATCHES_LIMIT_ID)
        except Exception:
            pass
        return update, trades

    async def _update_order_status(self):
        tracked_orders = list(self.in_flight_orders.values())
        for tracked_order in tracked_orders:
            try:
                order_update, order_trades = await self._get_order_status(tracked_order)
            except Exception:
                self.logger().network(
                    f"Error fetching status update for the order {tracked_order.client_order_id}",
                    app_warning_msg=f"Failed to fetch status update for the order {tracked_order.client_order_id}."
                )
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)
                continue

            if order_update.get("status") != "ok":
                self.logger().network(
                    f"Error fetching status update for the order {tracked_order.client_order_id}: {order_update}.",
                    app_warning_msg=f"Failed to fetch status update for the order {tracked_order.client_order_id}."
                )
                continue
            if order_trades and order_trades.get("status") == "ok":
                for trade in order_trades["data"]:
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        percent_token=trade["fee-currency"].upper(),
                        flat_fees=[TokenAmount(amount=Decimal(trade["filled-fees"]),
                                               token=trade["fee-currency"].upper())]
                    )
                    trade_update = TradeUpdate(
                        trade_id=str(trade["trade-id"]),
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=str(trade["order-id"]),
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(trade["filled-amount"]),
                        fill_quote_amount=Decimal(trade["filled-amount"]) * Decimal(trade["price"]),
                        fill_price=Decimal(trade["price"]),
                        fill_timestamp=trade["created-at"] * 1e-3,
                    )
                    self._order_tracker.process_trade_update(trade_update)
            order_state = order_update["data"]["state"]
            new_state = CONSTANTS.ORDER_STATE[order_state]
            ts = (order_update["data"]["finished-at"] * 1e-3) if order_state == "filled" else self.current_timestamp
            update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(order_update["data"]["id"]),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=ts,
                new_state=new_state,
            )
            self._order_tracker.process_order_update(update)

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
                channel = stream_message["ch"]
                if not channel.startswith("accounts") and not channel.startswith("orders") and not channel.startswith("trade.clearing"):
                    continue
                data = stream_message["data"]
                if channel.startswith("accounts"):
                    asset_name = data["currency"].upper()
                    balance = data["balance"]
                    available_balance = data["available"]

                    self._account_balances.update({asset_name: Decimal(balance)})
                    self._account_available_balances.update({asset_name: Decimal(available_balance)})
                elif channel.startswith("orders"):
                    safe_ensure_future(self._process_order_update(data))
                elif channel.startswith("trade.clearing"):
                    safe_ensure_future(self._process_trade_event(data))

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _process_order_update(self, msg: Dict[str, Any]):
        client_order_id = msg["clientOrderId"]
        order_status = msg["orderStatus"]
        tracked_order = self.in_flight_orders.get(client_order_id, None)
        ts = msg.get("orderCreateTime") or msg.get("tradeTime") or msg.get("lastActTime")
        if tracked_order is not None:
            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=ts * 1e-3,
                new_state=CONSTANTS.ORDER_STATE[order_status],
                client_order_id=client_order_id,
                exchange_order_id=str(msg["orderId"])
            )
            self._order_tracker.process_order_update(order_update=order_update)

    async def _process_trade_event(self, trade_event: Dict[str, Any]):
        client_order_id = trade_event["clientOrderId"]
        tracked_order = self.in_flight_orders.get(client_order_id, None)

        if tracked_order:
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=tracked_order.trade_type,
                percent_token=trade_event["feeCurrency"].upper(),
                flat_fees=[TokenAmount(amount=Decimal(trade_event["transactFee"]), token=trade_event["feeCurrency"].upper())]
            )
            trade_update = TradeUpdate(
                trade_id=str(trade_event["tradeId"]),
                client_order_id=client_order_id,
                exchange_order_id=str(trade_event["orderId"]),
                trading_pair=tracked_order.trading_pair,
                fee=fee,
                fill_base_amount=Decimal(trade_event["tradeVolume"]),
                fill_quote_amount=Decimal(trade_event["tradeVolume"]) * Decimal(trade_event["tradePrice"]),
                fill_price=Decimal(trade_event["tradePrice"]),
                fill_timestamp=trade_event["tradeTime"] * 1e-3,
            )
            self._order_tracker.process_trade_update(trade_update)

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
                           price: Decimal):
        path_url = CONSTANTS.PLACE_ORDER_URL
        side = trade_type.name.lower()
        order_type_str = "limit" if order_type is OrderType.LIMIT else "limit-maker"
        if not self._account_id:
            await self._update_account_id()
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        params = {
            "account-id": self._account_id,
            "amount": f"{amount}",
            "client-order-id": order_id,
            "symbol": exchange_symbol,
            "type": f"{side}-{order_type_str}",
        }
        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            params["price"] = f"{price}"
        exchange_order_id = await self._api_post(
            path_url=path_url,
            params=params,
            data=params,
            is_auth_required=True
        )
        return str(exchange_order_id["data"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        if tracked_order is None:
            raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
        path_url = CONSTANTS.CANCEL_ORDER_URL.format(tracked_order.exchange_order_id)
        params = {"order-id": str(tracked_order.exchange_order_id)}
        response = await self._api_post(path_url=path_url, params=params, data=params, limit_id=CONSTANTS.CANCEL_URL_LIMIT_ID, is_auth_required=True)
        if response.get("status") == "ok":
            return True
        return False

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(is_exchange_information_valid, exchange_info.get("data", [])):
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base=symbol_data["bc"].upper(),
                                                                        quote=symbol_data["qc"].upper())

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        path_url = CONSTANTS.TICKER_URL
        resp_json = await self._api_get(
            path_url=path_url
        )
        ex_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        resp_record = [o for o in resp_json["data"] if o["symbol"] == ex_symbol][0]
        return float(resp_record["close"])

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None):
        return build_trade_fee(self.name, is_maker, base_currency=base_currency, quote_currency=quote_currency, order_type=order_type, order_side=order_side, amount=amount, price=price)
