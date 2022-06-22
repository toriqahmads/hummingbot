import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoindcxAPIOrderBookDataSource
from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoindcxAPIUserStreamDataSource
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoindcxAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class CoindcxExchange(ExchangePyBase):
    web_utils = web_utils

    def __init__(
            self,
            coindcx_api_key: str,
            coindcx_secret_key: str,
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
    ):
        self.coindcx_api_key = coindcx_api_key
        self.coindcx_secret_key = coindcx_secret_key
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        self._trading_pair_ecode_symbol_map: Optional[Mapping[str, str]] = None
        super().__init__()

    @property
    def authenticator(self):
        return CoindcxAuth(api_key=self.coindcx_api_key, secret_key=self.coindcx_secret_key)

    @property
    def name(self):
        return "coindcx"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return CONSTANTS.DEFAULT_DOMAIN

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_CLIENT_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.CLIENT_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.CHECK_NETWORK_REQUEST_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoindcxAPIOrderBookDataSource(
            trading_pairs=self.trading_pairs, connector=self, api_factory=self._web_assistants_factory
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoindcxAPIUserStreamDataSource(auth=self._auth, connector=self, api_factory=self._web_assistants_factory)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:

        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _api_request(
            self,
            path_url,
            method: RESTMethod = RESTMethod.GET,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            is_auth_required: bool = False,
            limit_id: Optional[str] = None,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
            return_error: bool = False,
    ) -> Dict[str, Any]:

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        if is_auth_required:
            url = self.web_utils.private_rest_url(path_url, domain=domain)
        else:
            url = self.web_utils.public_rest_url(path_url, domain=domain)

        return await rest_assistant.execute_request(
            url=url,
            params=params,
            data=data,
            method=method,
            is_auth_required=is_auth_required,
            throttler_limit_id=limit_id if limit_id else path_url,
            return_err=return_error
        )

    def trading_pair_symbol_map_ready(self):
        """
        Checks if the mapping from exchange symbols to client trading pairs has been initialized

        :return: True if the mapping has been initialized, False otherwise
        """
        return super().trading_pair_symbol_map_ready() and (
            self._trading_pair_ecode_symbol_map is not None and len(self._trading_pair_ecode_symbol_map) > 0
        )

    async def trading_pair_ecode_symbol_map(self):
        await super().trading_pair_symbol_map()
        current_map = self._trading_pair_ecode_symbol_map or bidict()
        return current_map

    def _set_trading_pair_ecode_symbol_map(self, trading_pair_ecode_symbol_mapping: Mapping[str, str]):
        self._trading_pair_ecode_symbol_map = trading_pair_ecode_symbol_mapping

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._api_request(
                path_url=self.trading_pairs_request_path,
                method=RESTMethod.GET,
                limit_id=CONSTANTS.COINDCX_GLOBAL_RATE_LIMIT,
            )
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("An error occured requesting markets info.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        """
        Initializes the trading pair symbols from exchange info.

        :param exchange_info: List of Market Details.
        :type exchange_info: List[Dict[str, Any]]
        """
        mapping = bidict()
        exchange_code_mapping = bidict()
        for symbol_data in exchange_info:
            # NOTE: There might be duplicate entries, we will just use the latest entry.

            if symbol_data["symbol"] in mapping and symbol_data["ecode"] != "B":
                continue

            mapping.forceput(
                symbol_data["symbol"],
                combine_to_hb_trading_pair(
                    base=symbol_data["target_currency_short_name"], quote=symbol_data["base_currency_short_name"]
                ),
            )
            exchange_code_mapping.forceput(
                symbol_data["pair"],
                combine_to_hb_trading_pair(
                    base=symbol_data["target_currency_short_name"], quote=symbol_data["base_currency_short_name"]
                ),
            )

        disjoint_symbol_set: Set[str] = set(mapping.values()).difference(set(exchange_code_mapping.values()))
        if len(disjoint_symbol_set) > 0:
            self.logger().warning(
                f"Mismatched trading pair symbol maps. Unable to fully initialize trading pair maps for {disjoint_symbol_set}"
            )
            return

        self._set_trading_pair_ecode_symbol_map(exchange_code_mapping)
        self._set_trading_pair_symbol_map(mapping)

    async def exchange_ecode_symbol_associated_to_pair(self, trading_pair: str):
        """
        Used to translate a trading pair from the client notation to the CoinDCX ecode notation.

        :param trading_pair: Trading pair in client notation
        :type trading_pair: str
        :return: Trading pair in exchange notation
        :rtype: str
        """
        symbol_ecode_map = await self.trading_pair_ecode_symbol_map()
        return symbol_ecode_map.inverse[trading_pair]

    async def trading_pair_associated_to_exchange_ecode_symbol(self, symbol: str):
        """
        Used to translate a trading pair from the exchange notation to the client notation

        :param symbol: trading pair in exchange notation

        :return: trading pair in client notation
        """
        symbol_ecode_map = await self.trading_pair_ecode_symbol_map()
        return symbol_ecode_map[symbol]

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
    ) -> Tuple[str, float]:
        data = {
            "market": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "total_quantity": float(amount),
            "side": "buy" if trade_type is TradeType.BUY else "sell",
            "client_order_id": order_id,
            "order_type": "limit_order" if order_type is OrderType.LIMIT else "market_order",
        }

        if order_type is OrderType.LIMIT:
            data.update({"price_per_unit": float(price)})

        response = await self._api_request(
            path_url=CONSTANTS.PLACE_ORDER_PATH_URL,
            method=RESTMethod.POST,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.COINDCX_GLOBAL_RATE_LIMIT,
        )

        order_data: Dict[str, Any] = response["orders"][0]

        return str(order_data["id"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        data = {"client_order_id": order_id}

        try:
            await self._api_request(
                path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
                method=RESTMethod.POST,
                data=data,
                is_auth_required=True,
                limit_id=CONSTANTS.COINDCX_GLOBAL_RATE_LIMIT,
            )
        except IOError as error:
            error_message = str(error)
            failed_due_to_non_existing_order = '"code":422' in error_message and "Invalid Request" in error_message

            if not failed_due_to_non_existing_order:
                raise

        return True

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        param = {
            "pair": await self.exchange_ecode_symbol_associated_to_pair(trading_pair=trading_pair),
        }
        resp_json = await self._api_request(
            path_url=CONSTANTS.TRADE_HISTORY_PATH_URL,
            params=param,
            domain=CONSTANTS.PUBLIC_DOMAIN
        )

        last_traded_data: Dict[str, Any] = resp_json[0]

        return float(last_traded_data["p"])

    async def _update_balances(self):
        balances: List[Dict[str, Any]] = await self._api_request(
            path_url=CONSTANTS.BALANCE_PATH_URL,
            method=RESTMethod.POST,
            is_auth_required=True,
            limit_id=CONSTANTS.COINDCX_GLOBAL_RATE_LIMIT,
        )

        self._account_available_balances.clear()
        self._account_balances.clear()

        for balance in balances:
            self._account_balances[balance["currency"]] = Decimal(balance["balance"]) + Decimal(
                balance["locked_balance"]
            )
            self._account_available_balances[balance["currency"]] = Decimal(balance["balance"])

    async def _format_trading_rules(self, exchange_info_dict: List[Dict[str, Any]]):
        trading_rules = []

        for info in exchange_info_dict:
            try:
                trading_rules.append(
                    TradingRule(
                        trading_pair=await self.trading_pair_associated_to_exchange_symbol(symbol=info["coindcx_name"]),
                        min_order_size=Decimal(str(info["min_quantity"])),
                        max_order_size=Decimal(str(info["max_quantity"])),
                        min_price_increment=Decimal(f"1e-{info['base_currency_precision']}"),
                        min_base_amount_increment=Decimal(str(info['step'])),
                        min_notional_size=Decimal(str(info["min_notional"])),
                    )
                )
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {info}. Skipping.")
        return trading_rules

    async def _update_trading_fees(self):
        pass

    async def _request_trade_history(self) -> List[Dict[str, Any]]:
        task_start_ts = int(round(self.current_timestamp))

        earliest_creation_ts = (
            int(min([order.creation_timestamp for order in list(self.in_flight_orders.values())])) * 1e3
            if len(self.in_flight_orders) > 0
            else int(round(self.current_timestamp))
        )

        trade_updates = []
        request_param_ts = earliest_creation_ts
        try:
            while True:
                response = await self._api_request(
                    path_url=CONSTANTS.USER_TRADE_HISTORY_PATH_URL,
                    method=RESTMethod.POST,
                    data={
                        "from_timestamp": request_param_ts,
                        "to_timestamp": task_start_ts,
                        "limit": 5000
                    },
                    is_auth_required=True,
                    limit_id=CONSTANTS.COINDCX_GLOBAL_RATE_LIMIT,
                    return_error=True
                )
                trade_updates.extend(response)
                if len(response) < 5000:
                    break
                request_param_ts = response[-1]["timestamp"]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error occurred fetching trade history. {str(e)}",
                                exc_info=True)

        return trade_updates

    async def _request_order_status(self, order: InFlightOrder) -> Tuple[str, Any]:
        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            data={"client_order_id": order.client_order_id},
            is_auth_required=True,
            return_error=True
        )
        return order.client_order_id, response

    async def _update_order_status(self):
        order_update_requests = []
        for order in list(self.in_flight_orders.values()):
            order_update_requests.append(asyncio.create_task(self._request_order_status(order)))

        order_updates: List[Tuple[str, Any]] = await safe_gather(*order_update_requests, return_exceptions=True)
        trade_updates: List[Dict[str, Any]] = await self._request_trade_history()

        if len(trade_updates) > 0:
            for data in trade_updates:
                if isinstance(data, Exception):
                    self.logger().network(
                        f"Error fetching trade history. {str(data)}",
                        app_warning_msg=f"Failed to fetch trade history. {str(data)}"
                    )

                tracked_order: InFlightOrder = self._order_tracker.fetch_order(exchange_order_id=data["order_id"])

                if tracked_order is not None:
                    fee_token: str = tracked_order.quote_asset
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        percent_token=fee_token,
                        flat_fees=[TokenAmount(amount=Decimal(data["fee_amount"]), token=fee_token)],
                    )
                    trade_update = TradeUpdate(
                        trade_id=str(data["id"]),
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=str(data["order_id"]),
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_price=Decimal(str(data["price"])),
                        fill_base_amount=Decimal(str(data["quantity"])),
                        fill_quote_amount=Decimal(str(data["quantity"])) * Decimal(str(data["price"])),
                        fill_timestamp=float(data["timestamp"]) * 1e-3,
                    )
                    self._order_tracker.process_trade_update(trade_update)

        if len(order_updates) > 0:
            for client_order_id, data in order_updates:
                if "Order not found" in str(data):
                    self.logger().network(
                        f"Error fetching status update for order {client_order_id}.",
                        app_warning_msg=f"Failed to fetch status update for an order {client_order_id}."
                    )
                    await self._order_tracker.process_order_not_found(client_order_id)
                else:
                    if client_order_id not in self.in_flight_orders:
                        continue
                    tracked_order = self.in_flight_orders[client_order_id]
                    order_update = OrderUpdate(
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=str(data["id"]),
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=int(data["updated_at"]) * 1e-3,
                        new_state=CONSTANTS.ORDER_STATE[data["status"]],
                    )
                    self._order_tracker.process_order_update(order_update)

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_event_queue():
            try:
                channel = stream_message["channel"]
                data = stream_message["data"]

                if channel == CONSTANTS.USER_BALANCE_EVENT_TYPE:
                    for asset_info in data:
                        available_balance = Decimal(asset_info["balance"])
                        locked_balance = Decimal(asset_info["locked_balance"])
                        total_balance = available_balance + locked_balance
                        asset_name = asset_info["currency"]["short_name"]
                        self._account_balances.update({asset_name: total_balance})
                        self._account_available_balances.update({asset_name: available_balance})
                elif channel == CONSTANTS.USER_TRADE_EVENT_TYPE:
                    for trade_update in data:
                        client_order_id = trade_update["c"]
                        tracked_order: InFlightOrder = self._order_tracker.fetch_order(client_order_id=client_order_id)

                        if tracked_order is not None:
                            fee_token = tracked_order.quote_asset
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=fee_token,
                                flat_fees=[TokenAmount(amount=Decimal(trade_update["f"]), token=fee_token)],
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(trade_update["t"]),
                                client_order_id=tracked_order.client_order_id,
                                exchange_order_id=str(trade_update["o"]),
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_price=Decimal(trade_update["p"]),
                                fill_base_amount=Decimal(trade_update["q"]),
                                fill_quote_amount=Decimal(trade_update["q"]) * Decimal(trade_update["p"]),
                                fill_timestamp=float(trade_update["T"]) * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)
                elif channel == CONSTANTS.USER_ORDER_EVENT_TYPE:
                    for order_update in data:
                        client_order_id = order_update["client_order_id"]
                        if client_order_id not in self.in_flight_orders:
                            continue

                        tracked_order = self.in_flight_orders[client_order_id]
                        order_update = OrderUpdate(
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=order_update["id"],
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=int(order_update["updated_at"]) * 1e-3,
                            new_state=CONSTANTS.ORDER_STATE[order_update["status"]],
                        )
                        self._order_tracker.process_order_update(order_update)
                else:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)

    async def _update_time_synchronizer(self):
        pass
