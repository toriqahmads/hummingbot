import json
import re
from decimal import Decimal
from typing import Any, Callable, List, Optional, Tuple
from unittest.mock import patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall
from bidict import bidict

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.core.network_iterator import NetworkStatus


class CoindcxExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.api_key = "someKey"
        cls.secret_key = "someSecretKey"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"
        cls.ecode_trading_pair = f"B-{cls.base_asset}_{cls.quote_asset}"
        cls.maxDiff = None

    def setUp(self) -> None:
        super().setUp()
        self.exchange._set_trading_pair_ecode_symbol_map(bidict({self.ecode_trading_pair: self.trading_pair}))

    @property
    def all_symbols_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKETS_PATH_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TRADE_HISTORY_PATH_URL, domain=CONSTANTS.PUBLIC_DOMAIN)
        url = f"{url}?pair={self.ecode_trading_pair}"
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(path_url="")
        return url

    @property
    def trading_rules_url(self):
        return self.all_symbols_url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.PLACE_ORDER_PATH_URL)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.BALANCE_PATH_URL)
        return url

    @property
    def all_symbols_request_mock_response(self):
        return [
            {
                "coindcx_name": self.ex_trading_pair,
                "base_currency_short_name": self.quote_asset,
                "target_currency_short_name": self.base_asset,
                "target_currency_name": "CoinAlpha",
                "base_currency_name": "Hummingbot",
                "min_quantity": 1.0e-05,
                "max_quantity": 9000.0,
                "max_quantity_market": 294.06417674,
                "min_price": 6121.522000000001,
                "max_price": 153038.05,
                "min_notional": 10.0,
                "base_currency_precision": 2,
                "target_currency_precision": 5,
                "step": 1.0e-05,
                "order_types": ["take_profit_market", "take_profit", "stop_limit", "market_order", "limit_order"],
                "symbol": f"{self.ex_trading_pair}",
                "ecode": "B",
                "bo_sl_safety_percent": None,
                "max_leverage": 10.0,
                "max_leverage_short": 10.0,
                "pair": f"{self.ecode_trading_pair}",
                "status": "active",
            },
        ]

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = [{"coindcx_name": "INVALIDPAIR", "target_currency_name": "INVALID", "base_currency_name": "PAIR"}]

        return "INVALID-PAIR", response

    @property
    def latest_prices_request_mock_response(self):
        return [
            {"p": self.expected_latest_price, "q": 0.023519, "s": self.trading_pair, "T": 1565163305770, "m": False}
        ]

    @property
    def network_status_request_successful_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        response = [
            {
                "coindcx_name": self.ex_trading_pair,
                "base_currency_short_name": self.quote_asset,
                "target_currency_short_name": self.base_asset,
                "target_currency_name": "CoinAlpha",
                "base_currency_name": "Hummingbot",
                "min_quantity": 1.0e-05,
                "max_quantity": 9000.0,
                "max_quantity_market": 294.06417674,
                "min_price": 1,
                "max_price": 10000,
                "min_notional": 10.0,
                "base_currency_precision": 2,
                "target_currency_precision": 5,
                "step": 1.0e-05,
                "order_types": ["take_profit_market", "take_profit", "stop_limit", "market_order", "limit_order"],
                "symbol": f"{self.ex_trading_pair}",
                "ecode": "B",
                "bo_sl_safety_percent": None,
                "max_leverage": 10.0,
                "max_leverage_short": 10.0,
                "pair": f"{self.ecode_trading_pair}",
                "status": "active",
            },
        ]
        return response

    @property
    def trading_rules_request_erroneous_mock_response(self):
        response = [
            {
                "coindcx_name": self.ex_trading_pair,
                "base_currency_short_name": self.quote_asset,
                "target_currency_short_name": self.base_asset,
                "target_currency_name": "CoinAlpha",
                "base_currency_name": "Hummingbot",
                "order_types": ["take_profit_market", "take_profit", "stop_limit", "market_order", "limit_order"],
                "symbol": f"{self.ex_trading_pair}",
                "ecode": "B",
                "bo_sl_safety_percent": None,
                "pair": f"{self.ecode_trading_pair}",
                "status": "active",
            },
        ]
        return response

    @property
    def order_creation_request_successful_mock_response(self):
        response = {
            "orders": [
                {
                    "id": self.expected_exchange_order_id,
                    "client_order_id": "1234567890",
                    "market": self.ex_trading_pair,
                    "order_type": "limit_order",
                    "side": "buy",
                    "status": "open",
                    "fee_amount": 0.0,
                    "fee": 0.1,
                    "total_quantity": 20,
                    "remaining_quantity": 20,
                    "avg_price": 0.0,
                    "price_per_unit": 10,
                    "created_at": "2018-04-19T18:17:28.022Z",
                    "updated_at": "2018-04-19T18:17:28.022Z",
                }
            ]
        }
        return response

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        response = [
            {"currency": self.base_asset, "balance": 10, "locked_balance": 5},
            {"currency": self.quote_asset, "balance": 2000, "locked_balance": 0},
        ]
        return response

    @property
    def balance_request_mock_response_only_base(self):
        response = [
            {"currency": self.base_asset, "balance": 10, "locked_balance": 5},
        ]
        return response

    @property
    def balance_event_websocket_update(self):
        response = {
            "channel": "balance-update",
            "data": [
                {
                    "id": "94dfde3c-cc1f-11ec-8e29-0f323befe763",
                    "balance": "10.0",
                    "locked_balance": "5.0",
                    "address": "0x711a1Fd5737eb0c33fF21dF9876Ad37c66854520",
                    "tag": "None",
                    "currency": {
                        "short_name": self.base_asset,
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
            ],
        }
        return response

    @property
    def expected_latest_price(self):
        return 10.5

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(str("1e-05")),
            max_order_size=Decimal(str("9000.0")),
            min_price_increment=Decimal(str("1e-2")),
            min_base_amount_increment=Decimal(str("1e-05")),
            min_notional_size=Decimal(str("10.0")),
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response[0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "1234567890"

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("10")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("5")

    @property
    def expected_partial_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset, flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0.05"))]
        )

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset, flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0.2"))]
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "someTradeID"

    @property
    def is_cancel_request_executed_synchronously_by_server(self) -> bool:
        return False

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        return CoindcxExchange(
            coindcx_api_key=self.api_key, coindcx_secret_key=self.secret_key, trading_pairs=[self.trading_pair]
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertIn("Content-Type", request_headers)
        self.assertIn("X-AUTH-APIKEY", request_headers)
        self.assertIn("X-AUTH-SIGNATURE", request_headers)

        self.assertEqual(self.api_key, request_headers["X-AUTH-APIKEY"])

        request_data = json.loads(request_call.kwargs["data"])
        self.assertIn("timestamp", request_data)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])

        self.assertIn("timestamp", request_data)
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["market"])
        self.assertEqual(order.trade_type.name.lower(), request_data["side"])
        self.assertEqual(CONSTANTS.ORDER_TYPE_MAPPING[order.order_type], request_data["order_type"])
        self.assertEqual(order.price, Decimal(str(request_data["price_per_unit"])))
        self.assertEqual(order.amount, Decimal(str(request_data["total_quantity"])))
        self.assertEqual(order.client_order_id, request_data["client_order_id"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])

        self.assertIn("timestamp", request_data)
        self.assertEqual(order.client_order_id, request_data["client_order_id"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])

        self.assertIn(order.client_order_id, request_data["client_order_id"])
        self.assertEqual(order.client_order_id, request_data["client_order_id"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertIn("from_timestamp", request_data)
        self.assertIn("to_timestamp", request_data)
        self.assertIn("limit", request_data)

        self.assertEqual(5000, request_data["limit"])

    def configure_successful_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *_, **__: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL, domain=CONSTANTS.DEFAULT_DOMAIN)
        response = {"message": "success", "status": 200, "code": 200}
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL, domain=CONSTANTS.DEFAULT_DOMAIN)
        response = {"code": 422, "message": "Invalid Request", "status": "error"}
        mock_api.post(url, status=422, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
        self, successful_order: InFlightOrder, erroneous_order: InFlightOrder, mock_api: aioresponses
    ) -> List[str]:
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_STATUS_PATH_URL)
        response = {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
            "side": order.trade_type.name.lower(),
            "status": "filled",
            "fee_amount": 0.2,
            "fee": 0.1,
            "total_quantity": str(order.amount),
            "remaining_quantity": 0.0,
            "avg_price": str(order.price),
            "price_per_unit": str(order.price),
            "created_at": 1654778004000,
            "updated_at": 1654778004000,
        }
        mock_api.post(url, body=json.dumps(response), callback=callback)

        return url

    def configure_canceled_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_STATUS_PATH_URL)
        response = {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
            "side": order.trade_type.name.lower(),
            "status": "cancelled",
            "fee_amount": 0.0,
            "fee": 0.0,
            "total_quantity": str(order.amount),
            "remaining_quantity": str(order.amount),
            "avg_price": 0.0,
            "price_per_unit": 0.0,
            "created_at": 1654778004000,
            "updated_at": 1654778004000,
        }
        mock_api.post(url, body=json.dumps(response), callback=callback)

        return url

    def configure_open_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_STATUS_PATH_URL)
        response = {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
            "side": order.trade_type.name.lower(),
            "status": "open",
            "fee_amount": 0.0,
            "fee": 0.0,
            "total_quantity": str(order.amount),
            "remaining_quantity": str(order.amount),
            "avg_price": 0.0,
            "price_per_unit": 0.0,
            "created_at": 1654778004000,
            "updated_at": 1654778004000,
        }

        mock_api.post(url, body=json.dumps(response), callback=callback)

        return url

    def configure_http_error_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_STATUS_PATH_URL)
        response = {"code": 404, "message": "Order not found", "status": "error"}
        mock_api.post(url, status=400, body=json.dumps(response), callback=callback)
        return url

    def configure_partially_filled_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_STATUS_PATH_URL)
        response = {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
            "side": order.trade_type.name.lower(),
            "status": "partially_filled",
            "fee_amount": str(self.expected_partial_fill_fee.flat_fees[0].amount),
            "fee": 0.1,
            "total_quantity": str(order.amount),
            "remaining_quantity": str(order.amount - self.expected_partial_fill_amount),
            "avg_price": str(self.expected_partial_fill_price),
            "price_per_unit": str(self.expected_partial_fill_price),
            "created_at": 1654778004000,
            "updated_at": 1654778004000,
        }
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return url

    def configure_partial_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.USER_TRADE_HISTORY_PATH_URL)
        response = [
            {
                "id": 58007810,
                "order_id": order.exchange_order_id,
                "side": order.trade_type.name.lower(),
                "fee_amount": str(self.expected_partial_fill_fee.flat_fees[0].amount),
                "ecode": "B",
                "quantity": str(self.expected_partial_fill_amount),
                "price": str(self.expected_partial_fill_price),
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "timestamp": 1654777617440.79,
            }
        ]
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.USER_TRADE_HISTORY_PATH_URL)
        response = [
            {
                "id": 58007810,
                "order_id": order.exchange_order_id,
                "side": order.trade_type.name.lower(),
                "fee_amount": str(self.expected_fill_fee.flat_fees[0].amount),
                "ecode": "B",
                "quantity": str(order.amount),
                "price": str(order.price),
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "timestamp": 1654777617440.79,
            }
        ]
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.USER_TRADE_HISTORY_PATH_URL)
        mock_api.post(url, body=json.dumps([]), status=400, callback=callback)
        return url

    def configure_empty_http_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.USER_TRADE_HISTORY_PATH_URL)
        mock_api.post(url, body=json.dumps([]), callback=callback)
        return url

    def configure_successful_balance_for_base_and_quote_response(self, mock_api: aioresponses) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.BALANCE_PATH_URL)
        mock_api.post(url, body=json.dumps(self.balance_request_mock_response_for_base_and_quote))
        return url

    def configure_successful_balance_for_only_base_response(self, mock_api: aioresponses) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.BALANCE_PATH_URL)
        mock_api.post(url, body=json.dumps(self.balance_request_mock_response_only_base))
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "channel": "order-update",
            "data": [
                {
                    "id": order.exchange_order_id,
                    "client_order_id": order.client_order_id,
                    "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
                    "side": order.trade_type.name.lower(),
                    "status": "open",
                    "fee_amount": 0,
                    "fee": 0.1,
                    "maker_fee": 0.1,
                    "taker_fee": 0.1,
                    "total_quantity": str(order.amount),
                    "remaining_quantity": str(order.amount),
                    "source": "None",
                    "base_currency_name": "Hummingbot",
                    "target_currency_name": "CoinAlpha",
                    "base_currency_short_name": self.quote_asset,
                    "target_currency_short_name": self.base_asset,
                    "base_currency_precision": 3,
                    "target_currency_precision": 1,
                    "avg_price": 0,
                    "price_per_unit": str(order.price),
                    "stop_price": 0,
                    "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "time_in_force": "good_till_cancel",
                    "created_at": 1654779949609,
                    "updated_at": 1654779949609,
                }
            ],
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "channel": "order-update",
            "data": [
                {
                    "id": order.exchange_order_id,
                    "client_order_id": order.client_order_id,
                    "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
                    "side": order.trade_type.name.lower(),
                    "status": "cancelled",
                    "fee_amount": 0,
                    "fee": 0.1,
                    "maker_fee": 0.1,
                    "taker_fee": 0.1,
                    "total_quantity": str(order.amount),
                    "remaining_quantity": str(order.amount),
                    "source": "None",
                    "base_currency_name": "Hummingbot",
                    "target_currency_name": "CoinAlpha",
                    "base_currency_short_name": self.quote_asset,
                    "target_currency_short_name": self.base_asset,
                    "base_currency_precision": 3,
                    "target_currency_precision": 1,
                    "avg_price": 0,
                    "price_per_unit": str(order.price),
                    "stop_price": 0,
                    "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "time_in_force": "good_till_cancel",
                    "created_at": 1654779949609,
                    "updated_at": 1654780081249,
                }
            ],
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "channel": "order-update",
            "data": [
                {
                    "id": order.exchange_order_id,
                    "client_order_id": order.client_order_id,
                    "order_type": CONSTANTS.ORDER_TYPE_MAPPING[order.order_type],
                    "side": order.trade_type.name.lower(),
                    "status": "filled",
                    "fee_amount": self.expected_fill_fee.flat_fees[0].amount,
                    "fee": 0.1,
                    "maker_fee": 0.1,
                    "taker_fee": 0.1,
                    "total_quantity": str(order.amount),
                    "remaining_quantity": 0,
                    "source": "None",
                    "base_currency_name": "Hummingbot",
                    "target_currency_name": "CoinAlpha",
                    "base_currency_short_name": self.quote_asset,
                    "target_currency_short_name": self.base_asset,
                    "base_currency_precision": 3,
                    "target_currency_precision": 1,
                    "avg_price": str(order.price),
                    "price_per_unit": str(order.price),
                    "stop_price": 0,
                    "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "time_in_force": "good_till_cancel",
                    "created_at": 1654779773564,
                    "updated_at": 1654779888200,
                }
            ],
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "channel": "trade-update",
            "data": [
                {
                    "o": order.exchange_order_id,
                    "c": order.client_order_id,
                    "t": "58008779",
                    "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "p": str(order.price),
                    "q": str(order.amount),
                    "T": 1654778738830.03,
                    "m": True,
                    "f": self.expected_fill_fee.flat_fees[0].amount,
                    "e": "B",
                    "x": "filled",
                }
            ],
        }

    @patch("hummingbot.connector.utils.get_tracking_nonce_low_res")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 9

        result = self.exchange.buy(
            trading_pair=self.trading_pair, amount=Decimal("1"), order_type=OrderType.LIMIT, price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.CLIENT_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_CLIENT_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair, amount=Decimal("1"), order_type=OrderType.LIMIT, price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.CLIENT_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_CLIENT_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        url = self.network_status_url
        mock_api.get(url, status=404)

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(ret, NetworkStatus.NOT_CONNECTED)

    @aioresponses()
    def test_update_balances(self, mock_api):
        _ = self.configure_successful_balance_for_base_and_quote_response(mock_api)
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("10"), available_balances[self.base_asset])
        self.assertEqual(Decimal("2000"), available_balances[self.quote_asset])
        self.assertEqual(Decimal("15"), total_balances[self.base_asset])
        self.assertEqual(Decimal("2000"), total_balances[self.quote_asset])

        _ = self.configure_successful_balance_for_only_base_response(mock_api)
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertNotIn(self.quote_asset, available_balances)
        self.assertNotIn(self.quote_asset, total_balances)
        self.assertEqual(Decimal("10"), available_balances[self.base_asset])
        self.assertEqual(Decimal("15"), total_balances[self.base_asset])

    @aioresponses()
    def test_update_order_status_when_order_has_not_changed_and_one_partial_fill(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders["OID1"]

        order_url = self.configure_partially_filled_order_status_response(
            order=order,
            mock_api=mock_api)

        if self.is_order_fill_http_update_included_in_status_update:
            trade_url = self.configure_partial_fill_trade_response(
                order=order,
                mock_api=mock_api)

        self.assertTrue(order.is_open)

        self.async_run_with_timeout(self.exchange._update_order_status())

        order_status_request = self._all_executed_requests(mock_api, order_url)[0]
        self.validate_auth_credentials_present(order_status_request)
        self.validate_order_status_request(
            order=order,
            request_call=order_status_request)

        self.assertTrue(order.is_open)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

        if self.is_order_fill_http_update_included_in_status_update:
            trades_request = self._all_executed_requests(mock_api, trade_url)[0]
            self.validate_auth_credentials_present(trades_request)
            self.validate_trades_request(
                order=order,
                request_call=trades_request)

            fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
            self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
            self.assertEqual(order.client_order_id, fill_event.order_id)
            self.assertEqual(order.trading_pair, fill_event.trading_pair)
            self.assertEqual(order.trade_type, fill_event.trade_type)
            self.assertEqual(order.order_type, fill_event.order_type)
            self.assertEqual(self.expected_partial_fill_price, fill_event.price)
            self.assertEqual(self.expected_partial_fill_amount, fill_event.amount)
            self.assertEqual(self.expected_partial_fill_fee, fill_event.trade_fee)
