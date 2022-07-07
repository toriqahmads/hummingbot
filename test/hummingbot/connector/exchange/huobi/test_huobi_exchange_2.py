from typing import Any, Callable, Dict, List, Optional, Tuple
from decimal import Decimal
import json
import re
import asyncio


from aioresponses.core import RequestCall
from aioresponses import aioresponses
from unittest.mock import AsyncMock, patch


from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.connector.exchange.huobi.huobi_exchange import HuobiExchange
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.exchange.huobi import huobi_constants as CONSTANTS, huobi_utils as web_utils
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase


class HuobiExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url= CONSTANTS.SYMBOLS_URL)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.LAST_TRADE_URL)
        url = f"{url}?symbol={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(CONSTANTS.SERVER_TIME_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.private_rest_url(CONSTANTS.TRADE_RULES_URL)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.PLACE_ORDER_URL)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_BALANCE_URL)
        return url
    
    @property
    def all_symbols_request_mock_response(self):
        return {
            "status":"ok",
            "data":[
                    {
                        "tags": "",
                        "state": "online",
                        "wr": "1.5",
                        "sc": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "p": [
                                {
                                    "id": 9,
                                    "name": "Grayscale",
                                    "weight": 91
                                }
                            ],
                        "bcdn": "COINALPHA",
                        "qcdn": "HBOT",
                        "elr": None,
                        "tpp": 2,
                        "tap": 4,
                        "fp": 8,
                        "smlr": None,
                        "flr": None,
                        "whe": None,
                        "cd": None,
                        "te": True,
                        "sp": "main",
                        "d": None,
                        "bc": self.base_asset,
                        "qc": self.quote_asset,
                        "toa": 1514779200000,
                        "ttp": 8,
                        "w": 999400000,
                        "lr": 5,
                        "dn": "ETH/USDT"
                    }
                ],
        "ts":"1639598493658",
        "full":1
                }
    
    @property
    def latest_prices_request_mock_response(self):
        return {
    "ch": f"market.{self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}.trade.detail",
    "status": "ok",
    "ts": 1639598493658,
    "tick": {
        "id": 136107843051,
        "ts": 1639598493658,
        "data": [
            {
                "id": 136107843051348400221001656,
                "ts": 1639598493658,
                "trade-id": 102517374388,
                "amount": 0.028416,
                "price": 49806.0,
                "direction": "buy"
            },
        ]
    }
}


    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "status":"ok",
            "data":[
                    {
                        "tags": "",
                        "state": "online",
                        "wr": "1.5",
                        "sc": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "p": [
                                {
                                    "id": 9,
                                    "name": "Grayscale",
                                    "weight": 91
                                }
                            ],
                        "bcdn": "COINALPHA",
                        "qcdn": "HBOT",
                        "elr": None,
                        "tpp": 2,
                        "tap": 4,
                        "fp": 8,
                        "smlr": None,
                        "flr": None,
                        "whe": None,
                        "cd": None,
                        "te": True,
                        "sp": "main",
                        "d": None,
                        "bc": self.base_asset.lower(),
                        "qc": self.quote_asset.lower(),
                        "toa": 1514779200000,
                        "ttp": 8,
                        "w": 999400000,
                        "lr": 5,
                        "dn": "ETH/USDT"
                    },
                    {
                        "tags": "",
                        "state": "online",
                        "wr": "1.5",
                        "sc": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                        "p": [
                                {
                                    "id": 9,
                                    "name": "Grayscale",
                                    "weight": 91
                                }
                            ],
                        "bcdn": "INVALID",
                        "qcdn": "PAIR",
                        "elr": None,
                        "tpp": 2,
                        "tap": 4,
                        "fp": 8,
                        "smlr": None,
                        "flr": None,
                        "whe": None,
                        "cd": None,
                        "te": True,
                        "sp": "main",
                        "d": None,
                        "bc": "invalid",
                        "qc": "pair",
                        "toa": 1514779200000,
                        "ttp": 8,
                        "w": 999400000,
                        "lr": 5,
                        "dn": "ETH/USDT"
                    }

                ],
        "ts":"1639598493658",
        "full":1
                }

        return "INVALID-PAIR", response
    
    @property
    def network_status_request_successful_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        return {
    "status":"ok",
    "data":[
        {
            "symbol":self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "state":"online",
            "bc":self.base_asset.lower(),
            "qc":self.quote_asset.lower(),
            "pp":4,
            "ap":4,
            "sp":"main",
            "vp":8,
            "minoa":0.01,
            "maxoa":199.0515,
            "minov":5,
            "lominoa":0.01,
            "lomaxoa":199.0515,
            "lomaxba":199.0515,
            "lomaxsa":199.0515,
            "smminoa":0.01,
            "blmlt":1.1,
            "slmgt":0.9,
            "smmaxoa":199.0515,
            "bmmaxov":2500,
            "msormlt":0.1,
            "mbormlt":0.1,
            "maxov":2500,
            "u":"btcusdt",
            "mfr":0.035,
            "ct":"23:55:00",
            "rt":"00:00:00",
            "rthr":4,
            "in":16.3568,
            "at":"enabled",
            "tags":"etp,nav,holdinglimit,activities"
        }
    ],
    "ts":"1565246363776",
    "full":1
}

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
    "status":"ok",
    "data":[
        {
            "symbol":self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "state":"online",
            "bc":self.base_asset.lower(),
            "qc":self.quote_asset.lower(),
            "pp":4,
            "ap":4,
            "sp":"main",
            "vp":8,
            "lominoa":0.01,
            "lomaxoa":199.0515,
            "lomaxba":199.0515,
            "lomaxsa":199.0515,
            "smminoa":0.01,
            "blmlt":1.1,
            "slmgt":0.9,
            "smmaxoa":199.0515,
            "bmmaxov":2500,
            "msormlt":0.1,
            "mbormlt":0.1,
            "maxov":2500,
            "u":"btcusdt",
            "mfr":0.035,
            "ct":"23:55:00",
            "rt":"00:00:00",
            "rthr":4,
            "in":16.3568,
            "at":"enabled",
            "tags":"etp,nav,holdinglimit,activities"
        }
    ],
    "ts":"1565246363776",
    "full":1
}

    @property
    def order_creation_request_successful_mock_response(self):
        return {
    "status": "ok",
    "data": "356501383558845"
}

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
    "status": "ok",
    "data": {
        "id": 1000001,
        "type": "spot",
        "state": "working",
        "list": [
            {
                "currency": self.base_asset.lower(),
                "type": "trade",
                "balance": "91.850043797676510303",
                "seq-num": "477"
            },
            {
                "currency": self.quote_asset.lower(),
                "type": "trade",
                "balance": "91.850043797676510303",
                "seq-num": "477"
            },
        ]
    }
}

    @property
    def balance_request_mock_response_only_base(self):
        return {
    "status": "ok",
    "data": {
        "id": 1000001,
        "type": "spot",
        "state": "working",
        "list": [
            {
                "currency": self.base_asset.lower(),
                "type": "trade",
                "balance": "91.850043797676510303",
                "seq-num": "477"
            },
        ]
    }
}

    @property
    def balance_event_websocket_update(self):
        return {
    "action": "push",
    "ch": "accounts.update#2",
    "data": {
        "currency": "COINALPHA",
        "accountId": 123456,
        "balance": "15.0",
        "available": "10.0",
        "changeType": "transfer",
        "accountType":"trade",
        "seqNum": "86872993928",
        "changeTime": 1568601800000
    }
}

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(self.trading_rules_request_mock_response["data"][0]["minoa"]),
            min_price_increment=Decimal(
                self.trading_rules_request_mock_response["data"][0]["pp"]),
            min_base_amount_increment=Decimal(
                self.trading_rules_request_mock_response["data"][0]["ap"]),
            min_notional_size=Decimal(
                self.trading_rules_request_mock_response["data"][0]["minov"]),
        )
    
    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["data"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return 28

    @property
    def is_cancel_request_executed_synchronously_by_server(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return False

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("30"))])

    @property
    def expected_fill_trade_id(self) -> str:
        return 30000

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        return HuobiExchange(
            huobi_api_key="testAPIKey",
            huobi_secret_key="testSecret",
            trading_pairs=[self.trading_pair],
        )
    
    def validate_auth_credentials_present(self, request_call: RequestCall):
        self._validate_auth_credentials_taking_parameters_from_argument(
            request_call_tuple=request_call,
            params=request_call.kwargs["params"] or request_call.kwargs["data"]
        )
    
    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = dict(request_call.kwargs["data"])
        test_order_type = f"{order.trade_type.name.lower()}-limit"
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data['data']["symbol"])
        self.assertEqual(test_order_type, request_data["data"]["type"])
        self.assertEqual(Decimal("100"), Decimal(request_data["data"]["amount"]))
        self.assertEqual(Decimal("10000"), Decimal(request_data["data"]["price"]))
        self.assertEqual(order.client_order_id, request_data["data"]["client-order-id"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = dict(request_call.kwargs["params"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                         request_data["symbol"])
        self.assertEqual(order.client_order_id, request_data["data"]["client-order-id"])
        self.assertEqual("canceled",request_data["data"]["state"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                         request_params["symbol"])
        self.assertEqual(order.client_order_id, request_params["data"]["client-order-id"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        self.fail()

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.CANCEL_ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url
    
    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.CANCEL_ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.delete(regex_url, status=400, callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        """
        :return: a list of all configured URLs for the cancelations
        """
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.ORDER_DETAIL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url
    
    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.ORDER_DETAIL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # Trade fills not requested during status update in this connector
        pass

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        """
        :return: the URL configured
        """
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.ORDER_DETAIL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url
    
    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.ORDER_DETAIL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.API_VERSION_OLD+CONSTANTS.ORDER_DETAIL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # Trade fills are not requested in Binance as part of the status update
        pass

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable]) -> str:
        """
        :return: the URL configured
        """
        # Trade fills are not requested in Binance as part of the status update
        pass

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        test_type = f"{order.trade_type.name.lower()}-limit"
        return {
    "action":"push",
    "ch":f"orders#{self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}",
    "data":
    {
        "orderSize":str(order.amount),
        "orderCreateTime":1640780000,
        "accountld":992701,
        "orderPrice":str(order.price),
        "type":test_type,
        "orderId":order.exchange_order_id,
        "clientOrderId":"O1D1",
        "orderSource":"spot-api",
        "orderStatus":"submitted",
        "symbol":self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
        "eventType":"creation"

    }
}


    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        test_type = f"{order.trade_type.name.lower()}-limit"
        return {
    "action":"push",
    "ch":"orders#btcusdt",
    "data":
    {
        "lastActTime":1583853475406,
        "remainAmt":str(order.amount),
        "execAmt":"2",
        "orderId":order.exchange_order_id,
        "type":test_type,
        "clientOrderId":order.client_order_id,
        "orderSource":"spot-api",
        "orderPrice":str(order.price),
        "orderSize":str(order.amount),
        "orderStatus":"canceled",
        "symbol":self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
        "eventType":"cancellation"
    }
}

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
    "action":"push",
    "ch":f"orders#{self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}",
    "data":
    {
        "tradePrice":"76.000000000000000000",
        "tradeVolume":"1.013157894736842100",
        "tradeId":301,
        "tradeTime":1583854188883,
        "aggressor":True,
        "remainAmt":"0.000000000000000400000000000000000000",
        "execAmt":"2",
        "orderId":27163536,
        "type":"sell-limit",
        "clientOrderId":order.client_order_id,
        "orderSource":"spot-api",
        "orderPrice":"15000",
        "orderSize":"0.01",
        "orderStatus":"filled",
        "symbol":"btcusdt",
        "eventType":"trade"
    }
}

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
    "ch": f"trade.clearing#{self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}#0",
    "data": {
         "eventType": "trade",
         "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
         "orderId": 99998888,
         "tradePrice": "9999.99",
         "tradeVolume": "0.96",
         "orderSide": "buy",
         "aggressor": True,
         "tradeId": 919219323232,
         "tradeTime": 998787897878,
         "transactFee": "19.88",
         "feeDeduct ": "0",
         "feeDeductType": "",
         "feeCurrency": "btc",
         "accountId": 9912791,
         "source": "spot-api",
         "orderPrice": "10000",
         "orderSize": "1",
         "clientOrderId": "a001",
         "orderCreateTime": 998787897878,
         "orderStatus": "filled"
    }
}

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
    "status": "ok",
    "data": {
        "id": 357632718898331,
        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
        "account-id": 13496526,
        "client-order-id": "23456",
        "amount": "5.000000000000000000",
        "price": "1.000000000000000000",
        "created-at": 1630649406687,
        "type": "buy-limit-maker",
        "field-amount": "0.0",
        "field-cash-amount": "0.0",
        "field-fees": "0.0",
        "finished-at": 0,
        "source": "spot-api",
        "state": "partial-filled",
        "canceled-at": 0
    }
}

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return {
    "status": "ok",
    "data": {
        "id": 357632718898331,
        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
        "account-id": 13496526,
        "client-order-id": "23456",
        "amount": "5.000000000000000000",
        "price": "1.000000000000000000",
        "created-at": 1630649406687,
        "type": "buy-limit-maker",
        "field-amount": "0.0",
        "field-cash-amount": "0.0",
        "field-fees": "0.0",
        "finished-at": 0,
        "source": "spot-api",
        "state": "submitted",
        "canceled-at": 0
    }
}

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
    "status": "ok",
    "data": {
        "id": 357632718898331,
        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
        "account-id": 13496526,
        "client-order-id": "23456",
        "amount": "5.000000000000000000",
        "price": "1.000000000000000000",
        "created-at": 1630649406687,
        "type": "buy-limit-maker",
        "field-amount": "0.0",
        "field-cash-amount": "0.0",
        "field-fees": "0.0",
        "finished-at": 0,
        "source": "spot-api",
        "state": "canceled",
        "canceled-at": 0
    }
}

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
    "status": "ok",
    "data": {
        "id": 357632718898331,
        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
        "account-id": 13496526,
        "client-order-id": "23456",
        "amount": "5.000000000000000000",
        "price": "1.000000000000000000",
        "created-at": 1630649406687,
        "type": "buy-limit-maker",
        "field-amount": "0.0",
        "field-cash-amount": "0.0",
        "field-fees": "0.0",
        "finished-at": 0,
        "source": "spot-api",
        "state": "filled",
        "canceled-at": 0
    }
}

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
    "status": "error",
    "err-code": "order-orderstate-error",
    "err-msg": "Incorrect order state",
    "data": None,
    "order-state": 7
    }

    def _validate_auth_credentials_taking_parameters_from_argument(self,
                                                                   request_call_tuple: RequestCall,
                                                                   params: Dict[str, Any]):
        self.assertIn("timestamp", params)
        self.assertIn("signature", params)
        request_headers = request_call_tuple.kwargs["headers"]
        self.assertIn("X-MBX-APIKEY", request_headers)
        self.assertEqual("testAPIKey", request_headers["X-MBX-APIKEY"])
    
    @patch("hummingbot.connector.utils.get_tracking_nonce_low_res")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 8

        result = self.exchange.buy(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=True, trading_pair=self.trading_pair, hbot_order_id_prefix=CONSTANTS.BROKER_ID
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False, trading_pair=self.trading_pair, hbot_order_id_prefix=CONSTANTS.BROKER_ID
        )

        self.assertEqual(result, expected_client_order_id)

    @aioresponses()
    @patch("hummingbot.connector.time_synchronizer.TimeSynchronizer._current_seconds_counter")
    def test_update_time_synchronizer_successfully(self, mock_api, seconds_counter_mock):
        seconds_counter_mock.side_effect = [0, 0, 0]

        self.exchange._time_synchronizer.clear_time_offset_ms_samples()
        url = web_utils.public_rest_url(CONSTANTS.SERVER_TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
            "status": "ok",
            "data": 1640000003000
        }

        mock_api.get(regex_url, body=json.dumps(response))

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertEqual(response["data"] * 1e-3, self.exchange._time_synchronizer.time())


    @aioresponses()
    def test_update_time_synchronizer_failure_is_logged(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.SERVER_TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
            "status": "fail"
        }

        mock_api.get(regex_url, body=json.dumps(response))

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertTrue(self.is_logged("NETWORK", "Error getting server time."))

    @aioresponses()
    def test_update_time_synchronizer_raises_cancelled_error(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.SERVER_TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout, self.exchange._update_time_synchronizer())
    
    @aioresponses()
    def test_update_order_fills_from_trades_triggers_filled_event(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="100234",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.private_rest_url(CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        trade_fill = {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "id": 28457,
            "orderId": int(order.exchange_order_id),
            "orderListId": -1,
            "price": "9999",
            "qty": "1",
            "quoteQty": "48.000012",
            "commission": "10.10000000",
            "commissionAsset": self.quote_asset,
            "time": 1499865549590,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True
        }

        trade_fill_non_tracked_order = {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "id": 30000,
            "orderId": 99999,
            "orderListId": -1,
            "price": "4.00000100",
            "qty": "12.00000000",
            "quoteQty": "48.000012",
            "commission": "10.10000000",
            "commissionAsset": "BNB",
            "time": 1499865549590,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True
        }

        mock_response = [trade_fill, trade_fill_non_tracked_order]
        mock_api.get(regex_url, body=json.dumps(mock_response))

        self.exchange.add_exchange_order_ids_from_market_recorder(
            {str(trade_fill_non_tracked_order["orderId"]): "OID99"})

        self.async_run_with_timeout(self.exchange._update_order_fills_from_trades())

        request = self._all_executed_requests(mock_api, url)[0]
        self.validate_auth_credentials_present(request)
        request_params = request.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(Decimal(trade_fill["price"]), fill_event.price)
        self.assertEqual(Decimal(trade_fill["qty"]), fill_event.amount)
        self.assertEqual(0.0, fill_event.trade_fee.percent)
        self.assertEqual([TokenAmount(trade_fill["commissionAsset"], Decimal(trade_fill["commission"]))],
                         fill_event.trade_fee.flat_fees)

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[1]
        self.assertEqual(float(trade_fill_non_tracked_order["time"]) * 1e-3, fill_event.timestamp)
        self.assertEqual("OID99", fill_event.order_id)
        self.assertEqual(self.trading_pair, fill_event.trading_pair)
        self.assertEqual(TradeType.BUY, fill_event.trade_type)
        self.assertEqual(OrderType.LIMIT, fill_event.order_type)
        self.assertEqual(Decimal(trade_fill_non_tracked_order["price"]), fill_event.price)
        self.assertEqual(Decimal(trade_fill_non_tracked_order["qty"]), fill_event.amount)
        self.assertEqual(0.0, fill_event.trade_fee.percent)
        self.assertEqual([
            TokenAmount(
                trade_fill_non_tracked_order["commissionAsset"],
                Decimal(trade_fill_non_tracked_order["commission"]))],
            fill_event.trade_fee.flat_fees)
        self.assertTrue(self.is_logged(
            "INFO",
            f"Recreating missing trade in TradeFill: {trade_fill_non_tracked_order}"
        ))
