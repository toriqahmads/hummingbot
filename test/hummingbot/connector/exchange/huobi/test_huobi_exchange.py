import asyncio
import json
import re

from decimal import Decimal
from typing import Awaitable, Optional, Dict, List, NamedTuple
from unittest import TestCase
from unittest.mock import AsyncMock, patch

from bidict import bidict
from aioresponses import aioresponses

from hummingbot.connector.exchange.huobi import huobi_constants as CONSTANTS, huobi_utils as web_utils
from hummingbot.connector.exchange.huobi.huobi_exchange import HuobiExchange
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    MarketEvent,
    OrderFilledEvent,
    MarketOrderFailureEvent
)
from hummingbot.core.network_iterator import NetworkStatus



class HuobiExchangeTests(TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.exchange_trading_pair = cls.trading_pair
        cls.symbol = f"{cls.base_asset}{cls.quote_asset}"
        cls.listen_key = "TEST_LISTEN_KEY"

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.test_task: Optional[asyncio.Task] = None

        self.exchange = HuobiExchange(
            huobi_api_key="testAPIKey",
            huobi_secret_key="testSecret",
            trading_pairs=[self.trading_pair],
        )

        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(0)
        self.exchange._time_synchronizer.logger().setLevel(1)
        self.exchange._time_synchronizer.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

        self._initialize_event_loggers()

        self.exchange._set_trading_pair_symbol_map(bidict({self.trading_pair: self.trading_pair}))


    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        super().tearDown()

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.buy_order_created_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_failure_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.sell_order_created_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.BuyOrderCreated, self.buy_order_created_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFailure, self.order_failure_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.SellOrderCreated, self.sell_order_created_logger)]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret
    
    
    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal(str(0.01)),
                min_price_increment=Decimal(str(0.0001)),
                min_base_amount_increment=Decimal(str(0.000001)),
            )
        }
    @aioresponses()
    def test_all_trading_pairs(self, mock_api):
        self.exchange._set_trading_pair_symbol_map(None)
        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_NEW + CONSTANTS.SYMBOLS_URL

        resp = {
    "status":"ok",
    "data":[
        {
            "tags": "",
            "state": "online",
            "wr": "1.5",
            "sc": "coinalphahbot",
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
            "whe": False,
            "cd": False,
            "te": True,
            "sp": "main",
            "d": None,
            "bc": "coinalpha",
            "qc": "hbot",
            "toa": 1514779200000,
            "ttp": 8,
            "w": 999400000,
            "lr": 5,
            "dn": "COINALPHA/HBOT"
        },
        {
            "tags": "",
            "state": "offline",
            "wr": "1.5",
            "sc": "somepair",
            "p": [
                {
                    "id": 9,
                    "name": "Grayscale",
                    "weight": 91
                }
            ],
            "bcdn": "SOME",
            "qcdn": "PAIR",
            "elr": None,
            "tpp": 2,
            "tap": 4,
            "fp": 8,
            "smlr": None,
            "flr": None,
            "whe": None,
            "cd": None,
            "te": False,
            "sp": "main",
            "d": None,
            "bc": "some",
            "qc": "pair",
            "toa": 1514779200000,
            "ttp": 8,
            "w": 999400000,
            "lr": 5,
            "dn": "SOME/PAIR"
        }
    ],
    "ts":"1641870869718",
    "full":1
}
        mock_api.get(url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(coroutine=self.exchange.all_trading_pairs())

        self.assertEqual(1, len(ret))
        self.assertIn(self.trading_pair, ret)
        self.assertNotIn("SOME-PAIR", ret)
    
    @aioresponses()
    def test_all_trading_pairs_does_not_raise_exception(self, mock_api):
        self.exchange._set_trading_pair_symbol_map(None)

        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_NEW + CONSTANTS.SYMBOLS_URL
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=Exception)

        result: List[str] = self.async_run_with_timeout(self.exchange.all_trading_pairs())

        self.assertEqual(0, len(result))
    
    @aioresponses()
    def test_get_last_traded_prices(self, mock_api):
        map = self.async_run_with_timeout(self.exchange.trading_pair_symbol_map())
        map["TKN1-TKN2"] = "TKN1-TKN2"
        self.exchange._set_trading_pair_symbol_map(map)

        url1 = CONSTANTS.REST_URL + CONSTANTS.LAST_TRADE_URL
        url1 = f"{url1}?symbol={self.trading_pair}"
        regex_url = re.compile(f"^{url1}".replace(".", r"\.").replace("?", r"\?"))
        resp = {
                "ch": "market.coinalphahbot.trade.detail",
                "status": "ok",
                "ts": 1629792192037,
                "tick": {
                        "id": 136107843051,
                        "ts": 1629792191928,
                    "data": [
                            {
                             "id": 136107843051348400221001656,
                             "ts": 1629792191928,
                             "trade-id": 102517374388,
                             "amount": 0.028416,
                             "price": 100.0,
                             "direction": "buy"
                        },
                            ]
                        }       
                }
        mock_api.get(regex_url, body=json.dumps(resp))

        url2 = CONSTANTS.REST_URL + CONSTANTS.LAST_TRADE_URL
        url2 = f"{url2}?symbol=TKN1-TKN2"
        regex_url = re.compile(f"^{url2}".replace(".", r"\.").replace("?", r"\?"))
        resp = {
                "ch": "market.tkn1tkn2.trade.detail",
                "status": "ok",
                "ts": 1629792192039,
                "tick": {
                        "id": 136107843025,
                        "ts": 1629792191939,
                    "data": [
                            {
                             "id": 136107843051348400221001737,
                             "ts": 1629792191939,
                             "trade-id": 102517374364,
                             "amount": 0.028537,
                             "price": 200.0,
                             "direction": "buy"
                        },
                            ]
                        }       
                }
        mock_api.get(regex_url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(
            coroutine=self.exchange.get_last_traded_prices([self.trading_pair, "TKN1-TKN2"])
        )

        ticker_requests = [(key, value) for key, value in mock_api.requests.items()
                           if key[1].human_repr().startswith(url1) or key[1].human_repr().startswith(url2)]

        request_params = ticker_requests[0][1][0].kwargs["params"]
        self.assertEqual(f"{self.base_asset}-{self.quote_asset}", request_params["symbol"])
        request_params = ticker_requests[1][1][0].kwargs["params"]
        self.assertEqual("TKN1-TKN2", request_params["symbol"])

        self.assertEqual(ret[self.trading_pair], 100)
        self.assertEqual(ret["TKN1-TKN2"], 200)
    
    def test_supported_order_types(self):
        supported_types = self.exchange.supported_order_types()
        self.assertIn(OrderType.MARKET, supported_types)
        self.assertIn(OrderType.LIMIT, supported_types)
        self.assertIn(OrderType.LIMIT_MAKER, supported_types)
    
    @aioresponses()
    def test_check_network_success(self, mock_api):
        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_OLD + CONSTANTS.SERVER_TIME_URL
        resp = {
            "status": "ok",
            "data": 1629715504949
        }
        mock_api.get(url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(NetworkStatus.CONNECTED, ret)
    
    @aioresponses()
    def test_check_network_failure(self, mock_api):
        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_OLD + CONSTANTS.SERVER_TIME_URL
        mock_api.get(url, status=500)

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(ret, NetworkStatus.NOT_CONNECTED)
    
    @aioresponses()
    def test_check_network_raises_cancel_exception(self, mock_api):
        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_OLD + CONSTANTS.SERVER_TIME_URL

        mock_api.get(url, exception=asyncio.CancelledError)

        self.assertRaises(asyncio.CancelledError, self.async_run_with_timeout, self.exchange.check_network())


    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_OLD + CONSTANTS.TRADE_RULES_URL
        resp = {
            "status":"ok",
            "data":[
                {
                "symbol": self.trading_pair,
                "state":"online",
                "bc":self.base_asset,
                "qc":self.quote_asset,
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
                "u":"coinalhbot",
                "mfr":0.035,
                "ct":"23:55:00",
                "rt":"00:00:00",
                "rthr":4,
                "in":16.3568,
                "at":"enabled",
                "tags":"etp,nav,holdinglimit,activities"
            }
        ],
        "ts":"1641880897191",
        "full":1
        }
        mock_api.get(url, body=json.dumps(resp))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertTrue(self.trading_pair in self.exchange._trading_rules)

    
    @aioresponses()
    def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION_OLD + CONSTANTS.TRADE_RULES_URL
        resp = {
            "status":"ok",
            "data": [
                {
                    "symbol": self.trading_pair,
                    "baseCurrency": self.base_asset,
                    "quoteCurrency": self.quote_asset,
                },
            ],
        }
        mock_api.get(url, body=json.dumps(resp))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertEqual(0, len(self.exchange._trading_rules))
        self.assertTrue(
            self._is_logged("ERROR", f"Error parsing the trading pair rule {resp['data'][0]}. Skipping.")
        )

    
    def test_order_fill_event_takes_fee_from_update_event(self):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="99998888",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )

        order = self.exchange.in_flight_orders.get("OID1")

        partial_fill = {
            "eventType": "trade",
            "symbol": "choinalphahbot",
            "orderId": 99998888,
            "tradePrice": "10050.0",
            "tradeVolume": "0.1",
            "orderSide": "buy",
            "aggressor": True,
            "tradeId": 1,
            "tradeTime": 998787897878,
            "transactFee": "10.00",
            "feeDeduct ": "0",
            "feeDeductType": "",
            "feeCurrency": "usdt",
            "accountId": 9912791,
            "source": "spot-api",
            "orderPrice": "10000",
            "orderSize": "1",
            "clientOrderId": "OID1",
            "orderCreateTime": 998787897878,
            "orderStatus": "partial-filled"
        }

        message = {
            "ch": CONSTANTS.HUOBI_TRADE_DETAILS_TOPIC,
            "data": partial_fill
        }

        mock_user_stream = AsyncMock()
        mock_user_stream.get.side_effect = [message, asyncio.CancelledError()]

        self.exchange.user_stream_tracker._user_stream = mock_user_stream

        self.test_task = asyncio.get_event_loop().create_task(self.exchange._user_stream_event_listener())
        try:
            self.async_run_with_timeout(self.test_task)
        except asyncio.CancelledError:
            pass

        self.assertEqual(Decimal("10"), order.fee_paid)
        self.assertEqual(1, len(self.order_filled_logger.event_log))
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(Decimal("0"), fill_event.trade_fee.percent)
        self.assertEqual([TokenAmount(partial_fill["feeCurrency"].upper(), Decimal(partial_fill["transactFee"]))],
                         fill_event.trade_fee.flat_fees)
        self.assertTrue(self._is_logged(
            "INFO",
            f"Filled {Decimal(partial_fill['tradeVolume'])} out of {order.amount} of order "
            f"{order.order_type.name}-{order.client_order_id}"
        ))

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

        complete_fill = {
            "eventType": "trade",
            "symbol": "choinalphahbot",
            "orderId": 99998888,
            "tradePrice": "10060.0",
            "tradeVolume": "0.9",
            "orderSide": "buy",
            "aggressor": True,
            "tradeId": 2,
            "tradeTime": 998787897878,
            "transactFee": "30.0",
            "feeDeduct ": "0",
            "feeDeductType": "",
            "feeCurrency": "usdt",
            "accountId": 9912791,
            "source": "spot-api",
            "orderPrice": "10000",
            "orderSize": "1",
            "clientOrderId": "OID1",
            "orderCreateTime": 998787897878,
            "orderStatus": "partial-filled"
        }

        message["data"] = complete_fill

        mock_user_stream = AsyncMock()
        mock_user_stream.get.side_effect = [message, asyncio.CancelledError()]

        self.exchange.user_stream_tracker._user_stream = mock_user_stream

        self.test_task = asyncio.get_event_loop().create_task(self.exchange._user_stream_event_listener())
        try:
            self.async_run_with_timeout(self.test_task)
        except asyncio.CancelledError:
            pass

        self.assertEqual(Decimal("40"), order.fee_paid)

        self.assertEqual(2, len(self.order_filled_logger.event_log))
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[1]
        self.assertEqual(Decimal("0"), fill_event.trade_fee.percent)
        self.assertEqual([TokenAmount(complete_fill["feeCurrency"].upper(), Decimal(complete_fill["transactFee"]))],
                         fill_event.trade_fee.flat_fees)

        # The order should be marked as complete only when the "done" event arrives, not with the fill event
        self.assertFalse(self._is_logged(
            "INFO",
            f"The LIMIT_BUY order {order.client_order_id} has completed according to order delta websocket API."
        ))

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

    def test_order_fill_event_processed_before_order_complete_event(self):
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="99998888",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )

        order = self.exchange.in_flight_orders.get("OID1")

        complete_fill = {
            "eventType": "trade",
            "symbol": "choinalphahbot",
            "orderId": 99998888,
            "tradePrice": "10060.0",
            "tradeVolume": "1",
            "orderSide": "buy",
            "aggressor": True,
            "tradeId": 1,
            "tradeTime": 998787897878,
            "transactFee": "30.0",
            "feeDeduct ": "0",
            "feeDeductType": "",
            "feeCurrency": "usdt",
            "accountId": 9912791,
            "source": "spot-api",
            "orderPrice": "10000",
            "orderSize": "1",
            "clientOrderId": "OID1",
            "orderCreateTime": 998787897878,
            "orderStatus": "partial-filled"
        }

        fill_message = {
            "ch": CONSTANTS.HUOBI_TRADE_DETAILS_TOPIC,
            "data": complete_fill
        }

        update_data = {
            "tradePrice": "10060.0",
            "tradeVolume": "1",
            "tradeId": 1,
            "tradeTime": 1583854188883,
            "aggressor": True,
            "remainAmt": "0.0",
            "execAmt": "1",
            "orderId": 99998888,
            "type": "buy-limit",
            "clientOrderId": "OID1",
            "orderSource": "spot-api",
            "orderPrice": "10000",
            "orderSize": "1",
            "orderStatus": "filled",
            "symbol": "btcusdt",
            "eventType": "trade"
        }

        update_message = {
            "action": "push",
            "ch": CONSTANTS.HUOBI_ORDER_UPDATE_TOPIC,
            "data": update_data,
        }

        mock_user_stream = AsyncMock()
        # We simulate the case when the order update arrives before the order fill
        mock_user_stream.get.side_effect = [update_message, fill_message, asyncio.CancelledError()]

        self.exchange.user_stream_tracker._user_stream = mock_user_stream

        self.test_task = asyncio.get_event_loop().create_task(self.exchange._user_stream_event_listener())
        try:
            self.async_run_with_timeout(self.test_task)
        except asyncio.CancelledError:
            pass

        self.async_run_with_timeout(order.wait_until_completely_filled())

        self.assertEqual(Decimal("30"), order.fee_paid)
        self.assertEqual(1, len(self.order_filled_logger.event_log))
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(Decimal("0"), fill_event.trade_fee.percent)
        self.assertEqual([TokenAmount(complete_fill["feeCurrency"].upper(), Decimal(complete_fill["transactFee"]))],
                         fill_event.trade_fee.flat_fees)
        self.assertTrue(self._is_logged(
            "INFO",
            f"Filled {Decimal(complete_fill['tradeVolume'])} out of {order.amount} of order "
            f"{order.order_type.name}-{order.client_order_id}"
        ))

        self.assertTrue(self._is_logged(
            "INFO",
            f"The {order.trade_type.name} order {order.client_order_id} "
            f"has completed according to order delta websocket API."
        ))

        self.assertEqual(1, len(self.buy_order_completed_logger.event_log))

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
            is_buy=True, trading_pair=self.trading_pair, hbot_order_id_prefix=web_utils.BROKER_ID
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False, trading_pair=self.trading_pair, hbot_order_id_prefix=web_utils.BROKER_ID
        )

        self.assertEqual(result, expected_client_order_id)

    def test_restore_tracking_states_only_registers_open_orders(self):
        orders = []
        orders.append(InFlightOrder(
            client_order_id="OID1",
            exchange_order_id="EOID1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
        ))
        orders.append(InFlightOrder(
            client_order_id="OID2",
            exchange_order_id="EOID2",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.CANCELED
        ))
        orders.append(InFlightOrder(
            client_order_id="OID3",
            exchange_order_id="EOID3",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        ))
        orders.append(InFlightOrder(
            client_order_id="OID4",
            exchange_order_id="EOID4",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1000.0"),
            price=Decimal("1.0"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FAILED
        ))

        tracking_states = {order.client_order_id: order.to_json() for order in orders}

        self.exchange.restore_tracking_states(tracking_states)

        self.assertIn("OID1", self.exchange.in_flight_orders)
        self.assertNotIn("OID2", self.exchange.in_flight_orders)
        self.assertNotIn("OID3", self.exchange.in_flight_orders)
        self.assertNotIn("OID4", self.exchange.in_flight_orders)
    
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

        self.assertTrue(self._is_logged("NETWORK", "Error getting server time."))

    @aioresponses()
    def test_update_time_synchronizer_raises_cancelled_error(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.SERVER_TIME_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout, self.exchange._update_time_synchronizer())
    
    @aioresponses()
    def test_update_balances(self, mock_api):
        
        test_account_id = 1000001
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_BALANCE_URL.format(test_account_id))
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
        "status": "ok",
        "data": {
            "id": 1000001,
            "type": "spot",
            "state": "working",
            "list": [
                {
                    "currency": "btc",
                    "type": "trade",
                    "balance": "2000",
                    "seq-num": "477"
                }
        ]
    }
}

        mock_api.get(regex_url, body=json.dumps(response))
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("2000"), available_balances["BTC"])
        self.assertEqual(Decimal("2000"), total_balances["BTC"])

        response = {
            "code": "200000",
            "data": [
                {
                    "id": "5bd6e9286d99522a52e458de",
                    "currency": "BTC",
                    "type": "trade",
                    "balance": "15.0",
                    "available": "10.0",
                    "holds": "0"
                }]
        }

        mock_api.get(regex_url, body=json.dumps(response))
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertNotIn("LTC", available_balances)
        self.assertNotIn("LTC", total_balances)
        self.assertEqual(Decimal("10"), available_balances["BTC"])
        self.assertEqual(Decimal("15"), total_balances["BTC"])
