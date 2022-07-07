import re
from decimal import Decimal
from typing import Optional, Tuple, Dict, Any, Callable

from hummingbot.client.config.config_methods import using_exchange
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.connector.exchange.huobi.huobi_ws_post_processor import HuobiWSPostProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
import hummingbot.connector.exchange.huobi.huobi_constants as CONSTANTS
from hummingbot.core.web_assistant.connections.data_types import RESTMethod



DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)


RE_4_LETTERS_QUOTE = re.compile(r"^(\w+)(usdt|husd|usdc)$")
RE_3_LETTERS_QUOTE = re.compile(r"^(\w+)(btc|eth|trx)$")
RE_2_LETTERS_QUOTE = re.compile(r"^(\w+)(ht)$")

CENTRALIZED = True

EXAMPLE_PAIR = "ETH-USDT"


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        m = RE_4_LETTERS_QUOTE.match(trading_pair)
        if m is None:
            m = RE_3_LETTERS_QUOTE.match(trading_pair)
            if m is None:
                m = RE_2_LETTERS_QUOTE.match(trading_pair)
        return m.group(1), m.group(2)
    # Exceptions are now logged as warnings in trading pair fetcher
    except Exception:
        return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None
    # Huobi uses lowercase (btcusdt)
    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset.upper()}-{quote_asset.upper()}"


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # Huobi uses lowercase (btcusdt)
    return hb_trading_pair.replace("-", "").lower()


def build_api_factory(throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = None,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None, ) -> WebAssistantsFactory:
    throttler = throttler or AsyncThrottler(CONSTANTS.RATE_LIMITS)
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        ws_post_processors=[HuobiWSPostProcessor()],
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    return api_factory

def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler,)
    return api_factory


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = None,
) -> float:
    throttler = throttler or AsyncThrottler(CONSTANTS.RATE_LIMITS)
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_URL,
    )
    server_time = response["data"]
    return server_time

def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("te", None)

def public_rest_url(path_url: str, domain: str = None) -> str:
    """
    Creates a full URL for provided REST endpoint

    :param path_url: a public REST endpoint

    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + path_url

def private_rest_url(path_url: str, domain: str = None) -> str:
    """
    Creates a full URL for provided REST endpoint

    :param path_url: a private REST endpoint

    :return: the full URL to the endpoint
    """
    return public_rest_url(path_url=path_url, domain=domain)

KEYS = {
    "huobi_api_key":
        ConfigVar(key="huobi_api_key",
                  prompt="Enter your Huobi API key >>> ",
                  required_if=using_exchange("huobi"),
                  is_secure=True,
                  is_connect_key=True),
    "huobi_secret_key":
        ConfigVar(key="huobi_secret_key",
                  prompt="Enter your Huobi secret key >>> ",
                  required_if=using_exchange("huobi"),
                  is_secure=True,
                  is_connect_key=True),
}
