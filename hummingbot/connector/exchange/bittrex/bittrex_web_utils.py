import time
from typing import Callable, Optional

from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

bittrex_throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS,
                                   limits_share_percentage=CONSTANTS.BITTREX_LIMIT_SHARE_PERCENTAGE)


def public_rest_url(path_url: str, domain=None) -> str:
    return CONSTANTS.BITTREX_REST_URL + path_url


def private_rest_url(path_url: str, domain: str = None) -> str:
    return public_rest_url(path_url)


def build_api_factory(
        time_synchronizer: Optional[TimeSynchronizer] = None,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None, ) -> WebAssistantsFactory:
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time())
    api_factory = WebAssistantsFactory(
        throttler=bittrex_throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor() -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=bittrex_throttler)
    return api_factory


async def get_current_server_time(throttler: Optional[AsyncThrottler] = None,
                                  domain: str = CONSTANTS.DEFAULT_DOMAIN, ) -> float:
    api_factory = build_api_factory_without_time_synchronizer_pre_processor()
    rest_assistant = await api_factory.get_rest_assistant()
    server_time = None
    try:
        response = await rest_assistant.execute_request(
            url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_URL),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SERVER_TIME_URL,
        )
        server_time = response["serverTime"]
    except IOError:
        server_time = time.time() * 1000
    return server_time
