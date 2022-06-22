from typing import Any, Dict, Optional

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the CoinDCX API domain to connect to ("api", "public", "hft-api"). The default value is "api"
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL.format(domain) + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: the CoinDCX API domain to connect to ("api", "public", "hft-api"). The default value is "api"
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL.format(domain) + path_url


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(auth=auth,
                                       throttler=throttler)
    return api_factory


async def api_request(
    path: str,
    api_factory: Optional[WebAssistantsFactory] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    method: RESTMethod = RESTMethod.GET,
    is_auth_required: bool = False,
    return_err: bool = False,
    timeout: Optional[float] = None,
    headers: Dict[str, Any] = {},
):

    # If api_factory is not provided a default one is created
    # The default instance has no authentication capabilities and all authenticated requests will fail
    api_factory = api_factory or build_api_factory()
    rest_assistant = await api_factory.get_rest_assistant()

    local_headers = {
        "Content-Type": "application/json" if method == RESTMethod.POST else "application/x-www-form-urlencoded"
    }
    local_headers.update(headers)
    if is_auth_required:
        url = private_rest_url(path, domain=domain)
    else:
        url = public_rest_url(path, domain=domain)

    request = RESTRequest(
        method=method, url=url, params=params, data=data, headers=local_headers, is_auth_required=is_auth_required,
    )
    response = await rest_assistant.call(request=request, timeout=timeout)

    if response.status != 200:
        if return_err:
            error_response = await response.json()
            return error_response
        else:
            error_response = await response.text()
            if error_response is not None and "code" in error_response and "msg" in error_response:
                raise IOError(f"The request to CoinDCX failed. Error: {error_response}. Request: {request}")
            else:
                raise IOError(f"Error executing request {method.name} {path}. " f"HTTP status is {response.status}.")

    return await response.json()
