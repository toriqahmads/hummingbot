import hashlib
import hmac
import json
import time
from dataclasses import replace
from typing import Any, Dict

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest


class CoindcxAuth(AuthBase):

    def __init__(self, api_key: str, secret_key: str) -> None:
        self.api_key: str = api_key
        self.secret_key: str = secret_key

    def _generate_auth_signature(self, data: Dict[str, Any]) -> str:
        """
        Helper function that includes the timestamp and generates the appropriate authentication signature for the
        request.
        """

        payload = json.dumps(data, separators = (',', ':'))
        secret_bytes = bytes(self.secret_key, encoding='utf-8')

        return hmac.new(secret_bytes, payload.encode(), hashlib.sha256).hexdigest()

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.

        :param request: the request to be configured for authenticated interaction
        """
        data: Dict[str, Any] = {}
        if request.data is not None:
            data.update(json.loads(request.data))

        data.update({
            "timestamp": self._time()
        })

        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": str(self.api_key),
            "X-AUTH-SIGNATURE": self._generate_auth_signature(data=data)
        }
        if request.headers is not None:
            headers.update(request.headers)

        request = replace(request, data=json.dumps(data, separators = (',', ':')))
        request = replace(request, headers=headers)

        return request

    async def ws_authenticate(self) -> Dict[str, Any]:
        """
        This method is intended to generate the appropriate websocket auth payload.
        """

        payload: Dict[str, Any] = {
            "channel": "coindcx"
        }

        auth_payload = {
            "channelName": "coindcx",
            "authSignature": self._generate_auth_signature(payload),
            "apiKey": self.api_key
        }

        return auth_payload

    def _time(self) -> float:
        return int(round(time.time() * 1e3))
