import hashlib
import hmac
import urllib
import uuid
from typing import Any, Dict

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BittrexAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {}
        if request.headers:
            headers.update(request.headers)
        headers.update(self.generate_REST_auth_params(request=request))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request  # pass-through

    @staticmethod
    def construct_content_hash(body: str) -> str:
        json_byte = "".encode()
        if body:
            json_byte = body.encode()
        return hashlib.sha512(json_byte).hexdigest()

    def generate_REST_auth_params(self, request: RESTRequest) -> Dict[str, Any]:
        timestamp = str(int(self.time_provider.time() * 1000))
        url = request.url
        request_body = request.data
        content_hash = self.construct_content_hash(request_body)
        if request.params:
            param_str = urllib.parse.urlencode(request.params)
            url = f"{url}?{param_str}"
        content_to_sign = "".join([timestamp, url, request.method.name, content_hash, ""])
        signature = hmac.new(self.secret_key.encode(), content_to_sign.encode(), hashlib.sha512).hexdigest()
        headers = {
            "Api-Key": self.api_key,
            "Api-Timestamp": timestamp,
            "Api-Content-Hash": content_hash,
            "Api-Signature": signature,
        }
        return headers

    def generate_WS_auth_params(self):
        timestamp = str(int(self.time_provider.time() * 1000))
        randomized = str(uuid.uuid4())
        content_to_sign = f"{timestamp}{randomized}"
        signature = hmac.new(self.secret_key.encode(), content_to_sign.encode(), hashlib.sha512).hexdigest()
        return [self.api_key, timestamp, randomized, signature]
