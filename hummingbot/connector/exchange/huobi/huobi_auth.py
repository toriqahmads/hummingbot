import base64
import hashlib
import hmac
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict
from urllib.parse import urlencode

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSJSONRequest

HUOBI_HOST_NAME = "api.huobi.pro"


class HuobiAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
        self.api_key: str = api_key
        self.hostname: str = HUOBI_HOST_NAME
        self.secret_key: str = secret_key

    @staticmethod
    def keysort(dictionary: Dict[str, str]) -> Dict[str, str]:
        return OrderedDict(sorted(dictionary.items(), key=lambda t: t[0]))

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:

        auth_params = self.add_auth_to_params_for_REST(request=request)
        request.params = auth_params

        return request

    async def ws_authenticate(self, request: WSJSONRequest) -> WSJSONRequest:
        auth_params = self.add_auth_to_params_for_WS(request=request)
        request.payload['params'] = auth_params

        return request

    def add_auth_to_params_for_REST(self, request: RESTRequest) -> Dict[str, Any]:
        timestamp: str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        path_url = f"/v1{request.url.split('v1')[-1]}"
        params = request.params or {}
        params.update({
            "AccessKeyId": self.api_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": timestamp
        })
        sorted_params = self.keysort(params)
        signature = self.generate_signature(method=request.method.value.upper(),
                                            path_url=path_url,
                                            params=sorted_params,
                                            )
        sorted_params["Signature"] = signature
        return sorted_params

    def add_auth_to_params_for_WS(self, request: WSJSONRequest) -> Dict[str, Any]:
        timestamp: str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        path_url = "/ws/v2"
        params = request.payload.get("params") or {}
        params.update({
            "accessKey": self.api_key,
            "signatureMethod": "HmacSHA256",
            "signatureVersion": "2.1",
            "timestamp": timestamp
        })
        sorted_params = self.keysort(params)
        signature = self.generate_signature(method="get",
                                            path_url=path_url,
                                            params=sorted_params,
                                            )
        sorted_params["signature"] = signature
        sorted_params["authType"] = "api"
        return sorted_params

    def generate_signature(self,
                           method: str,
                           path_url: str,
                           params: Dict[str, Any],
                           ) -> str:

        query_endpoint = path_url
        encoded_params_str = urlencode(params)
        payload = "\n".join([method.upper(), self.hostname, query_endpoint, encoded_params_str])
        digest = hmac.new(self.secret_key.encode("utf8"), payload.encode("utf8"), hashlib.sha256).digest()
        signature_b64 = base64.b64encode(digest).decode()

        return signature_b64
