import asyncio
import hashlib
import hmac
import json
from datetime import datetime
from typing import Any, Awaitable, Dict, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

from hummingbot.connector.exchange.coindcx.coindcx_auth import CoindcxAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class CoindcxAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"

        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        self.auth = CoindcxAuth(api_key=self.api_key, secret_key=self.secret_key,)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def _sign(self, data: Dict[str, Any], expected_timestamp: Optional[float] = None) -> str:
        if expected_timestamp:
            data.update({"timestamp": expected_timestamp})
        payload = json.dumps(data, separators=(",", ":"))
        secret_bytes = bytes(self.secret_key, encoding="utf-8")

        return hmac.new(secret_bytes, payload.encode(), hashlib.sha256).hexdigest()

    def _format_timestamp(self, timestamp: int) -> str:
        return datetime.utcfromtimestamp(timestamp).isoformat(timespec="milliseconds") + "Z"

    @patch("hummingbot.connector.exchange.coindcx.coindcx_auth.CoindcxAuth._time")
    def test_add_auth_headers_to_post_request(self, mock_time):
        mock_time.return_value = 1

        body = {"param_z": "value_param_z", "param_a": "value_param_a"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://test.url/api/endpoint",
            data=json.dumps(body),
            is_auth_required=True,
            throttler_limit_id="/api/endpoint",
        )

        signed_request: RESTRequest = self.async_run_with_timeout(self.auth.rest_authenticate(request))
        expected_signature = self._sign(body, 1)

        self.assertEqual(self.api_key, signed_request.headers["X-AUTH-APIKEY"])
        self.assertEqual(expected_signature, signed_request.headers["X-AUTH-SIGNATURE"])

    @patch("hummingbot.connector.exchange.coindcx.coindcx_auth.CoindcxAuth._time")
    def test_ws_authenticate(self, mock_time):
        mock_time.return_value = 1

        result = self.async_run_with_timeout(self.auth.ws_authenticate())

        expected_signature = self._sign({"channel": "coindcx"})
        self.assertEqual("coindcx", result["channelName"])
        self.assertEqual(expected_signature, result["authSignature"])
