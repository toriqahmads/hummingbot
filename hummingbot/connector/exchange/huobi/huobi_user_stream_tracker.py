import logging
from typing import Optional

from hummingbot.connector.exchange.huobi.huobi_api_user_stream_data_source import HuobiAPIUserStreamDataSource
from hummingbot.connector.exchange.huobi.huobi_auth import HuobiAuth
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger


class HuobiUserStreamTracker(UserStreamTracker):
    _hust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._hust_logger is None:
            cls._hust_logger = logging.getLogger(__name__)
        return cls._hust_logger

    def __init__(
            self,
            huobi_auth: Optional[HuobiAuth] = None,
            api_factory: Optional[WebAssistantsFactory] = None,
    ):
        self._huobi_auth: HuobiAuth = huobi_auth
        self._api_factory = api_factory
        super().__init__(data_source=HuobiAPIUserStreamDataSource(
            huobi_auth=self._huobi_auth,
            api_factory=self._api_factory))

    @property
    def exchange_name(self) -> str:
        return "huobi"
