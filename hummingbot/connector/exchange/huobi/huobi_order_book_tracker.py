import logging
from typing import List, Optional

from hummingbot.connector.exchange.huobi.huobi_api_order_book_data_source import HuobiAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger


class HuobiOrderBookTracker(OrderBookTracker):
    _hobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._hobt_logger is None:
            cls._hobt_logger = logging.getLogger(__name__)
        return cls._hobt_logger

    def __init__(self,
                 trading_pairs: Optional[List[str]] = None,
                 api_factory: Optional[WebAssistantsFactory] = None):
        super().__init__(data_source=HuobiAPIOrderBookDataSource(trading_pairs=trading_pairs,
                                                                 api_factory=api_factory),
                         trading_pairs=trading_pairs)

    @property
    def exchange_name(self) -> str:
        return "huobi"
