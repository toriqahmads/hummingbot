#!/usr/bin/env python
import asyncio
import logging
import sys
import unittest
from os.path import join, realpath

import conf
from hummingbot.connector.exchange.ascend_ex import ascend_ex_constants as CONSTANTS
from hummingbot.connector.exchange.ascend_ex.ascend_ex_auth import AscendExAuth
from hummingbot.connector.exchange.ascend_ex.ascend_ex_user_stream_tracker import AscendExUserStreamTracker
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger.struct_logger import METRICS_LOG_LEVEL

sys.path.insert(0, realpath(join(__file__, "../../../../../")))
logging.basicConfig(level=METRICS_LOG_LEVEL)


class AscendExUserStreamTrackerUnitTest(unittest.TestCase):
    api_key = conf.ascend_ex_api_key
    api_secret = conf.ascend_ex_secret_key

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        cls.ascend_ex_auth = AscendExAuth(cls.api_key, cls.api_secret)
        cls.trading_pairs = ["BTC-USDT"]
        cls.user_stream_tracker: AscendExUserStreamTracker = AscendExUserStreamTracker(
            cls.throttler, ascend_ex_auth=cls.ascend_ex_auth, trading_pairs=cls.trading_pairs
        )
        cls.user_stream_tracker_task: asyncio.Task = safe_ensure_future(cls.user_stream_tracker.start())

    def test_user_stream(self):
        # Wait process some msgs.
        self.ev_loop.run_until_complete(asyncio.sleep(120.0))
        print(self.user_stream_tracker.user_stream)
