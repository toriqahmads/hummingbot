from decimal import Decimal

from hummingbot.client.config.config_methods import using_exchange
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
)

EXAMPLE_PAIR = "BTC-USDT"

CENTRALIZED = True

KEYS = {
    "coindcx_api_key":
        ConfigVar(key="coindcx_api_key",
                  prompt="Enter your CoinDCX API key >>> ",
                  required_if=using_exchange("coindcx"),
                  is_secure=True,
                  is_connect_key=True),
    "coindcx_secret_key":
        ConfigVar(key="coindcx_secret_key",
                  prompt="Enter your CoinDCX secret key >>> ",
                  required_if=using_exchange("coindcx"),
                  is_secure=True,
                  is_connect_key=True),
}
