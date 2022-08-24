import datetime
import json
import time
from base64 import b64decode
from decimal import Decimal
from typing import Any, Dict
from zlib import MAX_WBITS, decompress

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.connector.exchange.bittrex.bittrex_constants import BITTREX_DATETIME_FORMAT
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"


DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0035"),
    taker_percent_fee_decimal=Decimal("0.0035"),
)


class BittrexConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="bittrex", const=True, client_data=None)
    bittrex_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Bittrex API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    bittrex_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Bittrex secret key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )


KEYS = BittrexConfigMap.construct()


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    return exchange_info.get("status") == "ONLINE"


def _get_timestamp(transact_info: str):
    try:
        return datetime.datetime.strptime(transact_info, BITTREX_DATETIME_FORMAT).timestamp()
    except ValueError:
        return time.time()


async def _decode_message(raw_message: bytes) -> Dict[str, Any]:
    try:
        decoded_msg: bytes = decompress(b64decode(raw_message, validate=True), -MAX_WBITS)
    except SyntaxError:
        decoded_msg: bytes = decompress(b64decode(raw_message, validate=True))
    return json.loads(decoded_msg.decode())
