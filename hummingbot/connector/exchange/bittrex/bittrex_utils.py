import datetime
from base64 import b64decode
from decimal import Decimal
from typing import Any, AsyncIterable, Dict
from zlib import MAX_WBITS, decompress

import signalr_aio
import ujson
from async_timeout import timeout
from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.connector.exchange.bittrex import bittrex_constants as CONSTANTS
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
    if exchange_info.get("status") == "ONLINE":
        return True
    return False


def _get_timestamp(transact_info: str):
    transact_time_info = datetime.datetime.strptime(transact_info, '%Y-%m-%dT%H:%M:%S.%fZ')
    return datetime.datetime.timestamp(transact_time_info)


async def _socket_stream(conn: signalr_aio.Connection) -> AsyncIterable[str]:
    while True:
        async with timeout(CONSTANTS.MESSAGE_TIMEOUT):
            msg = await conn.msg_queue.get()
            yield msg


def _decode_message(raw_message: bytes) -> Dict[str, Any]:
    try:
        decoded_msg: bytes = decompress(b64decode(raw_message, validate=True), -MAX_WBITS)
    except SyntaxError:
        decoded_msg: bytes = decompress(b64decode(raw_message, validate=True))
    return ujson.loads(decoded_msg.decode())


def _transform_raw_message(msg) -> Dict[str, Any]:
    output = {"type": None, "results": {}}
    msg: Dict[str, Any] = ujson.loads(msg)
    msg_content = msg.get("M", [])
    if msg_content != [] and type(msg_content[0]) == dict and msg_content[0].get("M", None):
        output["type"] = msg_content[0].get("M")
        if output["type"] not in ["authenticationExpiring", "heartbeat"]:
            output["results"] = _decode_message(msg_content[0]["A"][0])
    return output
