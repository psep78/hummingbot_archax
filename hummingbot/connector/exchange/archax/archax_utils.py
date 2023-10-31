from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USD"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("status") == "Trading" and exchange_info.get("type") == "crypto"


class ArchaxConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="archax", const=True, client_data=None)
    archax_email: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Archax Email",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    archax_password: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Archax Password",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )

    class Config:
        title = "archax"


KEYS = ArchaxConfigMap.construct()

# "archax_testnet"
OTHER_DOMAINS = ["archax_testnet"]
OTHER_DOMAINS_PARAMETER = {"archax_testnet": "archax_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"archax_testnet": "BTC-USD"}
OTHER_DOMAINS_DEFAULT_FEES = {"archax_testnet": DEFAULT_FEES}


class ArchaxTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="archax_testnet", const=True, client_data=None)
    archax_testnet_email: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Archax Testnet Email",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    archax_testnet_password: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Archax Testnet Password",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "archax_testnet"


OTHER_DOMAINS_KEYS = {"archax_testnet": ArchaxTestnetConfigMap.construct()}
