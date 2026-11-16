"""Explicit uniswap_v3 prepared inputs and economic model selection."""

from typing import Literal

from almanak.connectors import v3_economic_config as family
from almanak.connectors.v3_economic_config import Costs as Costs


class DataPolicy(family.DataPolicy):
    source: Literal["uniswap_v3_archive"]


class EconomicConfig(family.EconomicConfig[DataPolicy]):
    pass
