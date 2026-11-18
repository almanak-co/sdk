"""Concrete V3 fork parameters preserve the pool's CAIP chain and source type."""

from typing import Literal

from almanak.connectors.dex_fork import Fork
from almanak.connectors.dex_fork import ForkConfig as FamilyForkConfig
from almanak.connectors.dex_fork import fork_allocation as fork_allocation
from almanak.connectors.dex_fork import validate_assets as validate_assets
from almanak.connectors.dex_fork import validate_capital as validate_capital
from almanak.connectors.uniswap_v3.models import Allocation, Config


class ForkConfig(FamilyForkConfig[Allocation]):
    connector: Literal["uniswap_v3"] = "uniswap_v3"


class UniswapFork(Fork):
    config_type = Config
