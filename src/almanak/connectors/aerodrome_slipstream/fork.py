"""Concrete V3 fork parameters preserve the pool's CAIP chain and source type."""

from typing import Literal

from almanak.connectors.aerodrome_slipstream.models import Allocation, Config
from almanak.connectors.dex_fork import Fork
from almanak.connectors.dex_fork import ForkConfig as FamilyForkConfig
from almanak.connectors.dex_fork import fork_allocation as fork_allocation
from almanak.connectors.dex_fork import validate_assets as validate_assets
from almanak.connectors.dex_fork import validate_capital as validate_capital


class ForkConfig(FamilyForkConfig[Allocation]):
    connector: Literal["aerodrome_slipstream"] = "aerodrome_slipstream"


class SlipstreamFork(Fork):
    config_type = Config
