"""Named fixture funds use explicit funding and the shared exchange fork harness."""

from typing import Literal

from almanak.connectors.aerodrome.models import Allocation, Config
from almanak.connectors.dex_fork import Fork
from almanak.connectors.dex_fork import ForkConfig as ExchangeForkConfig


class ForkConfig(ExchangeForkConfig[Allocation]):
    connector: Literal["aerodrome"] = "aerodrome"


class AerodromeFork(Fork):
    config_type = Config
