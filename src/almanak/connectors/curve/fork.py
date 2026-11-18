"""Named fixture funds use explicit funding and the shared exchange fork harness."""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.curve.models import Allocation, Config
from almanak.connectors.dex_fork import Fork, fork_arrays
from almanak.connectors.dex_fork import ForkConfig as ExchangeForkConfig


def curve_arrays(value):
    raw = fork_arrays(value)
    if not isinstance(raw, dict):
        return raw
    # Generic fork validation rechecks nested values in Python mode.
    for allocation in raw.get("allocations", ()):
        if isinstance(allocation, dict) and isinstance(allocation.get("pool"), dict):
            pool = allocation["pool"]
            if isinstance(pool.get("coins"), list):
                allocation["pool"] = {**pool, "coins": tuple(pool["coins"])}
    return raw


class ForkConfig(ExchangeForkConfig[Allocation]):
    connector: Literal["curve"] = "curve"

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        return curve_arrays(value)


class CurveFork(Fork):
    config_type = Config
