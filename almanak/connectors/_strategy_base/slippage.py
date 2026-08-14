"""Canonical slippage arithmetic exposed to strategy-side connectors.

The implementation belongs to the intent framework, which owns the public
slippage contract.  Connector leaves import this foundation facade so protocol
code does not depend on private framework module layout and cannot grow local
rounding policies.
"""

from almanak.framework.intents._compiler_helpers import (
    BPS_PER_UNIT,
    SlippagePrecisionError,
    compute_min_amount_out,
    compute_min_amount_out_from_bps,
    slippage_to_bps,
)
from almanak.framework.intents.min_out_guard import (
    UnprotectedTradeError,
    derive_min_out,
    require_protective_min,
    slippage_bps_to_fraction,
    validate_max_slippage_fraction,
)

__all__ = [
    "BPS_PER_UNIT",
    "SlippagePrecisionError",
    "UnprotectedTradeError",
    "compute_min_amount_out",
    "compute_min_amount_out_from_bps",
    "derive_min_out",
    "require_protective_min",
    "slippage_bps_to_fraction",
    "slippage_to_bps",
    "validate_max_slippage_fraction",
]
