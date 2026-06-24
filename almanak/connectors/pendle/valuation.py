"""Pendle position valuation — connector-flavoured re-export (VIB-5313).

The position math is generic over the **gateway price authority**
(``MarketSnapshot.pt_price`` → ``PtPriceData``) and lives in the connector
skeleton: :mod:`almanak.connectors._strategy_base.principal_token_valuation`.
Housing it there (an allowed coupling home) lets the framework portfolio valuer
import the math WITHOUT importing this connector — keeping the
framework→connector coupling ratchet green
(``scripts/ci/scan_chain_protocol_coupling.py``) — while Pendle keeps its
historical public names (``value_pendle_position`` / ``PendlePositionValue`` /
``value_pendle_lp_from_components``) for connector callers and tests.

See the skeleton module for the full contract (PT/SY/LP math, Empty ≠ Zero,
confidence propagation — design spine §0/§1/§3).
"""

from __future__ import annotations

from almanak.connectors._strategy_base.principal_token_valuation import (
    _APR_BPS_CAP,
    compute_pt_implied_apy_bps,
    value_pt_position,
    value_sy_position,
    value_yt_position,
)
from almanak.connectors._strategy_base.principal_token_valuation import (
    PrincipalTokenPositionValue as PendlePositionValue,
)
from almanak.connectors._strategy_base.principal_token_valuation import (
    value_principal_token_lp_from_components as value_pendle_lp_from_components,
)
from almanak.connectors._strategy_base.principal_token_valuation import (
    value_principal_token_position as value_pendle_position,
)

__all__ = [
    "_APR_BPS_CAP",
    "PendlePositionValue",
    "compute_pt_implied_apy_bps",
    "value_pendle_lp_from_components",
    "value_pendle_position",
    "value_pt_position",
    "value_sy_position",
    "value_yt_position",
]
