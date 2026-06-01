"""Strategy-side gas-estimate connector for Aave V3 (VIB-4858 / W6).

Publishes the ``lending_*`` and ``flash_loan*`` action gas estimates to
``STRATEGY_GAS_ESTIMATE_REGISTRY``. Aave V3 is the **owning connector**
for these action keys — the legacy ``DEFAULT_GAS_ESTIMATES`` table
comments explicitly attribute the higher-than-naive numbers to "Aave V3
on Arbitrum uses ~220k+ for supply due to hooks/incentives" and similar
profile notes. Every Aave V2/V3 fork that compiles through the
framework's ``LendingProtocolAdapter`` (Radiant V2, Spark, …) inherits
these numbers transparently.

Byte-equivalence (VIB-4858)
===========================

Each integer here MUST match the pre-W6 ``DEFAULT_GAS_ESTIMATES`` entry
for the same action. See module-level note in
``almanak/connectors/_strategy_base/gas_estimate_registry.py``.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateCapability,
    GasEstimateConnector,
)


class AaveV3GasEstimateConnector(GasEstimateConnector, GasEstimateCapability):
    """Gas-estimate connector for Aave V3's ``lending_*`` / ``flash_loan*`` families."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    _ESTIMATES: ClassVar[dict[str, int]] = {
        # Lending — Aave V3 on Arbitrum uses ~220k+ for supply due to hooks/incentives.
        "lending_supply": 300000,
        # Borrow tokens from lending protocol (Aave needs ~310k+).
        "lending_borrow": 450000,
        "lending_repay": 250000,
        "lending_withdraw": 250000,
        # Multi-asset flash loan base gas (executeOperation callback excluded).
        "flash_loan": 500000,
        # Single-asset flashLoanSimple base gas.
        "flash_loan_simple": 300000,
    }

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset(self._ESTIMATES)

    def gas_estimate(self, action: str, chain: str) -> int:
        return self._ESTIMATES[action]


__all__ = ["AaveV3GasEstimateConnector"]
