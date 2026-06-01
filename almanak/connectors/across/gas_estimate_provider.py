"""Strategy-side gas-estimate connector for Across (VIB-4858 / W6).

Publishes the ``bridge_deposit`` action gas estimate to
``STRATEGY_GAS_ESTIMATE_REGISTRY``. The legacy
``DEFAULT_GAS_ESTIMATES["bridge_deposit"]`` comment notes that quote-
dependent variance can push the deposit above 675K on some
destinations — the integer here is the pre-W6 default the framework's
bridge compiler consumes via ``get_gas_estimate(chain, "bridge_deposit")``.

Byte-equivalence (VIB-4858)
===========================

Integer MUST match the pre-W6 ``DEFAULT_GAS_ESTIMATES["bridge_deposit"]``
entry.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateCapability,
    GasEstimateConnector,
)


class AcrossGasEstimateConnector(GasEstimateConnector, GasEstimateCapability):
    """Gas-estimate connector for Across's ``bridge_deposit`` action."""

    protocol: ClassVar[ProtocolName] = ProtocolName("across")
    kind: ClassVar[ProtocolKind] = ProtocolKind.BRIDGE

    _ESTIMATES: ClassVar[dict[str, int]] = {
        # Cross-chain bridge deposit tx (quote-dependent; Across can exceed 675K
        # on some destinations).
        "bridge_deposit": 800000,
    }

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset(self._ESTIMATES)

    def gas_estimate(self, action: str, chain: str) -> int:
        return self._ESTIMATES[action]


__all__ = ["AcrossGasEstimateConnector"]
