"""Strategy-side gas-estimate connector for Balancer V2 (VIB-4858 / W6).

Publishes the ``balancer_flash_loan*`` action gas estimates to
``STRATEGY_GAS_ESTIMATE_REGISTRY``. Balancer V2's flash-loan API differs
from Aave's (zero-fee, batch-native) so the gas profile is distinct and
the action keys are namespaced (``balancer_*``) to keep the two
providers from colliding.

Byte-equivalence (VIB-4858)
===========================

Each integer here MUST match the pre-W6 ``DEFAULT_GAS_ESTIMATES`` entry
for the same action.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateCapability,
    GasEstimateConnector,
)


class BalancerV2GasEstimateConnector(GasEstimateConnector, GasEstimateCapability):
    """Gas-estimate connector for Balancer V2's ``balancer_flash_loan*`` family."""

    protocol: ClassVar[ProtocolName] = ProtocolName("balancer_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.FLASH_LOAN

    _ESTIMATES: ClassVar[dict[str, int]] = {
        # Balancer multi-token flash loan base gas (receiveFlashLoan callback excluded).
        "balancer_flash_loan": 400000,
        # Balancer single-token flash loan base gas.
        "balancer_flash_loan_simple": 250000,
    }

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset(self._ESTIMATES)

    def gas_estimate(self, action: str, chain: str) -> int:
        return self._ESTIMATES[action]


__all__ = ["BalancerV2GasEstimateConnector"]
