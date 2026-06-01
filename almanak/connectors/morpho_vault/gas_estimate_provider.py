"""Strategy-side gas-estimate connector for MetaMorpho vaults (VIB-4858 / W6).

Publishes the ``vault_deposit`` and ``vault_redeem`` action gas
estimates to ``STRATEGY_GAS_ESTIMATE_REGISTRY``. MetaMorpho is a generic
ERC-4626 vault wrapper over Morpho Blue markets; its multi-market
redeem path needs more gas than a vanilla ERC-4626 redeem.

Note: this provider publishes the framework-level ``vault_*`` action
keys consumed by ``get_gas_estimate(chain, "vault_deposit")`` callers in
the central compiler. The MetaMorpho connector's own ``adapter.py`` also
ships a more granular ``DEFAULT_GAS_ESTIMATES`` dict
(``approve``/``deposit``/``redeem``) used directly inside the connector;
that table stays put — it covers the connector's internal adapter
contract, not the framework-wide ``vault_*`` keys.

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


class MetaMorphoGasEstimateConnector(GasEstimateConnector, GasEstimateCapability):
    """Gas-estimate connector for MetaMorpho's framework-level ``vault_*`` keys."""

    protocol: ClassVar[ProtocolName] = ProtocolName("metamorpho")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    _ESTIMATES: ClassVar[dict[str, int]] = {
        # MetaMorpho deposit (approve handled separately by the compiler).
        "vault_deposit": 200000,
        # MetaMorpho redeem (multi-market withdrawal).
        "vault_redeem": 250000,
    }

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset(self._ESTIMATES)

    def gas_estimate(self, action: str, chain: str) -> int:
        return self._ESTIMATES[action]


__all__ = ["MetaMorphoGasEstimateConnector"]
