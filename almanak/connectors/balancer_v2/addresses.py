"""Balancer V2 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Balancer entries previously held in
``almanak.framework.intents.compiler_constants.BALANCER_VAULT_ADDRESSES``
and the duplicated dict in ``almanak.connectors.balancer_v2.adapter``
(VIB-4872 / epic VIB-4851).

The Balancer V2 Vault uses CREATE2 deterministic deployment, so the same
address appears on every chain that has a Balancer deployment. The
per-chain mapping shape is preserved (instead of collapsing to a single
address) so the gateway-side ``GatewayAddressCapability`` Protocol shape
matches every other connector — the capability lookup is uniformly
``addresses_for(chain)["vault"]`` rather than a Balancer-specific
special case.

The contract-kind vocabulary (``vault``) is connector-private — callers
outside this folder should consume the gateway registry, not guess key
names.
"""

from __future__ import annotations

# The Balancer V2 Vault was deployed via a deterministic CREATE2 factory
# (Mimic Smart Contract Wallet system); the same address appears on every
# chain where Balancer V2 has shipped. Keep the address as a module-level
# constant so future flash-loan code can reference it without going through
# the registry when the chain dimension is not yet known.
BALANCER_V2_VAULT_ADDRESS = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

BALANCER_V2: dict[str, dict[str, str]] = {
    "ethereum": {"vault": BALANCER_V2_VAULT_ADDRESS},
    "arbitrum": {"vault": BALANCER_V2_VAULT_ADDRESS},
    "optimism": {"vault": BALANCER_V2_VAULT_ADDRESS},
    "polygon": {"vault": BALANCER_V2_VAULT_ADDRESS},
    "base": {"vault": BALANCER_V2_VAULT_ADDRESS},
    "avalanche": {"vault": BALANCER_V2_VAULT_ADDRESS},
}


__all__ = ["BALANCER_V2", "BALANCER_V2_VAULT_ADDRESS"]
