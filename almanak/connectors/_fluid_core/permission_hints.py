"""Permission discovery hints for Fluid.

Fluid is **routerless**: each DEX pool is its own contract, so the Zodiac
Roles target for a swap is the per-pair pool address (resolved on-chain at
compile time), not a static router. Two consequences:

1. ``needs_rpc_discovery=True`` — synthetic SWAP compilation must reach the
   chain (gateway or RPC) to resolve pool addresses. Offline discovery
   degrades to a warning telling the operator to provide an RPC URL
   (Aerodrome precedent).
2. ``build_discovery_vectors`` owns synthetic SWAP dispatch — the framework
   default gates SWAP synthetics on ``PROTOCOL_ROUTERS`` membership, which
   would silently drop Fluid (TraderJoe V2 precedent, VIB-4121).

Manifest coverage note: each synthetic pair below authorises exactly the
pool backing that pair (target + ``swapIn`` selector, plus ERC-20 approve;
native-input pairs flip ``send_allowed`` on the pool). Strategies swapping
pairs outside this list need those pools added here — Fluid permissions are
per-pool by construction. Pairs below are the Phase-0-validated liquid
pairs per chain (VIB-5028 / VIB-5029).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

PERMISSION_HINTS = PermissionHints(
    synthetic_discovery_intents=frozenset({"SWAP", "SUPPLY", "WITHDRAW"}),
    needs_rpc_discovery=True,
    synthetic_swap_pair={
        "arbitrum": ("USDC", "USDT"),
        "ethereum": ("USDC", "USDT"),
        "polygon": ("USDC", "USDT"),
        "base": ("wstETH", "ETH"),
    },
    selector_labels={
        "0x2668dfaa": "Fluid pool swapIn",
        "0x6e553f65": "Fluid fToken deposit (ERC-4626)",
        "0xb460af94": "Fluid fToken withdraw (ERC-4626)",
        "0xba087652": "Fluid fToken redeem (ERC-4626)",
    },
)

# fToken lending synthetics (VIB-5030): targets are the per-underlying
# ERC-4626 fToken contracts, resolved on-chain like the swap pools. USDC is
# the validated market on both lending chains. The withdraw_all vector
# covers the redeem selector (full exits burn shares); the exact-amount
# vector covers withdraw.
_LENDING_CHAINS: frozenset[str] = frozenset({"arbitrum", "base"})
_LENDING_SYNTHETIC_TOKEN = "USDC"

# Synthetic SWAP vectors per chain. Each (from, to) pair compiles (with RPC)
# to the concrete pool target + swapIn selector; pairs with a native "from"
# leg compile to a value-bearing tx, flipping send_allowed on that pool.
# Pools verified live at Phase 0 / Phase-1 chain validation (VIB-5028/5029):
# arbitrum USDC/USDT + USDC/ETH, ethereum USDC/USDT + USDC/ETH,
# base wstETH/ETH, polygon USDC/USDT.
_SWAP_VECTORS_BY_CHAIN: dict[str, list[tuple[str, str, Decimal]]] = {
    "arbitrum": [("USDC", "USDT", Decimal("1")), ("ETH", "USDC", Decimal("0.01"))],
    "ethereum": [("USDC", "USDT", Decimal("1")), ("ETH", "USDC", Decimal("0.01"))],
    "base": [("wstETH", "ETH", Decimal("0.01")), ("ETH", "wstETH", Decimal("0.01"))],
    "polygon": [("USDC", "USDT", Decimal("1"))],
}


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
):
    """Emit synthetic intents covering Fluid's per-pool / per-fToken targets.

    SWAP vectors target the per-pair pools; SUPPLY/WITHDRAW vectors target
    the per-underlying fToken (deposit/withdraw/redeem + approve). Lending
    vectors return ``[]`` (not ``None``) on non-lending chains so the
    framework default — which would gate on lending-pool tables Fluid is
    not in — never emits a doomed synthetic there.
    """
    if intent_type == "SWAP":
        vectors = _SWAP_VECTORS_BY_CHAIN.get(chain)
        if not vectors:
            return None

        from almanak.framework.intents.vocabulary import SwapIntent

        return [
            SwapIntent(
                from_token=from_token,
                to_token=to_token,
                amount=amount,
                protocol=protocol,
                chain=chain,
            )
            for from_token, to_token, amount in vectors
        ]

    if intent_type == "SUPPLY":
        if chain not in _LENDING_CHAINS:
            return []

        from almanak.framework.intents.vocabulary import SupplyIntent

        return [
            SupplyIntent(
                protocol=protocol,
                token=_LENDING_SYNTHETIC_TOKEN,
                amount=Decimal("100"),
                chain=chain,
            )
        ]

    if intent_type == "WITHDRAW":
        if chain not in _LENDING_CHAINS:
            return []

        from almanak.framework.intents.vocabulary import WithdrawIntent

        return [
            # Exact amount → withdraw(assets, receiver, owner) selector.
            WithdrawIntent(
                protocol=protocol,
                token=_LENDING_SYNTHETIC_TOKEN,
                amount=Decimal("50"),
                chain=chain,
            ),
            # Full exit → redeem(shares, receiver, owner) selector.
            WithdrawIntent(
                protocol=protocol,
                token=_LENDING_SYNTHETIC_TOKEN,
                amount=Decimal("1"),
                withdraw_all=True,
                chain=chain,
            ),
        ]

    return None
