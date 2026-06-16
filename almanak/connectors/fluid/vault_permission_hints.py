"""Permission discovery hints for the Fluid vault surface (``fluid_vault``).

VIB-5031, ADR §7.1 — the explicit Zodiac target/selector/approval universe,
per pinned type-1 vault on arbitrum + base:

1. ``operate(uint256,int256,int256,address)`` on the VAULT address
   (selector ``0x032d2276``, byte-verified), with ``send_value`` flipping
   on for native-collateral vaults (vault id 1 takes raw ETH as msg.value
   — the synthetic SUPPLY/BORROW below compile value-bearing txs there);
2. ``approve(address,uint256)`` on the vault's collateral token with
   ``spender == vault`` (ERC-20 legs only);
3. ``approve(address,uint256)`` on the vault's debt token with
   ``spender == vault`` (repay pull; bounded amount, never MAX_UINT).

NO ERC-721 surface: ``operate()`` acts on caller-owned positions; the
factory NFT is never transferred or approved.

``build_discovery_vectors`` owns synthetic lending dispatch because the
vault ``market_id`` differs per chain (the single-string
``synthetic_market_id`` knob cannot express it). DELEVERAGE compiles to a
REPAY shape — no separate discovery vector (morpho convention).

The framework discovers this module through the ``fluid_vault`` package's
``permission_hints`` re-export (convention-based import keyed on the
protocol slug); the implementation lives here with the rest of the vault
code (ADR §7 — one codebase, two manifests).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

PERMISSION_HINTS = PermissionHints(
    # DELEVERAGE compiles to a REPAY shape — no separate vector.
    synthetic_discovery_intents=frozenset({"SUPPLY", "BORROW", "REPAY", "WITHDRAW"}),
    # The vault SDK transport needs an RPC/gateway handle even though
    # discovery-mode compiles skip every on-chain read (Aerodrome
    # precedent: offline discovery degrades to a warning).
    needs_rpc_discovery=True,
    selector_labels={
        "0x032d2276": "Fluid vault operate (NFT-CDP lifecycle)",
    },
)

#: Small synthetic amounts per collateral symbol (calldata shape only —
#: discovery-mode compiles skip balance/position pre-flights).
_SYNTHETIC_COLLATERAL_AMOUNTS: dict[str, Decimal] = {
    "ETH": Decimal("0.01"),
    "sUSDai": Decimal("25"),
}
_SYNTHETIC_BORROW_AMOUNT = Decimal("5")  # USDC on both pinned vaults


def _vault_entries(chain: str) -> list[tuple[str, dict]]:
    from almanak.connectors.fluid.addresses import FLUID_VAULT_MARKETS

    return sorted(FLUID_VAULT_MARKETS.get(chain, {}).items())


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
):
    """Emit synthetic lending intents per pinned type-1 vault on ``chain``.

    Returns ``[]`` (not ``None``) on chains with no pinned vault so the
    framework default — which would gate on lending-pool tables fluid_vault
    is not in — never emits a doomed synthetic there.
    """
    entries = _vault_entries(chain)
    if intent_type in {"SUPPLY", "BORROW", "REPAY", "WITHDRAW"} and not entries:
        return []

    if intent_type == "SUPPLY":
        from almanak.framework.intents.vocabulary import SupplyIntent

        return [
            SupplyIntent(
                protocol=protocol,
                token=entry["collateral_token"],
                amount=_SYNTHETIC_COLLATERAL_AMOUNTS.get(entry["collateral_token"], Decimal("1")),
                market_id=vault,
                chain=chain,
            )
            for vault, entry in entries
        ]

    if intent_type == "BORROW":
        from almanak.framework.intents.vocabulary import BorrowIntent

        # Synthetic bundled-collateral borrow for permission discovery only:
        # never executed, never accounted, so it bypasses the bundled-collateral
        # guard via for_permission_discovery (see BorrowIntent.validate_borrow_intent).
        return [
            BorrowIntent.for_permission_discovery(
                protocol=protocol,
                collateral_token=entry["collateral_token"],
                collateral_amount=_SYNTHETIC_COLLATERAL_AMOUNTS.get(entry["collateral_token"], Decimal("1")),
                borrow_token=entry["loan_token"],
                borrow_amount=_SYNTHETIC_BORROW_AMOUNT,
                market_id=vault,
                chain=chain,
            )
            for vault, entry in entries
        ]

    if intent_type == "REPAY":
        from almanak.framework.intents.vocabulary import RepayIntent

        return [
            RepayIntent(
                protocol=protocol,
                token=entry["loan_token"],
                amount=_SYNTHETIC_BORROW_AMOUNT,
                market_id=vault,
                chain=chain,
            )
            for vault, entry in entries
        ]

    if intent_type == "WITHDRAW":
        from almanak.framework.intents.vocabulary import WithdrawIntent

        return [
            WithdrawIntent(
                protocol=protocol,
                token=entry["collateral_token"],
                amount=_SYNTHETIC_COLLATERAL_AMOUNTS.get(entry["collateral_token"], Decimal("1")),
                market_id=vault,
                chain=chain,
            )
            for vault, entry in entries
        ]

    return None


__all__ = ["PERMISSION_HINTS", "build_discovery_vectors"]
