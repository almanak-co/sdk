"""Morpho Blue permission hints for permission discovery.

Morpho Blue is market-ID driven and routes SUPPLY / WITHDRAW on flags
(``use_as_collateral`` / ``is_collateral``) that decide between the loan-token
path (``supply`` / ``withdraw``) and the collateral path (``supplyCollateral``
/ ``withdrawCollateral``). A single synthetic intent only exercises one flag
value, so naive compilation-based discovery emits just one selector per
operation — the manifest would miss the opposite path.

Rather than paper over this with ``static_permissions`` (which would merge
unconditionally and over-authorise ``borrow`` / ``repay`` on the Safe for a
SUPPLY-only strategy — see codex review 3135601928), the connector owns its
discovery vectors via ``build_discovery_vectors`` below: BOTH flag variants
are emitted for SUPPLY / WITHDRAW, and BORROW / REPAY resolve the loan +
collateral tokens from the per-chain ``MORPHO_MARKETS`` registry so the
synthetic targets the same market the supply/withdraw paths discovered.
See :func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract.

The compiler then naturally emits ``supply`` + ``supplyCollateral`` for SUPPLY
and ``withdraw`` + ``withdrawCollateral`` for WITHDRAW, and the manifest
carries only the selectors matching the requested intent types.

``selector_labels`` remains so human-readable labels still render if a
selector appears on the manifest.

See ``almanak.core.contracts.MORPHO_BLUE`` for the per-chain singleton
addresses — ethereum / base use the vanity ``0xBBBB...FFCb``, arbitrum and
polygon use chain-specific deployments.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

# Well-known Morpho Blue market ID (WETH/USDC on Ethereum).
# Used as a synthetic market_id for permission discovery - the actual
# market_id value doesn't affect which selectors are discovered.
# Per-chain overrides come from ``MORPHO_MARKETS`` in the adapter via
# ``build_discovery_vectors`` below; this default is retained for ethereum
# where the registry key matches.
_SYNTHETIC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

# Morpho Blue singleton selectors (4-byte). Sourced from
# ``almanak/connectors/morpho_blue/adapter.py``; any change there
# must be mirrored here so manifests render human-readable labels.
_MORPHO_SELECTOR_LABELS: dict[str, str] = {
    "0xa99aad89": "supply((address,address,address,address,uint256),uint256,uint256,address,bytes)",
    "0x238d6579": "supplyCollateral((address,address,address,address,uint256),uint256,address,bytes)",
    "0x5c2bea49": "withdraw((address,address,address,address,uint256),uint256,uint256,address,address)",
    "0x8720316d": "withdrawCollateral((address,address,address,address,uint256),uint256,address,address)",
    "0x50d8cd4b": "borrow((address,address,address,address,uint256),uint256,uint256,address,address)",
    "0x20b76e81": "repay((address,address,address,address,uint256),uint256,uint256,address,bytes)",
}


PERMISSION_HINTS = PermissionHints(
    synthetic_market_id=_SYNTHETIC_MARKET_ID,
    selector_labels=dict(_MORPHO_SELECTOR_LABELS),
    # Synthetic-discovery participation (VIB-4928): the four core lending
    # primitives. Resolved only for the canonical ``morpho_blue`` slug — the
    # bare ``morpho`` compiler-loader alias has no permission_hints module and
    # so never enters the derived lending set.
    synthetic_discovery_intents=frozenset({"SUPPLY", "WITHDRAW", "BORROW", "REPAY"}),
)


def _synthetic_market_id(chain: str, fallback: str | None) -> str | None:
    """Return a valid synthetic market_id for morpho_blue on ``chain``.

    Morpho Blue markets are chain-specific: a market_id valid on ethereum
    will not resolve on arbitrum/base/polygon/monad. The adapter ships with
    a per-chain registry in ``MORPHO_MARKETS``; prefer its first entry for
    the requested chain so the compiler can actually build the supply tx.

    Falls back to ``fallback`` (the hint-level default, ethereum-tuned) only
    when the adapter registry has no entry for the chain.
    """
    from .adapter import MORPHO_MARKETS

    chain_markets = MORPHO_MARKETS.get(chain, {})
    if chain_markets:
        return next(iter(chain_markets))
    return fallback


def _loan_token(chain: str, fallback: str) -> str:
    """Return the loan-token address for morpho_blue's synthetic market.

    The loan-token path (``supply`` with ``use_as_collateral=False``) requires
    ``intent.token`` to match the market's loan token. Using the chain default
    USDC can mismatch the selected market (e.g. polygon's first registered
    market is USDT-quoted), producing a compile failure that drops both flag
    variants from the manifest.
    """
    from .adapter import MORPHO_MARKETS

    chain_markets = MORPHO_MARKETS.get(chain, {})
    if not chain_markets:
        return fallback
    first_market = next(iter(chain_markets.values()))
    return first_market.get("loan_token_address") or fallback


def _collateral_token(chain: str, fallback: str) -> str:
    """Return the collateral-token address for morpho_blue's synthetic market.

    Mirror of :func:`_loan_token` for the collateral path (``supply`` with
    ``use_as_collateral=True`` / ``withdrawCollateral``).
    """
    from .adapter import MORPHO_MARKETS

    chain_markets = MORPHO_MARKETS.get(chain, {})
    if not chain_markets:
        return fallback
    first_market = next(iter(chain_markets.values()))
    return first_market.get("collateral_token_address") or fallback


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Emit synthetic intents covering every selector the manifest needs.

    Morpho Blue routes SUPPLY on ``use_as_collateral`` (True → ``supplyCollateral``,
    False → ``supply``) and WITHDRAW on ``is_collateral`` (True →
    ``withdrawCollateral``, False → ``withdraw``). The manifest needs BOTH
    selectors for each operation, so we sweep the flag during discovery —
    without this sweep only one of the two lands on the manifest and the
    other path reverts on the Safe. See codex review 3135601928.

    BORROW / REPAY resolve loan + collateral tokens from the per-chain
    ``MORPHO_MARKETS`` registry. Markets are chain-specific in both pair AND
    id (e.g. polygon's first registered market is WBTC/USDC; arbitrum / base
    use wstETH/USDC). Aligning the synthetic with the same market the supply
    / withdraw paths discovered keeps the collateral approve + ``borrow``
    selectors coherent. Falling back to the chain-default ``weth`` /
    ``usdc`` would declare the wrong collateral and drop both selectors from
    the manifest. See #1904.

    Returns ``None`` for any ``intent_type`` other than SUPPLY / WITHDRAW /
    BORROW / REPAY so the framework default takes over (morpho_blue only
    owns lending discovery).
    """
    from almanak.framework.intents.vocabulary import (
        BorrowIntent,
        RepayIntent,
        SupplyIntent,
        WithdrawIntent,
    )

    market_id = _synthetic_market_id(chain, _SYNTHETIC_MARKET_ID)

    if intent_type == "SUPPLY":
        loan_token = _loan_token(chain, ctx.usdc)
        collateral_token = _collateral_token(chain, ctx.usdc)
        return [
            SupplyIntent(
                protocol=protocol,
                token=collateral_token,
                amount=Decimal("1"),
                chain=chain,
                market_id=market_id,
                use_as_collateral=True,
            ),
            SupplyIntent(
                protocol=protocol,
                token=loan_token,
                amount=Decimal("100"),
                chain=chain,
                market_id=market_id,
                use_as_collateral=False,
            ),
        ]

    if intent_type == "WITHDRAW":
        loan_token = _loan_token(chain, ctx.usdc)
        collateral_token = _collateral_token(chain, ctx.usdc)
        return [
            WithdrawIntent(
                protocol=protocol,
                token=collateral_token,
                amount=Decimal("1"),
                chain=chain,
                market_id=market_id,
                is_collateral=True,
            ),
            WithdrawIntent(
                protocol=protocol,
                token=loan_token,
                amount=Decimal("50"),
                chain=chain,
                market_id=market_id,
                is_collateral=False,
            ),
        ]

    if intent_type == "BORROW":
        loan_token = _loan_token(chain, ctx.usdc)
        collateral_token = _collateral_token(chain, ctx.weth)
        # Synthetic bundled-collateral borrow for permission discovery only:
        # never executed, never accounted, so it bypasses the bundled-collateral
        # guard via for_permission_discovery (see BorrowIntent.validate_borrow_intent).
        return [
            BorrowIntent.for_permission_discovery(
                protocol=protocol,
                collateral_token=collateral_token,
                collateral_amount=Decimal("1"),
                borrow_token=loan_token,
                borrow_amount=Decimal("100"),
                chain=chain,
                market_id=market_id,
            )
        ]

    if intent_type == "REPAY":
        loan_token = _loan_token(chain, ctx.usdc)
        return [
            RepayIntent(
                protocol=protocol,
                token=loan_token,
                amount=Decimal("50"),
                chain=chain,
                market_id=market_id,
            )
        ]

    return None
