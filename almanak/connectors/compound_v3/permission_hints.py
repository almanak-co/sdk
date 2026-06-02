"""Compound V3 permission hints for permission discovery.

Compound V3 is market-id driven and routes SUPPLY on a token-vs-Comet-base
match: ``token == base`` calls ``Comet.supply()``; anything else calls
``Comet.supplyCollateral()``. A single synthetic SupplyIntent only exercises
one of the two paths — the manifest would miss the other selector. The
matching path depends on chain: USDC IS the Comet base on
ethereum/arbitrum/base/optimism (base path), but on polygon the Comet base
is USDC.e while the chain-default USDC is the native variant (collateral
path), so the missing selector flips by chain.

Rather than paper over this with ``static_permissions`` (which would
unconditionally over-authorise on the Safe — see codex review 3135601928 on
the morpho_blue PR), the connector owns its discovery vectors via
``build_discovery_vectors`` below: SUPPLY emits BOTH the base path and the
collateral path; WITHDRAW / BORROW / REPAY resolve ``base_token`` from the
per-chain ``COMPOUND_V3_MARKETS`` registry so the synthetic targets the
same Comet base the live compile path would. Mirrors the morpho_blue
flag-sweep pattern, scaled to compound's market-id + collateral list shape.
See :func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent


# Synthetic-discovery participation (VIB-4928): the four core lending
# primitives. Compound III discovery vectors are produced by
# ``build_discovery_vectors`` below.
PERMISSION_HINTS = PermissionHints(
    synthetic_discovery_intents=frozenset({"SUPPLY", "WITHDRAW", "BORROW", "REPAY"}),
)


def _synthetic_tokens(chain: str, fallback_usdc: str, hint_market_id: str | None) -> tuple[str, str | None, str | None]:
    """Resolve ``(base_token_address, first_collateral_address, market_id)`` for
    a chain's primary Compound V3 Comet.

    Falls back to ``fallback_usdc`` for the base and ``None`` for the
    collateral if the adapter lookup yields nothing for the chain. Market
    id resolution mirrors what the lending compiler does at runtime
    (``intent.market_id`` → ``default_compound_v3_market_for_chain(chain)``).
    """
    from .adapter import (
        COMPOUND_V3_MARKETS,
        default_compound_v3_market_for_chain,
    )

    market = hint_market_id or default_compound_v3_market_for_chain(chain)
    market_cfg = COMPOUND_V3_MARKETS.get(chain, {}).get(market, {})
    base_token = market_cfg.get("base_token_address") or fallback_usdc
    collaterals = market_cfg.get("collaterals", {})
    collateral_token: str | None = None
    if collaterals:
        first = next(iter(collaterals.values()), None)
        if isinstance(first, dict):
            collateral_token = first.get("address")
    return base_token, collateral_token, market


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Emit synthetic intents covering every selector the manifest needs.

    Compound V3 dispatches on market id and on a base-vs-collateral token
    match. Each operation has its own quirk:

      - SUPPLY  → emit the base path always; emit the collateral path when
        the market exposes at least one collateral asset.
      - WITHDRAW → single intent against the Comet base (NOT the chain
        default USDC — see in-branch comment).
      - BORROW  → single intent borrowing the Comet base against the
        chain-default WETH collateral.
      - REPAY   → single intent against the Comet base.

    Returns ``None`` for any ``intent_type`` other than SUPPLY / WITHDRAW /
    BORROW / REPAY so the framework default takes over (compound_v3 only
    owns lending discovery).
    """
    from almanak.framework.intents.vocabulary import (
        BorrowIntent,
        RepayIntent,
        SupplyIntent,
        WithdrawIntent,
    )

    hint_market_id = PERMISSION_HINTS.synthetic_market_id

    if intent_type == "SUPPLY":
        # Compound V3 routes SUPPLY on token-vs-Comet-base match: token==base calls
        # ``Comet.supply()``, anything else calls ``Comet.supplyCollateral()``. The
        # manifest needs BOTH selectors. Without a sweep, the single synthetic
        # SupplyIntent below resolves to whichever path the chain-default USDC
        # lands on — which is the base path on ethereum/arbitrum/base/optimism (USDC
        # IS the Comet base there) but the collateral path on polygon (Comet base is
        # USDC.e, not native USDC). Either way, exactly one of the two selectors
        # lands on the manifest and the other path reverts on the Safe. Mirrors the
        # morpho_blue flag sweep, which lives in
        # ``connectors/morpho_blue/permission_hints.build_discovery_vectors``.
        comp_base, comp_collateral, comp_market = _synthetic_tokens(chain, ctx.usdc, hint_market_id)
        out: list[AnyIntent] = [
            SupplyIntent(  # base-asset path -> Comet.supply()
                protocol=protocol,
                token=comp_base,
                amount=Decimal("100"),
                chain=chain,
                market_id=comp_market,
            )
        ]
        if comp_collateral:
            out.append(
                SupplyIntent(  # collateral path -> Comet.supplyCollateral()
                    protocol=protocol,
                    token=comp_collateral,
                    amount=Decimal("1"),
                    chain=chain,
                    market_id=comp_market,
                    use_as_collateral=True,
                )
            )
        return out

    if intent_type == "WITHDRAW":
        # Compound V3: use the Comet's base token, not the chain default USDC. On
        # polygon these differ (Comet base = USDC.e, chain default = native USDC),
        # so the chain-default would compile against a non-base asset and emit
        # withdrawCollateral selector parameters that don't match real withdraws.
        base_token, _collateral, market = _synthetic_tokens(chain, ctx.usdc, hint_market_id)
        return [
            WithdrawIntent(
                protocol=protocol,
                token=base_token,
                amount=Decimal("50"),
                chain=chain,
                market_id=market,
            )
        ]

    if intent_type == "BORROW":
        # Compound V3: borrow_token must match the Comet base (USDC.e on polygon,
        # not native USDC). Collateral fallback to weth covers ethereum/arbitrum/
        # base/optimism/polygon — all polygon Comet collaterals include WETH.
        base_token, _collateral, market = _synthetic_tokens(chain, ctx.usdc, hint_market_id)
        return [
            BorrowIntent(
                protocol=protocol,
                collateral_token=ctx.weth,
                collateral_amount=Decimal("1"),
                borrow_token=base_token,
                borrow_amount=Decimal("100"),
                chain=chain,
                market_id=market,
            )
        ]

    if intent_type == "REPAY":
        # Compound V3: repay token must match the Comet base. Same rationale as
        # WITHDRAW above — polygon's base is USDC.e, not native USDC.
        base_token, _collateral, market = _synthetic_tokens(chain, ctx.usdc, hint_market_id)
        return [
            RepayIntent(
                protocol=protocol,
                token=base_token,
                amount=Decimal("50"),
                chain=chain,
                market_id=market,
            )
        ]

    return None
