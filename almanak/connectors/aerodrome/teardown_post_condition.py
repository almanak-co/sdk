"""Aerodrome teardown post-condition.

Slipstream LP positions are ERC-721 NFTs on one of TWO reviewed
NonfungiblePositionManager generations, so the V3-family hook (which resolves
one NPM per protocol) cannot serve them. This hook resolves the exact reviewed
manager the durable position names — the same resolver the post-teardown
reconciliation lane uses — and then applies the shared NPM closure rule
(``liquidity == 0`` and ``tokensOwed == (0, 0)``) through the gateway.

Classic (Solidly) LP positions reuse the framework's fungible-LP balance read
(the pool contract IS the LP token) with one asymmetry forced by the clamped
close: a wallet LP-token balance of zero is a MEASURED closure, but a non-zero
balance is ``unmeasured`` rather than a residual, because a Classic close is
clamped to this deployment's own outstanding liquidity and the remainder may
belong to another deployment or to the user (VIB-6162 / VIB-6487). Reporting
that remainder as a residual would false-fail a correct close.

Three-valued, Empty ≠ Zero: a measured residual is ``closed=False``; a read
fault, a missing chain/token id/gateway, or an ambiguous manager identity is
``unmeasured=True`` (UNVERIFIED), never a fabricated residual.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import (
    ClosureCheckResult,
    resolve_nft_token_id,
    verify_npm_position_closure,
)

SLIPSTREAM_PROTOCOL = "aerodrome_slipstream"
_LABEL = "Aerodrome Slipstream"


def _unmeasured(protocol: str, position_id: str, error: str) -> ClosureCheckResult:
    return ClosureCheckResult(closed=False, unmeasured=True, protocol=protocol, position_id=position_id, error=error)


def _classic_closure(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None,
    rpc_url: str | None,
    block: int | str | None,
    protocol: str,
    position_id: str,
) -> ClosureCheckResult:
    """Classic LP: zero LP-token balance proves closure; a remainder is honest don't-know.

    Delegates the read to the framework fungible-LP hook (bounded retry,
    ``details['lp_token']`` > ``lp_token_address`` > address-shaped
    ``position_id`` resolution) and only re-labels its one verdict that the
    clamped Classic close makes ambiguous: a MEASURED non-zero balance.
    """
    from almanak.connectors._strategy_base.fungible_lp_post_condition import fungible_lp_teardown_post_condition

    result = fungible_lp_teardown_post_condition(
        position, wallet_address, gateway_client=gateway_client, rpc_url=rpc_url, block=block
    )
    if result.closed or result.unmeasured or result.not_applicable:
        return result
    return ClosureCheckResult(
        closed=False,
        unmeasured=True,
        protocol=protocol,
        position_id=position_id,
        residual=result.residual,
        error=(
            "Aerodrome Classic post-condition: the wallet still holds LP tokens of this pool, but a Classic close "
            "is clamped to this deployment's own liquidity, so the remainder cannot be attributed to this "
            f"position (may belong to another deployment or the user); not certifiable. {result.error or ''}".rstrip()
        ),
    )


def aerodrome_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify an Aerodrome position is closed on-chain (Slipstream NFTs; Classic LP-token balance)."""
    protocol = (getattr(position, "protocol", "") or "").lower() or "aerodrome"
    position_id = str(getattr(position, "position_id", "") or "")

    position_type_raw = getattr(position, "position_type", None)
    position_type = (getattr(position_type_raw, "value", None) or str(position_type_raw or "")).upper()
    if position_type and position_type != "LP":
        return ClosureCheckResult(
            closed=True,
            not_applicable=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "skipped_reason": (
                    f"Aerodrome post-condition only verifies LP positions; "
                    f"position_type={position_type!r} is outside scope"
                ),
            },
        )

    if protocol != SLIPSTREAM_PROTOCOL:
        return _classic_closure(position, wallet_address, gateway_client, rpc_url, block, protocol, position_id)

    chain = str(getattr(position, "chain", None) or "").lower()
    if not chain:
        return _unmeasured(protocol, position_id, f"{_LABEL} post-condition needs position.chain; none found")

    token_id = resolve_nft_token_id(position)
    if token_id is None:
        return _unmeasured(
            protocol,
            position_id,
            f"{_LABEL} post-condition: could not resolve a numeric NFT tokenId "
            f"(position_id={position_id!r}); cannot verify on-chain closure",
        )

    if gateway_client is None:
        return _unmeasured(
            protocol,
            position_id,
            f"{_LABEL} post-condition requires a gateway_client to read on-chain truth "
            "(NPM.positions / liquidity / tokensOwed). None supplied — verification cannot proceed.",
        )

    # The exact reviewed manager this position lives on. Multi-generation: the
    # durable position must name its manager; an ambiguous identity is honest
    # don't-know, never a probe of the other generation's identically numbered NFT.
    from almanak.framework.teardown.live_position_reads import reviewed_npm_for_position

    npm_address = reviewed_npm_for_position(position=position, protocol=protocol, chain=chain)
    if not npm_address:
        return _unmeasured(
            protocol,
            position_id,
            f"{_LABEL} post-condition: no unambiguous reviewed position manager for tokenId={token_id} on "
            f"chain={chain!r} (position.details must name its nft_manager on a multi-generation chain)",
        )

    return verify_npm_position_closure(
        protocol=protocol,
        position_id=position_id,
        chain=chain,
        token_id=token_id,
        npm_address=npm_address,
        gateway_client=gateway_client,
        block=block,
        label=_LABEL,
    )


__all__ = ["SLIPSTREAM_PROTOCOL", "aerodrome_teardown_post_condition"]
