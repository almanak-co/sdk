"""Uniswap V4 teardown post-condition (VIB-5634).

A Uniswap V4 LP position is an NFT tokenId on the V4 PositionManager — a DISTINCT
contract from the V3 NonfungiblePositionManager, with a different read ABI
(``getPositionLiquidity`` / ``getPoolAndPositionInfo``, not ``positions(tokenId)``).
So V4 is NOT in ``AbiFamily.V3_NPM`` and the framework's V3 hook cannot verify it.

Before this hook, no V4 post-condition existed: a just-closed V4 position was, at
best, counted closed-by-execution (UNVERIFIED) — and when its empty-return read
raised through an unguarded decoder ("invalid string length"), the teardown
mis-reported FAILED / "0 of 1 confirmed". The TD-15 Plan-A reconciliation already
scopes V4 to ``NOT_APPLICABLE``, explicitly "deferring to its registered TD-14
post-condition" — this IS that hook, so the deferral now resolves to a real
verifier instead of a no-op.

Closure rules mirror the V3 hook (Empty != Zero, VIB-5573):

- the gateway reports the position gone (empty-return, burned NFT) OR a measured
  full drain (liquidity == 0 and no owed fees) -> ``closed=True``;
- a measured residual (liquidity / owed fees > 0) -> ``closed=False`` + residual
  map -> the seam FAILS the teardown;
- a read fault (RPC error, decode fault, missing client / addresses, unresolvable
  tokenId) -> ``unmeasured=True`` -> UNVERIFIED, never FAILED.

No direct network egress: all reads go through the supplied ``gateway_client``
(the gateway boundary). ``rpc_url`` is intentionally NOT consumed.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import (
    NFT_ID_DETAIL_KEYS,
    ClosureCheckResult,
    resolve_nft_token_id,
)


def uniswap_v4_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a Uniswap V4 LP NFT is closed on-chain (VIB-5634).

    Reads live state via the gateway ``QueryV4PositionState`` RPC (through
    ``gateway_client.query_v4_position_closure``), block-pinned to ``block`` (the
    close-tx receipt's block, VIB-5140/5148 parity) so a read replica trailing the
    writer cannot return PRE-close state and false-negative the closure.
    """
    protocol = str(getattr(position, "protocol", "") or "").lower() or "uniswap_v4"
    position_id = str(getattr(position, "position_id", "") or "")

    # Gate by position type: this NFT-shaped check must only run on LP positions.
    # A non-LP position (e.g. a TOKEN surfaced by a swap-only strategy) is outside
    # scope — report closed=True so the verifier moves on (mirrors the V3 hook).
    position_type_raw = getattr(position, "position_type", None)
    position_type_value = (getattr(position_type_raw, "value", None) or str(position_type_raw or "")).upper()
    if position_type_value and position_type_value != "LP":
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "skipped_reason": (
                    f"Uniswap V4 post-condition only verifies LP NFT positions; "
                    f"position_type={position_type_value!r} is outside scope"
                ),
            },
        )

    chain = str(getattr(position, "chain", None) or "").lower()
    if not chain:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error="Uniswap V4 post-condition needs position.chain; none found",
        )

    # Resolve the numeric NFT tokenId via the SHARED rule: detail keys
    # first, then the bare ``position_id`` — the same ``resolve_nft_token_id``
    # every verification lane uses. The helper carries this hook's type
    # discipline: bool / float are rejected BEFORE ``int()`` (``int(True)==1``,
    # ``int(1.5)==1`` would coerce a bad id into a valid-looking-but-WRONG
    # tokenId that queries the wrong position on-chain) — UNMEASURED
    # (fail-safe -> UNVERIFIED), never treated as closed.
    token_id = resolve_nft_token_id(position)
    if token_id is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Uniswap V4 post-condition: could not resolve a numeric NFT tokenId "
                f"(details keys {' / '.join(NFT_ID_DETAIL_KEYS)} were empty, non-numeric, "
                f"or of a non-integer type; position_id={position_id!r}); "
                f"cannot verify on-chain closure"
            ),
        )

    if gateway_client is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Uniswap V4 post-condition requires a gateway_client to read on-chain "
                "truth (V4 PositionManager liquidity / fees). None supplied — "
                "verification cannot proceed."
            ),
        )

    # Resolve the connector-owned V4 PositionManager + StateView for the chain.
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    addresses = UNISWAP_V4.get(chain) or {}
    position_manager = addresses.get("position_manager")
    state_view = addresses.get("state_view")
    if not position_manager or not state_view:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Uniswap V4 post-condition: no V4 PositionManager / StateView registered "
                f"for chain={chain!r} — cannot verify on-chain closure"
            ),
        )

    try:
        read = gateway_client.query_v4_position_closure(
            chain=chain,
            position_manager=position_manager,
            state_view=state_view,
            token_id=token_id,
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed (UNMEASURED, not a residual)
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=f"Uniswap V4 query_v4_position_closure raised: {exc}",
        )

    # UNMEASURED (read fault) -> UNVERIFIED, never FAILED (VIB-5573).
    if read.unmeasured:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=read.error or "Uniswap V4 closure read unmeasured (gateway/RPC fault)",
        )

    # MEASURED closed (position gone / fully drained).
    if read.closed:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    # MEASURED residual -> FAILED.
    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={
            "position_manager": position_manager,
            "token_id": token_id,
            "liquidity": int(read.residual_liquidity),
            "tokens_owed0": int(read.residual_owed0),
            "tokens_owed1": int(read.residual_owed1),
        },
    )


__all__ = ["uniswap_v4_post_condition"]
