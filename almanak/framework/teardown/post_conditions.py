"""Teardown post-conditions: protocol-specific on-chain closure verification.

VIB-3742 — Framework hardening for graceful teardown.

Background
----------
``TeardownManager._verify_closure`` historically only re-read
``strategy.get_open_positions()`` — an in-memory call that returns 0
immediately after ``on_intent_executed`` clears the strategy's tracked
``_position_id``. Result: the framework reported teardown success while
liquidity remained on-chain (the $1.16-leak scenario behind VIB-3741 / 3742).

Design
------
``TeardownPostCondition`` is a small Protocol that protocol owners implement
to assert "this position is closed on-chain." The teardown manager iterates
the positions that existed *before* execution started (via the snapshot it
already took for ``starting_value_usd``) and dispatches each to the
post-condition registered for the position's protocol.

The registry is open: connectors can register their own post-condition by
calling ``register_teardown_post_condition(protocol, hook)`` at import time.
We ship a default for TraderJoe V2 because that's the protocol the bug was
filed against; future CL connectors (Aerodrome Slipstream, Uniswap V4,
Orca/Raydium CLMM) plug in via the same hook.

Hard constraints
----------------
- All on-chain reads MUST go through the gateway in production. The TJ V2
  default uses ``TraderJoeV2Adapter`` which already accepts a
  ``GatewayClient`` and routes RPC through it. Callers that test locally
  may pass ``rpc_url`` for direct anvil access; this is the same dual-path
  the compiler uses.
- Failures in a hook return ``ClosureCheckResult(closed=False)`` with an
  error message rather than raising — verification is informational and a
  hook crash must not silently pass the teardown.
- No emojis. No Postgres DDL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class ClosureCheckResult:
    """Outcome of an on-chain closure verification for a single position.

    Attributes:
        closed: True iff the post-condition determined the position is fully
            closed on-chain. False means residual liquidity / debt was
            detected, OR the check itself errored out (fail-closed).
        protocol: Protocol the result is for (informational, for logs).
        position_id: Position identifier checked.
        residual: Protocol-specific residual data (e.g.
            ``{"bin_balances": {123: 4567}, "total_lb_tokens": 4567}`` for
            TraderJoe V2). Empty when ``closed=True``.
        error: Set when the check itself failed (RPC error, missing
            dependency, etc.). Treated as ``closed=False`` for the
            teardown verifier — fail-closed.
    """

    closed: bool
    protocol: str = ""
    position_id: str = ""
    residual: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class TeardownPostCondition(Protocol):
    """Protocol-specific on-chain closure check.

    Implementations must be side-effect free — only read state. They run
    AFTER the teardown intents have completed; the teardown manager uses
    the result to decide whether to mark the teardown ``success=False``.

    Implementations should:
    - Return ``ClosureCheckResult(closed=True)`` only when ALL liquidity /
      debt for the given position is provably gone on-chain.
    - Return ``ClosureCheckResult(closed=False, residual=...)`` with a
      detailed residual map when liquidity remains. The residual goes into
      the teardown ``error`` so operators can see exactly which bins / NFTs
      / accounts still have value.
    - Return ``ClosureCheckResult(closed=False, error=...)`` on internal
      failure. The teardown verifier treats this as fail-closed.
    """

    def __call__(
        self,
        position: Any,
        wallet_address: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
    ) -> ClosureCheckResult: ...


# =============================================================================
# Registry
# =============================================================================

_REGISTRY: dict[str, TeardownPostCondition] = {}


def register_teardown_post_condition(protocol: str, hook: TeardownPostCondition) -> None:
    """Register a post-condition for a protocol.

    Idempotent: re-registering the same hook is fine. Replacing an existing
    hook logs a warning so accidental shadowing is visible in logs.
    """
    key = protocol.lower()
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not hook:
        logger.warning(
            "Replacing existing teardown post-condition for protocol %r",
            protocol,
        )
    _REGISTRY[key] = hook


def get_teardown_post_condition(protocol: str) -> TeardownPostCondition | None:
    """Look up a registered post-condition. Returns ``None`` when none."""
    return _REGISTRY.get(protocol.lower())


def has_teardown_post_condition(protocol: str) -> bool:
    """``True`` iff a post-condition is registered for ``protocol``."""
    return protocol.lower() in _REGISTRY


# =============================================================================
# TraderJoe V2 default post-condition
# =============================================================================


def _traderjoe_v2_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
) -> ClosureCheckResult:
    """Verify a TraderJoe V2 LP position has zero residual LB token balance.

    Uses the SDK's ``balanceOfBatch`` over the position's known bin_ids when
    they're present in ``position.details``; otherwise falls back to the
    same ±50 bin scan the compiler uses (with the same caveats). When the
    fallback path runs we mark the result with ``residual['fallback_scan']``
    so operators can see the scan was inherently incomplete and treat
    ``closed=True`` from the fallback with appropriate skepticism.
    """
    protocol = "traderjoe_v2"
    position_id = getattr(position, "position_id", "") or ""

    # Gate by position type: ``protocol="traderjoe_v2"`` is shared between
    # LP positions (``PositionType.LP`` — the LB pair / bin-balances shape
    # this hook verifies) and TOKEN positions reported by swap-only
    # strategies (e.g. S-008 RSI flipper on Avalanche, which surfaces
    # ``PositionType.TOKEN`` with ``details={"asset": "WAVAX", "balance": ...}``
    # and no ``pool_address``). The LB-pair-shaped check must NOT run on
    # TOKEN positions — doing so fail-closes every swap-only TraderJoe V2
    # teardown on the missing-pool_address branch (VIB-3974). Mirror the
    # Uniswap V3 hook's gate: treat non-LP positions as "outside this
    # hook's scope" — closed=True with a residual note so the verifier
    # moves on. Balance-zero verification for TOKEN positions is the
    # strategy's ``get_open_positions()`` contract, not this hook's
    # responsibility.
    position_type_raw = getattr(position, "position_type", None)
    position_type_value = (getattr(position_type_raw, "value", None) or str(position_type_raw or "")).upper()
    if position_type_value and position_type_value != "LP":
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "skipped_reason": (
                    f"TraderJoe V2 post-condition only verifies LP LB-pair positions; "
                    f"position_type={position_type_value!r} is outside scope"
                ),
            },
        )

    # Pull pool + bin metadata. ``position`` is a teardown ``PositionInfo``
    # (from ``strategy.get_open_positions()``), or anything else with a
    # ``.details`` mapping.
    details = getattr(position, "details", None) or {}
    pool_address = details.get("pool_address") or details.get("pool_addr") or details.get("pool")
    if not pool_address:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "TraderJoe V2 post-condition needs position.details['pool_address'] "
                "(or 'pool', 'pool_addr'); none found"
            ),
        )

    # VIB-3943: ``details["pool"]`` is dual-purpose across the codebase — some
    # callers stash the LB pair address there, others stash a symbol triple
    # like ``"WAVAX/USDC/20"``. Without this hex check the symbol path slips
    # straight into ``balanceOf`` and web3.py raises ``ValueError: when sending
    # a str, it must be a hex string``. The on-chain TX already succeeded by
    # the time we get here, so a verifier crash flips a successful teardown
    # to a false-positive failure.  Reject non-hex addresses up front so the
    # operator gets a precise, actionable message instead of a stack trace.
    if not (isinstance(pool_address, str) and pool_address.startswith("0x") and len(pool_address) == 42):
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"TraderJoe V2 post-condition: position.details pool_address must be a "
                f"42-char hex address (got {pool_address!r}). Strategies must populate "
                "details['pool_address'] with the LB pair contract address, not a symbol."
            ),
        )

    try:
        from almanak.framework.connectors.traderjoe_v2 import (
            TraderJoeV2Adapter,
            TraderJoeV2Config,
        )
    except Exception as exc:  # noqa: BLE001 — defensive
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 connector unavailable: {exc}",
        )

    chain = getattr(position, "chain", None) or "avalanche"

    config = TraderJoeV2Config(
        chain=chain,
        wallet_address=wallet_address,
        rpc_url=rpc_url if gateway_client is None else None,
        gateway_client=gateway_client,
    )

    try:
        adapter = TraderJoeV2Adapter(config)
        sdk = adapter.sdk
    except Exception as exc:  # noqa: BLE001
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 SDK init failed: {exc}",
        )

    # Prefer the explicit bin_ids list — that's the only guaranteed-complete
    # check. If we have it, we're done with one balanceOfBatch round-trip.
    bin_ids_raw = details.get("bin_ids") or []
    try:
        known_bin_ids = [int(b) for b in bin_ids_raw]
    except (TypeError, ValueError):
        known_bin_ids = []

    used_fallback = False
    try:
        if known_bin_ids:
            balances = sdk.get_position_balances_for_ids(pool_address, wallet_address, known_bin_ids)
        else:
            used_fallback = True
            # The active-bin ±50 fallback. Mark as incomplete in residual so
            # the verifier surfaces "scan may have missed bins" to operators.
            balances = sdk.get_position_balances(pool_address, wallet_address)
    except Exception as exc:  # noqa: BLE001
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"TraderJoe V2 balanceOf query failed: {exc}",
        )

    residual: dict[str, Any] = {}
    if balances:
        residual["bin_balances"] = {int(b): int(v) for b, v in balances.items()}
        residual["total_lb_tokens"] = int(sum(balances.values()))
        residual["pool_address"] = pool_address
        if used_fallback:
            residual["fallback_scan"] = (
                "Used active-id ±50 heuristic (bin_ids unavailable in position.details). "
                "Bins outside that window were NOT checked."
            )

    if balances:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            residual=residual,
        )

    if used_fallback:
        # Closed-with-asterisk: heuristic returned empty, but it only checked
        # ±50 bins. Surface the caveat so operators know the verifier ran in
        # weak-mode for this position. We still return closed=True because
        # marking otherwise would block every TJ V2 teardown that did not
        # round-trip bin_ids — defeating the purpose of best-effort
        # verification. The compiler-level WARNING (item 1) catches the
        # silent-leak case at a different seam.
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "fallback_scan": (
                    "TraderJoe V2 post-condition used active-id ±50 fallback "
                    "(no bin_ids in position.details). No residual liquidity "
                    "found in the scanned window, but bins outside it were "
                    "NOT checked. To get strong verification, ensure your "
                    "strategy attaches bin_ids to the PositionInfo.details "
                    "for teardown."
                ),
            },
        )

    return ClosureCheckResult(
        closed=True,
        protocol=protocol,
        position_id=position_id,
    )


register_teardown_post_condition("traderjoe_v2", _traderjoe_v2_post_condition)


# =============================================================================
# Uniswap V3 (and forks) default post-condition
# =============================================================================
#
# A Uniswap V3 LP position is identified by an NFT tokenId on the
# NonfungiblePositionManager contract. There are two ways the position can
# legitimately end up "closed" after a teardown:
#
#   1. The teardown decreases liquidity, collects fees, AND burns the NFT.
#      ``positions(tokenId)`` then reverts with "Invalid token ID" because
#      the NFT no longer exists. This is the canonical Uniswap V3 LP_CLOSE
#      flow used by every demo/incubating strategy in this repo.
#   2. The teardown decreases liquidity and collects fees but skips burn.
#      The NFT still exists with ``liquidity == 0`` and
#      ``tokensOwed{0,1} == 0``. The position is empty but the wallet
#      still owns the NFT shell.
#
# The legacy in-memory check ``strategy.get_open_positions()`` runs BEFORE
# the strategy's ``on_teardown_completed`` hook clears the tracked
# ``_position_id`` — so it returns the same NFT tokenId that the teardown
# just torched, and the verifier raises a false-positive
# "positions still open" error: the on-chain truth was
# "NFT burnt, wallet holds only USDC", but the in-memory state
# claimed the position was open.
#
# This post-condition reads on-chain truth via the gateway's typed
# QueryPositionLiquidity / QueryPositionTokensOwed RPCs. Both already
# fold the "invalid token id" revert into a value-0 response, so a burnt
# NFT is correctly classified as closed without raising. For non-burnt
# but fully-decremented positions we cross-check ``tokensOwed{0,1}`` so
# residual fees do not slip past as "closed". Same registry mechanism as
# TJ V2: the V3 forks (Aerodrome Slipstream, PancakeSwap V3, SushiSwap V3,
# Agni Finance, JAINE DEX on 0G) share the same NPM ABI, so the hook
# registers under each protocol slug.

# Map protocol slug -> contracts registry key. Only protocols whose
# ``almanak.core.contracts`` registry actually carries a
# ``position_manager`` address are listed here — registering a slug
# without an NPM would cause every teardown of that protocol to
# fail-closed with "no NPM registered". PancakeSwap V3 has connector
# coverage for swaps but no NPM entry today; if/when an NPM lands in
# ``contracts.py`` add it here in the same line.
_V3_PROTOCOL_TO_REGISTRY = {
    "uniswap_v3": "UNISWAP_V3",
    "agni_finance": "AGNI_FINANCE",
    "sushiswap_v3": "SUSHISWAP_V3",
}


def _resolve_v3_position_manager(protocol: str, chain: str) -> str | None:
    """Look up the NonfungiblePositionManager address for a V3-fork protocol.

    Returns ``None`` when the protocol is not registered or the chain has
    no deployment. Callers fail-closed on ``None``.
    """
    registry_name = _V3_PROTOCOL_TO_REGISTRY.get(protocol.lower())
    if registry_name is None:
        return None
    try:
        from almanak.core import contracts as _contracts
    except Exception:  # noqa: BLE001 — defensive
        return None
    registry: dict[str, dict[str, str]] = getattr(_contracts, registry_name, None) or {}
    chain_entry = registry.get(chain.lower()) or registry.get(chain) or {}
    npm = chain_entry.get("position_manager")
    return npm or None


def _uniswap_v3_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
) -> ClosureCheckResult:
    """Verify a Uniswap V3 (or V3-fork) LP NFT is closed on-chain.

    Reads on-chain truth via the gateway's ``QueryPositionLiquidity`` and
    ``QueryPositionTokensOwed`` RPCs. Closure rules:

    - ``liquidity == 0`` AND ``tokensOwed0 == 0`` AND ``tokensOwed1 == 0``
      → ``closed=True``. This covers BOTH (a) the burnt-NFT path
      (``positions(tokenId)`` reverts with "Invalid token ID"; the gateway
      folds that revert into ``liquidity = 0`` and ``tokensOwed = (0, 0)``)
      AND (b) the decrease-without-burn path.
    - Any non-zero residual → ``closed=False`` with a residual map.
    - Either RPC returning ``None`` (gateway disconnected, RPC timeout,
      malformed response) → ``closed=False`` with an error string.
      Fail-closed: an unknown on-chain state must NOT be reported as
      closed.

    No direct network egress: the post-condition uses the supplied
    ``gateway_client``. ``rpc_url`` is intentionally NOT consumed here —
    framework code MUST go through the gateway boundary; tests that need
    to drive the closure paths inject a fake gateway_client.

    Note on the registered slug set: this hook is registered for the
    slugs in ``_V3_PROTOCOL_TO_REGISTRY`` (``uniswap_v3``, ``agni_finance``,
    ``sushiswap_v3``) — every V3-fork that exposes the canonical NPM ABI
    AND has a ``position_manager`` entry in ``almanak.core.contracts``.
    PancakeSwap V3 is intentionally NOT registered today because no NPM
    address is published in ``contracts.py`` for it (see the comment
    above ``_V3_PROTOCOL_TO_REGISTRY``); add it there before adding the
    slug here. Aerodrome's volatile/stable pools use ERC-20 LP tokens,
    not NFTs, so they do NOT register here; their teardown closure check
    is a different primitive and falls through to the legacy in-memory
    check until a dedicated post-condition is added.
    """
    protocol_raw = getattr(position, "protocol", "") or ""
    protocol = protocol_raw.lower() or "uniswap_v3"
    position_id_raw = getattr(position, "position_id", "") or ""
    position_id = str(position_id_raw)

    # Gate by position type: ``protocol="uniswap_v3"`` is shared between
    # LP NFT positions (``PositionType.LP``) and TOKEN positions reported by
    # swap-only strategies (``uniswap_rsi`` etc., which surface
    # ``PositionType.TOKEN`` with non-numeric ids like
    # ``"uniswap_rsi_token_0"``). The NFT-shaped check must NOT run on
    # TOKEN positions — doing so would fail-closed on every swap-strategy
    # teardown. We treat non-LP positions as "outside this hook's scope":
    # closed=True with a residual note so the verifier moves on. Balance-
    # zero verification for TOKEN positions is the strategy's
    # ``get_open_positions()`` contract, not this hook's responsibility.
    position_type_raw = getattr(position, "position_type", None)
    position_type_value = (getattr(position_type_raw, "value", None) or str(position_type_raw or "")).upper()
    if position_type_value and position_type_value != "LP":
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
            residual={
                "skipped_reason": (
                    f"Uniswap V3 post-condition only verifies LP NFT positions; "
                    f"position_type={position_type_value!r} is outside scope"
                ),
            },
        )

    chain = getattr(position, "chain", None) or ""
    if not chain:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error="Uniswap V3 post-condition needs position.chain; none found",
        )

    # NFT tokenId resolution: strategies that store a human-readable
    # ``position_id`` (e.g. ``"sushiswap-v3-lp-WETH-USDC-bsc"``) put the
    # actual numeric NFT id in ``position.details``.  Three key conventions
    # exist across the demo / incubating tree (no canonical name today):
    #
    #   * ``nft_position_id`` — sushiswap_v3, uniswap_v3 LP lifecycle,
    #     pancakeswap_v3 (most common shape)
    #   * ``nft_id`` — morpho_univ3_leveraged_lp, agni_lp_mantle,
    #     aave_uniswap_yield_stack, sushiswap_v3_optimism
    #   * ``position_id`` / ``token_id`` — strategies that mirror the
    #     attribute name into details for their own bookkeeping
    #
    # We try all four keys, then fall back to ``position.position_id`` for
    # strategies that store the numeric NFT id directly on the attribute.
    # Adding the lookup at the verifier layer keeps the fix one-edit
    # instead of editing every strategy.
    details = getattr(position, "details", None) or {}
    _NFT_ID_KEYS = ("nft_position_id", "nft_id", "token_id", "position_id")
    raw_nft_id: Any = None
    for key in _NFT_ID_KEYS:
        candidate = details.get(key)
        if candidate is not None and candidate != "":
            raw_nft_id = candidate
            break
    if raw_nft_id is None:
        raw_nft_id = position_id
    try:
        token_id = int(raw_nft_id)
    except (TypeError, ValueError):
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Uniswap V3 post-condition: could not resolve a numeric NFT "
                f"tokenId (details keys {' / '.join(_NFT_ID_KEYS)} were "
                f"empty or non-numeric, position_id={position_id!r}); "
                f"cannot verify on-chain closure"
            ),
        )

    if gateway_client is None:
        # Framework rule: no egress from the strategy container. Without a
        # gateway client we have no authoritative way to verify on-chain
        # closure — fail-closed so a missing client is loud, not silent.
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Uniswap V3 post-condition requires a gateway_client to read "
                "on-chain truth (NPM.positions / liquidity / tokensOwed). None "
                "supplied — verification cannot proceed."
            ),
        )

    npm_address = _resolve_v3_position_manager(protocol, chain)
    if not npm_address:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Uniswap V3 post-condition: no NonfungiblePositionManager "
                f"registered for protocol={protocol!r} on chain={chain!r}"
            ),
        )

    # Read on-chain truth via the gateway. Both helpers already fold the
    # "invalid token id" revert (canonical Uniswap V3 NPM behaviour for a
    # burnt NFT) into a value-0 response, so we don't need to pre-check
    # ownerOf separately.
    try:
        liquidity = gateway_client.query_position_liquidity(
            chain=chain,
            position_manager=npm_address,
            token_id=token_id,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"Uniswap V3 query_position_liquidity raised: {exc}",
        )

    if liquidity is None:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Uniswap V3 query_position_liquidity returned None "
                "(gateway/RPC error); cannot confirm closure — fail-closed"
            ),
        )

    try:
        tokens_owed = gateway_client.query_position_tokens_owed(
            chain=chain,
            position_manager=npm_address,
            token_id=token_id,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"Uniswap V3 query_position_tokens_owed raised: {exc}",
        )

    if tokens_owed is None:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Uniswap V3 query_position_tokens_owed returned None "
                "(gateway/RPC error); cannot confirm closure — fail-closed"
            ),
        )

    tokens_owed0, tokens_owed1 = tokens_owed

    if liquidity == 0 and tokens_owed0 == 0 and tokens_owed1 == 0:
        return ClosureCheckResult(
            closed=True,
            protocol=protocol,
            position_id=position_id,
        )

    residual: dict[str, Any] = {
        "position_manager": npm_address,
        "token_id": token_id,
        "liquidity": int(liquidity),
        "tokens_owed0": int(tokens_owed0),
        "tokens_owed1": int(tokens_owed1),
    }
    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual=residual,
    )


for _v3_slug in _V3_PROTOCOL_TO_REGISTRY:
    register_teardown_post_condition(_v3_slug, _uniswap_v3_post_condition)


__all__ = [
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
    "register_teardown_post_condition",
]
