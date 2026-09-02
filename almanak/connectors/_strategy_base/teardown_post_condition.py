"""Strategy-side teardown post-condition registry."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class ClosureCheckResult:
    """Outcome of an on-chain closure verification for a single position.

    Three-valued by design (VIB-5573, Empty ≠ Zero): a position is either
    MEASURED-closed, MEASURED-open (residual), or UNMEASURED (the read itself
    could not be completed). Conflating "we could not read" with "we read a
    residual" is a real bug: it lets a transient gateway/RPC blip during the
    post-teardown verify fabricate a residual → ``FAILED`` → hosted shutdown +
    entry latch on a healthy strategy. So a read fault sets ``unmeasured=True``
    (→ ``UNVERIFIED``, honest don't-know) and NEVER masquerades as a residual.
    Only a *positive on-chain measurement* of residual value is ``closed=False``
    (→ ``FAILED``).

    VIB-6285 adds a FOURTH state, ``not_applicable``, for a hook that is
    structurally OUT OF SCOPE for the position it was handed — e.g. the
    NFT-shaped Uniswap V3/V4 LP hooks reached with a ``PositionType.TOKEN`` row,
    because ``protocol="uniswap_v3"`` is shared between LP NFT positions and the
    TOKEN rows swap-only strategies surface. These skips previously returned a
    bare ``closed=True``, which is indistinguishable from a real measurement:
    the verifier counted the position chain-verified and recorded a
    ``hook_proven_position_keys`` entry having read NOTHING on-chain. That is a
    fabricated proof, and it is exactly the false-green vector
    ``lending_post_condition`` already refuses ("NOT a silent closed=True skip …
    a silent skip is a false-green vector").

    Out-of-scope is NOT the same as unmeasured, and the two must not be merged:
    marking these skips ``unmeasured=True`` would lower every swap-strategy
    teardown to UNVERIFIED and block certification for a whole strategy class.
    ``not_applicable`` says "this hook has no opinion about this position" — it
    contributes neither proof nor doubt, mirroring ``NOT_APPLICABLE`` in
    ``plan_a_reconciliation``.

    Attributes:
        closed: True iff the post-condition MEASURED the position fully closed
            on-chain. Only meaningful when ``unmeasured`` and ``not_applicable``
            are both False.
        unmeasured: True iff the check could not obtain a trustworthy on-chain
            reading (gateway/RPC fault after bounded read-retry, missing client,
            unresolved address, unsupported vault interface). The composition
            seam lowers this to ``UNVERIFIED`` — never ``FAILED``. When True,
            ``closed`` is ignored and MUST NOT be treated as a residual.
        not_applicable: True iff this hook is structurally out of scope for the
            position (wrong ``position_type`` for the check it implements).
            Takes precedence over ``closed``: the result contributes NO closure
            evidence — not to ``positions_with_hook``, not to
            ``hook_proven_position_keys`` — and is never a residual. Use this,
            never a bare ``closed=True``, for an out-of-scope skip.
        protocol: Protocol the result is for, for logs and operator output.
        position_id: Position identifier checked.
        residual: Protocol-specific residual data (only set on a MEASURED
            residual, i.e. ``closed=False`` AND ``unmeasured=False``).
        error: Human-readable reason. Set on a read fault (``unmeasured=True``)
            or, rarely, alongside a measured residual for operator context.
    """

    closed: bool
    protocol: str = ""
    position_id: str = ""
    residual: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    unmeasured: bool = False
    not_applicable: bool = False


class TeardownPostCondition(Protocol):
    """Protocol-specific on-chain closure check.

    VIB-5140: ``block`` is an OPTIONAL block reference (the close-tx receipt's
    ``block_number``). Hooks that re-query on-chain state SHOULD pin their
    reads to it so a read replica trailing the writer cannot return PRE-close
    state and false-negative the closure check. ``None`` (the default for any
    caller that omits it) preserves the legacy ``"latest"`` behaviour.
    """

    def __call__(
        self,
        position: Any,
        wallet_address: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        block: int | str | None = None,
    ) -> ClosureCheckResult: ...


# NFT tokenId key conventions, in priority order. Strategies that store a
# human-readable ``position_id`` (e.g. ``"sushiswap-v3-lp-WETH-USDC-bsc"``) put
# the actual numeric NFT id in ``position.details``. Three key conventions
# exist across the demo / incubating tree (no canonical name today):
#
#   * ``nft_position_id`` — sushiswap_v3, uniswap_v3 LP lifecycle,
#     pancakeswap_v3 (most common shape)
#   * ``nft_id`` — morpho_univ3_leveraged_lp, agni_lp_mantle,
#     aave_uniswap_yield_stack, sushiswap_v3_optimism
#   * ``position_id`` / ``token_id`` — strategies that mirror the attribute
#     name into details for their own bookkeeping
NFT_ID_DETAIL_KEYS: tuple[str, ...] = ("nft_position_id", "nft_id", "token_id", "position_id")


def resolve_nft_token_id(position: Any) -> int | None:
    """Resolve the numeric ERC-721 NFT tokenId for an NFT-based LP position.

    THE single id-resolution rule for every lane that reads a position's NFT
    tokenId back from chain — the TD-14 post-condition hooks (the framework V3
    family hook and connector-owned hooks such as Uniswap V4's) AND the Plan-A
    per-KNOWN-position reconciliation read
    (``almanak.framework.teardown.live_position_reads.chain_verify_lp_open``).
    Before this helper each lane had its own copy: Plan-A resolved only a
    numeric ``position.position_id`` while TD-14 also read the detail keys, so
    a strategy using a human-readable position id (``"my-lp-1"``) with the NFT
    id in ``details`` verified fine in TD-14 but reconciled UNVERIFIABLE in
    Plan-A — the two lanes disagreed about the same position (the
    VIB-5631 parity follow-up).

    Resolution order:

    1. ``position.details[key]`` for the first key in
       :data:`NFT_ID_DETAIL_KEYS` holding a non-``None``, non-empty value.
       A non-dict / missing ``details`` contributes nothing (malformed
       payloads must degrade to "unresolvable", never crash a verifier).
    2. Fallback: the ``position.position_id`` attribute (string-coerced) for
       strategies that store the numeric NFT id directly on the attribute.

    Type discipline (mirrors the Uniswap V4 hook): a tokenId is a base-10
    integer or its string form ONLY. ``bool`` / ``float`` are rejected before
    ``int()`` — ``int(True) == 1`` and ``int(1.5) == 1`` would coerce a bad id
    into a valid-looking-but-WRONG tokenId that queries someone else's
    position on-chain.

    Returns:
        The numeric tokenId, or ``None`` when no numeric id can be resolved —
        callers MUST treat ``None`` as *unresolvable / unmeasured* (TD-14:
        ``unmeasured=True`` → UNVERIFIED; Plan-A: ``None`` → UNVERIFIABLE),
        never as "closed" (Empty ≠ Zero).

    Pure and never raises — both consuming lanes promise they never fault the
    teardown verification path.
    """
    details = getattr(position, "details", None)
    if not isinstance(details, dict):
        details = {}
    raw: Any = None
    for key in NFT_ID_DETAIL_KEYS:
        candidate = details.get(key)
        if candidate is not None and candidate != "":
            raw = candidate
            break
    if raw is None:
        raw = str(getattr(position, "position_id", "") or "")
    # ``bool`` is checked explicitly because it is a subclass of ``int``.
    if isinstance(raw, bool | float) or not isinstance(raw, int | str):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


_REGISTRY: dict[str, TeardownPostCondition] = {}


def verify_npm_position_closure(
    *,
    protocol: str,
    position_id: str,
    chain: str,
    token_id: int,
    npm_address: str,
    gateway_client: Any,
    block: int | str | None = None,
    label: str = "Uniswap V3",
) -> ClosureCheckResult:
    """Read one NFT position on ``npm_address`` and classify its closure (three-valued).

    Shared tail of every NonfungiblePositionManager-shaped post-condition (the
    V3 family and Aerodrome Slipstream): the caller has already resolved WHICH
    manager owns the NFT; this reads ``liquidity`` and ``tokensOwed`` through the
    gateway's typed RPCs, pinned to ``block``, and applies the closure rule.

    - ``liquidity == 0`` and both ``tokensOwed == 0`` → ``closed=True`` (covers
      the burnt-NFT path, whose ``positions(tokenId)`` revert the gateway folds
      into value-0 responses, and the decrease-without-burn path).
    - Any measured non-zero residual → ``closed=False`` with a residual map.
    - A read fault (``None`` or a raise) → ``unmeasured=True`` (Empty ≠ Zero):
      lowered to UNVERIFIED by the verifier, never a fabricated residual.
    """
    try:
        liquidity = gateway_client.query_position_liquidity(
            chain=chain,
            position_manager=npm_address,
            token_id=token_id,
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=f"{label} query_position_liquidity raised: {exc}",
        )
    if liquidity is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"{label} query_position_liquidity returned None "
                "(gateway/RPC error); cannot confirm closure — fail-closed"
            ),
        )
    try:
        tokens_owed = gateway_client.query_position_tokens_owed(
            chain=chain,
            position_manager=npm_address,
            token_id=token_id,
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=f"{label} query_position_tokens_owed raised: {exc}",
        )
    if tokens_owed is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"{label} query_position_tokens_owed returned None "
                "(gateway/RPC error); cannot confirm closure — fail-closed"
            ),
        )
    tokens_owed0, tokens_owed1 = tokens_owed
    if liquidity == 0 and tokens_owed0 == 0 and tokens_owed1 == 0:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)
    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={
            "position_manager": npm_address,
            "token_id": token_id,
            "liquidity": int(liquidity),
            "tokens_owed0": int(tokens_owed0),
            "tokens_owed1": int(tokens_owed1),
        },
    )


def _register_teardown_post_condition(protocol: str, hook: TeardownPostCondition) -> None:
    """Register a post-condition for a protocol (framework-internal).

    Not a connector-facing API: connectors publish post-conditions through
    ``CONNECTOR.teardown_post_condition`` (an ``ImportRef`` on the manifest);
    the framework hydrates them into this registry at import time
    (``almanak.framework.teardown.post_conditions``).

    Re-registering the same hook is idempotent. Replacing an existing hook logs
    a warning so accidental shadowing is visible in logs.
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


__all__ = [
    "NFT_ID_DETAIL_KEYS",
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
    "resolve_nft_token_id",
    "verify_npm_position_closure",
]
