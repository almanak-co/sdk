"""Teardown post-conditions: protocol-specific on-chain closure verification.

VIB-3742 - Framework hardening for graceful teardown.

Background
----------
``TeardownManager._verify_closure`` historically only re-read
``strategy.get_open_positions()`` - an in-memory call that returns 0
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

The registry is manifest-backed: connectors publish post-conditions through
``CONNECTOR.teardown_post_condition``. This module loads those manifest refs
at import time and keeps the framework-facing lookup helpers stable.

Hard constraints
----------------
- All on-chain reads MUST go through the gateway in production. Connector
  post-conditions receive the teardown gateway client when one is available.
  Tests and local Anvil flows may pass ``rpc_url`` only when the connector
  hook explicitly supports the same dual path as its compiler.
- Failures in a hook return ``ClosureCheckResult(closed=False)`` with an
  error message rather than raising. Verification is informational and a
  hook crash must not silently pass the teardown.
- No emojis. No Postgres DDL.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError
from almanak.connectors._strategy_base.address_registry import AbiFamily, AddressRegistry
from almanak.connectors._strategy_base.teardown_post_condition import (
    ClosureCheckResult,
    TeardownPostCondition,
    _register_teardown_post_condition,
    get_teardown_post_condition,
    has_teardown_post_condition,
)
from almanak.connectors._strategy_base.vault_post_condition import (
    erc4626_vault_teardown_post_condition,
)

logger = logging.getLogger(__name__)


def _connector_teardown_slugs(connector: Any) -> frozenset[str]:
    """Every protocol slug a position from ``connector`` could carry, lowercased.

    VIB-5573: the registry is keyed by ``position.protocol`` (see
    ``teardown_manager._verify_closure_detailed``), but a position's protocol
    string is NOT always the connector's folder ``name`` — e.g. a ``morpho_vault``
    connector produces ``metamorpho`` positions. Registering a hook under the bare
    ``name`` therefore silently fails to resolve for such connectors (gmx/pendle
    only worked because their primitive string equals their name). Register under
    the connector's full identity set — ``discovery_keys`` (name + aliases +
    receipt-parser protocols) ∪ ``compiler_protocols`` — so lookup by any protocol
    string the connector emits resolves to the hook.

    ``connector.name`` is already inside ``discovery_keys`` (``Connector.protocol_keys``
    = ``{name, *aliases}``); it is added explicitly below too, defensively, so the
    connector name is guaranteed present regardless of how ``discovery_keys`` is
    computed (VIB-5573, Gemini). ``discovery_keys`` is a ``frozenset`` and
    ``compiler_protocols`` a tuple, so char-iteration of a stray string is not a
    risk here.
    """
    slugs: set[str] = set(getattr(connector, "discovery_keys", None) or ())
    slugs.update(getattr(connector, "compiler_protocols", None) or ())
    name = getattr(connector, "name", None)
    if name:
        slugs.add(name)
    return frozenset(s.lower() for s in slugs if s)


def _register_manifest_teardown_post_conditions() -> None:
    """Register connector-owned teardown post-conditions from manifests.

    Registered under every slug the connector can emit (``_connector_teardown_slugs``),
    not just ``connector.name`` — see that helper for the register-by-name /
    lookup-by-protocol mismatch this closes (VIB-5573).
    """
    for connector_manifest in CONNECTOR_REGISTRY.with_teardown_post_condition():
        if connector_manifest.teardown_post_condition is None:
            continue
        hook = connector_manifest.teardown_post_condition.load()
        if not callable(hook):
            raise ConnectorDiscoveryError(
                f"{connector_manifest.teardown_post_condition.module}."
                f"{connector_manifest.teardown_post_condition.attribute} must be callable, "
                f"got {type(hook).__qualname__}"
            )
        for slug in _connector_teardown_slugs(connector_manifest):
            _register_teardown_post_condition(slug, hook)


_register_manifest_teardown_post_conditions()


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

# V3-fork protocols that expose the canonical NonfungiblePositionManager ABI
# (``balanceOf`` / ``tokenOfOwnerByIndex`` / ``positions(tokenId)``) this hook
# verifies against. The membership is connector knowledge, so it lives on the
# strategy-side ``AddressRegistry`` under :attr:`AbiFamily.V3_NPM` — this module
# never names a protocol itself. Discovery (``discovery._NPM_PROTOCOLS``) derives
# from the same capability, so the two lanes cannot drift. Each member's per-chain
# NonfungiblePositionManager address is resolved through the registry
# (W1 / VIB-4853); the address tables live on the connectors. A slug only lands
# here if its connector ships an NPM address, so registering it cannot make a
# teardown fail-closed with "no NPM registered".
#
# PancakeSwap V3 records its NPM under the ``nft`` key (its receipt parser and
# intent compiler standardise on ``nft``); the others use ``position_manager``.
# ``_NPM_ADDRESS_KEYS`` lists both so this stays a single per-fork NPM source —
# the connector's ``addresses.py`` — without a key rename that would ripple
# through every Pancake reader (VIB-4902).
_V3_NPM_PROTOCOLS: frozenset[str] = frozenset(AddressRegistry.protocols_with_abi(AbiFamily.V3_NPM))

# Connectors record the NPM under ``position_manager`` (uniswap / agni / sushi)
# or ``nft`` (pancakeswap). Try both so a single per-fork ``addresses.py`` entry
# satisfies every reader (VIB-4902).
_NPM_ADDRESS_KEYS = ("position_manager", "nft")


def _resolve_v3_position_manager(protocol: str, chain: str) -> str | None:
    """Look up the NonfungiblePositionManager address for a V3-fork protocol.

    Returns ``None`` when the protocol is not registered or the chain has
    no deployment. Callers fail-closed on ``None``.
    """
    if protocol.lower() not in _V3_NPM_PROTOCOLS:
        return None
    return AddressRegistry.resolve_contract_address(protocol, chain, _NPM_ADDRESS_KEYS)


def _uniswap_v3_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a Uniswap V3 (or V3-fork) LP NFT is closed on-chain.

    Reads on-chain truth via the gateway's ``QueryPositionLiquidity`` and
    ``QueryPositionTokensOwed`` RPCs, both pinned to ``block`` (VIB-5140 —
    the close-tx receipt's ``block_number``). Pinning is the fix for the
    false-negative teardown verify: when the RPC pool routes to a read
    replica one block behind the writer, an unpinned ``"latest"`` read
    returns PRE-close liquidity / tokensOwed, the verifier concludes the
    position is still open, and the strategy transitions to STRATEGY_ERROR
    even though the close succeeded (user double-close / janitor hot-loop).
    ``block=None`` falls back to ``"latest"`` (legacy behaviour). Closure
    rules:

    - ``liquidity == 0`` AND ``tokensOwed0 == 0`` AND ``tokensOwed1 == 0``
      → ``closed=True``. This covers BOTH (a) the burnt-NFT path
      (``positions(tokenId)`` reverts with "Invalid token ID"; the gateway
      folds that revert into ``liquidity = 0`` and ``tokensOwed = (0, 0)``)
      AND (b) the decrease-without-burn path.
    - Any non-zero residual (a MEASURED liquidity / tokensOwed) →
      ``closed=False`` with a residual map → the seam fails the teardown.
    - A read fault (missing chain / gateway_client / NPM address /
      unresolvable NFT id, or either RPC returning ``None`` / raising) →
      ``unmeasured=True`` (VIB-5573) → the seam lowers to ``UNVERIFIED``,
      NEVER ``FAILED``. Empty ≠ Zero: an unknown on-chain state must not be
      reported as closed, but must also not be fabricated into a residual
      that false-fails the teardown on a transient gateway/RPC blip.

    No direct network egress: the post-condition uses the supplied
    ``gateway_client``. ``rpc_url`` is intentionally NOT consumed here —
    framework code MUST go through the gateway boundary; tests that need
    to drive the closure paths inject a fake gateway_client.

    Note on the registered slug set: this hook is registered for the
    slugs in ``_V3_NPM_PROTOCOLS`` (``uniswap_v3``, ``agni_finance``,
    ``pancakeswap_v3``, ``sushiswap_v3``) — every V3-fork that exposes the
    canonical NPM ABI AND whose connector ``addresses.py`` publishes a
    NonfungiblePositionManager address (under ``position_manager`` or, for
    PancakeSwap V3, ``nft``), resolved through the strategy-side
    ``AddressRegistry``. The slug set is the registry's
    :attr:`AbiFamily.V3_NPM` membership, so adding a new V3 fork only
    requires publishing its NPM address on the connector's ``addresses.py``
    and listing the slug under ``AbiFamily.V3_NPM`` — no edit here.
    Aerodrome's volatile/stable pools use ERC-20 LP tokens, not NFTs, so
    they do NOT register here; their teardown closure check is a different
    primitive and falls through to the legacy in-memory check until a
    dedicated post-condition is added.
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
            unmeasured=True,
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
            unmeasured=True,
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
            unmeasured=True,
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
            unmeasured=True,
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
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=f"Uniswap V3 query_position_liquidity raised: {exc}",
        )

    if liquidity is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
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
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
            protocol=protocol,
            position_id=position_id,
            error=f"Uniswap V3 query_position_tokens_owed raised: {exc}",
        )

    if tokens_owed is None:
        return ClosureCheckResult(
            closed=False,
            unmeasured=True,
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


# =============================================================================
# Cut-over LP primitive-label aliases (registry key-format bridge)
# =============================================================================
#
# The teardown WARM enumeration (``registry_enumeration._position_info_from_
# registry_row``) labels a cut-over LP position with the registry PRIMITIVE value
# (``lp`` for the V3 family, ``lp_v4`` for Uniswap V4), NOT the connector slug the
# strategy's own enumeration carries — the registry row holds no connector slug.
# So a restart-derived position (WARM registry read, strategy in-memory state
# wiped) reaches ``_verify_closure_detailed`` with ``protocol='lp_v4'`` and a
# hook registered only under ``uniswap_v4`` would NOT resolve — the closed V4
# position would be mis-classified (VIB-5634 strand 2, the "V4 pool_id vs V3
# pool_address" registry key-format mismatch).
#
# For Uniswap V4 the connector-owned post-condition resolves its addresses by
# CHAIN (not by protocol), so aliasing it under the ``lp_v4`` primitive value is
# safe and complete. The V3 family shares the ``lp`` primitive across many forks
# and its hook resolves the NPM by the specific fork protocol, so a bare ``lp``
# label is NOT resolvable to one NPM — V3 is deliberately NOT aliased here (its
# restart-safe verification is a separate concern, out of scope for VIB-5634).


def _register_lp_v4_primitive_alias() -> None:
    """Alias the connector-owned V4 post-condition under the LP_V4 primitive value.

    A V4 LP position enumerated from the WARM registry carries
    ``protocol=Primitive.LP_V4.value`` (``'lp_v4'``), not the connector slug. This
    registers the SAME connector-published hook (loaded by
    ``_register_manifest_teardown_post_conditions`` under ``uniswap_v4``) under the
    primitive value too, so a restart-derived closed V4 position resolves to the
    verifier instead of falling through to UNVERIFIED. Sourced canonically (the
    LP_V4 primitive + the connector's own manifest-registered hook) — no
    framework-side connector import or protocol-name literal. Idempotent and
    never clobbers an already-registered hook.
    """
    from almanak.framework.primitives.types import Primitive

    lp_v4 = Primitive.LP_V4.value
    if has_teardown_post_condition(lp_v4):
        return
    # The V4 connector is the one whose declared primitive is LP_V4; reuse the
    # hook it published (no re-import of the hook module here).
    for connector_manifest in CONNECTOR_REGISTRY.with_teardown_post_condition():
        primitive_ref = getattr(connector_manifest, "primitive", None)
        if primitive_ref is None or connector_manifest.teardown_post_condition is None:
            continue
        # Both loads run INSIDE the try: a broken primitive ref OR a broken
        # hook-module import in ONE connector must not crash framework
        # registration at startup (which would take down every strategy). Never
        # silent — log loudly with the connector name; skipping only means that
        # connector's LP_V4 alias is absent, so a restart-derived V4 position
        # falls back to UNVERIFIED (fail-safe), never a false closure.
        try:
            declared = primitive_ref.load()
            if getattr(declared, "primitive", None) is Primitive.LP_V4:
                _register_teardown_post_condition(lp_v4, connector_manifest.teardown_post_condition.load())
                return
        except Exception:  # noqa: BLE001 — one bad connector must not break registration
            logger.warning(
                "Failed to load LP_V4 primitive-alias hook for connector %r — skipping its "
                "lp_v4 teardown post-condition alias (a restart-derived V4 position will fall "
                "back to UNVERIFIED, fail-safe)",
                getattr(connector_manifest, "name", "<unknown>"),
                exc_info=True,
            )
            continue


def _register_default_v3_post_conditions() -> None:
    """Register the generic V3 NPM hook as the default for each V3-fork slug.

    Never clobbers a connector that already published its own
    ``teardown_post_condition`` via its manifest (loaded above by
    ``_register_manifest_teardown_post_conditions``): connector-owned hooks win,
    and this framework default is a fallback only. Without the guard a V3-fork
    connector that later owns its hook would have it silently overwritten here.
    """
    for v3_slug in sorted(_V3_NPM_PROTOCOLS):
        if not has_teardown_post_condition(v3_slug):
            _register_teardown_post_condition(v3_slug, _uniswap_v3_post_condition)


def _register_default_vault_post_conditions() -> None:
    """Register the generic ERC-4626 hook as the default for every VAULT connector.

    VIB-5573: before this, no ``ProtocolKind.VAULT`` connector (``beefy``,
    ``lagoon``, ``morpho_vault``, ``yearn``) had a post-condition, so a vault
    teardown was pinned at ``UNVERIFIED`` and a residual was invisible. This
    default gives the whole vault primitive an on-chain closure check via the
    ERC-4626 standard interface. A vault that is not ERC-4626 degrades to
    ``unmeasured`` (→ ``UNVERIFIED``), never a false result.

    Registered under every slug each vault connector can emit
    (``_connector_teardown_slugs``) and never clobbers a connector that owns its
    own hook via manifest (connector-owned wins — same fallback discipline as the
    V3 default).
    """
    for connector in CONNECTOR_REGISTRY.all():
        if connector.kind is not ProtocolKind.VAULT:
            continue
        for slug in _connector_teardown_slugs(connector):
            if not has_teardown_post_condition(slug):
                _register_teardown_post_condition(slug, erc4626_vault_teardown_post_condition)


_register_default_v3_post_conditions()
_register_lp_v4_primitive_alias()
_register_default_vault_post_conditions()


__all__ = [
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
]
