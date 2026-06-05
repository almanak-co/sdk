"""Synthetic intent factory for permission discovery.

Creates minimal valid intents for each (protocol, intent_type) pair.
These intents are compiled by the real IntentCompiler to discover which
contracts and function selectors each protocol uses -- without making
any RPC calls.
"""

from __future__ import annotations

import functools
import logging
from collections import defaultdict
from collections.abc import Callable
from decimal import Decimal
from typing import Literal, cast

from ..intents.compiler import (
    _CHAIN_NATIVE_SYMBOLS,
    CHAIN_TOKENS,
    DEFAULT_SWAP_FEE_TIER,
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    PROTOCOL_ROUTERS,
    SWAP_FEE_TIERS,
)
from ..intents.vocabulary import (
    AnyIntent,
    BorrowIntent,
    CollectFeesIntent,
    FlashLoanIntent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    PerpCloseIntent,
    PerpOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)
from .constants import VAULT_PROTOCOL_REPRESENTATIVE
from .hints import (
    _PROTOCOL_CONNECTOR_MAP,
    DiscoveryContext,
    PermissionHints,
    get_discovery_vectors_override,
    get_permission_hints,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synthetic-discovery membership — DERIVED from connector declarations (VIB-4928)
#
# Each connector slug declares the intent types it participates in for synthetic
# permission discovery on its ``PermissionHints.synthetic_discovery_intents``
# (and ``supports_native_in_swap`` for the native-in SWAP subset). The flash-loan
# providers declare opt-in via the flash-loan registry's per-provider
# ``synthetic_discovery`` flag. The six membership sets below are folded from
# those declarations instead of being hand-maintained here — one source of
# truth, per-slug, so a shared compiler (e.g. ``AerodromeCompiler`` backing both
# ``aerodrome`` and ``aerodrome_slipstream``) can express divergent participation.
#
# The exact pre-fold memberships are pinned verbatim by
# ``tests/unit/permissions/test_synthetic_membership_equivalence.py`` — any
# change to a connector declaration that shifts a derived set is caught there.
# ---------------------------------------------------------------------------

# Intent-type string groupings used to bucket each slug's declared participation
# into the legacy membership sets. SWAP is its own bucket; LP / lending / perp
# each fold their constituent intent-type strings. ``LP_COLLECT_FEES`` is
# deliberately absent — it stays gated by ``supports_standalone_fee_collection``
# (see ``get_protocol_intent_matrix`` / ``_build_lp_collect_fees_intents``).
_LP_INTENT_TYPES = frozenset({"LP_OPEN", "LP_CLOSE"})
_LENDING_INTENT_TYPES = frozenset({"SUPPLY", "WITHDRAW", "BORROW", "REPAY"})
_PERP_INTENT_TYPES = frozenset({"PERP_OPEN", "PERP_CLOSE"})
# Every intent-type string a connector may legally declare in
# ``synthetic_discovery_intents`` — derived from the per-category sets above so it
# cannot drift. A value outside this set is a typo (e.g. ``"L_OPEN"``) that would
# otherwise be silently ignored, dropping the connector from a membership set;
# ``_derive_membership_sets`` raises on it instead (VIB-4928).
_VALID_SYNTHETIC_INTENTS: frozenset[str] = (
    frozenset({"SWAP"}) | _LP_INTENT_TYPES | _LENDING_INTENT_TYPES | _PERP_INTENT_TYPES
)


def _all_connector_slugs() -> frozenset[str]:
    """Enumerate every connector slug whose ``PermissionHints`` can opt into
    synthetic discovery.

    The universe is the compiler-registry loader keys (every protocol slug with
    a connector-owned compiler) UNION the ``_PROTOCOL_CONNECTOR_MAP`` aliases.
    The alias map matters because a single connector directory can expose
    several protocol surfaces through distinct ``PermissionHints`` exports —
    e.g. ``aerodrome_slipstream`` resolves to ``aerodrome``'s
    ``PERMISSION_HINTS_SLIPSTREAM`` (it is also a loader key, so the union is
    defensive) and ``metamorpho`` resolves to ``morpho_vault``. A slug that
    declares no ``synthetic_discovery_intents`` simply contributes to none of
    the derived sets, so over-enumerating is harmless.
    """
    from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

    return frozenset(CompilerRegistry.supported_protocols()) | frozenset(_PROTOCOL_CONNECTOR_MAP)


def _derive_membership_sets() -> tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str], frozenset[str]]:
    """Fold the per-slug ``PermissionHints`` declarations into the five
    connector-membership frozensets (SWAP / native-in SWAP / LP / lending / perp).

    Returns them in a fixed order so the module-level unpack stays readable.
    """
    swap: set[str] = set()
    native_in_swap: set[str] = set()
    lp: set[str] = set()
    lending: set[str] = set()
    perp: set[str] = set()
    for slug in _all_connector_slugs():
        hints = get_permission_hints(slug)
        declared = hints.synthetic_discovery_intents
        if not declared:
            continue
        unknown = declared - _VALID_SYNTHETIC_INTENTS
        if unknown:
            raise ValueError(
                f"Connector {slug!r} declared unknown synthetic_discovery_intents "
                f"{sorted(unknown)}; valid intent types are "
                f"{sorted(_VALID_SYNTHETIC_INTENTS)}"
            )
        if "SWAP" in declared:
            swap.add(slug)
            if hints.supports_native_in_swap:
                native_in_swap.add(slug)
        if declared & _LP_INTENT_TYPES:
            lp.add(slug)
        if declared & _LENDING_INTENT_TYPES:
            lending.add(slug)
        if declared & _PERP_INTENT_TYPES:
            perp.add(slug)
    return frozenset(swap), frozenset(native_in_swap), frozenset(lp), frozenset(lending), frozenset(perp)


def _derive_flash_loan_providers() -> frozenset[str]:
    """Fold the flash-loan registry's per-provider ``synthetic_discovery`` flag
    into the ``_FLASH_LOAN_PROVIDERS`` membership set.

    Importing the registration boot module ensures the in-process registry is
    populated before we read it (it registers concrete connectors at import).
    """
    # Boot the registry (registers aave / balancer / morpho on import).
    import almanak.connectors._strategy_flash_loan_registry  # noqa: F401
    from almanak.connectors._strategy_base.flash_loan_registry import FLASH_LOAN_PROVIDER_REGISTRY

    return frozenset(FLASH_LOAN_PROVIDER_REGISTRY.synthetic_discovery_names())


@functools.cache
def _membership_sets() -> tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str], frozenset[str]]:
    """Lazily derive + cache the five connector-membership frozensets.

    Deferred out of module import (it was eager) because the derivation calls
    ``get_permission_hints`` for every connector; at *import* time a connector's
    ``permission_hints`` module can still be mid-import (circular), and
    ``get_permission_hints`` then silently falls back to default (empty) hints,
    dropping that connector from the derived sets. Under xdist worker import
    ordering that surfaced as ``morpho_blue`` missing from
    ``_LENDING_PROTOCOLS`` (VIB-4928). Computing on first *use* runs after all
    connector imports have settled, making the membership deterministic.
    """
    return _derive_membership_sets()


@functools.cache
def _flash_loan_providers() -> frozenset[str]:
    """Lazily derive + cache ``_FLASH_LOAN_PROVIDERS`` (see :func:`_membership_sets`)."""
    return _derive_flash_loan_providers()


def _swap_protocols() -> frozenset[str]:
    return _membership_sets()[0]


def _native_in_swap_protocols() -> frozenset[str]:
    return _membership_sets()[1]


def _lp_protocols() -> frozenset[str]:
    return _membership_sets()[2]


def _lending_protocols() -> frozenset[str]:
    return _membership_sets()[3]


def _perp_protocols() -> frozenset[str]:
    return _membership_sets()[4]


# Backwards-compatible module-level names (PEP 562). External consumers and tests
# read ``synthetic_intents._SWAP_PROTOCOLS`` etc.; resolving them via __getattr__
# keeps the derivation lazy so import-time access never triggers it (which is what
# poisoned the eager version under circular imports, VIB-4928).
_LAZY_MEMBERSHIP_ACCESSORS: dict[str, Callable[[], frozenset[str]]] = {
    "_SWAP_PROTOCOLS": _swap_protocols,
    "_NATIVE_IN_SWAP_PROTOCOLS": _native_in_swap_protocols,
    "_LP_PROTOCOLS": _lp_protocols,
    "_LENDING_PROTOCOLS": _lending_protocols,
    "_PERP_PROTOCOLS": _perp_protocols,
    "_FLASH_LOAN_PROVIDERS": _flash_loan_providers,
}


def __getattr__(name: str) -> frozenset[str]:
    accessor = _LAZY_MEMBERSHIP_ACCESSORS.get(name)
    if accessor is not None:
        return accessor()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_protocol_intent_matrix() -> dict[str, frozenset[IntentType]]:
    """Return ``{protocol: frozenset[IntentType]}`` for every pair the manifest
    generator covers via synthetic intent compilation.

    Authoritative source for any caller that needs to enumerate the full set of
    ``(protocol, intent_type)`` pairs whose generated permissions must be
    exercised on-chain. Single source of truth — keep this function and the
    ``_build_*_intents`` dispatch in ``build_synthetic_intents`` in lockstep.

    Excludes pairs that bypass the synthetic-discovery path:
    - ``BRIDGE`` (bridge connectors are not yet wired through the generator)
    - ``WRAP_NATIVE`` / ``UNWRAP_NATIVE`` (infra, not protocol-specific)
    - ``FLASH_LOAN`` (provider strings ``aave`` / ``balancer`` are not
      connector-directory names, so they need separate handling)
    """
    matrix: dict[str, set[IntentType]] = defaultdict(set)
    for proto in _swap_protocols():
        matrix[proto].add(IntentType.SWAP)
    for proto in _lp_protocols():
        matrix[proto].update({IntentType.LP_OPEN, IntentType.LP_CLOSE})
        if get_permission_hints(proto).supports_standalone_fee_collection:
            matrix[proto].add(IntentType.LP_COLLECT_FEES)
    for proto in _lending_protocols():
        matrix[proto].update(
            {
                IntentType.SUPPLY,
                IntentType.WITHDRAW,
                IntentType.BORROW,
                IntentType.REPAY,
            }
        )
    for proto in _perp_protocols():
        matrix[proto].update({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    for proto, chains in VAULT_PROTOCOL_REPRESENTATIVE.items():
        if chains:
            matrix[proto].update({IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM})
    return {proto: frozenset(types) for proto, types in matrix.items()}


def _get_token_pair(chain: str) -> tuple[str, str]:
    """Get a known token pair for a chain (usdc, weth-equivalent).

    Returns addresses from CHAIN_TOKENS in the compiler, falling back
    to well-known USDC/WETH addresses for arbitrum.
    """
    tokens = CHAIN_TOKENS.get(chain, {})
    usdc = tokens.get("usdc", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
    weth = tokens.get("weth") or tokens.get("wavax") or tokens.get("wbnb", "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
    return usdc, weth


def _resolve_lp_pair(hints: PermissionHints, chain: str) -> tuple[str, str]:
    """Return the (tokenA, tokenB) pair to seed synthetic LP discovery on
    ``chain`` given the protocol's already-resolved ``hints``.

    Per-protocol override via ``PermissionHints.synthetic_lp_pair`` takes
    precedence over the chain-default pair from ``_get_token_pair``.

    Required for chains where the framework's chain-default pair (e.g. bsc's
    ``(USDC, ETH-bridged)``) does not match the canonical liquid LP pair
    actually used by the protocol on that chain (e.g. sushiswap_v3 on bsc
    uses ``(USDT, WBNB)``). Without an override, the synthetic discovery
    emits approves on the wrong tokens and any test that LP-opens on the
    real pair fails Zodiac authorisation. Surfaced by #1902.

    Takes ``hints`` directly (instead of looking it up by protocol) because
    every caller already resolves ``PermissionHints`` for its own purposes
    in the same function — passing the object in avoids a redundant
    ``importlib`` lookup per LP-helper invocation.
    """
    override = hints.synthetic_lp_pair.get(chain)
    if override:
        return override
    return _get_token_pair(chain)


def build_synthetic_intents(
    protocol: str,
    intent_type: str,
    chain: str,
) -> list[AnyIntent]:
    """Build synthetic intents for a (protocol, intent_type) combination.

    Returns a list because some combinations need multiple intents to
    cover all code paths (e.g., LP_CLOSE produces decrease + collect + burn).

    Args:
        protocol: Protocol name (e.g., "uniswap_v3", "aave_v3")
        intent_type: Intent type string (e.g., "SWAP", "SUPPLY")
        chain: Target chain name

    Returns:
        List of synthetic intents. Empty if the combination is not supported.
    """
    try:
        it = IntentType(intent_type)
    except ValueError:
        logger.debug("Unknown intent type: %s", intent_type)
        return []

    protocol_lower = protocol.lower()
    usdc, weth = _get_token_pair(chain)

    if it == IntentType.SWAP:
        return _build_swap_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.LP_OPEN:
        return _build_lp_open_intents(protocol_lower, chain)
    elif it == IntentType.LP_CLOSE:
        return _build_lp_close_intents(protocol_lower, chain)
    elif it == IntentType.LP_COLLECT_FEES:
        return _build_lp_collect_fees_intents(protocol_lower, chain)
    elif it == IntentType.SUPPLY:
        return _build_supply_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.WITHDRAW:
        return _build_withdraw_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.BORROW:
        return _build_borrow_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.REPAY:
        return _build_repay_intents(protocol_lower, chain, usdc, weth)
    elif it == IntentType.PERP_OPEN:
        return _build_perp_open_intents(protocol_lower, chain, usdc)
    elif it == IntentType.PERP_CLOSE:
        return _build_perp_close_intents(protocol_lower, chain, usdc)
    elif it == IntentType.FLASH_LOAN:
        return _build_flash_loan_intents(protocol_lower, chain, usdc)
    elif it == IntentType.VAULT_DEPOSIT:
        return _build_vault_deposit_intents(protocol_lower, chain)
    elif it == IntentType.VAULT_REDEEM:
        return _build_vault_redeem_intents(protocol_lower, chain)
    else:
        logger.debug("Intent type %s not supported for permission discovery", intent_type)
        return []


def _build_swap_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _swap_protocols():
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "SWAP", chain, ctx)
        if result is not None:
            return result
    # Check that this protocol has a router on this chain.
    # Protocols with dedicated swap compile paths that still rely on framework
    # defaults (currently Enso) are exempt because their router address is not
    # stored in PROTOCOL_ROUTERS. Connectors that own synthetic discovery should
    # return from ``build_discovery_vectors`` above before this gate.
    #
    # ``traderjoe_v2`` USED to be in this tuple for the same reason — its
    # LBRouter lives in ``LP_POSITION_MANAGERS`` rather than
    # ``PROTOCOL_ROUTERS`` — but its synthetic SWAP dispatch is now fully
    # owned by ``connectors/traderjoe_v2/permission_hints.build_discovery_vectors``
    # (VIB-4121 connector self-containment). The override returns the
    # LBRouter swap synthetic above this gate ever fires; if the override
    # ever returns ``None`` for SWAP on a supported chain, that's a connector
    # bug — not a reason to keep the exemption.
    if protocol != "enso":
        routers = PROTOCOL_ROUTERS.get(chain, {})
        if protocol not in routers:
            return []
    # Some protocols need connector-declared token pairs.
    # Use hints override when available.
    hints = get_permission_hints(protocol)
    from_token, to_token = usdc, weth
    if hints.synthetic_swap_pair:
        pair = hints.synthetic_swap_pair.get(chain)
        if pair:
            from_token, to_token = pair
    intents: list[AnyIntent] = [
        SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount=Decimal("1"),
            protocol=protocol,
            chain=chain,
        )
    ]
    # For V3-style routers that support native-in via msg.value (auto-WETH9
    # wrap on the SwapRouter02 ABI), emit a second native→USDC synthetic so
    # ``tx.value > 0`` flips ``send_allowed=True`` on the router target.
    # Without this, the manifest authorises the selector but rejects every
    # value-bearing call at execTransactionWithRole.
    native_symbols = _CHAIN_NATIVE_SYMBOLS.get(chain, frozenset())
    if protocol in _native_in_swap_protocols() and native_symbols:
        # ``frozenset`` iteration order is process-stable but
        # implementation-defined. On chains with multiple aliases (polygon
        # exposes ``{"MATIC", "POL"}``), ``next(iter(...))`` could pick
        # either, making the synthetic's ``from_token`` flap across Python
        # versions / interpreter builds and producing a non-deterministic
        # discovery output. ``sorted(...)[0]`` pins the alias
        # lexicographically — for polygon that's ``"MATIC"`` (the
        # historically tested symbol). The compiler accepts both via
        # ``_CHAIN_NATIVE_SYMBOLS`` so functional behaviour is unchanged.
        native_symbol = sorted(native_symbols)[0]
        intents.append(
            SwapIntent(
                from_token=native_symbol,
                to_token=usdc,
                amount=Decimal("0.01"),
                protocol=protocol,
                chain=chain,
            )
        )
    return intents


def _build_lp_open_intents(protocol: str, chain: str) -> list[AnyIntent]:
    if protocol not in _lp_protocols():
        return []
    # Override hook runs BEFORE the LP_POSITION_MANAGERS gate so a connector
    # that owns its discovery via ``build_discovery_vectors`` is never blocked
    # by the legacy hardcoded registry — same ordering as the lending builders
    # (see commit 30b0b0e80 and ``TestOverrideBypassesLendingPoolGate``).
    # Pendle is the canonical case: it uses single-sided LP into a market
    # contract (no NFT position manager), so it's deliberately absent from
    # LP_POSITION_MANAGERS and relies on the override hook firing first.
    # ``ctx.usdc`` / ``ctx.weth`` carry the chain-default token pair so the
    # callee doesn't have to re-import ``_get_token_pair``.
    usdc, weth = _get_token_pair(chain)
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "LP_OPEN", chain, ctx)
        if result is not None:
            return result
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    hints = get_permission_hints(protocol)
    # Resolve the LP pair via the per-protocol override registry — the chain
    # default (USDC, WETH-equivalent) is wrong on chains where the framework's
    # ``_get_token_pair`` resolves to a non-liquid pair for the protocol on
    # that chain (e.g. sushiswap_v3 on bsc → (USDT, WBNB), not (USDC, ETH-bsc)).
    token0, token1 = _resolve_lp_pair(hints, chain)
    # Include fee tier in pool string for protocols that use Uniswap V3-style
    # fee tiers (parsed by _parse_pool_info which defaults to 3000).
    # Protocols with their own pool format (TraderJoe V2 bins, Aerodrome
    # volatile/stable) omit the fee tier so their parsers use the correct default.
    if protocol in SWAP_FEE_TIERS or hints.synthetic_fee_tier.get(chain):
        fee_tier = hints.synthetic_fee_tier.get(chain) or DEFAULT_SWAP_FEE_TIER.get(protocol, 3000)
        pool = f"{token0}/{token1}/{fee_tier}"
    else:
        pool = f"{token0}/{token1}"
    return [
        LPOpenIntent(
            pool=pool,
            amount0=Decimal("100"),
            amount1=Decimal("0.05"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("4000"),
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_lp_close_intents(protocol: str, chain: str) -> list[AnyIntent]:
    if protocol not in _lp_protocols():
        return []
    # Override hook runs BEFORE the LP_POSITION_MANAGERS gate — see
    # ``_build_lp_open_intents`` for the canonical placement rationale.
    usdc, weth = _get_token_pair(chain)
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "LP_CLOSE", chain, ctx)
        if result is not None:
            return result
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    hints = get_permission_hints(protocol)
    token0, token1 = _resolve_lp_pair(hints, chain)
    position_id = hints.synthetic_position_id.format(token0=token0, token1=token1)
    return [
        LPCloseIntent(
            position_id=position_id,
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_lp_collect_fees_intents(protocol: str, chain: str) -> list[AnyIntent]:
    hints = get_permission_hints(protocol)
    if not hints.supports_standalone_fee_collection:
        return []
    # Override hook runs BEFORE the position-manager gate so a connector that
    # owns its discovery via ``build_discovery_vectors`` is never blocked by
    # the framework's chain → position-manager registry. Mirrors
    # ``_build_swap_intents`` / ``_build_supply_intents``.
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        usdc, weth = _get_token_pair(chain)
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "LP_COLLECT_FEES", chain, ctx)
        if result is not None:
            return result
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if protocol not in managers:
        return []
    token0, token1 = _resolve_lp_pair(hints, chain)
    return [
        CollectFeesIntent(
            pool=f"{token0}/{token1}",
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_supply_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _lending_protocols():
        return []
    # Override hook runs BEFORE the chain/pool gate below so a connector that
    # owns its discovery via ``build_discovery_vectors`` is never blocked by
    # the legacy hardcoded singleton exception list. Mirrors
    # ``_build_swap_intents``.
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "SUPPLY", chain, ctx)
        if result is not None:
            return result
    # Check lending pool exists for this chain
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        SupplyIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("100"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_withdraw_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _lending_protocols():
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "WITHDRAW", chain, ctx)
        if result is not None:
            return result
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        WithdrawIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("50"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_borrow_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _lending_protocols():
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "BORROW", chain, ctx)
        if result is not None:
            return result
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        BorrowIntent(
            protocol=protocol,
            collateral_token=weth,
            collateral_amount=Decimal("1"),
            borrow_token=usdc,
            borrow_amount=Decimal("100"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_repay_intents(protocol: str, chain: str, usdc: str, weth: str) -> list[AnyIntent]:
    if protocol not in _lending_protocols():
        return []
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "REPAY", chain, ctx)
        if result is not None:
            return result
    pools = LENDING_POOL_ADDRESSES.get(chain, {})
    if protocol not in pools and protocol not in ("morpho_blue", "compound_v3"):
        return []
    hints = get_permission_hints(protocol)

    return [
        RepayIntent(
            protocol=protocol,
            token=usdc,
            amount=Decimal("50"),
            chain=chain,
            market_id=hints.synthetic_market_id,
        )
    ]


def _build_perp_open_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _perp_protocols():
        return []
    # Override hook runs BEFORE the protocol/chain dispatch so a connector that
    # owns its discovery via ``build_discovery_vectors`` is never blocked by
    # the legacy hardcoded chain branches. Mirrors ``_build_swap_intents`` /
    # ``_build_supply_intents``.
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        _, weth = _get_token_pair(chain)
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "PERP_OPEN", chain, ctx)
        if result is not None:
            return result
    return [
        PerpOpenIntent(
            market="ETH/USD",
            collateral_token=usdc,
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_perp_close_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _perp_protocols():
        return []
    # Override hook runs BEFORE the protocol/chain dispatch so a connector that
    # owns its discovery via ``build_discovery_vectors`` is never blocked by
    # the legacy hardcoded chain branches. Mirrors ``_build_swap_intents`` /
    # ``_build_supply_intents``.
    override = get_discovery_vectors_override(protocol)
    if override is not None:
        _, weth = _get_token_pair(chain)
        ctx = DiscoveryContext(usdc=usdc, weth=weth)
        result = override(protocol, "PERP_CLOSE", chain, ctx)
        if result is not None:
            return result
    return [
        PerpCloseIntent(
            market="ETH/USD",
            collateral_token=usdc,
            is_long=True,
            size_usd=Decimal("500"),
            protocol=protocol,
            chain=chain,
        )
    ]


def _build_flash_loan_intents(protocol: str, chain: str, usdc: str) -> list[AnyIntent]:
    if protocol not in _flash_loan_providers():
        return []
    _, weth = _get_token_pair(chain)
    # Flash loans require at least one callback intent
    callback = SwapIntent(
        from_token=usdc,
        to_token=weth,
        amount=Decimal("1"),
        protocol="uniswap_v3",
        chain=chain,
    )
    return [
        FlashLoanIntent(
            provider=cast(Literal["aave", "balancer", "morpho", "auto"], protocol),
            token=usdc,
            amount=Decimal("10000"),
            callback_intents=[callback],
            chain=chain,
        )
    ]


def _build_vault_deposit_intents(protocol: str, chain: str) -> list[AnyIntent]:
    vault_chains = VAULT_PROTOCOL_REPRESENTATIVE.get(protocol.lower())
    if not vault_chains:
        return []
    vault_info = vault_chains.get(chain)
    if not vault_info:
        return []
    return [
        VaultDepositIntent(
            protocol=protocol,
            vault_address=vault_info["vault"],
            amount=Decimal("100"),
            chain=chain,
        )
    ]


def _build_vault_redeem_intents(protocol: str, chain: str) -> list[AnyIntent]:
    vault_chains = VAULT_PROTOCOL_REPRESENTATIVE.get(protocol.lower())
    if not vault_chains:
        return []
    vault_info = vault_chains.get(chain)
    if not vault_info:
        return []
    return [
        VaultRedeemIntent(
            protocol=protocol,
            vault_address=vault_info["vault"],
            shares=Decimal("100"),
            chain=chain,
        )
    ]
