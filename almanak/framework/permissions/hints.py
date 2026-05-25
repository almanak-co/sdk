"""Protocol-specific permission hints.

Adapters export a PERMISSION_HINTS instance in a lightweight
``permission_hints.py`` file.  The permission system discovers it
via convention-based import - no central registry to maintain.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StaticPermissionEntry:
    """A pre-computed permission for protocols that can't use compilation-based discovery.

    Used when compilation requires external state (GatewayClient, RPC) that
    isn't available during offline permission discovery.

    Attributes:
        target: Lower-cased contract address the Roles modifier should authorise.
        label: Human-readable label surfaced in the generated manifest.
        selectors: ``selector -> human-readable label`` mapping for every
            function selector this entry authorises on ``target``.
        send_allowed: Whether the Safe is permitted to send native value to
            ``target`` for this entry's selectors.
        intent_types: Optional intent-type allow-list. ``None`` (default)
            means the entry applies to **every** manifest produced for the
            owning protocol (backward-compatible behaviour). When set to a
            ``frozenset`` of intent-type strings (e.g.
            ``frozenset({"LP_CLOSE"})``), discovery only injects the entry
            into manifests whose requested intent-type set intersects this
            allow-list. Use this to keep least-privilege manifests for
            protocols whose static permissions are only required by certain
            intent flows (e.g. TraderJoe V2's per-pair ``approveForAll`` is
            only emitted during LP_CLOSE teardown, so a SWAP-only strategy
            should not authorise it).
    """

    target: str
    label: str
    selectors: dict[str, str] = field(default_factory=dict)  # selector -> label
    send_allowed: bool = False
    intent_types: frozenset[str] | None = None  # None = all intent types; otherwise filter


@dataclass(frozen=True)
class PermissionHints:
    """Protocol-specific metadata for permission discovery.

    Attributes:
        synthetic_position_id: Format string for LP_CLOSE synthetic position_id.
            Supports ``{token0}`` and ``{token1}`` placeholders filled with
            chain token addresses.  Default ``"1"`` = NFT token ID
            (Uniswap V3 style).
        supports_standalone_fee_collection: Whether this protocol supports
            standalone LP_COLLECT_FEES intents.
        selector_labels: Extra selector -> human-readable label mappings.
            Merged into the label registry at runtime.
        synthetic_market_id: A synthetic market_id for protocols that require
            one for lending intent validation (e.g., Morpho Blue isolated markets).
            None means no market_id is needed.
        synthetic_swap_pair: Override the default (USDC, WETH) token pair for
            synthetic SWAP intents.  Dict mapping chain -> (from_token, to_token).
            Useful for protocols that only support specific token pairs
            (e.g., Curve stablecoin pools, Pendle PT tokens).
        synthetic_lp_pair: Override the default (USDC, WETH-equivalent) token
            pair for synthetic LP intents (LP_OPEN / LP_CLOSE / LP_COLLECT_FEES).
            Dict mapping chain -> (token0, token1).  Required when the framework's
            chain-default pair (e.g. bsc's ``(USDC, ETH-bridged)``) does not match
            the canonical liquid LP pair the protocol actually uses on that chain
            (e.g. sushiswap_v3 on bsc uses ``(USDT, WBNB)``).  Without an override
            the synthetic discovery seeds approves on the wrong tokens and any
            real LP test on the canonical pair fails Zodiac authorisation.
        synthetic_fee_tier: Override the default fee tier for synthetic intents
            on specific chains.  Dict mapping chain -> fee_tier.  Used when
            the protocol's default fee tier doesn't exist on a given chain
            (e.g., Agni Finance on mantle has no 3000 tier).
        static_permissions: Pre-computed permissions for protocols where
            compilation requires external state (GatewayClient, RPC).
            Dict mapping chain -> list of StaticPermissionEntry.
            These bypass compilation entirely and are injected directly.
        needs_rpc_discovery: Whether this protocol requires RPC access during
            compilation-based permission discovery.  When True and an rpc_url
            is provided to discover_permissions(), the compiler receives the
            RPC URL.  Protocols that only use static contract addresses (e.g.
            Uniswap V3, Aave V3) should leave this False to avoid unnecessary
            RPC calls during offline discovery.
    """

    synthetic_position_id: str = "1"
    supports_standalone_fee_collection: bool = False
    selector_labels: dict[str, str] = field(default_factory=dict)
    synthetic_market_id: str | None = None
    synthetic_swap_pair: dict[str, tuple[str, str]] = field(default_factory=dict)
    synthetic_lp_pair: dict[str, tuple[str, str]] = field(default_factory=dict)
    synthetic_fee_tier: dict[str, int] = field(default_factory=dict)
    static_permissions: dict[str, list[StaticPermissionEntry]] = field(default_factory=dict)
    needs_rpc_discovery: bool = False


_DEFAULT = PermissionHints()

# Protocol-literal → connector resolution.
#
# A bare string maps to ``connectors.<value>.permission_hints.PERMISSION_HINTS``
# (the convention-based default). A ``(connector_name, attribute_name)`` tuple
# resolves to ``connectors.<connector_name>.permission_hints.<attribute_name>``
# — used when one connector directory exposes multiple protocol surfaces
# through distinct module-level ``PermissionHints`` exports.
#
# The Aerodrome connector is the canonical example (audit VIB-4434 §B6,
# blueprint 05 §Aerodrome): one directory backs both ``aerodrome`` (Classic
# Solidly-fork) and ``aerodrome_slipstream`` (Uniswap V3-style CL NPM), with
# different routers, selectors, and synthetic-discovery requirements.
_PROTOCOL_CONNECTOR_MAP: dict[str, str | tuple[str, str]] = {
    "metamorpho": "morpho_vault",
    "aerodrome_slipstream": ("aerodrome", "PERMISSION_HINTS_SLIPSTREAM"),
}


def get_permission_hints(protocol: str) -> PermissionHints:
    """Load PermissionHints for a protocol via convention-based import.

    Tries ``almanak.framework.connectors.{protocol}.permission_hints.PERMISSION_HINTS``.
    If ``_PROTOCOL_CONNECTOR_MAP`` maps ``protocol`` to a
    ``(connector_name, attribute_name)`` tuple, loads
    ``connectors.{connector_name}.permission_hints.{attribute_name}`` instead.
    Falls back to defaults if the module or attribute does not exist.
    """
    mapping = _PROTOCOL_CONNECTOR_MAP.get(protocol, protocol)
    if isinstance(mapping, tuple):
        connector_name, attribute_name = mapping
    else:
        connector_name = mapping
        attribute_name = "PERMISSION_HINTS"
    try:
        mod = importlib.import_module(f"almanak.framework.connectors.{connector_name}.permission_hints")
        hints = getattr(mod, attribute_name, None)
        if isinstance(hints, PermissionHints):
            return hints
        logger.debug(
            "%s in %s.permission_hints is not a PermissionHints instance",
            attribute_name,
            connector_name,
        )
    except (ImportError, ModuleNotFoundError):
        pass
    return _DEFAULT


@dataclass(frozen=True)
class DiscoveryContext:
    """Per-chain inputs threaded to per-connector discovery_vectors overrides.

    Connectors that take ownership of vector construction receive this so they
    don't have to re-import the framework's default token pair / native-symbol
    machinery. Fields are stable at the synthetic-intents call boundary; add
    new ones only with backwards-compat defaults.
    """

    usdc: str  # default "from" token for the chain (from _get_token_pair)
    weth: str  # default "to" token for the chain (from _get_token_pair)


def get_discovery_vectors_override(
    protocol: str,
) -> Callable[[str, str, str, DiscoveryContext], list[AnyIntent] | None] | None:
    """Resolve a connector's optional ``build_discovery_vectors`` function.

    Mirrors :func:`get_permission_hints`' convention-based import + connector
    alias map, but looks up an OPTIONAL module-level function named
    ``build_discovery_vectors`` on the connector's ``permission_hints``
    module. Returns ``None`` when the connector hasn't defined one (the
    common case — most connectors stick with the declarative
    ``PermissionHints`` knobs).
    """
    mapping = _PROTOCOL_CONNECTOR_MAP.get(protocol, protocol)
    if isinstance(mapping, tuple):
        connector_name, _ = mapping
    else:
        connector_name = mapping
    module_path = f"almanak.framework.connectors.{connector_name}.permission_hints"
    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        # The override module (or a parent package along its path) is genuinely
        # absent — connector has no override. Distinguish from a NESTED import
        # error inside an existing override module (typo, broken refactor):
        # those must surface, not silently disable the override and degrade
        # the manifest.
        if exc.name and module_path.startswith(exc.name):
            return None
        raise
    fn = getattr(mod, "build_discovery_vectors", None)
    return fn if callable(fn) else None
