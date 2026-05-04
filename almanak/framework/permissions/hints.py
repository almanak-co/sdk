"""Protocol-specific permission hints.

Adapters export a PERMISSION_HINTS instance in a lightweight
``permission_hints.py`` file.  The permission system discovers it
via convention-based import - no central registry to maintain.
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field

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
    synthetic_fee_tier: dict[str, int] = field(default_factory=dict)
    static_permissions: dict[str, list[StaticPermissionEntry]] = field(default_factory=dict)
    needs_rpc_discovery: bool = False


_DEFAULT = PermissionHints()

# Protocol names that differ from their connector directory name.
_PROTOCOL_CONNECTOR_MAP: dict[str, str] = {
    "metamorpho": "morpho_vault",
}


def get_permission_hints(protocol: str) -> PermissionHints:
    """Load PermissionHints for a protocol via convention-based import.

    Tries ``almanak.framework.connectors.{protocol}.permission_hints.PERMISSION_HINTS``.
    Falls back to defaults if the module or attribute does not exist.
    """
    connector_name = _PROTOCOL_CONNECTOR_MAP.get(protocol, protocol)
    try:
        mod = importlib.import_module(f"almanak.framework.connectors.{connector_name}.permission_hints")
        hints = getattr(mod, "PERMISSION_HINTS", None)
        if isinstance(hints, PermissionHints):
            return hints
        logger.debug("PERMISSION_HINTS in %s.permission_hints is not a PermissionHints instance", connector_name)
    except (ImportError, ModuleNotFoundError):
        pass
    return _DEFAULT
