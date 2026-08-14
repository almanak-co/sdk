"""Protocol-specific permission hints.

Adapters export a PERMISSION_HINTS instance in a lightweight
``permission_hints.py`` file.  The permission system discovers it
via convention-based import - no central registry to maintain.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from almanak.core.intent_types import IntentType

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
            ``frozenset`` of canonical intent types (e.g.
            ``frozenset({IntentType.LP_CLOSE})``), discovery only injects the entry
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
    intent_types: frozenset[IntentType] | None = None  # None = all intent types; otherwise filter

    def __post_init__(self) -> None:
        if self.intent_types is not None and any(not isinstance(value, IntentType) for value in self.intent_types):
            raise TypeError("StaticPermissionEntry.intent_types must contain canonical IntentType members")


@dataclass(frozen=True)
class PermissionHints:
    """Protocol-specific metadata for permission discovery.

    Attributes:
        synthetic_position_id: Format string for the synthetic ``position_id``
            used by BOTH LP_CLOSE and LP_COLLECT_FEES (VIB-6149 — collect
            carries it inside ``protocol_params`` rather than as a top-level
            field). Supports ``{token0}`` and ``{token1}`` placeholders filled
            with chain token addresses. Default ``"1"`` = NFT token ID
            (Uniswap V3 style).

            **Constraint when ``supports_standalone_fee_collection`` is True**:
            every NFT-position collect compiler coerces this via ``int(...)``,
            so it must be integer-parseable. A composite form (e.g. aerodrome's
            ``"{token0}/{token1}/volatile"``, valid for its LP_CLOSE) would fail
            to compile and silently empty that protocol's collect manifest —
            discovery degrades to zero targets rather than raising.
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
        offline_discovery: Whether this protocol's compiler can produce complete
            calldata with NO network reads. When True, the discovery compiler is
            built with ``offline_discovery=True``, which stops
            ``_get_chain_rpc_url`` from resolving an *implicit* transport (a
            managed Anvil fork, then a free public RPC) that the caller never
            asked for. An explicit ``rpc_url`` is still honoured.

            This is what makes a manifest a pure function of the connector
            registry rather than of RPC weather: curve LP discovery on arbitrum
            was issuing 43 live ``eth_call``s and producing 7/3/7 targets and
            6/2/7 selectors across three consecutive runs, because a 429 on the
            ``is_ng`` ABI probe silently flipped which selector family got
            authorised (VIB-6046 D5).

            Opt-IN. ``False`` preserves today's behaviour exactly. Several
            connectors (gmx_v2, pendle, traderjoe_v2, uniswap_v4 hooks) discover
            NOTHING without the implicit fallback, so this must not be flipped
            on globally without first giving each of them an offline path.
        synthetic_discovery_intents: The set of canonical intent types (e.g.
            ``IntentType.SWAP``, ``IntentType.LP_OPEN``, ``IntentType.SUPPLY``) this protocol slug
            participates in for synthetic permission discovery. **Opt-OUT
            default** (empty) — a protocol with no declaration emits no
            synthetic intents and therefore appears in none of the derived
            membership sets in ``synthetic_intents.py``. This is the per-slug
            source of truth that replaced the hardcoded ``_SWAP_PROTOCOLS`` /
            ``_LP_PROTOCOLS`` / ``_LENDING_PROTOCOLS`` / ``_PERP_PROTOCOLS``
            frozensets (VIB-4928). It is declared per-*slug* (not per-compiler)
            because several distinct protocol slugs share one compiler class
            with different discovery participation — e.g. ``agni_finance``
            shares ``UniswapV3Compiler`` but participates in neither SWAP nor
            LP. Note the converse trap (VIB-5990): sharing a compiler class is
            NOT a reason to assume a slug participates in *fewer* intents.
            ``aerodrome`` and ``aerodrome_slipstream`` share
            ``AerodromeCompiler``, and this docstring previously cited
            slipstream as "LP only, not SWAP" — that was wrong. The compiler
            dispatches SWAP for both slugs, so the omission produced an empty
            Zodiac manifest and every Safe-path slipstream swap reverted
            unauthorized. Derive participation from what the compiler can
            actually build for the slug, and verify with
            ``discover_permissions(...)`` — never from a sibling slug's
            declaration. ``LP_COLLECT_FEES``
            stays gated by ``supports_standalone_fee_collection`` and need not be
            listed here. Each listed value must be an intent the protocol's
            compiler can actually build (a subset of the compiler's ``intents``).
        supports_native_in_swap: Whether this protocol's SwapRouter wraps the
            chain's native gas token via ``msg.value`` (no ERC-20 approve,
            single value-bearing tx). When True, synthetic SWAP discovery emits
            an extra native-input intent so ``tx.value > 0`` flips
            ``send_allowed=True`` on the router target — Zodiac Roles requires
            this for a value-bearing call to pass authorisation at
            ``execTransactionWithRole``. V3-style SwapRouter02 routers
            (Uniswap V3 / PancakeSwap V3 / Sushiswap V3) set this True; Solidly
            forks and others leave it False (the historical
            ``_NATIVE_IN_SWAP_PROTOCOLS`` membership). Only meaningful when
            ``IntentType.SWAP`` is in ``synthetic_discovery_intents``.
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
    offline_discovery: bool = False
    synthetic_discovery_intents: frozenset[IntentType] = frozenset()
    supports_native_in_swap: bool = False

    def __post_init__(self) -> None:
        if any(not isinstance(value, IntentType) for value in self.synthetic_discovery_intents):
            raise TypeError("PermissionHints.synthetic_discovery_intents must contain canonical IntentType members")


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


def _is_missing_along_path(module_path: str, missing: str | None) -> bool:
    """True when ``missing`` is ``module_path`` or a PACKAGE ON ITS PATH.

    This is the discriminator between "connector genuinely has no
    ``permission_hints`` module" (return the empty default) and "an import
    INSIDE an existing module is broken" (raise).

    It must compare DOTTED COMPONENTS, not raw string prefixes. A bare
    ``module_path.startswith(missing)`` matches partial component names and
    swallows exactly the class this function exists to catch:

    * target ``…uniswap_v3.permission_hints``, nested failure
      ``almanak.connectors.uniswap`` (a refactor that dropped the version
      suffix) — ``startswith`` is True, so the broken import is misread as an
      absent module;
    * target ``…curve.permission_hints``, nested ``from .permission import X``
      failing as ``almanak.connectors.curve.permission`` — likewise swallowed.

    Both return empty hints, dropping the connector out of every membership set
    in ``synthetic_intents.py`` and yielding a manifest with zero core grants —
    the fail-open this module exists to close, on a money-path connector.
    Anchoring on ``missing + "."`` makes a partial component a non-match while
    keeping every genuine parent-package case working.
    """
    if not missing:
        return False
    return module_path == missing or module_path.startswith(missing + ".")


class PermissionHintsError(RuntimeError):
    """A connector's ``permission_hints`` module exists but is unusable.

    Distinct from "this protocol has no hints", which is a legitimate state and
    still yields the empty default. This is raised only when the module is
    present and broken, where silently substituting empty hints would produce a
    manifest that authorises nothing while looking successful (VIB-6018).
    """


class PermissionBindingError(RuntimeError):
    """A deployment-scoped permission target could not be verified.

    Exact target addresses supplied by a strategy are required permissions,
    not best-effort hints.  Resolution, identity validation, or compilation of
    such a target must therefore abort manifest generation rather than degrade
    to a warning and emit an incomplete Safe role.
    """


def get_permission_hints(protocol: str) -> PermissionHints:
    """Load PermissionHints for a protocol via convention-based import.

    Tries ``almanak.connectors.{protocol}.permission_hints.PERMISSION_HINTS``.
    If ``_PROTOCOL_CONNECTOR_MAP`` maps ``protocol`` to a
    ``(connector_name, attribute_name)`` tuple, loads
    ``connectors.{connector_name}.permission_hints.{attribute_name}`` instead.
    Returns the empty default only when the module is genuinely ABSENT.

    A **nested** import error inside an existing ``permission_hints`` module —
    a typo, a broken refactor, a renamed symbol upstream — is re-raised
    (VIB-6018). Swallowing it silently substituted empty hints, which drops the
    connector out of every derived membership set in ``synthetic_intents.py``
    and yields an empty Zodiac manifest: the exact VIB-5990 failure shape, with
    no signal anywhere. An empty manifest is not a degraded result here, it is a
    wrong one — every Safe-path call for that connector reverts unauthorized (or,
    pre-VIB-6057, executes unconstrained inside a MultiSend batch).

    A module that exists but exports no usable ``PERMISSION_HINTS`` raises
    :class:`PermissionHintsError` for the same reason — see the raise site.

    This mirrors :func:`get_discovery_vectors_override`, which already made this
    distinction; the two now agree, so a connector cannot lose its declarative
    hints while keeping its vector override or vice versa.

    Raises:
        PermissionHintsError: the module exists but does not export a usable
            ``PermissionHints`` instance.
        ImportError: a nested import inside an existing module is broken.
    """
    mapping = _PROTOCOL_CONNECTOR_MAP.get(protocol, protocol)
    if isinstance(mapping, tuple):
        connector_name, attribute_name = mapping
    else:
        connector_name = mapping
        attribute_name = "PERMISSION_HINTS"
    module_path = f"almanak.connectors.{connector_name}.permission_hints"
    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        # ``exc.name`` is the module that could not be found. It names the target
        # module (or a package along its path) exactly when that module is the
        # thing that is missing — i.e. this connector genuinely has no
        # permission_hints module. Anything else is a broken import INSIDE an
        # existing module and must surface.
        if _is_missing_along_path(module_path, exc.name):
            return _DEFAULT
        raise
    hints = getattr(mod, attribute_name, None)
    if isinstance(hints, PermissionHints):
        return hints
    # The module EXISTS but does not export usable hints — a renamed constant, a
    # bad merge, a plain dict where a PermissionHints was meant. Returning
    # _DEFAULT here would reintroduce the very failure this function was changed
    # to close, just through a different door: the connector silently drops out
    # of every derived membership set and its manifest comes back empty, with a
    # DEBUG line as the only trace. Fail closed instead — the connector's own
    # module is the one place this is unambiguously a defect, never a legitimate
    # "this protocol has no hints" signal (that case is the ModuleNotFoundError
    # branch above). Measured 2026-07-28: all 44 registered protocols export a
    # valid PERMISSION_HINTS, so this raises for nobody today.
    raise PermissionHintsError(
        f"{module_path} exists but does not export a usable {attribute_name}: "
        f"expected a PermissionHints instance, found {type(hints).__name__}. "
        "An empty manifest is not a degraded result — every Safe-path call for "
        f"{connector_name!r} would revert unauthorized. Fix the export rather "
        "than letting discovery fall back to empty hints."
    )


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
    # Deployment-scoped inputs.  Connector overrides may turn these into
    # connector-owned immutable bindings; the framework never interprets the
    # payload or copies target addresses into a global registry.
    strategy_config: Mapping[str, Any] = field(default_factory=dict)
    rpc_url: str | None = None
    gateway_client: Any = field(default=None, repr=False, compare=False)


def get_discovery_vectors_override(
    protocol: str,
) -> Callable[[str, IntentType, str, DiscoveryContext], list[AnyIntent] | None] | None:
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
    module_path = f"almanak.connectors.{connector_name}.permission_hints"
    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        # The override module (or a parent package along its path) is genuinely
        # absent — connector has no override. Distinguish from a NESTED import
        # error inside an existing override module (typo, broken refactor):
        # those must surface, not silently disable the override and degrade
        # the manifest.
        if _is_missing_along_path(module_path, exc.name):
            return None
        raise
    fn = getattr(mod, "build_discovery_vectors", None)
    return fn if callable(fn) else None
