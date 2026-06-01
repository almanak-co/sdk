"""Compiler constants — protocol addresses, gas estimates, and selectors.

These are extracted from compiler.py for file-size management.
All symbols remain importable from ``almanak.framework.intents.compiler``.
"""

from __future__ import annotations

from typing import Any

# =============================================================================
# Constants
# =============================================================================

# Baseline gas estimates for chain-level common primitives (VIB-4858 / W6).
#
# Note: ``approve`` is set high (80K) to handle proxy contracts like
# Avalanche native USDC. ``swap_simple`` / ``swap_multi_hop`` are the
# DefaultSwapAdapter fallback used when a connector-owned adapter does not
# override ``estimate_gas`` — they intentionally stay generic.
_BASELINE_GAS_ESTIMATES: dict[str, int] = {
    "approve": 80000,
    "swap_simple": 200000,  # Increased from 120k - USDC proxy contracts need ~180k+
    "swap_multi_hop": 350000,  # Increased from 200k - Arbitrum swaps use more gas
    "wrap_eth": 30000,
    "unwrap_eth": 30000,
}


# Legacy back-compat merged view of every gas estimate the framework knows.
#
# VIB-4858 (W6): the per-protocol half of this dict moved onto each owning
# connector's ``gas_estimate_provider.py`` and is resolved through
# ``STRATEGY_GAS_ESTIMATE_REGISTRY``. This module-level dict is preserved as
# a derived merged view (baseline ∪ every registered connector's keys, with
# the baseline winning on overlap) so downstream SDK consumers that still do
# ``from almanak.framework.intents.compiler_constants import DEFAULT_GAS_ESTIMATES``
# and index protocol actions directly (``DEFAULT_GAS_ESTIMATES["lp_mint"]``)
# keep working byte-equivalent. Mutating it has no production effect — to
# change a connector's estimate, edit the connector's
# ``gas_estimate_provider.py``.
#
# Each per-protocol integer is resolved through the connector's
# ``gas_estimate(action, chain="")`` with an empty ``chain`` placeholder; the
# pre-W6 dict had no chain dimension, so this matches the legacy semantic
# (callers that needed per-chain overrides went through ``get_gas_estimate``,
# not this dict).
def _build_default_gas_estimates() -> dict[str, int]:
    """Materialize the legacy ``DEFAULT_GAS_ESTIMATES`` shape from the registry.

    Imports the strategy-side gas-estimate registry lazily — that module's
    boot ``_register_all()`` is what populates every connector's keys, and
    importing it at module load is safe (no back-cycle to compiler_constants).
    """
    from almanak.connectors._strategy_base.gas_estimate_registry import (
        GasEstimateRegistryError,
    )
    from almanak.connectors._strategy_gas_estimate_registry import (
        STRATEGY_GAS_ESTIMATE_REGISTRY,
    )

    merged: dict[str, int] = {}
    for action in sorted(STRATEGY_GAS_ESTIMATE_REGISTRY.actions()):
        # ``chain=""`` is the no-chain placeholder. Every current connector
        # ignores ``chain`` and returns a flat integer; for connectors that
        # specialise (e.g. Aave V3 incentive hooks), the ``get_gas_estimate``
        # call path threads the real chain through — this dict is only for
        # legacy SDK consumers that pre-W6 did not have a chain dimension.
        #
        # ``actions()`` only yields keys some connector publishes, so
        # ``lookup`` is guaranteed non-``None`` here. Fail loudly rather than
        # mask a ``None`` (or a stray ``0``) into the public dict — a zero gas
        # estimate would silently underprice every transaction for that action
        # and break the byte-equivalence contract. (CodeRabbit PR #2477.)
        estimate = STRATEGY_GAS_ESTIMATE_REGISTRY.lookup(action, "")
        if estimate is None:
            raise GasEstimateRegistryError(
                f"registry published action {action!r} via actions() but "
                f"lookup(action, '') returned None — registry invariant broken"
            )
        merged[action] = estimate
    # Baseline wins on overlap — the W6 design forbids a connector claiming
    # a baseline key (enforced by ``test_w6_gas_estimate_byte_equivalence``)
    # so this branch is defensive, but it keeps the merge order obvious.
    merged.update(_BASELINE_GAS_ESTIMATES)
    return merged


DEFAULT_GAS_ESTIMATES: dict[str, int] = _build_default_gas_estimates()


def get_gas_estimate(chain: str, operation: str) -> int:
    """Get gas estimate for an operation, with chain-specific overrides.

    Resolution order (preserved byte-equivalent across W5 + W6):

    1. **Per-chain override** — ``ChainDescriptor.gas.operation_overrides``
       (owned by ``almanak/core/chains/<chain>.py`` per W5). Wins
       whenever the descriptor publishes the operation.
    2. **Per-protocol connector estimate** — looked up via
       ``STRATEGY_GAS_ESTIMATE_REGISTRY`` (VIB-4858 / W6). Each connector
       owns the action keys it publishes; the registry routes
       ``operation`` to its owning connector's
       ``gas_estimate(action, chain)`` method.
    3. **Baseline default** — ``DEFAULT_GAS_ESTIMATES.get(operation, 120000)``
       for chain-level common primitives (approve, wrap_eth, unwrap_eth,
       swap_simple, swap_multi_hop). ``120000`` is the historical
       unknown-action fallback the legacy ``dict.get(operation, 120000)``
       expression produced.

    Args:
        chain: Target blockchain (ethereum, arbitrum, bsc, etc.). May be
            an alias — ``ChainRegistry.try_resolve`` handles the lookup.
        operation: Operation type (``swap_simple``, ``approve``,
            ``lp_mint``, ``lending_supply``, ``balancer_flash_loan``, …).

    Returns:
        Gas estimate in units.
    """
    # Lazy import to avoid a cycle with almanak.core (W5: was previously
    # done unconditionally for resolve_chain_name; now the registry IS
    # the alias resolver).
    from almanak.core.chains import ChainRegistry

    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is not None and descriptor.gas.operation_overrides is not None:
        override = descriptor.gas.operation_overrides.get(operation)
        if override is not None:
            return override

    # VIB-4858 (W6): consult the per-protocol gas-estimate registry
    # before the baseline default. Importing here keeps the boot-time
    # graph free of a strategy_base->framework cycle (the registration
    # site imports each connector's provider module which transitively
    # imports framework symbols).
    from almanak.connectors._strategy_gas_estimate_registry import (
        STRATEGY_GAS_ESTIMATE_REGISTRY,
    )

    estimate = STRATEGY_GAS_ESTIMATE_REGISTRY.lookup(operation, chain)
    if estimate is not None:
        return estimate

    # Fall back to baseline default (chain-level common primitives,
    # ``approve``/``wrap_eth``/``unwrap_eth``/``swap_simple``/``swap_multi_hop``)
    # or the unknown-action fallback 120000 for anything else.
    return _BASELINE_GAS_ESTIMATES.get(operation, 120000)


# Legacy back-compat re-export of the per-(chain, operation) overrides.
#
# VIB-4857 (W5): the per-chain data now lives on
# ``ChainDescriptor.gas.operation_overrides`` (Optional[Mapping[str, int]]).
# This module-level dict is preserved as a derived read-only view so
# downstream SDK consumers that still do
# ``from almanak.framework.intents.compiler_constants import CHAIN_GAS_OVERRIDES``
# (or via the ``compiler`` re-export) keep working. Mutating it has no
# production effect — to change a chain's overrides, edit the descriptor
# under ``almanak/core/chains/<chain>.py``.
def _build_chain_gas_overrides() -> dict[str, dict[str, int]]:
    """Materialize the legacy ``CHAIN_GAS_OVERRIDES`` shape from the registry."""
    from almanak.core.chains import ChainRegistry

    overrides: dict[str, dict[str, int]] = {}
    for descriptor in ChainRegistry.all():
        if descriptor.gas.operation_overrides is not None:
            overrides[descriptor.name] = dict(descriptor.gas.operation_overrides)
    return overrides


CHAIN_GAS_OVERRIDES: dict[str, dict[str, int]] = _build_chain_gas_overrides()


# Protocol router / LP-position-manager addresses per chain.
#
# VIB-4872 (W6-followup): per-protocol address tables now live on each
# connector's ``addresses.py`` module. The legacy module-level dicts
# below are preserved as derived read-only views so downstream SDK
# consumers (compiler / swap adapter / synthetic intents / permission
# discovery) keep working unchanged.
#
# Two sources contribute to each derived view:
#
# 1. Connector ``addresses.py`` (canonical, per-connector kind vocabulary).
# 2. A small ``_LEGACY_*`` overlay for routers that have no dedicated
#    connector folder today (uniswap_v2 router, 1inch aggregator,
#    sushiswap V2 router, quickswap V2 router, pancakeswap_v2 router).
#    Overlay entries are pure pre-refactor literals: byte-equivalence is
#    preserved.
#
# NOTE (VIB-4874): the Uniswap V4 PositionManager was *removed* from the
# overlay and now derives from ``uniswap_v4/addresses.py`` like every
# other connector-owned address. The overlay had advertised a single
# garbled value (``0xBd2165...e83b24``) across all chains that is not a
# deployed contract anywhere; the per-chain connector values are the
# canonical, on-chain-verified PositionManager addresses.


# (protocol, connector-addresses-dict-import-path, kind-in-connector-dict)
# tuples for the address dicts derived directly from connector data. The
# kind name is the connector's internal vocabulary (W1 / VIB-4853); the
# central dict re-keys by protocol so the legacy lookup shape stays
# unchanged.
def _build_protocol_routers() -> dict[str, dict[str, str]]:
    """Materialize the legacy ``PROTOCOL_ROUTERS`` shape from connector data + overlay."""
    from almanak.connectors.aerodrome.addresses import AERODROME
    from almanak.connectors.camelot.addresses import CAMELOT
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3

    routers: dict[str, dict[str, str]] = {}

    # (protocol, connector-dict, kind)
    sources: tuple[tuple[str, dict[str, dict[str, str]], str], ...] = (
        ("uniswap_v3", UNISWAP_V3, "swap_router"),
        ("sushiswap_v3", SUSHISWAP_V3, "swap_router"),
        ("pancakeswap_v3", PANCAKESWAP_V3, "swap_router"),
        ("agni_finance", AGNI_FINANCE, "swap_router"),
        ("aerodrome", AERODROME, "router"),
        ("camelot", CAMELOT, "swap_router"),
    )
    for protocol, table, kind in sources:
        for chain, kinds in table.items():
            if (protocol, chain) in _PROTOCOL_ROUTER_EXCLUSIONS:
                continue
            address = kinds.get(kind)
            if address is None:
                continue
            routers.setdefault(chain, {})[protocol] = address

    # Optimism's Velodrome V2 router is the same as the Aerodrome router
    # on Optimism. The legacy dict carried both keys for VIB-4389 (the
    # Zodiac permissions manifest generator looks up under both names);
    # preserve that exact shape.
    optimism = routers.get("optimism")
    if optimism is not None and "aerodrome" in optimism:
        optimism.setdefault("velodrome", optimism["aerodrome"])

    # Legacy routers without a dedicated connector folder. Kept as a
    # pre-refactor literal overlay so byte-equivalence holds — see the
    # block comment at the top of this section.
    for chain, protocol_to_addr in _LEGACY_PROTOCOL_ROUTERS.items():
        for protocol, address in protocol_to_addr.items():
            routers.setdefault(chain, {}).setdefault(protocol, address)

    return routers


def _build_lp_position_managers() -> dict[str, dict[str, str]]:
    """Materialize the legacy ``LP_POSITION_MANAGERS`` shape from connector data + overlay."""
    from almanak.connectors.aerodrome.addresses import AERODROME
    from almanak.connectors.camelot.addresses import CAMELOT
    from almanak.connectors.fluid.addresses import FLUID
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    managers: dict[str, dict[str, str]] = {}

    # (protocol, connector-dict, kind). traderjoe_v2 surfaces its
    # LBRouter under ``router`` (Liquidity Book uses the router for LP);
    # the legacy ``LP_POSITION_MANAGERS`` slot historically held that
    # address because synthetic intents look up the LBRouter from
    # ``LP_POSITION_MANAGERS[chain][protocol]``. PancakeSwap V3 uses
    # ``nft`` (its kind name in pancakeswap_v3/addresses.py); Aerodrome
    # exposes ``router`` for the fungible-LP path on base/optimism.
    # uniswap_v4 derives its PositionManager from the connector's own
    # per-chain ``position_manager`` kind (VIB-4874) — the central dict
    # previously carried a single garbled CREATE2-style value across all
    # chains, which is not a deployed contract on any chain. See the
    # anti-drift test in tests/unit/connectors/uniswap_v4/.
    sources: tuple[tuple[str, dict[str, dict[str, str]], str], ...] = (
        ("uniswap_v3", UNISWAP_V3, "position_manager"),
        ("uniswap_v4", UNISWAP_V4, "position_manager"),
        ("sushiswap_v3", SUSHISWAP_V3, "position_manager"),
        ("pancakeswap_v3", PANCAKESWAP_V3, "nft"),
        ("agni_finance", AGNI_FINANCE, "position_manager"),
        ("aerodrome", AERODROME, "router"),
        ("aerodrome_slipstream", AERODROME, "cl_nft"),
        ("traderjoe_v2", TRADERJOE_V2, "router"),
        ("camelot", CAMELOT, "position_manager"),
        ("fluid", FLUID, "dex_factory"),
    )
    for protocol, table, kind in sources:
        for chain, kinds in table.items():
            if (protocol, chain) in _PROTOCOL_ROUTER_EXCLUSIONS:
                continue
            address = kinds.get(kind)
            if address is None:
                continue
            managers.setdefault(chain, {})[protocol] = address

    return managers


# Per-(protocol, chain) exclusions: the connector's ``addresses.py`` may
# legitimately publish data for more chains than the central
# ``PROTOCOL_ROUTERS`` dict has historically surfaced. SushiSwap V3 on
# Avalanche, for instance, has a deployed router but was removed from the
# legacy dict because of unusable on-chain liquidity (VIB-2069); Uniswap
# V3 on Blast is published in the connector but the central dict never
# surfaced it. The derived view honours those exclusions to preserve
# byte-equivalence at the compile-time lookup boundary.
_PROTOCOL_ROUTER_EXCLUSIONS: frozenset[tuple[str, str]] = frozenset(
    {
        ("sushiswap_v3", "avalanche"),  # VIB-2069: zero usable liquidity
        ("uniswap_v3", "blast"),  # blast not in legacy PROTOCOL_ROUTERS
    }
)


# Legacy router overlay: protocols / chains where the central dict
# advertises a router/manager that has no connector-folder home today.
# Editing this overlay is a stopgap; the strategic move is to either
# create the matching connector folder + ``addresses.py`` and drop the
# overlay entry, or to retire the entry if no consumer uses it.
_LEGACY_PROTOCOL_ROUTERS: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "arbitrum": {
        "sushiswap": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "optimism": {
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "polygon": {
        "quickswap": "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff",
        "1inch": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    },
    "bsc": {
        "pancakeswap_v2": "0x10ED43C718714eb63d5aA57B78B54704E256024E",
        "sushiswap": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
    },
}

PROTOCOL_ROUTERS: dict[str, dict[str, str]] = _build_protocol_routers()
LP_POSITION_MANAGERS: dict[str, dict[str, str]] = _build_lp_position_managers()


# =============================================================================
# NFT Position Manager (NPM) address views — migration backfill consumer
# =============================================================================
#
# VIB-4864 (W2-followup): the migration backfill
# (``almanak/framework/migration/backfill.py``) used to reach directly into
# each connector's ``receipt_parser`` module for the chain -> NPM address maps
# (a ``framework -> connector.receipt_parser`` cross-layer coupling). The NPM
# address is value-bearing — it is the emitter component of an LP position's
# ``physical_identity_hash`` — so the lookups must be byte-equivalent to the
# pre-VIB-4864 parser maps.
#
# These derived views reproduce the parser maps EXACTLY, but source from each
# connector's self-contained ``addresses.py`` (W1 / VIB-4853) rather than the
# parser module. The framework's ``compiler_constants`` is the sanctioned
# connector-data aggregation point (same pattern as ``LP_POSITION_MANAGERS`` /
# ``PROTOCOL_ROUTERS``), so the backfill imports from here.
#
# Casing is preserved per-family to stay byte-equivalent with each consumer:
#   * UniV3 canonical family -> original (EIP-55) case, matching the
#     uniswap_v3 parser's ``POSITION_MANAGER_ADDRESSES`` literal.
#   * PancakeSwap V3 / Slipstream -> lowercased, matching those parsers'
#     ``_build_*`` helpers (which ``.lower()`` at view-build time).
# (Casing is hash-irrelevant downstream — ``physical_identity_hash_univ3``
# lowercases the emitter before hashing — but the views match each consumer's
# legacy return value exactly so the byte-equivalence harness stays green.)


# Chains the uniswap_v3 connector publishes a ``position_manager`` for that
# the legacy UniV3 backfill NPM map never surfaced. The parser's hand-curated
# ``POSITION_MANAGER_ADDRESSES`` literal predates these connector additions;
# surfacing them here would silently widen the backfill's supported-chain set
# (Empty != Zero — an unrecognised chain must keep returning ``None``). Honour
# the curated subset to preserve byte-equivalence.
_UNIV3_NPM_CHAIN_EXCLUSIONS: frozenset[str] = frozenset({"blast", "linea"})


def _build_univ3_nft_position_managers() -> dict[str, str]:
    """Materialize the canonical UniV3-family ``{chain: NPM}`` map.

    Byte-equivalent to ``uniswap_v3.receipt_parser.POSITION_MANAGER_ADDRESSES``:
    the canonical Uniswap V3 ``position_manager`` per chain, with Agni Finance
    overlaying Mantle (Agni rides on the uniswap_v3 connector and deploys its
    own NPM there), the ``bnb`` alias of ``bsc`` preserved, and the curated
    chain subset honoured (see ``_UNIV3_NPM_CHAIN_EXCLUSIONS``). Returns
    original-case (EIP-55) addresses.
    """
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3

    managers: dict[str, str] = {}
    # Lowercase the chain keys (consistent with the PancakeSwap / Slipstream
    # builders below) so a future mixed-case chain name in the connector
    # tables can't slip past ``_UNIV3_NPM_CHAIN_EXCLUSIONS`` or a downstream
    # ``.strip().lower()`` lookup. The connector tables are lowercase today,
    # so this is byte-equivalent.
    for chain, kinds in UNISWAP_V3.items():
        chain_lower = chain.lower()
        if chain_lower in _UNIV3_NPM_CHAIN_EXCLUSIONS:
            continue
        address = kinds.get("position_manager")
        if address:
            managers[chain_lower] = address
    # Agni Finance overlays Mantle with its own NPM (the parser literal pins
    # the Agni address for ``mantle``, not the canonical Uniswap V3 one).
    for chain, kinds in AGNI_FINANCE.items():
        chain_lower = chain.lower()
        if chain_lower in _UNIV3_NPM_CHAIN_EXCLUSIONS:
            continue
        address = kinds.get("position_manager")
        if address:
            managers[chain_lower] = address
    # Preserve the historical ``bnb`` alias of ``bsc``.
    if "bsc" in managers and "bnb" not in managers:
        managers["bnb"] = managers["bsc"]
    return managers


def _build_pancakeswap_v3_nft_position_managers() -> dict[str, str]:
    """Materialize PancakeSwap V3 ``{chain: NPM}`` (lowercased, ``bnb`` alias).

    Byte-equivalent to
    ``pancakeswap_v3.receipt_parser.POSITION_MANAGER_ADDRESSES``.
    """
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3

    managers: dict[str, str] = {}
    for chain, kinds in PANCAKESWAP_V3.items():
        nft = kinds.get("nft")
        if nft:
            managers[chain.lower()] = nft.lower()
    if "bsc" in managers and "bnb" not in managers:
        managers["bnb"] = managers["bsc"]
    return managers


def _build_slipstream_nft_position_managers() -> dict[str, str]:
    """Materialize Aerodrome / Velodrome Slipstream ``{chain: NPM}`` (lowercased).

    Byte-equivalent to ``aerodrome.receipt_parser._SLIPSTREAM_NPM_ADDRESSES``.
    """
    from almanak.connectors.aerodrome.addresses import AERODROME

    managers: dict[str, str] = {}
    for chain, kinds in AERODROME.items():
        cl_nft = kinds.get("cl_nft")
        if cl_nft:
            managers[chain.lower()] = cl_nft.lower()
    return managers


# Canonical UniV3-family NPM map (uniswap_v3 / sushiswap_v3 / agni_finance —
# Sushi V3 shares the canonical Uniswap V3 NPM on every chain it supports).
UNIV3_NFT_POSITION_MANAGERS: dict[str, str] = _build_univ3_nft_position_managers()

# PancakeSwap V3 ships its own NPM at a different address than canonical
# UniV3 on the same chain.
PANCAKESWAP_V3_NFT_POSITION_MANAGERS: dict[str, str] = _build_pancakeswap_v3_nft_position_managers()

# Aerodrome / Velodrome Slipstream NPM (Base today; Optimism unpopulated).
SLIPSTREAM_NFT_POSITION_MANAGERS: dict[str, str] = _build_slipstream_nft_position_managers()


def _build_univ3_lp_grouping_protocols() -> frozenset[str]:
    """Union of every UniV3-shape DEX connector's LP-grouping membership.

    VIB-4864 (W2-followup): replaces the hardcoded ``_UNIV3_LP_PROTOCOLS``
    frozenset that lived in the migration backfill. Each connector declares
    the protocol slugs it implements with the ``univ3_lp@v1`` grouping policy
    in its ``lp_constants.py``; this aggregates the union. Mirrors the
    VIB-4872 ``AAVE_V2_FORKS`` derivation. ``frozenset`` (not ``set``) so a
    downstream ``protocol in UNIV3_LP_GROUPING_PROTOCOLS`` consumer cannot
    silently widen the family by mutation.
    """
    from almanak.connectors.aerodrome.lp_constants import (
        UNIV3_LP_GROUPING_PROTOCOLS as _aero_lp,
    )
    from almanak.connectors.pancakeswap_v3.lp_constants import (
        UNIV3_LP_GROUPING_PROTOCOLS as _pcs_lp,
    )
    from almanak.connectors.sushiswap_v3.lp_constants import (
        UNIV3_LP_GROUPING_PROTOCOLS as _sushi_lp,
    )
    from almanak.connectors.uniswap_v3.lp_constants import (
        UNIV3_LP_GROUPING_PROTOCOLS as _uni_lp,
    )

    return frozenset(_uni_lp | _sushi_lp | _pcs_lp | _aero_lp)


# Protocol slugs using the Uniswap-V3-shape LP grouping policy
# (``univ3_lp@v1``) — NFT-position-manager-keyed concentrated liquidity.
UNIV3_LP_GROUPING_PROTOCOLS: frozenset[str] = _build_univ3_lp_grouping_protocols()

# Chain-specific known-tokens catalogue.
#
# VIB-4872 (W6-followup): per-chain entries now live on
# ``ChainDescriptor.tokens`` (Optional[Mapping[str, str]] keyed by
# lowercase symbol). The module-level dict below is preserved as a
# derived read-only view so downstream SDK consumers that still do
# ``from almanak.framework.intents.compiler_constants import CHAIN_TOKENS``
# keep working. Mutating it has no production effect — to change a
# chain's known-tokens map, edit the descriptor under
# ``almanak/core/chains/<chain>.py``.
#
# Used by ``DefaultSwapAdapter`` (fee-tier selection for common pairs)
# and Zodiac permission discovery (``almanak/framework/permissions/
# synthetic_intents._get_chain_tokens``).


def _build_chain_tokens() -> dict[str, dict[str, str]]:
    """Materialize the legacy ``CHAIN_TOKENS`` shape from the registry."""
    from almanak.core.chains import ChainRegistry

    tokens: dict[str, dict[str, str]] = {}
    for descriptor in ChainRegistry.all():
        if descriptor.tokens is not None:
            tokens[descriptor.name] = dict(descriptor.tokens)
    return tokens


CHAIN_TOKENS: dict[str, dict[str, str]] = _build_chain_tokens()

# Swap-router classification + fee-tier metadata.
#
# VIB-4872 (W6-followup): per-DEX-connector data now lives in each
# connector's ``swap_constants.py``:
#
# * ``almanak/connectors/uniswap_v3/swap_constants.py``     (uniswap_v3 + agni_finance)
# * ``almanak/connectors/sushiswap_v3/swap_constants.py``   (sushiswap_v3)
# * ``almanak/connectors/pancakeswap_v3/swap_constants.py`` (pancakeswap_v3)
# * ``almanak/connectors/camelot/swap_constants.py``        (camelot — Algebra V1.9)
#
# The legacy module-level dicts / frozensets below are preserved as
# derived read-only views aggregated at view-build time. Mutating them
# has no production effect; edit the connector's ``swap_constants.py``
# to change behaviour.


def _swap_constants_sources() -> tuple[Any, ...]:
    """Lazy-import every DEX connector's ``swap_constants`` module.

    Returns the modules themselves so the per-dict aggregator helpers
    below can pluck whichever symbol they need without each helper
    re-paying the import cost.
    """
    from almanak.connectors.pancakeswap_v3 import swap_constants as _pcsv3_sc
    from almanak.connectors.sushiswap_v3 import swap_constants as _sushi_sc
    from almanak.connectors.uniswap_v3 import swap_constants as _uni_sc

    return (_uni_sc, _sushi_sc, _pcsv3_sc)


def _build_swap_fee_tiers() -> dict[str, tuple[int, ...]]:
    tiers: dict[str, tuple[int, ...]] = {}
    for source in _swap_constants_sources():
        for protocol, entry in source.SWAP_FEE_TIERS.items():
            if protocol in tiers and tiers[protocol] != entry:
                raise ValueError(
                    f"protocol {protocol!r} has conflicting SWAP_FEE_TIERS contributions: {tiers[protocol]} vs {entry}"
                )
            tiers[protocol] = entry
    return tiers


def _build_default_swap_fee_tier() -> dict[str, int]:
    defaults: dict[str, int] = {}
    for source in _swap_constants_sources():
        for protocol, fee in source.DEFAULT_SWAP_FEE_TIER.items():
            if protocol in defaults and defaults[protocol] != fee:
                raise ValueError(
                    f"protocol {protocol!r} has conflicting DEFAULT_SWAP_FEE_TIER contributions: "
                    f"{defaults[protocol]} vs {fee}"
                )
            defaults[protocol] = fee
    return defaults


def _build_swap_router_v1_protocols() -> frozenset[str]:
    members: set[str] = set()
    for source in _swap_constants_sources():
        members |= source.SWAP_ROUTER_V1_PROTOCOLS
    return frozenset(members)


def _build_swap_router_v1_chain_overrides() -> dict[str, frozenset[str]]:
    overrides: dict[str, set[str]] = {}
    for source in _swap_constants_sources():
        for chain, protocols in source.SWAP_ROUTER_V1_CHAIN_OVERRIDES.items():
            overrides.setdefault(chain, set()).update(protocols)
    return {chain: frozenset(protos) for chain, protos in overrides.items()}


def _build_swap_router_algebra_protocols() -> frozenset[str]:
    from almanak.connectors.camelot.swap_constants import SWAP_ROUTER_ALGEBRA_PROTOCOLS as _ca

    return _ca


SWAP_FEE_TIERS: dict[str, tuple[int, ...]] = _build_swap_fee_tiers()

# Chain-specific fee-tier overrides. Empty today; reserved for cases
# where a V3 fork on a specific chain supports additional fee tiers
# beyond the base protocol's contribution. Kept as a derived view so
# the lookup shape stays available for the consumer in
# ``_strategy_base/base/swap_adapter.py``.
SWAP_FEE_TIERS_CHAIN: dict[tuple[str, str], tuple[int, ...]] = {}

DEFAULT_SWAP_FEE_TIER: dict[str, int] = _build_default_swap_fee_tier()

# Protocols using the original SwapRouter interface (8-param
# ``exactInputSingle`` WITH deadline). All other V3 forks use
# SwapRouter02 (7-param, no deadline).
SWAP_ROUTER_V1_PROTOCOLS: frozenset[str] = _build_swap_router_v1_protocols()

# Chain-specific overrides: V3 forks that use the V1-style router on a
# specific chain (e.g., Agni on Mantle).
SWAP_ROUTER_V1_CHAIN_OVERRIDES: dict[str, frozenset[str]] = _build_swap_router_v1_chain_overrides()

# Protocols using the Algebra V1.9 router interface (VIB-1636).
# exactInputSingle((address,address,address,uint256,uint256,uint256,uint160))
# Selector ``0xbc651188``. Algebra has no ``fee`` parameter — fees are
# determined dynamically by the pool.
SWAP_ROUTER_ALGEBRA_PROTOCOLS: frozenset[str] = _build_swap_router_algebra_protocols()

# Quoter addresses used for AUTO fee tier selection.
#
# VIB-4872 (W6-followup): per-protocol quoter addresses now live on each
# connector's ``addresses.py`` module (W1 / VIB-4853 vocabulary —
# ``quoter_v2`` for Uniswap V3 forks, ``quoter`` for PancakeSwap V3 /
# Camelot Algebra V1.9). Legacy module-level dict preserved as a derived
# read-only view; the ``bnb`` alias for ``bsc`` is built at view-build
# time so the existing ``SWAP_QUOTER_ADDRESSES["bnb"]`` lookups (per the
# VIB-708 unification) keep working.


# Per-(protocol, chain) exclusions for SWAP_QUOTER_ADDRESSES. SushiSwap V3
# on Optimism / Avalanche had its quoter dropped from the legacy central
# dict even though the connector publishes the address (Optimism has no
# Sushi quoter entry in the legacy dict; Avalanche tracks the same
# VIB-2069 liquidity-impact exclusion as PROTOCOL_ROUTERS / LP managers).
# Uniswap V3 on Blast was never surfaced in the legacy quoter dict either.
_SWAP_QUOTER_EXCLUSIONS: frozenset[tuple[str, str]] = frozenset(
    {
        ("sushiswap_v3", "avalanche"),
        ("sushiswap_v3", "optimism"),
        ("uniswap_v3", "blast"),
    }
)


def _build_swap_quoter_addresses() -> dict[str, dict[str, str]]:
    """Materialize the legacy ``SWAP_QUOTER_ADDRESSES`` shape from connector data."""
    from almanak.connectors.camelot.addresses import CAMELOT
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3

    quoters: dict[str, dict[str, str]] = {}

    sources: tuple[tuple[str, dict[str, dict[str, str]], str], ...] = (
        ("uniswap_v3", UNISWAP_V3, "quoter_v2"),
        ("sushiswap_v3", SUSHISWAP_V3, "quoter_v2"),
        ("pancakeswap_v3", PANCAKESWAP_V3, "quoter"),
        ("agni_finance", AGNI_FINANCE, "quoter_v2"),
        ("camelot", CAMELOT, "quoter"),
    )
    for protocol, table, kind in sources:
        for chain, kinds in table.items():
            if (protocol, chain) in _SWAP_QUOTER_EXCLUSIONS:
                continue
            address = kinds.get(kind)
            if address is None:
                continue
            quoters.setdefault(chain, {})[protocol] = address

    # The bsc / bnb alias unification (VIB-708): the legacy dict carries
    # a ``"bnb"`` mirror of every ``"bsc"`` quoter so callers that pass
    # the alias resolve. Replicate by copying the bsc map; both keys
    # point at the same address values, no behaviour change.
    bsc = quoters.get("bsc")
    if bsc is not None:
        quoters["bnb"] = dict(bsc)

    return quoters


SWAP_QUOTER_ADDRESSES: dict[str, dict[str, str]] = _build_swap_quoter_addresses()

# Lending pool + data-provider addresses per chain/protocol.
#
# VIB-4872 (W6-followup): the per-(chain, protocol) lending address tables
# now live on each lending connector's ``addresses.py`` module:
#
# * ``almanak/connectors/aave_v3/addresses.py``    -> ``AAVE_V3``
# * ``almanak/connectors/radiant_v2/addresses.py`` -> ``RADIANT_V2``
# * ``almanak/connectors/spark/addresses.py``      -> ``SPARK``
#
# Each connector publishes its own contract-kind vocabulary (``pool`` /
# ``pool_data_provider`` / ``oracle``). The legacy module-level dicts
# below are preserved as derived read-only views so downstream SDK
# consumers that import them directly keep working; mutating them has no
# production effect.
#
# Aave V2 and Aave V3 share the ``getReserveConfigurationData(address)``
# selector + ABI-encoded return layout, so the same pre-flight code works
# for both. Radiant V2 is an Aave V2 fork — only Ethereum has a working
# deployment (Arbitrum's pool was reduced to a stub after Oct 2024; see
# #1842 / #1847 / #1889 and the Radiant connector's ``addresses.py``).


def _build_lending_pool_addresses() -> dict[str, dict[str, str]]:
    """Materialize the legacy ``LENDING_POOL_ADDRESSES`` shape from connector data."""
    from almanak.connectors.aave_v3.addresses import AAVE_V3
    from almanak.connectors.radiant_v2.addresses import RADIANT_V2
    from almanak.connectors.spark.addresses import SPARK

    sources: tuple[tuple[str, dict[str, dict[str, str]]], ...] = (
        ("aave_v3", AAVE_V3),
        ("radiant_v2", RADIANT_V2),
        ("spark", SPARK),
    )

    pools: dict[str, dict[str, str]] = {}
    for protocol, table in sources:
        for chain, kinds in table.items():
            pool = kinds.get("pool")
            if pool is None:
                continue
            pools.setdefault(chain, {})[protocol] = pool
    return pools


def _build_lending_pool_data_providers() -> dict[str, dict[str, str]]:
    """Materialize the legacy ``LENDING_POOL_DATA_PROVIDERS`` shape from connector data.

    Spark intentionally omitted — the legacy central dict only carried
    aave_v3 + radiant_v2 entries for the lending pre-flight; preserving
    that exact shape avoids accidentally widening the surface as part of
    a pure-data-move refactor. Adding Spark to the pre-flight surface is
    tracked as a separate decision (the Spark adapter already owns its
    own ``pool_data_provider`` address via ``addresses.SPARK``).
    """
    from almanak.connectors.aave_v3.addresses import AAVE_V3
    from almanak.connectors.radiant_v2.addresses import RADIANT_V2

    sources: tuple[tuple[str, dict[str, dict[str, str]]], ...] = (
        ("aave_v3", AAVE_V3),
        ("radiant_v2", RADIANT_V2),
    )

    providers: dict[str, dict[str, str]] = {}
    for protocol, table in sources:
        for chain, kinds in table.items():
            provider = kinds.get("pool_data_provider")
            if provider is None:
                continue
            providers.setdefault(chain, {})[protocol] = provider
    return providers


LENDING_POOL_ADDRESSES: dict[str, dict[str, str]] = _build_lending_pool_addresses()
LENDING_POOL_DATA_PROVIDERS: dict[str, dict[str, str]] = _build_lending_pool_data_providers()

# Standard ERC20 function selectors
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)
ERC20_ALLOWANCE_SELECTOR = "0xdd62ed3e"  # allowance(address,address)
ERC20_TRANSFER_SELECTOR = "0xa9059cbb"  # transfer(address,uint256)
ERC20_TRANSFER_FROM_SELECTOR = "0x23b872dd"  # transferFrom(address,address,uint256)

# Tokens that require approve(0) before approving a new amount if allowance > 0
# This is a security feature in USDC/USDT to prevent certain attack vectors
APPROVE_ZERO_FIRST_TOKENS: set[str] = {
    # Avalanche USDC
    "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E".lower(),
    # Avalanche USDC.e (bridged)
    "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664".lower(),
    # Avalanche USDT
    "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7".lower(),
    # Arbitrum USDC
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831".lower(),
    # Arbitrum USDC.e (bridged)
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8".lower(),
    # Arbitrum USDT
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9".lower(),
    # Ethereum USDC
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),
    # Ethereum USDT
    "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(),
    # Base USDC
    "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".lower(),
    # Optimism USDC
    "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85".lower(),
    # Optimism USDC.e (bridged)
    "0x7F5c764cBc14f9669B88837ca1490cCa17c31607".lower(),
}

# Uniswap V3 NonfungiblePositionManager function selectors
# mint(MintParams): create new position
NFT_POSITION_MINT_SELECTOR = "0x88316456"
# increaseLiquidity(IncreaseLiquidityParams): add liquidity to existing position
NFT_POSITION_INCREASE_SELECTOR = "0x219f5d17"
# decreaseLiquidity(DecreaseLiquidityParams): remove liquidity from position
NFT_POSITION_DECREASE_SELECTOR = "0x0c49ccbe"
# collect(CollectParams): collect tokens owed (fees + withdrawn liquidity)
NFT_POSITION_COLLECT_SELECTOR = "0xfc6f7865"
# burn(tokenId): burn position NFT (requires position to be empty)
NFT_POSITION_BURN_SELECTOR = "0x42966c68"


# Aave V2 forks (use ``deposit()`` instead of ``supply()``, otherwise same ABI).
#
# VIB-4872 (W6-followup): per-connector membership now lives in each
# lending connector's ``lending_constants.py``. The derived module-level
# frozenset below is the union of every V2-fork connector's contribution.
# CodeRabbit (PR #2478): the derived set is read-only by contract — keep
# it as a ``frozenset`` rather than ``set`` so accidental mutation by a
# downstream consumer raises rather than silently widening the family.
# ``set == frozenset`` semantically (equality, ``in`` membership) so the
# pre-refactor consumers' ``protocol in AAVE_V2_FORKS`` branches are
# byte-equivalent at the lookup boundary.
def _build_aave_v2_forks() -> frozenset[str]:
    from almanak.connectors.radiant_v2.lending_constants import AAVE_V2_FORK_PROTOCOLS

    return frozenset(AAVE_V2_FORK_PROTOCOLS)


AAVE_V2_FORKS: frozenset[str] = _build_aave_v2_forks()


# Protocols sharing the Aave V3 lending-pool interface (same ABI,
# different addresses). VIB-4872: derived from the V3-family connector
# membership + the V2-fork set (V2 forks share the V3 read surface; the
# compile-time selector branch handles the ``deposit`` vs ``supply``
# difference). Same ``frozenset`` immutability as ``AAVE_V2_FORKS``.
def _build_aave_compatible_protocols() -> frozenset[str]:
    from almanak.connectors.aave_v3.lending_constants import AAVE_V3_FAMILY_PROTOCOLS

    return frozenset(AAVE_V3_FAMILY_PROTOCOLS) | AAVE_V2_FORKS


AAVE_COMPATIBLE_PROTOCOLS: frozenset[str] = _build_aave_compatible_protocols()

# Aave V2 Pool function selectors (used by V2 forks: Radiant V2, etc.)
# deposit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
AAVE_V2_DEPOSIT_SELECTOR = "0xe8eda9df"

# Aave V3 Pool function selectors
# supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode)
AAVE_SUPPLY_SELECTOR = "0x617ba037"
# borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf)
AAVE_BORROW_SELECTOR = "0xa415bcad"
# repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf)
AAVE_REPAY_SELECTOR = "0x573ade81"
# withdraw(address asset, uint256 amount, address to)
AAVE_WITHDRAW_SELECTOR = "0x69328dec"
# setUserUseReserveAsCollateral(address asset, bool useAsCollateral)
AAVE_SET_COLLATERAL_SELECTOR = "0x5a3b74b9"
# flashLoan(address receiverAddress, address[] assets, uint256[] amounts, uint256[] modes, address onBehalfOf, bytes params, uint16 referralCode)
AAVE_FLASH_LOAN_SELECTOR = "0xab9c4b5d"
# flashLoanSimple(address receiverAddress, address asset, uint256 amount, bytes params, uint16 referralCode)
AAVE_FLASH_LOAN_SIMPLE_SELECTOR = "0x42b0b77c"

# Aave interest rate modes
AAVE_VARIABLE_RATE_MODE = 2  # Variable rate (stable rate deprecated on Aave V3)


# Balancer Vault function selectors
# flashLoan(address recipient, address[] tokens, uint256[] amounts, bytes userData)
BALANCER_FLASH_LOAN_SELECTOR = "0x5c38449e"

# Balancer Vault addresses per chain.
#
# VIB-4872 (W6-followup): now owned by
# ``almanak/connectors/balancer_v2/addresses.py`` (the Balancer V2 Vault
# is a CREATE2 deterministic deployment so every chain pins the same
# address). The legacy module-level dict below is preserved as a derived
# read-only view so downstream SDK consumers (and the strategy-side
# adapter import) keep working unchanged.


def _build_balancer_vault_addresses() -> dict[str, str]:
    """Materialize the legacy ``BALANCER_VAULT_ADDRESSES`` shape from connector data."""
    from almanak.connectors.balancer_v2.addresses import BALANCER_V2

    return {chain: kinds["vault"] for chain, kinds in BALANCER_V2.items() if "vault" in kinds}


BALANCER_VAULT_ADDRESSES: dict[str, str] = _build_balancer_vault_addresses()

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1
# Max uint128 for collecting all fees/tokens
MAX_UINT128 = 2**128 - 1
