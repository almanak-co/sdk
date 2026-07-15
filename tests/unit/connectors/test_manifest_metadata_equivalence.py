"""Equivalence contract for manifest-owned metadata registries (VIB-4851).

The capabilities / supported-chains / primitive registries used to hold
hardcoded central loader tables (``_BUILTIN_LOADERS``) mapping protocol
identifiers to connector metadata modules. Those rows now live on each
connector's ``CONNECTOR`` manifest (``capabilities=CapabilitiesSpec(...)``,
``supported_chains=SupportedChainsSpec(...)``,
``primitive=ImportRef(..., "PRIMITIVE")``) and the registries derive their
ownership maps from manifest discovery.

These tests freeze the legacy tables VERBATIM (as last committed before the
inversion) and assert the manifest-derived maps equal them exactly. Exact
``==`` is deliberately anti-widening AND anti-narrowing: a connector silently
gaining or losing a metadata key is a behavior change that must show up here
and be acknowledged by editing the frozen dict in the same PR.
"""

from __future__ import annotations

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.capabilities_registry import CapabilitiesRegistry
from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.connectors._strategy_base.gateway_stub_registry import GatewayStubRegistry
from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry
from almanak.connectors._strategy_base.prediction_execute_registry import PredictionExecuteRegistry
from almanak.connectors._strategy_base.prediction_read_registry import PredictionReadRegistry
from almanak.connectors._strategy_base.supported_chains_registry import SupportedChainsRegistry

# almanak/connectors/_strategy_base/capabilities_registry.py
# ``CapabilitiesRegistry._BUILTIN_LOADERS`` as of 2026-06-10, frozen verbatim.
FROZEN_CAPABILITIES_LOADERS = {
    "aave_v3": "almanak.connectors.aave_v3.capabilities",
    "spark": "almanak.connectors.spark.capabilities",
    "compound_v3": "almanak.connectors.compound_v3.capabilities",
    "benqi": "almanak.connectors.benqi.capabilities",
    "euler_v2": "almanak.connectors.euler_v2.capabilities",
    "morpho": "almanak.connectors.morpho_blue.capabilities",
    "morpho_blue": "almanak.connectors.morpho_blue.capabilities",
    "curvance": "almanak.connectors.curvance.capabilities",
    "silo_v2": "almanak.connectors.silo_v2.capabilities",
    "kamino": "almanak.connectors.kamino.capabilities",
    "gmx_v2": "almanak.connectors.gmx_v2.capabilities",
    "hyperliquid": "almanak.connectors.hyperliquid.capabilities",
    "drift": "almanak.connectors.drift.capabilities",
    "uniswap_v3": "almanak.connectors.uniswap_v3.capabilities",
    "enso": "almanak.connectors.enso.capabilities",
    "pendle": "almanak.connectors.pendle.capabilities",
    "metamorpho": "almanak.connectors.morpho_vault.capabilities",
    "polymarket": "almanak.connectors.polymarket.capabilities",
    "raydium_clmm": "almanak.connectors.raydium.capabilities",
    "meteora_dlmm": "almanak.connectors.meteora.capabilities",
    "orca_whirlpools": "almanak.connectors.orca.capabilities",
    # VIB-5031: the vault NFT-CDP key (requires_market_id) — scoped to
    # fluid_vault ONLY; fluid/fluid_lending deliberately have no entry.
    "fluid_vault": "almanak.connectors._fluid_core.capabilities",
    # Protocol-enum removal: curve gained a capabilities entry when the
    # LPCloseIntent exit-selector guards (coin_index / imbalanced_amounts)
    # moved from a Protocol.CURVE comparison onto the connector-declared
    # lp_close_exit_selectors flag.
    "curve": "almanak.connectors.curve.capabilities",
}

# almanak/connectors/_strategy_base/supported_chains_registry.py
# ``SupportedChainsRegistry._BUILTIN_LOADERS`` as of 2026-06-10, frozen verbatim.
FROZEN_SUPPORTED_CHAINS_LOADERS = {
    "aave_v3": "almanak.connectors.aave_v3.supported_chains",
    "spark": "almanak.connectors.spark.supported_chains",
    "benqi": "almanak.connectors.benqi.supported_chains",
    "euler_v2": "almanak.connectors.euler_v2.supported_chains",
    "silo_v2": "almanak.connectors.silo_v2.supported_chains",
    "uniswap_v3": "almanak.connectors.uniswap_v3.supported_chains",
    "agni_finance": "almanak.connectors.uniswap_v3.supported_chains",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.supported_chains",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.supported_chains",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.supported_chains",
    "enso": "almanak.connectors.enso.supported_chains",
    "gmx_v2": "almanak.connectors.gmx_v2.supported_chains",
    "hyperliquid": "almanak.connectors.hyperliquid.supported_chains",
    "lido": "almanak.connectors.lido.supported_chains",
    "ethena": "almanak.connectors.ethena.supported_chains",
    "gimo": "almanak.connectors.gimo.supported_chains",
}

# almanak/connectors/_strategy_base/primitive_registry.py
# ``PrimitiveRegistry._BUILTIN_LOADERS`` as of 2026-06-10, frozen verbatim.
FROZEN_PRIMITIVE_LOADERS = {
    "uniswap_v3": "almanak.connectors.uniswap_v3.primitive",
    "aerodrome": "almanak.connectors.aerodrome.primitive",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.primitive",
    "uniswap_v4": "almanak.connectors.uniswap_v4.primitive",
    "aave_v3": "almanak.connectors.aave_v3.primitive",
    "morpho_blue": "almanak.connectors.morpho_blue.primitive",
    "compound_v3": "almanak.connectors.compound_v3.primitive",
    "gmx_v2": "almanak.connectors.gmx_v2.primitive",
    "drift": "almanak.connectors.drift.primitive",
    "hyperliquid": "almanak.connectors.hyperliquid.primitive",
    "polymarket": "almanak.connectors.polymarket.primitive",
}


def test_capabilities_ownership_equals_frozen_legacy_table() -> None:
    """Manifest-derived capabilities ownership == the legacy hardcoded table."""
    assert CapabilitiesRegistry._loaders() == FROZEN_CAPABILITIES_LOADERS


def test_supported_chains_ownership_equals_frozen_legacy_table() -> None:
    """Manifest-derived chain-coverage ownership == the legacy hardcoded table."""
    assert SupportedChainsRegistry._loaders() == FROZEN_SUPPORTED_CHAINS_LOADERS


def test_primitive_ownership_equals_frozen_legacy_table() -> None:
    """Manifest-declared primitive modules == the legacy hardcoded table."""
    derived = {
        connector.name: connector.primitive.module
        for connector in CONNECTOR_REGISTRY.with_primitive()
        if connector.primitive is not None
    }
    assert derived == FROZEN_PRIMITIVE_LOADERS


def test_primitive_refs_use_the_canonical_attribute() -> None:
    """Every manifest primitive ref loads the conventional ``PRIMITIVE`` symbol."""
    for connector in CONNECTOR_REGISTRY.with_primitive():
        assert connector.primitive is not None
        assert connector.primitive.attribute == "PRIMITIVE", (
            f"{connector.name!r} declares primitive attribute "
            f"{connector.primitive.attribute!r}; the registry convention is 'PRIMITIVE'"
        )


# almanak/connectors/_strategy_base/lending_read_registry.py tables as of
# 2026-06-10, frozen verbatim.
FROZEN_LENDING_SPEC_LOADERS = {
    "aave_v3": ("almanak.connectors.aave_v3.lending_read", "LENDING_READ_SPEC"),
    "spark": ("almanak.connectors.spark.lending_read", "LENDING_READ_SPEC"),
}
FROZEN_LENDING_ACCOUNT_STATE_LOADERS = {
    "aave_v3": ("almanak.connectors.aave_v3.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    "spark": ("almanak.connectors.spark.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    "morpho_blue": ("almanak.connectors.morpho_blue.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    "compound_v3": ("almanak.connectors.compound_v3.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    "silo_v2": ("almanak.connectors.silo_v2.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    "euler_v2": ("almanak.connectors.euler_v2.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    "benqi": ("almanak.connectors.benqi.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    # VIB-5030: fToken (ERC-4626) aggregate read — market-scoped on the
    # per-underlying fToken (Compound V3 / Silo V2 shape).
    "fluid": ("almanak.connectors.fluid.lending_read", "ACCOUNT_STATE_READ_SPEC"),
    # VIB-5031: the vault NFT-CDP positionsByUser read — its own manifest
    # slot (the registry holds one account_state per manifest name).
    "fluid_vault": ("almanak.connectors._fluid_core.vault_lending_read", "ACCOUNT_STATE_READ_SPEC"),
}
FROZEN_LENDING_MARKET_HEALTH_LOADERS = {
    "compound_v3": ("almanak.connectors.compound_v3.lending_read", "read_compound_v3_market_health"),
}
FROZEN_LENDING_MARKET_TABLE_LOADERS = {
    "morpho_blue": ("almanak.connectors.morpho_blue.addresses", "MORPHO_MARKETS"),
    "compound_v3": ("almanak.connectors.compound_v3.addresses", "COMPOUND_V3_ACCOUNT_STATE_MARKETS"),
    "silo_v2": ("almanak.connectors.silo_v2.lending_read", "SILO_V2_ACCOUNT_STATE_MARKETS"),
    "euler_v2": ("almanak.connectors.euler_v2.lending_read", "EULER_V2_ACCOUNT_STATE_MARKETS"),
    "benqi": ("almanak.connectors.benqi.lending_read", "BENQI_ACCOUNT_STATE_MARKETS"),
    # VIB-5030: per-chain underlying-symbol -> fToken catalogue.
    "fluid": ("almanak.connectors.fluid.lending_read", "FLUID_FTOKEN_MARKETS"),
    # VIB-5031: the pinned type-1 vault universe (vault address -> params).
    "fluid_vault": ("almanak.connectors._fluid_core.addresses", "FLUID_VAULT_MARKETS"),
}
FROZEN_LENDING_BACKTEST_PROVIDER_LOADERS = {
    "aave_v3": ("almanak.connectors.aave_v3.backtest_apy", "AaveV3APYProvider"),
    "spark": ("almanak.connectors.spark.backtest_apy", "SparkAPYProvider"),
    "morpho_blue": ("almanak.connectors.morpho_blue.backtest_apy", "MorphoBlueAPYProvider"),
    "compound_v3": ("almanak.connectors.compound_v3.backtest_apy", "CompoundV3APYProvider"),
}
# B3 (VIB-4851) deliberately WIDENED the lending aliases beyond the legacy
# registry table: the spellings previously private to
# ``position_health._normalize_protocol`` now resolve in every registry
# consumer. Hyphenated variants ("aave-v3", "morpho-blue", "compound-v3") are
# NOT aliases — ``_normalize`` folds hyphens before the alias map is consulted.
FROZEN_LENDING_ALIASES = {
    "aave": "aave_v3",
    "aavev3": "aave_v3",
    "morpho": "morpho_blue",
    "morphoblue": "morpho_blue",
    "comet": "compound_v3",
    "compound": "compound_v3",
    "compoundv3": "compound_v3",
    # VIB-5030: the platform spec emits ``protocol: "fluid_lending"``; the
    # accounting boundary canonicalizes it through this lending-scoped alias
    # (gate + position-key parity — see lending_accounting / position_events).
    "fluid_lending": "fluid",
}

# almanak/connectors/_strategy_base/perps_read_registry.py tables as of
# 2026-06-10, frozen verbatim.
FROZEN_PERPS_SPEC_LOADERS = {
    "gmx_v2": ("almanak.connectors.gmx_v2.perps_read", "PERPS_READ_SPEC"),
    "aster_perps": ("almanak.connectors.aster_perps.perps_read", "PERPS_READ_SPEC"),
    "hyperliquid": ("almanak.connectors.hyperliquid.perps_read", "PERPS_READ_SPEC"),
}
# B3 (VIB-4851) added "gmx" (previously a local tuple in the backtesting
# funding-rate dispatch) as a manifest-declared perps alias.
FROZEN_PERPS_ALIASES = {"pancakeswap_perps": "aster_perps", "gmx": "gmx_v2"}


def test_lending_read_dispatch_equals_frozen_legacy_tables() -> None:
    """Manifest-derived lending dispatch == the five legacy hardcoded tables."""
    dispatch = LendingReadRegistry._dispatch()
    assert dispatch.spec_loaders == FROZEN_LENDING_SPEC_LOADERS
    assert dispatch.account_state_loaders == FROZEN_LENDING_ACCOUNT_STATE_LOADERS
    assert dispatch.market_health_loaders == FROZEN_LENDING_MARKET_HEALTH_LOADERS
    assert dispatch.market_table_loaders == FROZEN_LENDING_MARKET_TABLE_LOADERS
    assert dispatch.backtest_provider_loaders == FROZEN_LENDING_BACKTEST_PROVIDER_LOADERS
    assert dispatch.aliases == FROZEN_LENDING_ALIASES


def test_lending_backtest_provider_accessor_loads_connector_classes() -> None:
    """Manifest ImportRefs lazily resolve the connector-owned APY providers."""
    from almanak.connectors.aave_v3.backtest_apy import AaveV3APYProvider
    from almanak.connectors.compound_v3.backtest_apy import CompoundV3APYProvider
    from almanak.connectors.morpho_blue.backtest_apy import MorphoBlueAPYProvider
    from almanak.connectors.spark.backtest_apy import SparkAPYProvider

    LendingReadRegistry.reset_cache()
    expected = {
        "aave_v3": AaveV3APYProvider,
        "aave": AaveV3APYProvider,
        "compound_v3": CompoundV3APYProvider,
        "compound": CompoundV3APYProvider,
        "morpho_blue": MorphoBlueAPYProvider,
        "morpho": MorphoBlueAPYProvider,
        "spark": SparkAPYProvider,
    }
    for protocol, provider_cls in expected.items():
        assert LendingReadRegistry.backtest_provider(protocol) is provider_cls
    assert LendingReadRegistry.backtest_provider("unknown") is None


def test_perps_read_dispatch_equals_frozen_legacy_tables() -> None:
    """Manifest-derived perps dispatch == the legacy hardcoded tables."""
    assert PerpsReadRegistry._spec_loaders() == FROZEN_PERPS_SPEC_LOADERS
    assert PerpsReadRegistry._aliases() == FROZEN_PERPS_ALIASES


# almanak/framework/backtesting/pnl/providers/funding_rates.py legacy tables as
# of 2026-06-10, frozen verbatim (VIB-4851 Phase D / D1):
#   SUPPORTED_PROTOCOLS = ["gmx", "gmx_v2", "hyperliquid"]
#   chain ctor validation = GMX_STATS_API.keys() = {"arbitrum", "avalanche"}
FROZEN_FUNDING_SUPPORTED_PROTOCOLS = ("gmx", "gmx_v2", "hyperliquid")
FROZEN_FUNDING_VENUES = {"gmx_v2": "gmx_v2", "hyperliquid": "hyperliquid"}
FROZEN_FUNDING_ALIASES = {"gmx": "gmx_v2"}
FROZEN_FUNDING_CHAINS = {"gmx_v2": ("arbitrum", "avalanche"), "hyperliquid": ()}


def test_funding_history_dispatch_equals_frozen_legacy_tables() -> None:
    """Manifest-derived funding dispatch == the legacy hardcoded tables."""
    assert FundingHistoryRegistry.supported_protocols() == FROZEN_FUNDING_SUPPORTED_PROTOCOLS
    assert FundingHistoryRegistry._venues() == FROZEN_FUNDING_VENUES
    assert FundingHistoryRegistry._aliases() == FROZEN_FUNDING_ALIASES
    assert FundingHistoryRegistry._chains() == FROZEN_FUNDING_CHAINS
    assert FundingHistoryRegistry.all_declared_chains() == frozenset({"arbitrum", "avalanche"})


def _frozen_keys_resolve(registry_canonical, frozen_map: dict[str, str]) -> None:
    """Assert every frozen lookup key resolves to its frozen target."""
    for key, want in frozen_map.items():
        assert registry_canonical(key) == want, (key, registry_canonical(key))


# almanak/framework/backtesting/pnl/fee_models/__init__.py registration block
# as of 2026-06-10, frozen verbatim (VIB-4851 Phase D / D2): every legacy
# lookup key -> (primary registry name, model class name).
FROZEN_FEE_MODEL_LOOKUPS = {
    "uniswap_v3": ("uniswap_v3", "UniswapV3FeeModel"),
    "uniswap": ("uniswap_v3", "UniswapV3FeeModel"),
    "uni_v3": ("uniswap_v3", "UniswapV3FeeModel"),
    "pancakeswap_v3": ("pancakeswap_v3", "PancakeSwapV3FeeModel"),
    "pancakeswap": ("pancakeswap_v3", "PancakeSwapV3FeeModel"),
    "pancake_v3": ("pancakeswap_v3", "PancakeSwapV3FeeModel"),
    "pcs_v3": ("pancakeswap_v3", "PancakeSwapV3FeeModel"),
    "aerodrome": ("aerodrome", "AerodromeFeeModel"),
    "aero": ("aerodrome", "AerodromeFeeModel"),
    "velodrome": ("aerodrome", "AerodromeFeeModel"),
    "curve": ("curve", "CurveFeeModel"),
    "curve_fi": ("curve", "CurveFeeModel"),
    "crv": ("curve", "CurveFeeModel"),
    "aave_v3": ("aave_v3", "AaveV3FeeModel"),
    "aave": ("aave_v3", "AaveV3FeeModel"),
    "aave_v2": ("aave_v3", "AaveV3FeeModel"),
    "morpho": ("morpho", "MorphoFeeModel"),
    "morpho_blue": ("morpho", "MorphoFeeModel"),
    "morpho_optimizer": ("morpho", "MorphoFeeModel"),
    "compound_v3": ("compound_v3", "CompoundV3FeeModel"),
    "compound": ("compound_v3", "CompoundV3FeeModel"),
    "comet": ("compound_v3", "CompoundV3FeeModel"),
    "gmx": ("gmx", "GMXFeeModel"),
    "gmx_v2": ("gmx", "GMXFeeModel"),
    "hyperliquid": ("hyperliquid", "HyperliquidFeeModel"),
    "hl": ("hyperliquid", "HyperliquidFeeModel"),
    "hyper": ("hyperliquid", "HyperliquidFeeModel"),
}
FROZEN_FEE_MODEL_PRIMARY_NAMES = [
    "aave_v3",
    "aerodrome",
    "compound_v3",
    "curve",
    "gmx",
    "hyperliquid",
    "morpho",
    "pancakeswap_v3",
    "uniswap_v3",
]


def test_fee_model_registry_equals_frozen_legacy_registrations() -> None:
    """Manifest-derived fee-model lookups == the legacy registration block."""
    from almanak.framework.backtesting.pnl.fee_models.base import FeeModelRegistry

    for key, (primary, class_name) in FROZEN_FEE_MODEL_LOOKUPS.items():
        metadata = FeeModelRegistry.get_metadata(key)
        assert metadata is not None, key
        assert metadata.name == primary, (key, metadata.name)
        assert metadata.model_class.__name__ == class_name, (key, metadata.model_class.__name__)
    assert FeeModelRegistry.list_protocols() == FROZEN_FEE_MODEL_PRIMARY_NAMES


# almanak/framework/backtesting/pnl/providers/multi_dex_volume.py +
# liquidity_depth.py legacy tables as of 2026-06-10, frozen verbatim
# (VIB-4851 Phase D / D3). Deliberate widening, acknowledged here: the
# connector folder name "balancer_v2" now also resolves to "balancer"
# (previously only "balancer"/"bal" resolved).
FROZEN_DEX_VOLUME_LOOKUPS = {
    # STRING_PROTOCOL_MAP keys
    "uniswap_v3": "uniswap_v3",
    "sushiswap_v3": "sushiswap_v3",
    "pancakeswap_v3": "pancakeswap_v3",
    "aerodrome": "aerodrome",
    "traderjoe_v2": "traderjoe_v2",
    "curve": "curve",
    "balancer": "balancer",
    "uni_v3": "uniswap_v3",
    "sushi_v3": "sushiswap_v3",
    "pancake_v3": "pancakeswap_v3",
    "joe_v2": "traderjoe_v2",
    "bal": "balancer",
    "crv": "curve",
    # PROTOCOL_PROVIDER_MAP keys (Protocol enum values, lowercased)
    "UNISWAP_V3".lower(): "uniswap_v3",
    # Phase D widening (connector folder name)
    "balancer_v2": "balancer",
}
FROZEN_DEX_VOLUME_CHAINS = {
    "uniswap_v3": ("ethereum", "arbitrum", "base", "optimism", "polygon"),
    "sushiswap_v3": ("ethereum",),
    "pancakeswap_v3": ("ethereum", "arbitrum", "bsc", "base"),
    "aerodrome": ("base",),
    "traderjoe_v2": ("avalanche",),
    "curve": ("ethereum", "optimism"),
    "balancer": ("ethereum", "arbitrum", "polygon"),
}
FROZEN_DEX_VOLUME_DATA_SOURCES = {
    "uniswap_v3": "uniswap_v3_subgraph",
    "sushiswap_v3": "sushiswap_v3_subgraph",
    "pancakeswap_v3": "pancakeswap_v3_subgraph",
    "aerodrome": "aerodrome_subgraph",
    "traderjoe_v2": "traderjoe_v2_subgraph",
    "curve": "curve_messari_subgraph",
    "balancer": "balancer_v2_subgraph",
}
FROZEN_DEX_LIQUIDITY_SUBGRAPH_IDS = {
    "uniswap_v3": {
        "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
        "base": "96eJ9Go8gFjySRGnndG7EYxThaiwVDV8BYPp1TMDcoYh",
        "optimism": "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
        "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
    },
    "sushiswap_v3": {"ethereum": "2tGWMrDha4164KkFAfkU3rDCtuxGb4q1emXmFdLLzJ8x"},
    "pancakeswap_v3": {
        "ethereum": "CJYGNhb7RvnhfBDjqpRnD3oxgyhibzc7fkAMa38YV3oS",
        "arbitrum": "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve",
        "bsc": "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",
        "base": "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3",
    },
    "aerodrome": {"base": "GENunSHWLBXm59mBSgPzQ8metBEp9YDfdqwFr91Av1UM"},
    "traderjoe_v2": {"avalanche": "6KD9JYCg2qa3TxNK3tLdhj5zuZTABoLLNcnUZXKG9vuH"},
    "curve": {
        "ethereum": "3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF",
        "optimism": "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX",
    },
    "balancer": {
        "ethereum": "C4ayEZP2yTXRAB8vSaTrgN4m9anTe9Mdm2ViyiAuV9TV",
        "arbitrum": "98cQDy6tufTJtshDCuhh9z2kWXsQWBHVh2bqnLHsGAeS",
        "polygon": "H9oPAbXnobBRq1cB3HDmbZ1E8MWQyJYQjT1QDJMrdbNp",
    },
}
# liquidity_depth.py family lists, frozen verbatim ("balancer" was in
# WEIGHTED_POOL_PROTOCOLS, "curve" in STABLESWAP_PROTOCOLS, ...).
FROZEN_DEX_FAMILIES = {
    "v3_concentrated": ("pancakeswap_v3", "sushiswap_v3", "uniswap_v3"),
    "solidly_v2": ("aerodrome",),
    "liquidity_book": ("traderjoe_v2",),
    "weighted": ("balancer",),
    "stableswap": ("curve",),
}
# multi_dex_volume / liquidity_depth chain-detection defaults.
FROZEN_DEX_CHAIN_DEFAULTS = {"base": "aerodrome", "avalanche": "traderjoe_v2", "arbitrum": "uniswap_v3"}


def test_dex_volume_dispatch_equals_frozen_legacy_tables() -> None:
    """Manifest-derived DEX dispatch == the legacy hardcoded tables."""
    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    _frozen_keys_resolve(DexVolumeRegistry.canonical, FROZEN_DEX_VOLUME_LOOKUPS)
    for key, chains in FROZEN_DEX_VOLUME_CHAINS.items():
        entry = DexVolumeRegistry.entry_for(key)
        assert entry is not None, key
        assert entry.chains == chains, (key, entry.chains)
        assert entry.volume_data_source == FROZEN_DEX_VOLUME_DATA_SOURCES[key]
    for family, protocols in FROZEN_DEX_FAMILIES.items():
        assert DexVolumeRegistry.protocols_by_family(family) == protocols, family
    for chain, want in FROZEN_DEX_CHAIN_DEFAULTS.items():
        assert DexVolumeRegistry.chain_default(chain) == want, chain
    assert DexVolumeRegistry.chain_default("sonic") is None


def test_dex_volume_decls_match_gateway_capability_implementers() -> None:
    """Decl dex keys + chains == the GatewayDexVolumeCapability implementers.

    Two-sources-of-truth guard (Phase D plan DEC-2): the manifest decl is the
    strategy-side truth; the gateway capability is the gateway-side
    implementation. Chains are compared as sets (the capability returns a
    frozenset; the decl preserves declaration order).
    """
    from almanak.connectors._base.gateway_capabilities import GatewayDexVolumeCapability
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    gateway_chains_by_dex = {
        str(provider.dex_name()).lower(): {str(c).lower() for c in provider.volume_supported_chains()}
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayDexVolumeCapability)  # type: ignore[type-abstract]
    }
    decl_chains_by_dex = {
        entry.dex: set(entry.chains)
        for entry in (DexVolumeRegistry.entry_for(p) for p in DexVolumeRegistry.supported_protocols())
        if entry is not None
    }
    assert decl_chains_by_dex == gateway_chains_by_dex


def test_dex_volume_decls_pin_liquidity_subgraph_ids() -> None:
    """Decl liquidity IDs == the deleted legacy per-DEX wrapper tables."""
    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    for key, expected_ids in FROZEN_DEX_LIQUIDITY_SUBGRAPH_IDS.items():
        assert DexVolumeRegistry.liquidity_subgraph_ids_for(key) == expected_ids
        entry = DexVolumeRegistry.entry_for(key)
        assert entry is not None, key
        assert tuple(expected_ids) == entry.chains, (key, expected_ids, entry.chains)
    assert DexVolumeRegistry.liquidity_subgraph_ids_for("balancer_v2") == FROZEN_DEX_LIQUIDITY_SUBGRAPH_IDS["balancer"]
    assert DexVolumeRegistry.liquidity_subgraph_ids_for("unknown") is None


# almanak/framework/data/rates/monitor.py + backtesting/pnl/providers/
# lending_apy.py legacy tables as of 2026-06-10, frozen verbatim (VIB-4851
# Phase D / D5). Deliberate widenings, acknowledged here:
#
# * lending_apy's legacy SUPPORTED_PROTOCOLS was ["aave_v3", "compound_v3"];
#   morpho_blue joined because its gateway rate lane has existed since W7 —
#   the client gate was the only thing excluding it.
# * spark joins (new SparkGatewayConnector on the
#   fork-shared getReserveData pipeline); morpho_blue widens to
#   arbitrum + polygon (MORPHO_MARKETS catalogues markets there and the
#   gateway provider already served every catalogue chain); aave_v3 gains
#   bsc (addresses.py had shipped the bsc pool_data_provider all along).
FROZEN_LENDING_RATE_PROTOCOLS = ("aave_v3", "compound_v3", "morpho_blue", "spark")
FROZEN_LENDING_RATE_CHAINS = {
    "aave_v3": ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bsc"),
    "compound_v3": ("ethereum", "arbitrum", "optimism", "polygon", "base"),
    # VIB-5729: monad + robinhood join for the SAME reason arbitrum + polygon
    # did — MORPHO_MARKETS catalogues markets (with IRM addresses) on both, and
    # the gateway provider derives its servable set from that catalogue, so the
    # live lane already served them while this declaration under-reported them
    # to the CLI support matrix. Widening the DISPLAY truth to match the runtime
    # truth; `MarketSnapshot.lending_rate` never gated on this tuple.
    "morpho_blue": ("ethereum", "base", "arbitrum", "polygon", "monad", "robinhood"),
    "spark": ("ethereum",),
}
# monitor.py PROTOCOL_CHAINS rows (values now sorted; legacy insertion order
# ["aave_v3", "morpho_blue", "compound_v3"] carried no semantics).
FROZEN_LENDING_PROTOCOL_CHAINS = {
    "ethereum": ["aave_v3", "compound_v3", "morpho_blue", "spark"],
    "arbitrum": ["aave_v3", "compound_v3", "morpho_blue"],
    "optimism": ["aave_v3", "compound_v3"],
    "polygon": ["aave_v3", "compound_v3", "morpho_blue"],
    "base": ["aave_v3", "compound_v3", "morpho_blue"],
    "avalanche": ["aave_v3"],
    "bsc": ["aave_v3"],
    # VIB-5729: morpho_blue is the only lending venue with a registered market
    # catalogue on these chains, so it is the only rate-lane provider there.
    "monad": ["morpho_blue"],
    "robinhood": ["morpho_blue"],
}
FROZEN_LENDING_DEFAULT_APYS = {
    "aave_v3": ("0.03", "0.05"),
    "compound_v3": ("0.025", "0.045"),
    # Plan 022 (PR #2762): morpho_blue now declares the APY defaults that
    # InterestCalculator had hardcoded all along, consciously overturning
    # VIB-5040's deliberate omission — see the PR body for the rationale.
    "morpho_blue": ("0.035", "0.04"),
    # Plan 022: values verbatim from the pre-rewire hardcoded interest.py dict.
    "spark": ("0.05", "0.055"),
}


def test_lending_rate_lane_equals_frozen_legacy_tables() -> None:
    """Manifest-derived lending rate lane == the legacy hardcoded tables."""
    assert LendingReadRegistry.rate_history_protocols() == FROZEN_LENDING_RATE_PROTOCOLS
    for protocol, chains in FROZEN_LENDING_RATE_CHAINS.items():
        assert LendingReadRegistry.rate_history_chains(protocol) == chains, protocol
    for chain, protocols in FROZEN_LENDING_PROTOCOL_CHAINS.items():
        assert list(LendingReadRegistry.rate_history_protocols_for_chain(chain)) == protocols, chain
    for protocol, apys in FROZEN_LENDING_DEFAULT_APYS.items():
        assert LendingReadRegistry.backtest_default_apys(protocol) == apys, protocol


def test_lending_rate_chains_subset_of_gateway_capability() -> None:
    """Each declared rate-lane chain set ⊆ the gateway-side servable set.

    Subset (not equality) is the contract: the gateway sets derive from
    address registries and may serve more chains than the framework
    consumers declare; everything declared must be servable.

    Registry-driven drift guard: EVERY connector that
    declares ``rate_history_chains`` must ship a gateway provider whose
    ``lending_supported_chains()`` covers the declaration — keyed by the
    provider's ``protocol`` ClassVar so a new rate-lane connector is
    covered automatically instead of silently skipped by a name map.
    """
    from almanak.connectors._base.gateway_capabilities import (
        GatewayLendingRateHistoryCapability,
    )
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    gateway_chains: dict[str, frozenset[str]] = {
        str(provider.protocol): frozenset(provider.lending_supported_chains())
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayLendingRateHistoryCapability)  # type: ignore[type-abstract]
    }
    rate_lane_protocols = LendingReadRegistry.rate_history_protocols()
    assert rate_lane_protocols == FROZEN_LENDING_RATE_PROTOCOLS
    for protocol in rate_lane_protocols:
        assert protocol in gateway_chains, f"{protocol} declares rate_history_chains but has no gateway rate provider"
        declared = set(LendingReadRegistry.rate_history_chains(protocol))
        assert declared <= gateway_chains[protocol], (protocol, declared - gateway_chains[protocol])


# Connector-owned TWAP / pool-reference catalogue consumed by framework
# compatibility views in backtesting/pnl/providers/twap.py,
# data/price/dex_twap.py, and fee_models/liquidity.py.
FROZEN_TWAP_POOLS = {
    "ethereum": {
        "WETH/USDC-500": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        # EIP-55 case normalization vs the legacy lowercase literal —
        # case-only, non-semantic (test_all_production_addresses_are_eip55).
        "WETH/USDC-3000": "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
        "WETH/USDT-3000": "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
        "WBTC/USDC-3000": "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35",
        "WBTC/WETH-3000": "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
        "USDC/USDT-100": "0x3416cF6C708Da44DB2624D63ea0AAef7113527C6",
        "LINK/WETH-3000": "0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8",
        "UNI/WETH-3000": "0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801",
        "AAVE/WETH-3000": "0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB",
    },
    "arbitrum": {
        "WETH/USDC-500": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        "WETH/USDC-3000": "0xC6962004f452bE9203591991D15f6b388e09E8D0",
        "ARB/USDC-500": "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c",
        "WBTC/WETH-500": "0x2f5e87C9312fa29aed5c179E456625D79015299c",
        "GMX/WETH-3000": "0x80A9ae39310abf666A87C743d6ebBD0E8C42158E",
        "LINK/WETH-3000": "0x468b88941e7Cc0B88c1869d68ab6b570bCEF62Ff",
    },
    "base": {
        "WETH/USDC-500": "0xd0b53D9277642d899DF5C87A3966A349A798F224",
        "CBETH/WETH-500": "0x10648BA41B8565907Cfa1496765fA4D95390aa0d",
    },
    "optimism": {
        "WETH/USDC-500": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9",
        "OP/USDC-3000": "0x1C3140aB59d6cAf9fa7459C6f83D4B52ba881d36",
        "WBTC/WETH-500": "0x73B14a78a0D396C521f954532d43fd5fFe385216",
    },
    "polygon": {
        "WETH/USDC-500": "0x45dDa9cb7c25131DF268515131f647d726f50608",
        "WMATIC/USDC-500": "0xA374094527e1673A86dE625aa59517c5dE346d32",
        "WBTC/WETH-500": "0x50eaEDB835021E4A108B7290636d62E9765cc6d7",
    },
    "avalanche": {
        "WAVAX/USDC-3000": "0xfAe3f424a0a47706811521E3ee268f00cFb5c45E",
    },
}
FROZEN_TWAP_TOKEN_TO_POOL = {
    "ETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
        "polygon": "WETH/USDC-500",
    },
    "WETH": {
        "ethereum": "WETH/USDC-500",
        "arbitrum": "WETH/USDC-500",
        "base": "WETH/USDC-500",
        "optimism": "WETH/USDC-500",
        "polygon": "WETH/USDC-500",
    },
    "BTC": {
        "ethereum": "WBTC/USDC-3000",
        "arbitrum": "WBTC/WETH-500",
        "optimism": "WBTC/WETH-500",
        "polygon": "WBTC/WETH-500",
    },
    "WBTC": {
        "ethereum": "WBTC/USDC-3000",
        "arbitrum": "WBTC/WETH-500",
        "optimism": "WBTC/WETH-500",
        "polygon": "WBTC/WETH-500",
    },
    "LINK": {"ethereum": "LINK/WETH-3000", "arbitrum": "LINK/WETH-3000"},
    "UNI": {"ethereum": "UNI/WETH-3000"},
    "AAVE": {"ethereum": "AAVE/WETH-3000"},
    "ARB": {"arbitrum": "ARB/USDC-500"},
    "GMX": {"arbitrum": "GMX/WETH-3000"},
    "OP": {"optimism": "OP/USDC-3000"},
    "CBETH": {"base": "CBETH/WETH-500"},
    "MATIC": {"polygon": "WMATIC/USDC-500"},
    "WMATIC": {"polygon": "WMATIC/USDC-500"},
    "AVAX": {"avalanche": "WAVAX/USDC-3000"},
    "WAVAX": {"avalanche": "WAVAX/USDC-3000"},
}


def test_twap_reference_pools_equal_frozen_legacy_tables() -> None:
    """Connector-declared TWAP reference tables == the legacy twap.py tables."""
    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    merged = DexVolumeRegistry.twap_reference_pools()
    assert merged["pools"] == FROZEN_TWAP_POOLS
    assert merged["token_to_pool"] == FROZEN_TWAP_TOKEN_TO_POOL


# backtesting/paper/position_queries.py legacy contract tables as of
# 2026-06-10, frozen verbatim (VIB-4851 Phase D / D5, plan DEC-6): the
# runtime now resolves these through AddressRegistry; this test pins the
# registry-derived values to the deleted local copies so a connector-side
# address change is a conscious decision, not silent drift (VIB-4874).
FROZEN_POSITION_QUERY_ADDRESSES = {
    ("uniswap_v3", "position_manager"): {
        "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
        "zerog": "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72",
    },
    ("gmx_v2", "reader"): {"arbitrum": "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"},
    ("gmx_v2", "data_store"): {"arbitrum": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"},
    ("aave_v3", "pool_data_provider"): {
        "ethereum": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        "arbitrum": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "optimism": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "polygon": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "base": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac",
        "avalanche": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    },
}


def test_position_query_addresses_match_connector_tables() -> None:
    """AddressRegistry serves the exact addresses position_queries hardcoded."""
    from almanak.connectors._strategy_base.address_registry import AddressRegistry

    for (protocol, role), per_chain in FROZEN_POSITION_QUERY_ADDRESSES.items():
        for chain, want in per_chain.items():
            got = AddressRegistry.resolve_contract_address(protocol, chain, role)
            assert got == want, (protocol, role, chain, got)


def test_funding_history_venues_match_gateway_capability_implementers() -> None:
    """Decl venues == the GatewayFundingHistoryCapability implementer set.

    Two-sources-of-truth guard (Phase D plan DEC-2): the manifest decl is the
    strategy-side truth, the gateway capability the gateway-side
    implementation. This parity test pins them to each other so neither can
    drift silently.
    """
    from almanak.connectors._base.gateway_capabilities import (
        GatewayFundingHistoryCapability,
    )
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    gateway_venues = {
        provider.funding_venue().lower()
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayFundingHistoryCapability)  # type: ignore[type-abstract]
    }
    assert set(FROZEN_FUNDING_VENUES.values()) == gateway_venues


# almanak/connectors/_strategy_base/{prediction_read,prediction_execute,
# gateway_stub}_registry.py ``_SPEC_LOADERS`` as of 2026-06-10, frozen verbatim.
FROZEN_PREDICTION_READ_LOADERS = {
    "polymarket": ("almanak.connectors.polymarket.prediction_read", "PREDICTION_READ_SPEC"),
}
FROZEN_PREDICTION_EXECUTE_LOADERS = {
    "polymarket": ("almanak.connectors.polymarket.clob_handler", "PREDICTION_EXECUTE_SPEC"),
}
FROZEN_GATEWAY_STUB_LOADERS = {
    "polymarket": ("almanak.connectors.polymarket.gateway_stub", "GATEWAY_STUB_SPEC"),
}


def test_prediction_and_stub_dispatch_equals_frozen_legacy_tables() -> None:
    """Manifest-derived prediction/stub dispatch == the legacy hardcoded tables."""
    assert PredictionReadRegistry._spec_loaders() == FROZEN_PREDICTION_READ_LOADERS
    assert PredictionExecuteRegistry._spec_loaders() == FROZEN_PREDICTION_EXECUTE_LOADERS
    assert GatewayStubRegistry._spec_loaders() == FROZEN_GATEWAY_STUB_LOADERS


# almanak/framework/execution/orchestrator.py amount-encoding frozensets as of
# 2026-06-10, frozen verbatim (VIB-3747; manifest-derived in VIB-4851 C1).
# VIB-5030 added "fluid": its lending compiler emits ERC-4626 asset base
# units (wei) in ``supply_amount`` / ``withdraw_amount``.
FROZEN_WEI_LENDING_PROTOCOLS = frozenset({"aave_v3", "spark", "fluid", "fluid_vault"})
FROZEN_HUMAN_AMOUNT_SWAP_PROTOCOLS = frozenset({"curve", "aerodrome"})


def test_metadata_amount_encoding_equals_frozen_legacy_sets() -> None:
    """Manifest-declared amount encodings == the legacy orchestrator frozensets."""
    declaring = CONNECTOR_REGISTRY.with_metadata_amount_encoding()
    wei_lending = frozenset(
        connector.name
        for connector in declaring
        if connector.metadata_amount_encoding is not None and connector.metadata_amount_encoding.lending == "wei"
    )
    human_swap = frozenset(
        connector.name
        for connector in declaring
        if connector.metadata_amount_encoding is not None and connector.metadata_amount_encoding.swap == "human"
    )
    assert wei_lending == FROZEN_WEI_LENDING_PROTOCOLS
    assert human_swap == FROZEN_HUMAN_AMOUNT_SWAP_PROTOCOLS


# almanak/framework/observability/ledger.py and
# almanak/framework/execution/result_enricher.py protocol carve-outs as of
# 2026-06-10, frozen verbatim (manifest-derived in VIB-4851 C2/C3).
FROZEN_FUNGIBLE_LP_PROTOCOLS = frozenset({"curve", "fluid_dex_lp"})  # VIB-5032 — Fluid SmartLending fungible DEX LP
FROZEN_POOL_KEY_LOOKUP_PROTOCOLS = frozenset({"uniswap_v4"})


def test_fungible_lp_equals_frozen_legacy_set() -> None:
    """Manifest fungible_lp flags == the legacy ledger frozenset."""
    derived = frozenset(connector.name for connector in CONNECTOR_REGISTRY.with_fungible_lp())
    assert derived == FROZEN_FUNGIBLE_LP_PROTOCOLS


def test_pool_key_lookup_kwarg_equals_frozen_legacy_carveout() -> None:
    """Manifest receipt_parser_kwargs declarations == the legacy V4 enricher carve-out."""
    derived = frozenset(
        key
        for connector in CONNECTOR_REGISTRY.all()
        if "pool_key_lookup" in connector.receipt_parser_kwargs
        for key in connector.receipt_parser_keys
    )
    assert derived == FROZEN_POOL_KEY_LOOKUP_PROTOCOLS


def test_twap_reference_pools_rejects_non_dict_table(monkeypatch) -> None:
    """A twap_reference_pools ref resolving to a non-dict fails loud, not empty."""
    from types import SimpleNamespace

    from almanak.connectors._connector import CONNECTOR_REGISTRY, ImportRef
    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    bad_manifest = SimpleNamespace(
        name="fake_dex",
        dex_volume=SimpleNamespace(
            twap_reference_pools=ImportRef(
                module="almanak.connectors._strategy_base.dex_volume_registry",
                attribute="logger",  # importable, but not a dict
            )
        ),
    )
    monkeypatch.setattr(CONNECTOR_REGISTRY, "with_dex_volume", lambda: [bad_manifest])
    with pytest.raises(TypeError, match="fake_dex.*must resolve to a dict"):
        DexVolumeRegistry.twap_reference_pools()


# almanak/framework/backtesting/adapters/registry.py
# ``PROTOCOL_TO_STRATEGY_TYPE`` as of 2026-06-11, frozen verbatim
# (manifest-derived in VIB-4851: connector ``BacktestStrategyTypeDecl`` rows
# plus the ``_NON_CONNECTOR_STRATEGY_TYPES`` residual map).
FROZEN_PROTOCOL_TO_STRATEGY_TYPE = {
    # LP protocols
    "uniswap_v3": "lp",
    "uniswap_v2": "lp",
    "uniswap": "lp",
    "pancakeswap_v3": "lp",
    "pancakeswap": "lp",
    "aerodrome": "lp",
    "velodrome": "lp",
    "traderjoe": "lp",
    "traderjoe_v2": "lp",
    "curve": "lp",
    "balancer": "lp",
    "sushiswap": "lp",
    # Perp protocols
    "gmx_v2": "perp",
    "gmx": "perp",
    "hyperliquid": "perp",
    "dydx": "perp",
    "perpetual_protocol": "perp",
    # Lending protocols
    "aave_v3": "lending",
    "aave": "lending",
    "compound_v3": "lending",
    "compound": "lending",
    "morpho_blue": "lending",
    "morpho": "lending",
    "spark": "lending",
    # Yield protocols
    "lido": "yield",
    "ethena": "yield",
    "yearn": "yield",
    "convex": "yield",
}


def test_protocol_strategy_type_equals_frozen_legacy_dict() -> None:
    """Manifest-derived strategy-type detection map == the legacy hand literal."""
    from almanak.framework.backtesting.adapters.registry import PROTOCOL_TO_STRATEGY_TYPE

    assert PROTOCOL_TO_STRATEGY_TYPE == FROZEN_PROTOCOL_TO_STRATEGY_TYPE


def test_backtest_strategy_type_values_are_known_framework_types() -> None:
    """Every manifest-declared strategy type is in the framework vocabulary.

    Pins the descriptor-side ``_BACKTEST_STRATEGY_TYPES`` literal (the
    descriptor stays strategy-safe and cannot import the framework set) to the
    framework's ``KNOWN_STRATEGY_TYPES``.
    """
    from almanak.connectors._connector_descriptor import _BACKTEST_STRATEGY_TYPES
    from almanak.framework.backtesting.adapters.registry import KNOWN_STRATEGY_TYPES

    assert frozenset(_BACKTEST_STRATEGY_TYPES) == KNOWN_STRATEGY_TYPES
    for connector in CONNECTOR_REGISTRY.with_backtest_strategy_type():
        decl = connector.backtest_strategy_type
        assert decl is not None
        assert decl.strategy_type in KNOWN_STRATEGY_TYPES, (connector.name, decl.strategy_type)


def test_non_connector_strategy_type_residue_stays_connectorless() -> None:
    """Residual strategy-type keys must not collide with any discovered connector.

    The residual map is reserved for venues with NO connector package; the
    moment a connector claims one of these keys (canonical name, discovery
    alias, or backtest decl key), the residual entry must move onto that
    connector's manifest. This makes ``_NON_CONNECTOR_STRATEGY_TYPES`` shrink
    automatically as connectors appear.
    """
    from almanak.framework.backtesting.adapters.registry import _NON_CONNECTOR_STRATEGY_TYPES

    for connector in CONNECTOR_REGISTRY.all():
        decl = connector.backtest_strategy_type
        decl_keys = frozenset() if decl is None else frozenset((decl.name or connector.name, *decl.aliases))
        for residual_key in _NON_CONNECTOR_STRATEGY_TYPES:
            assert residual_key not in connector.discovery_keys, (
                f"Residual strategy-type key {residual_key!r} is a discovery key of connector "
                f"{connector.name!r}; move the entry onto that connector's backtest_strategy_type decl."
            )
            assert residual_key not in decl_keys, (
                f"Residual strategy-type key {residual_key!r} is already declared by connector "
                f"{connector.name!r}'s backtest_strategy_type decl; delete the residual entry."
            )
