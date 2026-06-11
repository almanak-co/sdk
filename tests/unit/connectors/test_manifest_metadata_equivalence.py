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
}

# almanak/connectors/_strategy_base/perps_read_registry.py tables as of
# 2026-06-10, frozen verbatim.
FROZEN_PERPS_SPEC_LOADERS = {
    "gmx_v2": ("almanak.connectors.gmx_v2.perps_read", "PERPS_READ_SPEC"),
    "aster_perps": ("almanak.connectors.aster_perps.perps_read", "PERPS_READ_SPEC"),
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
    assert dispatch.aliases == FROZEN_LENDING_ALIASES


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


def test_dex_volume_decls_match_wrapper_provider_chains() -> None:
    """Decl chains == the legacy per-DEX wrapper SUPPORTED_CHAINS tables.

    The wrapper modules keep their client-side subgraph-ID tables until the
    liquidity-depth gateway lane lands; this parity test stops the two chain
    vocabularies drifting in the meantime.
    """
    import importlib

    from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

    wrapper_modules = {
        "uniswap_v3": "uniswap_v3_volume",
        "sushiswap_v3": "sushiswap_v3_volume",
        "pancakeswap_v3": "pancakeswap_v3_volume",
        "aerodrome": "aerodrome_volume",
        "traderjoe_v2": "traderjoe_v2_volume",
        "curve": "curve_volume",
        "balancer": "balancer_volume",
    }
    for key, module_name in wrapper_modules.items():
        module = importlib.import_module(f"almanak.framework.backtesting.pnl.providers.dex.{module_name}")
        wrapper_chains = [c.value.lower() for c in module.SUPPORTED_CHAINS]
        entry = DexVolumeRegistry.entry_for(key)
        assert entry is not None, key
        assert list(entry.chains) == wrapper_chains, (key, entry.chains, wrapper_chains)


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
FROZEN_WEI_LENDING_PROTOCOLS = frozenset({"aave_v3", "spark"})
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
FROZEN_FUNGIBLE_LP_PROTOCOLS = frozenset({"curve"})
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
