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
