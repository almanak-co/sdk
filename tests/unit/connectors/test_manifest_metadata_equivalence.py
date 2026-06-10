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
