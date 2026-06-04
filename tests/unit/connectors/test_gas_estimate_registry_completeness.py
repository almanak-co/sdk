"""Guard test: every gas-estimate provider must be descriptor registered.

The gas-estimate registry is now populated from connector-owned manifests.
This test catches the old failure mode where a connector adds
``gas_estimate_provider.py`` but forgets to publish it from its
``connector.py`` manifest, leaving ``get_gas_estimate`` unable to route the
provider's action keys.
"""

from pathlib import Path

from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateCapability,
)
from almanak.connectors._strategy_gas_estimate_registry import (
    STRATEGY_GAS_ESTIMATE_REGISTRY,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"


def _discover_gas_estimate_providers() -> list[tuple[str, ...]]:
    """Find connector paths that contain a gas-estimate provider module."""
    providers: list[tuple[str, ...]] = []
    for provider_file in sorted(CONNECTORS_DIR.rglob("gas_estimate_provider.py")):
        rel_parts = provider_file.relative_to(CONNECTORS_DIR).parts[:-1]
        if not rel_parts or any(part.startswith("_") for part in rel_parts):
            continue
        providers.append(tuple(rel_parts))
    return providers


def _registered_provider_modules() -> set[str]:
    """Return gas-estimate provider module paths registered into the registry."""
    return {
        type(connector).__module__
        for connector in STRATEGY_GAS_ESTIMATE_REGISTRY.all()
        if isinstance(connector, GasEstimateCapability)
    }


def test_all_gas_estimate_providers_are_descriptor_registered() -> None:
    """Every ``gas_estimate_provider.py`` must be registered via ``connector.py``."""
    gas_estimate_providers = _discover_gas_estimate_providers()
    registered_provider_modules = _registered_provider_modules()
    missing: list[str] = []
    for connector_parts in gas_estimate_providers:
        suffix = ".".join(connector_parts)
        expected_provider_module = f"almanak.connectors.{suffix}.gas_estimate_provider"
        if expected_provider_module not in registered_provider_modules:
            connector_id = "/".join(connector_parts)
            missing.append(
                f"  {connector_id} has gas_estimate_provider.py but {expected_provider_module} is not registered"
            )

    assert not missing, (
        "Gas-estimate providers missing from STRATEGY_GAS_ESTIMATE_REGISTRY. "
        "Publish each provider from its connector manifest as "
        "CONNECTOR.gas_estimate_connector:\n" + "\n".join(missing)
    )


def test_no_stale_gas_estimate_registry_entries() -> None:
    """Every registered gas-estimate provider must have a sibling provider file."""
    stale: list[str] = []
    expected_prefix = ("almanak", "connectors")
    for connector in STRATEGY_GAS_ESTIMATE_REGISTRY.all():
        if not isinstance(connector, GasEstimateCapability):
            continue
        provider_module = type(connector).__module__
        cls_name = type(connector).__name__
        parts = provider_module.split(".")
        if len(parts) < 4 or tuple(parts[:2]) != expected_prefix or parts[-1] != "gas_estimate_provider":
            stale.append(f"  {connector.protocol} -> invalid module path format: {provider_module}::{cls_name}")
            continue

        connector_dir = CONNECTORS_DIR.joinpath(*parts[2:-1])
        provider_file = connector_dir / "gas_estimate_provider.py"
        if not provider_file.is_file():
            stale.append(
                f"  {connector.protocol} -> {provider_module}::{cls_name} (no sibling gas_estimate_provider.py)"
            )

    assert not stale, (
        "Stale entries in STRATEGY_GAS_ESTIMATE_REGISTRY (provider modules no longer exist):\n" + "\n".join(stale)
    )


def test_discovery_finds_gas_estimate_providers() -> None:
    """Sanity check: discovery should find the current provider modules."""
    connector_ids = {"/".join(parts) for parts in _discover_gas_estimate_providers()}
    assert {"aave_v3", "across", "balancer_v2", "morpho_vault", "uniswap_v3"} <= connector_ids
