"""Guard test: every agent-tool provider must be descriptor registered.

The agent-tool registries are populated from connector-owned manifests.
This test catches the old failure mode where a connector adds an
``agent_read_provider.py`` or ``vault_tool_provider.py`` file but forgets to
publish it from its ``connector.py`` manifest.
"""

from pathlib import Path

from almanak.connectors._strategy_agent_tool_registry import (
    STRATEGY_AGENT_READ_REGISTRY,
    STRATEGY_VAULT_TOOL_REGISTRY,
)
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
)
from almanak.connectors._strategy_base.vault_tool_registry import (
    VaultToolCapability,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"


def _discover_provider_modules(filename: str) -> list[str]:
    """Find connector provider modules by filename."""
    modules: list[str] = []
    for provider_file in sorted(CONNECTORS_DIR.rglob(filename)):
        rel_parts = provider_file.relative_to(CONNECTORS_DIR).parts[:-1]
        if not rel_parts or any(part.startswith("_") for part in rel_parts):
            continue
        module_suffix = ".".join((*rel_parts, provider_file.stem))
        modules.append(f"almanak.connectors.{module_suffix}")
    return modules


def _registered_agent_read_modules() -> set[str]:
    """Return agent-read provider module paths registered into the registry."""
    return {
        type(connector).__module__
        for connector in STRATEGY_AGENT_READ_REGISTRY.all()
        if isinstance(connector, AgentReadCapability)
    }


def _registered_vault_tool_modules() -> set[str]:
    """Return vault-tool provider module paths registered into the registry."""
    return {
        type(connector).__module__
        for connector in STRATEGY_VAULT_TOOL_REGISTRY.all()
        if isinstance(connector, VaultToolCapability)
    }


AGENT_READ_PROVIDER_MODULES = _discover_provider_modules("agent_read_provider.py")
VAULT_TOOL_PROVIDER_MODULES = _discover_provider_modules("vault_tool_provider.py")


def test_all_agent_read_providers_are_descriptor_registered() -> None:
    """Every ``agent_read_provider.py`` must be registered via ``connector.py``."""
    registered = _registered_agent_read_modules()
    missing = sorted(module for module in AGENT_READ_PROVIDER_MODULES if module not in registered)
    assert not missing, (
        "Agent-read providers missing from STRATEGY_AGENT_READ_REGISTRY. "
        "Publish each provider from its connector manifest as "
        "CONNECTOR.agent_read_connector or CONNECTOR.agent_read_connectors:\n  "
        + "\n  ".join(missing)
    )


def test_all_vault_tool_providers_are_descriptor_registered() -> None:
    """Every ``vault_tool_provider.py`` must be registered via ``connector.py``."""
    registered = _registered_vault_tool_modules()
    missing = sorted(module for module in VAULT_TOOL_PROVIDER_MODULES if module not in registered)
    assert not missing, (
        "Vault-tool providers missing from STRATEGY_VAULT_TOOL_REGISTRY. "
        "Publish each provider from its connector manifest as "
        "CONNECTOR.vault_tool_connector or CONNECTOR.vault_tool_connectors:\n  "
        + "\n  ".join(missing)
    )


def test_no_stale_agent_tool_registry_entries() -> None:
    """Every registered agent-tool provider must have a sibling provider file."""
    expected_agent_modules = set(AGENT_READ_PROVIDER_MODULES)
    expected_vault_modules = set(VAULT_TOOL_PROVIDER_MODULES)
    stale: list[str] = []

    for connector in STRATEGY_AGENT_READ_REGISTRY.all():
        if not isinstance(connector, AgentReadCapability):
            continue
        module = type(connector).__module__
        if module not in expected_agent_modules:
            stale.append(f"  agent-read {connector.protocol} -> {module}")

    for connector in STRATEGY_VAULT_TOOL_REGISTRY.all():
        if not isinstance(connector, VaultToolCapability):
            continue
        module = type(connector).__module__
        if module not in expected_vault_modules:
            stale.append(f"  vault-tool {connector.protocol} -> {module}")

    assert not stale, (
        "Stale entries in agent-tool registries "
        "(provider modules no longer exist):\n" + "\n".join(stale)
    )


def test_discovery_finds_agent_tool_providers() -> None:
    """Sanity check: discovery should find the current provider modules."""
    assert {
        "almanak.connectors.aave_v3.agent_read_provider",
        "almanak.connectors.aerodrome.agent_read_provider",
        "almanak.connectors.pancakeswap_v3.agent_read_provider",
        "almanak.connectors.sushiswap_v3.agent_read_provider",
        "almanak.connectors.uniswap_v3.agent_read_provider",
    } <= set(AGENT_READ_PROVIDER_MODULES)
    assert {"almanak.connectors.lagoon.vault_tool_provider"} <= set(VAULT_TOOL_PROVIDER_MODULES)
