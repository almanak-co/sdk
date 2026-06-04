"""Strategy-side agent-tool registration site (VIB-4860 / W8).

Sibling of :mod:`almanak.connectors._strategy_gas_estimate_registry` (W6)
and :mod:`almanak.connectors._strategy_receipt_registry` (W2), scoped to
the agent-tool read-descriptor + vault-tool concern.

Lives one level up from ``_strategy_base/`` because it imports every
agent-callable connector's descriptor, and ``_strategy_base/`` must stay
protocol-clean (no concrete connector imports). Provider classes are loaded
from connector-owned lazy import references.

Connectors that publish ``almanak/connectors/<protocol>/connector.py`` with
``CONNECTOR.agent_read_connector`` / ``CONNECTOR.agent_read_connectors`` or
``CONNECTOR.vault_tool_connector`` / ``CONNECTOR.vault_tool_connectors`` import
references are registered from that connector object.

The agent-tool executor
(``almanak/framework/agent_tools/executor.py``) imports the populated
registries from here so its read / vault handlers no longer import any
``almanak.connectors.<protocol>`` module directly (the W8 goal — see plan
§6 + the ``rg`` acceptance checks in §10).

Why a strategy-side registry (vs. reading from ``GATEWAY_REGISTRY``)
====================================================================

The agent-tool executor runs strategy-side / operator-side (``ax`` CLI,
MCP server) — never inside the gateway sidecar (``rg`` confirms
``almanak/gateway/`` imports zero ``agent_tools`` modules). Strategy-side
modules are forbidden from importing the gateway-side registry
(``almanak.connectors._gateway_registry``) per
``tests/static/test_strategy_import_boundary.py``, so the agent-tool
dispatch consumes this strategy-side mirror instead.

This file is allow-listed in the strategy-side import boundary scan the
same way ``_strategy_gas_estimate_registry.py`` is: it is the boot-time
discovery entry point. It no longer knows connector names.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY, ImportRef
from almanak.connectors._strategy_base.agent_read_registry import (
    STRATEGY_AGENT_READ_REGISTRY,
)
from almanak.connectors._strategy_base.vault_tool_registry import (
    STRATEGY_VAULT_TOOL_REGISTRY,
)

__all__ = [
    "STRATEGY_AGENT_READ_REGISTRY",
    "STRATEGY_VAULT_TOOL_REGISTRY",
]


def _ordered_refs(refs: list[ImportRef]) -> list[ImportRef]:
    """Return import refs in explicit order, with unordered refs last."""
    return sorted(refs, key=lambda ref: (ref.order is None, ref.order if ref.order is not None else 0))


def _register_discovered_agent_reads() -> None:
    """Register agent-read connectors published by connector manifests."""
    refs: list[ImportRef] = []
    for connector_manifest in CONNECTOR_REGISTRY.with_agent_read():
        refs.extend(connector_manifest.agent_read_connector_refs)

    for import_ref in _ordered_refs(refs):
        STRATEGY_AGENT_READ_REGISTRY.register(import_ref.instantiate())


def _register_discovered_vault_tools() -> None:
    """Register vault-tool connectors published by connector manifests."""
    refs: list[ImportRef] = []
    for connector_manifest in CONNECTOR_REGISTRY.with_vault_tool():
        refs.extend(connector_manifest.vault_tool_connector_refs)

    for import_ref in _ordered_refs(refs):
        STRATEGY_VAULT_TOOL_REGISTRY.register(import_ref.instantiate())


def _register_all() -> None:
    """Register every strategy-side agent-read + vault-tool connector.

    Descriptor-backed connectors are discovered here. Import targets are
    stored as strings on each connector descriptor so loading this module
    does not transitively import every provider module's class until the
    registries bootstrap.
    """
    _register_discovered_agent_reads()
    _register_discovered_vault_tools()


_register_all()
