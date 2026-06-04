"""Vault connector capability resolution for framework runtime paths."""

from __future__ import annotations

from almanak.connectors._strategy_base.vault_tool_registry import VaultToolCapability


class VaultToolCapabilityError(RuntimeError):
    """Vault connector capability lookup failed."""


def default_vault_protocol() -> str:
    """Return the only registered lifecycle-vault protocol.

    Runtime vault auto-deploy is intentionally single-protocol today. When a
    second lifecycle-managed vault connector lands, callers must choose from
    config instead of relying on this default.
    """
    from almanak.connectors._strategy_agent_tool_registry import STRATEGY_VAULT_TOOL_REGISTRY

    protocols = sorted(str(protocol) for protocol in STRATEGY_VAULT_TOOL_REGISTRY.protocols())
    if len(protocols) != 1:
        raise VaultToolCapabilityError(
            "default vault protocol is ambiguous; expected exactly one registered "
            f"lifecycle-vault connector, found {protocols!r}"
        )
    return protocols[0]


def get_vault_tool_capability(protocol: str | None = None) -> VaultToolCapability:
    """Resolve a vault connector construction capability by protocol key."""
    from almanak.connectors._strategy_agent_tool_registry import STRATEGY_VAULT_TOOL_REGISTRY

    protocol = protocol or default_vault_protocol()
    cap = STRATEGY_VAULT_TOOL_REGISTRY.lookup(protocol)
    if cap is None:
        raise VaultToolCapabilityError(
            f"vault connector {protocol!r} not registered in STRATEGY_VAULT_TOOL_REGISTRY (boot-time wiring bug)"
        )
    return cap


__all__ = ["VaultToolCapabilityError", "default_vault_protocol", "get_vault_tool_capability"]
