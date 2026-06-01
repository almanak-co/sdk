"""Strategy-side agent-tool registration site (VIB-4860 / W8).

Sibling of :mod:`almanak.connectors._strategy_gas_estimate_registry` (W6)
and :mod:`almanak.connectors._strategy_receipt_registry` (W2), scoped to
the agent-tool read-descriptor + vault-tool concern.

Lives one level up from ``_strategy_base/`` because it imports every
agent-callable connector's ``agent_read_provider`` /
``vault_tool_provider`` module — and ``_strategy_base/`` must stay
protocol-clean (no concrete connector imports). Adding a new agent-callable
connector means one import + one ``register`` line below.

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
discovery entry point that legitimately knows every connector by name. (It
imports only connector *strategy-side* modules — addresses / adapter /
sdk — never ``<protocol>/gateway/``, so it actually passes the scan as-is.)
"""

from __future__ import annotations

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


def _register_all() -> None:
    """Register every strategy-side agent-read + vault-tool connector.

    Imports are local to the function so loading this module does not
    transitively import every provider module's class until the registries
    are first consumed. Mirrors the W6 ``_strategy_gas_estimate_registry``
    convention.
    """
    # ── CL-DEX pool/LP read descriptors ──────────────────────────────────
    # Uniswap V3 publishes both the canonical ``uniswap_v3`` row and its
    # ``agni_finance`` Mantle-fork alias (the Agni address tables live inside
    # the Uniswap V3 connector).
    from almanak.connectors.uniswap_v3.agent_read_provider import (
        AgniFinanceAgentReadConnector,
        UniswapV3AgentReadConnector,
    )

    STRATEGY_AGENT_READ_REGISTRY.register(UniswapV3AgentReadConnector())
    STRATEGY_AGENT_READ_REGISTRY.register(AgniFinanceAgentReadConnector())

    # ── Aerodrome Slipstream (concentrated-liquidity) read descriptors ────
    # Registered under the ``aerodrome_slipstream`` canonical slug — the int24
    # tick-spacing getPool variant the pre-W8 LP handler special-cased.
    from almanak.connectors.aerodrome.agent_read_provider import (
        AerodromeSlipstreamAgentReadConnector,
    )

    STRATEGY_AGENT_READ_REGISTRY.register(AerodromeSlipstreamAgentReadConnector())

    # ── Uniswap V3 forks (uint24-fee getPool, shared decode) ──────────────
    from almanak.connectors.pancakeswap_v3.agent_read_provider import (
        PancakeswapV3AgentReadConnector,
    )
    from almanak.connectors.sushiswap_v3.agent_read_provider import (
        SushiswapV3AgentReadConnector,
    )

    STRATEGY_AGENT_READ_REGISTRY.register(PancakeswapV3AgentReadConnector())
    STRATEGY_AGENT_READ_REGISTRY.register(SushiswapV3AgentReadConnector())

    # ── Lending account-data read descriptors ────────────────────────────
    from almanak.connectors.aave_v3.agent_read_provider import (
        AaveV3AgentReadConnector,
    )

    STRATEGY_AGENT_READ_REGISTRY.register(AaveV3AgentReadConnector())

    # ── Vault-tool construction factories ─────────────────────────────────
    # Lagoon is the only vault connector today; it backs the deploy/settle/
    # state/approve/deposit/teardown vault tools. Routing its SDK/deployer/
    # adapter construction through the registry keeps the executor free of
    # connector imports.
    from almanak.connectors.lagoon.vault_tool_provider import (
        LagoonVaultToolConnector,
    )

    STRATEGY_VAULT_TOOL_REGISTRY.register(LagoonVaultToolConnector())


_register_all()
