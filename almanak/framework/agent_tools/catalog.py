"""Tool catalog -- registry of all agent-callable tools with metadata.

The catalog is the single source of truth for tool names, descriptions,
risk tiers, and schema references. Adapters (MCP, OpenAI, LangChain) read
from this catalog to generate framework-specific tool definitions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from almanak.framework.agent_tools import schemas

if TYPE_CHECKING:
    from pydantic import BaseModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ToolCategory(StrEnum):
    """Logical grouping for tools."""

    DATA = "data"
    PLANNING = "planning"
    ACTION = "action"
    STATE = "state"


class RiskTier(StrEnum):
    """Risk classification that determines policy behaviour."""

    NONE = "none"  # Read-only, no side-effects
    LOW = "low"  # State writes, no financial impact
    MEDIUM = "medium"  # Financial action below threshold
    HIGH = "high"  # Financial action above threshold


class LatencyClass(StrEnum):
    """Expected latency bucket for documentation and budgeting."""

    FAST = "fast"  # < 500 ms
    MEDIUM = "medium"  # 500 ms - 5 s
    SLOW = "slow"  # > 5 s (includes on-chain confirmation)


# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolDefinition:
    """Immutable metadata for a single agent tool."""

    name: str
    description: str
    category: ToolCategory
    risk_tier: RiskTier
    request_schema: type[BaseModel]  # Pydantic model class
    response_schema: type[BaseModel]  # Pydantic model class
    idempotent: bool = True
    latency_class: LatencyClass = LatencyClass.FAST
    requires_approval_above_usd: float | None = None

    # -- Schema generation helpers ------------------------------------------

    def input_json_schema(self) -> dict:
        """Return JSON Schema dict for the request model."""
        return self.request_schema.model_json_schema()

    def output_json_schema(self) -> dict:
        """Return JSON Schema dict for the response model."""
        return self.response_schema.model_json_schema()

    def to_mcp_schema(self) -> dict:
        """Generate an MCP-compatible tool definition."""
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_json_schema(),
        }

    def to_openai_schema(self) -> dict:
        """Generate an OpenAI function-calling compatible schema."""
        input_schema = self.input_json_schema()
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": input_schema,
            },
        }


# ---------------------------------------------------------------------------
# Tool catalog
# ---------------------------------------------------------------------------


class ToolCatalog:
    """Registry of all available agent tools.

    Provides lookup, iteration, and bulk schema generation.
    """

    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}
        self._register_builtin_tools()

    # -- Public API ---------------------------------------------------------

    def get(self, name: str) -> ToolDefinition | None:
        return self._tools.get(name)

    def list_tools(self, *, category: ToolCategory | None = None) -> list[ToolDefinition]:
        """Return tool definitions, optionally filtered by category."""
        tools = list(self._tools.values())
        if category is not None:
            tools = [t for t in tools if t.category == category]
        return tools

    def list_names(self) -> list[str]:
        return list(self._tools.keys())

    def register(self, tool: ToolDefinition) -> None:
        if tool.name in self._tools:
            logger.warning("Overwriting existing tool registration: %s", tool.name)
        self._tools[tool.name] = tool

    def to_mcp_tools(self) -> list[dict]:
        """Generate MCP tools/list payload."""
        return [t.to_mcp_schema() for t in self._tools.values()]

    def to_openai_tools(self) -> list[dict]:
        """Generate OpenAI-compatible function tool list."""
        return [t.to_openai_schema() for t in self._tools.values()]

    def __len__(self) -> int:
        return len(self._tools)

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    # -- Built-in tool registration -----------------------------------------

    def _register_builtin_tools(self) -> None:
        """Register all built-in tools from the schemas module."""
        for tool in _BUILTIN_TOOLS:
            self._tools[tool.name] = tool


# ---------------------------------------------------------------------------
# Built-in tool definitions
# ---------------------------------------------------------------------------

_BUILTIN_TOOLS: list[ToolDefinition] = [
    # ── DATA TOOLS ──────────────────────────────────────────────────────
    ToolDefinition(
        name="get_price",
        description="Get the current USD price of a token on a specific chain.",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetPriceRequest,
        response_schema=schemas.GetPriceResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="get_balance",
        description="Get the balance of a single token in a wallet. Defaults to the strategy wallet; pass wallet_address to query another (e.g. a Safe).",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetBalanceRequest,
        response_schema=schemas.GetBalanceResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="batch_get_balances",
        description="Get token balances for a wallet on a chain. Defaults to the strategy wallet; pass wallet_address to query another (e.g. a Safe).",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.BatchGetBalancesRequest,
        response_schema=schemas.BatchGetBalancesResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="get_indicator",
        description=("Calculate a technical indicator (RSI, SMA, EMA, MACD, Bollinger Bands, ATR) for a token."),
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetIndicatorRequest,
        response_schema=schemas.GetIndicatorResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    ToolDefinition(
        name="get_pool_state",
        description="Get details about a liquidity pool: price, tick, liquidity, volume, fees, TVL.",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetPoolStateRequest,
        response_schema=schemas.GetPoolStateResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    ToolDefinition(
        name="get_lp_position",
        description="Get details about an existing LP position: range, liquidity, accrued fees.",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetLPPositionRequest,
        response_schema=schemas.GetLPPositionResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    ToolDefinition(
        name="resolve_token",
        description="Resolve a token symbol or address to its full metadata (address, decimals, chain).",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.ResolveTokenRequest,
        response_schema=schemas.ResolveTokenResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="get_risk_metrics",
        description="Get portfolio risk metrics: VaR, Sharpe ratio, volatility, max drawdown.",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetRiskMetricsRequest,
        response_schema=schemas.GetRiskMetricsResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    # ── PLANNING / SAFETY TOOLS ─────────────────────────────────────────
    ToolDefinition(
        name="compile_intent",
        description="Compile a high-level intent into an executable ActionBundle for review or execution.",
        category=ToolCategory.PLANNING,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.CompileIntentRequest,
        response_schema=schemas.CompileIntentResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    ToolDefinition(
        name="simulate_intent",
        description="Dry-run an intent or compiled bundle without on-chain execution to check feasibility.",
        category=ToolCategory.PLANNING,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.SimulateIntentRequest,
        response_schema=schemas.SimulateIntentResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    ToolDefinition(
        name="validate_risk",
        description="Check an intent against RiskGuard constraints before execution.",
        category=ToolCategory.PLANNING,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.ValidateRiskRequest,
        response_schema=schemas.ValidateRiskResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="estimate_gas",
        description="Estimate gas cost (in USD and native token) for an intent.",
        category=ToolCategory.PLANNING,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.EstimateGasRequest,
        response_schema=schemas.EstimateGasResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="compute_rebalance_candidate",
        description="Deterministic economic viability check for LP rebalancing. Computes gas cost vs expected fee revenue.",
        category=ToolCategory.PLANNING,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.ComputeRebalanceCandidateRequest,
        response_schema=schemas.ComputeRebalanceCandidateResponse,
        latency_class=LatencyClass.MEDIUM,
    ),
    # ── ACTION TOOLS ────────────────────────────────────────────────────
    ToolDefinition(
        name="swap_tokens",
        description="Swap one token for another on a DEX. Supports dry_run for simulation.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.SwapTokensRequest,
        response_schema=schemas.SwapTokensResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
        requires_approval_above_usd=10_000,
    ),
    ToolDefinition(
        name="open_lp_position",
        description="Open a new concentrated liquidity position. Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.HIGH,
        request_schema=schemas.OpenLPPositionRequest,
        response_schema=schemas.OpenLPPositionResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
        requires_approval_above_usd=5_000,
    ),
    ToolDefinition(
        name="close_lp_position",
        description="Close or reduce a liquidity position. Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.CloseLPPositionRequest,
        response_schema=schemas.CloseLPPositionResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="supply_lending",
        description="Supply tokens to a lending protocol (e.g. Aave V3). Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.SupplyLendingRequest,
        response_schema=schemas.SupplyLendingResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="borrow_lending",
        description="Borrow tokens from a lending protocol. Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.HIGH,
        request_schema=schemas.BorrowLendingRequest,
        response_schema=schemas.BorrowLendingResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="repay_lending",
        description="Repay a lending position. Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.RepayLendingRequest,
        response_schema=schemas.RepayLendingResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="bridge_tokens",
        description="Bridge tokens from one chain to another. Uses Across or Stargate bridges. Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.BridgeTokensRequest,
        response_schema=schemas.BridgeTokensResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
        requires_approval_above_usd=10_000,
    ),
    ToolDefinition(
        name="unwrap_native",
        description="Unwrap wrapped native tokens back to native currency (e.g. WETH -> ETH, WMATIC -> MATIC). Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.UnwrapNativeRequest,
        response_schema=schemas.UnwrapNativeResponse,
        idempotent=False,
        latency_class=LatencyClass.MEDIUM,
    ),
    ToolDefinition(
        name="execute_compiled_bundle",
        description="Execute a previously compiled ActionBundle. Supports dry_run.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.HIGH,
        request_schema=schemas.ExecuteCompiledBundleRequest,
        response_schema=schemas.ExecuteCompiledBundleResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    # ── VAULT TOOLS ──────────────────────────────────────────────────────
    ToolDefinition(
        name="deploy_vault",
        description="Deploy a new Lagoon vault via factory contract. Returns the vault address on success.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.HIGH,
        request_schema=schemas.DeployVaultRequest,
        response_schema=schemas.DeployVaultResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="get_vault_state",
        description="Read current state of a Lagoon vault: total assets, pending deposits/redeems, share price.",
        category=ToolCategory.DATA,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.GetVaultStateRequest,
        response_schema=schemas.GetVaultStateResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="settle_vault",
        description="Run a vault settlement cycle: propose new valuation, settle deposits and redeems.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.SettleVaultRequest,
        response_schema=schemas.SettleVaultResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="approve_vault_underlying",
        description="Approve the vault to pull underlying tokens from the Safe (required for redemption settlement).",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.ApproveVaultUnderlyingRequest,
        response_schema=schemas.ApproveVaultUnderlyingResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="deposit_vault",
        description="Deposit underlying tokens into a vault (approve + requestDeposit). Depositor is the EOA.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=schemas.DepositVaultRequest,
        response_schema=schemas.DepositVaultResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    ToolDefinition(
        name="teardown_vault",
        description="Initiate a deterministic vault teardown: close all LP positions, swap to underlying, run final settlement.",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.HIGH,
        request_schema=schemas.TeardownVaultRequest,
        response_schema=schemas.TeardownVaultResponse,
        idempotent=False,
        latency_class=LatencyClass.SLOW,
    ),
    # ── STATE TOOLS ─────────────────────────────────────────────────────
    ToolDefinition(
        name="save_agent_state",
        description="Persist agent/strategy state for retrieval across iterations.",
        category=ToolCategory.STATE,
        risk_tier=RiskTier.LOW,
        request_schema=schemas.SaveAgentStateRequest,
        response_schema=schemas.SaveAgentStateResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="load_agent_state",
        description="Load previously saved agent/strategy state.",
        category=ToolCategory.STATE,
        risk_tier=RiskTier.NONE,
        request_schema=schemas.LoadAgentStateRequest,
        response_schema=schemas.LoadAgentStateResponse,
        latency_class=LatencyClass.FAST,
    ),
    ToolDefinition(
        name="record_agent_decision",
        description="Record an agent decision summary and tool call log for audit trail.",
        category=ToolCategory.STATE,
        risk_tier=RiskTier.LOW,
        request_schema=schemas.RecordAgentDecisionRequest,
        response_schema=schemas.RecordAgentDecisionResponse,
        latency_class=LatencyClass.FAST,
    ),
]


def get_default_catalog() -> ToolCatalog:
    """Return a fresh catalog with all built-in tools registered."""
    return ToolCatalog()
