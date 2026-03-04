# Agent Tools API Reference

The `almanak.framework.agent_tools` package exposes Almanak's DeFi capabilities as validated, policy-enforced tools that any LLM agent can consume.

```python
from almanak.framework.agent_tools import (
    ToolExecutor,
    AgentPolicy,
    PolicyEngine,
    ToolCatalog,
    ToolDefinition,
    ToolResponse,
    get_default_catalog,
)
```

## ToolExecutor

The executor is the bridge between the LLM and the gateway. It validates inputs, enforces policy, dispatches to the gateway, and wraps results in `ToolResponse` envelopes.

```python
from almanak.framework.agent_tools import ToolExecutor, AgentPolicy, get_default_catalog
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

gateway = GatewayClient(GatewayClientConfig.from_env())
gateway.connect()

executor = ToolExecutor(
    gateway,
    policy=AgentPolicy(allowed_chains={"arbitrum"}),
    catalog=get_default_catalog(),
    wallet_address="0x...",
    strategy_id="my-agent",
    default_chain="arbitrum",
)

# Execute a tool call
result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
# result.status == "success"
# result.data == {"token": "ETH", "price_usd": 1850.0, ...}
```

**Constructor parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `gateway_client` | `GatewayClient` | required | Connected gateway instance |
| `policy` | `AgentPolicy` | safe defaults | Safety constraints |
| `catalog` | `ToolCatalog` | built-in | Tool registry |
| `wallet_address` | `str` | `""` | Strategy wallet for balance/execution calls |
| `strategy_id` | `str` | `""` | Strategy ID for state operations |
| `default_chain` | `str` | `"arbitrum"` | Default chain when not specified in args |
| `safe_addresses` | `set[str] \| None` | `None` | Allowlist of Safe wallet addresses |

## AgentPolicy

Dataclass defining safety constraints for an agent session. Every field has a safe default.

**Spend limits:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_single_trade_usd` | `Decimal` | `10000` | Max USD value per trade |
| `max_daily_spend_usd` | `Decimal` | `50000` | Max cumulative USD per day |
| `max_position_size_usd` | `Decimal` | `100000` | Max position value |

**Scope constraints:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `allowed_tools` | `set[str] \| None` | `None` (all) | Allowlist of tool names |
| `allowed_chains` | `set[str]` | `{"arbitrum"}` | Allowed blockchains |
| `allowed_protocols` | `set[str] \| None` | `None` (all) | Allowed DeFi protocols |
| `allowed_tokens` | `set[str] \| None` | `None` (all) | Allowed token symbols |
| `allowed_intent_types` | `set[str] \| None` | `None` (all) | Allowed intent types |
| `allowed_execution_wallets` | `set[str] \| None` | `None` (any) | Allowed wallet addresses |

**Approval gates:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `require_human_approval_above_usd` | `Decimal` | `10000` | Hard block above this threshold |
| `require_simulation_before_execution` | `bool` | `True` | Require dry-run before on-chain |

**Rate limits:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_trades_per_hour` | `int` | `10` | Trade rate limit |
| `max_tool_calls_per_minute` | `int` | `60` | Tool call rate limit |

**Circuit breakers:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `stop_loss_pct` | `Decimal` | `5.0` | Portfolio drawdown stop-loss (%) |
| `max_consecutive_failures` | `int` | `3` | Failures before circuit breaker trips |

**Economic thresholds:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min_rebalance_benefit_usd` | `Decimal` | `10` | Min benefit to justify LP rebalance |
| `cooldown_seconds` | `int` | `300` | Seconds between trades |
| `require_rebalance_check` | `bool` | `True` | Require viability check before LP actions |

## PolicyEngine

Evaluates tool calls against an `AgentPolicy`. Tracks runtime state (daily spend, trade count, failures).

```python
from almanak.framework.agent_tools import PolicyEngine, AgentPolicy

engine = PolicyEngine(AgentPolicy(max_single_trade_usd=Decimal("100")))
decision = engine.check(tool_def, {"token_in": "USDC", "amount": "50"})

if decision.allowed:
    # proceed with execution
    ...
else:
    print(decision.violations)   # ["Estimated trade value $500 exceeds..."]
    print(decision.suggestions)  # ["Reduce amount to at most $100."]
```

**Key methods:**

| Method | Description |
|--------|-------------|
| `check(tool_def, arguments)` | Run all policy checks, return `PolicyDecision` |
| `record_trade(usd_amount, success, tool_name)` | Update spend/failure accounting after a trade |
| `record_tool_call()` | Track tool call for rate limiting |
| `update_portfolio_value(usd_value)` | Update portfolio value for stop-loss tracking |
| `reset_daily()` | Reset daily accumulators |

## ToolCatalog

Registry of all available agent tools.

```python
from almanak.framework.agent_tools import get_default_catalog

catalog = get_default_catalog()

# Look up a specific tool
tool = catalog.get("swap_tokens")

# List all tools
all_tools = catalog.list_tools()

# Filter by category
data_tools = catalog.list_tools(category=ToolCategory.DATA)

# Generate framework-specific schemas
openai_tools = catalog.to_openai_tools()
mcp_tools = catalog.to_mcp_tools()

# Register a custom tool
catalog.register(my_custom_tool)
```

## ToolDefinition

Immutable metadata for a single tool.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Tool name (e.g. `"swap_tokens"`) |
| `description` | `str` | Human-readable description |
| `category` | `ToolCategory` | `DATA`, `PLANNING`, `ACTION`, or `STATE` |
| `risk_tier` | `RiskTier` | `NONE`, `LOW`, `MEDIUM`, or `HIGH` |
| `request_schema` | `type[BaseModel]` | Pydantic input model |
| `response_schema` | `type[BaseModel]` | Pydantic output model |
| `idempotent` | `bool` | Whether the tool is safe to retry |
| `latency_class` | `LatencyClass` | `FAST` (<500ms), `MEDIUM` (500ms-5s), `SLOW` (>5s) |
| `requires_approval_above_usd` | `float \| None` | Per-tool approval threshold |

## ToolResponse

Standard envelope returned by every tool call.

| Field | Type | Description |
|-------|------|-------------|
| `status` | `str` | `"success"`, `"simulated"`, `"blocked"`, or `"error"` |
| `data` | `dict \| None` | Tool-specific result payload |
| `error` | `dict \| None` | Structured error if `status == "error"` |
| `decision_hints` | `dict \| None` | Machine-readable hints for agent reasoning |
| `explanation` | `str \| None` | Human-readable context about the result |

## Built-in Tools

The default catalog includes 29 tools organized into four categories.

### Data Tools (9)

Read-only tools with no on-chain side effects. Risk tier: **NONE**.

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `get_price` | Get current USD price of a token | `token`, `chain` |
| `get_balance` | Get balance of a single token in a wallet | `token`, `chain`, `wallet_address` |
| `batch_get_balances` | Get token balances for a wallet on a chain | `tokens`, `chain`, `wallet_address` |
| `get_indicator` | Calculate technical indicator (RSI, SMA, EMA, MACD, BB, ATR) | `token`, `indicator`, `period`, `chain` |
| `get_pool_state` | Get liquidity pool details (price, tick, TVL, fees) | `token_a`, `token_b`, `fee_tier`, `chain` |
| `get_lp_position` | Get LP position details (range, liquidity, fees) | `position_id`, `chain` |
| `resolve_token` | Resolve token symbol/address to full metadata | `token`, `chain` |
| `get_risk_metrics` | Get portfolio risk metrics (VaR, Sharpe, vol, drawdown) | `chain` |
| `get_vault_state` | Get Lagoon vault state (assets, deposits, share price) | `vault_address`, `chain` |

### Planning Tools (5)

Pre-execution analysis tools. Risk tier: **NONE**.

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `compile_intent` | Compile a high-level intent into an ActionBundle | `intent_type`, `chain`, token/amount fields |
| `simulate_intent` | Dry-run an intent without on-chain execution | `intent_type`, `chain`, token/amount fields |
| `validate_risk` | Check an intent against RiskGuard constraints | `intent_type`, `chain`, token/amount fields |
| `estimate_gas` | Estimate gas cost (USD + native token) | `intent_type`, `chain` |
| `compute_rebalance_candidate` | Check economic viability of LP rebalance | `position_id`, `chain` |

### Action Tools (12)

On-chain execution tools. Risk tier: **MEDIUM** or **HIGH**. All support `dry_run` for simulation.

| Tool | Risk | Description | Key Parameters |
|------|------|-------------|----------------|
| `swap_tokens` | MEDIUM | Swap tokens on a DEX | `token_in`, `token_out`, `amount`, `chain` |
| `open_lp_position` | HIGH | Open concentrated LP position | `token_a`, `token_b`, `amount_a`, `amount_b`, `price_lower`, `price_upper` |
| `close_lp_position` | MEDIUM | Close or reduce LP position | `position_id`, `chain` |
| `supply_lending` | MEDIUM | Supply tokens to lending protocol | `token`, `amount`, `protocol`, `chain` |
| `borrow_lending` | HIGH | Borrow from lending protocol | `token`, `amount`, `collateral_token`, `chain` |
| `repay_lending` | MEDIUM | Repay a lending position | `token`, `amount`, `chain` |
| `execute_compiled_bundle` | HIGH | Execute a previously compiled ActionBundle | `bundle_id` |
| `deploy_vault` | HIGH | Deploy a new Lagoon vault | `underlying_token`, `chain` |
| `settle_vault` | MEDIUM | Run vault settlement cycle | `vault_address`, `chain` |
| `approve_vault_underlying` | MEDIUM | Approve vault to pull tokens | `vault_address`, `amount`, `chain` |
| `deposit_vault` | MEDIUM | Deposit tokens into a vault | `vault_address`, `amount`, `chain` |
| `teardown_vault` | HIGH | Deterministic vault teardown | `vault_address`, `chain` |

### State Tools (3)

Agent state persistence. Risk tier: **LOW** or **NONE**.

| Tool | Risk | Description | Key Parameters |
|------|------|-------------|----------------|
| `save_agent_state` | LOW | Persist agent state across iterations | `strategy_id`, `state` |
| `load_agent_state` | NONE | Load previously saved state | `strategy_id` |
| `record_agent_decision` | LOW | Record decision for audit trail | `strategy_id`, `decision_summary` |

## Error Types

All tool errors extend `ToolError` and include machine-readable codes for agent consumption.

| Error Class | Code | Recoverable | When Triggered |
|-------------|------|-------------|----------------|
| `ToolValidationError` | `validation_error` | Yes | Malformed or missing arguments |
| `RiskBlockedError` | `risk_blocked` | No | Policy or RiskGuard rejection |
| `SimulationFailedError` | `simulation_failed` | Yes | On-chain simulation reverted |
| `ToolTimeoutError` | `timeout` | Yes | Gateway or RPC timeout |
| `UpstreamUnavailableError` | `upstream_unavailable` | Yes | External service unreachable |
| `PermissionDeniedError` | `permission_denied` | No | Action not in allowed set |
| `ExecutionFailedError` | `execution_failed` | No | Transaction reverted on-chain |

Every error includes:

- `code` -- Machine-readable error code
- `message` -- Human-readable description
- `recoverable` -- Whether the agent should retry
- `suggestion` -- Remediation hint for the agent

```python
from almanak.framework.agent_tools.errors import ToolError

try:
    result = await executor.execute("swap_tokens", args)
except ToolError as e:
    print(e.code)        # "risk_blocked"
    print(e.recoverable) # False
    print(e.suggestion)  # "Reduce trade size to at most $100."
```
