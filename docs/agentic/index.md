# Agentic Trading

The Almanak SDK supports two ways to build DeFi strategies:

1. **Deterministic strategies** (`IntentStrategy`) -- You write Python logic in `decide()`. The framework compiles and executes. Predictable, testable, auditable.
2. **Agentic strategies** -- An LLM decides what to do using Almanak's tools. The framework enforces safety via a non-bypassable policy engine.

This section covers the agentic path.

!!! warning "BYO LLM API Key"
    Agentic strategies require **your own LLM API key** from any OpenAI-compatible
    provider (OpenAI, Anthropic, Ollama, etc.). The SDK does not include LLM access.
    Set `AGENT_LLM_API_KEY` before running. The agent will verify connectivity
    before starting -- if the key is missing or invalid, you'll get a clear error
    message immediately.

## Bot vs Agent

| | Deterministic Bot | LLM Agent |
|---|---|---|
| **Decision maker** | Python `decide()` method | LLM (GPT-4, Claude, etc.) |
| **Execution entry** | `Intent.swap(...)` | `swap_tokens` tool call |
| **Safety model** | RiskGuard (compile-time) | PolicyEngine (runtime, per-call) |
| **Testing** | Unit tests + Anvil fork | Mock LLM + Anvil fork |
| **When to use** | Known rules, quantitative signals | Complex reasoning, multi-step plans, natural language goals |

Both paths share the same gateway, connectors, and on-chain execution pipeline. The only difference is *who decides* -- your code or an LLM.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  YOUR CODE                                          │
│                                                     │
│  run.py ── agent loop ──┐                           │
│  config.json            │   LLM (OpenAI/Claude/...) │
│  prompts.py             │   ↕ tool calls            │
│                    ToolExecutor ◄── AgentPolicy      │
│                         │       (spend limits,       │
│                         │        allowed tokens,     │
│                         │        rate limits)        │
├─────────────────────────┼───────────────────────────┤
│  ALMANAK STACK          │                           │
│                         ▼                           │
│  Gateway (gRPC sidecar)                             │
│  ├─ MarketService   (prices, balances, indicators)  │
│  ├─ ExecutionService (compile + execute intents)    │
│  ├─ StateService     (save/load agent state)        │
│  └─ ObserveService   (audit trail)                  │
│                         ▼                           │
│  Blockchain (mainnet or Anvil fork)                 │
└─────────────────────────────────────────────────────┘
```

The LLM never touches the blockchain directly. Every tool call passes through:

1. **Schema validation** -- Pydantic models reject malformed inputs
2. **Policy engine** -- Spend limits, allowed tokens, rate limits, circuit breakers
3. **Gateway dispatch** -- gRPC call to the appropriate service
4. **Response envelope** -- Structured `ToolResponse` with status, data, and error fields

## LLM Configuration

The agent loop connects to any OpenAI-compatible API. Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_LLM_API_KEY` | -- | API key (required for real mode) |
| `AGENT_LLM_BASE_URL` | `https://api.openai.com/v1` | LLM endpoint URL |
| `AGENT_LLM_MODEL` | `gpt-4o` | Model name |

Any OpenAI-compatible provider works (OpenAI, Anthropic via proxy, Ollama, vLLM, etc.).

## Safety Model

Every agent gets an `AgentPolicy` that constrains what the LLM can do:

```python
from decimal import Decimal
from almanak.framework.agent_tools import AgentPolicy

policy = AgentPolicy(
    # Spend limits
    max_single_trade_usd=Decimal("50"),
    max_daily_spend_usd=Decimal("200"),
    max_position_size_usd=Decimal("1000"),

    # Scope constraints
    allowed_chains={"arbitrum"},
    allowed_tokens={"WETH", "USDC", "ETH"},
    allowed_tools={"get_price", "get_balance", "swap_tokens", "save_agent_state"},

    # Rate limits
    max_trades_per_hour=5,
    cooldown_seconds=30,

    # Circuit breakers
    max_consecutive_failures=3,
    stop_loss_pct=Decimal("5.0"),

    # Approval gates
    require_human_approval_above_usd=Decimal("10000"),
    require_simulation_before_execution=True,
)
```

The `PolicyEngine` enforces these constraints on **every tool call**. It cannot be bypassed -- the LLM never gets direct gateway access.

## Quick Start

### 1. Install

```bash
pip install almanak
```

### 2. Start the gateway

```bash
# Terminal 1: gateway with local Anvil fork
almanak gateway --network anvil
```

### 3. Run with mock LLM (no API key needed)

```bash
# Terminal 2: smoke test
python examples/agentic/agent_swap/run.py --once --mock
```

### 4. Run with a real LLM

```bash
AGENT_LLM_API_KEY=sk-... python examples/agentic/agent_swap/run.py --once
```

### What happens if the key is missing?

```
$ python examples/agentic/agent_swap/run.py --once

ERROR: No LLM API key configured.

Agentic strategies require your own LLM API key.
Set it via environment variable:

  export AGENT_LLM_API_KEY=sk-...

Any OpenAI-compatible provider works (OpenAI, Anthropic, Ollama, etc.).
See: https://docs.almanak.co/agentic/
```

## Building Your Own Agent

1. **Copy a template** -- Start from `examples/agentic/agent_swap/` (simplest) or `agent_lp/` (LP management) or `defai_vault_lp/` (vault + LP)
2. **Edit `config.json`** -- Set chain, tokens, amounts, strategy parameters
3. **Write prompts** -- `prompts.py` defines the system prompt with available tools and decision rules
4. **Configure policy** -- `create_policy()` in `run.py` sets spend limits, allowed tools/tokens/chains
5. **Create a mock LLM** -- Script a realistic tool-call sequence for testing without an API key
6. **Run** -- `AGENT_LLM_API_KEY=... python your_agent/run.py --once`

## Next Steps

- [Agent Tools API Reference](agent-tools.md) -- All 29 tools, ToolExecutor, PolicyEngine, error types
- [Framework Adapters](adapters.md) -- OpenAI, MCP, and LangChain integrations
- [Examples source](https://github.com/almanak-co/sdk/tree/main/examples/agentic) -- Working agent implementations
