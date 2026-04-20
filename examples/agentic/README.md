# Agentic Trading Examples

Consumer examples showing how to use the Almanak `agent_tools` framework to build autonomous AI agents that trade on-chain. Each example is a standalone agent with its own config, prompts, and policy.

## Prerequisites

- **Almanak SDK** installed (`pip install almanak`)
- **Foundry** for Anvil fork testing
- **Your own LLM API key** -- any OpenAI-compatible provider:
  - OpenAI: `AGENT_LLM_API_KEY=sk-...`
  - Anthropic (via proxy): `AGENT_LLM_API_KEY=sk-ant-... AGENT_LLM_BASE_URL=...`
  - Local (Ollama): `AGENT_LLM_API_KEY=dummy AGENT_LLM_BASE_URL=http://localhost:11434/v1`

The `--mock` flag bypasses the LLM requirement for smoke testing.

## Architecture

```
+-------------------------------------------------------+
|  USER SPACE (this directory)                           |
|                                                        |
|  run.py  --- LLM loop ---+                             |
|  config.json             |                             |
|  prompts.py              |   LLM (OpenAI/Anthropic)    |
|                          |   <-> tool calls            |
|                     ToolExecutor  <-- AgentPolicy       |
|                          |        (spend limits,        |
|                          |         allowed tokens,      |
|                          |         rate limits)          |
+-------------------------------------------------------+
|  ALMANAK STACK           |                             |
|                          v                             |
|  Gateway (gRPC sidecar)                                |
|  +- MarketService  (prices, balances, indicators)      |
|  +- ExecutionService (compile + execute intents)       |
|  +- StateService   (save/load agent state)             |
|  +- ObserveService (audit trail)                       |
|                          v                             |
|  Blockchain (mainnet or Anvil fork)                    |
+-------------------------------------------------------+
```

## Examples

### AgentLP (`agent_lp/`)

Autonomous LP management on Trader Joe V2 (Avalanche). The LLM reads WAVAX price, opens a concentrated LP position in the WAVAX/USDC pool, and rebalances when price moves outside the range.

- **Chain:** Avalanche
- **Protocol:** Trader Joe V2
- **Tools:** 7 (get_price, get_balance, open_lp_position, close_lp_position, swap_tokens, save/load state)

### AgentSwap (`agent_swap/`)

The simplest agentic example -- a "Buy the Dip" RSI agent on Arbitrum.
Reads ETH price and RSI, buys when oversold, sells when overbought.

- **Chain:** Arbitrum
- **Protocol:** Uniswap V3 (via Enso)
- **Tools:** 7 (get_price, get_balance, get_indicator, swap_tokens, save/load state, record decision)

### AgentYield (`agent_yield/`)

Autonomous yield farming on Aave V3 (Avalanche). The LLM reads prices and RSI, supplies stablecoins, and rotates between USDC and WAVAX based on momentum signals.

- **Chain:** Avalanche
- **Protocol:** Aave V3
- **Tools:** 7 (get_price, get_balance, get_indicator, supply_lending, swap_tokens, save/load state)

### DeFAI Vault + LP (`defai_vault_lp/`)

The flagship DeFAI example. Deploys a [Lagoon](https://lagoon.finance) vault (ERC-7540), funds it with USDC, opens a Uniswap V3 LP position using vault funds via a Safe wallet, and manages the position 24/7.

- **Chain:** Base
- **Protocols:** Lagoon (vault), Uniswap V3 (LP)
- **Tools:** 17 (vault lifecycle + LP + data + state)
- **Wallets:** Dual-signer (EOA + Safe)

Two operating modes:
- **INIT** -- 10-phase lifecycle: deploy vault, approve, settle, deposit, open LP, settle NAV, persist state
- **RUNNING** -- Priority loop: P0 teardown, P1 settle, P2 LP health/rebalance, P3 deploy idle, P4 hold

See [`defai_vault_lp/README.md`](defai_vault_lp/README.md) for full documentation.

### Shared Utilities (`shared/`)

Reusable components shared across all examples:

| File | Purpose |
|------|---------|
| `agent_loop.py` | Generic agent loop: LLM call -> tool execution -> repeat |
| `llm_client.py` | `LLMClient` (OpenAI-compatible), `MockLLMClient`, `DynamicMockLLMClient` |

## Quick Start

### AgentSwap (Arbitrum) -- Simplest Example

```bash
# Terminal 1: Start gateway with Anvil fork
almanak gateway --network anvil

# Terminal 2: Run the agent (real LLM)
AGENT_LLM_API_KEY=sk-... python examples/agentic/agent_swap/run.py --once

# Or smoke test with mock LLM (no API key needed)
python examples/agentic/agent_swap/run.py --once --mock
```

### AgentLP / AgentYield (Avalanche)

```bash
# Terminal 1: Start gateway with Anvil fork
almanak gateway --network anvil

# Terminal 2: Run an agent (real LLM)
AGENT_LLM_API_KEY=sk-... python examples/agentic/agent_lp/run.py --once
AGENT_LLM_API_KEY=sk-... python examples/agentic/agent_yield/run.py --once

# Or smoke test with mock LLM (no API key needed)
python examples/agentic/agent_lp/run.py --once --mock
python examples/agentic/agent_yield/run.py --once --mock
```

### DeFAI Vault LP (Base)

```bash
# Terminal 1: Start gateway with Safe mode
ALMANAK_GATEWAY_SAFE_ADDRESS=0x98aE... \
ALMANAK_GATEWAY_SAFE_MODE=direct \
almanak gateway --network anvil

# Terminal 2: Run the vault agent (real LLM)
AGENT_LLM_API_KEY=sk-... python examples/agentic/defai_vault_lp/run.py --once

# Or smoke test with mock LLM
python examples/agentic/defai_vault_lp/run.py --once --mock
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AGENT_LLM_API_KEY` | Yes (real mode) | -- | OpenAI or compatible API key |
| `AGENT_LLM_BASE_URL` | No | `https://api.openai.com/v1` | LLM API endpoint |
| `AGENT_LLM_MODEL` | No | `gpt-4o` | Model to use |
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Vault only | -- | Safe wallet for vault operations |
| `ALMANAK_GATEWAY_SAFE_MODE` | Vault only | -- | Must be `direct` for vault operations |

Each agent has a `config.json` with strategy-specific settings (pool, amounts, tokens, intervals, policy limits).

## Safety

Each agent creates a tight `AgentPolicy` that constrains the LLM:

- **Allowed tools** -- only the tools the agent needs (no access to unused tools)
- **Allowed chains/tokens** -- agent cannot operate on wrong chain or unapproved tokens
- **Spend limits** -- per-trade and daily caps (e.g., `max_single_trade_usd=$100`)
- **Rate limits** -- `max_trades_per_hour`, `cooldown_seconds` between trades
- **Circuit breaker** -- consecutive failures halt the agent
- **Simulation** -- optionally require simulation before every on-chain action

All errors are wrapped in `ToolResponse` envelopes -- the LLM never sees raw exceptions.

## Building Your Own Agent

1. Copy `agent_lp/` as a starting template (or `defai_vault_lp/` for vault-based agents)
2. Edit `config.json` with your strategy parameters (chain, pool, tokens, amounts)
3. Write your system prompt in `prompts.py` listing the available tools and decision rules
4. Adjust `create_policy()` in `run.py` to constrain the agent:
   - Set `allowed_chains`, `allowed_tokens`, `allowed_tools`
   - Set spend limits, rate limits, cooldown
5. Create a mock LLM flow for testing (see `create_dynamic_mock_llm()` in `defai_vault_lp/run.py` for the advanced pattern)
6. Run: `AGENT_LLM_API_KEY=... python your_agent/run.py --once`

For the full tool catalog and framework documentation, see [docs.almanak.co](https://docs.almanak.co/).

## New Framework Features (March 2026)

Recent audit-driven improvements to the agent tools framework:

| Feature | How to use | PR |
|---------|-----------|-----|
| **Decision tracing** | Pass `trace_sink=callback` to `run_agent_loop()` for structured audit logs | #485 |
| **Structured errors** | Typed `ToolError` subclasses with `recoverable` flag and `suggestion` hint | #483 |
| **Human approval** | Set `require_human_approval_above_usd` threshold in `AgentPolicy` | #480 |
| **Policy persistence** | Set `state_persistence_path` to survive restarts | #473 |
| **Risk metrics** | `get_risk_metrics` returns portfolio value from on-chain balances | #481 |
| **Pre-trade validation** | `validate_risk` returns structured risk assessment without executing | #482 |
| **MCP server** | `almanak mcp serve` starts an MCP tool server for Claude Desktop | #484 |
| **Mock LLM client** | `MockLLMClient` in `shared/llm_client.py` for testing agents without live LLM | #471 |

### MCP Server (use with Claude Desktop)

```bash
# Schema-only mode (no gateway needed, for tool discovery)
almanak mcp serve --schema-only

# Full mode with gateway
almanak gateway &
almanak mcp serve --max-daily-spend-usd 5000 --allowed-chains arbitrum
```

Claude Desktop config (`~/.claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "almanak": {
      "command": "almanak",
      "args": ["mcp", "serve"]
    }
  }
}
```

### Decision Tracing

The shared `run_agent_loop()` now accepts an optional `trace_sink` callback:

```python
import json, pathlib

def file_trace_sink(event: dict) -> None:
    path = pathlib.Path("traces/agent_decisions.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(event, default=str) + "\n")

result = await run_agent_loop(
    llm_client, executor, tools,
    system_prompt, user_prompt,
    trace_sink=file_trace_sink,
)
```

## Tests

```bash
# Framework unit tests (300+ tests)
uv run pytest tests/unit/agent_tools/ -q

# Consumer example tests (LLM client + agent loop)
uv run pytest tests/unit/agent_tools/test_llm_client.py tests/unit/agent_tools/test_agent_loop.py -v
```
