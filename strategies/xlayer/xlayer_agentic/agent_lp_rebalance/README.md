# Agentic LP Rebalance on X-Layer

> **X-Layer Build-X Hackathon submission** — X-Layer Arena track
> _Season: April 1–15, 2026_

An autonomous AI agent that manages a concentrated Uniswap V3 liquidity
position on the **WOKB/USDT** pool on X-Layer. Instead of a hand-written
state machine, an LLM reads live market data through the Almanak gateway
and decides — on every tick — whether to **open**, **hold**, or
**rebalance** the position.

This is the LLM-driven counterpart of the deterministic reference
strategy `almanak/demo_strategies/xlayer_lp_rebalance`. Same on-chain
footprint, same pool, same economics — but the policy logic is
reasoned by an LLM at runtime instead of encoded in Python.

---

## Why this matters for X-Layer

- **Fully deployed on X-Layer.** Every on-chain action (approvals,
  mints, burns, swaps) hits the `xlayer` network through the Almanak
  execution gateway. The deployment ID, chain, and pool are all locked to
  X-Layer by policy — the agent is not allowed to touch any other chain.
- **Real Uniswap V3 integration.** Opens a concentrated position on the
  native `WOKB/USDT/3000` pool using the production Uniswap V3 connector
  (NonfungiblePositionManager mint / decreaseLiquidity / collect).
- **Autonomous operation.** Once started, the agent manages the position
  end-to-end: monitoring prices, deciding when the range is stale,
  closing the old position, swapping to rebalance token ratios, and
  re-opening at a new range.
- **Policy-constrained by design.** The LLM runs inside a tight
  `AgentPolicy` allowlist — only the `xlayer` chain, only WOKB/USDT,
  only a small set of tools, per-trade and daily USD caps, cooldowns
  between actions. The model cannot rug its own wallet.

---

## What the strategy does

```text
                    +-------------------+
                    |   LLM (GPT-4o)    |
                    +---------+---------+
                              |
                   OpenAI tool_call JSON
                              |
                    +---------v---------+
                    |   ToolExecutor    |
                    | (AgentPolicy gate) |
                    +---------+---------+
                              |
                          gRPC tools
                              |
    +-------------------------v--------------------------+
    |  Almanak Gateway  (MarketService, ExecutionService)|
    +-------------------------+--------------------------+
                              |
                      JSON-RPC + signing
                              |
                    +---------v---------+
                    |   X-Layer chain   |
                    |  Uniswap V3 NPM   |
                    |   WOKB/USDT/3000  |
                    +-------------------+
```

**Decision loop (one iteration):**

1. **Load state** — `load_agent_state` recovers the last-known position
   (if any) from gateway-backed storage so the agent survives restarts.
2. **Read the market** — `get_price` for WOKB and USDT, `get_balance`
   for both legs.
3. **Decide**:
   - _No position?_ → compute a symmetric range around the current pair
     price using `range_width_pct` (default ±5%) and call
     `open_lp_position` with the configured amounts.
   - _Position in range?_ → return a short text response and exit. No
     gas, no on-chain action.
   - _Position out of range by more than `rebalance_threshold_pct`?_ →
     `close_lp_position`, re-read balances, `swap_tokens` to rebalance
     the token ratio, then `open_lp_position` at a new range.
4. **Persist** — `save_agent_state` records the new `position_id` and
   range; `record_agent_decision` writes a one-line audit trail.
5. **Return** a human-readable text summary to the operator.

---

## How to run

### Prerequisites

- Python ≥ 3.12 with the Almanak SDK installed (`uv sync` or
  `pip install -e .` from the repo root)
- Foundry (`anvil`) for local X-Layer fork testing
- An LLM API key (OpenAI, Anthropic via proxy, or local Ollama) —
  **only needed for non-mock runs**
- X-Layer RPC access for the Almanak gateway (via `ALCHEMY_API_KEY` or
  any X-Layer RPC endpoint)

### Step 1 — start the gateway against an X-Layer Anvil fork

```bash
# In terminal 1
almanak gateway --network anvil --chain xlayer
```

The gateway auto-forks X-Layer mainnet, funds the default Anvil wallet
with the tokens listed in `config.json -> anvil_funding` (OKB, WOKB,
USDT, USDC), and exposes the gRPC services the agent will use.

### Step 2 — smoke test with the mock LLM (no API key)

```bash
# In terminal 2
python strategies/xlayer/agent_lp_rebalance/run.py --once --mock
```

The `--mock` flag swaps in a scripted `MockLLMClient` that replays a
deterministic sequence (`load_state → get_price × 2 → get_balance × 2
→ open_lp_position → save_agent_state + record_agent_decision → text
reply`). Useful for CI, judges, and offline review.

### Step 3 — run with a real LLM

```bash
# OpenAI
AGENT_LLM_API_KEY=sk-... \
  python strategies/xlayer/agent_lp_rebalance/run.py --once

# Anthropic via proxy
AGENT_LLM_API_KEY=sk-ant-... \
  AGENT_LLM_BASE_URL=https://api.anthropic.com/v1 \
  AGENT_LLM_MODEL=claude-opus-4-6 \
  python strategies/xlayer/agent_lp_rebalance/run.py --once

# Local Ollama
AGENT_LLM_API_KEY=dummy \
  AGENT_LLM_BASE_URL=http://localhost:11434/v1 \
  AGENT_LLM_MODEL=llama3.1 \
  python strategies/xlayer/agent_lp_rebalance/run.py --once
```

Drop the `--once` flag to run continuously every `interval_seconds` (60
seconds by default).

---

## Tools the AI agent is allowed to call

The agent operates inside a tight allowlist enforced by `AgentPolicy`
at the `ToolExecutor` layer. The LLM cannot call any tool outside this
set — attempts are rejected before they reach the gateway.

### Data tools (safe, read-only)

| Tool | Purpose |
|------|---------|
| `get_price` | Fetch current token price from the Almanak `MarketService`. |
| `get_balance` | Query the wallet ERC-20 balance for a specific token on X-Layer. |
| `get_lp_position` | Inspect a Uniswap V3 position NFT (ticks, liquidity, owed fees). |

### Action tools (on-chain, policy-gated)

| Tool | Purpose |
|------|---------|
| `open_lp_position` | Mint a concentrated LP position on `uniswap_v3` with an explicit `price_lower` / `price_upper` and `amount_a` / `amount_b`. |
| `close_lp_position` | Full-close an existing position by `position_id`, collecting accrued fees. |
| `swap_tokens` | Rebalance token ratios between close and re-open. Used to restore the ~50/50 balance the LP needs. |

### State tools (persistence + audit)

| Tool | Purpose |
|------|---------|
| `save_agent_state` | Persist `{position_id, range_lower, range_upper, ...}` via `StateService`. |
| `load_agent_state` | Recover the last-known state on restart. |
| `record_agent_decision` | Write a one-line rationale to the audit trail. |

All tool calls flow through `AgentPolicy.validate()` before execution.
The policy enforces:

- `allowed_chains={"xlayer"}`
- `allowed_tokens={"WOKB", "OKB", "USDT", "USDC"}`
- `max_single_trade_usd = $500`
- `max_daily_spend_usd = $2000`
- `cooldown_seconds = 5`
- `max_trades_per_hour = 20`

The policy also allows `compute_rebalance_candidate` (helper that
suggests new tick ranges without executing anything).

Tools outside the allowlist (`bridge_tokens`, `deploy_vault`,
`supply_lending`, etc.) are simply not exposed to the LLM — the OpenAI
tool manifest only contains the allowed tools above.

---

## Configuration (`config.json`)

```json
{
    "chain": "xlayer",
    "wallet_address": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    "pool": "WOKB/USDT/3000",
    "amount_token0": "1.0",
    "amount_token1": "50",
    "range_width_pct": "0.10",
    "rebalance_threshold_pct": "0.05",
    "interval_seconds": 60,
    "max_tool_rounds": 12,
    "deployment_id": "agent-xlayer-lp-rebalance",
    "anvil_funding": {
        "OKB": 100, "WOKB": 10, "USDT": 10000, "USDC": 10000
    }
}
```

| Key | Meaning |
|-----|---------|
| `pool` | Uniswap V3 pool identifier (`TOKEN0/TOKEN1/FEE_BPS`). |
| `amount_token0` / `amount_token1` | Max position size per leg. |
| `range_width_pct` | Full width of the LP range as a decimal (0.10 = ±5%). |
| `rebalance_threshold_pct` | How far outside the range the price must move before the agent rebalances. |
| `interval_seconds` | Loop period in non-`--once` mode. |
| `max_tool_rounds` | Safety cap on tool calls per iteration — prevents runaway LLM loops. |
| `deployment_id` | Opaque id used for state persistence. |
| `anvil_funding` | Initial wallet funding for local Anvil fork smoke tests. |

---

## Safety model

1. **Policy allowlist** — the LLM never sees any tool it shouldn't use.
2. **Per-trade and daily USD caps** — enforced before the gateway ever
   signs a tx.
3. **Cooldowns and rate limits** — the agent cannot rebalance more than
   20 times per hour, with a 5-second cooldown between actions.
4. **Simulation path** — every action tool accepts an optional
   `dry_run=true` flag that routes through the Almanak simulator
   without broadcasting.
5. **Circuit breaker** — consecutive tool errors halt the agent loop.
6. **Structured error envelopes** — the LLM never sees raw gRPC
   exceptions, only typed `ToolError` objects with `recoverable` flags.
7. **Auditability** — every decision is tagged with `deployment_id` and
   written to the `ObserveService` audit trail via
   `record_agent_decision`.

---

## File layout

```
strategies/xlayer/agent_lp_rebalance/
├── README.md       # this file
├── __init__.py
├── config.json     # chain, pool, amounts, policy-facing params
├── prompts.py      # SYSTEM_PROMPT template + USER_PROMPT
└── run.py          # entrypoint: gateway + ToolExecutor + agent loop
```

The run script reuses the shared agent utilities from
`examples/agentic/shared/` (`agent_loop.py`, `llm_client.py`) so there
is no duplicated LLM plumbing.

---

## Hackathon relevance

- **Track:** X-Layer Arena — full-stack agentic application.
- **X-Layer deployment:** all on-chain actions target the `xlayer`
  network by policy; no cross-chain paths are allowed.
- **Creativity:** the same strategy surface (concentrated LP lifecycle)
  that is typically a rigid state machine is here expressed as
  _natural-language rules_ + _tool calls_. Tweaking the policy is as
  easy as editing `prompts.py`; no recompile, no state-machine
  refactor.
- **Practicality:** the deterministic sibling
  (`almanak/demo_strategies/xlayer_lp_rebalance`) has been regression
  tested on the X-Layer Anvil fork with real Uniswap V3 transactions.
  The agentic version reuses the same connectors, compiler, signer and
  submitter — so anything the deterministic version can do on-chain,
  this version can do too.
- **AI-agent judge reviewability:** `run.py` and `prompts.py` are
  short, self-contained, and heavily commented. The mock-LLM smoke
  test runs in seconds with no external dependencies.

---

## Related

- **Sister agentic strategy**: [`agent_aave_carry/`](../agent_aave_carry/)
  -- LLM-driven Aave V3.6 carry on X-Layer
- **Deterministic companion**: [`aave_okb_clmm_loop/`](../aave_okb_clmm_loop/)
  -- Aave + Uniswap V3 CLMM loop (same protocol surface, no LLM)
- **Shared agent utilities**: `examples/agentic/shared/` (agent loop, LLM client)
- **Agent tools framework**: `almanak/framework/agent_tools/`
- **Uniswap V3 connector**: `almanak/connectors/uniswap_v3/`
