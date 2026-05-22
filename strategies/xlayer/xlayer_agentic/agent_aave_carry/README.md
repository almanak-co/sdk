# Agentic Aave V3.6 Carry on X-Layer

> **X-Layer Build-X Hackathon submission** — X-Layer Arena track
> _Season: April 1–15, 2026_

An autonomous AI agent that runs a **supply / borrow carry** on
**Aave V3.6** on X-Layer. The agent supplies stablecoin collateral
(USDT0), borrows another stable against it (USDG) at a configurable
LTV, monitors health factor across iterations, and **deleverages
itself** if the position drifts into unsafe territory.

This is the LLM-driven counterpart of the deterministic reference
strategy `almanak/demo_strategies/xlayer_aave_carry`. Same on-chain
footprint, same Aave market, same economics — but the policy logic is
reasoned by an LLM at runtime instead of encoded in a hand-rolled
Python state machine.

---

## Why this matters for X-Layer

- **Fully deployed on X-Layer.** Every on-chain action (approvals,
  `supply`, `borrow`, `repay`, swaps) hits the `xlayer` network
  through the Almanak execution gateway. The strategy is locked to
  X-Layer by `AgentPolicy.allowed_chains={"xlayer"}`.
- **First-class use of the new X-Layer Aave V3.6 deployment.** Aave
  V3.6 was deployed to X-Layer via governance proposal **#460**. This
  agent uses the production `aave_v3` connector against that exact
  deployment — the same `Pool` address humans interact with through the
  Aave UI.
- **Autonomous carry management.** The agent doesn't just _open_ the
  carry; it comes back every `interval_seconds`, recomputes a
  health-factor proxy from live prices, and deleverages by calling
  `repay_lending` if the proxy dips below the configured
  `min_health_factor`.
- **Policy-constrained by design.** The LLM runs inside a tight
  `AgentPolicy` allowlist — only the `xlayer` chain, only the
  USDT0/USDG/USDT/USDC/WOKB/OKB token set, only a small set of tools,
  per-trade and daily USD caps, cooldowns between actions. The model
  cannot rug its own wallet.

---

## X-Layer Aave V3.6 facts the agent operates on

The X-Layer Aave deployment is smaller than mainnet and has some
quirks. The prompt encodes these as hard rules so the LLM cannot pick
invalid parameters:

| Reserve | LTV | Notes |
|---------|-----|-------|
| **USDT0** (`0x779Ded…`) | 70% | Primary stablecoin collateral. Used by this agent. |
| **xETH** (`0xE7B000…`) | 70% | Borrowable but very limited pool liquidity. |
| **xBTC** (`0xb7C000…`) | 70% | Borrowable. |
| **WOKB** | **0%** | Cannot be used as collateral on X-Layer Aave. |
| **USDG / GHO** | — | Borrow-side only. **USDG is the borrow target of this agent.** |

Governance proposal: [Aave proposal #460](https://app.aave.com/governance/v3/proposal/?proposalId=460).

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
                    |   Aave V3.6 Pool  |
                    | USDT0 -> USDG     |
                    +-------------------+
```

**Decision loop (one iteration):**

1. **Load state** — `load_agent_state` recovers the last-known phase
   (`idle`, `supplied`, or `carry_open`) from gateway-backed storage.
2. **Read the market** —
   - `get_balance(USDT0)` — confirm the wallet is funded.
   - `get_price(USDT0)` and `get_price(USDG)` — both should be ≈ $1
     but the agent reads them live rather than assuming.
3. **Branch on phase**:
   - _No state?_ → `supply_lending(USDT0, min(balance, 4.0))` with
     `use_as_collateral=true`. Save `phase=supplied`.
   - _Supplied, not yet borrowed?_ → compute
     `borrow_amount = supplied * supply_price * ltv_target / borrow_price`
     and call `borrow_lending(collateral=USDT0, borrow=USDG,
     interest_rate_mode=variable)`. Save `phase=carry_open`.
   - _Carry open?_ → compute the health-factor proxy
     `hf = supplied * supply_price * 0.70 / (borrowed * borrow_price)`.
     If `hf < min_health_factor`, call `repay_lending(USDG, partial)`
     to deleverage. Otherwise hold.
4. **Persist** — `save_agent_state`, then `record_agent_decision` with
   a one-line rationale.
5. **Return** a human-readable text summary to the operator.

> **Scope note.** This agent manages supply / borrow / repay. Full
> collateral teardown (withdrawing the `aToken` supply) is out of
> scope for the current `agent_tools` catalog — use the deterministic
> sibling (`almanak/demo_strategies/xlayer_aave_carry`) when you need
> to fully unwind the position. The agent is designed as a long-running
> carry manager, not a one-shot open-and-close script.

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
USDT0), and exposes the gRPC services the agent will use.

### Step 2 — smoke test with the mock LLM (no API key)

```bash
# In terminal 2
python strategies/xlayer/agent_aave_carry/run.py --once --mock
```

The `--mock` flag swaps in a scripted `MockLLMClient` that replays a
deterministic sequence (`load_state → get_balance → get_price × 2 →
supply_lending → borrow_lending → save_state + record_decision →
text reply`). Useful for CI, judges, and offline review.

### Step 3 — run with a real LLM

```bash
# OpenAI
AGENT_LLM_API_KEY=sk-... \
  python strategies/xlayer/agent_aave_carry/run.py --once

# Anthropic via proxy
AGENT_LLM_API_KEY=sk-ant-... \
  AGENT_LLM_BASE_URL=https://api.anthropic.com/v1 \
  AGENT_LLM_MODEL=claude-opus-4-6 \
  python strategies/xlayer/agent_aave_carry/run.py --once

# Local Ollama
AGENT_LLM_API_KEY=dummy \
  AGENT_LLM_BASE_URL=http://localhost:11434/v1 \
  AGENT_LLM_MODEL=llama3.1 \
  python strategies/xlayer/agent_aave_carry/run.py --once
```

Drop the `--once` flag to run continuously every `interval_seconds`
(120 seconds by default).

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
| `get_indicator` | Fetch technical indicators (e.g. RSI) to inform deleverage timing. |

### Action tools (on-chain, policy-gated)

| Tool | Purpose |
|------|---------|
| `supply_lending` | Supply USDT0 to Aave V3.6 as collateral (`use_as_collateral=true`). |
| `borrow_lending` | Borrow USDG against the USDT0 collateral with variable rate. |
| `repay_lending` | Deleverage path — repay part or all of the USDG debt. |
| `swap_tokens` | Escape hatch if the wallet needs to acquire extra USDG for repay. |

### State tools (persistence + audit)

| Tool | Purpose |
|------|---------|
| `save_agent_state` | Persist `{phase, supplied, borrowed, ...}` via `StateService`. |
| `load_agent_state` | Recover the last-known phase on restart. |
| `record_agent_decision` | Write a one-line rationale to the audit trail. |

All tool calls flow through `AgentPolicy.validate()` before execution.
The policy enforces:

- `allowed_chains={"xlayer"}`
- `allowed_tokens={"USDT0", "USDG", "USDT", "USDC", "WOKB", "OKB"}`
- `max_single_trade_usd = $500`
- `max_daily_spend_usd = $2000`
- `cooldown_seconds = 5`
- `max_trades_per_hour = 20`

Tools outside the allowlist (`bridge_tokens`, `deploy_vault`,
`open_lp_position`, etc.) are simply not exposed to the LLM — the
OpenAI tool manifest only contains the 10 tools above.

---

## Configuration (`config.json`)

```json
{
    "chain": "xlayer",
    "wallet_address": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    "supply_token": "USDT0",
    "borrow_token": "USDG",
    "initial_supply_amount": "4.0",
    "ltv_target": "0.5",
    "min_health_factor": "1.5",
    "collateral_factor": "0.70",
    "interest_rate_mode": "variable",
    "interval_seconds": 120,
    "max_tool_rounds": 12,
    "deployment_id": "agent-xlayer-aave-carry",
    "skip_llm_enabled": true,
    "skip_llm_when_hf_above": "2.0",
    "anvil_funding": {
        "OKB": 100, "WOKB": 10, "USDT0": 10000
    }
}
```

| Key | Meaning |
|-----|---------|
| `supply_token` | Collateral asset. Must be an X-Layer Aave reserve with `LTV > 0` (USDT0, xETH, xBTC). |
| `borrow_token` | Debt asset. Defaults to USDG (borrow-side only on X-Layer Aave). |
| `initial_supply_amount` | Cap on the collateral leg. |
| `ltv_target` | Target loan-to-value. `0.5` means the agent borrows up to 50% of the collateral's dollar value. |
| `min_health_factor` | Safety floor. If the agent's HF proxy dips below this, the next iteration triggers `repay_lending`. |
| `interest_rate_mode` | Must be `"variable"` — Aave V3 deprecated stable rate. |
| `interval_seconds` | Loop period in non-`--once` mode. |
| `max_tool_rounds` | Safety cap on tool calls per iteration — prevents runaway LLM loops. |
| `deployment_id` | Opaque id used for state persistence. |
| `collateral_factor` | On-chain LTV for the supply token on Aave V3.6 X-Layer (0.70 for USDT0). Used by the pre-LLM health-factor guard. |
| `skip_llm_enabled` | When `true`, a deterministic health-factor check can skip the LLM call if the position is healthy. Saves API cost. |
| `skip_llm_when_hf_above` | HF threshold above which the LLM is skipped entirely (position is comfortable). |
| `anvil_funding` | Initial wallet funding for local Anvil fork smoke tests. |

---

## Safety model

1. **Policy allowlist** — the LLM never sees any tool it shouldn't use.
2. **Per-trade and daily USD caps** — enforced before the gateway ever
   signs a tx.
3. **Cooldowns and rate limits** — the agent cannot trade more than
   20 times per hour, with a 5-second cooldown between actions.
4. **Aave reserve hard rules in the prompt** — the system prompt
   encodes the X-Layer reserve matrix (WOKB has LTV=0, USDG is
   borrow-only, stable-rate is deprecated). The LLM is told exactly
   which assets are eligible for which side of the carry.
5. **Simulation path** — every action tool accepts an optional
   `dry_run=true` flag that routes through the Almanak simulator
   without broadcasting.
6. **Health-factor proxy** — every iteration recomputes a conservative
   `hf = supplied * supply_price * 0.70 / (borrowed * borrow_price)`
   from live prices. A drop below `min_health_factor` triggers an
   automatic `repay_lending` call.
7. **Pre-LLM skip guard** — when `skip_llm_enabled=true`, a
   deterministic health-factor check runs _before_ calling the LLM. If
   the carry is healthy (`hf_proxy >= skip_llm_when_hf_above`), the
   iteration is a no-op — no LLM tokens consumed, no on-chain actions.
   Fail-open: any error in the guard falls through to the LLM.
8. **Circuit breaker** — consecutive tool errors halt the agent loop.
9. **Structured error envelopes** — the LLM never sees raw gRPC
   exceptions, only typed `ToolError` objects with `recoverable` flags.
10. **Auditability** — every decision is tagged with `deployment_id` and
   written to the `ObserveService` audit trail via
   `record_agent_decision`.

---

## File layout

```
strategies/xlayer/agent_aave_carry/
├── README.md       # this file
├── __init__.py
├── config.json     # chain, tokens, supply amount, LTV, HF floor
├── prompts.py      # SYSTEM_PROMPT (Aave reserve rules + decision tree)
└── run.py          # entrypoint: gateway + ToolExecutor + agent loop
```

The run script reuses the shared agent utilities from
`examples/agentic/shared/` (`agent_loop.py`, `llm_client.py`) so there
is no duplicated LLM plumbing.

---

## Hackathon relevance

- **Track:** X-Layer Arena — full-stack agentic application.
- **X-Layer deployment:** all on-chain actions target the `xlayer`
  network by policy; no cross-chain paths are allowed. The agent
  drives the live Aave V3.6 deployment that shipped via governance
  proposal #460.
- **Creativity:** a stablecoin carry is usually implemented as a
  deterministic `Supply → Borrow → Hold → Repay → Withdraw` state
  machine. This version exposes the same economic surface as a
  _natural-language policy_ plus _typed tools_. A risk manager can
  adjust `ltv_target` or `min_health_factor` without touching Python;
  a quant can rewrite the deleverage trigger rule in English.
- **Practicality:** the deterministic sibling
  (`almanak/demo_strategies/xlayer_aave_carry`) has been regression
  tested on the X-Layer Anvil fork with real Aave V3.6 transactions
  (supply, borrow, repay, withdraw). The agentic version reuses the
  same connectors, compiler, signer and submitter — so anything the
  deterministic version can do on-chain, this version can do too
  (minus the final `withdraw`, which is not yet in the tool catalog).
- **AI-agent judge reviewability:** `run.py` and `prompts.py` are
  short, self-contained, and heavily commented. The mock-LLM smoke
  test runs in seconds with no external dependencies.

---

## Related

- **Sister agentic strategy**: [`agent_lp_rebalance/`](../agent_lp_rebalance/)
  -- LLM-driven Uniswap V3 LP rebalance on X-Layer
- **Deterministic companion**: [`aave_okb_clmm_loop/`](../aave_okb_clmm_loop/)
  -- Aave + Uniswap V3 CLMM loop (same protocol surface, no LLM)
- **Shared agent utilities**: `examples/agentic/shared/` (agent loop, LLM client)
- **Agent tools framework**: `almanak/framework/agent_tools/`
- **Aave V3 connector**: `almanak/framework/connectors/aave_v3/`
