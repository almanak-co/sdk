# X-Layer Agentic Strategies

> **X-Layer Build-X Hackathon submission** — Project #1: Agentic DeFi
> _Season: April 1-15, 2026_

Two autonomous AI agent strategies that manage real on-chain positions
on X-Layer using **Aave V3.6** and **Uniswap V3**, driven by LLM
reasoning at runtime instead of hardcoded state machines.

---

## Strategies

### 1. Agentic Aave Carry (`agent_aave_carry/`)

An LLM-driven supply/borrow carry trade on Aave V3.6. The agent
supplies USDT0 collateral, borrows USDG, monitors health factor, and
autonomously deleverages if the position drifts into unsafe territory.

- **Protocol**: Aave V3.6 (governance proposal #460)
- **Tokens**: USDT0 (collateral), USDG (debt)
- **AI model**: OpenAI GPT-4o / Claude / local Ollama
- **Safety**: AgentPolicy allowlist, per-trade USD caps, cooldowns

### 2. Agentic LP Rebalance (`agent_lp_rebalance/`)

An LLM-driven concentrated LP lifecycle manager on the WOKB/USDT
Uniswap V3 pool. The agent opens a position, monitors price vs range,
and autonomously rebalances when the position goes out of range.

- **Protocol**: Uniswap V3 (governance proposal #67)
- **Pool**: WOKB/USDT/3000
- **AI model**: OpenAI GPT-4o / Claude / local Ollama
- **Safety**: AgentPolicy allowlist, per-trade USD caps, cooldowns

---

## Wallet

| Strategy | Wallet | Funding needed |
|----------|--------|----------------|
| agent_aave_carry | `0x6BA553d60E8515E9b4026e377da72Dd379E78daD` | OKB (gas) + USDT0 |
| agent_lp_rebalance | `0x062678Adef2Ffc999296f86D4ed40Ce24359976d` | OKB (gas) + WOKB + USDT |

Each strategy has its own `.env` with an isolated private key. Wallets
are deterministically derived from a master key so they can be
reproduced without a seed phrase.

---

## How to run

### Prerequisites

- Python >= 3.12 with Almanak SDK (`uv sync` from repo root)
- Foundry (`anvil`) for local testing
- LLM API key (OpenAI, Anthropic, or local Ollama)
- X-Layer RPC access

### Agentic Aave Carry

```bash
# Smoke test with mock LLM (no API key needed)
python strategies/xlayer/xlayer_agentic/agent_aave_carry/run.py --once --mock

# Run with real LLM on Anvil
almanak gateway --network anvil --chain xlayer
AGENT_LLM_API_KEY=sk-... python strategies/xlayer/xlayer_agentic/agent_aave_carry/run.py --once

# Run on mainnet
AGENT_LLM_API_KEY=sk-... python strategies/xlayer/xlayer_agentic/agent_aave_carry/run.py
```

### Agentic LP Rebalance

```bash
# Smoke test
python strategies/xlayer/xlayer_agentic/agent_lp_rebalance/run.py --once --mock

# Run with real LLM
AGENT_LLM_API_KEY=sk-... python strategies/xlayer/xlayer_agentic/agent_lp_rebalance/run.py
```

---

## Architecture

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
    |  Almanak Gateway  (MarketService, ExecutionService) |
    +-------------------------+--------------------------+
                              |
                      JSON-RPC + signing
                              |
    +---------v---------+             +---------v---------+
    |   X-Layer chain   |             |   X-Layer chain   |
    |   Aave V3.6 Pool  |             |  Uniswap V3 NPM   |
    +-------------------+             +-------------------+
```

The LLM never touches private keys or sends raw transactions. It calls
typed tools (`supply_lending`, `open_lp_position`, etc.) that flow
through the `AgentPolicy` allowlist before reaching the gateway. The
gateway handles signing, simulation, and submission.

---

## Why agentic?

The same strategies exist as deterministic Python state machines
(`almanak/demo_strategies/xlayer_aave_carry` and
`xlayer_lp_rebalance`). The agentic versions replace the hardcoded
decision logic with natural-language reasoning:

- **Adaptable**: a risk manager can tweak behavior by editing the
  prompt, not Python code
- **Explainable**: every decision includes a one-line LLM rationale
  written to the audit trail
- **Composable**: the same tool catalog can power new strategies
  without new code

The trade-off: LLM inference latency (~1-3s) and non-determinism.
For production use at scale, the deterministic sibling
(`xlayer_deterministic/`) is more reliable.

---

## Related

- **Deterministic sibling project**: `strategies/xlayer/xlayer_deterministic/`
- **Shared agent utilities**: `examples/agentic/shared/`
- **Agent tools framework**: `almanak/framework/agent_tools/`
- **Individual strategy READMEs**: see `agent_aave_carry/README.md` and
  `agent_lp_rebalance/README.md` for detailed documentation
