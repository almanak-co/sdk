# X-Layer DeFi Strategies -- Build-X Hackathon

> **X-Layer Build-X Hackathon** -- X-Layer Arena track
> _Season: April 1-15, 2026_

Three autonomous DeFi strategies built on X-Layer using the
[Almanak SDK](https://github.com/almanak-finance/almanak-sdk), submitted
as **two hackathon projects**: one agentic (LLM-driven) and one
deterministic (rule-based). Both use the same execution infrastructure
but differ in how decisions are made.

---

## Hackathon submissions

### Project #1: [Agentic Strategies](xlayer_agentic/) -- LLM-driven DeFi

Two AI agent strategies where an LLM reasons about market data and
calls typed tools to manage on-chain positions. The model decides
*what* to do; the Almanak gateway handles *how* to execute it safely.

| Strategy | Protocol | What it does |
|----------|----------|-------------|
| [agent_aave_carry](xlayer_agentic/agent_aave_carry/) | Aave V3.6 | Supply/borrow carry with autonomous deleverage |
| [agent_lp_rebalance](xlayer_agentic/agent_lp_rebalance/) | Uniswap V3 | Concentrated LP lifecycle with autonomous rebalance |

**Wallet 1**: `0x6BA553d60E8515E9b4026e377da72Dd379E78daD` (Aave carry)
**Wallet 2**: `0x062678Adef2Ffc999296f86D4ed40Ce24359976d` (LP rebalance)

### Project #2: [Deterministic Strategy](xlayer_deterministic/) -- Multi-protocol yield loop

A production-grade `IntentStrategy` that chains **Aave V3.6 + Uniswap
V3** into a supply -> borrow -> CLMM yield loop with adaptive
auto-rebalance driven by realized volatility. No LLM, no
non-determinism -- pure algorithmic execution.

| Strategy | Protocols | What it does |
|----------|-----------|-------------|
| [aave_okb_clmm_loop](xlayer_deterministic/aave_okb_clmm_loop/) | Aave V3.6 + Uniswap V3 | Borrow-funded concentrated LP with vol-adaptive range |

**Wallet 3**: `0xc48E245cc551bd6853EeB1c3068C10eA8856D6ad`

---

## Proven on mainnet

The deterministic strategy was deployed on xlayer mainnet (April 11-13,
2026) and demonstrated the full lifecycle:

- **Entry**: 5 intents (supply, borrow, 2 swaps, LP open) -- all SUCCESS
- **Monitoring**: 88 hours of live fee accumulation at 17-19% net APR
- **Teardown**: 7 intents (LP close, 3 swaps, repay, 2 withdrawals) -- all SUCCESS
- **Backtest**: 90-day historical simulation with 63-config parameter sweep

See `xlayer_deterministic/aave_okb_clmm_loop/README.md` for full
execution logs, economics, and backtesting results.

---

## Architecture

All three strategies share the same execution pipeline:

```text
Strategy (LLM or state machine)
        |
        v
  Almanak Gateway (gRPC)
        |
    +---+---+
    |       |
MarketSvc  ExecutionSvc
    |       |
    v       v
 Prices   X-Layer chain
           (Aave V3.6 + Uniswap V3)
```

The **agentic strategies** use an LLM (GPT-4o, Claude, or local Ollama)
that calls typed tools through an `AgentPolicy` gate. The policy
enforces chain/token/tool allowlists, per-trade caps, and cooldowns.

The **deterministic strategy** uses the standard `IntentStrategy` base
class with an intent compiler that converts high-level intents (Swap,
Supply, Borrow, LPOpen) into signed transactions.

**Wallet isolation**: each strategy uses a dedicated wallet with its own
`.env` file containing an isolated private key. Keys are
deterministically derived from a master key so they can be reproduced
without a seed phrase.

---

## Quick start

```bash
# 1. Install the SDK
uv sync  # or: pip install -e .

# 2. Fund the wallets (OKB for gas + strategy tokens)
#    Wallet 1 (Aave carry):  OKB + USDT0
#    Wallet 2 (LP rebalance): OKB + WOKB + USDT
#    Wallet 3 (CLMM loop):   OKB + USDT0 ($200)

# 3. Run agentic strategies (mock LLM, no API key needed)
python strategies/xlayer/xlayer_agentic/agent_aave_carry/run.py --once --mock
python strategies/xlayer/xlayer_agentic/agent_lp_rebalance/run.py --once --mock

# 4. Run agentic with real LLM
AGENT_LLM_API_KEY=sk-... \
  python strategies/xlayer/xlayer_agentic/agent_aave_carry/run.py --once

# 5. Run deterministic strategy
almanak strat run \
  -d strategies/xlayer/xlayer_deterministic/aave_okb_clmm_loop \
  --network mainnet --interval 30 --fresh

# 6. Backtest the deterministic strategy
uv run python strategies/xlayer/xlayer_deterministic/aave_okb_clmm_loop/backtest.py --sweep
```

---

## File layout

```
strategies/xlayer/
  README.md                              # this file (umbrella overview)
  xlayer_agentic/                        # Hackathon Project #1
    README.md                            # project-level docs
    agent_aave_carry/                    # Strategy 1: LLM Aave carry
      README.md, config.json, prompts.py, run.py, .env
    agent_lp_rebalance/                  # Strategy 2: LLM LP rebalance
      README.md, config.json, prompts.py, run.py, .env
  xlayer_deterministic/                  # Hackathon Project #2
    README.md                            # project-level docs
    aave_okb_clmm_loop/                  # Strategy 3: Deterministic CLMM loop
      README.md, config.json, strategy.py, backtest.py, .env
```

---

## Safety model (shared across all strategies)

1. **Policy allowlist** -- LLMs only see tools they are allowed to use
2. **Chain lock** -- all strategies are locked to `xlayer`
3. **Wallet isolation** -- each strategy has a dedicated wallet
4. **Per-trade and daily USD caps** -- enforced before signing
5. **Cooldowns and rate limits** -- prevent rapid-fire transactions
6. **Teardown mechanism** -- every strategy can cleanly unwind all positions
7. **Mock LLM support** -- `--mock` flag for deterministic smoke tests
