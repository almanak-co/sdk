# DeFAI Vault + LP Agent

Autonomous agent that deploys a [Lagoon](https://lagoon.finance) vault (ERC-7540), funds it with USDC, opens a concentrated Uniswap V3 LP position using vault funds via a Safe wallet, and manages the position 24/7 -- all driven by an LLM through the Almanak `agent_tools` framework.

This is the flagship DeFAI example: an AI agent that owns and operates a full on-chain fund.

## Architecture

```
                         LLM (GPT-4o / Claude)
                               |
                        tool_calls (OpenAI format)
                               |
                               v
   +----------------------------------------------------------+
   |  run.py                                                   |
   |  +-----------+    +-----------+    +------------------+   |
   |  | LLMClient |    | AgentPolicy|   | config.json      |   |
   |  +-----------+    +-----------+    +------------------+   |
   |       |                |                                  |
   |       v                v                                  |
   |  +-------------------------------------------+           |
   |  | ToolExecutor (28 tools, policy-gated)      |           |
   |  +-------------------------------------------+           |
   +----------------------------------------------------------+
                               |
                          gRPC calls
                               |
   +----------------------------------------------------------+
   |  Gateway sidecar                                          |
   |  +- MarketService    prices, balances, indicators         |
   |  +- ExecutionService  compile -> simulate -> execute      |
   |  +- StateService      save/load agent state               |
   |  +- ObserveService    audit trail                         |
   +----------------------------------------------------------+
                               |
   +----------------------------------------------------------+
   |  Base blockchain (mainnet or Anvil fork)                  |
   |                                                           |
   |  Lagoon Factory  -->  Vault Proxy (ERC-7540)              |
   |  Uniswap V3 NPM  -->  LP Position (NFT)                  |
   |  Safe Wallet      -->  Custody (holds funds + positions)  |
   +----------------------------------------------------------+
```

**Key actors:**

| Actor | Address source | Role |
|-------|---------------|------|
| EOA wallet | `config.json: wallet_address` | Signs vault deployment, NAV proposals, deposits |
| Safe wallet | `config.json: safe_address` | Vault owner, holds funds, signs settlements + LP operations |

## Two Operating Modes

### INIT mode (first boot, no vault exists)

A 10-phase lifecycle that bootstraps the entire on-chain infrastructure:

```
Phase 1   Market Assessment       load state, get prices, get balances
Phase 2   Deploy Vault            create Lagoon vault proxy via factory
Phase 3   Approve Underlying      Safe approves vault for USDC redemptions
Phase 4   Initial Settlement      V0.5.0 requires first settle with total_assets=0
Phase 5   Deposit into Vault      EOA deposits USDC via approve + requestDeposit
Phase 6   Process Deposits        settle to mint shares and move USDC to Safe
Phase 7   Open LP Position        open ALMANAK/USDC LP using Safe's funds
Phase 8   NAV Settlement          report LP value as new total assets
Phase 9   Persist State           save vault address, position ID, pool config
Phase 10  Summary                 final text report of all actions taken
```

### RUNNING mode (24/7, vault exists)

A priority-ordered loop that runs every `interval_seconds` (default: 120s):

```
P0  Teardown       if teardown_requested: close positions, swap to USDC, settle, stop
P1  Settle Vault   if pending deposits/redeems: compute NAV, settle
P2  LP Health      check if position is in-range; rebalance if needed
P3  Deploy Idle    if Safe USDC > threshold: add to LP position
P4  Hold           save state with timestamp, report observations
```

The agent automatically detects which mode to use by checking for persisted state via `load_agent_state`.

## Quick Start

### Smoke test (no API key needed)

Uses a `DynamicMockLLMClient` that replays the 10-phase INIT lifecycle with deterministic tool calls:

```bash
# Terminal 1: Start gateway with Safe mode on Anvil fork
ALMANAK_GATEWAY_SAFE_ADDRESS=0x98aE9CE2606e2773eE948178C3a163fdB8194c04 \
ALMANAK_GATEWAY_SAFE_MODE=direct \
almanak gateway --network anvil

# Terminal 2: Run the mock agent
python examples/agentic/defai_vault_lp/run.py --once --mock
```

### Real LLM

```bash
# Terminal 1: Start gateway
ALMANAK_GATEWAY_SAFE_ADDRESS=0x98aE... \
ALMANAK_GATEWAY_SAFE_MODE=direct \
almanak gateway --network anvil

# Terminal 2: Run with real LLM (single iteration)
AGENT_LLM_API_KEY=sk-... python examples/agentic/defai_vault_lp/run.py --once

# Or run continuously (every 120 seconds)
AGENT_LLM_API_KEY=sk-... python examples/agentic/defai_vault_lp/run.py
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AGENT_LLM_API_KEY` | Yes (real mode) | -- | OpenAI or compatible API key |
| `AGENT_LLM_BASE_URL` | No | `https://api.openai.com/v1` | LLM API endpoint |
| `AGENT_LLM_MODEL` | No | `gpt-4o` | Model to use |
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Yes | -- | Safe wallet for vault operations |
| `ALMANAK_GATEWAY_SAFE_MODE` | Yes | -- | Must be `direct` for vault operations |

### config.json

```jsonc
{
  "chain": "base",                    // Target chain
  "wallet_address": "0x...",          // EOA deployer/valuator
  "safe_address": "0x...",            // Safe wallet (vault owner)
  "pool": "ALMANAK/USDC/3000",       // Target LP pool
  "pool_address": "0x...",            // Uniswap V3 pool address
  "almanak_token": "0x...",           // ALMANAK token address
  "usdc_token": "0x...",              // USDC token address
  "vault": {
    "name": "Almanak DeFAI Vault",    // ERC-7540 vault name
    "symbol": "aALM",                // Vault share symbol
    "underlying_token": "0x..."       // USDC address (vault denomination)
  },
  "lp": {
    "amount_almanak": "100",          // ALMANAK per LP position
    "amount_usdc": "10",              // USDC per LP position
    "range_width_pct": "0.50"         // LP range width (50% above/below)
  },
  "deposit": {
    "amount_usdc_raw": "10000000",    // 10 USDC in raw units (6 decimals)
    "min_deploy_threshold_usdc_raw": "5000000"  // Min USDC to deploy to LP
  },
  "rebalance": {
    "min_rebalance_interval_minutes": 30,  // Cooldown between rebalances
    "range_edge_pct": 0.15,           // Rebalance when price is within 15% of edge
    "out_of_range_max_iterations": 2  // Max iterations out of range before force rebalance
  },
  "policy": {
    "max_single_trade_usd": "10000",  // Per-trade spend cap
    "max_daily_spend_usd": "50000",   // Daily spend cap
    "cooldown_seconds": 0,            // Min seconds between trades
    "max_trades_per_hour": 30         // Rate limit
  },
  "strategy_id": "defai-vault-lp",    // State persistence key
  "max_tool_rounds": 15,              // Max LLM rounds per iteration
  "interval_seconds": 120             // Loop interval (continuous mode)
}
```

## Tools Used

The agent has access to 17 tools (out of 28 in the catalog), scoped by its `AgentPolicy`:

| Category | Tools | Description |
|----------|-------|-------------|
| DATA | `get_price`, `get_balance`, `get_vault_state`, `get_pool_state`, `get_lp_position`, `get_indicator`, `resolve_token` | Read-only market and position data |
| PLANNING | `compute_rebalance_candidate`, `simulate_intent` | Evaluate trades before execution |
| ACTION (Vault) | `deploy_vault`, `approve_vault_underlying`, `deposit_vault`, `settle_vault` | Vault lifecycle operations |
| ACTION (LP) | `open_lp_position`, `close_lp_position`, `swap_tokens` | LP management |
| STATE | `save_agent_state`, `load_agent_state`, `record_agent_decision` | Persistence and audit trail |

## Files

| File | Purpose |
|------|---------|
| `run.py` | Entry point, gateway setup, executor wiring, mock LLM, main loop |
| `config.json` | Strategy parameters (chain, tokens, pool, policy limits) |
| `prompts.py` | System prompts for INIT and RUNNING modes with tool documentation |

## How It Differs from AgentLP / AgentYield

| Aspect | AgentLP / AgentYield | DeFAI Vault LP |
|--------|---------------------|----------------|
| **Wallet** | EOA only | EOA + Safe (dual-signer) |
| **Fund structure** | Direct wallet trading | Lagoon vault (ERC-7540) with shares |
| **Lifecycle** | Simple: read -> trade -> save | 10-phase init + 5-priority running loop |
| **Settlement** | None | NAV proposal + deposit/redeem settlement |
| **Chain** | Avalanche | Base |
| **Complexity** | ~100 lines of agent logic | ~300 lines + structured prompts |
| **Mock LLM** | Static responses | `DynamicMockLLMClient` with context-aware 10-round flow |
