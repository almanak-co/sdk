# Cross-Chain Arbitrage Strategy

> Cross-chain arbitrage with comprehensive fee and latency accounting

This strategy monitors token prices across multiple chains and executes arbitrage when price spreads exceed configurable thresholds **after accounting for all fees and risks**.

## Overview

```
OPTIMISM (CHEAPER)                         ARBITRUM (MORE EXPENSIVE)
──────────────────                         ─────────────────────────

┌──────────────┐                           ┌──────────────┐
│ 1. BUY       │                           │ 3. SELL      │
│ USDC → ETH   │                           │ ETH → USDC   │
│ (Uniswap V3) │                           │ (Uniswap V3) │
└──────┬───────┘                           └──────────────┘
       │                                          ▲
       │       ┌──────────────┐                   │
       └──────►│ 2. BRIDGE    │───────────────────┘
               │ ETH Opt → Arb│
               │ (Across)     │
               └──────────────┘
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Multi-chain monitoring** | Arbitrum, Optimism, Base (configurable) |
| **Bridge fee accounting** | Subtracts bridge fees from profit calculation |
| **Latency risk assessment** | Checks if bridge time exceeds acceptable limits |
| **Net profit calculation** | Only executes if profitable after ALL fees |
| **State machine** | MONITORING → OPPORTUNITY_FOUND → COOLDOWN |
| **Multi-Anvil support** | Testable on local multi-chain forks |

## Profitability Calculation

The strategy calculates **net profit** by accounting for:

```python
# Raw spread (e.g., 1% = 100 bps)
raw_spread_bps = abs(price_chain_a - price_chain_b) / min_price * 10000

# Total fees
total_fees_bps = (
    bridge_fee_bps         # 10-50 bps depending on provider
    + swap_slippage * 2    # 30 bps each for buy + sell = 60 bps
    + bridge_slippage      # 50 bps
)

# Net profit
net_profit_bps = raw_spread_bps - total_fees_bps

# USD profit
profit_usd = (trade_amount * net_profit_bps / 10000) - gas_costs
```

### Bridge Fee Estimates

| Bridge | Fee (bps) | Latency |
|--------|-----------|---------|
| Across | 10 | ~2 min |
| Stargate | 15 | ~10 min |
| Hop | 20 | ~10 min |
| cBridge | 25 | ~15 min |
| Synapse | 30 | ~10 min |

## Configuration

```python
from strategies.cross_chain_arbitrage import CrossChainArbConfig

config = CrossChainArbConfig(
    # === Chains ===
    chains=["arbitrum", "optimism", "base"],

    # === Token ===
    quote_token="ETH",      # Token to arbitrage
    base_token="USDC",      # Base currency

    # === Thresholds ===
    min_spread_bps=50,              # 0.5% raw spread minimum
    min_spread_after_fees_bps=10,   # 0.1% net profit minimum

    # === Bridge ===
    bridge_provider=None,           # None = auto-select
    max_bridge_latency_seconds=900, # 15 min max
    account_for_bridge_fees=True,

    # === Risk ===
    max_slippage_swap=Decimal("0.003"),    # 0.3%
    max_slippage_bridge=Decimal("0.005"),  # 0.5%

    # === Position Sizing ===
    trade_amount_usd=Decimal("1000"),
    min_balance_usd=Decimal("100"),

    # === Gas ===
    estimated_swap_gas_usd=Decimal("5"),
    estimated_bridge_gas_usd=Decimal("10"),

    # === Cooldown ===
    cooldown_seconds=60,
)
```

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `chains` | ["arbitrum", "optimism", "base"] | Chains to monitor |
| `quote_token` | "ETH" | Token to arbitrage |
| `base_token` | "USDC" | Base currency for trades |
| `min_spread_bps` | 50 | Minimum raw spread (0.5%) |
| `min_spread_after_fees_bps` | 10 | Minimum net profit (0.1%) |
| `bridge_provider` | None | Preferred bridge (None = auto) |
| `account_for_bridge_fees` | True | Include bridge fees in calculations |
| `max_slippage_swap` | 0.003 | Max swap slippage (0.3%) |
| `max_slippage_bridge` | 0.005 | Max bridge slippage (0.5%) |
| `trade_amount_usd` | 1000 | Trade size in USD |
| `cooldown_seconds` | 60 | Time between trades |

## Usage

### 1. Environment Setup

```bash
# Set per-chain RPC URLs
export ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_OPTIMISM_RPC_URL=https://opt-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/YOUR_KEY

# Set wallet
export ALMANAK_PRIVATE_KEY=0x...
```

### 2. Run Strategy

```bash
# Dry run (shows what would happen without execution)
python -m src.cli.run --strategy cross_chain_arbitrage --dry-run

# Live execution
python -m src.cli.run --strategy cross_chain_arbitrage
```

### 3. Multi-Anvil Testing

The strategy supports testing on multi-Anvil forks using the `scripts/multi_anvil.py` tool:

```bash
# Start Anvil forks for all chains
python scripts/multi_anvil.py start --chains arbitrum optimism base

# Check fork status
python scripts/multi_anvil.py status

# Fund test wallet
python scripts/multi_anvil.py fund --address 0xYourAddress --amount 10

# Run strategy against forks
export ALMANAK_ARBITRUM_RPC_URL=http://localhost:8545
export ALMANAK_OPTIMISM_RPC_URL=http://localhost:8546
export ALMANAK_BASE_RPC_URL=http://localhost:8547
python -m src.cli.run --strategy cross_chain_arbitrage

# Cleanup when done
python scripts/multi_anvil.py cleanup
```

## State Machine

```
                    ┌─────────────────┐
                    │   MONITORING    │◄──────────────┐
                    └────────┬────────┘               │
                             │                        │
                   [opportunity found]                │
                             │                        │
                    ┌────────▼────────┐               │
                    │ OPPORTUNITY_    │               │
                    │ FOUND           │               │
                    └────────┬────────┘               │
                             │                        │
                     [execute trade]                  │
                             │                        │
                    ┌────────▼────────┐               │
                    │   COOLDOWN      │───────────────┘
                    └─────────────────┘
                       [after 60s]
```

## API

### Strategy Methods

```python
strategy = CrossChainArbitrageStrategy(config)

# Get current state
state = strategy.get_state()  # ArbState enum

# Get current opportunity (if any)
opportunity = strategy.get_current_opportunity()  # CrossChainOpportunity or None

# Get statistics
stats = strategy.get_stats()
# Returns: {
#     "state": "monitoring",
#     "total_trades": 5,
#     "failed_trades": 0,
#     "total_profit_usd": "127.50",
#     "cooldown_remaining": 0,
#     ...
# }

# Calculate expected fees for a trade
fees = strategy.calculate_expected_fees(bridge="across")
# Returns: {
#     "bridge_fee_bps": 10,
#     "swap_slippage_bps": 60,
#     "bridge_slippage_bps": 50,
#     "total_fees_bps": 120,
# }

# Clear state (for testing)
strategy.clear_state()
```

### Config Methods

```python
config = CrossChainArbConfig()

# Get bridge fee for a provider
fee = config.get_bridge_fee_bps("across")  # 10

# Get bridge latency
latency = config.get_bridge_latency_seconds("across")  # 120

# Calculate total fees
total = config.calculate_total_fees_bps("across")  # 120

# Calculate net profit
net = config.calculate_net_profit_bps(spread_bps=150, bridge="across")  # 30

# Check profitability
is_profitable = config.is_profitable(spread_bps=150, bridge="across")  # True

# Estimate USD profit
profit = config.estimate_profit_usd(
    spread_bps=150,
    amount_usd=Decimal("10000"),
    bridge="across"
)  # ~$10 after gas
```

## Risk Considerations

| Risk | Mitigation |
|------|------------|
| **Bridge latency** | Price may move during 2-15 min bridge time |
| **Swap slippage** | Configurable max slippage protects against bad execution |
| **Bridge failure** | Framework handles retries; funds return to source chain |
| **Gas spikes** | Estimated gas costs subtracted from profit |
| **Front-running** | Bridge transactions are harder to front-run than DEX swaps |
| **Sequencer downtime** | L2s may have sequencer issues - monitor chain health |

### Recommended Settings by Risk Profile

**Conservative:**
```python
CrossChainArbConfig(
    min_spread_bps=100,              # 1% minimum raw spread
    min_spread_after_fees_bps=30,    # 0.3% minimum net profit
    trade_amount_usd=Decimal("500"),
    bridge_provider="across",        # Fast finality
    cooldown_seconds=120,            # 2 minute cooldown
)
```

**Aggressive:**
```python
CrossChainArbConfig(
    min_spread_bps=30,               # 0.3% minimum raw spread
    min_spread_after_fees_bps=5,     # 0.05% minimum net profit
    trade_amount_usd=Decimal("10000"),
    bridge_provider=None,            # Auto-select
    cooldown_seconds=30,             # 30 second cooldown
)
```

## Code Structure

```
strategies/cross_chain_arbitrage/
├── __init__.py              # Package exports
├── config.py                # CrossChainArbConfig with fee calculations
├── strategy.py              # CrossChainArbitrageStrategy implementation
├── README.md                # This file
└── tests/
    └── test_strategy.py     # Unit tests
```

## Related Documentation

- [Multi-Anvil Test Harness](../../scripts/README_MULTI_ANVIL.md) - Multi-chain fork testing
- [Cross-DEX Arbitrage](../cross_dex_arb/README.md) - Same-chain DEX arbitrage
- [Bridge Adapters](../../src/connectors/bridges/README.md) - Bridge integration details
