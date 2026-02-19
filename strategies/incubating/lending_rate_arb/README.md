# Lending Rate Arbitrage Strategy

A DeFi strategy that captures lending rate differentials across protocols by automatically moving capital to the highest-yielding venue.

## Overview

This strategy monitors supply APY rates across Aave V3, Morpho Blue, and Compound V3, and executes atomic rebalancing when the rate spread exceeds a configurable threshold.

**Example Scenario:**

If USDC supply APY is:
- Aave V3: 4.2%
- Morpho Blue: 5.1%
- Compound V3: 3.8%

And capital is currently in Compound V3, the strategy will:
1. Withdraw USDC from Compound V3
2. Supply USDC to Morpho Blue

(Only if spread > `min_spread_bps` threshold)

## Configuration Parameters

### Cold Parameters (require restart)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `strategy_id` | str | "" | Unique identifier for the strategy instance |
| `chain` | str | "ethereum" | Target blockchain network |
| `wallet_address` | str | "" | Wallet address for transactions |
| `tokens` | List[str] | ["USDC", "USDT", "DAI", "WETH"] | Tokens to monitor for arbitrage |
| `protocols` | List[str] | ["aave_v3", "morpho_blue", "compound_v3"] | Protocols to compare rates |

### Hot-Reloadable Parameters (can change at runtime)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_spread_bps` | int | 50 | Minimum rate spread in basis points (50 = 0.5%) |
| `rebalance_threshold_usd` | Decimal | 100 | Minimum USD amount to trigger rebalance |
| `check_interval_seconds` | int | 60 | How often to check for opportunities |
| `max_position_usd` | Decimal | 100,000 | Maximum position size per token |
| `max_slippage` | Decimal | 0.005 | Maximum allowed slippage (0.5%) |
| `pause_strategy` | bool | False | Pause strategy execution |

## Usage

```python
from strategies.lending_rate_arb import (
    LendingRateArbStrategy,
    LendingRateArbConfig,
)
from src.data.rates import RateMonitor

# Create configuration
config = LendingRateArbConfig(
    strategy_id="lending_arb_eth_1",
    chain="ethereum",
    wallet_address="0xYourWallet",
    tokens=["USDC", "USDT", "DAI"],
    protocols=["aave_v3", "morpho_blue", "compound_v3"],
    min_spread_bps=50,  # 0.5% minimum spread
    rebalance_threshold_usd=Decimal("1000"),  # Min $1000 to move
)

# Initialize strategy
strategy = LendingRateArbStrategy(config=config)

# Run decision loop (typically called by framework)
intent = strategy.decide(market_snapshot)
```

## Decision Logic

The strategy's `decide()` method:

1. **Rate Comparison**: Fetches current supply APY from all configured protocols
2. **Opportunity Detection**: Identifies tokens where current position APY < best available APY
3. **Threshold Check**: Verifies spread exceeds `min_spread_bps`
4. **Amount Check**: Verifies position size exceeds `rebalance_threshold_usd`
5. **Execution**: Returns `Intent.sequence([withdraw, supply])` for atomic execution

## Supported Protocols

| Protocol | Chains |
|----------|--------|
| Aave V3 | Ethereum, Arbitrum, Optimism, Polygon, Base, Avalanche |
| Morpho Blue | Ethereum, Base |
| Compound V3 | Ethereum, Arbitrum, Polygon, Base |

## Supported Tokens

Common tokens monitored:
- Stablecoins: USDC, USDT, DAI, FRAX
- ETH variants: WETH, stETH, wstETH, cbETH, rETH
- Others: WBTC, LINK, ARB

Token availability varies by protocol and chain.

## Risk Considerations

1. **Gas Costs**: Rebalancing incurs gas costs - ensure spread justifies transaction fees
2. **Slippage**: Large positions may experience slippage during rebalance
3. **Protocol Risk**: Each protocol has its own smart contract risk
4. **Rate Volatility**: Rates can change quickly, especially around large deposits/withdrawals
5. **Liquidity**: Ensure sufficient protocol liquidity for withdrawals

## Dry-Run Testing

To test on Anvil fork:

```bash
# Start Anvil fork
anvil --fork-url $ETH_RPC_URL --port 8545

# Run strategy dry-run
python -m strategies.lending_rate_arb.dry_run
```

## Files

```
strategies/lending_rate_arb/
├── __init__.py          # Module exports
├── config.py            # LendingRateArbConfig dataclass
├── strategy.py          # LendingRateArbStrategy implementation
├── README.md            # This documentation
└── tests/
    ├── __init__.py
    └── test_strategy.py # Unit tests
```

## Related Components

- `src/data/rates/RateMonitor`: Fetches lending rates from protocols
- `src/intents/Intent`: Intent factory for supply/withdraw operations
- `src/strategies/IntentStrategy`: Base class for intent-based strategies
