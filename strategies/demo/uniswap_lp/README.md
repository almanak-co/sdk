# Uniswap V3 LP Strategy (Demo)

A tutorial strategy demonstrating how to manage Uniswap V3 concentrated liquidity positions.

## What This Strategy Does

This strategy manages Uniswap V3 LP positions:

1. **Open Position**: Creates a concentrated liquidity position around the current price
2. **Monitor Position**: Checks if the position is still in range
3. **Rebalance (if needed)**: Closes out-of-range positions and re-opens centered on current price
4. **Collect Fees**: Collects accumulated trading fees when closing positions

## Concentrated Liquidity Explained

Uniswap V3's concentrated liquidity allows LPs to provide liquidity within a specific price range:

- **Traditional AMM**: Liquidity spread from $0 to infinity (capital inefficient)
- **Uniswap V3**: Liquidity focused in a range (e.g., $3000-$4000 for ETH)

### Benefits
- **Higher capital efficiency**: Up to 4000x compared to V2 for tight ranges
- **More fee revenue**: Earn more fees per dollar of capital deployed
- **Better execution**: Lower slippage for traders

### Risks
- **Out of range**: If price moves outside your range, you stop earning fees
- **Single-sided exposure**: When out of range, you hold 100% of one token
- **Impermanent loss**: Can be higher with tighter ranges

## Quick Start

### Test on Anvil (Recommended)

```bash
# Prerequisites: Foundry installed, RPC URL in .env

# Run with default settings (opens LP position)
python strategies/demo/uniswap_lp/run_anvil.py

# Force specific action
python strategies/demo/uniswap_lp/run_anvil.py --action open
python strategies/demo/uniswap_lp/run_anvil.py --action close --position-id 123456
```

> **Tip: Funding the Anvil Wallet**
>
> If using Claude Code, ask it to fund your wallet with the required tokens:
> ```
> "cast send 0.1 WETH and 340 USDC to Anvil wallet on Arbitrum"
> ```
> Claude Code will use `anvil_setStorageAt` to set token balances for testing.

### Run with CLI

```bash
# Set required environment variables
export ALMANAK_CHAIN=arbitrum
export ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# Run once to open a position
almanak strat run --once

# Run continuously to monitor and rebalance
almanak strat run --interval 60
```

## Configuration

Edit `config.json` to customize the strategy:

```json
{
    "pool": "WETH/USDC.e/500",     // Pool: TOKEN0/TOKEN1/FEE_TIER
    "range_width_pct": 0.20,       // 20% total range width (±10%)
    "amount0": "0.1",              // WETH amount to deposit
    "amount1": "340",              // USDC amount to deposit
    "force_action": "open"         // "open", "close", or "" for auto
}
```

### Fee Tiers

Common Uniswap V3 fee tiers (in hundredths of a basis point):
- `100` = 0.01% (stablecoin pairs)
- `500` = 0.05% (stable/major pairs)
- `3000` = 0.3% (most pairs)
- `10000` = 1% (exotic pairs)

### Range Width

The `range_width_pct` determines how concentrated your liquidity is:
- `0.10` = 10% width = ±5% from current price (tighter, more fees, higher IL risk)
- `0.20` = 20% width = ±10% from current price (balanced)
- `0.50` = 50% width = ±25% from current price (wider, lower fees, lower IL risk)

## How It Works

### 1. Strategy Initialization

```python
@almanak_strategy(
    name="demo_uniswap_lp",
    supported_chains=["arbitrum", "ethereum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class UniswapLPStrategy(IntentStrategy):
    def __init__(self, config, chain, wallet_address):
        self.pool = config.get("pool", "WETH/USDC.e/500")
        self.range_width_pct = config.get("range_width_pct", 0.20)
        ...
```

### 2. Opening a Position

```python
def _create_open_intent(self, current_price):
    # Calculate price range (±10% for 20% width)
    half_width = self.range_width_pct / 2
    range_lower = current_price * (1 - half_width)  # e.g., 3060
    range_upper = current_price * (1 + half_width)  # e.g., 3740

    return Intent.lp_open(
        pool="WETH/USDC.e/500",
        amount0=Decimal("0.1"),      # WETH
        amount1=Decimal("340"),      # USDC
        range_lower=range_lower,
        range_upper=range_upper,
        protocol="uniswap_v3",
    )
```

### 3. Closing a Position

```python
def _create_close_intent(self, position_id):
    return Intent.lp_close(
        position_id=position_id,
        pool="WETH/USDC.e/500",
        collect_fees=True,  # Collect accumulated fees
        protocol="uniswap_v3",
    )
```

### 4. Intent Execution

The framework handles:
1. Compiling the intent to transactions (approvals + mint/burn)
2. Converting price range to tick range
3. Calculating minimum amounts with slippage
4. Executing transactions in order

## Understanding Price Ranges

### Tick Math

Uniswap V3 uses ticks instead of prices directly:
- `price = 1.0001^tick`
- Each tick represents a 0.01% price change
- Ticks must be aligned to tick spacing (depends on fee tier)

### Example Calculation

For WETH/USDC at $3400 with 20% range width:
- `range_lower = 3400 * 0.90 = 3060 USDC/WETH`
- `range_upper = 3400 * 1.10 = 3740 USDC/WETH`

In ticks:
- `tick_lower = log(3060) / log(1.0001) ≈ 80,000`
- `tick_upper = log(3740) / log(1.0001) ≈ 82,000`

## File Structure

```
strategies/demo/uniswap_lp/
├── __init__.py      # Package exports
├── strategy.py      # Main strategy logic (with tutorial comments)
├── config.json      # Default configuration
├── run_anvil.py     # Test script using CLI runner
└── README.md        # This file
```

## Key Concepts for LP Strategies

### 1. Pool Identifier
Pool format: `TOKEN0/TOKEN1/FEE`
- TOKEN0: First token (sorted by address)
- TOKEN1: Second token
- FEE: Fee tier in hundredths of bp

### 2. Position NFT
Each LP position is an NFT (ERC-721):
- Unique token ID identifies the position
- Contains metadata about tick range and liquidity
- Required for closing/modifying positions

### 3. Concentrated Liquidity Math
Unlike V2's constant product (`x * y = k`), V3 uses:
- Real reserves only counted within active tick range
- More capital efficiency but more complex math
- Position value changes non-linearly with price

## Limitations

This is a **demo strategy** for educational purposes:

- Simple open/close logic without sophisticated rebalancing
- No impermanent loss calculation
- No gas cost optimization for rebalancing decisions
- No multi-position management
- Real strategies need comprehensive backtesting

## Next Steps

1. Read the heavily-commented `strategy.py` file
2. Run on Anvil to see LP minting in action
3. Modify range width and see how it affects capital efficiency
4. Study the dynamic_lp_rebalance strategy for advanced concepts

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
