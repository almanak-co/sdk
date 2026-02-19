# Enso RSI Strategy (Demo)

A tutorial strategy demonstrating RSI-based trading using the Enso DEX aggregator.

## What is Enso?

Enso is a DEX aggregator that finds optimal swap routes across multiple decentralized exchanges:

### Benefits Over Direct DEX Execution

| Feature | Direct DEX | Enso Aggregator |
|---------|-----------|-----------------|
| Routing | Single pool/DEX | Multi-DEX optimization |
| Price | Pool-dependent | Best across all DEXs |
| Slippage | Manual calculation | Auto with `safeRouteSingle` |
| Cross-chain | Not supported | Bridge aggregation |

### How Enso Works

1. You specify: token in, token out, amount, slippage
2. Enso queries all available DEXs (Uniswap, SushiSwap, Camelot, etc.)
3. Finds optimal route (may split across DEXs)
4. Returns ready-to-execute transaction

## Quick Start

### Test on Anvil

```bash
# Prerequisites: Foundry installed, RPC URL in .env

# BUY: USDC -> WETH via Enso
python strategies/demo/enso_rsi/run_anvil.py --action buy

# SELL: WETH -> USDC via Enso
python strategies/demo/enso_rsi/run_anvil.py --action sell
```

> **Tip: Funding the Anvil Wallet**
>
> If using Claude Code, ask it to fund your wallet with the required tokens:
> ```
> "cast send 100 USDC and 0.05 WETH to Anvil wallet on Arbitrum"
> ```
> Claude Code will use `anvil_setStorageAt` to set token balances for testing.

### Run with CLI

```bash
# Set required environment variables
export ALMANAK_CHAIN=arbitrum
export ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# Optional: Enso API key for higher rate limits
export ENSO_API_KEY=your_api_key

# Run once
almanak strat run --once
```

## Configuration

Edit `config.json` to customize:

```json
{
    "trade_size_usd": "100",
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    "max_slippage_pct": 0.5,
    "base_token": "WETH",
    "quote_token": "USDC",
    "force_action": "buy"
}
```

### Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `trade_size_usd` | string | USD amount per trade | "100" |
| `rsi_oversold` | int | RSI level triggering buy | 30 |
| `rsi_overbought` | int | RSI level triggering sell | 70 |
| `max_slippage_pct` | float | Max slippage percentage | 0.5 |
| `base_token` | string | Token to trade | "WETH" |
| `quote_token` | string | Quote token | "USDC" |
| `force_action` | string | Force "buy" or "sell" | null |

## How It Works

### RSI Logic

```
RSI < 30 (oversold)     -> BUY base token via Enso
RSI > 70 (overbought)   -> SELL base token via Enso
RSI 30-70 (neutral)     -> HOLD
```

### Enso vs Uniswap Intent

The key difference from `demo_uniswap_rsi` is the `protocol` parameter:

**Uniswap Direct:**
```python
Intent.swap(
    from_token="USDC",
    to_token="WETH",
    amount_usd=Decimal("100"),
    protocol="uniswap_v3",  # Direct Uniswap
)
```

**Enso Aggregator:**
```python
Intent.swap(
    from_token="USDC",
    to_token="WETH",
    amount_usd=Decimal("100"),
    protocol="enso",  # Enso aggregator
)
```

### Execution Flow

1. Strategy decides to swap based on RSI
2. Creates `SwapIntent` with `protocol="enso"`
3. Compiler routes to `EnsoAdapter`
4. Enso API returns optimal route
5. Transaction executed on-chain

## Supported Chains

Enso supports multiple chains:
- Ethereum (mainnet)
- Arbitrum
- Optimism
- Base
- Polygon
- Avalanche
- And more...

## Enso Router Addresses

| Chain | Router Address |
|-------|---------------|
| Ethereum | 0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf |
| Arbitrum | 0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf |
| Optimism | 0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf |
| Base | 0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf |
| Polygon | 0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf |

## File Structure

```
strategies/demo/enso_rsi/
├── __init__.py      # Package exports
├── strategy.py      # Main strategy logic
├── config.json      # Default configuration
├── run_anvil.py     # Test script
└── README.md        # This file
```

## Intent Types Used

- **SWAP**: Exchange tokens via Enso aggregator
- **HOLD**: Wait without action

## Cross-Chain Swaps (Advanced)

Enso also supports cross-chain swaps:

```python
Intent.swap(
    from_token="USDC",
    to_token="WETH",
    amount_usd=Decimal("1000"),
    chain="base",                  # Source chain
    destination_chain="arbitrum",  # Target chain
    protocol="enso",               # Required for cross-chain
)
```

This routes through Enso's bridge aggregation (Stargate, LayerZero).

## Comparison: Enso vs Direct DEX

### When to Use Enso

- Large trades that benefit from multi-DEX splitting
- When you want automatic route optimization
- Cross-chain swaps
- Complex multi-hop routes

### When to Use Direct DEX

- Small trades where gas savings matter
- When you need specific pool execution
- Testing/development on specific protocols

## API Requirements

- **Without API Key**: Rate limited, suitable for testing
- **With API Key**: Higher limits, recommended for production

Set `ENSO_API_KEY` environment variable for production use.

## Limitations

This is a **demo strategy** for educational purposes:

- Simple RSI logic (no confirmation signals)
- No position tracking
- No risk management
- Force action for testing

Real strategies would include:
- RSI divergence detection
- Volume confirmation
- Dynamic position sizing
- Stop-loss logic

## Next Steps

1. Read the heavily-commented `strategy.py` file
2. Compare with `demo_uniswap_rsi` to see the difference
3. Run on Anvil to see Enso routing in action
4. Try cross-chain swaps for advanced usage

## References

- [Enso Documentation](https://docs.enso.finance/)
- [Enso API Reference](https://api.enso.finance/api)
- [RSI Technical Analysis](https://www.investopedia.com/terms/r/rsi.asp)

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
