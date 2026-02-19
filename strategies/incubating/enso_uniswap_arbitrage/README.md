# Enso-Uniswap Arbitrage Strategy (Demo)

A tutorial strategy demonstrating cross-protocol arbitrage using Intent.sequence().

## Concept

This strategy shows how to combine two different protocols in a single trade:

1. **Buy** base token via Enso DEX aggregator (finds optimal route)
2. **Sell** base token on Uniswap V3 directly

### Why Cross-Protocol?

DEX aggregators like Enso scan multiple DEXs to find the best price. Sometimes the aggregated route gives a better price than any single DEX. By buying via the aggregator and selling on a single DEX, you can potentially profit from the difference.

### Real-World Considerations

In practice, this arbitrage is challenging because:
- Gas costs eat into small spreads
- Price impact on both legs
- MEV bots front-run obvious arbitrage
- Non-atomic execution (two separate transactions)

This demo is for **educational purposes** to show the pattern.

## Quick Start

### Test on Anvil

```bash
# Prerequisites: Foundry installed, RPC URL in .env

# Default mode: Buy via Enso, Sell on Uniswap
python strategies/demo/enso_uniswap_arbitrage/run_anvil.py

# Alternative mode: Buy on Uniswap, Sell via Enso
python strategies/demo/enso_uniswap_arbitrage/run_anvil.py --mode buy_uniswap_sell_enso
```

> **Tip: Funding the Anvil Wallet**
>
> If using Claude Code, ask it to fund your wallet with the required tokens:
> ```
> "cast send 100 USDC to Anvil wallet on Arbitrum"
> ```
> Claude Code will use `anvil_setStorageAt` to set token balances for testing.

### Run with CLI

```bash
# Set required environment variables
export ALMANAK_CHAIN=arbitrum
export ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# Run once
python -m src.cli.run --strategy demo_enso_uniswap_arbitrage --once
```

## Configuration

Edit `config.json` to customize:

```json
{
    "trade_size_usd": "100",
    "max_slippage_pct": 0.5,
    "base_token": "WETH",
    "quote_token": "USDC",
    "mode": "buy_enso_sell_uniswap"
}
```

### Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `trade_size_usd` | string | USD amount per arbitrage | "100" |
| `max_slippage_pct` | float | Max slippage percentage | 0.5 |
| `base_token` | string | Token to arbitrage | "WETH" |
| `quote_token` | string | Quote token | "USDC" |
| `mode` | string | Arbitrage direction | "buy_enso_sell_uniswap" |

### Modes

| Mode | Step 1 | Step 2 |
|------|--------|--------|
| `buy_enso_sell_uniswap` | Buy via Enso | Sell on Uniswap |
| `buy_uniswap_sell_enso` | Buy on Uniswap | Sell via Enso |

## How It Works

### Intent Sequence Pattern

The key learning from this demo is using `Intent.sequence()`:

```python
return Intent.sequence(
    [
        # Step 1: Buy via Enso aggregator
        Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            protocol="enso",
        ),

        # Step 2: Sell on Uniswap V3
        Intent.swap(
            from_token="WETH",
            to_token="USDC",
            amount="all",  # Use output from step 1
            protocol="uniswap_v3",
        ),
    ],
    description="Cross-protocol arbitrage",
)
```

### Key Patterns Demonstrated

1. **Intent.sequence()**: Chains dependent intents
2. **amount="all"**: Uses actual output from previous step
3. **Cross-protocol**: Different protocols in same sequence
4. **Two-leg trades**: Buy and sell in one strategy execution

### Execution Flow

```
1. Strategy.decide() returns IntentSequence
2. Framework extracts first intent (Enso swap)
3. Compiler routes to EnsoAdapter
4. Execute swap: USDC -> WETH via Enso
5. Framework extracts second intent (Uniswap swap)
6. Resolve amount="all" to actual WETH received
7. Compiler routes to UniswapV3Adapter
8. Execute swap: WETH -> USDC on Uniswap
9. Net result: Started with USDC, ended with USDC (hopefully more!)
```

## Sequence vs Parallel

### When to Use Sequence

Use `Intent.sequence()` when intents are **dependent**:
- Output of one is input to another
- Order matters
- Same token/asset involved

```python
# Dependent: Must execute in order
Intent.sequence([
    Intent.swap("USDC", "WETH", amount_usd=100),
    Intent.swap("WETH", "USDC", amount="all"),  # Depends on first
])
```

### When to Use Parallel (List)

Return a list when intents are **independent**:
- Different assets
- Different chains
- No dependency

```python
# Independent: Can execute in parallel
return [
    Intent.swap("USDC", "WETH", amount_usd=100, chain="arbitrum"),
    Intent.swap("USDC", "DAI", amount_usd=100, chain="optimism"),
]
```

## File Structure

```
strategies/demo/enso_uniswap_arbitrage/
├── __init__.py      # Package exports
├── strategy.py      # Main strategy logic
├── config.json      # Default configuration
├── run_anvil.py     # Test script
└── README.md        # This file
```

## Intent Types Used

- **SWAP**: Exchange tokens (used twice per arbitrage)
- **SEQUENCE**: Wrapper for ordered execution

## Comparison with Other Demos

| Demo | Protocols | Intents | Pattern |
|------|-----------|---------|---------|
| `uniswap_rsi` | Uniswap V3 | SWAP | Single intent |
| `enso_rsi` | Enso | SWAP | Single intent |
| `aave_borrow` | Aave V3 | SUPPLY | Single intent |
| `enso_uniswap_arbitrage` | Enso + Uniswap | SEQUENCE | Multi-step |
| `cross_chain_arbitrage` | Multiple chains | SEQUENCE + BRIDGE | Cross-chain |

## Advanced: Real Arbitrage

For production arbitrage, you would need:

```python
def decide(self, market: MarketSnapshot) -> Optional[Intent]:
    # 1. Query prices from both protocols
    enso_price = self._get_enso_quote(amount)
    uniswap_price = self._get_uniswap_quote(amount)

    # 2. Calculate expected profit
    spread = abs(enso_price - uniswap_price) / min(enso_price, uniswap_price)
    gas_cost = self._estimate_gas_cost()
    expected_profit = spread * amount - gas_cost

    # 3. Only execute if profitable
    if expected_profit < min_profit_threshold:
        return Intent.hold(reason="Spread too small")

    # 4. Execute in profitable direction
    if enso_price < uniswap_price:
        return self._create_buy_enso_sell_uniswap_sequence()
    else:
        return self._create_buy_uniswap_sell_enso_sequence()
```

## Limitations

This is a **demo strategy** for educational purposes:

- Always executes (no profitability check)
- No price comparison logic
- No gas cost calculation
- No MEV protection
- Non-atomic execution

Real arbitrage strategies would include:
- Flashloan-based atomic execution
- Profitability thresholds
- Gas optimization
- MEV protection (private mempools)
- Position limits

## Next Steps

1. Read the heavily-commented `strategy.py` file
2. Understand `Intent.sequence()` pattern
3. Compare with `cross_chain_arbitrage` for more complex sequences
4. Run on Anvil to see multi-step execution
5. Explore flashloan-based atomic arbitrage

## References

- [Enso Documentation](https://docs.enso.finance/)
- [Uniswap V3 Docs](https://docs.uniswap.org/)
- [Flashbots for MEV Protection](https://docs.flashbots.net/)
- [Atomic Arbitrage Patterns](https://github.com/flashbots/pm/blob/main/guides/flashbots-alpha.md)

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
