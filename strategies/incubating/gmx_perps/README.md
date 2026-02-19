# GMX V2 Perpetual Futures Strategy (Demo)

A tutorial strategy demonstrating how to trade perpetual futures on GMX V2.

## What are Perpetual Futures?

Perpetual futures (perps) are derivative contracts that:

1. **Track underlying assets** (ETH, BTC, etc.) without owning them
2. **Allow leverage** - amplify gains (and losses) up to 100x
3. **Never expire** - unlike traditional futures contracts
4. **Enable shorting** - profit from price decreases

### Example Position

Opening a 2x LONG ETH position with 0.1 WETH collateral at $3,500:
- Collateral value: 0.1 * $3,500 = $350
- Position size: $350 * 2 = $700
- If ETH goes to $3,850 (+10%): +$70 profit (20% on collateral)
- If ETH goes to $3,150 (-10%): -$70 loss (20% on collateral)

## Key Concepts

### Leverage

Leverage multiplies your exposure:
- **1x**: No leverage (spot equivalent)
- **2x**: Double the exposure, double the risk
- **10x**: 10x exposure, liquidation at ~10% adverse move
- **100x**: Max on GMX, liquidation at ~1% adverse move

GMX V2 supports 1.1x to 100x leverage depending on the market.

### Long vs Short

- **LONG**: Profit when price goes UP
- **SHORT**: Profit when price goes DOWN

### Collateral

Tokens you deposit to back your position:
- WETH, USDC, USDC.e, USDT, DAI, WBTC
- Collateral type affects liquidation price

### Execution Fees

GMX V2 uses keepers to execute orders. You pay:
- ~0.0005 ETH execution fee per order
- Network gas fees

## Quick Start

### Test on Anvil

```bash
# Prerequisites: Foundry installed, RPC URL in .env

# Open a LONG position
python strategies/demo/gmx_perps/run_anvil.py --action open

# Close the position
python strategies/demo/gmx_perps/run_anvil.py --action close
```

> **Tip: Funding the Anvil Wallet**
>
> If using Claude Code, ask it to fund your wallet with the required tokens:
> ```
> "cast send 0.1 WETH to Anvil wallet on Arbitrum"
> ```
> Claude Code will use `anvil_setStorageAt` to set token balances for testing.

**Note:** GMX orders are asynchronous - keepers execute them. On Anvil without keepers, orders will be created but not executed. This tests the order creation flow.

### Run with CLI

```bash
# Set required environment variables
export ALMANAK_CHAIN=arbitrum
export ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# Run once to open a position
python -m src.cli.run --strategy demo_gmx_perps --once
```

## Configuration

Edit `config.json` to customize:

```json
{
    "market": "ETH/USD",
    "collateral_token": "WETH",
    "collateral_amount": "0.1",
    "leverage": "2.0",
    "is_long": true,
    "hold_minutes": 60,
    "max_slippage_pct": 1.0,
    "force_action": "open"
}
```

### Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `market` | string | GMX market (ETH/USD, BTC/USD, etc.) | "ETH/USD" |
| `collateral_token` | string | Token for collateral | "WETH" |
| `collateral_amount` | string | Amount of collateral | "0.1" |
| `leverage` | string | Leverage multiplier (1.1-100) | "2.0" |
| `is_long` | bool | True for long, False for short | true |
| `hold_minutes` | int | Minutes to hold position | 60 |
| `max_slippage_pct` | float | Max slippage percentage | 1.0 |
| `force_action` | string | Force "open" or "close" for testing | "open" |

## How It Works

### 1. Open Position

```python
def _create_open_intent(self, current_price):
    # Calculate position size
    collateral_value = self.collateral_amount * current_price
    position_size_usd = collateral_value * self.leverage

    return Intent.perp_open(
        market="ETH/USD",
        collateral_token="WETH",
        collateral_amount=Decimal("0.1"),
        size_usd=position_size_usd,
        is_long=True,
        leverage=Decimal("2.0"),
        max_slippage=Decimal("0.01"),
        protocol="gmx_v2",
    )
```

### 2. Close Position

```python
def _create_close_intent(self):
    return Intent.perp_close(
        market="ETH/USD",
        collateral_token="WETH",
        is_long=True,
        size_usd=self._position_size_usd,  # Full position
        max_slippage=Decimal("0.01"),
        protocol="gmx_v2",
    )
```

## Available Markets (Arbitrum)

| Market | Index Token | Min Leverage | Max Leverage |
|--------|-------------|--------------|--------------|
| ETH/USD | ETH | 1.1x | 100x |
| BTC/USD | BTC | 1.1x | 100x |
| LINK/USD | LINK | 1.1x | 50x |
| ARB/USD | ARB | 1.1x | 50x |
| SOL/USD | SOL | 1.1x | 50x |
| UNI/USD | UNI | 1.1x | 50x |
| DOGE/USD | DOGE | 1.1x | 50x |

## Risk Management

### Liquidation Risk

If losses exceed your collateral minus maintenance margin, your position gets liquidated:
- All collateral is lost
- Small liquidation penalty applies

**Mitigation:**
- Use lower leverage (2-5x for beginners)
- Monitor positions actively
- Set stop-loss orders

### Funding Rate Risk

Perpetuals have funding rates to keep prices aligned with spot:
- Longs pay shorts when price > index (or vice versa)
- Rates can be positive or negative
- Calculated every hour on GMX

**Mitigation:**
- Check funding rates before opening positions
- Consider direction of funding in your strategy

### Slippage Risk

Large orders or low liquidity can cause slippage:
- Worse execution price than expected
- Can significantly impact profits

**Mitigation:**
- Set appropriate `max_slippage_pct`
- Split large orders into smaller chunks
- Trade liquid markets (ETH, BTC)

## GMX V2 Contract Addresses (Arbitrum)

| Contract | Address |
|----------|---------|
| ExchangeRouter | 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41 |
| DataStore | 0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8 |
| Reader | 0xf60becbba223EEaA9495Da3f606753867eC10d139 |
| OrderVault | 0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5 |

## File Structure

```
strategies/demo/gmx_perps/
├── __init__.py      # Package exports
├── strategy.py      # Main strategy logic (with tutorial comments)
├── config.json      # Default configuration
├── run_anvil.py     # Test script using CLI runner
└── README.md        # This file
```

## Intent Types Used

- **PERP_OPEN**: Open a leveraged perpetual position
- **PERP_CLOSE**: Close an existing perpetual position
- **HOLD**: Wait without action

## Limitations

This is a **demo strategy** for educational purposes:

- Simple time-based exit (no technical analysis)
- No stop-loss or take-profit orders
- No funding rate monitoring
- No position size optimization
- In-memory state (not persisted)

Real perpetual strategies would include:
- Technical indicators for entry/exit
- Risk-adjusted position sizing
- Trailing stops and take-profits
- Funding rate analysis
- Cross-exchange hedging

## Next Steps

1. Read the heavily-commented `strategy.py` file
2. Run on Anvil to see order creation
3. Study the `gmx_perps_simple` strategy for a production-ready version
4. Implement your own entry/exit signals

## References

- [GMX V2 Documentation](https://docs.gmx.io/docs/overview)
- [GMX V2 Contracts](https://github.com/gmx-io/gmx-synthetics)
- [Perpetuals Trading Guide](https://academy.binance.com/en/articles/what-are-perpetual-futures-contracts)

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
