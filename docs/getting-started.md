# Getting Started

This guide walks you through installing the Almanak SDK, configuring your environment, and running your first strategy.

## Installation

```bash
pip install almanak
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install almanak
```

Use the CLI to scaffold a new strategy:

```bash
almanak strat new
```

This creates a strategy directory with:

- `strategy.py` - Your strategy implementation with `decide()` method
- `config.json` - Chain, protocol, and parameter configuration
- `.env` - Environment variables (fill in your keys)
- `__init__.py` - Package exports
- `tests/` - Test scaffolding

## Run Your Strategy

A managed gateway is auto-started in the background:

```bash
cd my_strategy
almanak strat run --once
```

### Run on a Local Anvil Fork

For testing without real transactions:

```bash
almanak strat run --network anvil --once
```

This auto-starts both Anvil (forking mainnet via your Alchemy key) and the gateway.

### Dry Run

To simulate without submitting transactions:

```bash
almanak strat run --dry-run --once
```

## Strategy Structure

A strategy implements the `decide()` method, which receives a `MarketSnapshot` and returns an `Intent`:

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyStrategy(IntentStrategy):
    def decide(self, market: MarketSnapshot) -> Intent | None:
        price = market.price("ETH")
        balance = market.balance("USDC")

        if price < Decimal("2000") and balance.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="No opportunity")
```

## Available Intents

| Intent | Description |
|--------|-------------|
| `SwapIntent` | Token swaps on DEXs |
| `HoldIntent` | No action, wait for next cycle |
| `LPOpenIntent` | Open liquidity position |
| `LPCloseIntent` | Close liquidity position |
| `BorrowIntent` | Borrow from lending protocols |
| `RepayIntent` | Repay borrowed assets |
| `SupplyIntent` | Supply to lending protocols |
| `WithdrawIntent` | Withdraw from lending protocols |
| `StakeIntent` | Stake tokens |
| `UnstakeIntent` | Unstake tokens |
| `PerpOpenIntent` | Open perpetuals position |
| `PerpCloseIntent` | Close perpetuals position |
| `FlashLoanIntent` | Flash loan operations |
| `PredictionBuyIntent` | Buy prediction market shares |
| `PredictionSellIntent` | Sell prediction market shares |
| `PredictionRedeemIntent` | Redeem prediction market winnings |

## Next Steps

- [API Reference](api/index.md) - Full Python API documentation
- [CLI Reference](cli/almanak.md) - All CLI commands
- [Gateway API](gateway/api-reference.md) - Gateway gRPC services
