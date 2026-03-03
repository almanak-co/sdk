# Getting Started

This guide walks you through installing the Almanak SDK, scaffolding your first strategy, and running it locally on an Anvil fork -- no wallet or API keys required.

## Prerequisites

- **Python 3.11+**
- **Foundry** (provides Anvil for local fork testing):

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## Installation

```bash
pip install almanak
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install almanak
```

**Using an AI coding agent?** Teach it the SDK in one command:

```bash
almanak agent install
```

This auto-detects your platform (Claude Code, Codex, Cursor, Copilot, and [6 more](agent-skills.md)) and installs the strategy builder skill.

## 1. Get a Strategy

### Option A: Copy a working demo (recommended for beginners)

```bash
almanak strat demo
```

This shows an interactive menu of 13 working demo strategies. Pick one and it gets copied into your current directory, ready to run. You can also skip the menu:

```bash
almanak strat demo --name uniswap_rsi
```

### Option B: Scaffold from a template

```bash
almanak strat new
```

Follow the interactive prompts to pick a template, chain, and name. This creates a strategy directory with:

- `strategy.py` - Your strategy implementation with `decide()` method
- `config.json` - Chain, protocol, and parameter configuration
- `.env` - Environment variables (fill in your keys later)
- `__init__.py` - Package exports
- `tests/` - Test scaffolding

## 2. Run on a Local Anvil Fork

The fastest way to test your strategy -- no wallet keys, no real funds, no risk:

```bash
cd my_strategy
almanak strat run --network anvil --once
```

This command automatically:

1. **Starts an Anvil fork** of the chain specified in your `config.json` (free public RPCs are used by default)
2. **Uses a default Anvil wallet** -- no `ALMANAK_PRIVATE_KEY` needed
3. **Starts the gateway** sidecar in the background
4. **Funds your wallet** with tokens listed in `anvil_funding` (see below)
5. **Runs one iteration** of your strategy's `decide()` method

### Wallet Funding on Anvil

Add an `anvil_funding` block to your `config.json` to automatically fund your wallet when the fork starts:

```json
{
    "strategy_id": "my_strategy",
    "chain": "arbitrum",
    "anvil_funding": {
        "ETH": 10,
        "USDC": 10000,
        "WETH": 5
    }
}
```

Native tokens (ETH, AVAX, etc.) are funded via `anvil_setBalance`. ERC-20 tokens are funded via storage slot manipulation. This happens automatically each time the fork starts.

### Better RPC Performance (Optional)

Free public RPCs work but are rate-limited. For faster forking, set an Alchemy key in your `.env`:

```bash
ALCHEMY_API_KEY=your_alchemy_key
```

This auto-constructs RPC URLs for all supported chains. Any provider works -- see [Environment Variables](environment-variables.md) for the full priority order.

## 3. Run on Mainnet

!!! warning
    Mainnet execution uses **real funds**. Start with small amounts and use a dedicated wallet.

To run against live chains, you need a wallet private key in your `.env`:

```bash
# .env
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# RPC access (pick one)
ALCHEMY_API_KEY=your_alchemy_key
# or: RPC_URL=https://your-rpc-provider.com/v1/your-key
```

Then run without the `--network anvil` flag:

```bash
almanak strat run --once
```

!!! tip
    Test with `--dry-run` first to simulate without submitting transactions:

    ```bash
    almanak strat run --dry-run --once
    ```

See [Environment Variables](environment-variables.md) for the full list of configuration options including protocol-specific API keys.

!!! info "Before going live"
    - Always run `--dry-run --once` before your first live execution to verify intent compilation
      without submitting transactions.
    - If swaps revert with "Too little received", switch from `amount_usd=` to `amount=` (token
      units). `amount_usd=` relies on the gateway price oracle for USD-to-token conversion, which
      may diverge from the DEX price.
    - Start with small amounts, monitor the first few iterations, and note your instance ID for
      `--id` resume.

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
| `CollectFeesIntent` | Collect LP fees |
| `PredictionBuyIntent` | Buy prediction market shares |
| `PredictionSellIntent` | Sell prediction market shares |
| `PredictionRedeemIntent` | Redeem prediction market winnings |
| `VaultDepositIntent` | Deposit into a vault |
| `VaultRedeemIntent` | Redeem from a vault |
| `BridgeIntent` | Bridge tokens cross-chain |
| `EnsureBalanceIntent` | Meta-intent that resolves to a `BridgeIntent` or `HoldIntent` to ensure minimum token balance on a target chain |

## Strategy Teardown (Required)

Every strategy must implement teardown so operators can safely close positions. Without teardown, close-requests are silently ignored and positions remain open. The `almanak strat new` templates include stubs -- fill them in as you build your strategy.

```python
class MyStrategy(IntentStrategy):
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Query on-chain state and return open positions."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary
        # ... return TeardownPositionSummary with your positions

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Return ordered intents to unwind all positions."""
        from almanak.framework.teardown import TeardownMode
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [Intent.swap(from_token="WETH", to_token="USDC", amount="all", max_slippage=max_slippage)]
```

If your strategy holds multiple position types, close them in order: **perps -> borrows -> supplies -> LPs -> tokens**. See the [Teardown CLI](cli/strat-teardown.md) for how operators trigger teardown.

!!! note "Backtest CLI"
    Unlike `almanak strat run` which auto-discovers the strategy from the current directory,
    backtest commands require an explicit strategy name: `almanak strat backtest pnl -s my_strategy`.
    Use `--list-strategies` to see available strategies.

## Next Steps

- [Environment Variables](environment-variables.md) - All configuration options
- [API Reference](api/index.md) - Full Python API documentation
- [CLI Reference](cli/almanak.md) - All CLI commands
- [Gateway API](gateway/api-reference.md) - Gateway gRPC services
