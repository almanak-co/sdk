<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/logo-light.svg">
    <img alt="Almanak" src="docs/assets/logo-light.svg" width="300">
  </picture>
</p>

<h3 align="center">Production DeFi strategy framework for quants</h3>

<p align="center">
  <a href="https://pypi.org/project/almanak/"><img src="https://img.shields.io/pypi/v/almanak?style=flat-square&color=blue" alt="PyPI version"></a>
  <a href="https://pypi.org/project/almanak/"><img src="https://img.shields.io/pypi/pyversions/almanak?style=flat-square" alt="Python 3.12+"></a>
  <a href="https://github.com/almanak-co/almanak-sdk/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-green?style=flat-square" alt="License: Apache-2.0"></a>
  <a href="https://docs.almanak.co/"><img src="https://img.shields.io/badge/docs-almanak.co-purple?style=flat-square" alt="Docs"></a>
  <a href="https://discord.gg/c4jY28WrEB"><img src="https://img.shields.io/badge/Discord-join-5865F2?style=flat-square&logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://x.com/Almanak__"><img src="https://img.shields.io/badge/Twitter-follow-1DA1F2?style=flat-square&logo=x&logoColor=white" alt="Twitter"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> |
  <a href="#quick-start">Quick Start</a> |
  <a href="#writing-a-strategy">Strategies</a> |
  <a href="#backtesting">Backtesting</a> |
  <a href="https://docs.almanak.co/">Docs</a>
</p>

---

Almanak is an intent-based Python framework for developing, testing, and deploying autonomous DeFi strategies. Express trading logic as high-level intents - the framework handles compilation, execution, and state management across 12 chains and 20+ protocols.

## Features

- **Intent-Based Architecture**: Express trading logic as high-level intents (Swap, LP, Borrow, etc.) - the framework handles compilation and execution
- **Three-Tier State Management**: Automatic persistence with HOT/WARM/COLD tiers for reliability
- **Comprehensive Backtesting**: PnL simulation, paper trading on Anvil forks, and parameter sweeps
- **Multi-Chain Support**: Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Blast, Mantle, Berachain
- **Protocol Integration**: Uniswap V3, Aave V3, Morpho Blue, GMX V2, Lido, Ethena, Polymarket, Kraken, and more
- **Non-Custodial Design**: Full control over your funds through Safe smart accounts
- **Production-Ready**: Built-in alerting, stuck detection, emergency management, and canary deployments

## Installation

```bash
pip install almanak
```

**Using an AI coding agent?** Teach it the SDK in one command:

```bash
almanak agent install
```

This auto-detects your platform (Claude Code, Codex, Cursor, Copilot, and [6 more](https://docs.almanak.co/agent-skills/)) and installs the strategy builder skill.

## Quick Start

1. **Create a New Strategy**
   ```bash
   almanak strat new
   ```

2. **Test Your Strategy**

   A managed gateway is auto-started in the background when you run a strategy.
   Use `--dashboard` to launch a live monitoring dashboard alongside execution:

   ```bash
   uv run almanak strat run -d strategies/demo/uniswap_lp --network anvil --dashboard --once
   ```

   This single command auto-starts Anvil + gateway, opens the dashboard in your browser, and runs one iteration of the strategy.

## Writing a Strategy

Strategies implement the `decide()` method, which receives a `MarketSnapshot` and returns an `Intent` (or `None` to skip the cycle):

```python
from decimal import Decimal
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot

class MyStrategy(IntentStrategy):
    """A simple mean-reversion strategy."""

    def decide(self, market: MarketSnapshot) -> Intent | None:
        eth_price = market.price("ETH")
        usdc = market.balance("USDC")

        if eth_price < Decimal("2000") and usdc.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="Waiting for better conditions")
```

## Available Intents

| Intent | Description |
|--------|-------------|
| `SwapIntent` | Token swaps on DEXs |
| `HoldIntent` | No action, wait for next cycle |
| `LPOpenIntent` / `LPCloseIntent` | Open/close liquidity positions |
| `BorrowIntent` / `RepayIntent` | Borrow/repay on lending protocols |
| `SupplyIntent` / `WithdrawIntent` | Supply/withdraw from lending protocols |
| `StakeIntent` / `UnstakeIntent` | Stake/unstake tokens |
| `PerpOpenIntent` / `PerpCloseIntent` | Open/close perpetuals positions |
| `FlashLoanIntent` | Flash loan operations |
| `PredictionBuyIntent` / `PredictionSellIntent` / `PredictionRedeemIntent` | Prediction market trading |

## CLI Commands

```bash
# Gateway (auto-started by strat run, or start standalone)
almanak gateway                # Start standalone gateway server
almanak gateway --network anvil  # Start standalone for local Anvil testing

# Strategy development
almanak strat new              # Create new strategy from template
almanak strat run --once       # Run single iteration (auto-starts gateway)
almanak strat run --network anvil --once  # Run on local Anvil fork (auto-starts Anvil + gateway)
almanak strat run --network anvil --dashboard  # Run with live dashboard

# Backtesting
almanak strat backtest pnl     # Historical price simulation
almanak strat backtest sweep   # Parameter optimization
almanak strat backtest paper   # Paper trading on Anvil

# Advanced backtesting
almanak strat backtest monte-carlo  # Statistical robustness analysis
almanak strat backtest optimize     # Bayesian parameter optimization
almanak strat backtest scenario     # Crisis scenario stress testing
almanak strat backtest dashboard    # Interactive results dashboard

```

## Backtesting

The SDK provides a dual-engine backtesting system for institutional-grade strategy validation:

| Engine | Best For | Requirements |
|--------|----------|--------------|
| **PnL Backtester** | Historical analysis with price data | No Anvil required |
| **Paper Trader** | Live-like simulation with real execution | Anvil fork |

### Quick Example

```python
from almanak.framework.backtesting import PnLBacktester, PnLBacktestConfig
from datetime import datetime, UTC
from decimal import Decimal

config = PnLBacktestConfig(
    start_time=datetime(2024, 1, 1, tzinfo=UTC),
    end_time=datetime(2024, 6, 1, tzinfo=UTC),
    initial_capital_usd=Decimal("10000"),
)

backtester = PnLBacktester(data_provider, fee_models, slippage_models)
result = await backtester.backtest(strategy, config)

print(f"Total Return: {result.metrics.total_return_pct:.2f}%")
print(f"Sharpe Ratio: {result.metrics.sharpe_ratio:.2f}")
print(f"Max Drawdown: {result.metrics.max_drawdown_pct:.2f}%")
```

### CLI Usage

```bash
# Historical PnL backtest
almanak strat backtest pnl -s my_strategy --start 2024-01-01 --end 2024-06-01

# Parameter sweep optimization
almanak strat backtest sweep -s my_strategy --param "window:10,20,30"

# Paper trading on Anvil fork
almanak strat backtest paper start -s my_strategy --chain arbitrum

# Monte Carlo simulation (1000 price paths)
almanak strat backtest monte-carlo -s my_strategy --n-paths 1000

# Crisis scenario stress testing
almanak strat backtest scenario -s my_strategy --scenario terra_collapse
```

### Working Examples

Complete runnable examples are available in `examples/`:

```bash
python examples/backtest_ta_strategy.py      # RSI mean reversion
python examples/backtest_lp_strategy.py      # Concentrated LP
python examples/backtest_looping_strategy.py # Leveraged yield
```

For complete documentation, see [`almanak/framework/backtesting/README.md`](almanak/framework/backtesting/README.md).

## Supported Networks

- Ethereum
- Arbitrum
- Optimism
- Base
- Avalanche
- Polygon
- BSC
- Sonic
- Plasma
- Blast
- Mantle
- Berachain

## Supported Protocols

- **DEXs**: Uniswap V3, SushiSwap V3, PancakeSwap V3, TraderJoe V2, Aerodrome, Curve, Balancer
- **Lending**: Aave V3, Morpho Blue, Compound V3, Spark
- **Liquid Staking**: Lido, Ethena
- **Yield**: Pendle
- **Perpetuals**: GMX V2, Hyperliquid
- **Prediction Markets**: Polymarket
- **CEX Integration**: Kraken
- **Aggregators**: Enso, LiFi

## Demo Strategies

The SDK includes educational demo strategies to help you learn:

| Strategy | Description | Chain | Protocol |
|----------|-------------|-------|----------|
| `uniswap_rsi` | RSI-based trading on Uniswap V3 | Ethereum | Uniswap V3 |
| `uniswap_lp` | Dynamic LP position management | Ethereum | Uniswap V3 |
| `aave_borrow` | Supply collateral and borrow | Ethereum | Aave V3 |
| `gmx_perps` | Perpetuals trading | Arbitrum | GMX V2 |
| `enso_rsi` | RSI trading via DEX aggregator | Ethereum | Enso |
| `enso_uniswap_arbitrage` | Cross-protocol arbitrage | Ethereum | Enso, Uniswap |
| `traderjoe_lp` | Liquidity Book position management | Avalanche | TraderJoe V2 |
| `aerodrome_lp` | Solidly-based LP management | Base | Aerodrome |
| `lido_staker` | Stake ETH for liquid staking yield | Ethereum | Lido |
| `ethena_yield` | Stake USDe for yield-bearing sUSDe | Ethereum | Ethena |
| `spark_lender` | Supply DAI for lending yield | Ethereum | Spark |
| `morpho_looping` | Leveraged yield farming via recursive borrowing | Ethereum | Morpho Blue |
| `kraken_rebalancer` | CEX deposit, swap, and withdraw | Arbitrum | Kraken |
| `polymarket_signal_trader` | Signal-based prediction trading | Polygon | Polymarket |
| `polymarket_arbitrage` | Cross-market arbitrage | Polygon | Polymarket |
| `pancakeswap_simple` | Simple swap on PancakeSwap V3 | Arbitrum | PancakeSwap V3 |
| `sushiswap_lp` | LP position management on SushiSwap | Arbitrum | SushiSwap V3 |
| `pendle_basics` | Yield tokenization basics | Plasma | Pendle |
| `almanak_rsi` | RSI trading variant | Base | Uniswap V3 |

Run any demo with:
```bash
cd strategies/demo/<strategy_name>
uv run almanak strat run --once --dry-run
```

## Architecture

```
almanak/
  framework/           # V2 Strategy Framework
    strategies/        # IntentStrategy base class
    intents/           # Intent vocabulary & compiler
    state/             # Three-tier state management
    execution/         # Transaction orchestration
    backtesting/       # PnL, paper trading, sweeps
    connectors/        # Protocol adapters
    data/              # Price oracles, indicators
    alerting/          # Slack/Telegram notifications
    services/          # Stuck detection, emergency mgmt
  transaction_builder/ # Low-level tx building
  core/                # Enums, models, utilities
  cli/                 # Command-line interface
```

## Security

- All strategy code is encrypted at rest and in transit
- Agent EOA private keys are encrypted and never accessible to humans
- Fine-grained permission controls through Zodiac Roles Modifier
- Non-custodial design ensures users maintain full control of funds

### Gateway Architecture

All strategies run through a gateway-only architecture for security:

- **Gateway Sidecar**: Holds all secrets (API keys, private keys), exposes controlled gRPC API
- **Strategy Container**: Runs user code with no secrets and no internet access

This ensures strategy code cannot access secrets directly - all external access is mediated through the gateway.

```bash
# Run your strategy (auto-starts gateway in background)
cd my_strategy
almanak strat run --once

# Or run a standalone gateway for shared use
almanak gateway

# For full container isolation (production-like)
docker-compose -f deploy/docker/docker-compose.yml up
```

For more details, visit [docs.almanak.co](https://docs.almanak.co/).

## AI Agent Skills

Supercharge your strategy development with AI agent support. The Almanak strategy builder skill teaches AI coding agents (Claude Code, Codex, Cursor, Copilot, and others) how to write strategies using the SDK.

### Install for Your Agent

```bash
# Claude Code / Codex / any skills.sh-compatible agent
npx skills add almanak-co/almanak-sdk

# Or via the Almanak CLI (auto-detects your agent platform)
almanak agent install

# OpenClaw
clawhub install almanak-strategy-builder
```

Once installed, your agent understands the full intent vocabulary, market data API, backtesting tools, and CLI commands. Just describe what you want to build.

## Documentation

For detailed documentation, visit [docs.almanak.co](https://docs.almanak.co/)

## Support

- [Discord](https://discord.gg/c4jY28WrEB)
- [Telegram](https://t.me/+G1O9NFuz-AAzYmQy)
- [Twitter](https://x.com/Almanak__)
