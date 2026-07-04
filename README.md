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
  <a href="https://sdk.docs.almanak.co/"><img src="https://img.shields.io/badge/docs-sdk.docs.almanak.co-purple?style=flat-square" alt="Docs"></a>
  <a href="https://discord.gg/c4jY28WrEB"><img src="https://img.shields.io/badge/Discord-join-5865F2?style=flat-square&logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://x.com/almanak"><img src="https://img.shields.io/badge/Twitter-follow-1DA1F2?style=flat-square&logo=x&logoColor=white" alt="Twitter"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> |
  <a href="#quick-start">Quick Start</a> |
  <a href="#writing-a-strategy">Strategies</a> |
  <a href="#backtesting">Backtesting</a> |
  <a href="https://sdk.docs.almanak.co/">Docs</a>
</p>

<p align="center">
  <a href="https://sdk.docs.almanak.co/">English</a> |
  <a href="https://sdk.docs.almanak.co/zh/">中文</a> |
  <a href="https://sdk.docs.almanak.co/fr/">Français</a> |
  <a href="https://sdk.docs.almanak.co/es/">Español</a>
</p>

---

Almanak is an intent-based Python framework for developing, testing, and deploying autonomous DeFi strategies. Express trading logic as high-level intents - the framework handles compilation, execution, and state management across 18 chains and 46 protocol connectors.

## Features

- **Intent-Based Architecture**: Express trading logic as high-level intents (Swap, LP, Borrow, etc.) - the framework handles compilation and execution
- **Three-Tier State Management**: Automatic persistence with HOT/WARM/COLD tiers for reliability
- **Comprehensive Backtesting**: PnL simulation, paper trading on Anvil forks, and parameter sweeps
- **Multi-Chain Support**: 18 chains across EVM and SVM — Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Mantle, X-Layer, Monad, 0G, Solana, HyperEVM, Linea, plus Sonic, Blast, Berachain, Plasma (chain configs present; protocol coverage pending).
- **Protocol Integration**: Uniswap V3, Aave V3, Morpho Blue, GMX V2, Lido, Ethena, Polymarket, Kraken, and more
- **Non-Custodial Design**: Full control over your funds through Safe smart accounts
- **Agentic DeFAI Trading**: Build autonomous LLM-driven agents with 39 built-in tools and policy-enforced safety (BYO LLM API key)
- **Production-Ready**: Built-in alerting, stuck detection, emergency management, and canary deployments

## Installation

```bash
pipx install almanak
```

Optional features ship as extras:

```bash
pipx install 'almanak[dashboard,backtest]'   # web dashboard + backtest charts/optimization
```

| Extra | Enables |
|---|---|
| `dashboard` | `almanak dashboard` and the strategy/backtest web dashboards (streamlit, plotly) |
| `backtest` | backtest chart export and `almanak strat backtest optimize` (matplotlib, plotly, optuna) |
| `code` | Python LSP for `almanak code` (pyright) |

**Using an AI coding agent?** Teach it the SDK in one command:

```bash
almanak agent install
```

This auto-detects your platform (Claude Code, Codex, Cursor, Copilot, and [6 more](https://sdk.docs.almanak.co/agent-skills/)) and installs the strategy builder skill.

## Quick Start

1. **Create a New Strategy** (scaffolds a self-contained Python project with `pyproject.toml`, `.venv/`, `uv.lock`)
   ```bash
   almanak strat new
   ```

2. **Test Your Strategy**

   A managed gateway is auto-started in the background when you run a strategy.
   Use `--dashboard` to launch a live monitoring dashboard alongside execution:

   ```bash
   uv run almanak strat run -d almanak/demo_strategies/uniswap_lp --network anvil --dashboard --once
   ```

   This single command auto-starts Anvil + gateway, opens the dashboard in your browser, and runs one iteration of the strategy.

> **New here?** Start with the [`uniswap_rsi` demo strategy](almanak/demo_strategies/uniswap_rsi/) -
> a fully commented tutorial that walks through RSI-based trading on Uniswap V3.

## Writing a Strategy

Strategies implement the `decide()` method, which receives a `MarketSnapshot` and returns an `Intent` (or `None` to skip the cycle):

```python
from decimal import Decimal
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy

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
| `CollectFeesIntent` | Collect accrued fees on an open LP position |
| `BorrowIntent` / `RepayIntent` | Borrow/repay on lending protocols |
| `SupplyIntent` / `WithdrawIntent` | Supply/withdraw from lending protocols |
| `StakeIntent` / `UnstakeIntent` | Stake/unstake tokens |
| `PerpOpenIntent` / `PerpCloseIntent` | Open/close perpetuals positions |
| `BridgeIntent` | Cross-chain asset bridging |
| `VaultDepositIntent` / `VaultRedeemIntent` | Deposit into / redeem from ERC-4626-style vaults |
| `FlashLoanIntent` | Flash loan operations _(experimental — not yet listed as supported in `almanak info matrix`; pending testing)_ |
| `PredictionBuyIntent` / `PredictionSellIntent` / `PredictionRedeemIntent` | Prediction market trading _(experimental — not yet listed as supported in `almanak info matrix`; pending testing)_ |

## CLI Commands

```bash
# Gateway (auto-started by strat run, or start standalone)
almanak gateway                # Start standalone gateway server
almanak gateway --network anvil  # Start standalone for local Anvil testing

# Direct DeFi actions (no strategy files needed)
almanak ax price ETH                           # Get token price
almanak ax balance USDC --chain base           # Check wallet balance
almanak ax swap USDC ETH 100 --dry-run         # Simulate a swap
almanak ax swap USDC ETH 100                   # Execute after confirmation
almanak ax --chain polygon lending-reserves    # Which Aave reserves are borrowable/active before you configure a strategy
almanak ax -n "swap 5 USDC to WETH on base"   # Natural language mode

# Strategy development
almanak strat new              # Create new strategy from template
almanak strat run --once       # Run single iteration (auto-starts gateway)
almanak strat run --network anvil --once  # Run on local Anvil fork (auto-starts Anvil + gateway)
almanak strat run --network anvil --dashboard  # Run with live dashboard

# Strategy reporting (reads the persisted local SQLite state DB; no gateway call)
almanak strat pnl -s <deployment_id>          # Per-strategy PnL breakdown (human text)
almanak strat pnl -s <deployment_id> --json   # Same payload as machine-readable JSON
#   JSON is version-stamped (`schema_version`) and includes `net_strategy_nav_usd`
#   (positive position value minus lending debt; equals total_value_usd when the
#   strategy holds no borrow positions).

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

> **v1 (beta) — enabled by default since v2.19.1.** The PnL engine is
> conservation-certified on the network-free Trust Matrix. Treat results as
> carrying documented variance bounds (LP fees ±10-15%, perp funding ±15%,
> lending APY ±10%, large-trade slippage ±30%, gas ±20%) and certify on the
> paper trader before going live. Perp support is beta.

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

The demo strategies under [`almanak/demo_strategies/`](almanak/demo_strategies/) are
runnable end to end (see the Demo Strategies table above) and double as backtest
subjects:

```bash
almanak strat backtest run -d almanak/demo_strategies/uniswap_rsi   # RSI mean reversion
almanak strat backtest run -d almanak/demo_strategies/uniswap_lp    # Concentrated LP
almanak strat backtest run -d almanak/demo_strategies/morpho_looping # Leveraged yield
```

> Standalone backtest example scripts are being reorganized into a dedicated
> `Tutorials/` folder (VIB-5332). For complete documentation, see
> [`almanak/framework/backtesting/README.md`](almanak/framework/backtesting/README.md).

## Supported Networks

18 chains have first-class `ChainDescriptor` configs. Chains marked with `*` have one or more registered protocol connectors today; the others have chain configs in place but no protocol coverage yet.

**EVM**

- Ethereum *
- Arbitrum *
- Optimism *
- Base *
- Avalanche *
- Polygon *
- BSC *
- Mantle *
- X-Layer *
- Monad *
- 0G *
- Sonic
- Blast
- Linea *
- Berachain
- Plasma
- HyperEVM *

**SVM**

- Solana *

For the authoritative per-protocol chain matrix, see the [connector reference](https://sdk.docs.almanak.co/api/connectors/).

## Supported Protocols

46 connectors are registered today (`ConnectorRegistry.all()`). See the [connector reference](https://sdk.docs.almanak.co/api/connectors/) for the full matrix of chains × intent types per connector.

- **DEXs / AMMs**: Uniswap V3, Uniswap V4, SushiSwap V3, PancakeSwap V3, TraderJoe V2, Aerodrome, Camelot, Curve, Fluid, Pendle, Orca, Meteora, Raydium CLMM
- **Swap aggregators**: Jupiter, Enso, LiFi
- **Lending**: Aave V3, Morpho Blue, Compound V3, Spark, Euler V2, Fluid, BenQi, Silo V2, Curvance, Kamino
- **Liquid Staking & yield-bearing assets**: Lido, Ethena, Gimo
- **Yield tokenization**: Pendle
- **Perpetuals**: GMX V2, Hyperliquid, Aster Perps, PancakeSwap Perps, Drift
- **CEX Integration**: Kraken
- **Bridges**: Across, Stargate, LiFi
- **Vaults**: Morpho Vault, Lagoon

> **Pending testing (not currently listed as supported):** Prediction Markets
> (Polymarket) and Flash loans (Balancer, Aave V3, Morpho Blue) are
> undergoing further validation and are temporarily withheld from the
> supported matrix (`almanak info matrix`). The connectors remain in the
> codebase but should be treated as experimental until re-listed.

## Demo Strategies

The SDK includes educational demo strategies to help you learn:

| Strategy | Description | Chain | Protocol |
|----------|-------------|-------|----------|
| `uniswap_rsi` | Config-driven RSI swap — buy oversold, sell overbought | Ethereum | Uniswap V3 |
| `uniswap_lp` | Concentrated liquidity position management | Arbitrum | Uniswap V3 |
| `uniswap_v4_hooks` | Hook-aware V4 LP — dynamic-fee hooks + typed hookData | Base | Uniswap V4 |
| `traderjoe_lp` | Liquidity Book position management | Avalanche | TraderJoe V2 |
| `morpho_looping` | Leveraged yield farming via recursive borrowing | Ethereum | Morpho Blue |
| `morpho_blue_collateral_rotator_ethereum` | Rotate collateral across Morpho Blue markets | Ethereum | Morpho Blue |
| `metamorpho_base_yield` | MetaMorpho USDC yield via Moonwell Flagship vault | Base | Morpho |
| `spark_lender` | Supply DAI for lending yield | Ethereum | Spark |
| `euler_v2_supply_ethereum` | Supply/withdraw USDC lifecycle | Ethereum | Euler V2 |
| `benqi_lending_lifecycle` | Borrow → repay → withdraw lending lifecycle | Avalanche | BENQI |
| `pancakeswap_aave_carry_bsc` | Borrow → swap → repay carry trade | BSC | PancakeSwap V3, Aave V3 |
| `lido_staker` | Stake ETH for liquid staking yield | Ethereum | Lido |
| `gmx_perp_lifecycle` | Perpetual futures lifecycle (open + close) | Arbitrum | GMX V2 |
| `gmx_v2_directional_perp` | Directional perp — EMA crossover, close-before-reverse, funding gate, stop-loss | Arbitrum | GMX V2 |
| `mantle_mnt_accumulator` | Multi-signal MNT accumulation | Mantle | Agni Finance |
| `0g_swap` | Wrap native A0GI into W0G | 0G Chain | 0G |

Run any demo with:
```bash
cd almanak/demo_strategies/<strategy_name>
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

For more details, visit [sdk.docs.almanak.co](https://sdk.docs.almanak.co/).

## AI Agent Skills

Supercharge your strategy development with AI agent support. The Almanak strategy builder skill teaches AI coding agents (Claude Code, Codex, Cursor, Copilot, and others) how to write strategies using the SDK.

### Install for Your Agent

```bash
# Claude Code / Codex / any skills.sh-compatible agent
npx skills add almanak-co/almanak-sdk

# Or via the Almanak CLI (auto-detects your agent platform)
almanak agent install
```

Once installed, your agent understands the full intent vocabulary, market data API, backtesting tools, and CLI commands. Just describe what you want to build.

## Agentic DeFAI Trading

The SDK also supports LLM-driven autonomous agents. Instead of writing `decide()` logic,
you write a system prompt and let the LLM reason over market data and call tools.

**Requirements:** Your own LLM API key (OpenAI, Anthropic, or any OpenAI-compatible provider).

The `almanak ax` CLI exposes a **natural language mode** that drives the agentic LLM
infrastructure for one-shot DeFi actions without writing any strategy files:

```bash
AGENT_LLM_API_KEY=sk-... almanak ax -n "swap 5 USDC to WETH on base"
AGENT_LLM_API_KEY=sk-... almanak ax -n "what's the price of ETH?"
```

> Full agentic-agent example strategies are being reorganized into a dedicated
> `Tutorials/` folder (VIB-5332) and will be linked here once they land.

## Contributing

We welcome contributions. See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding standards, and the PR process.

- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)

## Documentation

For detailed documentation, visit [sdk.docs.almanak.co](https://sdk.docs.almanak.co/)

## Support

- [Discord](https://discord.gg/yuCMvQv3rN)
- [Telegram](https://t.me/+G1O9NFuz-AAzYmQy)
- [Twitter](https://x.com/almanak)

