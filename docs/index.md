![Almanak](assets/logo-dark.svg){ .hero-logo }

The Almanak SDK provides a comprehensive framework for developing, testing, and deploying autonomous DeFi agents. Built on an intent-based architecture, strategies are expressed as high-level intents with minimal boilerplate.

## Features

- **Intent-Based Architecture** - Express trading logic as high-level intents (Swap, LP, Borrow, etc.). The framework handles compilation and execution.
- **Three-Tier State Management** - Automatic persistence with HOT/WARM/COLD tiers for reliability.
- **Comprehensive Backtesting** - PnL simulation, paper trading on Anvil forks, and parameter sweeps.
- **Multi-Chain Support** - Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Plasma, Blast, Mantle, Berachain, and more.
- **Protocol Integration** - Uniswap V3, Aave V3, Morpho Blue, GMX V2, Pendle, Polymarket, Kraken, and more.
- **Non-Custodial Design** - Full control over your funds through Safe smart accounts.
- **Production-Ready** - Built-in alerting, stuck detection, emergency management, and canary deployments.

## Installation

```bash
pip install almanak
```

## Quick Start

```bash
# Scaffold a new strategy from a template
almanak strat new

# Run it on a local Anvil fork -- no wallet or API keys required
cd my_strategy
almanak strat run --network anvil --once
```

Anvil fork testing is the recommended starting point. The SDK auto-starts a local fork, uses a default funded wallet, and runs your strategy with zero configuration. See [Getting Started](getting-started.md) for the full walkthrough.

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

## Architecture

```text
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
  gateway/             # gRPC gateway sidecar
  transaction_builder/ # Low-level tx building
  core/                # Enums, models, utilities
  cli/                 # Command-line interface
```

All strategies run through a **gateway-only architecture** for security. The gateway sidecar holds all secrets and exposes a controlled gRPC API. Strategy containers have no secrets and no direct internet access.

## Feedback & Feature Requests

Have an idea, found a bug, or want to request a feature? Head over to our [Discord](https://discord.gg/yuCMvQv3rN) and post in the appropriate channel. We actively monitor feedback there and use it to shape the SDK roadmap.

## Next Steps

- [Getting Started](getting-started.md) - Installation and first strategy walkthrough
- [CLI Reference](cli/almanak.md) - All CLI commands
- [API Reference](api/index.md) - Full Python API documentation
- [Gateway](gateway/api-reference.md) - Gateway gRPC API
