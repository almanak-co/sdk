# Almanak SDK

[![PyPI version](https://badge.fury.io/py/almanak.svg)](https://badge.fury.io/py/almanak)

The Almanak SDK is a powerful Python library for developing, testing, and deploying autonomous DeFi agents. Built on an intent-based architecture, it provides a comprehensive framework for creating sophisticated trading strategies with minimal boilerplate.

## Features

- **Intent-Based Architecture**: Express trading logic as high-level intents (Swap, LP, Borrow, etc.) - the framework handles compilation and execution
- **Gateway Security**: All external access mediated through secure gRPC gateway
- **Three-Tier State Management**: Automatic persistence with HOT/WARM/COLD tiers
- **Comprehensive Backtesting**: PnL simulation, paper trading on Anvil forks, and parameter sweeps
- **Multi-Chain Support**: Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Blast, Mantle, Berachain
- **Protocol Integration**: Uniswap V3, Aave V3, Morpho Blue, GMX V2, Lido, Ethena, Polymarket, Kraken, and more
- **Non-Custodial Design**: Full control over your funds through Safe smart accounts

## Installation

```bash
pip install almanak
```

## Quick Start

1. **Create a New Strategy**
   ```bash
   almanak strat new
   ```

2. **Start the Gateway and Test**
   ```bash
   # Terminal 1: Start gateway
   almanak gateway

   # Terminal 2: Run strategy
   cd my_strategy
   almanak strat run --once
   ```

## Writing a Strategy

Strategies use an intent-based architecture. Implement the `decide()` method to return an intent:

```python
from almanak import IntentStrategy, SwapIntent, HoldIntent, MarketSnapshot

class MyStrategy(IntentStrategy):
    def decide(self, market: MarketSnapshot) -> Intent:
        eth_price = market.prices.get("ETH")
        usdc_balance = market.balances.get("USDC")

        if eth_price < 2000 and usdc_balance > 1000:
            return SwapIntent(
                token_in="USDC",
                token_out="ETH",
                amount=1000,
                slippage=0.005,
            )
        return HoldIntent(reason="Waiting for better conditions")
```

## Supported Networks

Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Plasma, Blast, Mantle, Berachain

## Supported Protocols

- **DEXs**: Uniswap V3, SushiSwap V3, PancakeSwap V3, TraderJoe V2, Aerodrome, Curve, Balancer
- **Lending**: Aave V3, Morpho Blue, Compound V3, Spark
- **Liquid Staking**: Lido, Ethena
- **Yield**: Pendle
- **Perpetuals**: GMX V2, Hyperliquid
- **Prediction Markets**: Polymarket
- **CEX Integration**: Kraken
- **Aggregators**: Enso, LiFi

## Documentation

For detailed documentation, visit [docs.almanak.co](https://docs.almanak.co/)

## Support

- [Discord](https://discord.gg/c4jY28WrEB)
- [Telegram](https://t.me/+G1O9NFuz-AAzYmQy)
- [Twitter](https://x.com/Almanak__)
