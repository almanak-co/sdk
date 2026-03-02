# Changelog

All notable changes to the Almanak SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Unified Token Resolution System via `TokenResolver` class
  - Single source of truth for token addresses and decimals across all chains
  - Multi-layer resolution: memory cache, disk cache, static registry, gateway on-chain lookup
  - Native token auto-wrapping for DEX operations (ETH -> WETH, etc.)
  - Bridged token alias handling (USDC.e, USDbC, USDT.e, WETH.e)
  - On-chain ERC-20 metadata discovery via gateway
  - Thread-safe singleton with disk-persistent caching
- `get_token_resolver()` as primary entry point for all token resolution
- `TokenService` gRPC service in gateway for on-chain token discovery
- 50+ tokens across 8 chains (Ethereum, Arbitrum, Optimism, Base, Polygon, Avalanche, BSC, Plasma)

### Removed
- `TOKEN_ADDRESSES` dict from `almanak.framework.intents.compiler` - use `get_token_resolver()` instead
- `_DeprecatedDict` wrapper class from `almanak.framework.intents.compiler`
- `TOKEN_REGISTRY` dict from `almanak.gateway.data.balance.web3_provider` - use `get_token_resolver()` instead
- `TOKEN_ADDRESSES` and `TOKEN_DECIMALS` dicts from `almanak.gateway.data.price.multi_dex` - use `get_token_resolver()` instead
- Local fallback registries from `Web3BalanceProvider` and `MultiDexPriceService` - both now use `TokenResolver` exclusively

### Deprecated
- `get_default_registry()` function - use `get_token_resolver()` instead
- `TokenRegistry` class - use `TokenResolver` via `get_token_resolver()` instead
- Internal `TOKEN_ADDRESSES` and `TOKEN_DECIMALS` dicts in protocol adapters - all adapters now use `TokenResolver` internally

  **Migration:** Replace all usage of the deprecated APIs with `get_token_resolver()`.
  See `almanak/framework/data/tokens/` for the resolver implementation and migration examples.

  **Timeline:** Deprecated APIs emit `DeprecationWarning` in this release. They will be removed in the next major release (minimum 1 release cycle).

## [2.0.0] - 2026-02-28

First public open-source release of the Almanak SDK.

### Added
- **Intent-based strategy framework** with 19 intent types (Swap, Hold, LP Open/Close, Borrow, Repay, Supply, Withdraw, Stake, Unstake, Perp Open/Close, Flash Loan, Prediction Buy/Sell/Redeem, and more)
- **26 protocol connectors**: Uniswap V3, SushiSwap V3, PancakeSwap V3, TraderJoe V2, Aerodrome, Curve, Balancer, Aave V3, Morpho Blue, Compound V3, Spark, Lido, Ethena, Pendle, GMX V2, Hyperliquid, Polymarket, Kraken, Enso, LiFi, and others
- **12-chain support**: Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Plasma, Blast, Mantle, Berachain
- **Dual backtesting engine**: PnL backtester (historical price simulation) and Paper Trader (live-like execution on Anvil forks), with parameter sweeps, Monte Carlo, walk-forward optimization, and crisis scenario testing
- **Gateway architecture**: Secure gRPC sidecar holding all secrets, with strategy containers running user code in isolation
- **CLI tools**: `almanak strat new`, `almanak strat run`, `almanak strat backtest`, `almanak gateway`, with auto-managed Anvil and gateway lifecycle
- **17+ demo strategies** covering DEX trading, LP management, lending, perpetuals, prediction markets, CEX integration, yield farming, and copy trading
- **Multi-language documentation** site at docs.almanak.co (English, Mandarin, French, Spanish)
- **AI agent skills**: Strategy builder skill for Claude Code, Codex, Cursor, Copilot, and 6 more platforms via `almanak agent install`
- **Non-custodial Safe design**: Fine-grained permission controls through Zodiac Roles Modifier, user maintains full control of funds
- **Three-tier state management**: Automatic HOT/WARM/COLD persistence for strategy state
- **Production services**: Alerting (Slack/Telegram), stuck detection, emergency management, canary deployments
