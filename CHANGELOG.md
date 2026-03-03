# Changelog

All notable changes to the Almanak SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [2.2.0] - 2026-03-03

### Added
- Curve Finance swap and LP support wired into intent compiler (#403)
- Velodrome V2 (Optimism) addresses added to Aerodrome connector for cross-chain Solidly-fork support (#412)
- `--teardown-after` CLI flag to auto-close positions after `--once` runs (#416)
- Live on-chain Aave V3 supply/borrow rates via `market.lending_rate()` (#404)
- Live on-chain Compound V3 lending rate fetching (#430)
- `MarketSnapshot.collateral_value_usd()` helper for perp position sizing (#424)
- Interactive platform selector for `almanak agent install` when no platform is auto-detected (#407)
- Nightly Market Data API contract tests and 4 new indicator calculators: ADX, OBV, CCI, Ichimoku (#442)
- Multi-language documentation: Mandarin, French, Spanish translations (#418)
- `/release` skill for automated changelog, tagging, and GitHub release creation

### Changed
- Renamed public repo references from almanak-sdk to sdk (#467)
- Removed ClawHub marketplace references; OpenClaw platform support retained

### Fixed
- Prevent $6.14B wstETH price via magnitude outlier detection in price aggregator (#401)
- Suppress spurious amount-chaining warnings for single intents (#443)
- Pre-fetch prices in teardown path to avoid placeholder fallback (#437)
- Load .env in backtest commands (#453)
- Patch _version.py during release so CLI reports correct version (#448)
- Harden Anvil fork lifecycle and fix flaky intent tests (#417, #441)
- Correct Polygon WETH balance slot from 3 to 0 (#415)
- Accurate revert diagnostic for compilation failures (#414)
- Skip simulation estimation for non-first TXs in multi-TX bundles (#402, #421)
- BorrowIntent summary shows actual amounts instead of N/A (#427)
- GMX V2 receipt parser: correct event topic hashes and EventEmitter matching (#423)
- Prevent 30s gateway timeout during Aerodrome LP_CLOSE compilation (#408)
- Defer Polymarket warning from init to compile time (#406)
- Gas price cap quick wins (#405)
- Receipt parser logs tx=N/A, 0 gas (#410)
- Transfer-based fallback for Aerodrome LP_CLOSE lp_close_data (#409)

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
