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
  See `blueprints/17-token-resolution.md` for the complete migration guide.

  **Timeline:** Deprecated APIs emit `DeprecationWarning` in this release. They will be removed in the next major release (minimum 1 release cycle).
