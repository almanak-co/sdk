# Environment Variables

All strategies run through the **gateway sidecar** (auto-started by `almanak strat run`). The gateway holds secrets, provides RPC access, and executes transactions.

Create a `.env` file in your strategy directory with the variables below.

---

## Required

These must be set before running any strategy.

| Variable | Description | Example |
|----------|-------------|---------|
| `ALMANAK_PRIVATE_KEY` | Wallet private key for signing transactions and deriving your wallet address | `0x4c0883a6...` |

### RPC Access (recommended; free public RPCs used if unset)

| Variable | Priority | Description | Example |
|----------|----------|-------------|---------|
| `ALMANAK_{CHAIN}_RPC_URL` | 1 (highest) | Per-chain RPC URL with ALMANAK prefix | `https://arb-mainnet.infura.io/v3/KEY` |
| `{CHAIN}_RPC_URL` | 2 | Per-chain RPC URL (e.g. `ARBITRUM_RPC_URL`) | `https://arb-mainnet.infura.io/v3/KEY` |
| `ALMANAK_RPC_URL` | 3 | Generic RPC URL for all chains | `https://your-rpc.com/v1/KEY` |
| `RPC_URL` | 4 | Bare generic RPC URL | `https://your-rpc.com/v1/KEY` |
| `ALCHEMY_API_KEY` | 5 (fallback) | Alchemy API key -- URLs built automatically per chain | `abc123def456` |
| `TENDERLY_API_KEY_{CHAIN}` | 6 (fallback) | Tenderly API key for chain-specific RPC (e.g. `TENDERLY_API_KEY_ARBITRUM`) | `abc123...` |

Any provider works: Infura, QuickNode, self-hosted, Alchemy, etc. `ALCHEMY_API_KEY` is an optional fallback that auto-constructs URLs for all supported chains. If none are set, the gateway falls back to free public RPCs (rate-limited, best-effort).

!!! warning
    Never commit private keys. Use a dedicated testing wallet for development.

**Note:** The gateway also accepts `ALMANAK_GATEWAY_PRIVATE_KEY` (with its own prefix). If set, it takes precedence. Otherwise, the gateway falls back to `ALMANAK_PRIVATE_KEY` -- so you only need one variable.

---

## Optional API Keys

Set these based on which protocols and features your strategy uses.

| Variable | When needed | Get a key |
|----------|-------------|-----------|
| `ENSO_API_KEY` | Swap routing via Enso Finance aggregator | [enso.finance](https://enso.finance/) |
| `COINGECKO_API_KEY` | Improves rate limits for price data (works without key) | [coingecko.com/en/api](https://www.coingecko.com/en/api) |
| `ALMANAK_API_KEY` | Platform features: `strat push`, `strat pull`, deployment | [app.almanak.co](https://app.almanak.co/) |
| `THEGRAPH_API_KEY` | Backtesting with subgraph data (DEX volumes, lending APYs) | [thegraph.com/studio](https://thegraph.com/studio/) |

---

## Protocol-Specific

Only needed if your strategy uses these specific protocols.

### Kraken

| Variable | Description |
|----------|-------------|
| `KRAKEN_API_KEY` | Kraken API key ([get credentials](https://www.kraken.com/u/security/api)) |
| `KRAKEN_API_SECRET` | Kraken API secret |

### Polymarket

| Variable | Description |
|----------|-------------|
| `POLYMARKET_WALLET_ADDRESS` | Polymarket wallet address |
| `POLYMARKET_PRIVATE_KEY` | Polymarket signing key |
| `POLYMARKET_API_KEY` | CLOB API key |
| `POLYMARKET_SECRET` | HMAC secret |
| `POLYMARKET_PASSPHRASE` | API passphrase |

### Pendle

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_PENDLE_API_KEY` | Pendle protocol API key |

### Solana

| Variable | Description |
|----------|-------------|
| `SOLANA_PRIVATE_KEY` | Ed25519 keypair in base58 format (or 64-char hex seed). Required for Solana strategies. |
| `SOLANA_RPC_URL` | Solana RPC endpoint. Defaults to `https://api.mainnet-beta.solana.com` (rate-limited). Use Helius, QuickNode, or Triton for production. |
| `JUPITER_API_KEY` | Jupiter aggregator API key. Free tier is used if unset. |

---

## Safe Wallet

For strategies that execute through a Gnosis Safe multisig.

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Safe wallet address |
| `ALMANAK_GATEWAY_SAFE_MODE` | `direct` (Anvil/threshold-1) or `zodiac` (production) |
| `ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS` | Zodiac Roles module address (zodiac mode) |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_URL` | Remote signer service URL (zodiac mode) |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_JWT` | Remote signer JWT (zodiac mode) |

---

## Backtesting

### Archive RPC URLs

Required for historical on-chain data (Chainlink prices, TWAP calculations). Standard RPC nodes don't support historical state queries. Use archive-enabled providers like Alchemy (paid), QuickNode, or Infura.

Pattern: `ARCHIVE_RPC_URL_{CHAIN}` (e.g., `ARCHIVE_RPC_URL_ARBITRUM`, `ARCHIVE_RPC_URL_ETHEREUM`, `ARCHIVE_RPC_URL_BASE`, `ARCHIVE_RPC_URL_OPTIMISM`, `ARCHIVE_RPC_URL_POLYGON`, `ARCHIVE_RPC_URL_AVALANCHE`)

### Block Explorer API Keys

Optional, for historical gas price data. Pattern: `{EXPLORER}_API_KEY`

| Variable | Explorer |
|----------|----------|
| `ETHERSCAN_API_KEY` | [etherscan.io](https://etherscan.io/apis) |
| `ARBISCAN_API_KEY` | [arbiscan.io](https://arbiscan.io/apis) |
| `BASESCAN_API_KEY` | [basescan.org](https://basescan.org/apis) |
| `OPTIMISTIC_ETHERSCAN_API_KEY` | [optimistic.etherscan.io](https://optimistic.etherscan.io/apis) |
| `POLYGONSCAN_API_KEY` | [polygonscan.com](https://polygonscan.com/apis) |
| `SNOWTRACE_API_KEY` | [snowtrace.io](https://snowtrace.io/apis) |
| `BSCSCAN_API_KEY` | [bscscan.com](https://bscscan.com/apis) |

---

## Quick Start `.env`

```bash
# Required
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# RPC access (pick one)
RPC_URL=https://your-rpc-provider.com/v1/your-key
# ALCHEMY_API_KEY=your_alchemy_key  # alternative: auto-builds URLs per chain

# Recommended
ENSO_API_KEY=your_enso_key
COINGECKO_API_KEY=your_coingecko_key
```

All other gateway and framework settings have sensible defaults and do not need to be set. See [`.env.example`](https://github.com/almanak-co/almanak-sdk/blob/main/.env.example) for the full list of advanced options.
