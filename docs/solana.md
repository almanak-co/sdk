# Solana Support

Solana is the first non-EVM chain supported by the Almanak SDK. The same intent-based workflow applies -- `decide() -> Intent -> compile -> execute` -- but the execution substrate underneath is fundamentally different from Ethereum and its L2s.

All three Solana demo strategies have been verified on mainnet with real funds.

## Solana vs EVM -- What's Different

| Aspect | EVM (Ethereum, Arbitrum, etc.) | Solana |
|--------|-------------------------------|--------|
| **State model** | Account-based state trie | Account model with program-owned data accounts |
| **Transactions** | `eth_sendTransaction` with RLP encoding | `VersionedTransaction` with compact binary format |
| **Signing** | secp256k1 ECDSA | Ed25519 |
| **Addresses** | Hex checksummed (`0x...`) | Base58 encoded |
| **Gas** | Gas price bidding (EIP-1559) | Compute units + priority fees (no bidding) |
| **Token standard** | ERC-20 (per-contract) | SPL Token (single program, per-mint accounts) |
| **Local testing** | Anvil fork (full local simulation) | `solana-test-validator` via `--network anvil` (intent compilation still calls real APIs) |

## Supported Protocols

| Protocol | Intent Types | EVM Equivalent | Description |
|----------|-------------|----------------|-------------|
| **Jupiter** | `SwapIntent` | Enso | Swap aggregator routing across Solana DEXs |
| **Kamino** | `SupplyIntent`, `WithdrawIntent` | Aave V3 | Lending and borrowing on Solana |
| **Raydium CLMM** | `LPOpenIntent`, `LPCloseIntent` | Uniswap V3 | Concentrated liquidity positions |

## Environment Setup

Set the following environment variables in your strategy's `.env` file or shell:

```bash
# Required -- Ed25519 keypair in base58 format
SOLANA_PRIVATE_KEY=your_base58_keypair

# Optional -- defaults to public mainnet RPC
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com

# Optional -- Jupiter free tier is used if not set
JUPITER_API_KEY=your_jupiter_api_key
```

!!! note
    `SOLANA_PRIVATE_KEY` accepts both base58-encoded keypairs (standard Solana wallet export) and 64-character hex seeds. The SDK auto-detects the format.

## Demo Strategies

Three demo strategies are included to cover all supported intent types.

### solana_swap -- Token Swap via Jupiter

Swaps a fixed amount of USDC to SOL on every iteration using the Jupiter aggregator. The simplest possible Solana strategy.

**`strategies/demo/solana_swap/config.json`:**

```json
{
    "from_token": "USDC",
    "to_token": "SOL",
    "amount": "0.10",
    "max_slippage_pct": 1.0,
    "chain": "solana"
}
```

### solana_lend -- Supply USDC to Kamino

Supplies a fixed amount of USDC to Kamino Finance on every iteration. Demonstrates the lending intent path on Solana.

**`strategies/demo/solana_lend/config.json`:**

```json
{
    "token": "USDC",
    "amount": "1.0",
    "chain": "solana"
}
```

### solana_lp -- Concentrated LP on Raydium

Opens a concentrated liquidity position on Raydium CLMM in the SOL/USDC pool with a specified price range.

**`strategies/demo/solana_lp/config.json`:**

```json
{
    "pool": "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv",
    "amount_sol": "0.001",
    "amount_usdc": "0.15",
    "range_lower": "80",
    "range_upper": "95",
    "chain": "solana"
}
```

## Running Strategies

Solana strategies run through the gateway, just like EVM strategies. The gateway handles balance queries (via `SolanaBalanceProvider`), execution routing, and RPC access for Solana.

```bash
# Real execution on mainnet (gateway auto-starts)
almanak strat run -d strategies/demo/solana_swap --once

# Dry run (compile intents, no submission)
almanak strat run -d strategies/demo/solana_swap --once --dry-run

# Local fork testing with solana-test-validator
almanak strat run -d strategies/demo/solana_swap --network anvil --once
```

!!! warning
    On mainnet, Solana strategies execute with **real funds**. Start with the small default amounts in the demo configs.

!!! tip
    Use `--dry-run` first to verify that intent compilation succeeds before executing with real funds:

    ```bash
    almanak strat run -d strategies/demo/solana_swap --once --dry-run
    ```

## Local Testing

### solana-test-validator

Running with `--network anvil` starts a local `solana-test-validator` via `SolanaForkManager`. The gateway connects to it at `http://localhost:8899`. Note that intent compilation still calls real external APIs (Jupiter quotes, Raydium on-chain state) since those APIs don't support local validators.

The testing approach for Solana relies on:

1. **Intent compilation tests** -- call real protocol APIs (Jupiter quotes, Raydium on-chain state) to build valid transactions without submitting them
2. **Transaction verification** -- confirm the compiled `VersionedTransaction` is deserializable and structurally valid
3. **On-chain testing** -- execute on mainnet with small amounts to verify end-to-end

### Running Intent Tests

Intent compilation tests live in `tests/intents/solana/` and exercise the full pipeline: `strategy.decide()` -> `compiler.compile()` -> `ActionBundle` containing a valid `VersionedTransaction`.

```bash
# Run all Solana intent tests
uv run pytest tests/intents/solana/ -v

# Run a specific protocol
uv run pytest tests/intents/solana/test_jupiter_swap.py -v
```

!!! note
    These tests call real Solana APIs (Jupiter, Raydium RPC) so they require network access and may occasionally fail if an API is temporarily unavailable.

## Architecture

Solana strategies follow the same gateway-mediated path as EVM strategies. The gateway routes Solana-specific operations to the appropriate providers:

```
decide() -> Intent
         -> Gateway (CompileIntent)
         -> SolanaIntentCompiler (dispatches to protocol adapters)
         -> ActionBundle (contains serialized VersionedTransaction)
         -> Gateway (Execute)
         -> SolanaExecutionPlanner (blockhash, signing, RPC submission)
         -> SolanaSigner (Ed25519 keypair operations)
         -> On-chain result
```

Key components:

- **`ChainFamily.SOLANA`** -- enum value that routes execution away from the EVM pipeline
- **`SolanaIntentCompiler`** -- maps intent types to protocol-specific adapters (Jupiter, Kamino, Raydium)
- **`SolanaExecutionPlanner`** -- handles recent blockhash fetching, transaction signing, and RPC submission
- **`SolanaSigner`** -- wraps Ed25519 keypair operations; auto-detects base58 vs hex seed format
- **`SolanaBalanceProvider`** -- queries native SOL and SPL token balances via Solana JSON-RPC (`getBalance`, `getTokenAccountsByOwner`)
- **Token resolution** -- preserves base58 case for Solana addresses (EVM addresses are lowercased)

### Gateway integration

The gateway's `MarketService` automatically routes balance queries to `SolanaBalanceProvider` when `chain="solana"`. The `RpcService` supports Solana-native RPC methods (`getBalance`, `getTokenAccountsByOwner`, `getTransaction`, etc.) alongside EVM methods. EVM-only convenience methods (`QueryAllowance`, `QueryBalance`, `QueryPositionLiquidity`) return graceful early responses for Solana -- for example, `QueryAllowance` returns max uint64 since SPL tokens don't use ERC-20 allowances.

## Production Considerations

### RPC Provider

The default public RPC (`api.mainnet-beta.solana.com`) is rate-limited and not suitable for production. Use a dedicated provider:

| Provider | Notes |
|----------|-------|
| **Helius** | Popular for Solana DeFi, generous free tier |
| **QuickNode** | Multi-chain, good if you already use them for EVM |
| **Triton** | Solana-native, high throughput |

Set `SOLANA_RPC_URL` to your provider's endpoint. No Alchemy -- Solana RPC is independent of the EVM RPC stack.

### Gateway Support

Solana runs through the gateway like all other chains. The gateway provides:

- **Secret mediation** -- `SOLANA_PRIVATE_KEY` is held by the gateway, not the strategy process
- **Balance queries** -- `MarketService.GetBalance()` routes to `SolanaBalanceProvider` for native SOL and SPL token balances
- **Price data** -- CoinGecko prices work for SOL, USDC, and other Solana tokens via the existing price aggregator
- **State persistence** -- strategy state uses the same gateway-backed storage as EVM strategies
- **RPC proxy** -- `RpcService.Call()` proxies Solana JSON-RPC methods with rate limiting and validation

### Fees

Solana transactions cost ~0.000005 SOL base fee (~$0.001). Jupiter adds a priority fee (currently hardcoded at "veryHigh" priority level) to ensure fast inclusion. Typical all-in cost per swap: $0.01-0.05 depending on network congestion.

Priority fee configuration is not yet exposed to strategy authors (tracked in VIB-378 follow-ups).

### Known Limitations

| Limitation | Impact | Tracking |
|-----------|--------|----------|
| LP close doesn't query on-chain position state | Raydium LP positions with liquidity can't be closed yet | VIB-375 |
| No Anvil-style local fork | `--network anvil` starts `solana-test-validator` but intent compilation still calls real APIs (Jupiter, Raydium) | By design |
| No cross-chain bridging | Can't bridge assets between Solana and EVM chains | Not yet planned |
| On-chain price source is EVM-only | Solana prices come from CoinGecko, not from on-chain oracles (Pyth/Switchboard) | Future work |

## Next Steps

- [Getting Started](getting-started.md) -- General SDK setup and EVM strategy walkthrough
- [Environment Variables](environment-variables.md) -- All configuration options
- [API Reference](api/index.md) -- Full Python API documentation
