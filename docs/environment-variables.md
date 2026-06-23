# Environment Variables

All strategies run through the **gateway sidecar** (auto-started by `almanak strat run`). The gateway holds secrets, provides RPC access, and executes transactions.

Create a `.env` file in your strategy directory with the variables below.

!!! info "ALMANAK_GATEWAY_ env_prefix"
    Every field in `GatewaySettings` is automatically bound to `ALMANAK_GATEWAY_<UPPER_FIELD_NAME>`; only the ones documented below have load-bearing effects worth calling out. The full list lives in `almanak/gateway/core/settings.py` and in the project's [`.env.example`](https://github.com/almanak-co/sdk/blob/main/.env.example).

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

!!! warning "Some public RPCs are unsuitable as Anvil-fork upstreams"
    The free public RPCs for **0G** (`https://rpc.ankr.com/0g_mainnet_evm`) and **X-Layer** (`https://rpc.xlayer.tech`) are full nodes that aggressively prune historical state and frequently return `DEADLINE_EXCEEDED` under sustained load. They work for one-off swaps but break Anvil-fork demos that hold positions across blocks (LP teardown, lending repay, etc.) because the LP_CLOSE / REPAY compile path queries storage slots on a block that has already been pruned (`missing trie node` from the upstream).

    For these chains, set a paid archive-capable endpoint:

    ```bash
    ZEROG_RPC_URL=https://your-archive-0g-endpoint
    XLAYER_RPC_URL=https://your-archive-xlayer-endpoint
    ```

!!! warning
    Never commit private keys. Use a dedicated testing wallet for development.

**Note:** The gateway also accepts `ALMANAK_GATEWAY_PRIVATE_KEY` (with its own prefix). If set, it takes precedence. Otherwise, the gateway falls back to `ALMANAK_PRIVATE_KEY` -- so you only need one variable.

---

## Optional API Keys

Set these based on which protocols and features your strategy uses.

| Variable | When needed | Get a key |
|----------|-------------|-----------|
| `ENSO_API_KEY` | Swap routing via Enso Finance aggregator | [enso.finance](https://enso.finance/) |
| `COINGECKO_API_KEY` | CoinGecko API key for market prices. Also required for CoinGecko Onchain pool/OHLCV data when running a local gateway. | [coingecko.com/en/api](https://www.coingecko.com/en/api) |
| `ALMANAK_API_KEY` | Almanak platform authentication | [app.almanak.co](https://app.almanak.co/) |
| `ALMANAK_DASHBOARD_API_KEY` | API key used by the operator dashboard when calling non-gateway REST endpoints (pause/resume go through gateway; `bump-gas` / `cancel-tx` still use REST). Must match a key listed in `ALMANAK_API_KEYS` on the API server. | `dash_abc123...` |
| `THEGRAPH_API_KEY` | Backtesting with subgraph data (DEX volumes, lending APYs) | [thegraph.com/studio](https://thegraph.com/studio/) |

### Agentic CLI (`almanak ax -n`)

Required for the natural-language mode of the operator CLI (`almanak ax -n "<prompt>"`). Any OpenAI-compatible chat-completions endpoint works.

| Variable | Description | Default |
|----------|-------------|---------|
| `AGENT_LLM_API_KEY` | API key for the LLM provider. Without this set, `almanak ax -n` refuses to run. | unset |
| `AGENT_LLM_BASE_URL` | OpenAI-compatible base URL. Point at OpenAI, Anthropic via a compat proxy, Together, etc. | `https://api.openai.com/v1` |
| `AGENT_LLM_MODEL` | Model identifier to use. | `gpt-4o` |

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
| `ALMANAK_PRIVATE_KEY` | Preferred local EOA signer for the managed gateway. The gateway derives the Polymarket signer address from this key. |
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Hosted Safe funder address for deployed strategies. When set with Safe mode, the gateway submits Polymarket orders for the Safe wallet instead of an EOA wallet. |
| `POLYMARKET_WALLET_ADDRESS` | Optional gateway-side override for the Polymarket funder address. Usually unnecessary; defaults to the Safe address in Safe mode, otherwise the signer address. |
| `POLYMARKET_API_KEY` | Optional pre-provisioned CLOB API key. If unset, the gateway derives/creates credentials from the signer when needed. |
| `POLYMARKET_SECRET` | Optional pre-provisioned HMAC secret paired with `POLYMARKET_API_KEY`. |
| `POLYMARKET_PASSPHRASE` | Optional pre-provisioned API passphrase paired with `POLYMARKET_API_KEY`. |
| `POLYMARKET_PRIVATE_KEY` | Optional override for Polymarket signing. Falls through to `ALMANAK_PRIVATE_KEY` when unset (see fallback ladder below). |
| `ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS` | TTL (seconds) for the gateway's bounded LRU cache of Polymarket market metadata used on every BUY/SELL. Default `60`; set to `0` to disable (every order re-reads from Gamma); hard-capped at 86400 (24 h). Read once at gateway startup, so changes require a gateway restart. Lower under incident if you suspect stale tick / min-size metadata. |

Polymarket signing-key fallback ladder (VIB-3772). The gateway resolves
`polymarket_private_key` in this order; the first non-empty value wins:

1. `ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY` — gateway-prefixed pydantic field.
2. `POLYMARKET_PRIVATE_KEY` — bare legacy name.
3. `ALMANAK_POLYMARKET_PRIVATE_KEY` — almanak-prefixed alias.
4. `ALMANAK_PRIVATE_KEY` (via the resolved primary signer key) — unifies the
   default flow so a single `ALMANAK_PRIVATE_KEY` in `.env` is enough to use
   Polymarket strategies. The gateway logs an INFO line at startup when this
   rung is taken so operators see which key is signing Polymarket orders.

Use a dedicated `POLYMARKET_PRIVATE_KEY` only when you intentionally want
Polymarket trades signed by a different wallet from the rest of the SDK.

Notes:
- Strategy containers should not need `POLYMARKET_*` secrets.
- Local SDK usage should typically only need the gateway signer key (`ALMANAK_PRIVATE_KEY`).
- Hosted Safe deployments should use the Safe variables below plus the gateway signer setup; Polymarket-specific API credentials can be auto-derived by the gateway.

### Pendle

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_PENDLE_API_KEY` | Pendle protocol API key |
| `ALMANAK_GATEWAY_PENDLE_API_CACHE_TTL` | TTL (seconds) for the gateway's Pendle API response cache. Default `15.0`. |

### Solana

| Variable | Description |
|----------|-------------|
| `SOLANA_PRIVATE_KEY` | Ed25519 keypair in base58 format (or 64-char hex seed). Required for Solana strategies. |
| `SOLANA_RPC_URL` | Solana RPC endpoint. Defaults to `https://api.mainnet-beta.solana.com` (rate-limited). Use Helius, QuickNode, or Triton for production. |
| `JUPITER_API_KEY` | Jupiter aggregator API key. Free tier is used if unset. |
| `DRIFT_DATA_API_BASE_URL` | Override for the Drift data API base URL (default `https://data.api.drift.trade`). |
| `METEORA_API_BASE_URL` | Override for the Meteora API base URL (default `https://dlmm.datapi.meteora.ag`). |
| `ORCA_API_BASE_URL` | Override for the Orca API base URL (default `https://api.orca.so/v2/solana`). |
| `RAYDIUM_API_BASE_URL` | Override for the Raydium API base URL (default `https://api-v3.raydium.io`). |

### Polymarket data endpoints

| Variable | Description |
|----------|-------------|
| `POLYMARKET_GAMMA_URL` | Override for the Polymarket Gamma (market metadata) endpoint. Falls back to the upstream default when unset. |
| `POLYMARKET_CLOB_URL` | Override for the Polymarket CLOB (orderbook + order management) endpoint. |
| `POLYMARKET_DATA_API_URL` | Override for the Polymarket data API (positions / history) endpoint. |

### Other external integrations

| Variable | Description |
|----------|-------------|
| `LIFI_API_KEY` | Li.Fi bridge / swap aggregator API key. |
| `RUGCHECK_API_KEY` | Rugcheck.xyz API key for Solana token risk scoring. |

---

## Gateway Auth & Security

Load-bearing for hosted (Almanak Infra) deployments. Each variable is read once at gateway startup; changes require a restart.

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_AUTH_TOKEN` | Shared-secret token for gRPC authentication. When set, clients must provide this token in metadata to access services. **Required on hosted deployments.** |
| `ALMANAK_GATEWAY_ALLOW_INSECURE` | When `true`, allows the gateway to start without `ALMANAK_GATEWAY_AUTH_TOKEN`. Default `false` (gateway refuses to start). **Local development only** — never set on hosted deployments. |
| `ALMANAK_GATEWAY_OPERATOR_TOKEN` | Second-factor token (VIB-4493 Phase 1) required for mutation RPCs on `DashboardService` (`PreviewReconcile`, `ApplyReconcile`, `RefreshRegistryFromChain`). Callers must send the same value in the `x-operator-token` metadata header in addition to the regular auth token. When unset (default), the handlers fall back to auth-token-only — safe for single-user / local deployments. |

!!! danger "Hosted deployments are unsafe without these"
    Omitting `ALMANAK_GATEWAY_AUTH_TOKEN` (or enabling `ALMANAK_GATEWAY_ALLOW_INSECURE=true`) on a hosted gateway exposes every gRPC service to unauthenticated callers — including `ExecutionService`, which signs and submits transactions. Treat both as production secrets.

---

## Gateway Connection & Networking

How the gateway binds itself and how strategy clients reach it. The defaults are tuned for local development on `localhost`; hosted deployments override these via the Infra-owned K8s manifest.

| Variable | Description | Default |
|----------|-------------|---------|
| `ALMANAK_GATEWAY_HOST` | HTTP server bind address (FastAPI). Set to `0.0.0.0` to bind externally (containers / shared dev hosts). | `127.0.0.1` |
| `ALMANAK_GATEWAY_PORT` | HTTP server port. | `8000` |
| `ALMANAK_GATEWAY_GRPC_HOST` | gRPC server bind address — this is the one strategy containers reach. | `127.0.0.1` |
| `ALMANAK_GATEWAY_GRPC_PORT` | gRPC server port. | `50051` |
| `ALMANAK_GATEWAY_TIMEOUT` | Default RPC deadline (seconds) for the strategy-side gRPC client. Must be `> 0`. | `30.0` |
| `ALMANAK_GATEWAY_STANDALONE` | Opt-in flag — when `true` the gateway resolves its local SQLite path through the lenient `local_db_path` helper (falls back to `~/.local/share/almanak/utility/almanak_state.db`). When `false` (default in local mode), the gateway uses `local_strategy_db_path`, which raises `LocalPathError` rather than silently writing to the per-user utility DB. The CLI flag `almanak gateway --standalone` is the operator surface; `almanak ax` / test workflows that need a non-strategy gateway pass it explicitly. Hosted mode (`AGENT_ID` set) ignores this field. | `false` |
| `ALMANAK_GATEWAY_LIFECYCLE_WRITER` | Hosted-only — distinguishes the strategy-pod gateway (writer) from the dashboard-pod gateway (reader). Both pods ship the same image; only the strategy-pod gateway sets this to `true`, so the dashboard-pod gateway stays read-only for lifecycle state and avoids racing the strategy's `agent_state` writes. Local mode ignores this field. | `false` |
| `ALMANAK_GATEWAY_DATABASE_URL` | Postgres DSN for the hosted state backend (`metrics_db`). **Must be set in hosted mode; must NOT be set in local mode.** A mismatch is fatal at boot. | unset |
| `ALMANAK_GATEWAY_CHAINS` | Comma-separated list of chains to pre-initialize at startup (`bnb,arb,base`). Empty = accept any chain on-demand. Each entry is canonicalized via `resolve_chain_name` so aliases work (`bsc`/`bnb`/`binance` all resolve). | unset |
| `ALMANAK_GATEWAY_PRICE_SOURCE_TIMEOUT_SECONDS` | Per-source wall-clock bound (seconds) on each price source's `get_price` coroutine in the `PriceAggregator`. A source that exceeds it is recorded as an error ("unmeasured", never a zero price) and does not sink the aggregate. Above each source's internal HTTP timeout, below the 30s `decide()` budget. `<= 0` disables the bound. | `10.0` |
| `ALMANAK_GATEWAY_PRICE_AGGREGATOR_TIMEOUT_SECONDS` | Global wall-clock bound (seconds) on the whole concurrent price fan-out across all sources. On the cutoff, sources that haven't returned are recorded as timeout errors and the aggregate proceeds with whatever valid results arrived. Sits under the 30s `decide()` budget / 60s pre-warm window. `<= 0` disables the bound. | `15.0` |

### Client connection flags & env-var precedence

The CLI flags that tell a **client** how to reach the gateway — `--gateway-host` /
`--gateway-port`, shared by `almanak strat run`, `strat status` / `list` / `logs` /
`pause` / `resume`, `ax`, and `teardown` — resolve their value with this precedence
(matching `GatewayClientConfig.from_env`):

| Precedence | Source |
|---|---|
| 1 (highest) | the explicit `--gateway-host` / `--gateway-port` flag |
| 2 | `ALMANAK_GATEWAY_HOST` / `ALMANAK_GATEWAY_PORT` (canonical) |
| 3 | `GATEWAY_HOST` / `GATEWAY_PORT` (legacy, **deprecated**) |
| 4 (default) | `127.0.0.1` / `50051` |

Set the canonical `ALMANAK_GATEWAY_*` names. The legacy unprefixed `GATEWAY_*` names
still work but emit a one-time deprecation `UserWarning` at CLI start
(`warn_legacy_gateway_envvars`) and will be removed in a future release. All CLI surfaces
share the single `gateway_client_options` decorator, so this precedence is identical
across every command (VIB-5163 / GH #2099).

---

## Logging & Audit

| Variable | Description | Default |
|----------|-------------|---------|
| `ALMANAK_GATEWAY_DEBUG` | When `true`, the gateway runs in debug mode (verbose logging, FastAPI hot-reload). | `false` |
| `ALMANAK_GATEWAY_LOG_LEVEL` | Log level for the gateway process: `debug`, `info`, `warning`, `error`. | `info` |
| `ALMANAK_GATEWAY_AUDIT_ENABLED` | Toggle the audit-event log (mutation RPCs, executions). | `true` |
| `ALMANAK_GATEWAY_AUDIT_LOG_LEVEL` | Log level for audit events (independent of `ALMANAK_GATEWAY_LOG_LEVEL`). | `info` |
| `ALMANAK_LOG_EMOJIS` | Strategy-process log emoji prefixes. Set to `false` / `0` / `no` to disable. | `true` |
| `ALMANAK_REDACT_SECRETS` | Redact known secret patterns (private keys, JWTs) from strategy logs. Set to `false` to disable. | `true` |

---

## Anvil & Fork Health

Read by the strategy launcher and gateway when running against an Anvil fork (`almanak strat run --network anvil` / `almanak ax --network anvil`).

| Variable | Description | Default |
|----------|-------------|---------|
| `ANVIL_PORT` | Generic Anvil port. Per-chain overrides take precedence (see `ANVIL_<CHAIN>_PORT` in the strategy CLI docs). | `8545` |
| `ANVIL_URL` | Explicit Anvil URL override (e.g., `http://localhost:8545`). When set, takes precedence over per-chain port discovery. | unset |
| `ANVIL_FORK_BLOCK` | Generic fork block for all chains. | unset |
| `ANVIL_FORK_BLOCK_<CHAIN>` | Per-chain fork block (e.g., `ANVIL_FORK_BLOCK_ARBITRUM=180000000`). Takes precedence over `ANVIL_FORK_BLOCK`. Invalid values fall back to chain head. | unset |
| `ANVIL_FORK_CACHE_PATH` | Override for the Anvil fork-state cache directory. | platform default |
| `ALMANAK_GATEWAY_ANVIL_WATCHDOG_INTERVAL` | Anvil-process watchdog interval (seconds). Must be `> 0`. | `5.0` |
| `ALMANAK_FORK_RPC_TIMEOUT` | RPC timeout (seconds) for fork-mode strategies. | `8.0` |
| `ALMANAK_FORK_HEALTH_TIMEOUT` | Health-probe timeout (seconds) when bringing up a fork. | `5.0` |
| `SOLANA_VALIDATOR_PORT` | Solana local-validator port for fork tests. | `8899` |

---

## Strategy Runtime & Local Paths

Strategy-process-side flags that govern how the SDK locates state and what guardrails it applies.

| Variable | Description | Default |
|----------|-------------|---------|
| `ALMANAK_STRATEGY_FOLDER` | Pin the active strategy folder. Auto-set when the CLI is launched from inside a strategy directory; only set manually when launching from elsewhere. | unset |
| `ALMANAK_STATE_DB` | Explicit override for the strategy SQLite path. By default the SDK derives this from `local_db_path(strategy_folder)`; the cwd-relative `./almanak_state.db` legacy default is **removed** (VIB-3761). | unset |
| `ALMANAK_DEMO_MODE` | When truthy, the SDK relaxes some safety checks intended for built-in demo strategies (smaller wallet balances, default funding plans). Never set on real strategies. | `false` |
| `ALMANAK_FORCE_PRODUCTION` | Force-enable production guardrails even when other heuristics would relax them. | `false` |
| `MAX_VALUE_USD` / `ALMANAK_MAX_VALUE_USD` | Hard ceiling (USD) on per-intent transaction value. Enforced at orchestrator submission time; non-zero values block live intents that exceed it. Decimal string; leading `$` and commas rejected. Gateway / paper-trading paths do not enforce this — it is an EOA-mode last-resort guardrail. | unset |
| `ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S` | TTL (seconds) for the token-resolution negative cache (unknown-token responses). | `300` |
| `ALMANAK_TOKEN_NEGATIVE_CACHE_MAX` | Max entries in the token-resolution negative cache. | `1000` |

### Post-execution reconciliation

Controls the post-execution balance reconciliation that verifies a confirmed swap's
on-chain deltas match the intent (VIB-3158 / VIB-3348 / VIB-3350). Reconciliation reads
are **block-anchored** — the post-execution balance is read as of the confirmed receipt
block, not unanchored `"latest"` (which a lagging RPC replica could answer with pre-tx
state, producing a false zero-delta incident on a successful swap).

| Variable | Description | Default |
|----------|-------------|---------|
| `ALMANAK_RECONCILIATION_ENFORCEMENT` | When truthy (`1` / `true` / `yes`), a reconciliation incident flips the iteration to failure (circuit breaker + alert). Default is **observation mode** (incidents logged + attached to the result, never blocking) until the block-anchored read work has baked. A *degraded* report (no receipt block, or a post-read that fell back to unpinned `"latest"`) is **never** enforced even when this is on. | `false` |
| `ALMANAK_RECONCILIATION_CONFIRMATION_DEPTH` | Proactive confirmation-depth wait (blocks) before the block-pinned post-read, so a lagging replica has indexed the receipt block. **Opt-in, default OFF.** Unset / `0` → no wait; a positive int → that many confirmations on every chain; `-1` → the per-chain recommended depth from `ChainDescriptor.reorg_safe_depth` (Ethereum 12, Polygon 10, Avalanche 5; generic-L2 default 3). **Warning:** a depth larger than the strategy cycle interval serializes cycles (Ethereum @ 12 ≈ 2.5 min). A non-integer value fails at boot. | unset (OFF) |
| `ALMANAK_RECONCILIATION_CONFIRMATION_TIMEOUT_SECONDS` | Upper bound (seconds) on the confirmation-depth wait above; on timeout the read proceeds anyway (still pinned) and the report is flagged unconfirmed. Ignored when the wait is OFF. Must be `> 0`. | `12.0` |

### Gas cost caps

VIB-4879 made gas-cost caps **chain-safe by default**. The deprecated global `ALMANAK_MAX_GAS_PRICE_GWEI` is the wrong unit for multi-chain (gwei is per-chain — see the [Migration](#migration-vib-4879) note below).

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_GAS_COST_USD` / `ALMANAK_MAX_GAS_COST_USD` | **Recommended primary cap.** Per-intent gas cost ceiling in USD. Chain-agnostic by construction; the in-memory price oracle (already maintained for accounting / portfolio valuation) supplies the per-chain native price at zero new I/O cost. When the oracle has no native price (yet-to-be-fetched, fetch failed, circuit open), the USD path is disabled with a WARNING and the gwei descriptor cap is the sole backstop. | unset (off) |
| `MAX_GAS_COST_NATIVE` / `ALMANAK_MAX_GAS_COST_NATIVE` | Per-intent gas cost ceiling in native-token units. Per-chain by construction; only useful when running a single-chain strategy where the operator knows the native token. | chain descriptor (e.g. Polygon 50 MATIC) |
| `ALMANAK_MAX_GAS_PRICE_GWEI_<CHAIN>` | Chain-scoped gwei cap (escape hatch). Example: `ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON=600`. Affects only the named chain. Values exceeding 10,000 gwei (the sane absolute ceiling) are clamped with a WARNING; malformed or non-positive values raise `ConfigurationError` at boot. The legacy unprefixed `MAX_GAS_PRICE_GWEI_<CHAIN>` form is also accepted. | chain descriptor default |
| `ALMANAK_MAX_GAS_PRICE_GWEI` *(deprecated)* | **Deprecated and ignored on mainnet (VIB-4879).** A single global gwei number cannot represent operator intent across chains with ~22,000× native-price spread. Setting this emits a one-time WARNING per chain at boot. Anvil mode is unchanged (gas costs no real money locally). Migrate to `ALMANAK_MAX_GAS_COST_USD` (recommended) or the chain-scoped form above. | unset |

#### Migration (VIB-4879)

If your `.env` contains `ALMANAK_MAX_GAS_PRICE_GWEI=...`:

- **Before VIB-4879:** the global value silently clobbered every chain's descriptor cap — breaking multi-chain strategies on chains with high gwei / cheap native (Polygon, BSC, …). This was the reported "gas price" Polygon symptom.
- **After VIB-4879:** the global value is ignored with a WARNING. For most operators this is a strict improvement; no action required. To re-enable explicit gas-cost control:
  - **Recommended:** `ALMANAK_MAX_GAS_COST_USD=25` — one number, every chain.
  - **Chain-specific:** `ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON=600`, etc.

---

## Pool History Service

Operator tunables for the gateway's historical-pool snapshots service. The service is feature-flagged off by default — see the [PoolHistoryService section in the Gateway API reference](gateway/api-reference.md#poolhistoryservice) for behavior and provider order.

| Variable | Description | Default |
|----------|-------------|---------|
| `ALMANAK_GATEWAY_POOL_HISTORY_ENABLED` | Kill-switch (VIB-4728 / POOL-2). Default `false` until POOL-5 wires real providers. When `false`, `GetPoolHistory` returns `UNAVAILABLE`. | `false` |
| `ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_1H` | Soft cap (days) on 1h-resolution requests. Non-positive overrides fall back to the default. | `90` |
| `ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_4H` | Soft cap (days) on 4h-resolution requests. | `180` |
| `ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_1D` | Soft cap (days) on 1d-resolution requests. | `730` |
| `ALMANAK_GATEWAY_POOL_HISTORY_CACHE_MAX_ENTRIES` | In-memory cache entry cap. | `5000` |
| `ALMANAK_GATEWAY_POOL_HISTORY_CACHE_MAX_BYTES` | In-memory cache byte cap. | `67108864` (64 MiB) |

---

## DexScreener Scam Gates

Thresholds the gateway applies when accepting a DexScreener-sourced pool as a pricing reference. NaN or non-positive overrides are rejected at boot (so a typo can't silently disable the gates).

| Variable | Description | Default |
|----------|-------------|---------|
| `ALMANAK_GATEWAY_DEXSCREENER_MIN_LIQUIDITY_USD` | Minimum pool liquidity (USD). | `10000.0` |
| `ALMANAK_GATEWAY_DEXSCREENER_MIN_VOLUME_USD` | Minimum 24h volume (USD). | `1000.0` |
| `ALMANAK_GATEWAY_DEXSCREENER_MIN_TURNOVER_RATIO` | Minimum 24h volume / TVL ratio. Must be in `[0, 1]`. | `0.05` |
| `ALMANAK_GATEWAY_DEXSCREENER_DOMINANCE_MULTIPLE` | Multiple of next-largest pool's TVL the candidate must beat to be considered dominant. | `3.0` |

---

## Manual Price Overrides

Last-resort fallback for tokens that no real oracle source can price (e.g., long-tail tokens on emerging chains).

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_ENABLE_MANUAL_PRICE_OVERRIDES` | Enable the `ManualPriceOverrideSource` fallback. Default `false`. Off by default because a mis-set env var can feed a wrong price into slippage / teardown decisions. |
| `ALMANAK_PRICE_OVERRIDE_<TOKEN>` | Per-token override price in USD. Consulted only when every real oracle source failed to price the token. Example: `ALMANAK_PRICE_OVERRIDE_W0G=0.012`. |

Set both: the enable flag turns the source on; the per-token vars supply the prices.

---

## Tenderly Simulation

Used by `SimulationService.SimulateBundle` when the simulator is set to `"tenderly"` (or auto-selected). All three must be set together — leaving any one empty disables Tenderly and falls back to Alchemy simulation when available.

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_TENDERLY_ACCOUNT_SLUG` | Tenderly account slug (the `<account>` segment of the dashboard URL). |
| `ALMANAK_GATEWAY_TENDERLY_PROJECT_SLUG` | Tenderly project slug within the account. |
| `ALMANAK_GATEWAY_TENDERLY_ACCESS_KEY` | Tenderly access key with simulation permissions ([account settings → access keys](https://dashboard.tenderly.co/account/authorization)). |

---

## Portfolio Provider (Multi-Provider)

Configures the gateway's portfolio valuation source(s). Used by `IntegrationService.GetWalletPortfolio` / `GetWalletPositions` to aggregate balances and DeFi positions across chains.

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_PORTFOLIO_API_KEY` | Single-provider API key (legacy single-provider path). |
| `ALMANAK_GATEWAY_PORTFOLIO_API_PROVIDER` | Single-provider name. Default `zerion`. |
| `ALMANAK_GATEWAY_PORTFOLIO_PROVIDERS` | Multi-provider override. Comma-separated provider names in priority order (e.g., `zerion,moralis`). When set, takes precedence over the single-provider keys. Each provider reads its own API key from `{NAME}_API_KEY` (e.g., `ZERION_API_KEY`, `MORALIS_API_KEY`). |

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

For deployed or sidecar gateway environments, set
`ALMANAK_GATEWAY_COINGECKO_API_KEY`. CoinGecko Onchain DEX endpoints require a
valid Pro API key via the gateway; without it, pool analytics, pool history, and
DEX-native OHLCV fallbacks fail fast with an explicit key error.

All other gateway and framework settings have sensible defaults and do not need to be set. See [`.env.example`](https://github.com/almanak-co/sdk/blob/main/.env.example) for the full list of advanced options.
