# Gateway API Reference

This document describes the gRPC API exposed by the Almanak Gateway.

## Services Overview

| Service | Methods | Description |
|---------|---------|-------------|
| Health | 3 | Standard gRPC health checks and chain registration |
| MarketService | 5 | Price data, balances, batch balances, technical indicators, and Uniswap V4 pool key lookup |
| StateService | 29 | Strategy state persistence, portfolio snapshots/metrics, transaction ledger, accounting events, position events, accounting outbox, atomic ledger+registry writes, and cutover migration state |
| ExecutionService | 3 | Intent compilation and transaction execution |
| ObserveService | 4 | Logging, alerts, metrics, and timeline events |
| RpcService | 6 | JSON-RPC proxy to blockchains with typed queries |
| IntegrationService | 12 | Third-party data (Binance, CoinGecko, TheGraph, GeckoTerminal, Zerion) |
| DashboardService | 22 | Operator dashboard data, actions, transaction ledger, PnL/cost stack, audit posture, trade tape, activity feed, positions, reconciliation report, and operator reconciliation actions |
| FundingRateService | 2 | Perpetual funding rates and spreads |
| SimulationService | 1 | Transaction bundle simulation (Tenderly/Alchemy) |
| PoolAnalyticsService | 1 | DEX pool analytics (TVL, volume, fees) for risk-adjusted decisions |
| PoolHistoryService | 1 | Historical pool snapshots (TVL, volume, fees over time). Feature-flagged off by default; enable via `ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true` |
| PolymarketService | 20 | Polymarket CLOB API proxy (market data, orders, positions, price history, trade tape) |
| EnsoService | 4 | Enso Finance routing and bundling |
| TokenService | 4 | Token resolution and on-chain metadata |
| LifecycleService | 6 | Agent state management, heartbeat, and commands |
| TeardownService | 23 | Hosted teardown state routing (V2 deployments): teardown requests, execution state, and operator approvals |
| PositionService | 1 | Position registry reconciliation against on-chain truth (T24 / VIB-4210) |

## Health

### Check

Check if the service is healthy (liveness probe).

```protobuf
rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
```

### Watch

Watch for health status changes (streaming readiness probe).

```protobuf
rpc Watch(HealthCheckRequest) returns (stream HealthCheckResponse);
```

### RegisterChains

Pre-initialize execution orchestrators and compilers for specified chains. Call this at startup to warm up chain-specific resources.

```protobuf
rpc RegisterChains(RegisterChainsRequest) returns (RegisterChainsResponse);
```

**Request:**
```protobuf
message RegisterChainsRequest {
  repeated string chains = 1;      // Chain names to pre-initialize (e.g., "arbitrum", "base")
  string wallet_address = 2;       // Wallet address for orchestrator initialization
}
```

**Response:**
```protobuf
message RegisterChainsResponse {
  bool success = 1;
  repeated string initialized_chains = 2;  // Chains successfully initialized
  string wallet_address = 3;               // Wallet address derived from gateway private key
  string error = 4;
  map<string, string> chain_wallets = 5;   // Per-chain wallet addresses resolved from wallet registry
}
```

## MarketService

### GetPrice

Get the current price of a token.

```protobuf
rpc GetPrice(PriceRequest) returns (PriceResponse)
```

**Request:**
```protobuf
message PriceRequest {
  string token = 1;      // Token symbol or address
  string quote = 2;      // Quote currency (default: "USD")
}
```

**Response:**
```protobuf
message PriceResponse {
  string price = 1;          // Decimal as string (precision preserved)
  int64 timestamp = 2;
  string source = 3;
  double confidence = 4;     // 0.0-1.0
  bool stale = 5;
}
```

**Example:**
```python
from almanak.framework.data.price import GatewayPriceOracle

oracle = GatewayPriceOracle(gateway_client)
price = await oracle.get_price("ETH", "USD")
```

### GetBalance

Get token balance for a wallet.

```protobuf
rpc GetBalance(BalanceRequest) returns (BalanceResponse)
```

**Request:**
```protobuf
message BalanceRequest {
  string token = 1;            // Token symbol or address
  string chain = 2;
  string wallet_address = 3;
}
```

**Response:**
```protobuf
message BalanceResponse {
  string balance = 1;          // Human-readable units as string
  string balance_usd = 2;
  string address = 3;
  int32 decimals = 4;
  string raw_balance = 5;      // Wei/raw units as string
  int64 timestamp = 6;
  bool stale = 7;
  string error = 8;
}
```

### BatchGetBalances

Get token balances across multiple chains in a single call.

```protobuf
rpc BatchGetBalances(BatchBalanceRequest) returns (BatchBalanceResponse)
```

**Request:**
```protobuf
message BatchBalanceRequest {
  repeated BalanceRequest requests = 1;
}
```

**Response:**
```protobuf
message BatchBalanceResponse {
  repeated BalanceResponse responses = 1;
}
```


### GetIndicator

Calculate a technical indicator.

```protobuf
rpc GetIndicator(IndicatorRequest) returns (IndicatorResponse)
```

### LookupV4PoolKey

Resolve a Uniswap V4 `bytes32` pool id back to its structured `PoolKey`. Useful for receipt parsers and indexers that observe on-chain events whose log payload carries the bytes32 id rather than the structured key. The gateway populates the cache from observed `PoolManager.Initialize` events; unknown ids return `NOT_FOUND`.

```protobuf
rpc LookupV4PoolKey(LookupV4PoolKeyRequest) returns (LookupV4PoolKeyResponse)
```

## StateService

### LoadState

Load strategy state from storage.

```protobuf
rpc LoadState(LoadStateRequest) returns (StateData)
```

**Request:**
```protobuf
message LoadStateRequest {
  string deployment_id = 1;
}
```

**Response:**
```protobuf
message StateData {
  string deployment_id = 1;
  int64 version = 2;
  bytes data = 3;              // JSON-serialized state
  int32 schema_version = 4;
  string checksum = 5;         // SHA-256 hex
  int64 created_at = 6;
  int64 updated_at = 7;
  string loaded_from = 8;      // "hot", "warm"
}
```

### SaveState

Save strategy state to storage.

```protobuf
rpc SaveState(SaveStateRequest) returns (SaveStateResponse)
```

**Request:**
```protobuf
message SaveStateRequest {
  string deployment_id = 1;
  int64 expected_version = 2;  // For optimistic locking (0 = new state)
  bytes data = 3;              // JSON-serialized state
  int32 schema_version = 4;
}
```

**Response:**
```protobuf
message SaveStateResponse {
  bool success = 1;
  int64 new_version = 2;
  string error = 3;
  string checksum = 4;
}
```

### DeleteState

Delete strategy state.

```protobuf
rpc DeleteState(DeleteStateRequest) returns (DeleteStateResponse)
```

### SavePortfolioSnapshot

Save a portfolio snapshot for tracking valuation over time.

```protobuf
rpc SavePortfolioSnapshot(SaveSnapshotRequest) returns (SaveSnapshotResponse)
```

### GetLatestSnapshot

Get the most recent portfolio snapshot.

```protobuf
rpc GetLatestSnapshot(GetLatestSnapshotRequest) returns (SnapshotData)
```

### GetSnapshotsSince

Get all portfolio snapshots since a given timestamp.

```protobuf
rpc GetSnapshotsSince(GetSnapshotsSinceRequest) returns (SnapshotList)
```

### SavePortfolioMetrics

Save computed portfolio metrics (PnL, Sharpe, drawdown, etc.).

```protobuf
rpc SavePortfolioMetrics(SaveMetricsRequest) returns (SaveMetricsResponse)
```

### GetPortfolioMetrics

Retrieve stored portfolio metrics.

```protobuf
rpc GetPortfolioMetrics(GetMetricsRequest) returns (PortfolioMetricsData)
```

### SaveLedgerEntry

Persist a single structured trade record to the transaction ledger.

```protobuf
rpc SaveLedgerEntry(SaveLedgerEntryRequest) returns (SaveLedgerEntryResponse)
```

**Request:**
```protobuf
message SaveLedgerEntryRequest {
  string id = 1;                   // UUID primary key (idempotent ON CONFLICT target)
  string cycle_id = 2;
  string deployment_id = 3;
  string deployment_id = 4;
  string execution_mode = 5;       // "live" | "paper" | "dry_run"
  int64 timestamp = 6;             // Unix epoch seconds
  string intent_type = 7;
  string token_in = 8;
  string amount_in = 9;            // Decimal string
  string token_out = 10;
  string amount_out = 11;          // Decimal string
  string effective_price = 12;     // Decimal string, "" when not applicable
  optional double slippage_bps = 13;
  int64 gas_used = 14;
  string gas_usd = 15;             // Decimal string, "" when unknown
  string tx_hash = 16;
  string chain = 17;
  string protocol = 18;
  bool success = 19;
  string error = 20;
  bytes extracted_data_json = 21;  // Serialised extracted_data dict
}
```

**Response:**
```protobuf
message SaveLedgerEntryResponse {
  bool success = 1;
  string error = 2;
}
```

### GetLedgerEntry

Retrieve a single ledger entry by id (for re-derivation and audit lookups).

```protobuf
rpc GetLedgerEntry(GetLedgerEntryRequest) returns (GetLedgerEntryResponse)
```

### SumLedgerGasUsd

Sum `transaction_ledger.gas_usd` across a deployment for the
`portfolio_metrics.gas_spent_usd` aggregate (VIB-4247). The gateway performs the
read because hosted strategy containers cannot access Postgres directly.

```protobuf
rpc SumLedgerGasUsd(SumLedgerGasUsdRequest) returns (SumLedgerGasUsdResponse)
```

### SaveAccountingEvent

Persist a typed accounting event (Layer 5 accounting). Writers must route through
the accounting outbox; direct calls from connectors or the runner hot path are
forbidden (see `blueprints/27-accounting.md`).

```protobuf
rpc SaveAccountingEvent(SaveAccountingEventRequest) returns (SaveAccountingEventResponse)
```

### GetAccountingEvents

Retrieve typed accounting events for a strategy or ledger entry.

```protobuf
rpc GetAccountingEvents(GetAccountingEventsRequest) returns (GetAccountingEventsResponse)
```

### HasAccountingEventsForLedger

Idempotency check: returns whether typed events already exist for a given
ledger entry (used by the outbox processor to avoid double-writes).

```protobuf
rpc HasAccountingEventsForLedger(HasAccountingEventsForLedgerRequest) returns (HasAccountingEventsForLedgerResponse)
```

### SavePositionEvent

Persist an LP or perp position lifecycle event (open/close/collect-fees).
Lending intents (`SUPPLY`, `BORROW`, `REPAY`, `WITHDRAW`) are intentionally
**not** routed through this RPC — lending positions are fungible (no stable
`position_id`) and are tracked via Layer 5 typed accounting events instead.

```protobuf
rpc SavePositionEvent(SavePositionEventRequest) returns (SavePositionEventResponse)
```

### SavePositionStateSnapshots

Persist per-iteration position state snapshots (tied to a parent
`portfolio_snapshots.id`, one row per open position). Backs accountant cells
G14 / G15 / L2 / L3 / L5. Non-blocking writes — logged and swallowed in
non-live modes; raises `AccountingPersistenceError` in live via the runner's
mode-aware caller. The hosted Postgres path returns `UNIMPLEMENTED` until the
`metrics-database` migration adds `position_state_snapshots` (PRD T-DRAFT-25,
Infra-owned).

```protobuf
rpc SavePositionStateSnapshots(SavePositionStateSnapshotsRequest) returns (SavePositionStateSnapshotsResponse)
```

### GetPositionHistory

Retrieve the lifecycle event history for a single position.

```protobuf
rpc GetPositionHistory(GetPositionHistoryRequest) returns (GetPositionHistoryResponse)
```

### UpdatePositionAttribution

Update lot-level attribution metadata (FIFO matching policy results) on an
existing position event.

```protobuf
rpc UpdatePositionAttribution(UpdatePositionAttributionRequest) returns (UpdatePositionAttributionResponse)
```

### SaveOutboxEntry

Enqueue a typed-event payload onto the accounting outbox for the async
processor (VIB-3467).

```protobuf
rpc SaveOutboxEntry(SaveOutboxEntryRequest) returns (SaveOutboxEntryResponse)
```

### GetOutboxEntry

Read a single outbox entry by id.

```protobuf
rpc GetOutboxEntry(GetOutboxEntryRequest) returns (GetOutboxEntryResponse)
```

### GetOutboxPending

Fetch the next batch of pending outbox entries for the processor to drain.

```protobuf
rpc GetOutboxPending(GetOutboxPendingRequest) returns (GetOutboxPendingResponse)
```

### UpdateOutboxEntry

Mark an outbox entry as processed, failed, or retry-pending.

```protobuf
rpc UpdateOutboxEntry(UpdateOutboxEntryRequest) returns (UpdateOutboxEntryResponse)
```

### SaveLedgerAndRegistry

Atomic single-transaction commit of `transaction_ledger`, `position_registry`, and (when
supplied) the position handle mapping. Replaces the legacy `SaveLedgerEntry` →
`SavePositionEvent` sequence with a single atomic write so a gateway crash between rows
cannot orphan a registry handle or strand a phantom position (GH bug #2130).

**Wire `mode` field** (`SaveLedgerAndRegistryRequest.mode`):

| Wire value | Behavior |
|---|---|
| `""` (proto3 default) | Equivalent to `"commit"`. Backwards-compatible for clients that don't set the field. |
| `"commit"` | Full atomic three-write: ledger INSERT + registry UPSERT + handle backfill. |
| `"registry_reconciliation"` | Registry UPSERT + handle backfill only — **ledger is NOT touched**. Used exclusively by `PositionService.Reconcile` when `apply=true`. Writing a synthesized ledger row on this path would pollute the immutable intent history (reconciliation discovers chain-only positions with no corresponding intent). |

Any other value (including the framework-side `CommitMode` Literal values
`"accounting_only"` and `"registry"`) is rejected with `INVALID_ARGUMENT`. The Python
framework wrapper (`almanak/framework/accounting/commit.py:save_ledger_and_registry`)
exposes a higher-level `CommitMode = Literal["accounting_only", "registry",
"registry_reconciliation"]` API where `"accounting_only"` is routed through
`SaveLedgerEntry` instead and `"registry"` is translated to the wire's `"commit"`.

Ticket: VIB-4197 (local SQLite) / VIB-4205 / T19 (hosted Postgres) / VIB-4210 / T24
(reconciliation mode). See Blueprint 28 §4.

```protobuf
rpc SaveLedgerAndRegistry(SaveLedgerAndRegistryRequest) returns (SaveLedgerAndRegistryResponse)
```

### UpsertMigrationState

Insert or update the `migration_state` row for a `(deployment_id, primitive, cutover_key)`
tuple. Drives the registry-mode cutover boot guard (Blueprint 06 §"Migration State Table").
Part of T22 / VIB-4208 (SQLite half).

```protobuf
rpc UpsertMigrationState(UpsertMigrationStateRequest) returns (UpsertMigrationStateResponse)
```

### GetMigrationState

Read the current `migration_state` row for a `(deployment_id, primitive, cutover_key)`.
Returns `null` when no row exists — interpreted as "cutover not yet deployed for this
surface" (raises `RegistryCutoverNotDeployedError` at boot if `mode='registry'` write is
attempted).

```protobuf
rpc GetMigrationState(GetMigrationStateRequest) returns (GetMigrationStateResponse)
```

### UpdateMigrationState

Partial-update of a `migration_state` row (e.g. recording backfill progress, watermarks).
Distinct from `MarkBackfillComplete` which is the terminal one-shot flip.

```protobuf
rpc UpdateMigrationState(UpdateMigrationStateRequest) returns (UpdateMigrationStateResponse)
```

### MarkBackfillComplete

One-shot atomic flip of the `migration_state.complete` flag from `0` to `1`. Gates the
boot guard against `RegistryBackfillIncompleteError`: until this RPC fires for a surface,
registry-mode writes are refused in all execution modes (live, paper, dry_run).

```protobuf
rpc MarkBackfillComplete(MarkBackfillCompleteRequest) returns (MarkBackfillCompleteResponse)
```

### GetPositionEventsFiltered

Read `position_events` rows filtered by `(deployment_id, primitive, opened_after_block,
status, …)`. Used by the cutover backfill job to project historical position lifecycle
into `position_registry`. Distinct from `GetPositionHistory` which targets a single
position lifecycle.

```protobuf
rpc GetPositionEventsFiltered(GetPositionEventsFilteredRequest) returns (GetPositionEventsFilteredResponse)
```

### GetPositionRegistryOpenRows

Enumerate currently-open rows in `position_registry` for a `(deployment_id, chain,
primitive)` tuple. Used by `PositionService.Reconcile` to compute the diff vs on-chain
truth, and by the cutover backfill job to detect duplicates before insert.

```protobuf
rpc GetPositionRegistryOpenRows(GetPositionRegistryOpenRowsRequest) returns (GetPositionRegistryOpenRowsResponse)
```

## ExecutionService

### CompileIntent

Compile a strategy intent into an action bundle.

```protobuf
rpc CompileIntent(CompileIntentRequest) returns (CompilationResult)
```

**Request:**
```protobuf
message CompileIntentRequest {
  string intent_type = 1;          // Case-insensitive with aliases: "swap", "lp_open", etc.
  bytes intent_data = 2;           // JSON-serialized intent
  string chain = 3;
  string wallet_address = 4;
  map<string, string> price_map = 5;  // Token symbol -> USD price string (empty = use placeholder prices)
}
```

**Response:**
```protobuf
message CompilationResult {
  bool success = 1;
  bytes action_bundle = 2;     // JSON-serialized ActionBundle
  string error = 3;
  string error_code = 4;       // Structured error code
}
```

### Execute

Execute an action bundle (sign, submit, confirm).

```protobuf
rpc Execute(ExecuteRequest) returns (ExecutionResult)
```

### GetTransactionStatus

Get the status of a submitted transaction.

```protobuf
rpc GetTransactionStatus(TxStatusRequest) returns (TxStatus)
```

## ObserveService

### Log

Send log entries to the platform.

```protobuf
rpc Log(LogEntry) returns (Empty)
```

### Alert

Send an alert to configured channels (Slack, Telegram).

```protobuf
rpc Alert(AlertRequest) returns (AlertResponse)
```

**Request:**
```protobuf
message AlertRequest {
  string severity = 1;     // info, warning, error, critical
  string title = 2;
  string message = 3;
  string deployment_id = 4;
}
```

### RecordMetric

Record a custom metric.

```protobuf
rpc RecordMetric(MetricEntry) returns (Empty)
```

### RecordTimelineEvent

Record a timeline event for a strategy (trades, rebalances, errors, state changes).

```protobuf
rpc RecordTimelineEvent(RecordTimelineEventRequest) returns (RecordTimelineEventResponse)
```

**Request:**
```protobuf
message RecordTimelineEventRequest {
  string deployment_id = 1;
  string event_type = 2;       // "TRADE", "REBALANCE", "ERROR", "STATE_CHANGE", etc.
  string description = 3;
  string tx_hash = 4;          // Optional: transaction hash
  string chain = 5;            // Optional: chain name
  string details_json = 6;     // Optional: JSON-encoded details
  int64 timestamp = 7;         // Optional: uses server time if 0
}
```

**Response:**
```protobuf
message RecordTimelineEventResponse {
  bool success = 1;
  string event_id = 2;
  string error = 3;
}
```

## RpcService

### Call

Make a single JSON-RPC call to a blockchain.

```protobuf
rpc Call(RpcRequest) returns (RpcResponse)
```

**Request:**
```protobuf
message RpcRequest {
  string chain = 1;        // Must be in allowed list
  string method = 2;       // Must be in allowed list
  string params = 3;       // JSON-encoded params
  string id = 4;
}
```

**Response:**
```protobuf
message RpcResponse {
  bool success = 1;
  string result = 2;       // JSON-encoded result
  string error = 3;        // JSON-encoded error
  string id = 4;
}
```

**Allowed Chains:**

EVM chains:

- ethereum, arbitrum, base, optimism, polygon, avalanche, bsc, bnb, sonic, plasma, linea, blast, mantle, berachain, monad, xlayer, zerog

Non-EVM chains:

- solana

**Allowed Methods (EVM):**
- `eth_call`
- `eth_getBalance`
- `eth_getTransactionCount`
- `eth_getTransactionReceipt`
- `eth_getBlockByNumber`
- `eth_getBlockByHash`
- `eth_blockNumber`
- `eth_chainId`
- `eth_gasPrice`
- `eth_estimateGas`
- `eth_getLogs`
- `eth_getCode`
- `eth_getStorageAt`
- `eth_sendRawTransaction`
- `net_version`

**Allowed Methods (Solana):**
- `getBalance`
- `getTokenAccountsByOwner`
- `getTokenAccountBalance`
- `getTransaction`
- `getSignaturesForAddress`
- `getAccountInfo`
- `getMultipleAccounts`
- `getLatestBlockhash`
- `getSlot`
- `getBlockHeight`
- `getEpochInfo`
- `getMinimumBalanceForRentExemption`
- `sendTransaction`
- `simulateTransaction`
- `getRecentPrioritizationFees`
- `isBlockhashValid`

**Blocked Methods:**
- `debug_*` - Debugging methods
- `admin_*` - Admin methods
- `personal_*` - Personal key management
- `miner_*` - Mining control
- `txpool_*` - Transaction pool access

### BatchCall

Make multiple JSON-RPC calls in parallel.

```protobuf
rpc BatchCall(RpcBatchRequest) returns (RpcBatchResponse)
```

**Request:**
```protobuf
message RpcBatchRequest {
  string chain = 1;
  repeated RpcRequest requests = 2;  // Max 100
}
```

### QueryAllowance

Query ERC-20 token allowance (typed convenience method).

```protobuf
rpc QueryAllowance(AllowanceRequest) returns (AllowanceResponse)
```

**Solana:** Returns `allowance = MAX_UINT64` and `success = true`. SPL tokens don't use ERC-20-style allowances.

### QueryBalance

Query token balance (typed convenience method).

```protobuf
rpc QueryBalance(BalanceQueryRequest) returns (BalanceQueryResponse)
```

**Solana:** Returns an error directing callers to use `MarketService.GetBalance()` instead, which routes to the Solana-native balance provider.

### QueryPositionLiquidity

Query LP position liquidity (typed convenience method).

```protobuf
rpc QueryPositionLiquidity(PositionLiquidityRequest) returns (PositionLiquidityResponse)
```

**Solana:** Returns "not applicable for Solana". Solana LP positions use different on-chain structures.

### QueryPositionTokensOwed

Query tokens owed to an LP position (typed convenience method).

```protobuf
rpc QueryPositionTokensOwed(PositionTokensOwedRequest) returns (PositionTokensOwedResponse)
```

**Solana:** Returns "not applicable for Solana".

## IntegrationService

### BinanceGetTicker

Get 24-hour ticker data from Binance.

```protobuf
rpc BinanceGetTicker(BinanceTickerRequest) returns (BinanceTickerResponse)
```

**Request:**
```protobuf
message BinanceTickerRequest {
  string symbol = 1;       // e.g., "BTCUSDT"
}
```

**Response:**
```protobuf
message BinanceTickerResponse {
  string symbol = 1;
  string price = 2;
  string price_change = 3;
  string price_change_percent = 4;
  string high_24h = 5;
  string low_24h = 6;
  string volume_24h = 7;
  string quote_volume_24h = 8;
  int64 timestamp = 9;
}
```

### BinanceGetKlines

Get candlestick/kline data from Binance.

```protobuf
rpc BinanceGetKlines(BinanceKlinesRequest) returns (BinanceKlinesResponse)
```

**Request:**
```protobuf
message BinanceKlinesRequest {
  string symbol = 1;
  string interval = 2;     // 1m, 5m, 15m, 1h, 4h, 1d, etc.
  int32 limit = 3;         // Max 1000
  int64 start_time = 4;
  int64 end_time = 5;
}
```

### BinanceGetOrderBook

Get order book depth from Binance.

```protobuf
rpc BinanceGetOrderBook(BinanceOrderBookRequest) returns (BinanceOrderBookResponse)
```

### CoinGeckoGetPrice

Get token price from CoinGecko.

```protobuf
rpc CoinGeckoGetPrice(CoinGeckoGetPriceRequest) returns (CoinGeckoGetPriceResponse)
```

**Request:**
```protobuf
message CoinGeckoGetPriceRequest {
  string token_id = 1;     // e.g., "bitcoin", "ethereum"
  repeated string vs_currencies = 2;  // e.g., ["usd", "eur"]
}
```

### CoinGeckoGetPrices

Get prices for multiple tokens.

```protobuf
rpc CoinGeckoGetPrices(CoinGeckoGetPricesRequest) returns (CoinGeckoGetPricesResponse)
```

### CoinGeckoGetMarkets

Get market data with rankings.

```protobuf
rpc CoinGeckoGetMarkets(CoinGeckoGetMarketsRequest) returns (CoinGeckoGetMarketsResponse)
```

### TheGraphQuery

Execute a GraphQL query on a subgraph.

```protobuf
rpc TheGraphQuery(TheGraphQueryRequest) returns (TheGraphQueryResponse)
```

**Request:**
```protobuf
message TheGraphQueryRequest {
  string subgraph_id = 1;  // e.g., "uniswap-v3-arbitrum"
  string query = 2;        // GraphQL query (max 10KB)
  string variables = 3;    // JSON-encoded variables
}
```

**Note:** Introspection queries (`__schema`, `__type`) are blocked.

### CoinGeckoGetHistoricalPrice

Get historical price at a specific date.

```protobuf
rpc CoinGeckoGetHistoricalPrice(CoinGeckoHistoricalPriceRequest) returns (CoinGeckoHistoricalPriceResponse)
```

### CoinGeckoGetMarketChartRange

Get price chart data for a date range.

```protobuf
rpc CoinGeckoGetMarketChartRange(CoinGeckoMarketChartRangeRequest) returns (CoinGeckoMarketChartRangeResponse)
```

### GeckoTerminalGetOHLCV

Get DEX OHLCV data from GeckoTerminal for on-chain pool pricing.

```protobuf
rpc GeckoTerminalGetOHLCV(GeckoTerminalOHLCVRequest) returns (GeckoTerminalOHLCVResponse)
```

### GetWalletPortfolio

Get aggregated wallet portfolio valuation via Zerion.

```protobuf
rpc GetWalletPortfolio(WalletPortfolioRequest) returns (WalletPortfolioResponse)
```

### GetWalletPositions

Get detailed wallet positions (DeFi protocol positions) via Zerion.

```protobuf
rpc GetWalletPositions(WalletPortfolioRequest) returns (WalletPortfolioResponse)
```

## DashboardService

Provides data and actions for the operator dashboard.

### ListStrategies

List strategies with optional filters.

```protobuf
rpc ListStrategies(ListStrategiesRequest) returns (ListStrategiesResponse)
```

**Request:**
```protobuf
message ListStrategiesRequest {
  string status_filter = 1;
  string chain_filter = 2;
  bool include_position = 3;
}
```

### GetStrategyDetails

Get detailed information about a strategy including timeline and PnL history.

```protobuf
rpc GetStrategyDetails(GetStrategyDetailsRequest) returns (StrategyDetails)
```

**Request:**
```protobuf
message GetStrategyDetailsRequest {
  string deployment_id = 1;
  bool include_timeline = 2;
  bool include_pnl_history = 3;
  int32 timeline_limit = 4;
}
```

### GetTimeline

Get timeline events for a strategy.

```protobuf
rpc GetTimeline(GetTimelineRequest) returns (GetTimelineResponse)
```

### GetStrategyConfig

Get strategy configuration.

```protobuf
rpc GetStrategyConfig(GetStrategyConfigRequest) returns (StrategyConfigResponse)
```

### GetStrategyState

Get strategy state.

```protobuf
rpc GetStrategyState(GetStrategyStateRequest) returns (StrategyStateResponse)
```

### ExecuteAction

Execute an operator action on a strategy.

```protobuf
rpc ExecuteAction(ExecuteActionRequest) returns (ExecuteActionResponse)
```

**Request:**
```protobuf
message ExecuteActionRequest {
  string deployment_id = 1;
  string action = 2;       // PAUSE, RESUME, BUMP_GAS, CANCEL_TX, EMERGENCY_UNWIND
  string reason = 3;
  map<string, string> params = 4;
}
```

### RegisterStrategyInstance

Register a new strategy instance in the persistent registry.

```protobuf
rpc RegisterStrategyInstance(RegisterInstanceRequest) returns (RegisterInstanceResponse)
```

### UpdateStrategyInstanceStatus

Update the status of a registered strategy instance.

```protobuf
rpc UpdateStrategyInstanceStatus(UpdateInstanceStatusRequest) returns (UpdateInstanceStatusResponse)
```

### ArchiveStrategyInstance

Archive a strategy instance (soft delete).

```protobuf
rpc ArchiveStrategyInstance(ArchiveInstanceRequest) returns (ArchiveInstanceResponse)
```

### PurgeStrategyInstance

Permanently remove a strategy instance from the registry.

```protobuf
rpc PurgeStrategyInstance(PurgeInstanceRequest) returns (PurgeInstanceResponse)
```

### GetTransactionLedger

Retrieve the transaction ledger for a strategy instance.

```protobuf
rpc GetTransactionLedger(GetTransactionLedgerRequest) returns (GetTransactionLedgerResponse)
```

### GetPnLSummary

Aggregated PnL summary (realized, unrealized, lifetime) for the operator
dashboard. Sourced from `portfolio_metrics` and `accounting_events`.

```protobuf
rpc GetPnLSummary(GetPnLSummaryRequest) returns (PnLSummary)
```

### GetCostStack

Per-strategy cost decomposition (gas, slippage, protocol fees, MEV).

```protobuf
rpc GetCostStack(GetCostStackRequest) returns (CostStackInfo)
```

### GetAuditPosture

Audit-readiness snapshot: lot-policy version coverage, missing receipts,
outbox lag, and reconciliation drift signals.

```protobuf
rpc GetAuditPosture(GetAuditPostureRequest) returns (AuditPosture)
```

### GetTradeTape

Time-ordered tape of executed trades for the dashboard timeline view.

```protobuf
rpc GetTradeTape(GetTradeTapeRequest) returns (GetTradeTapeResponse)
```

### GetActivityFeed

Time-ordered feed of strategy lifecycle events (intent emitted, compiled, executed,
teardown, alerts) for the dashboard activity view. Distinct from `GetTradeTape` which
shows only trade fills.

```protobuf
rpc GetActivityFeed(GetActivityFeedRequest) returns (GetActivityFeedResponse)
```

### GetPositions

Registry-authoritative position identity (`position_registry`) joined with snapshot-authoritative valuation (`portfolio_snapshots` + `position_state_snapshots`). Replaces SQLite-direct reads in `framework/dashboard/pages/detail.py`. Carries `cutover_state` per `accounting_category` so renderers can split authoritative positions from pre-cutover "Unverified Holdings". (VIB-4493)

```protobuf
rpc GetPositions(GetPositionsRequest) returns (GetPositionsResponse)
```

### GetPositionRangeHistory

Per-position range / fee / balance history. Source-routes by primitive: LP/PERP from `position_events`, lending from `accounting_events`. Swap/prediction return empty + `stub_message` (history concept N/A). (VIB-4493)

```protobuf
rpc GetPositionRangeHistory(GetPositionRangeHistoryRequest) returns (GetPositionRangeHistoryResponse)
```

### GetReconciliationReport

Three-way diff across `transaction_ledger` / `portfolio_snapshots` / `position_registry`. Read-only. LP-only in v1; non-LP primitives surface per-primitive stubs (pending VIB-4202/4209/4501). 5s TTL cache. (VIB-4493)

```protobuf
rpc GetReconciliationReport(GetReconciliationReportRequest) returns (GetReconciliationReportResponse)
```

### PreviewReconcile

Dry-run reconciliation. Thin wrapper over `PositionService.Reconcile(apply=false)`. Returns a `preview_token` bound to current registry/ledger state hashes — pass to `ApplyReconcile` to apply. Operator-only; requires the `x-operator-token` second-factor header when `ALMANAK_GATEWAY_OPERATOR_TOKEN` is set. (VIB-4493)

```protobuf
rpc PreviewReconcile(PreviewReconcileRequest) returns (PreviewReconcileResponse)
```

### ApplyReconcile

Applies a previously-issued preview. Fails with `STATE_DRIFT` if registry/ledger state changed since the preview was issued. Operator-only; requires the `x-operator-token` second-factor header when `ALMANAK_GATEWAY_OPERATOR_TOKEN` is set. (VIB-4493)

```protobuf
rpc ApplyReconcile(ApplyReconcileRequest) returns (ApplyReconcileResponse)
```

### RefreshRegistryFromChain

Forces fresh on-chain reads for every position in `position_registry` for the strategy. Updates `on_chain_verified_at`, re-emits divergent events. Rate-limited at the DashboardService layer to one in-flight per strategy. Operator-only; requires the `x-operator-token` second-factor header when `ALMANAK_GATEWAY_OPERATOR_TOKEN` is set. (VIB-4493)

```protobuf
rpc RefreshRegistryFromChain(RefreshRegistryFromChainRequest) returns (RefreshRegistryFromChainResponse)
```

## FundingRateService

Provides perpetual funding rate data from venues like GMX V2 and Hyperliquid.

### GetFundingRate

Get current funding rate for a market on a specific venue.

```protobuf
rpc GetFundingRate(FundingRateRequest) returns (FundingRateResponse)
```

**Request:**
```protobuf
message FundingRateRequest {
  string venue = 1;        // gmx_v2, hyperliquid
  string market = 2;       // e.g., ETH-USD, BTC-USD
  string chain = 3;
}
```

**Response:**
```protobuf
message FundingRateResponse {
  string venue = 1;
  string market = 2;
  string rate_hourly = 3;
  string rate_8h = 4;
  string rate_annualized = 5;
  int64 next_funding_time = 6;
  string open_interest_long = 7;
  string open_interest_short = 8;
  string mark_price = 9;
  string index_price = 10;
  bool is_live_data = 11;
  bool success = 12;
  string error = 13;
}
```

### GetFundingRateSpread

Get the funding rate spread between two venues.

```protobuf
rpc GetFundingRateSpread(FundingRateSpreadRequest) returns (FundingRateSpreadResponse)
```

**Request:**
```protobuf
message FundingRateSpreadRequest {
  string market = 1;
  string venue_a = 2;
  string venue_b = 3;
  string chain = 4;
}
```

## SimulationService

Simulate transaction bundles before execution using Tenderly or Alchemy.

### SimulateBundle

```protobuf
rpc SimulateBundle(SimulateBundleRequest) returns (SimulateBundleResponse)
```

**Request:**
```protobuf
message SimulateBundleRequest {
  string chain = 1;
  repeated SimulateTransaction transactions = 2;
  repeated SimulateStateOverride state_overrides = 3;
  string simulator = 4;   // "tenderly", "alchemy", or empty for auto-select
}
```

**Response:**
```protobuf
message SimulateBundleResponse {
  bool success = 1;
  bool simulated = 2;
  repeated int64 gas_estimates = 3;
  string revert_reason = 4;
  repeated string warnings = 5;
  string simulation_url = 6;
  string simulator_used = 7;
  string error = 8;
}
```

## PoolAnalyticsService

Aggregated DEX pool analytics (TVL, volume, fees) sourced through the gateway's analytics providers. Used by risk-adjusted strategies that gate position size on pool depth or 24h turnover.

### GetPoolAnalytics

```protobuf
rpc GetPoolAnalytics(PoolAnalyticsRequest) returns (PoolAnalyticsResponse)
```

## PoolHistoryService

Historical pool snapshots — TVL, volume, fee revenue, and per-token reserves
over time at 1h / 4h / 1d resolution.

The service is **feature-flagged off by default** (`pool_history_enabled=False`
in `GatewaySettings`). Set `ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true` on the
gateway to enable. When the flag is off the handler returns `UNAVAILABLE` with
a message pointing at VIB-4728; when the flag is on but providers are not yet
wired (POOL-2 → POOL-5 window) it returns `UNIMPLEMENTED`.

### GetPoolHistory

Return a time-series of `PoolSnapshot` rows for a single pool. Provider fallback
order is dispatcher policy (VIB-4753 / POOL-5):

- **1h / 4h:** TheGraph → GeckoTerminal (DefiLlama is skipped — daily-only).
- **1d:** TheGraph → DefiLlama → GeckoTerminal.

Returns `UNAVAILABLE` when all eligible providers fail or when the pool is not
found anywhere — **never** fake-success with an empty `snapshots` array.

```protobuf
rpc GetPoolHistory(PoolHistoryRequest) returns (PoolHistoryResponse)
```

**Request:**
```protobuf
message PoolHistoryRequest {
  string pool_address = 1;       // EVM: case-insensitive; Solana: case-sensitive base58
  string chain = 2;              // e.g. "arbitrum", "ethereum", "base"
  string protocol = 3;           // e.g. "uniswap_v3", "aerodrome"
  int64 start_ts = 4;            // Unix seconds, UTC
  int64 end_ts = 5;              // Unix seconds, UTC (rejected if in the future)
  Resolution resolution = 6;     // RESOLUTION_1H | RESOLUTION_4H | RESOLUTION_1D
}
```

**Response:**
```protobuf
message PoolHistoryResponse {
  repeated PoolSnapshot snapshots = 1;     // Ascending by timestamp, one row per aligned bucket
  TruncationReason truncation_reason = 2;  // CAP_EXCEEDED | PROVIDER_PAGE_CAP | PROVIDER_RETENTION
  int64 next_start_ts = 3;                 // Re-chunk hint for paged backfill
  string source = 4;                       // "the_graph" | "defillama" | "geckoterminal"
  bool finalized_only = 5;                 // false if any row is provisional within the finality cutoff
  bool success = 6;
  string error = 7;
}
```

**Soft caps** are operator-tunable via `ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_{1H,4H,1D}`
(defaults: 90, 180, 730 days). When a request exceeds a cap the handler returns
a `CAP_EXCEEDED` truncation and a `next_start_ts` hint for the next chunk.

## PolymarketService

Proxy for the Polymarket CLOB API. Provides market data, order management, and position tracking.

### GetMarket

Fetch metadata for a single Polymarket market (tokens, condition id, status).

```protobuf
rpc GetMarket(PolymarketGetMarketRequest) returns (PolymarketMarketResponse)
```

### GetMarkets

List markets with optional filters (category, status, cursor pagination).

```protobuf
rpc GetMarkets(PolymarketGetMarketsRequest) returns (PolymarketMarketsResponse)
```

### GetMidpoint

Return the current midpoint price for a token (CLOB outcome).

```protobuf
rpc GetMidpoint(PolymarketMidpointRequest) returns (PolymarketMidpointResponse)
```

### GetSimplifiedMarkets

List condensed market summaries optimized for discovery flows.

```protobuf
rpc GetSimplifiedMarkets(PolymarketGetSimplifiedMarketsRequest) returns (PolymarketSimplifiedMarketsResponse)
```

### GetOrderBook

Return the full bid / ask book for a token.

```protobuf
rpc GetOrderBook(PolymarketOrderBookRequest) returns (PolymarketOrderBookResponse)
```

### GetTickSize

Return the minimum price increment (tick size) accepted for a token.

```protobuf
rpc GetTickSize(PolymarketTickSizeRequest) returns (PolymarketTickSizeResponse)
```

### GetSpread

Return the current bid-ask spread for a token.

```protobuf
rpc GetSpread(PolymarketSpreadRequest) returns (PolymarketSpreadResponse)
```

### GetPriceHistory

Return time-bucketed historical prices for a token (used for backtesting and charts).

```protobuf
rpc GetPriceHistory(PolymarketGetPriceHistoryRequest) returns (PolymarketPriceHistoryResponse)
```

### GetTradeTape

Return a time-ordered trade tape across markets (most-recent first).

```protobuf
rpc GetTradeTape(PolymarketGetTradeTapeRequest) returns (PolymarketTradeTapeResponse)
```

### GetBalanceAllowance

Return the funder's USDC balance and CLOB exchange allowance.

```protobuf
rpc GetBalanceAllowance(PolymarketBalanceAllowanceRequest) returns (PolymarketBalanceAllowanceResponse)
```

### GetOpenOrders

List open orders for the configured funder, optionally filtered by market.

```protobuf
rpc GetOpenOrders(PolymarketGetOpenOrdersRequest) returns (PolymarketOpenOrdersResponse)
```

### GetOrder

Return details for a single order by id.

```protobuf
rpc GetOrder(PolymarketGetOrderRequest) returns (PolymarketOrderInfoResponse)
```

### GetTradesHistory

List historical fills for the funder, with cursor pagination.

```protobuf
rpc GetTradesHistory(PolymarketGetTradesRequest) returns (PolymarketTradesResponse)
```

### CancelOrder

Cancel a single open order by id.

```protobuf
rpc CancelOrder(PolymarketCancelOrderRequest) returns (PolymarketCancelResponse)
```

### CancelOrders

Cancel multiple open orders in a single call.

```protobuf
rpc CancelOrders(PolymarketCancelOrdersRequest) returns (PolymarketCancelResponse)
```

### CancelAll

Cancel every open order for the funder.

```protobuf
rpc CancelAll(PolymarketCancelAllRequest) returns (PolymarketCancelResponse)
```

### CreateAndPostOrder

Sign a limit order against the funder's key and POST it to the CLOB in a single
RPC. Returns the order id and CLOB acknowledgement payload.

```protobuf
rpc CreateAndPostOrder(PolymarketCreateOrderRequest) returns (PolymarketOrderResponse)
```

### CreateAndPostMarketOrder

Sign and submit a market order in a single RPC (fills against the resting book).

```protobuf
rpc CreateAndPostMarketOrder(PolymarketMarketOrderRequest) returns (PolymarketOrderResponse)
```

## EnsoService

Proxy for Enso Finance routing and bundling, supporting cross-chain swaps.

### GetRoute

Get an optimized swap route.

```protobuf
rpc GetRoute(EnsoRouteRequest) returns (EnsoRouteResponse)
```

**Request:**
```protobuf
message EnsoRouteRequest {
  string chain = 1;
  string token_in = 2;
  string token_out = 3;
  string amount_in = 4;
  string from_address = 5;
  string receiver = 6;
  int32 slippage_bps = 7;
  string routing_strategy = 8;
  int32 max_price_impact_bps = 9;
  int32 destination_chain_id = 10;
  string refund_receiver = 11;
}
```

### GetQuote

Get a price quote without generating calldata.

```protobuf
rpc GetQuote(EnsoQuoteRequest) returns (EnsoQuoteResponse)
```

### GetApproval

Get the approval transaction for a token.

```protobuf
rpc GetApproval(EnsoApprovalRequest) returns (EnsoApprovalResponse)
```

### GetBundle

Bundle multiple DeFi actions into a single transaction.

```protobuf
rpc GetBundle(EnsoBundleRequest) returns (EnsoBundleResponse)
```

## TokenService

Provides token resolution and on-chain metadata lookups.

### ResolveToken

Resolve a token by symbol or address to get its full metadata.

```protobuf
rpc ResolveToken(ResolveTokenRequest) returns (TokenMetadataResponse)
```

### GetTokenMetadata

Get metadata for a token by address.

```protobuf
rpc GetTokenMetadata(GetTokenMetadataRequest) returns (TokenMetadataResponse)
```

### GetTokenDecimals

Get the decimal precision for a token.

```protobuf
rpc GetTokenDecimals(GetTokenDecimalsRequest) returns (GetTokenDecimalsResponse)
```

### BatchResolveTokens

Resolve multiple tokens in a single call.

```protobuf
rpc BatchResolveTokens(BatchResolveTokensRequest) returns (BatchResolveTokensResponse)
```

## LifecycleService

Agent state management and command dispatch for V2 deployments.

### WriteState

Write the current agent state (INITIALIZING, RUNNING, PAUSED, ERROR, STOPPING, TERMINATED).

```protobuf
rpc WriteState(WriteAgentStateRequest) returns (WriteAgentStateResponse)
```

### ReadState

Read the current agent state.

```protobuf
rpc ReadState(ReadAgentStateRequest) returns (ReadAgentStateResponse)
```

### Heartbeat

Send a heartbeat to update the last activity timestamp and increment the iteration count.

```protobuf
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)
```

### ReadCommand

Read the most recent unprocessed command for an agent (PAUSE, RESUME, STOP).

```protobuf
rpc ReadCommand(ReadAgentCommandRequest) returns (ReadAgentCommandResponse)
```

### AckCommand

Acknowledge (mark processed) a command.

```protobuf
rpc AckCommand(AckAgentCommandRequest) returns (AckAgentCommandResponse)
```

### WriteCommand

Write a command to an agent.

```protobuf
rpc WriteCommand(WriteAgentCommandRequest) returns (WriteAgentCommandResponse)
```

## TeardownService

Hosted teardown state routing for V2 deployments. Splits into two halves: the **request half** (`teardown_requests`) which tracks operator-issued teardown signals through their lifecycle (acknowledged → started → progress → completed / failed / cancelled), and the **adapter half** (`teardown_execution_state` + `teardown_approvals`) which persists the in-flight intent state machine and operator approvals required for risk-elevated teardown steps. See `blueprints/14-teardown-system.md`.

### CreateTeardownRequest

Insert a new operator-issued teardown request. This is the row that
`almanak strat teardown request -s <deployment_id>` writes; the runner's polling
query picks it up by `deployment_id`.

```protobuf
rpc CreateTeardownRequest(CreateTeardownRequestRequest) returns (CreateTeardownRequestResponse)
```

### GetTeardownRequest

Fetch a single teardown request by id.

```protobuf
rpc GetTeardownRequest(GetTeardownRequestRequest) returns (GetTeardownRequestResponse)
```

### GetActiveTeardownRequest

Fetch the active (non-terminal) teardown request for a strategy, if any.

```protobuf
rpc GetActiveTeardownRequest(GetActiveTeardownRequestRequest) returns (GetTeardownRequestResponse)
```

### GetPendingTeardownRequests

List teardown requests not yet acknowledged by any runner (used by the runner
polling loop and the operator dashboard's pending queue).

```protobuf
rpc GetPendingTeardownRequests(Empty) returns (ListTeardownRequestsResponse)
```

### GetAllActiveTeardownRequests

List every non-terminal teardown request across all strategies (operator dashboard).

```protobuf
rpc GetAllActiveTeardownRequests(Empty) returns (ListTeardownRequestsResponse)
```

### GetAllTeardownRequests

List every teardown request (administrative / audit endpoint).

```protobuf
rpc GetAllTeardownRequests(Empty) returns (ListTeardownRequestsResponse)
```

### UpdateTeardownRequest

Generic update of a teardown request (mode, slippage tolerance, operator note).

```protobuf
rpc UpdateTeardownRequest(UpdateTeardownRequestRequest) returns (TeardownRequestMutationResponse)
```

### AcknowledgeTeardownRequest

Mark a request as acknowledged by the runner that owns the strategy. Until this
fires, the request is in `pending`.

```protobuf
rpc AcknowledgeTeardownRequest(AckTeardownRequestRequest) returns (TeardownRequestMutationResponse)
```

### MarkTeardownStarted

Transition the request to `started` once the teardown manager begins executing
intents (this is the state `almanak strat teardown request --wait` reports).

```protobuf
rpc MarkTeardownStarted(MarkTeardownStartedRequest) returns (TeardownRequestMutationResponse)
```

### UpdateTeardownProgress

Stream per-step progress (current intent index, status, last tx_hash). Read by
`teardown request --wait` to print the live progress feed.

```protobuf
rpc UpdateTeardownProgress(UpdateTeardownProgressRequest) returns (TeardownRequestMutationResponse)
```

### MarkTeardownCompleted

Terminal success state — every teardown intent completed and the strategy is
fully unwound.

```protobuf
rpc MarkTeardownCompleted(MarkTeardownCompletedRequest) returns (TeardownRequestMutationResponse)
```

### MarkTeardownFailed

Terminal failure state with an error code + message. The on-chain state may be
partially unwound; check the latest `transaction_ledger` rows for the strategy.

```protobuf
rpc MarkTeardownFailed(MarkTeardownFailedRequest) returns (TeardownRequestMutationResponse)
```

### RequestTeardownCancel

Operator-issued cancellation signal (cooperative — the runner stops at the next
safe checkpoint).

```protobuf
rpc RequestTeardownCancel(RequestTeardownCancelRequest) returns (BoolMutationResponse)
```

### MarkTeardownCancelled

Terminal cancellation state — the runner halted teardown at a safe checkpoint.

```protobuf
rpc MarkTeardownCancelled(MarkTeardownCancelledRequest) returns (TeardownRequestMutationResponse)
```

### DeleteTeardownRequest

Hard-delete a request row (administrative endpoint).

```protobuf
rpc DeleteTeardownRequest(DeleteTeardownRequestRequest) returns (BoolMutationResponse)
```

### SaveTeardownState

Persist a snapshot of the teardown intent state machine. Used by the runner to
survive restarts mid-teardown.

```protobuf
rpc SaveTeardownState(SaveTeardownStateRequest) returns (SaveTeardownStateResponse)
```

### LoadTeardownState

Restore the teardown state machine on runner restart.

```protobuf
rpc LoadTeardownState(LoadTeardownStateRequest) returns (LoadTeardownStateResponse)
```

### DeleteTeardownState

Clear persisted teardown state after a terminal transition.

```protobuf
rpc DeleteTeardownState(DeleteTeardownStateRequest) returns (DeleteTeardownStateResponse)
```

### CreateApprovalRequest

Block teardown on operator approval for risk-elevated steps (e.g., HARD-mode
slippage bumps). The runner waits on a matching `GetApprovalResponse` /
`WriteApprovalResponse` pair.

```protobuf
rpc CreateApprovalRequest(CreateApprovalRequestRequest) returns (CreateApprovalRequestResponse)
```

### GetApprovalResponse

Poll for an operator's response to an approval request by approval id.

```protobuf
rpc GetApprovalResponse(GetApprovalResponseRequest) returns (GetApprovalResponseResponse)
```

### WriteApprovalResponse

Operator writes an approve / deny decision against an approval id.

```protobuf
rpc WriteApprovalResponse(WriteApprovalResponseRequest) returns (BoolMutationResponse)
```

### GetLatestPendingApproval

Fetch the most recent pending approval for a strategy (operator UI lookup when
approval id is not known).

```protobuf
rpc GetLatestPendingApproval(GetLatestPendingApprovalRequest) returns (GetLatestPendingApprovalResponse)
```

### WriteApprovalResponseByStrategy

Approve / deny by strategy id (used when the operator UI does not yet have the
approval id but has the strategy context).

```protobuf
rpc WriteApprovalResponseByStrategy(WriteApprovalResponseByStrategyRequest) returns (BoolMutationResponse)
```

## PositionService

Control-plane reconciliation of `position_registry` against on-chain truth. v1 scope is
UniV3 LP only (T24 / VIB-4210). Backs the `almanak ax positions reconcile` operator CLI.
Closes user-facing bug GH #2131 (phantom-missing rows after partial-write outages).

### Reconcile

Re-derive registry rows for a deployment by querying chain state and diffing against the
current `position_registry` contents. Reports four diff categories:

- `matched` — on-chain and registry agree.
- `phantom_missing` — on-chain has a position, registry doesn't (the GH #2131 case).
- `stranded` — registry `status='open'`, chain absent. **Never auto-closed** — operator
  must run a teardown for the position.
- `rebuilt` — phantom-missing rows just written (only when request `apply=true`).

When `apply=false` (default), the RPC is dry-run and returns the diff without writing.
When `apply=true`, phantom-missing rows are inserted into `position_registry` via
`SaveLedgerAndRegistry(mode='registry_reconciliation')` (registry-only write; ledger is
NEVER touched on the reconciliation path).

```protobuf
rpc Reconcile(ReconcileRequest) returns (ReconcileResponse)
```

**Request:**
```protobuf
message ReconcileRequest {
  string deployment_id = 1;             // ClassName:hash
  string chain = 2;
  string wallet_address = 3;
  repeated string primitives = 4;        // v1: "lp" only
  repeated string physical_identity_hashes = 5;  // optional filter
  bool apply = 6;                        // default false (dry-run)
  int64 max_age_blocks = 7;              // 0 = no check; v1: rejected if > 0 on first-page requests (no page_cursor)
  bytes page_cursor = 8;                 // Opaque pagination cursor (v1: honored only for stale-cursor validation; single-page contract)
  int32 page_size = 9;                   // Max rows per page (default 64, cap 256; v1: clamped but does not slice)
  string operator_note = 10;             // ≤ 256 bytes
  string trigger = 11;                   // operator_cli | hosted_boot | dashboard | ci
}
```

## Error Codes

| gRPC Code | HTTP | Description |
|-----------|------|-------------|
| INVALID_ARGUMENT | 400 | Invalid request parameters |
| NOT_FOUND | 404 | Resource not found |
| PERMISSION_DENIED | 403 | Operation not allowed |
| RESOURCE_EXHAUSTED | 429 | Rate limit exceeded |
| FAILED_PRECONDITION | 412 | Chain not configured |
| INTERNAL | 500 | Internal server error |
| UNAVAILABLE | 503 | Service temporarily unavailable |

## Rate Limits

| Service | Limit |
|---------|-------|
| RpcService (EVM chains) | 300 req/min per chain |
| RpcService (Solana) | 100 req/min |
| IntegrationService.Binance | 1200 req/min |
| IntegrationService.CoinGecko | 50 req/min (free tier) |
| IntegrationService.TheGraph | 100 req/min |
