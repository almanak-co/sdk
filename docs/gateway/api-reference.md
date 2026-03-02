# Gateway API Reference

This document describes the gRPC API exposed by the Almanak Gateway.

## Services Overview

| Service | Methods | Description |
|---------|---------|-------------|
| Health | 3 | Standard gRPC health checks and chain registration |
| MarketService | 4 | Price data, balances, batch balances, and technical indicators |
| StateService | 3 | Strategy state persistence with optimistic locking |
| ExecutionService | 3 | Intent compilation and transaction execution |
| ObserveService | 4 | Logging, alerts, metrics, and timeline events |
| RpcService | 6 | JSON-RPC proxy to blockchains with typed queries |
| IntegrationService | 9 | Third-party data (Binance, CoinGecko, TheGraph) |
| DashboardService | 10 | Operator dashboard data and actions |
| FundingRateService | 2 | Perpetual funding rates and spreads |
| SimulationService | 1 | Transaction bundle simulation (Tenderly/Alchemy) |
| PolymarketService | 18 | Polymarket CLOB API proxy (market data, orders, positions) |
| EnsoService | 4 | Enso Finance routing and bundling |
| TokenService | 4 | Token resolution and on-chain metadata |

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
  string chain = 1;      // e.g., "arbitrum", "ethereum"
  string token = 2;      // Token symbol or address
  string quote = 3;      // Quote currency (default: "USD")
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
  string chain = 1;
  string wallet_address = 2;
  string token_address = 3;  // Optional, empty for native token
}
```

**Response:**
```protobuf
message BalanceResponse {
  string balance = 1;      // Balance as string (precision preserved)
  int32 decimals = 2;
  string symbol = 3;
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

## StateService

### LoadState

Load strategy state from storage.

```protobuf
rpc LoadState(LoadStateRequest) returns (StateData)
```

**Request:**
```protobuf
message LoadStateRequest {
  string strategy_id = 1;
  string key = 2;          // Optional key within state
}
```

**Response:**
```protobuf
message StateData {
  bytes data = 1;          // Serialized state data
  int64 version = 2;
  int64 timestamp = 3;
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
  string strategy_id = 1;
  bytes data = 2;          // Max 1MB
  string key = 3;          // Optional key
}
```

### DeleteState

Delete strategy state.

```protobuf
rpc DeleteState(DeleteStateRequest) returns (DeleteStateResponse)
```

## ExecutionService

### CompileIntent

Compile a strategy intent into executable transactions.

```protobuf
rpc CompileIntent(IntentRequest) returns (IntentResponse)
```

**Request:**
```protobuf
message IntentRequest {
  string chain = 1;
  string wallet_address = 2;
  string intent_json = 3;   // Serialized intent
}
```

### Execute

Execute a compiled intent.

```protobuf
rpc Execute(ExecuteRequest) returns (ExecutionResult)
```

**Request:**
```protobuf
message ExecuteRequest {
  string chain = 1;
  string wallet_address = 2;
  string transaction_json = 3;
  bool simulate = 4;        // Dry run without execution
}
```

**Response:**
```protobuf
message ExecutionResult {
  bool success = 1;
  string tx_hash = 2;
  string error = 3;
  string receipt_json = 4;
}
```

### GetTransactionStatus

Get the status of a submitted transaction.

```protobuf
rpc GetTransactionStatus(TxStatusRequest) returns (TxStatusResponse)
```

## ObserveService

### Log

Send log entries to the platform.

```protobuf
rpc Log(LogRequest) returns (LogResponse)
```

**Request:**
```protobuf
message LogRequest {
  string level = 1;        // debug, info, warning, error
  string message = 2;
  string strategy_id = 3;
  map<string, string> metadata = 4;
}
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
  string strategy_id = 4;
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
  string strategy_id = 1;
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
- ethereum
- arbitrum
- base
- optimism
- polygon
- avalanche
- bsc
- bnb
- sonic
- plasma
- linea
- blast
- mantle
- berachain

**Allowed Methods:**
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

### QueryBalance

Query token balance (typed convenience method).

```protobuf
rpc QueryBalance(BalanceQueryRequest) returns (BalanceQueryResponse)
```

### QueryPositionLiquidity

Query LP position liquidity (typed convenience method).

```protobuf
rpc QueryPositionLiquidity(PositionLiquidityRequest) returns (PositionLiquidityResponse)
```

### QueryPositionTokensOwed

Query tokens owed to an LP position (typed convenience method).

```protobuf
rpc QueryPositionTokensOwed(PositionTokensOwedRequest) returns (PositionTokensOwedResponse)
```

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
  string strategy_id = 1;
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
  string strategy_id = 1;
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

## PolymarketService

Proxy for the Polymarket CLOB API. Provides market data, order management, and position tracking.

### Market Data Methods

| Method | Description |
|--------|-------------|
| `GetMarket` | Get details for a single market |
| `GetMarkets` | Get multiple markets with filters |
| `GetSimplifiedMarkets` | Get simplified market summaries |
| `GetOrderBook` | Get order book for a token |
| `GetMidpoint` | Get midpoint price |
| `GetPrice` | Get current price |
| `GetSpread` | Get bid-ask spread |
| `GetTickSize` | Get minimum tick size |

### Order Management Methods

| Method | Description |
|--------|-------------|
| `CreateAndPostOrder` | Create and submit a limit order |
| `CreateAndPostMarketOrder` | Create and submit a market order |
| `CancelOrder` | Cancel a single order |
| `CancelOrders` | Cancel multiple orders |
| `CancelAll` | Cancel all open orders |

### Position and History Methods

| Method | Description |
|--------|-------------|
| `GetPositions` | Get current positions |
| `GetOpenOrders` | Get open orders |
| `GetTradesHistory` | Get trade history |
| `GetOrder` | Get order details |
| `GetBalanceAllowance` | Get balance and allowance |

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
| RpcService | 300 req/min per chain |
| IntegrationService.Binance | 1200 req/min |
| IntegrationService.CoinGecko | 50 req/min (free tier) |
| IntegrationService.TheGraph | 100 req/min |
