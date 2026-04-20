# PancakeSwap Perps

Connector for PancakeSwap Perps (ApolloX Diamond) on BSC. PancakeSwap is broker
id 2 on the underlying ApolloX (ASX) perpetual trading platform.

## Overview

PancakeSwap Perps is an oracle-priced margin trading venue. The Almanak SDK
integrates it through the intent system, supporting `PERP_OPEN` (full pipeline)
and `PERP_CLOSE` (direct-SDK in v1).

The router is a Diamond proxy (EIP-2535) at
`0x1b6f2d3844c6ae7d56ceb3c3643b9060ba28feb0`. Trade-path facets: TradingPortal
(open/close), TradingOpen / TradingClose (keeper settle), TradingReader
(views), PriceFacade (keeper entry).

## Market Format

Markets use the same slash separator convention as GMX V2: `"BTC/USD"`,
`"ETH/USD"`, `"BNB/USD"`. Each market resolves to a `pairBase` BSC ERC-20
address (e.g. BTCB, ETH-bsc, WBNB).

```python
Intent.perp_open(
    market="BTC/USD",
    collateral_token="BNB",          # native BNB margin (auto-wraps to WBNB)
    collateral_amount=Decimal("0.3"),
    size_usd=Decimal("500"),
    is_long=True,
    leverage=Decimal("1.5"),
    max_slippage=Decimal("0.01"),
    protocol="pancakeswap_perps",
)
```

## Supported Operations

| Intent | Description |
|--------|-------------|
| `Intent.perp_open()` | Open a leveraged long or short position |
| `Intent.perp_close(position_id=<tradeHash>, ...)` | Close a position by its bytes32 `tradeHash`. v1 accepts full closes only: `size_usd` (partial close) is rejected at compile time (`CompilationStatus.FAILED`). Strategies persist the `tradeHash` emitted at open and pass it back as `position_id`. `build_close_transaction(trade_hash)` remains available as a direct-SDK escape hatch, but the intent-compiler path is the recommended flow. |

## Keeper Execution Model

PancakeSwap Perps uses a **two-step oracle-fill execution model** (similar in
shape to GMX V2 but driven by Pyth on mainnet):

1. **Pending trade creation** — your transaction calls `openMarketTrade` /
   `openMarketTradeBNB`, which emits a `MarketPendingTrade(user, tradeHash, trade)`
   event. This is the transaction the SDK signs and submits.
2. **Keeper settlement** — an off-chain keeper holding `PRICE_FEEDER_ROLE`
   subsequently calls `PriceFacadeFacet.requestPriceCallback(priceRequestId, price)`
   (or its Pyth-VAA variant). This invokes `marketTradeCallback` internally,
   which either fills the position (`OpenMarketTrade` event) or refunds it
   (`PendingTradeRefund` event) based on slippage / oracle gap checks.

Close follows the same shape: `closeTrade(tradeHash)` emits a pending close
request; keeper settles via `closeTradeCallback`, emitting
`CloseTradeSuccessful(user, tradeHash, closeInfo)` and one or more
`CloseTradeReceived(user, tradeHash, token, amount)` payout events.

**Implications for strategies:**

- `on_intent_executed(success=True)` fires when the **pending** TX confirms,
  not when the keeper settles. The strategy must persist the `tradeHash` and
  poll for fill confirmation on subsequent ticks via
  `getPositionByHashV2(tradeHash)`.
  > **Extraction shape**: `ResultEnricher.position_id` only accepts integer
  > NFT IDs or 40-char hex addresses, so a 64-hex-char bytes32 tradeHash is
  > surfaced on `result.extracted_data["position_id"]` rather than
  > `result.position_id`. Strategies should prefer `result.position_id` and
  > fall back to `result.extracted_data["position_id"]`:
  > ```python
  > position_id = getattr(result, "position_id", None)
  > if position_id is None:
  >     position_id = (result.extracted_data or {}).get("position_id")
  > ```
- Filled `entry_price` is only available after keeper settlement (a separate
  TX), so `extract_entry_price()` returns `None` from the open TX's receipt.
- The slippage-to-limit-price conversion must produce a fill bound that the
  keeper considers acceptable AND is within `highPriceGapP` (1.5% as of v1)
  of the on-chain oracle's cached `beforePrice`.

## Minimum Position Size

ApolloX enforces `TradingConfig.minNotionalUsd` (200 USD as of v1) on every
open. Notional is computed as `price (1e8) × qty (1e10) ÷ 1e18`. Strategies
opening positions below this floor will revert at the synchronous open call
with `TradingCheckerFacet: Position is too small`.

## Collateral Tokens

| Chain | Supported Margin |
|-------|------------------|
| BSC | BNB (native, via `openMarketTradeBNB`), WBNB, USDT, USDC |

For native BNB margin, the SDK routes through `openMarketTradeBNB` with the
margin sent as `msg.value`; the router wraps it to WBNB inside the
transaction (verifiable in the receipt's WBNB `Deposit` event). For ERC-20
margin, the intent compiler prepends an `approve()` transaction.

## Markets (v1)

| Market | pairBase (BSC) |
|--------|---------------|
| BTC/USD | `0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c` (BTCB) |
| ETH/USD | `0x2170Ed0880ac9A755fd29B2688956BD959F933F8` (ETH-bsc) |
| BNB/USD | `0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c` (WBNB) |

ApolloX also lists synthetic equity markets (NVDA, TSLA, etc.) with
ApolloX-issued pairBase contracts. These are out of v1 scope pending
gateway-side equity-oracle support; see the design doc follow-ups.

## Known Limitations

- **Keeper delay**: Position state is not immediately available after the open
  call. Wait for the keeper-settled `OpenMarketTrade` event before relying on
  filled position data.
- **Refunds**: If the keeper-quoted oracle price violates the trader's
  acceptable price (slippage limit), the keeper emits `PendingTradeRefund`
  instead of `OpenMarketTrade`, and the trader's margin is returned. Your
  strategy must handle this case (poll `getPendingTrade` going to zero
  without a corresponding `getPositionByHashV2` populated).
- **No SL/TP in v1**: The intent vocabulary doesn't yet carry stop-loss /
  take-profit. The contract supports them; the integration is deferred to a
  cross-venue (PCS Perps + GMX V2 + Hyperliquid) vocabulary RFC.
- **No Arbitrum**: ApolloX has separate deployments per chain. v1 ships BSC
  only.
- **Full-close only via intents**: `PerpCloseIntent(position_id=<tradeHash>)`
  compiles to `closeTrade(bytes32)`. Partial closes (`size_usd` set) are
  rejected at compile time — ApolloX's `closeTrade(bytes32)` selector always
  flattens the whole position. Strategies persist the `tradeHash` emitted at
  open and pass it back as `position_id`. `build_close_transaction(trade_hash)`
  remains available for manual transaction construction.

## API Reference

::: almanak.framework.connectors.pancakeswap_perps
    options:
      show_root_heading: true
      members_order: source
