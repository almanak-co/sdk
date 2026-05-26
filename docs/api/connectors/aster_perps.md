# Aster Perps

Connector for Aster Perps (formerly ApolloX) on BSC. `aster_perps` is the
canonical protocol key post-rebrand; it uses **broker_id=0** (raw Aster, no
attribution). For PancakeSwap-branded access to the same underlying Diamond,
see [PancakeSwap Perps](pancakeswap_perps.md) which uses **broker_id=2**.

## Overview

Aster Perps is an oracle-priced perpetual trading venue. The Almanak SDK
integrates it through the intent system, supporting `PERP_OPEN` (full
pipeline) and `PERP_CLOSE` (intent-level compilation).

The router is an EIP-2535 Diamond proxy at
`0x1b6f2d3844c6ae7d56ceb3c3643b9060ba28feb0` on BSC — the same Diamond that
powers PancakeSwap Perps. Broker attribution is passed in the open-trade
payload; `aster_perps` sets it to 0, meaning the position is attributed to
raw Aster rather than any front-end partner.

Trade-path facets: TradingPortal (open/close), TradingOpen / TradingClose
(keeper settle), TradingReader (views), PriceFacade (keeper entry).

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
    protocol="aster_perps",
)
```

## Supported Operations

| Intent | Description |
|--------|-------------|
| `Intent.perp_open()` | Open a leveraged long or short position, attributed to broker_id=0 |
| `Intent.perp_close(position_id=<tradeHash>, ...)` | Close a position by its bytes32 `tradeHash`. v1 accepts full closes only: `size_usd` (partial close) is rejected at compile time (`CompilationStatus.FAILED`). Strategies persist the `tradeHash` emitted at open and pass it back as `position_id`. |

## Relationship to `pancakeswap_perps`

Both protocol keys target the **same on-chain Diamond** on BSC:

| Protocol key | Broker id | Use when |
|--------------|-----------|----------|
| `aster_perps` | 0 | Building a strategy that trades Aster directly (recommended for new strategies). |
| `pancakeswap_perps` | 2 | Building a strategy that specifically needs PancakeSwap front-end attribution, or maintaining compatibility with pre-rebrand strategies. The module is a thin shim that re-exports `aster_perps` with `broker_id=2` defaulted and emits a `DeprecationWarning` once per process. |

Both keys accept `PerpOpenIntent` / `PerpCloseIntent` and share the identical
event schema (`MarketPendingTrade`, `OpenMarketTrade`, `CloseTradeSuccessful`,
`CloseTradeReceived`, `PendingTradeRefund`). The only runtime difference is
the broker value encoded in the open-trade payload and reflected back in the
`MarketPendingTrade` event.

## Keeper Execution Model

Aster uses a **two-step oracle-fill execution model** (similar in shape to
GMX V2 but driven by keepers pushing price callbacks):

1. **Pending trade creation** — your transaction calls `openMarketTrade` /
   `openMarketTradeBNB`, which emits a `MarketPendingTrade(user, tradeHash, trade)`
   event. This is the transaction the SDK signs and submits.
2. **Keeper settlement** — an off-chain keeper holding `PRICE_FEEDER_ROLE`
   subsequently calls `PriceFacadeFacet.requestPriceCallback(priceRequestId, price)`.
   This invokes `marketTradeCallback` internally, which either fills the
   position (`OpenMarketTrade` event) or refunds it (`PendingTradeRefund`
   event) based on slippage / oracle-gap checks.

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

Aster enforces `TradingConfig.minNotionalUsd` (200 USD as of v1) on every
open. Notional is computed as `price (1e8) × qty (1e10) ÷ 1e18`. Strategies
opening positions below this floor will revert at the synchronous open call
with `TradingCheckerFacet: Position is too small`. This is enforced on-chain,
not in the SDK compiler — strategies must size their orders above the floor.

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

Aster also lists additional markets and synthetic equity markets (NVDA,
TSLA, etc.) with Aster-issued pairBase contracts. These are out of v1 scope
pending gateway-side equity-oracle support.

## Known Limitations

- **BSC only in v1**: Aster v2 is deployed on Arbitrum, Base, and opBNB with
  byte-identical function selectors and event topics (see RQ-1 research
  memo in `docs/internal/discussions/aster-dex-rq1-findings-20260418.md`).
  However, user-initiated trading volume on the non-BSC Diamonds is
  effectively zero as of this writing — Aster routes most non-BSC traffic
  through their off-chain Pro CLOB. Multi-chain expansion is deferred until
  on-chain volume on a non-BSC chain warrants adapter work.
- **Keeper delay**: Position state is not immediately available after the
  open call. Wait for the keeper-settled `OpenMarketTrade` event before
  relying on filled position data.
- **Refunds**: If the keeper-quoted oracle price violates the trader's
  acceptable price (slippage limit), the keeper emits `PendingTradeRefund`
  instead of `OpenMarketTrade`, and the trader's margin is returned. Your
  strategy must handle this case (poll `getPendingTrade` going to zero
  without a corresponding `getPositionByHashV2` populated).
- **No SL/TP in v1**: The intent vocabulary doesn't yet carry stop-loss /
  take-profit. The contract supports them; the integration is deferred to a
  cross-venue vocabulary RFC.
- **Full-close only via intents**: `PerpCloseIntent(position_id=<tradeHash>)`
  compiles to `closeTrade(bytes32)`. Partial closes (`size_usd` set) are
  rejected at compile time — Aster's `closeTrade(bytes32)` selector always
  flattens the whole position. `build_close_transaction(trade_hash)` remains
  available for manual transaction construction.

## Demo Strategy

A working open → close round-trip demo lives at
`almanak/demo_strategies/aster_perps_basic/`. It targets BNB/USD with 3x
leverage and reliably completes both legs on a BNB Anvil fork with keeper
impersonation.

## API Reference

::: almanak.connectors.aster_perps
    options:
      show_root_heading: true
      members_order: source
