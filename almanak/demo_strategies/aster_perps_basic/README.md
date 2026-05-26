# Aster Perps Basic Demo

Minimum-viable demo of the canonical `aster_perps` connector: tick 1 opens a
3x BNB/USD long with native BNB margin; tick 2 closes it. No signal, no
indicators — it's a connector lifecycle smoke test.

## Protocol attribution

Routes through `protocol="aster_perps"` — the **canonical, broker-agnostic**
path (broker id = 0, "raw Aster"). If you want PancakeSwap attribution instead
(broker id = 2), use the sibling `pancakeswap_perps_trend` demo, which exercises
the same connector via the `pancakeswap_perps` backwards-compatibility shim.

## Scope (Phase 1)

- **Chain:** BSC only. Multi-chain expansion (Arbitrum, opBNB, Base, Ethereum)
  is deferred to Phase 2 pending DR-V2ABI / DR-CHAINS research (VIB-3044 epic).
- **Market:** Any registered Aster Perps market — default is `BNB/USD`
  (`ASTER_PERPS_MARKETS['bsc']` also includes `BTC/USD`, `ETH/USD`).
- **Margin:** Native BNB (`openMarketTradeBNB`) by default. ERC-20 margin
  (USDT / USDC) is supported — set `collateral_token` in `config.json`.
- **Order type:** Market orders only. No SL/TP, no limit orders.

## Two-phase execution (what to expect on-chain)

Aster is oracle-priced, so every open and close is a two-phase flow:

1. Your user-signed call emits a `MarketPendingTrade` event with a 32-byte
   `tradeHash`. That's the position identifier. The position is *pending*, not
   open yet.
2. A `PRICE_FEEDER_ROLE` keeper subsequently calls
   `PriceFacadeFacet.requestPriceCallback` to fill the trade at the oracle
   price, emitting `OpenMarketTrade`.

The strategy persists the tradeHash to state so tick 2 can close by
`position_id`. On live BSC the keeper fills within ~1 block; on an Anvil fork
you can simulate it with the helpers in
`tests/intents/bnb/conftest.py::pcs_perps_keeper_fulfill`.

## Running

### Local Anvil BSC fork (recommended for first run)

The default `config.json` opens a $500 position with 0.3 BNB margin, which
means the wallet needs at least 0.3 BNB + gas headroom. On a fresh Anvil
fork, pre-fund the wallet derived from `ALMANAK_PRIVATE_KEY` before running:

```bash
# 1. Start Anvil BSC fork in terminal 1 (or let almanak auto-start it):
anvil --fork-url "https://bnb-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}" --port 8545

# 2. Fund your dev wallet with BNB via Anvil's set_balance RPC (10 BNB):
WALLET=$(uv run python -c "import os; from eth_account import Account; print(Account.from_key(os.environ['ALMANAK_PRIVATE_KEY']).address)")
cast rpc anvil_setBalance "$WALLET" 0x8ac7230489e80000 --rpc-url http://localhost:8545

# 3. Run the strategy — tick 1 opens the position:
almanak strat run -d almanak/demo_strategies/aster_perps_basic --network anvil --once

# 4. Run again — tick 2 closes it (reads tradeHash from persisted state):
almanak strat run -d almanak/demo_strategies/aster_perps_basic --network anvil --once
```

> On a live BSC fork, step 4 will emit a `MarketPendingTrade` that an Aster
> keeper settles within ~1 block. On a local Anvil fork without a running
> keeper, use `tests/intents/bnb/conftest.py::pcs_perps_keeper_fulfill` to
> simulate the settle step.

### Live BSC mainnet

```bash
# Requires ALMANAK_PRIVATE_KEY funded with real BNB (>= 0.3 BNB + gas):
almanak strat run -d almanak/demo_strategies/aster_perps_basic --once
```

## Files

- `strategy.py` — `AsterPerpsBasicStrategy` (`IntentStrategy` subclass)
- `config.json` — tunable params (market, collateral, size, leverage, is_long)
- `__init__.py` — empty

## Related

- Canonical connector: `almanak/connectors/aster_perps/`
- Compatibility shim: `almanak/connectors/pancakeswap_perps/`
- PRD: `docs/internal/discussions/aster-dex-integration-20260418.md`
- Epic: VIB-3044
