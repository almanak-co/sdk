# Curvance

Connector for the [Curvance](https://curvance.com) isolated-market lending
protocol. Curvance pairs a **collateral cToken** (ERC-4626-style) with a
**BorrowableCToken** (debt side) per market. Each market is governed by its
own `MarketManager` — there is no global pool.

Supported chains: **Monad**.

Supported markets on Monad (as of 2026-04-18): `ezETH→WETH`, `WETH→USDC`,
`WMON→USDC`, `WBTC→USDC`, `aprMON→WMON`, `shMON→WMON`. See
[`CURVANCE_MARKETS`][almanak.connectors.curvance.CURVANCE_MARKETS]
for the authoritative list.

## Quick example

```python
from decimal import Decimal
from almanak.framework.intents import SupplyIntent

intent = SupplyIntent(
    protocol="curvance",
    token="WMON",
    amount=Decimal("1.0"),
    use_as_collateral=True,
    market_id="0xa6A2A92F126b79Ee0804845ee6B52899b4491093",  # WMON-USDC MarketManager
    chain="monad",
)
```

## Known constraints

- **20-minute `MIN_HOLD_PERIOD`** on collateral before it can be withdrawn
  (reverts with `MarketManager__MinimumHoldPeriod()`).
- **`MIN_LOAN_SIZE` per market**, set at deployment in the `[10e18, 100e18]`
  WAD range. Borrows below the market's floor revert with
  `LiquidityManager__InsufficientLoanSize()`.
- `redeemCollateral` does **not** accept `MAX_UINT256` as a "redeem all"
  sentinel — strategies must read `balanceOf(user)` on the cToken and pass
  the exact share count. Use `withdrawCollateral(assets, receiver, owner)`
  for asset-amount-denominated exits.
- `repay(0)` is Curvance's full-debt-repay sentinel (NOT `MAX_UINT256`).

## Module reference

::: almanak.connectors.curvance
    options:
      show_root_heading: true
      members_order: source
