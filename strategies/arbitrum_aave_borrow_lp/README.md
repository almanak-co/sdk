# Aave V3 borrow, then Uniswap V3 liquidity on Arbitrum

> Borrow USDC against WETH on Aave V3 and provide it as a banded Uniswap V3 position funded in the loan asset, with a health-factor unwind that repays from the position.

One fund holds the collateral, the debt and the position. Every step is derived from
what the fund owns:

| The fund owns | Entry decides |
|---|---|
| WETH, no debt yet (collateral is the pool's WETH) | supply it as collateral |
| collateral capital, nothing supplied (collateral outside the pool, e.g. wstETH) | supply the collateral `capital` |
| collateral, no debt | borrow `borrow_fraction` of the collateral value |
| USDC only (the borrow, plus any reserve) | one band rebalance swap to the ratio the centred band consumes |
| USDC and the pool's other asset | open the band position with both |
| an open position | hold |

The collateral need not be a pool asset: with `cover_shortfall: false` (the default)
the pool only has to include the loan asset, so wstETH collateral with the WETH/USDC
pool works. WETH the user funds for the position (inventory beside wstETH collateral)
stays in the fund and joins the position. When the collateral *is* the pool's WETH,
every WETH unit owned before the borrow is supplied as collateral, so fund the position
side with a loan-asset reserve instead.

## Why the position is funded in the loan asset

The liquidity allocation's `capital` is USDC, the borrowed asset. Its exit (close, then
sell the position's WETH) yields USDC, which is what the repay needs. Funded in WETH,
the exit would convert to WETH and leave the debt asset missing.

## Parameters (`strategy.parameters`)

| Parameter | Meaning |
|---|---|
| `borrow_fraction` | Share of the collateral value borrowed once. A borrow whose projected health factor would end below `unwind_health_factor` is not made. |
| `unwind_health_factor` | The unwind starts below this. `validate_config` requires it strictly above the allocations' `minimum_health_factor`: that floor refuses every withdrawal that would end below it, the unwind's own included. |
| `swap_headroom_bps` | With `cover_shortfall`, the discount on the quoted sale when checking it covers the debt, and the margin by which collateral released for a shortfall exceeds it, for the pool fee, price impact and interest. |
| `cover_shortfall` | Default `false`. When the position's proceeds and the fund's own assets fall short of the debt, `true` withdraws and sells collateral through the pool to cover the rest. |

## The loan-asset reserve and `cover_shortfall`

Swap fees, price moves and accrued interest leave the position's proceeds a little
short of the debt; interest is never prefunded. The authored config therefore funds a
5 USDC loan-asset reserve in `inventory`, the user's own USDC: it joins the position at
entry and returns with it at close, so teardown repays in full without selling
collateral. Size it to the expected interest and fees plus the position loss you accept;
user-funded WETH for the position works the same way.

With `cover_shortfall: false`, a teardown still short after selling the position's WETH
repays what the fund owns and holds with the rest of the debt open. Memory's `shortfall`
and the dashboard name the amount. Send loan asset to the fund until its wallet holds the
debt: the full repay may spend loan asset the strategy does not own, so the next decision
repays in full.

Turn `cover_shortfall` on when the margin is thin: little or no reserve or user-funded
pool asset, or a band likely to close at a loss larger than the reserve. It needs the
collateral to be one of the pool's assets, since it sells the withdrawn collateral
through the same pool, and `validate_config` refuses it otherwise. A second pool for the
conversion (say wstETH/WETH) is not an option: a fund has one Uniswap V3 allocation.
For other collateral keep it off and fund the reserve.

## Unwind

Teardown, a health factor below `unwind_health_factor`, a failed action or an external
event starts the unwind:

1. close the position;
2. if the fund's USDC covers the debt, repay it in full and go to step 5. This checks the
   wallet balance, not owned inventory: the full repay may also spend USDC sent to the
   fund, while swaps and partial repays spend only what the strategy owns;
3. with `cover_shortfall`, value the closed position's WETH at the exit quote and add the
   owned USDC, less `swap_headroom_bps`; if that is short of the debt, withdraw the
   collateral that covers the rest (never below the floor) *before* selling, so one sale
   carries the position's WETH and the withdrawn collateral together;
4. sell the owned WETH for USDC in one exit swap, then repay in full. Still short after
   the sale (the pool trailing the oracle, a price move), the next decision withdraws
   again; without `cover_shortfall`, repay what the fund owns and hold;
5. withdraw the collateral, then revoke leftover approvals.

With the reserve, a typical teardown is close, swap, repay, withdraw, revoke. With
`cover_shortfall` and no reserve, swap fees alone leave the position slightly short, so
it is close, withdraw (shortfall), swap, repay, withdraw, revoke: six steps, one exit
swap. The headroom applies to the whole sale, so about twice `swap_headroom_bps` of the
debt is released beyond the bare shortfall. Teardown never buys WETH, so any USDC left
after the repay stays as USDC. An unwind started by the trigger, a failure or an event
converts it back to WETH when WETH is the collateral, and then holds; the position is
not reopened.

The position is opened once, centred on the price at entry, and is held even when the
price leaves the band; see the `*_lp_band` references for recentring.

The gas amounts are examples; size them from the live gas price
before a production run. Entry is supply, borrow, swap and open with their approvals (6). Exit is 10:
close, approve and swap, approve, repay and revoke, withdraw, a revoke for each of up to
two residual position-manager allowances, and one withdraw before the swap if the
position returns less than the debt.
