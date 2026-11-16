# Aave V3 lending short on Arbitrum

> Short WETH with Aave V3 and Uniswap V3: supply USDC, borrow WETH and sell it, then buy it back and repay at a price stop, a take-profit or a health-factor trigger above the floor.

One fund holds the USDC collateral, the WETH debt and the sale proceeds. Every step is
derived from what the fund owns:

| The fund owns | Entry decides |
|---|---|
| USDC, no debt yet | supply it as collateral |
| collateral, no debt | borrow WETH worth `borrow_fraction` of the collateral value |
| borrowed WETH | sell it for USDC (the short); the proceeds stay unsupplied |
| the proceeds | hold, watching the stop |

Memory keeps the Pool oracle's WETH price at the first sale (the stop's reference, so a
short sold in slices is measured from its first slice) and whether the unwind has started.

## Parameters (`strategy.parameters`)

| Parameter | Meaning |
|---|---|
| `borrow_fraction` | Share of the collateral value borrowed once. A borrow whose projected health factor would end below `unwind_health_factor` is not made. |
| `stop_loss_bps` | Unwind once the oracle WETH price rises this far above the sale price. |
| `take_profit_bps` | Unwind once it falls this far below. |
| `unwind_health_factor` | The unwind also starts below this. `validate_config` requires it strictly above the allocations' `minimum_health_factor`: that floor refuses every withdrawal that would end below it, the unwind's own included. |
| `swap_headroom_bps` | The discount on the quoted buy-back when checking it covers the debt, and the margin by which collateral released for a shortfall exceeds it, for the pool fee, price impact and interest. |

The trade's `capital` is a WETH slice size, not prefunding: the sale takes the borrowed
WETH. Keep it above the borrow so the short sells in one step.

## Unwind

The stop, the take-profit, the trigger, teardown, a failed action or an external event
starts it:

1. if the fund's WETH covers the debt, repay in full and go to step 4. This checks the
   wallet balance, not owned inventory: the full repay may also spend WETH sent to the
   fund, while swaps and partial repays spend only what the strategy owns;
2. value the buy-back of all owned USDC at the exit quote and add the owned WETH, less
   `swap_headroom_bps`; if that is short of the debt, withdraw the collateral that covers
   the rest (never below the floor) *before* buying, so one buy-back spends the sale
   proceeds and the withdrawn USDC together;
3. buy WETH back with all the USDC the fund holds (the trade's exit swap), then repay in
   full. Still short after the buy-back (a price move between decisions, the pool trailing
   the oracle), the next decision withdraws again;
4. withdraw the collateral, then revoke leftover approvals.

A losing short (price up), or fees at an unchanged price, takes the shortfall from
collateral: withdraw, swap, repay, withdraw (four steps, one buy-back). The headroom
applies to the whole buy-back, so about twice `swap_headroom_bps` of the debt is bought
beyond the bare shortfall; in teardown that extra stays as WETH. A winning one buys back more WETH than it owes. Teardown never sells WETH again,
so that profit stays as WETH; an unwind started by the stop, the take-profit, the trigger
or a failure sells it back to USDC and then holds. A closed short is not reopened.

Step 2 covers accrued interest from collateral, so the short needs no loan-asset (WETH)
reserve; the loan allocation starts empty, and the entry sale would sell any WETH the fund
starts with.

## Not for Aave V4

The shortfall path needs Aave V3's partial withdraw (`withdraw_amount`) and partial repay
(`repay_amount`). Aave V4 has only the full withdraw and the full repay, so
`validate_config` refuses an `aave_v4` market. On Aave V4, supply only part of the USDC
and keep the rest unsupplied as a buy-back reserve: the exit swap spends every owned USDC
unit, then the strategy repays in full and withdraws, and does nothing after the withdraw.
`avalanche_aave_v4_borrow_and_trade` is that shape (full repay, then full withdraw, with an
unsupplied loan-asset buffer). Size the reserve from `stop_loss_bps` plus headroom: at the
stop, the proceeds plus the reserve must buy back the whole debt.

## Gas

The gas amounts are examples; size them from the live gas price
before a production run. Entry is supply, borrow and swap with their approvals (6). Exit is up to 8:
withdraw (shortfall), swap, repay, withdraw and revoke, plus a withdraw, swap and repay
cycle if the health-factor floor caps the first withdraw.

## Fork tests

Fork tests use a static price, so the Pool oracle's WETH price never moves from the sale
price and no `stop_loss_bps` or `take_profit_bps` fires there. A fork run covers supply,
borrow, the sale and the teardown unwind (withdraw the shortfall, buy back, full repay,
withdraw), with any profit kept as WETH. The stop, take-profit and health-factor unwinds
need a price move; `tests/test_authoring_templates.py` covers them offline.
