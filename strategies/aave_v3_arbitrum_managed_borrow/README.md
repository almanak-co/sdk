# Aave V3 health-factor-managed borrow on Arbitrum

> Supply WETH, borrow USDC to a target health factor, and keep it inside a band: repay to the target below a repay trigger set above the gateway floor, borrow to it above an upper trigger.

The fund supplies its WETH `capital` and borrows USDC; the borrowed USDC stays in the
fund. Every decision reads the account's health factor from the Pool:

| Health factor | Decision |
|---|---|
| above `borrow_health_factor` (including no debt) | borrow until it reaches `target_health_factor` |
| below `repay_health_factor` | repay held USDC until it reaches `target_health_factor` |
| in between | hold |

## Parameters (`strategy.parameters`)

`validate_config` requires
`minimum_health_factor < repay_health_factor < target_health_factor < borrow_health_factor`.

- `minimum_health_factor` (on both allocations) is the gateway floor: it refuses any
  borrow or withdrawal that would end below it, teardown's included. It is not the trigger.
- `repay_health_factor` is the de-risking trigger. It sits above the floor so the strategy
  acts before the gateway would block it; a repay only raises the health factor, so the
  floor never blocks the repay itself.
- The gap between the triggers and the target keeps interest drift from causing a
  borrow or repay every decision.

## Teardown and interest

Teardown repays in full, then withdraws. When the fund holds less than the debt, it repays
what the fund holds and holds, naming the shortfall on the dashboard. A failed action or an external event (such as a
liquidation) also closes. Interest is not prefunded: the full repay needs the debt plus
the interest accrued since the borrow, so the loan allocation's `capital: '1'` is a
[loan-asset reserve](../../docs/authoring.md#a-loan-asset-reserve-for-interest) of 1 USDC
that the funding statement shows and the fork operator file funds. Size it to the
expected interest, or send loan asset to the fund when the dashboard names a shortfall.

The gas amounts are examples; size them from the live gas price
before a production run. `total_budget` covers `exit_reserve` plus 6 entry fees and 20 band
adjustments (a borrow or a repay each). Exit is 4: approve, repay and revoke, then
withdraw.

Fork tests: the default run is supply, borrow (to the target), then holds; teardown is
repay, withdraw. A repay inside the run needs the health factor to fall below
`repay_health_factor` (a price drop), which a fork run does not produce; the offline test
`tests/test_authoring_templates.py` covers that branch.
