# Aave V3 leverage loop on Arbitrum

> Leverage loop with configurable rounds and a health-factor unwind: supply WETH on Aave V3, borrow USDC, buy WETH on Uniswap V3 and resupply, repeated `loops` times.

One fund holds the collateral, the debt and the trade. Every step is derived from
what the strategy owns, so a restart or a partial round resumes where it is:

| The fund owns | Entry decides |
|---|---|
| USDC (borrowed proceeds) | swap it into WETH |
| unsupplied WETH | supply it |
| neither, rounds done < `loops` | borrow `borrow_fraction` of the collateral value |
| neither, all rounds done | hold |

Memory keeps the completed borrow count, whether an unwind has started, why the
trigger check refused a round, if it did (`capped_reason`), and why an unwind holds
with debt open (`shortfall`, see Unwind).

## Parameters (`strategy.parameters`)

| Parameter | Meaning |
|---|---|
| `loops` | Borrow rounds. A round whose projected health factor would end below `unwind_health_factor` is refused, and the loop holds there; memory and the dashboard name the round and why (see below). |
| `borrow_fraction` | Share of the current collateral value borrowed each round. |
| `unwind_health_factor` | The unwind starts below this. `validate_config` requires it strictly above the allocations' `minimum_health_factor`: that floor refuses every withdrawal that would end below it, the unwind's own included. |
| `swap_headroom_bps` | Extra collateral released and sold per unwind cycle beyond the remaining debt, so the pool fee, price impact and accrued interest leave no repay shortfall. |

`minimum_health_factor` on the lending allocations is the gateway's floor, not the
trigger. The authored 1.15 floor sits well below the 1.8 trigger: with all borrowed
proceeds resupplied, the unwind repays only by withdrawing and selling collateral, and the
floor refuses that withdrawal too. The gap between them is how far a price may gap past the
trigger between decisions and still unwind (WETH 4000 to 2600 takes the health factor
from about 2.3 to about 1.5). A floor close to the trigger stalls the unwind after such a
gap.

## Which rounds are reachable

Each round borrows `borrow_fraction` (f) of the collateral value. The borrow is refused
unless the health factor right after it, before the borrowed USDC is swapped and
resupplied, stays at or above `unwind_health_factor` (HF). With the collateral's liquidation threshold
LT, round n is reachable only when

    ((1 + f)^n - 1) / (1 + f)^(n - 1) x HF <= LT

so round 1 needs f x HF <= LT and round 2 needs f x (2 + f) / (1 + f) x HF <= LT.
`borrow_fraction` x `unwind_health_factor` must stay below the liquidation threshold, with more room
for every extra round. Example: Arbitrum WETH has LT about 0.83, so with HF 1.8 round 2
needs f at most about 0.257; at the default 0.25 round 3 is out of reach, so `loops`
above 2 holds at two rounds.

A refused round is never silent: memory records `capped_reason`
(the projected health factor, the trigger and the liquidation threshold), and the dashboard's
`rounds` block shows the reason. Check the formula against the market's liquidation threshold
before choosing `loops` and `borrow_fraction`.

## Trade capital is a slice size

The trade allocation's `capital` is not prefunding. One entry swap sells
min(owned USDC, `capital` x `entry_swap_bps` / 10,000), and owned USDC includes borrowed
proceeds, so the fund starts with WETH only. Keep `capital` above any single borrow
so each round swaps in one step; a smaller value just takes more swaps.

## Unwind

Teardown, a health factor below `unwind_health_factor`, a failed action or an
external event (such as a liquidation) starts the unwind. It repeats one cycle
until the debt is zero:

1. withdraw the most collateral that keeps the health factor above the floor,
   capped at the collateral worth, at the oracle price, the remaining debt x
   (10,000 + `swap_headroom_bps`) / 10,000;
2. swap it to USDC (the trade's exit swap);
3. repay: the full-repay form when the fund's USDC covers the debt, otherwise all owned
   USDC. The full repay checks the wallet balance, since it may also spend USDC sent to
   the fund; the partial repay spends only what the strategy owns.

At or below the floor nothing can be withdrawn and the fund owns no USDC, so the unwind
holds with the debt open. Memory's `shortfall` and the dashboard name the health factor and
the floor; send USDC to the fund until its wallet holds the debt and the next decision
repays it in full.

Then it withdraws the remaining collateral and revokes the swap approval. Leftover
USDC (the headroom) depends on how the unwind started: teardown never proposes an
exposure-increasing intent, so it leaves the USDC as USDC; an unwind started by the
trigger, a failure or an event converts it back to WETH and then holds, and the
strategy does not lever up again.

With `loops: 2` and `borrow_fraction: '0.25'` on Arbitrum WETH (liquidation threshold
about 83%), entry is supply, then borrow, swap, supply twice, ending near health
factor 2.3. Teardown at that price is one cycle: withdraw, swap, full repay, withdraw,
as with `loops: 1`. After a price drop toward the floor each withdrawal is capped, so the
unwind repays over several cycles: withdraw, swap, partial repay, then again until a full
repay, then withdraw.

The gas amounts are examples sized for `loops: 2`; size them from the live gas price
before a production run. Entry is 12 transactions: approve and supply, then per
round borrow, approve and swap, approve and supply. Teardown at the entry price is 7:
withdraw, approve and swap, approve, full repay and revoke, then the final withdraw.
`exit_reserve` covers 12, a two-cycle unwind after a price drop, which first adds
withdraw, approve and swap, approve and partial repay; `total_budget` adds the 12 entry
fees. Each further round adds 5 entry transactions; the exit count depends on how many
unwind cycles the price path needs.

## Interest and a loan-asset reserve

The unwind sells collateral for the remaining debt plus `swap_headroom_bps` of it, so accrued
interest is repaid from collateral and the loop needs no loan-asset reserve. The loan
allocation starts empty; loan asset in `inventory` is not held back, since the first
entry swap sells every owned unit of it into collateral.

## Fork tests

Fork tests use simulated funds and a static price. `strategy.test_parameters` sets
`loops: 1`, which `almanak test run` applies by default (the verdict's
`parameter_overrides` and `parameter_sources` say so); production keeps `loops: 2`.
One round covers supply, one borrow-swap-supply round, and a teardown of withdraw, swap,
full repay, withdraw. The health-factor trigger unwind, the multi-cycle unwind with
partial repays and the hold below the floor need a price drop, which a fork run does not
produce; the offline test `tests/test_authoring_templates.py` covers them.
