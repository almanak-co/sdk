# Uniswap V3 Arbitrum trading

> Single Uniswap V3 swap on Arbitrum USDC/WETH: buy WETH with all the USDC once, hold for a fixed time, then sell back to USDC and revoke approvals.

Reference on Arbitrum One (`eip155:42161`). Uses the explicitly configured native USDC/WETH 0.05% pool and verified factory, SwapRouter02, QuoterV2 and position-manager revisions.

The single author configuration fixes the fund, inventory, five-minute hold and one-minute decision interval. The separate economic backtest configuration freezes archival pool observations and integer tick coverage. Execution costs are explicitly excluded from economic results.

Fork execution uses generated Safe/Roles authority on an identified Anvil fixture. `almanak support --connector uniswap_v3 --chain eip155:42161` states the declaration; fork proofs use simulated funds.

## Timing the hold (and any repeated schedule)

`bought_at` is the buy's fill time (`result.effective_at`), which lands a little after
the decision that bought. The sell is due once `hold_seconds` has passed, with half a
`decision_interval` of tolerance: without it, the decision exactly `hold_seconds` later
sees a few seconds missing and the sell slips to the next decision. A strategy that
repeats a buy on a schedule (one buy a day on an hourly interval, say) and times each
round from the last fill must keep that tolerance, or the schedule drifts by one
decision interval per round (25 h instead of 24 h). After the sell this reference holds:
it trades once per deployment.
