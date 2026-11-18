# Uniswap V3 Base trading

> Single Uniswap V3 swap on Base USDC/WETH: buy WETH with all the USDC once, hold for a fixed time, then sell back to USDC and revoke approvals.

Reference using the explicit USDC/WETH pool and capital in `config.yaml`;
providers and the fork fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:8453` states the
declaration; fork proofs use simulated funds.

Buy WETH, hold, then return retained inventory to USDC and revoke approvals.
Strategy memory records completed economic actions; the connector owns
inventory accounting. Restart retains both. Teardown exits outstanding obligations
regardless of the normal hold period.

`backtest-economic.yaml` explicitly selects archival Uniswap V3 pool data,
60-second samples, a 72-second maximum age and collection limits. Decisions
retain the author's decision interval. Costs are explicitly excluded. These
inputs require preparation and verification before they establish a historical
lifecycle proof.

## Timing the hold (and any repeated schedule)

`bought_at` is the buy's fill time (`result.effective_at`), which lands a little after
the decision that bought. The sell is due once `hold_seconds` has passed, with half a
`decision_interval` of tolerance: without it, the decision exactly `hold_seconds` later
sees a few seconds missing and the sell slips to the next decision. A strategy that
repeats a buy on a schedule (one buy a day on an hourly interval, say) and times each
round from the last fill must keep that tolerance, or the schedule drifts by one
decision interval per round (25 h instead of 24 h). After the sell this reference holds:
it trades once per deployment.
