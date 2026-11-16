# GMX V2 Arbitrum collateral path

> GMX V2 collateral management on Arbitrum: open, increase, add then withdraw collateral at fixed notional, partially decrease, hold and close.

Open, increase, add collateral without changing notional, withdraw the added
collateral without changing notional, partially decrease, hold and close. Each
entry after the first waits for a new chain timestamp after the previous
completed request. Long and short proofs passed through Safe/Roles and EOA,
including both restarts, funding cleanup and direct Safe permission rejection
checks. `almanak support --connector gmx_v2 --chain eip155:42161` states the
declaration; fork proofs use simulated funds.
