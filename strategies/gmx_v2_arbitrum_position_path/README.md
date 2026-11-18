# GMX V2 Arbitrum position path

> GMX V2 position sizing on Arbitrum: open, increase, partially decrease, hold and close one long (or short) position, each step keeper-settled.

Open, increase by the same configured size and collateral, partially decrease by
that size, hold and close. Each request waits for actual keeper completion.
Capital covers the sequence's collateral and keeper fees. The configured side is
long; the same source accepts a short configuration. Runtime proofs passed for
long and short through Safe/Roles and EOA, including both restarts and funding
cleanup. `almanak support --connector gmx_v2 --chain eip155:42161` states the
declaration; fork proofs use simulated funds.
