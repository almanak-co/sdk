# Slipstream Base trading

> Single Aerodrome Slipstream swap on Base: sell all the WETH for USDC once, hold for a fixed time, then buy WETH back and revoke approvals.

Reference for the configured verified Slipstream pool and periphery.
Pool identity uses tick spacing; the current swap fee is observed independently.
Only transaction submitters without fee discounts are modeled.

The fork configuration uses a named Safe with generated Zodiac Roles authority.
`almanak support --connector aerodrome_slipstream --chain eip155:8453` states
the declaration; fork proofs use simulated funds.
