# Slipstream Base LP

> Fixed-range Aerodrome Slipstream LP on Base WETH/USDC: swap half the WETH, mint, increase, partially decrease, collect, hold, then exit to WETH.

Reference for the configured verified Slipstream pool and periphery.
Pool identity uses tick spacing; the current swap fee is observed independently.
Only unstaked positions and transaction submitters without fee discounts are modeled.

The fork configuration uses a named Safe with generated Zodiac Roles authority.
Broader NFT manager grants can affect compatible owned or approved NFTs; gateway
operations require the retained strategy-owned NFT.
`almanak support --connector aerodrome_slipstream --chain eip155:8453` states
the declaration; fork proofs use simulated funds.
