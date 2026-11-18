# PancakeSwap V3 Base LP

> Fixed-range PancakeSwap V3 LP on Base USDC/WETH: swap half the WETH, mint, increase, partially decrease, collect, hold, then exit to WETH.

The explicit configuration starts with 0.1 wrapped ether, uses the configured
USDC/WETH 500-fee pool and a fixed tick range, and returns to WETH on exit.
Token names are display metadata; CAIP references establish financial identity.

Broader NFT manager grants can affect compatible owned or approved NFTs; gateway
operations require the retained strategy-owned NFT.
`almanak support --connector pancakeswap_v3 --chain eip155:8453` states the
declaration; fork proofs use simulated funds.

