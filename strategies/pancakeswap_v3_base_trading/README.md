# PancakeSwap V3 Base trading

> Single PancakeSwap V3 swap on Base: sell all the WETH for USDC once, hold for a fixed time, then buy WETH back and revoke approvals.

The explicit configuration starts with 0.1 wrapped ether, uses the configured
USDC/WETH 500-fee pool, and returns to WETH on exit.
Token names are display metadata; CAIP references establish financial identity.

Trading permissions are generated from the sandboxed capability hook.
`almanak support --connector pancakeswap_v3 --chain eip155:8453` states the
declaration; fork proofs use simulated funds.

