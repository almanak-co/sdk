# PancakeSwap V3 BNB trading

> Single PancakeSwap V3 swap on BNB Chain: sell all the WBNB for USDC once, hold for a fixed time, then buy WBNB back and revoke approvals.

The explicit configuration starts with 0.1 wrapped BNB, uses the configured
USDC/WBNB 2500-fee pool, and returns to WBNB on exit.
Token names are display metadata; CAIP references establish financial identity.

Trading permissions are generated from the sandboxed capability hook.
`almanak support --connector pancakeswap_v3 --chain eip155:56` states the
declaration; fork proofs use simulated funds.

