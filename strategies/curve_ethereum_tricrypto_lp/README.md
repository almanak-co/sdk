# Ethereum Curve TriCrypto NG LP

> Curve TriCrypto NG LP on Ethereum USDC/WBTC/WETH: two funds swap, add, increase, partially remove, hold, then exit proportionally or to WETH.

Reference for the verified USDC/WBTC/WETH TriCrypto NG v2.0.0 pool.
Each of two named funds starts with wrapped ETH, swaps part into USDC, opens
and increases fungible LP exposure, partially removes it, holds across restart,
and closes. One fund removes proportionally and the other selects wrapped ETH.
All remaining non-funding coins are exchanged into wrapped ETH before cleanup.

Author inputs are `strategy.py` and `config.yaml`. `backtest-economic.yaml` selects the explicit
prepared window, sample interval, source age, inventory and exclusions. `almanak support --connector curve
--chain eip155:1` states the declaration; fork proofs use simulated funds.
Pool calls use wrapped ERC-20 transfers with the native convenience flag false.
Automatic protocol admin claims mint LP shares to the retained fee receiver;
these shares belong to that receiver, separately from the strategy's holdings.
The sample accounts must not own unrelated pool LP shares.
