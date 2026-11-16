# Arbitrum Aave V3 borrowing and Uniswap V3 trading

> Composed Aave V3 borrow then one Uniswap V3 swap on Arbitrum: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

Composed reference: one fund supplies 0.01 WETH as Aave collateral, borrows 5 USDC, trades 2.5 of the borrowed USDC through the configured WETH/USDC pool, reverses, repays and withdraws. Explicit configured market, source and 1 USDC repayment reserve. `almanak support --connector aave_v3 --chain eip155:42161` and `almanak support --connector uniswap_v3 --chain eip155:42161` state the declarations; fork proofs use simulated funds.

## The loan-asset reserve

Interest is not prefunded: the teardown's full repay needs the debt plus the interest
accrued since the borrow, and the trade's round trip loses the pool fee twice. Borrowed
USDC alone is therefore always a little short. The 1 USDC in `inventory` is the user's own
loan-asset reserve, which `validate_config` accepts and the strategy never supplies or
trades (the swap takes only the trade's `capital` slice), so the full repay settles.
Without it, the repay refuses with `repayment_funding` until loan asset is sent to the
fund. Size it to the interest over the expected holding time plus the round trip's fees;
the fork operator file funds it.

The trade reverses once (`exit`, while `traded`), so withdrawn collateral is never
swapped. Teardown is exit swap, repay, withdraw, revoke.
