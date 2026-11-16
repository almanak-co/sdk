# Ethereum Compound V3 borrowing and Curve trading

> Compound V3 borrow then one Curve StableSwap NG swap on Ethereum with two funds: a lender supplies USDC; a borrower posts WETH, borrows USDC, swaps some to USDe, then unwinds.

A named lender supplies USDC to the configured Comet. A separate fund supplies WETH, borrows USDC, trades part of the proceeds on the explicit configured Curve pool, reverses the trade, repays debt, revokes allowances and releases collateral; the lender then withdraws its supply. Shared cash is counted once. Backtesting uses separately configured archive-derived prepared inputs with excluded costs. `almanak support --connector compound_v3 --chain eip155:1` and `almanak support --connector curve --chain eip155:1` state the declarations; fork proofs use simulated funds.

Retained composition proofs cover EOA, generated Safe/Roles and isolated prepared replay.
