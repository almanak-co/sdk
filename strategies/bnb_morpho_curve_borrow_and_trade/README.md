# BNB lender-funded Morpho borrowing and Curve trading

> Morpho Blue borrow then one Curve StableSwap NG swap on BNB Chain with two funds: a lender supplies USDT; a borrower posts WBNB, borrows USDT, swaps some to crvUSD, then unwinds.

A named lender supplies USDT into the configured initialized Morpho market. A separate fund supplies WBNB collateral, borrows USDT and trades part of those proceeds through the explicit Curve pool. Teardown reverses the trade, repays debt, releases collateral and withdraws lender supply. The lender step supplies actual liquidity; no external market liquidity is assumed for fork execution.

Each fund retains its own cash and protocol ownership. Generated Safe/Zodiac authority is installed only on forks. Prepared execution retains explicit archival inputs and runs offline. `almanak support --connector morpho_blue --chain eip155:56` and `almanak support --connector curve --chain eip155:56` state the declarations; fork proofs use simulated funds.
