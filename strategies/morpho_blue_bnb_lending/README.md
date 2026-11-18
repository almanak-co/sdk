# Morpho Blue BNB lending

> Morpho Blue on BNB Chain with two funds: a lender supplies USDT; a borrower posts WBNB, borrows USDT, partially repays and withdraws, then fully exits.

Named funds supply USDT and borrow it against WBNB collateral, partially repay and withdraw, then close all obligations. The explicit market has no external supplied liquidity at the qualification snapshots; the lender fund supplies actual liquidity before borrowing. Fork runs install generated Safe/Roles authority. Prepared runs acquire explicit archival inputs before isolated execution. `almanak support --connector morpho_blue --chain eip155:56` states the declaration; fork proofs use simulated funds.
