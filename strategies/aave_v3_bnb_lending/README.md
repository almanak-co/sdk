# Aave V3 BNB lending

> Aave V3 supply-and-borrow lifecycle on BNB Chain: supply WBNB, enable collateral, borrow USDT, hold, partially repay and withdraw, then close fully.

This reference supplies WBNB, toggles collateral, borrows USDT, holds
across restart, then partially repays and withdraws before full closure. Both
ERC-20 reserves use 18 decimals. Configuration names the Pool and exact Aave
oracle feeds. Safe funds own positions and proceeds; operators pay gas.

Prepared mode samples archive accounting and reads oracle risk at every block.
Costs remain explicitly excluded. The 512-block collection cap is explicit.

Proxy metadata differences remain disclosed despite executable source equivalence
and full hash admission. `almanak support --connector aave_v3 --chain eip155:56`
states the declaration; fork proofs use simulated funds.
