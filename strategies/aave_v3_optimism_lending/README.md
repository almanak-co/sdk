# Aave V3 Optimism lending

> Aave V3 supply-and-borrow lifecycle on Optimism: supply WETH, enable collateral, borrow USDC, hold, partially repay and withdraw, then close fully.

This reference supplies WETH, toggles collateral, borrows native USDC, holds across
restart, then partially repays and withdraws before full closure. Configuration
names the Pool, both reserve assets and the exact Aave oracle feeds. Safe funds
own the positions and proceeds; operators submit and pay gas.

The prepared model uses explicit archive sampling and Aave oracle risk reads.
Approval mechanics and transaction costs remain excluded from that model.
`almanak support --connector aave_v3 --chain eip155:10` states the
declaration; fork proofs use simulated funds.
