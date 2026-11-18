# Aave V3 Polygon lending

> Aave V3 supply-and-borrow lifecycle on Polygon: supply WETH, enable collateral, borrow USDC, hold, partially repay and withdraw, then close fully.

This reference supplies WETH and borrows native USDC on Polygon PoS.
It toggles collateral, holds across restart, then partially repays and withdraws
before full closure. The configuration selects exact Aave oracle feeds. Safe
funds own positions and proceeds; operators pay native POL gas. Fork WETH is
seeded explicitly through local storage because it is not wrapped native POL.

Prepared mode retains archive accounting and oracle risk at every block. Costs
are explicitly excluded. `almanak support --connector aave_v3 --chain eip155:137`
states the declaration; fork proofs use simulated funds.
