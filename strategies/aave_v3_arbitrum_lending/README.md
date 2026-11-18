# Aave V3 Arbitrum lending

> Aave V3 supply-and-borrow lifecycle on Arbitrum: supply WETH, enable collateral, borrow USDC, hold, partially repay and withdraw, then close fully.

This reference supplies WETH, toggles collateral before borrowing native USDC,
holds and restarts, then partially repays and withdraws before full closure.
The author configuration names the Pool, both reserve assets and exact Aave price
sources. Safe funds own positions and borrowed proceeds; operators pay gas.

The economic configuration selects an explicit captured window, sampling and
pool-oracle risk reads. Arbitrum block density has explicit collection bounds.
Approval mechanics and transaction costs are excluded from the economic model.
`almanak support --connector aave_v3 --chain eip155:42161` states the
declaration; fork proofs use simulated funds.

The loan allocation accepts an optional
[loan-asset reserve](../../docs/authoring.md#a-loan-asset-reserve-for-interest) for the
interest the teardown's full repay needs.
