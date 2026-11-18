# Compound V3 Optimism lending

> Compound V3 USDC Comet on Optimism with two funds: a lender supplies USDC; a borrower posts WETH, borrows USDC, then repays in steps and withdraws.

This reusable strategy supplies the configured loan asset in the lender fund and borrows it against WETH collateral in the borrower fund. It exercises partial and full repayment and withdrawal while keeping independent fund ownership. The author configuration names the exact eip155:10 market and financial sources.

Providers and the fork fixture are operator inputs in [examples/operators](../../examples/operators/); fork execution uses generated Safe/Zodiac authority and simulated initial capital. EOA execution is exercised by the dedicated proof harness. The separate economic backtest configuration prepares explicit archive-derived inputs before offline execution.

`almanak support --connector compound_v3 --chain eip155:10` states the declaration; fork proofs use simulated funds.
