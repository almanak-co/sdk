# Compound V3 Arbitrum lending

> Compound V3 USDC Comet on Arbitrum with two funds: a lender supplies USDC; a borrower posts WETH, borrows USDC, then repays in steps and withdraws.

This reference selects one explicit WETH/USDC market on `eip155:42161`.
The lender supplies 1000 USDC. A separate borrower supplies 1 WETH and borrows
200 USDC; the operator fork fixture funds 20 USDC more so the borrower can repay interest. Teardown partially repays and
withdraws before repaying all debt and closing both funds' positions.

`config.yaml` owns the market, oracle, capital, borrowing grant and cadence;
providers and the fork fixture are operator inputs in
[examples/operators](../../examples/operators/). `backtest-economic.yaml` selects explicit archival inputs and simulation
policy. Generated Safe/Zodiac funds own positions; operators submit and pay gas.
The configuration carries an Arbitrum-specific gas policy; do not copy it to another chain.
EOA and generated Safe execution remain subject to the same economic checks.

`almanak support --connector compound_v3 --chain eip155:42161` states the
declaration; fork proofs use simulated funds.
