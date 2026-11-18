# Avalanche Aave V3 borrowing and Uniswap V3 trading

> Composed Aave V3 borrow then one Uniswap V3 swap on Avalanche: supply WAVAX, borrow USDC, swap part to WAVAX, hold, swap back, repay, withdraw.

One fund supplies 5 WAVAX, borrows 5 USDC and trades 2.5 of the borrowed USDC in
the configured WAVAX/USDC fee-500 V3 pool; 1 initial USDC is a repayment reserve. Reversal precedes repayment and collateral release.
`almanak support --connector aave_v3 --chain eip155:43114` and `almanak support --connector uniswap_v3 --chain eip155:43114` state the declarations; fork proofs use simulated funds.
