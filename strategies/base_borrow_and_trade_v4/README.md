# Base Aave V3 borrowing and Uniswap V4 trading

> Composed Aave V3 borrow then one hookless Uniswap V4 swap on Base: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

One named fund supplies WETH to Aave V3, borrows USDC and trades part of the borrowed USDC through the explicitly configured Uniswap V4 pool. Teardown reverses that trade, cleans up exchange approvals, repays the loan and releases collateral. A failed reversal retains the loan and collateral obligations for recovery. Shared fund cash is counted once.

The author supplies `strategy.py` and `config.yaml`. `backtest-economic.yaml` selects prepared offline historical inputs and explicit model assumptions. Providers and the fork fixture are operator inputs. `almanak support --connector aave_v3 --chain eip155:8453` and `almanak support --connector uniswap_v4 --chain eip155:8453` state the declarations; fork proofs use simulated funds.

