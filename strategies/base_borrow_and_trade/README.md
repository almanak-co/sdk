# Base Aave V3 borrowing and Uniswap V3 trading

> Composed Aave V3 borrow then one Uniswap V3 swap on Base: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

One named fund supplies 0.01 WETH to Aave V3, borrows 5 USDC and trades 2.5 of the borrowed USDC through the explicitly configured WETH/USDC Uniswap V3 pool. One initial USDC is a repayment reserve. Teardown reverses that trade, cleans up exchange approvals, repays the loan and releases collateral. A failed reversal retains the loan and collateral obligations for recovery. Shared fund cash is counted once.

The author supplies `strategy.py` and `config.yaml`; providers and the fork fixture are operator inputs. `backtest-economic.yaml` selects prepared offline historical inputs and explicit model assumptions. `almanak support --connector aave_v3 --chain eip155:8453` and `almanak support --connector uniswap_v3 --chain eip155:8453` state the declarations; fork proofs use simulated funds.
