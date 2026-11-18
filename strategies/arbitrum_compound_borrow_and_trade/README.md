# Arbitrum Compound V3 borrowing and Uniswap V3 trading

> Composed Compound V3 borrow then one Uniswap V3 swap on Arbitrum: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

This reference reuses the lending and trading strategy with explicitly configured
Arbitrum contracts and oracle inputs. It supplies 1 WETH, borrows 200 USDC and
trades 100 of them; 40 initial USDC is a repayment reserve. Borrowed USDC funds a
WETH round trip.
Teardown closes the trade, repays debt and withdraws collateral.

The configuration defines the selected Comet market and pool; neither is a
connector default. `almanak support --connector compound_v3 --chain eip155:42161` and `almanak support --connector uniswap_v3 --chain eip155:42161` state the declarations; fork proofs use simulated funds.
