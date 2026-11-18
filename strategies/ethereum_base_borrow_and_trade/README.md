# Ethereum and Base independent borrowing and trading

> Aave V3 borrow then one Uniswap V3 swap on Ethereum and Base, one fund per chain: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

One strategy runs the same composed lifecycle independently in two named funds: `ethereum` on Ethereum (`eip155:1`) and `base` on Base (`eip155:8453`). Each fund supplies 0.01 WETH to Aave V3, borrows 5 USDC and trades 2.5 USDC through its chain's explicitly configured USDC/WETH Uniswap V3 pool. One initial USDC per fund remains a repayment reserve. Teardown reverses the trade, revokes exchange approvals, repays the loan and releases collateral, per fund.

Each fund's lending and trading stay on its own chain; cash and obligations remain scoped to the fund and chain. A failed action or liquidation moves only the affected fund into exit; an unavailable chain moves the funds on that chain into exit, and healthy-chain work can continue.

[strategy.py](strategy.py) and [config.yaml](config.yaml) are the author inputs; providers, account bindings and the fork fixture are operator inputs. `backtest-economic.yaml` selects archival Aave V3 and Uniswap V3 inputs for both chains with excluded costs. `almanak support --connector aave_v3 --chain eip155:1`, `almanak support --connector uniswap_v3 --chain eip155:1`, `almanak support --connector aave_v3 --chain eip155:8453` and `almanak support --connector uniswap_v3 --chain eip155:8453` state the declarations; fork proofs use simulated funds.
