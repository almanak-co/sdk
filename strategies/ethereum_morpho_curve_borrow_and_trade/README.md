# Ethereum Morpho borrowing and Curve trading

> Composed Morpho Blue borrow then one Curve StableSwap NG swap on Ethereum: supply WETH, borrow USDC, swap part to USDe, hold, swap back, repay, withdraw.

One named Ethereum fund supplies WETH collateral to its configured Morpho market,
borrows USDC and exchanges part of those proceeds for USDe through the configured
Curve StableSwap NG pool. Teardown reverses the trade, repays the loan and releases
collateral. The fund cash ledger is shared across both connectors.

The author config fixes markets, capital and fund inventory; providers and the fork
fixture are operator inputs. Generated Safe/Roles permissions come from the strategy
capability hook. `almanak support --connector morpho_blue --chain eip155:1` and
`almanak support --connector curve --chain eip155:1` state the declarations; fork
proofs use simulated funds.
