# Ethereum SparkLend borrowing and Curve trading

> Composed SparkLend borrow then one Curve StableSwap NG swap on Ethereum: supply WETH, borrow USDC, swap part to USDe, hold, swap back, repay, withdraw.

One named fund supplies WETH, borrows USDC, trades part of the proceeds on the explicit configured Curve pool, reverses the trade, repays debt, revokes allowances and releases collateral. Shared cash is counted once. Backtesting uses separately configured archive-derived prepared inputs with excluded costs. `almanak support --connector sparklend --chain eip155:1` and `almanak support --connector curve --chain eip155:1` state the declarations; fork proofs use simulated funds.

Retained composition proofs cover EOA, generated Safe/Roles and isolated prepared replay.
