# Robinhood Morpho borrowing and Curve trading

> Composed Morpho Blue borrow then one Curve StableSwap NG swap on Robinhood: supply WETH, borrow USDG, swap part to PUSD, hold, swap back, repay, withdraw.

One named fund supplies WETH collateral, borrows USDG, and trades part of the borrowed inventory for PUSD through the explicitly configured Curve StableSwap NG pool. Cleanup reverses the trade before repaying debt and releasing collateral. Oracles, pool, assets and chain remain explicit author inputs; no peg or common-currency valuation is invented. USDG unclaimed rewards are excluded.

The generated Safe/Zodiac profile installs only on a local fork. EOA and prepared proofs use the same reusable author and shared inventory. The separate economic configuration requires archived inputs before offline execution. The reference borrows 0.1 USDG and trades 0.05 USDG, within the retained market liquidity. Retained proofs cover all three native profiles and isolated prepared reproduction. `almanak support --connector morpho_blue --chain eip155:4663` and `almanak support --connector curve --chain eip155:4663` state the declarations; fork proofs use simulated funds.

The configured USDG/pUSD pool `0xaef38aec2b7daf4df4561a61e82b57d710e5b60b` is the only USDG/pUSD pool the reviewed Robinhood StableSwap NG factory registers. It never held more than about 3 USD and has held no balances or LP supply since about 15 September 2026, so `ax pool` reports it as `unfunded_pool` and fork and production checks refuse it until it is funded again. Its backtest window predates that; this reference demonstrates the connectors, not a market to trade in.
