# Polygon Morpho borrowing and Curve trading

> Composed Morpho Blue borrow then one Curve StableSwap NG swap on Polygon: supply WETH, borrow USDC, swap part to crvUSD, hold, swap back, repay, withdraw.

One explicitly named fund supplies WETH collateral, borrows native USDC and trades part of the proceeds through the configured Curve StableSwap NG pool. Teardown reverses the trade, repays the debt and releases collateral. The exact oracle, pool, assets and chain are author inputs.

The reference uses the existing reusable economic lifecycle and connector-owned execution semantics. Its fork profile installs generated Safe/Zodiac authority only on the local fork fixture. The separate economic configuration prepares captured inputs before offline execution. The rollout report records the completed composition proofs. `almanak support --connector morpho_blue --chain eip155:137` and `almanak support --connector curve --chain eip155:137` state the declarations; fork proofs use simulated funds.
