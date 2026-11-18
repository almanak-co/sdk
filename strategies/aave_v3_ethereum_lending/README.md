# Aave V3 Ethereum lending

> Aave V3 supply-and-borrow lifecycle on Ethereum: supply WETH, enable collateral, borrow USDC, hold, partially repay and withdraw, then close fully.

This reference uses two named reserve allocations in one fund and Pool. Its
configuration selects WETH collateral, USDC borrowing and explicit Pool oracle
feeds. The connector owns approvals, settlement and allowance cleanup.

The strategy supplies the collateral allocation's `capital`, disables and
re-enables collateral, borrows its `borrow_quantity` parameter and holds the proceeds until
teardown. Cleanup repays half the principal, withdraws one tenth of the original
collateral, repays the remaining debt, with the fixture-funded USDC covering
interest, and
withdraws the remaining supply. Those fractions are the reference's fixed
author policy. Starting capital, the borrowing grant and asset identities remain in
`config.yaml`.

Only completed outcomes advance its durable phase. A failed action or a modeled
liquidation switches to cleanup. Teardown reduces debt before partial collateral
release and clears it before full withdrawal; missing observations preserve the obligation. Supply and borrowing
are not repeated after restart.

Providers, account bindings and the funded fork fixture are operator
inputs in [examples/operators](../../examples/operators/). Safe/Zodiac permissions are generated
from the strategy capability hook. The historical `backtest-economic.yaml`
selects the window, cadence, latency, initial inventory, accounting sample age,
per-block risk collection bounds and liquidation model. Costs are explicitly
excluded in that sample. The prepared model retains hypothetical balances and
uses configured Pool oracle evidence without changing external market history.

Synthetic simulator checks are separate from captured historical reproduction
and EOA/Safe fork proofs. `almanak support --connector aave_v3 --chain eip155:1`
states the declaration; fork proofs use simulated funds.
