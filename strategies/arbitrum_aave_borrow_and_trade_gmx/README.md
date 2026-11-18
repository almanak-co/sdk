# Aave V3 borrowing and GMX V2 trading on Arbitrum

> Composed Aave V3 borrow then one GMX V2 swap on Arbitrum: supply USDC, borrow WETH, keeper-settled swap of half, hold, swap back, repay, withdraw.

This composed reference uses one fund and one journal. It supplies 100 USDC,
borrows 0.001 WETH and sends 0.0005 WETH through an asynchronous GMX swap. Initial
WETH cash separately covers the configured GMX execution fees. On teardown it
returns only the recorded output of that trade, revokes any residual GMX allowance, repays
the WETH debt and withdraws USDC collateral. Other shared fund cash is retained.

The configuration selects the Aave reserves, the GMX market and exact CAIP
assets; providers and the fork fixture are operator inputs. The Safe fixture uses
a separate gas-paying operator. GM shares and fund-native refunds have explicit
zero initial inventory entries. EOA variants must explicitly represent any
native gas reserve in their shared inventory.

A keeper must settle the retained GMX requests using authentic reports. The
strategy preserves intermediate outcomes and does not recreate accepted
requests. A prepared run additionally requires explicit GMX publication sources
and keeper policy. `almanak support --connector aave_v3 --chain eip155:42161` and `almanak support --connector gmx_v2 --chain eip155:42161` state the declarations; fork proofs use simulated funds.
