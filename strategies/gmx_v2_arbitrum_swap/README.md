# GMX V2 Arbitrum swap

> GMX V2 market swap on Arbitrum: request a WETH-to-USDC order, wait for keeper settlement, hold, then swap back to WETH and revoke approvals.

This reference requests one GMX market-order swap of an explicitly sized WETH
amount into USDC on the WETH/USDC market, waits for its actual keeper settlement,
holds the settled proceeds, then swaps them back and revokes token authority. It
uses `strategy.py` and `config.yaml`; deployment JSON and signing files are
generated runtime artifacts. The market, initial inventory, keeper execution fee,
entry amount, minimum outputs and hold are explicit author inputs. A request that
completes unsuccessfully exits instead of retrying.

A fork needs an external keeper fixture because live GMX keepers do not observe
its local state. `examples/operators/gmx_v2_arbitrum_swap.fork.yaml` selects the
fork anchor and funding. `backtest-economic.yaml` replays published signed GMX
reports with a fixed keeper payment; costs are excluded.

`almanak support --connector gmx_v2 --chain eip155:42161` states the declaration;
fork proofs use simulated funds.
