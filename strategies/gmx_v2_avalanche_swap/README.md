# GMX V2 Avalanche swap

> Fork-only GMX V2 market swap on Avalanche: request a WAVAX-to-USDC order, wait for keeper settlement, hold, then swap back to WAVAX and revoke approvals.

This candidate reference requests one GMX market-order swap of an explicitly
sized WAVAX amount into USDC on the WAVAX/USDC market, waits for its actual keeper
settlement, holds the settled proceeds, then swaps them back and revokes token
authority. It uses `strategy.py` and `config.yaml`; deployment JSON and signing
files are generated runtime artifacts. The market, initial inventory, keeper
execution fee, entry amount, minimum outputs and hold are explicit author inputs.
A request that completes unsuccessfully exits instead of retrying.

A fork needs an external keeper fixture because live GMX keepers do not observe
its local state. `examples/operators/gmx_v2_avalanche_swap.fork.yaml` selects the
fork anchor and funding.

`almanak support --connector gmx_v2 --chain eip155:43114` declares Avalanche for
identified forks only: it is a candidate, not promoted SDK support, and its
offline models are unproven.
