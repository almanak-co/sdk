# GMX V2 Arbitrum position

> One GMX V2 long on Arbitrum WETH/USDC: open a sized position, wait for keeper settlement, hold, then request a full close.

This reference opens one explicitly sized long position, waits for its
actual keeper settlement, holds, and requests a full close. It uses `strategy.py`
and `config.yaml`; deployment JSON and signing files are generated runtime artifacts.
The named market, collateral, fee asset, entry size and price arguments are
explicit author inputs.

The reusable strategy has actual identified-fork proofs through the dedicated
runtime and gateway with both EOA funds and generated Safe/Roles authority.
Runtime and gateway restarts preserve the position. A separate Safe proof kills
the coordinator after order creation is broadcast, recovers the same accepted
request, then cancels that exact request after protocol eligibility and closes.

A fork needs an external keeper fixture because live GMX keepers do not observe
its local state. The discovered test scenario submits authentic captured signed
reports through an existing authorized keeper. It retains those transactions and
report provenance; runtime accounting still reads the actual canonical events.

Run the explicit proofs with the operator's selected Alchemy environment:

```sh
uv run --locked pytest -n 2 tests/fork/test_gmx_requests.py \
  'tests/strategy_fork/test_strategies.py::test_strategy_lifecycle[configured-gmx_v2_arbitrum_position]' \
  --fork-env=.env
```

The reference Safe is dedicated. Accepted cancellation grants include any nonzero
compatible Safe-owned order, deposit or withdrawal key. Other requests can retain
foreign recipients and callbacks. The gateway permits only its exact retained
request; these additional checks do not narrow the installed on-chain grants.

`almanak support --connector gmx_v2 --chain eip155:42161` states the declaration;
fork proofs use simulated funds.
