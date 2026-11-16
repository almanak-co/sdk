# Getting started

This tutorial follows the
[Uniswap V4 USDC trading reference](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v4_trading).
It is small enough to read in one sitting but includes the full strategy
lifecycle: enter, hold, recover after restart and exit.

The tutorial uses a repository checkout because reference strategies and sample
operator configurations are not installed with the Python package.

## Requirements

Install Python 3.12+ and [uv](https://docs.astral.sh/uv/). Local strategy hook
execution is currently verified on Linux. On Debian or Ubuntu, install the
required sandbox with:

```sh
sudo apt-get install bubblewrap libseccomp2
```

Fork execution also requires Foundry's `anvil`. macOS packages and CLI commands
are available, but macOS currently rejects the author-worker memory limit, so
strategy hook execution fails closed there until that policy is redesigned.

Clone the repository and install its locked dependencies:

```sh
git clone https://github.com/almanak-co/sdk.git
cd sdk
uv sync --locked
```

For a separate project, install the SDK with `uv add almanak`. To install only
the command-line tool, use `uv tool install almanak`. Commands in this tutorial
run from the checkout and therefore use `uv run --locked`.

## Read the two author files

The reference has two inputs maintained by the strategy author:

- [`strategy.py`](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v4_trading/strategy.py)
  contains the decision hooks and typed memory.
- [`config.yaml`](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v4_trading/config.yaml)
  selects the market, fund, capital, risk bounds and timing.

The configuration joins three names. This abbreviated excerpt shows the binding;
use the complete reference file as the executable input:

```yaml
strategy:
  allocations:
    trading:
      market: trading_market
      account: trading
      capital:
        asset: eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        quantity: '100'

markets:
  trading_market:
    connector: uniswap_v4
    chain: eip155:1
    # Complete pool identity follows in the reference.

accounts:
  trading:
    chain: eip155:1
```

`trading` is a local allocation and account name. `trading_market` is a local
market name. On-chain identity comes from the connector target, CAIP-2 chain and
CAIP-19 assets, never from these labels or token symbols.

The strategy keeps only the time at which each purchase completed:

```python
from pydantic import Field

from almanak.api import StrategyState, declared


class State(StrategyState):
    bought_at: dict[str, int] = Field(default_factory=dict)


def capabilities(config):
    return declared(config)
```

At each evaluation, `decide()` reads the connector observation for an
allocation. It asks the connector to enter if the position is not complete, and
to perform a full exit when the hold expires or teardown begins. `hold_seconds` is
optional and read only by this code; without it the allocation holds until teardown:

```python
def decide_allocation(context, allocation, observed):
    started = state(context).bought_at.get(allocation.name)
    due = (
        started is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= started + allocation.hold_seconds
    )
    if context.teardown or due:
        return context.actions.exit(allocation.name)
    if not observed.owned.entry_complete:
        return context.actions.swap(allocation.name)
    return None
```

The connector owns approvals, swap construction, residual conversion and
allowance cleanup. `on_outcome()` records the purchase time only after the
economic swap is complete. An intermediate approval does not advance strategy
memory.

## Check it offline

Run the source and author-configuration checks without provider access:

```sh
uv run --locked almanak check strategies/uniswap_v4_trading --format text
```

The result should report `valid: True`, with `configuration` and `sandbox`
passed. It reports `ready: False` because market resolution, hooks and online
admission deliberately remain `not_checked` without an explicit provider and
run mode. A skipped online stage is not a passing readiness claim.

Inspect the exact connector declaration used during admission:

```sh
uv run --locked almanak support \
  --connector uniswap_v4 --chain eip155:1 --format text
```

Use this command before adapting the strategy to another connector, chain,
action or mode. The generated [support catalog](support.md) presents the same
data.

## Run the historical evaluation

The reference's `backtest-economic.yaml` adds a historical window,
hypothetical inventory, execution latency, sampling bounds, costs and end
treatment. Provider access is needed only to prepare or find matching historical
inputs. Create an ignored `.env` containing the operator's Alchemy key:

```text
ALCHEMY_API_KEY=your-key
```

Check the author and backtest inputs together, then run:

```sh
uv run --locked almanak check strategies/uniswap_v4_trading \
  --backtest-config strategies/uniswap_v4_trading/backtest-economic.yaml \
  --format text
uv run --locked almanak backtest run strategies/uniswap_v4_trading \
  --backtest-config strategies/uniswap_v4_trading/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-1.yaml \
  --env-file .env --output runs/uniswap-v4-tutorial
```

Preparation resolves the configured market at the historical boundary and
collects the explicit model inputs. Economic execution then runs offline. The
workspace retains the resolved facts, data identity, decisions, hypothetical
positions, costs and progress.

Reproduce the run without provider access and render a portable report:

```sh
uv run --locked almanak backtest reproduce --run runs/uniswap-v4-tutorial
uv run --locked almanak report --run runs/uniswap-v4-tutorial \
  --format html --output uniswap-v4-tutorial.html
```

## Optional fork rehearsal

A fork exercises the real deployed contracts at the exact anchor in the
operator fixture. It uses simulated funds and generated authority; it does not
authorize real spending.

First resolve and validate the complete fork inputs through a read-only gateway:

```sh
uv run --locked almanak check strategies/uniswap_v4_trading --fork --online \
  --gateway-config examples/operators/uniswap_v4_trading.fork.yaml \
  --env-file .env --format text
```

Then start the supervised run:

```sh
uv run --locked almanak run strategies/uniswap_v4_trading --fork \
  --gateway-config examples/operators/uniswap_v4_trading.fork.yaml \
  --env-file .env --output runs/uniswap-v4-fork
```

Ctrl-C pauses the coordinator while retaining the gateway, fork process,
positions and pending work. Inspect and close the deployment explicitly:

```sh
uv run --locked almanak status --run runs/uniswap-v4-fork
uv run --locked almanak resume --run runs/uniswap-v4-fork
uv run --locked almanak teardown --run runs/uniswap-v4-fork
uv run --locked almanak status --run runs/uniswap-v4-fork
uv run --locked almanak shutdown --run runs/uniswap-v4-fork
```

Wait for recorded closure before shutdown. A stopped fork cannot be recreated as
the same deployment, and resume never invents replacement history.

## Build your own

Use this order when adapting a reference:

1. Find the nearest supported lifecycle in [Example strategies](examples.md).
2. Confirm its connector, chain, actions and run modes in
   [Supported connectors](support.md).
3. Change author choices in `config.yaml` while preserving strict chain and
   asset identities.
4. Change `strategy.py` through the public hook and action contracts in
   [Strategy authoring](authoring.md).
5. Run the offline check after every small change.
6. Add an explicit backtest configuration and inspect the retained assumptions.
7. Use a fork only when the strategy passes its earlier checks.

Read [Configuration](configuration.md) for the author schema,
[Backtesting](backtesting.md) for data and model limits, and
[Operating](operating.md) before working with retained fork or production
workspaces.
