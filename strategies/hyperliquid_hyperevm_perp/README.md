# HyperCore BTC perp reference

> Deposit USDC from a Safe on HyperEVM into HyperCore, hold one cross-margin BTC perp and withdraw, in backtests, on HyperEVM forks and in production.

The Hyperliquid reference lifecycle as a strategy: the `trader` account deposits 30 USDC from HyperEVM into its HyperCore account (the first deposit pays the 1 USDC activation fee), sets 10x cross leverage on BTC while flat, opens one 0.002 BTC long with an `Ioc` order, holds it for two hours, closes it with a reduce-only `Ioc` order and withdraws every USDC left back to HyperEVM. A liquidation ends the hold early, and teardown closes and withdraws whatever remains.

The same `strategy.py` runs in both modes and sees the same observation: `effective_at`, `withdrawable` and the account's `activated`, `leverage` and `holding`. Production is declared for this connector on HyperEVM.

## Backtests

Economic backtests read HyperCore's own `/info` history: one-minute trade candles and hourly funding rates, retained in the prepared dataset. Trade candles stand in for the book, the mark and the oracle, and the fill model in `backtest-economic.yaml` states what the missing book would have cost. The venue serves only its latest 5000 candles of an interval, so a one-minute window must lie within about the last 3.5 days when it is prepared; a retained dataset replays offline at any time.

```sh
uv run --locked almanak backtest prepare strategies/hyperliquid_hyperevm_perp \
  --backtest-config strategies/hyperliquid_hyperevm_perp/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-999.yaml \
  --env-file .env --output datasets/hypercore
uv run --locked almanak backtest run strategies/hyperliquid_hyperevm_perp \
  --backtest-config strategies/hyperliquid_hyperevm_perp/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-999.yaml \
  --dataset datasets/hypercore --output runs/hypercore
```

## Fork runs

A fork run trades through the runtime on an identified Anvil fork of HyperEVM mainnet: the `trader` account is a fixture Safe with generated Roles grants, the operator submits its custody transactions, and a fresh agent key the gateway holds signs each order. HyperCore itself is the synthesized `hypercore-model` process the workspace runs beside the fork node, seeded from mainnet `/info`; every HyperCore effect is synthesized and none is evidence of HyperCore's behaviour. What the strategy sees of its HyperCore account is projected from the reconciled custody, trading and fill records, so an open position counts at its entry price in `withdrawable` (no mark price is block-verifiable).

A fork run needs what [getting started](../../docs/getting-started.md#requirements) lists (Linux with bubblewrap, and Foundry's `anvil`) and an ignored `.env` whose `ALCHEMY_API_KEY` belongs to an Alchemy app with HyperEVM (`hyperliquid-mainnet`) enabled. Creating the workspace also reads mainnet `/info` once, to seed the model.

`almanak test run` runs the whole lifecycle once, in a few minutes. It moves fork time an hour between decisions, so the strategy closes its position on the fifth decision and withdraws on the sixth without waiting two hours; `--decisions 8` leaves room for both, where the default of three would stop after the entry and leave the exit to teardown:

```sh
uv run --locked almanak test run strategies/hyperliquid_hyperevm_perp \
  --gateway-config examples/operators/hyperliquid_hyperevm_perp.fork.yaml \
  --env-file .env --output runs/hypercore-test \
  --through-exit --advance 3600 --decisions 8
```

Its verdict lists each decision and transaction. A passing run reports `passed`, `closed` and `evidence_clean` as true, `executed` names `deposit`, `set_leverage`, `increase_position`, `close_position` and `withdraw`, and `holdings` shows the USDC back in the Safe: 30 less the activation fee, both taker fees, the hold's hourly funding and the realized PnL.

`almanak run --fork` starts a workspace that runs at the strategy's own pace, a decision a minute and the whole two-hour hold, and returns once the workspace is running:

```sh
uv run --locked almanak run --fork \
  --config strategies/hyperliquid_hyperevm_perp/config.yaml \
  --strategy strategies/hyperliquid_hyperevm_perp/strategy.py \
  --gateway-config examples/operators/hyperliquid_hyperevm_perp.fork.yaml \
  --env-file .env --output runs/hypercore-fork
```

`almanak status --run runs/hypercore-fork` shows the position, `almanak teardown --run runs/hypercore-fork` closes it reduce-only and withdraws, `almanak shutdown --run runs/hypercore-fork` stops the workspace's services and keeps the recorded evidence, and `almanak report --run runs/hypercore-fork` renders that evidence.

The fork harness's scenario (`tests/strategy_fork/scenarios/hyperliquid_hyperevm_perp`) drives that workspace through entry, pause and resume, and teardown.

## Production

Production is declared Safe/Roles only and is rehearsed through the production adapter on a fork. A real-money run is the operator's own; agents prepare and verify it, and never fund, sign for or start it. The steps:

1. A Safe 1.4.1 on HyperEVM with a Roles module, dedicated to this strategy, and an operator keystore from `almanak wallet create`.
2. `config.production.yaml`, which is `config.yaml` with the operator's gas policy that production requires: custody calls use up to about 370,000 gas, so its 0.002 HYPE ceiling covers 400,000 gas at 5 gwei, and entry takes 5 transactions and exit 2.
3. An operator file filled in from `examples/operators/hyperliquid_hyperevm_perp.production.yaml`, whose provider serves HyperEVM's head exactly: Alchemy runs reads pinned near its head at head + 1, which the gateway refuses.
4. The online check, after which the Safe owners execute the permissions batch it reports. Fund the Safe with the 30 USDC and the operator with HYPE for gas (0.014 HYPE is the entry minimum under this gas policy):

   ```sh
   uv run --locked almanak check strategies/hyperliquid_hyperevm_perp \
     --config strategies/hyperliquid_hyperevm_perp/config.production.yaml \
     --production --gateway-config /private/operator/hyperliquid.production.yaml \
     --env-file /private/providers.env --online --format text
   ```

5. The run, started by the operator:

   ```sh
   uv run --locked almanak run --production \
     --config strategies/hyperliquid_hyperevm_perp/config.production.yaml \
     --strategy strategies/hyperliquid_hyperevm_perp/strategy.py \
     --gateway-config /private/operator/hyperliquid.production.yaml \
     --env-file /private/providers.env --output runs/hypercore-production
   ```

6. Once the first deposit has activated the Safe's HyperCore account, the owners send HYPE to it from the Safe, a transfer to `0x2222…2222`; 0.001 HYPE pays for fifty withdrawals at a 0.1 gwei base fee. Until then a withdrawal stops the run with `funding_unavailable` before anything is sent, and `almanak resume --run runs/hypercore-production` continues it.
7. Afterwards `almanak report --run runs/hypercore-production` renders the run, which is the cell's promotion evidence.
