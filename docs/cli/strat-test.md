
# strat test

Run a force-action lifecycle test for a strategy on a managed Anvil fork.

    Drives each --actions value through the production code path (managed gateway +
    Anvil + funding) as a separate iteration, then optionally exercises teardown.
    Designed for automated test agents that need end-to-end verification without
    orchestrating Anvil/gateway/funding by hand.

    Always runs on --network anvil with --once + --fresh semantics.

    Pass --no-gateway to reuse a long-lived gateway (with Anvil forks already running)
    at --gateway-host:--gateway-port; ALMANAK_GATEWAY_AUTH_TOKEN must match the
    gateway's token. Without --no-gateway, ManagedGateway boots a fresh Anvil per
    invocation.

    Pass --inject to seed synthetic market conditions (prices / balances /
    indicators) into the MarketSnapshot decide() consumes, so condition-triggered
    decision logic runs instead of being force-action short-circuited (VIB-5529).
    Used alone (no --actions), --inject runs one natural decide() iteration so the
    real condition branch executes.

    Examples:

        almanak strat test --actions supply --teardown --json
        almanak strat test --actions open,collect --teardown
        almanak strat test --actions supply,withdraw    # no teardown
        almanak strat test --teardown                   # teardown only (no force_actions)
        almanak strat test --no-gateway --actions open --teardown    # reuse sidecar gateway
        almanak strat test --inject '{"indicators": {"rsi": {"WETH": 25}}}'   # RSI oversold
        almanak strat test --inject '{"prices": {"USDC": "0.95"}}'            # stablecoin depeg
        almanak strat test --inject scenario.json --json                     # from a file
    

## Usage

```
Usage: almanak strat test [OPTIONS]
```

## Arguments


## Options

* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir
-d`
    Working directory containing the strategy files. Defaults to the current directory.


* `config_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--config
-c`
    Path to strategy config JSON file (overrides the one in --working-dir).


* `actions`:
    * Type: STRING
    * Default: ``
    * Usage: `--actions`
    Comma-separated force_action values to drive (e.g. 'open,collect' or 'supply'). Run in the order given. Each value mutates strategy.force_action between iterations; in-memory strategy state (position id, etc.) flows through naturally. Skip values that match what generate_teardown_intents() emits — let --teardown handle those.


* `teardown`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--teardown`
    After the action sequence completes, run a teardown iteration that closes any open positions via the strategy's generate_teardown_intents().


* `inject`:
    * Type: STRING
    * Default: `None`
    * Usage: `--inject`
    Seed synthetic market conditions into the MarketSnapshot decide() consumes, so condition-triggered logic runs (VIB-5529). Inline JSON or a path to a .json file: '{"prices": {"USDC": "0.95"}, "balances": {"USDC": "10000"}, "indicators": {"rsi": {"WETH": 25}}}'. Without --actions, this exercises the real condition branches (depeg = off-peg price; drawdown = lowered price/balance). Overrides win over live provider reads.


* `json_output`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--json`
    Emit a structured JSON result on stdout. The structured payload is the LAST top-level JSON object in stdout — startup/setup diagnostics from the framework's anvil + gateway boot may print human-readable lines BEFORE it. Parsers should extract the final JSON object (e.g. `python -c 'import json,sys; ...JSONDecoder().raw_decode(...)'`), not assume stdout is JSON-only. Exit code is 0 if every step passed (or run was skipped), non-zero otherwise.


* `anvil_ports`:
    * Type: STRING
    * Default: `None`
    * Usage: `--anvil-port`
    Use existing Anvil instance: CHAIN=PORT (e.g., --anvil-port arbitrum=8545). Repeatable.


* `gateway_host`:
    * Type: STRING
    * Default: `127.0.0.1`
    * Usage: `--gateway-host`
    Gateway sidecar hostname.


* `gateway_port`:
    * Type: INT
    * Default: `50051`
    * Usage: `--gateway-port`
    Gateway sidecar gRPC port.


* `no_gateway`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--no-gateway`
    Connect to an existing gateway at --gateway-host:--gateway-port instead of auto-starting a managed one. Useful when a long-lived gateway sidecar is already running (e.g. in a Cloud Run multi-container revision); skips ManagedGateway boot and the per-test Anvil cold-start.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat test [OPTIONS]

  Run a force-action lifecycle test for a strategy on a managed Anvil fork.

  Drives each --actions value through the production code path (managed
  gateway + Anvil + funding) as a separate iteration, then optionally
  exercises teardown. Designed for automated test agents that need end-to-end
  verification without orchestrating Anvil/gateway/funding by hand.

  Always runs on --network anvil with --once + --fresh semantics.

  Pass --no-gateway to reuse a long-lived gateway (with Anvil forks already
  running) at --gateway-host:--gateway-port; ALMANAK_GATEWAY_AUTH_TOKEN must
  match the gateway's token. Without --no-gateway, ManagedGateway boots a
  fresh Anvil per invocation.

  Pass --inject to seed synthetic market conditions (prices / balances /
  indicators) into the MarketSnapshot decide() consumes, so condition-
  triggered decision logic runs instead of being force-action short-circuited
  (VIB-5529). Used alone (no --actions), --inject runs one natural decide()
  iteration so the real condition branch executes.

  Examples:

      almanak strat test --actions supply --teardown --json     almanak strat
      test --actions open,collect --teardown     almanak strat test --actions
      supply,withdraw    # no teardown     almanak strat test --teardown
      # teardown only (no force_actions)     almanak strat test --no-gateway
      --actions open --teardown    # reuse sidecar gateway     almanak strat
      test --inject '{"indicators": {"rsi": {"WETH": 25}}}'   # RSI oversold
      almanak strat test --inject '{"prices": {"USDC": "0.95"}}'            #
      stablecoin depeg     almanak strat test --inject scenario.json --json
      # from a file

Options:
  -d, --working-dir PATH  Working directory containing the strategy files.
                          Defaults to the current directory.
  -c, --config PATH       Path to strategy config JSON file (overrides the one
                          in --working-dir).
  --actions TEXT          Comma-separated force_action values to drive (e.g.
                          'open,collect' or 'supply'). Run in the order given.
                          Each value mutates strategy.force_action between
                          iterations; in-memory strategy state (position id,
                          etc.) flows through naturally. Skip values that
                          match what generate_teardown_intents() emits — let
                          --teardown handle those.
  --teardown              After the action sequence completes, run a teardown
                          iteration that closes any open positions via the
                          strategy's generate_teardown_intents().
  --inject TEXT           Seed synthetic market conditions into the
                          MarketSnapshot decide() consumes, so condition-
                          triggered logic runs (VIB-5529). Inline JSON or a
                          path to a .json file: '{"prices": {"USDC": "0.95"},
                          "balances": {"USDC": "10000"}, "indicators": {"rsi":
                          {"WETH": 25}}}'. Without --actions, this exercises
                          the real condition branches (depeg = off-peg price;
                          drawdown = lowered price/balance). Overrides win
                          over live provider reads.
  --json                  Emit a structured JSON result on stdout. The
                          structured payload is the LAST top-level JSON object
                          in stdout — startup/setup diagnostics from the
                          framework's anvil + gateway boot may print human-
                          readable lines BEFORE it. Parsers should extract the
                          final JSON object (e.g. `python -c 'import json,sys;
                          ...JSONDecoder().raw_decode(...)'`), not assume
                          stdout is JSON-only. Exit code is 0 if every step
                          passed (or run was skipped), non-zero otherwise.
  --anvil-port TEXT       Use existing Anvil instance: CHAIN=PORT (e.g.,
                          --anvil-port arbitrum=8545). Repeatable.
  --gateway-host TEXT     Gateway sidecar hostname.
  --gateway-port INTEGER  Gateway sidecar gRPC port.
  --no-gateway            Connect to an existing gateway at --gateway-
                          host:--gateway-port instead of auto-starting a
                          managed one. Useful when a long-lived gateway
                          sidecar is already running (e.g. in a Cloud Run
                          multi-container revision); skips ManagedGateway boot
                          and the per-test Anvil cold-start.
  --help                  Show this message and exit.
```

