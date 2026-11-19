# Operating retained workspaces

Execution requires `run --fork` or `run --production` with an explicit operator
file (`--gateway-config`) and the private provider env file (`--env-file`).
Credentials never select production implicitly. Each private workspace captures
the authored inputs, source, the generated read context, resolved facts, setup
stages, endpoint references, process identities and logs. `state.sqlite`
contains authoritative execution facts; `workspace.json` contains supervision.
Keep credentials outside strategy-readable paths.

## Operator configuration

The operator file is YAML or JSON with `schema_version: operator-gateway-config-v1`.
It selects the enabled `modes`, one provider per chain under `networks`
(`alchemy` with `api_key_env`, or `rpc_url` with `url_env`; the env file holds the
values), named vendor access under `vendors` when a backtest requests one, the
named `accounts.bindings` the author file requires, and mode
authority: `fork` holds the fixture anchors, funds and operators; `production`
holds execution networks, independent payer ceilings and signing file references.
When the author requests gas policy, `fork.gas` also supplies explicit operator
ceilings, grouped by the fixture's actual payer relationships. A shared operator
requires one compatible policy and ceiling for all accounts it submits for.
The [fork](https://github.com/almanak-co/sdk/blob/main/examples/operators/aave_v4_weth_supply.fork.yaml),
[production](https://github.com/almanak-co/sdk/blob/main/examples/operators/aave_v4_weth_supply.production.yaml)
and [backtest](https://github.com/almanak-co/sdk/blob/main/examples/operators/backtest/eip155-1.yaml)
examples cover the Aave reference. An operator file never carries provider keys.

A run first binds the authored choices to the operator file and writes the read
context, then starts a read-only gateway that resolves the named markets at the
selected anchors and retains the verified facts. Fork runs read the anchors from
the operator fixture; production runs pin fresh anchors. Hooks run once on the
resolved parameters and the results are frozen before any execution authority
exists. `check --online` performs the same resolution and hook stages in a
temporary directory and reports a preview of the resolved facts without creating
a workspace.

| Command | Effect |
| --- | --- |
| `status --run PATH` | Retained financial state and current local process availability. |
| `pause --run PATH` | Wait for coordinator pause; leave services and positions available. |
| `resume --run PATH` | Explicit recovery through captured inputs and retained execution. |
| `teardown --run PATH` | Persist closure intent and start valid cleanup. |
| `shutdown --run PATH` | Pause then stop services after closure. |
| `shutdown --run PATH --force` | Stop despite unresolved obligations and preserve evidence. |
| `report --run PATH --format html --output FILE` | Offline shared report and optional custom hook. |

`resume` returns once the supervisor accepts it. Admission, the gateway start and
the coordinator start then run in the background: status reports
`provisioning: true` until they finish and then the resulting stage, such as
`running`, `action_required` or `failed` with its diagnostic. Other commands
except `pause` are refused meanwhile.

Production admission reports every gap in one diagnostic. When funding is
missing and Safe permissions are not installed, the stage is `action_required`
with both reasons, and the owner transactions are already exported under
`private/deployment/permissions/`.

Ctrl-C on a foreground run requests pause and waits for acknowledgement. Pending
work and open positions remain recorded. A failed coordinator is not restarted
automatically. Its diagnostic is the refusal the coordinator reported, such as an
`inventory` shortfall or a `gas_limit` or `gas_budget` refusal naming the
requested amount, the estimate or quote and the configured limit, and status keeps
reporting it after the supervisor exits. A coordinator that stopped without a
reported refusal leaves the generic instruction to inspect its log. A recorded
transaction revert (`execution_failed`) or accounting issue (`accounting`) blocks new
exposure on every resume, so that stop is the failure `teardown_required` and its
next command is `almanak teardown --run PATH`: teardown mode passes the block,
observes again and plans the closes afresh. A market fee
quote above `max_gas_price_wei` (`gas_price`) or `max_transaction_fee`
(`gas_fee_quote`) does not stop a single-chain coordinator: it signs nothing, keeps
its decision and quotes again with a back-off of up to a minute, for as long as the
market stays above the ceiling. While it holds, status stays `running` and the
diagnostic names the kind and the policy field; the coordinator log has a
`gas_ceiling_hold` line per attempt with the quote, then `gas_ceiling_recovered` once
a plan is admitted or `gas_ceiling_dropped` when the decision is dropped. On a
multi-chain deployment the refusal is the chain's retained fault and the chain is
retried on each pass. `almanak test run` stops on such a hold at once. Explicit
resume preserves pending identities and performs the
existing lease, signer-claim and receipt audits. Never remove a journal to clear
a pending transaction.

A strategy that declares [price series](authoring.md#price-series-and-technical-analysis)
needs no additional service or credential: the gateway reads each series from the
chain reader of the series' chain, which the deployment must route. Provisioning
refuses a series on a chain the deployment has no provider for, or whose connector
has no reviewed deployment there; at launch the gateway resolves each declared pool
and refuses to serve one that does not resolve or does not hold the declared asset.
Before each decision the gateway reads the series as of the earlier of the decision
time and the observed block time, and returns only the series the frozen
preparation declared. Boundary blocks, points and their evidence are retained in
`strategy-prices.sqlite` beside the journal, so a decision reads only boundaries it
has not seen, and each served series, holds included, is appended there. A fork
reads history before its fork block through its node, which forwards those reads to
the upstream provider. Single strategy prices are a backtest-only Aave V4 input and
are not served. A failed read at a decision records a hold for that decision;
teardown decisions read no series.

The gateway holds the journal's write lock only to commit a request. Provider
reads and remote signing run against the checkpoint read under the current lease;
the commit re-checks the lease and refuses a recording whose checkpoint changed
meanwhile, and a broadcast re-checks its envelope, hash and nonce and sends only
under the lock. Renewals therefore never wait behind a slow plan or signature.
Replacement families keep the lock for their whole request. Each
`gateway_request` log line reports provider calls by method (`rpc_calls`,
`rpc`), HTTP exchanges with the provider (`rpc_round_trips`, a batch counting
once), provider, signer and lock time (`rpc_ms`, `signer_ms`, `lock_wait_ms`,
`lock_held_ms`) alongside `duration_ms`.

Fork fixtures fund each declared asset with simulated capital. The chain's
wrapped native asset is funded by wrapping and transfer, and nothing else is
tried. Every other ERC-20 goes through an ordered ladder: a reviewed
token-specific funder, a transfer from a listed holder, a known balance slot or
seed recipe, `anvil_dealERC20`, then a probe of common balance slots unless the
token has a seed recipe. A token with a reviewed funder or a listed holder never
falls through to the generic routes; a holder short of the amount at the fork
head (after any earlier fixture transfers) is refused. Funding refuses a token
address with no contract code.
Each ladder route must leave the exact balance and pass a transfer check: the
account sends part of it to an address derived from the account and token, which
must start empty and receive exactly that amount, and the fork reverts the check.
The storage routes (`known_slot`, `seed_recipe`, `deal_erc20`, `probed_slot`)
write the balance without minting, so `totalSupply` is left unchanged; such a
route is refused when `totalSupply` is below the funded amount. A failed route is
rolled back before the next one runs. When no route passes, the fixture refuses
and names each outcome; nothing is partially funded. If `anvil_dealERC20` is
still running after 20 s, the node keeps tracing in the background, so funding
stops without rolling back and the fork must be recreated. In the compiled
fixture plan every non-wrapped ERC-20 has `method: local_storage`, which means
"any ERC-20 route", not a storage write. The `simulated_initial_capital` rows in
`fixture-installation.json` record the route actually used in `method`
(`wrap_native`, `reviewed`, `holder_transfer`, `known_slot`, `seed_recipe`,
`deal_erc20` or `probed_slot`) with its `slot`, `seed` or `holder`, so a fork
pass that relied on unreviewed funding is visible.

Fork resume verifies the original process and Anvil instance, chain and origin.
A stopped fork cannot be recreated as the same deployment. Interrupted fixture
funding reverts only through the retained snapshot token of the original node;
an uncertain rollback, a lost node, or execution state without its preparation
is refused. A completed fixture is reused and never rolled back. Production
provisioning writes reviewable artifacts and performs no owner actions.
Multi-host supervision is unsupported.

## Fork tests

`almanak test config` writes a fork operator file from the author configuration,
with the shape of the shipped `examples/operators/*.fork.yaml`:

```sh
uv run --locked almanak test config ./my-strategy --env-file /private/providers.env \
  --output /private/operator/my-strategy.fork.yaml [--block N] [--force]
```

It names one Alchemy provider per authored chain through `ALCHEMY_API_KEY`,
placeholder account bindings, anchors at the latest block (or `--block`, for a
single-chain strategy; a chain with a GMX market anchors at least three hours behind the
head, so the authentic keeper reports a fork test replays exist for its requests), Safe/Roles fixture funds sized from an online fork check's
`funding` block plus 1%, one fixture operator per chain and gas policy holding at
least one native unit and twice its entry minimum plus gas budget, and `fork.gas`
ceilings equal to the authored `strategy.gas`. On Base and Optimism, where forks
cannot bound the L1 data and operator fees and authored ceilings are refused, the
file sets `fork.author_gas: unenforced` and carries a note: the fork runs gasless and
the authored policy is retained but not enforced. An operator file without that
choice keeps the refusal. An existing `--output` is replaced only with `--force`.

`almanak test run` runs one supervised fork lifecycle through the same workspace
`run --fork` creates:

```sh
uv run --locked almanak test run ./my-strategy \
  --gateway-config /private/operator/my-strategy.fork.yaml \
  --env-file /private/providers.env --output /private/runs/test-1 \
  [--decisions 3] [--advance SECONDS] [--through-exit] [--param KEY=VALUE ...] \
  [--production-parameters] [--json]
```

It waits for admission, then lets up to `--decisions` decisions settle. Between
them it advances every fork node's time by the decision interval (or `--advance`,
raised to the next due decision when shorter, so it never waits on the wall clock)
with `evm_setNextBlockTimestamp` and `evm_mine`, and moves the coordinator's
decision clock by the same offset; the author's interval is never edited. It stops
early once an intent that increases exposure has executed, unless `--through-exit`
keeps it deciding up to `--decisions`, so the strategy's own later intents run: the
mint after an LP entry swap, fee collection, a recentre, its own close or sell. Size
`--advance` so those fall within the decisions, e.g. past a hold time. After the entry,
`--idle` consecutive holds (default 3) end the decisions early: a short run of holds is a
strategy waiting on its own timer, a longer one means it has nothing left to do. It then requests teardown,
waits until the deployment is closed, shuts the services down and releases the
run's fund claims. Interrupts and failures still stop every recorded service.

The config's [`strategy.test_parameters`](configuration.md#strategy-parameters)
replace their declared `strategy.parameters` values in the workspace's copy of the
inputs before admission, unless `--production-parameters` is given. Each
`--param KEY=VALUE` (repeatable; split at the first `=`, the value read as one YAML
scalar, so `1` is an integer, `true` a boolean and `'"0.5"'` a string) replaces one on
top of them. Each takes the declared type: a string knob keeps the text as written
(`1.10` stays `'1.10'`), a float knob takes any finite number (`0.00001`, `1e-5`), an
integer refuses `_` and exponents, and `null` is refused unless config.yaml declares
`null`. With a [typed model](authoring.md#typed-parameters) the model converts it
instead. An undeclared key
refuses the run as an input error. `config.yaml` is never modified.

Teardown never admits an intent that increases exposure; the coordinator drops it and
logs `exposure_refused_in_teardown`. A strategy that proposes one 3 times with no
teardown outcome between them ends the wait as `teardown_refused` (class `strategy`),
with the intent kind and allocation in its `details`.

While teardown waits, a fork whose journal has stalled for 30 seconds has its time
advanced, except while a decision is retained without a plan, attempt or an unresolved
request of its own (a completed GMX request exempts nothing): the
gateway prepares that decision's plan again against the chain before it reserves it,
and a block mined in between would refuse it as a plan that differs from verified
preparation.

A fork runs no protocol keeper, so a GMX order, deposit or withdrawal would stay
pending. While the run waits on an accepted GMX request it settles it the way a
keeper would, with nothing synthesized: it reads a keeper execution mined on the
origin chain after the request was created, impersonates that keeper (which already
holds the role at the fork origin) and submits its unchanged signed Chainlink Data
Streams payloads to the fork's handler, at the reports' time. GMX's contracts then
verify the signatures, report times, acceptable price and triggers. A request with no
eligible report within its expiration stops the lifecycle with that reason; the source
search is retained under `keepers/` in the workspace.

Keepers publish when somebody's request is pending, so on a quiet market they can be
minutes apart, longer than GMX's 300-second request expiration. Because the fork runs
behind the chain head, the run knows the coming publications and times the fork by
them: `test config` starts a single-market GMX fork where usable publications stay
under four minutes apart for fifteen minutes after admission, and each later advance
(the `--advance` step, the start of teardown and its stall advances) is lengthened when
needed so the decision lands 120 to 280 seconds before a usable publication: its
request follows the decision's approvals, and its reports must come after the request
and within GMX's 300-second expiration of it. This only
chooses when the fork acts; the strategy's interval and every report are unchanged.
The verdict is printed and retained as `test-verdict.json` in the new workspace; an
existing `--output` is refused and keeps its earlier verdict:

| Field | Meaning |
| --- | --- |
| `schema` | `almanak-test-verdict-v1` |
| `workspace` | The fork workspace directory, or null when the inputs were refused |
| `parameter_overrides` | The applied parameter values by key, in the declared type, or with a typed model as written for the model to convert: `strategy.test_parameters` (unless `--production-parameters`) with any `--param` on top; `{}` when none applied or the inputs were refused |
| `parameter_sources` | Per key in `parameter_overrides`, where its value came from: `test_parameters` or `--param` |
| `admission` | `ok`, and the `refusal` (an input error document, or the failed stage and its diagnostic, with the supervisor's failure kind as `error` when it recorded one), classified like a failure |
| `decisions` | Per decision: `time`, `result` (`hold` or `intent`) and the `intents` kinds; `settled: false` marks a decision the lifecycle stopped on before its work settled. From the `decision_evaluated` line the test run's coordinator logs (only `almanak test run` opts its coordinator in; production deployments never log it): `evaluated` (false when an unavailable price series held the decision before the strategy ran, with `price_error`), `observed` allocations, `price_series` (per declared series: `asset`, `complete`, `points`, `window`, `latest_at`, `age_seconds`; pool-sourced series also carry `timeframe`, `gaps` and `max_age_seconds`) and `availability` (chains not ready, with their fault). On a multi-chain deployment `chain` names the chain whose tick evaluated the decision; a row is given facts only when they are unambiguously its own. `observation_errors` lists each `*_error` field an observation recorded instead of a measurement, as `allocation`, `field` (its dotted path in the observation) and `message`; an `entry_quote_error` there means the strategy saw no entry to take, for example on an empty pool. These are facts, not the strategy's reason for a hold |
| `transactions` | Per executed transaction: `trigger` (`decision`, or `teardown` for teardown's own transactions), `action`, `allocation`, `status`, `gas_used`, `via_roles`, `transaction` |
| `entered`, `closed` | Whether an entry executed and the deployment closed. A request the connector settled unsuccessfully, such as a GMX order its keeper cancelled, is not an entry |
| `evidence_clean` | The final journal holds no pending or reserved attempt, undelivered outcome, accounting issue, integrity error or chain fault |
| `executed` | Intent kinds that executed completely and successfully, in first-execution order; teardown's are not included, nor an intent whose request was cancelled |
| `after_entry` | The `executed` kinds that ran after the first entry: what `--through-exit` exercised beyond it |
| `teardown_executed` | Intent kinds teardown executed completely, in first-execution order: an exit the strategy never ran itself was still exercised when it is listed here. Each kind is listed once; `teardown_sequence` has every execution |
| `teardown_sequence` | Every intent teardown executed completely, in order and with repeats, as `intent` and `allocation`: two exit swaps around a withdrawal are two rows |
| `teardown_decisions` | Every teardown decision the coordinator evaluated, in order: `time`, `result` (`hold` or `intent`), the `intents` kind and the same facts as `decisions` rows (`evaluated`, `observed`, `observation_errors`, `availability`). In teardown a hold claims that nothing is left to do; when the last row holds, `closure` is `closed`, or `not_closed` with the teardown failure's kind as `refusal` (such as `remaining_obligations`, whose `failures` entry lists what stayed open) |
| `in_flight` | After an interrupt, each decision the journal still held unsettled: its `trigger` (`interval`, `teardown`, …), `intent` kind, `allocation` and the broadcast `transaction`, null before broadcast. Empty otherwise. `transactions`, `teardown_executed` and `closed` are read from the journal at the interrupt, so teardown work done before it is reported |
| `repayment` | The runtime's latest repayment coverage per borrowing allocation, as at closure: `allocation`, `asset`, `block`, `debt`, `available`, `shortfall` and the labeled rate `projection`; empty when no debt was measured |
| `holdings` | Per fund account, its `balances` once teardown ends (after an interrupt, at the interrupt: `closed` is false and `in_flight` shows what was pending), read from its fork node: each configured asset on its chain with `asset`, `symbol` when known, `amount` in base units and the decimal `quantity`, zero balances included. These are whole on-chain balances, the fixture's 1% funding buffer included; compare them with the asset the exit should leave the fund in. `error` replaces balances that could not be read |
| `keepers` | Per request a fork keeper settled: its `request` key, `kind`, `chain`, the replayed `keeper` and its `source_transaction` and `source_block` on the origin chain, the `report_times`, the fork `transaction` and its `status` (`mined` or `reverted`, with the `revert` reason), and the fork time it `advanced_seconds`. A mined execution whose receipt carries the request's lifecycle event also names the `request_status` GMX gave the request (`executed`, `cancelled` or `frozen`); a cancellation adds its `reason` (GMX's reason string, else the error name for known price and output bounds, else the raw bytes) and `reason_bytes`. A keeper transaction mines whether GMX executed or cancelled the request |
| `failures` | Stage and message of anything that stopped the lifecycle early, each with its `kind`, `class` and `transient`, and the refusal's structured `details` when it has them: a `remaining_obligations` teardown refusal lists `details.remaining`, one row per allocation that still holds something, with its `allocation`, `asset` and each non-zero amount as a decimal string (`supply_scaled`, `debt_scaled`, `collateral_enabled`, `allowance`, `user_configuration`, `supply` and `debt` on Aave and Spark; `principal`, `collateral`, `allowance`, `supply` and `debt` on Compound; `supply_shares`, `borrow_shares`, `collateral`, `allowance` and `debt` on Morpho) |
| `failure_class` | The `class` of the admission refusal, else of the first failure; null when nothing failed |
| `passed` | Admission passed, the deployment closed, the evidence is clean and nothing failed |
| `status` | `entered` when `passed` and an entry executed, `not_entered` when `passed` but no entry executed: the strategy only held, or its entry's request was cancelled (admission and teardown alone were exercised), else `failed` |
| `duration_seconds` | Wall time of the whole command |

The exit code is 0 exactly when `passed` is true.

While it runs, the test run's coordinator log (`logs/coordinator.log` in the fork
workspace) is a progress stream of JSON lines; production coordinators
never write the three below. Once, when the coordinator starts, a `run_metadata` line
names what the run's amounts and venues read as, from the resolved configuration:
`assets` maps each verified CAIP-19 asset to its `symbol` and `decimals` (a chain's native
token carries decimals only) and `venues` maps each allocation to its connector. Each
`decision_evaluated` line also carries `reason`: the text
the strategy passed as `context.decision(..., reason=...)` with a hold or an intent,
cut to 300 characters, or null when it stated none. After each transaction outcome is
committed to the checkpoint, a `transaction_executed` line states it: `time` (the
coordinator clock at commit), `allocation`, `operation` (the connector's operation
kind, such as `approve`, `swap`, `supply`, `borrow`, `repay`, `withdraw`, `revoke`,
`mint` or `burn`), `success`, `tx_hash`, `gas_paid` (`asset` and base-unit `amount`
paid by the submitter), `changes` and `revert_reason` (cut to 300 characters, or
null). `changes` lists the fund's nonzero token balance changes as CAIP-19 `asset`
and signed base-unit `amount` strings, negative when spent: the connector's
receipt-derived execution effects, the amounts the fund inventory is updated with.
Approvals, revokes and reverted transactions have none, and a connector whose effects
cannot be read logs none rather than an estimate. A coordinator stopped between the
commit and the line loses that line; it never logs a transaction it did not commit.

Each refusal and failure carries a structured `kind`, taken from its source rather
than its message: the error kind of a refused input, the supervisor's failure kind,
the error kind of the refusal a stopped coordinator logged, the kind of a fork keeper's
data error, or else the stage or condition that ended the lifecycle (`decision_timeout`,
`admission_timeout`, `coordinator_stopped`, `keeper`, …). Its `class` is `infra` for
the kinds whose cause lies outside the strategy (`rpc`, `http`, `transport`,
`unavailable`, `gateway_unavailable`, `timeout`, `rate_limit`, `fork_unavailable`,
`unsupported_on_fork`, `admission_timeout`, the coordinator holds that retry without
stopping, `strategy_worker_timeout` and `transient_read_hold`, and the stops a wall
clock or the harness caused: `interrupt`, `decision_timeout`, `teardown_timeout`,
`worker_timeout` (the preloaded strategy worker did not answer during setup, recorded
with the supervisor's retryable `start` failure)) and `strategy` for every other kind,
including one not listed, and for a contract revert, which keeps the `rpc` kind but
is marked `reverted` by the gateway. `transient` is true only for an `infra` failure its source
marked transient, one the same run can pass unchanged; `decision_timeout`,
`teardown_timeout`, `worker_timeout`, `strategy_worker_timeout` and
`transient_read_hold` are, `interrupt` and `unsupported_on_fork` never are.

A coordinator that keeps a `strategy_worker_timeout` or `transient_read_hold` hold
(its strategy worker timing out on a starved host, or a provider read the gateway
marked transient) retries without stopping, so the test run reads its log: once the
hold has lasted 300 seconds without its recovery event, the decision or teardown wait
stops with a failure of that kind, its reason in the message and the hold's `kind`,
`attempt` and `elapsed_seconds` as `details`. A briefer hold that recovers does not
stop the lifecycle. When a wait does run out, a hold the coordinator is still on,
however brief, names the failure instead of `decision_timeout` or `teardown_timeout`;
without one the timeout keeps its own kind. After a lifecycle that settled no decision,
teardown waits at most 180 seconds instead of 900, so the verdict is written within a
20-minute budget; its `teardown_timeout` message says the wait was bounded.
A caller can therefore re-run once only when `failure_class` is `infra` and that
failure is `transient`, without reading the message.

A local fork can only execute EVM bytecode. Contracts a chain's node implements
natively, such as Base's B20 stock tokens whose code is the single byte `0xef`,
resolve on mainnet but fail every call on the fork. `check --fork --online` refuses
them in its `fork_compatibility` stage with code `unsupported_on_fork`. Fork setup
checks the fork nodes before any funding, and again when it reuses a funded fixture,
and stops `failed` with the failure `unsupported_on_fork`, naming every such contract.
No retry or resume can pass it, so status offers `shutdown` instead of `resume`,
`almanak run` exits 1 and the verdict's refusal carries `"error": "unsupported_on_fork"`.
This is a limitation of local fork simulation, not of the strategy, which can still be
exported and deployed.

Parameters the strategy's own `validate_config` refuses (see
[authoring](authoring.md#declaring-capabilities)) stop setup `failed` with the final
failure `invalid_config` and the author's message and line as the diagnostic. Status
offers `shutdown`, hosts do not retry it, and the verdict's refusal carries
`"error": "invalid_config"` with `class` `strategy`. `check --online` fails its
`strategy` stage with `"error": "invalid_config"` and code `strategy_config_refused`.

A fork node starts at the fork block's own base fee without Anvil's 1 gwei minimum
tip, so its gas quote follows the forked chain's market and a mainnet-calibrated
`strategy.gas` fits. On chains whose fee history reports no tips (BNB), the node
starts at the upstream gas price instead.

## Production

```sh
uv run --locked almanak wallet create --account treasury --output /private/operator
uv run --locked almanak check ./my-strategy --production --online \
  --gateway-config /private/operator/production.yaml --env-file /private/providers.env
uv run --locked almanak run ./my-strategy --production \
  --gateway-config /private/operator/production.yaml \
  --env-file /private/providers.env --output /private/runs/live
```

For Safe/Roles accounts the production online check also reports a
`permissions` block: per Safe, whether its authority is installed and, when it
is not, the Safe Transaction Builder batch the owner executes. It is generated
exactly as admission generates it, so the owner can install permissions before
the run exists. Admission still generates and verifies the installed authority.

The author file carries the economic gas policy under `strategy.gas`. Resolution
retains this policy in every mode. Identified forks enforce the same economic
bounds through explicit operator `fork.gas` ceilings and generated fixture payers.
Missing or incompatible ceilings are refused before fixture setup. Fork runs on
Base and Optimism refuse authored gas caps because their L1 data and operator
fees have no verified bound on a fork; gasless forks are unaffected. Production
runs on those chains accept authored caps: the gateway bounds the L1 data and
operator fees at quote time within the payer ceilings and reconciles the exact
charges from each receipt. Fee-only replacement
remains opt-in and limited to its supported Ethereum execution path.
The operator file carries the production authority: execution networks, independent
payer ceilings that the authored policy must fit, the funded account bindings and
the signing references. The online check verifies policy compatibility through
the read-only gateway and never opens signing files. EOA funds are their own
submitters. Safe funds use delegated Roles operators; owners remain distinct and
are never a fallback signer.

`wallet create` asks for the keystore password twice. Without a terminal, name
the password with `--password-file` (an operator-owned private file, which is not
copied), or pass `--generate-password` to have one generated into
`keystore-password` beside the keystore. Either way the printed result and
`wallet.json` name the file as `password_file`, so a later command can unlock the
keystore without a terminal. The result never contains the password or the key.

Encrypted keystores must match the named submitters. Interactive passwords are
passed ephemerally and are not saved in manifests. An explicit private
`password_file` in the operator signing section is an operator choice. A gateway
restart requests a password when required. Credentials must be operator-owned
private files outside sandbox readable paths.

An operator may replace `--key-file` with `--signer-file`, a private JSON
binding whose `kind` names an installed signer plugin (entry-point group
`almanak.signers`). The gateway then holds no key material: every envelope a
remote signer returns is decoded and checked against the requested transaction
and the bound submitter before it is broadcast or retained. The two flags are
exclusive and a remote signer takes no keystore password.

Missing Safe permissions are exported for owner review and installation. The
supervisor reports action required and does not install them. The Safe must be a
canonical Safe 1.4.1 (L1 or SafeL2) whose only module is its Roles 2.1.0 proxy, with
no guard and either no fallback handler or Safe's CompatibilityFallbackHandler
1.4.1. When the operator's role was assigned in the Roles setup transaction, the
exported bundle does not assign it again. After owner action,
explicit resume verifies installed authority before admitting execution.
Installed authority is read from Roles storage at a pinned block: every grant the
strategy needs must be installed exactly, with the operator in the role
and no default role. Grants beyond the strategy's own, other members of its role
and other roles of the operator are not detected; Safes are dedicated to one
strategy. The authority check before every plan, signature and broadcast replays
the previous check's block-pinned reads in batches at its own observation block.
Production advances on canonical mined receipts and audits recorded canonicality
before further work. Invalidated history halts with pending evidence retained.

## Reporting and advanced commands

Text is the default on terminals; redirected output is JSON. Override with
`--format text`, `--format json` or `--json`. Offline reports never obtain
credentials or contact a provider. They distinguish retained status from a fresh
online audit and preserve fund/payer scope, unavailable values and simulation
assumptions. Custom dashboard failure remains visible without hiding the report.

Explicit process management remains under `service gateway`, `service fork`,
`service start`, `service resume`, `service status` and `service teardown`.
`permissions generate|verify` uses an authenticated gateway.

### Operator actions

`almanak ax supply`, `withdraw`, `borrow`, `repay`, `collateral` and `swap` each run
one action for one wallet in one process, with no strategy, workspace or gateway
process to start. The lending commands:

```bash
almanak ax supply --connector aave_v4 --chain eip155:1 \
  --spoke 0x94e7a5dcbe816e498b89ab752661904e2f56c485 --reserve-id 0 \
  --amount 10000000000000000 --wallet /private/operator --env-file /private/providers.env \
  --max-gas-price-wei 5000000000 --max-transaction-fee 5000000000000000
almanak ax withdraw --connector compound_v3 --chain eip155:8453 \
  --comet 0xb125e6687d4313864e53df431d5425969c15eb2f \
  --asset eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 \
  --wallet /private/operator --public-rpc base \
  --max-gas-price-wei 1000000000 --max-transaction-fee 200000000000000
```

The market is named with the flags of `ax lending-reserve`. What each lending
connector serves:

| Command | Does | Connectors |
| --- | --- | --- |
| `supply --amount N` | Supplies the asset from the wallet. | all five |
| `withdraw` | Withdraws the whole supplied balance, with interest. | all five |
| `withdraw --amount N` | Withdraws that much and leaves the rest. | Aave V3, SparkLend, Compound V3, Morpho Blue |
| `borrow --amount N --min-health-factor H` | Borrows the asset to the wallet against the collateral it has supplied. | all five |
| `repay` | Repays the whole debt, with interest. | all five |
| `repay --amount N` | Repays that much. | Aave V3, SparkLend, Compound V3, Morpho Blue |
| `collateral --enable` or `--disable` | Sets whether the supplied balance backs debt. | Aave V3, SparkLend |

`almanak support` lists the intent kinds each chain serves under `direct_actions`;
a partial withdrawal is `withdraw_amount` and a partial repayment `repay_amount`.
A form a connector does not serve is refused before any read. On Compound V3 the
borrowed and repaid asset is the Comet's base asset, and on Morpho Blue it is the
loan leg.

Without `--execute` the command resolves the market, reads the wallet's current
position, plans the first step and prints it; nothing is signed, and the keystore is
not opened. With `--execute` it signs and sends each step, waits for its receipt and
prints the outcomes with the position before and after. A supply is an approval
followed by the supply, so the preview shows the approval.

An executed action reports `completed`, true only when the whole request finished,
and the command exits 1 when it is false. A supply is not atomic: its approval can
be mined and the supply then be refused, for example on a fee quote above the
ceiling, or revert. The position is then unchanged and the allowance stays on chain;
`outcomes` lists the steps that ran and `refused` names the refusal. A failure that
is not a refusal prints the steps already sent as one
`{"event": "steps_sent", "outcomes": ...}` line on standard error before its error.
A step that was signed and has no reconciled outcome yet is reported apart from
`outcomes`, as `unresolved` with its operation and transaction hash; it may or may
not have been sent, and `executed` is true for it. A first step refused at planning
fails the command with that refusal and signs nothing.

Standard output carries one JSON document, the result. Events and the error are
JSON lines on standard error. While a signed step is unresolved, a transient
provider or storage failure is held and asked again instead of ending the command.
An outcome lists `accounting_issues` when the step does not explain a wallet movement.

`position_before` and `position_after` state the requested position in the
protocol's own units, as strings; `ax lending-position` reports amounts in assets.

| Connector | Position fields |
| --- | --- |
| `aave_v4` | `supplied_shares`, `debt_shares` |
| `aave_v3`, `sparklend` | `supplied_scaled`, `debt_scaled` (scaled token balances), `collateral_enabled` |
| `compound_v3` | `asset`, `base_principal` (the Comet's signed principal: negative is debt, zero for a collateral asset), `collateral` (base units, zero for the base asset) |
| `morpho_blue` | `leg`, then `supply_shares` and `borrow_shares` on the loan leg or `collateral` (base units) on the collateral leg; `other_leg` in the same form when the wallet holds it |

The deployment covers the wallet's whole position in the venue, read when the command
starts, not only the requested market:

- An action the Pool risk-checks on Aave V3 or SparkLend binds every reserve of the
  Pool that the account uses as collateral or has borrowed, each with the price
  source the Pool uses for it now, because the risk check must account for all of
  them. A repayment, and a withdrawal the Pool does not risk-check, bind their own
  reserve only.
- Compound V3 binds the base asset and every collateral asset the wallet holds in
  the Comet, and Morpho Blue binds the other leg of the market when the wallet
  holds it.

A position that appears in the venue between that read and the first step is refused:
with `ownership_mismatch` on Compound V3 and Morpho Blue, and a risk-checked action
on Aave V3 or SparkLend with `risk_unavailable`. Running the command again reads it. A supply into
a reserve that is isolated collateral, or by a wallet that holds isolated collateral
in the Pool, is refused on Aave V3 and SparkLend. A Compound V3 Comet
with many collateral assets takes many reads, so a preview through a public endpoint
can take minutes.

`--min-health-factor` is the lowest health factor the action may leave the account
with, a decimal above 1, and one floor serves the wallet's whole position in the
venue. The connector projects the account's health after the step and refuses the
step below the floor; it is a check before signing, not a limit on chain. A borrow
always states one. Any other action a connector projects is refused without one
while the wallet has debt in the venue, and the refusal says to state it:

| Connector | Projected against the floor |
| --- | --- |
| `aave_v3`, `sparklend` | a borrow, a supply, a collateral change, and a withdrawal of a reserve used as collateral |
| `compound_v3` | a borrow, and a withdrawal of collateral, in full or in part |
| `morpho_blue` | a borrow, and a withdrawal of collateral in part |
| `aave_v4` | a borrow |

A repayment is never projected, so no floor can refuse it, and it takes no
`--min-health-factor`. Aave V4 refuses a floor on a supply or a withdrawal, which it
would not check. Stated on any other action a connector does not project, the floor
has no effect. Aave V3 and SparkLend project a supply and enabling collateral too,
so an account already below the stated floor is refused even those: state a floor
the account meets.

What a connector does not project it refuses while the debt is open: Morpho Blue
refuses to withdraw collateral in full, and Aave V4 refuses to withdraw a reserve
that is enabled as collateral while the account owes anything on the Spoke. The Aave
V4 check runs again when the step is planned and when it is signed, so a debt that
appears after the command's first read still refuses the withdrawal; what the chain
changes between signing and inclusion is not seen. Repay first. On Compound V3 the
last collateral asset
cannot leave either, since nothing would cover the debt. A supply of the
Compound V3 base asset while the wallet owes it is refused, since it would repay the
debt: use `repay`. A withdrawal of the base asset above the supplied balance is
refused too, since it would borrow: use `borrow`.

On Aave V4 a supply backs nothing until it is enabled as collateral. A borrow
enables each supplied reserve that can back debt first, one `collateral` step each,
so a preview of a first borrow shows a `collateral` step and has not yet tested the
floor: the floor is projected when the borrow step itself is planned. A borrow the
floor refuses can therefore leave its collateral steps mined; a repayment with no
debt releases them, and a withdrawal works either way. A borrow is served for one
debt reserve per wallet and Spoke. A repayment is served for any reserve the wallet
owes: it disables the wallet's collateral after the debt is gone only when no other
debt remains on the Spoke, and otherwise ends with the revocation.

A full repayment approves the wallet's whole balance of the debt asset, repays with
the protocol's full-repay form and revokes what is left of the approval, so the
wallet must hold the whole debt with its interest; a shortfall is refused with the
amount missing. When it stops after the repayment and before the revocation, the
approval of the rest of that balance stands: `repay` again revokes it. On Compound
V3 that works while the wallet still holds collateral in the Comet; otherwise the
refusal names the allowance, and `withdraw` on the base asset revokes it.

A supply plans against the wallet's allowance to the protocol: zero, or exactly the
requested amount left by an earlier approval. Any other allowance is refused, and
the refusal says so. Retry a half-finished supply with the same amount, or clear the
allowance first. A withdrawal clears it: when an allowance remains, the withdrawal
is followed by a revocation, and with nothing supplied the revocation is its only
step.

Each fee ceiling applies to one transaction, and an action can take several: a
supply two, a full repayment three, an Aave V4 borrow one for each collateral reserve
it enables and one more. So an action can spend `--max-transaction-fee` once per
step, and a preview shows only the first. A step bids as an author's
[gas policy](configuration.md) defaults on the chain: the quoted gas price is twice
the base fee plus the tip, or 1.25 times the base fee plus the tip on Arbitrum and
Robinhood Chain, so `--max-gas-price-wei` refuses a step once the base fee passes
that share of it. On Avalanche the base fee is the observed block's, because its fee
history may not report the next one. The gas limit is the estimate with a 25% buffer
and at most `--max-gas-limit`, 1,200,000 unless stated. Reads are no older than 120
seconds.

| Option | Meaning |
| --- | --- |
| market selection | The same flags or `--config` file as `ax lending-reserve`. |
| `--amount` | Base units of the asset. Required for `supply` and `borrow`; without it `withdraw` and `repay` take the whole balance or debt. |
| `--min-health-factor` | The lowest health factor a projected action may leave, a decimal above 1. Required for `borrow`; `repay` takes none. |
| `--enable`, `--disable` | `collateral` only: one of the two. |
| `--wallet` | The directory `almanak wallet create` wrote, under any account name. The keystore is unlocked with the recorded `password_file` or by asking. |
| `--env-file` | The provider key (`ALCHEMY_API_KEY`). |
| `--public-rpc` | A named public endpoint instead of `--env-file`. |
| `--max-gas-price-wei`, `--max-transaction-fee` | Required ceilings; a step quoted above either is refused. |
| `--max-gas-limit` | Optional gas limit ceiling per step; a step whose buffered estimate is above it is refused. |

Each connector declares the actions it serves directly, and `almanak support` lists
them per chain as `direct_actions`. A command for a connector, chain or action that
is not declared is refused before any read, and the gateway refuses the same request
again when it admits it and when it plans a step. Operator actions run for an EOA
wallet; Safe/Roles accounts are not supported. Morpho Blue on HyperEVM, GMX V2 and
Hyperliquid serve no direct action.

#### Swaps

`almanak ax swap` sells an exact amount of one pool currency for another, in one
named pool, on every chain where `almanak support` lists `swap_exact_input` under
`direct_actions`:

```bash
almanak ax swap --connector uniswap_v3 --chain eip155:8453 \
  --pool 0xd0b53d9277642d899df5c87a3966a349a798f224 \
  --sell eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 \
  --amount 25000000 --slippage-bps 50 \
  --wallet /private/operator --public-rpc base \
  --max-gas-price-wei 1000000000 --max-transaction-fee 200000000000000
```

| Connector | Names its pool with | Also takes |
| --- | --- | --- |
| `uniswap_v3`, `pancakeswap_v3`, `aerodrome_slipstream`, `aerodrome` | `--pool <address>` | |
| `uniswap_v4` | `--currency0`, `--currency1`, `--fee`, `--tick-spacing` and, for a reviewed hook, `--hooks` | |
| `curve` | `--pool <address>` | `--buy <asset>`, required: a Curve pool can hold more than two coins |

`--sell` is the CAIP-19 asset sold and `--amount` its base units. The native coin is
never named: where a Uniswap V4 pool key holds it, the wallet holds and sells its
wrapped token, and the router wraps and unwraps inside the swap. The pool itself is
resolved by its connector, which refuses an unreviewed deployment or hook.

`--slippage-bps` is required, 1 to 9999. The swap is quoted on the pool at the
observed block, and its minimum output is that quote less the slippage; the swap
reverts on chain below it. The quote is the pool's own price at that block. Nothing
compares it with another venue, so a pool whose price was already moved quotes that
price. `--deadline-seconds`, 300 unless stated, is how long after the observed block
the swap may still be mined. Curve calls carry no deadline: there it bounds only how
long after the observed block the swap may be signed, and a signed swap stays valid
until it is mined or its nonce is used, protected by its minimum output alone.

The wallet must hold the amount. `position_before` and `position_after` list the
`inventory` of the pool currencies the action owns, the amount sold and then what
it bought, and the nonzero `allowances` left to the venue:

| Connector | Steps | Allowance after a completed swap |
| --- | --- | --- |
| `uniswap_v3`, `pancakeswap_v3`, `aerodrome_slipstream`, `aerodrome` | `approve` to the router for exactly the amount, after a reset to zero when another allowance stands, then `swap` | none |
| `uniswap_v4` | `approve_erc20` to Permit2, `approve_permit2` to the router until the deadline, then `swap` | none from a wallet with none; allowances that already cover the amount are used as they stand and what the swap does not spend remains |
| `curve` | `approve` to the pool for exactly the amount, then the exchange | none |

A swap refused or reverted after its approval was mined leaves that approval: the
result lists it, and a later swap of the same amount uses it. No command only
revokes an allowance.

`ax recover` resolves an interrupted swap as it does any action. A signed Curve swap
that the pool would now pay below its minimum is neither sent nor released: recovery
reports `signed_attempt_unsendable` and the record and the wallet's claim stay until
the market returns or the nonce is used.

A swap deployment trades one pool and holds no liquidity position. On Aerodrome and
Curve a wallet that holds the pool's LP shares, however few, is refused, as a
trading-only strategy there is. Anyone can send such shares to a wallet, and no
direct command removes them.

The action runs through the same planning, simulation, signing and receipt
reconciliation as a strategy's, as an operator deployment that exists only for the
command: its journal is a private record directory under
`~/.local/state/almanak-v3/actions/`, removed when the command ends. The deployment
adopts the position the wallet already holds, read at the start, so a later command
sees what an earlier one did.

Nothing is kept between finished commands, so an interrupted command is not
resumed. If a command fails, is interrupted or receives SIGTERM or SIGHUP while a
signed step is unresolved, its record is kept. A command that still prints its
result names the record there as `record_kept`; one that fails prints
`{"event": "record_kept", "path": ...}` on standard error. A process that is killed
outright prints nothing, so the absence of both does not mean nothing was kept.
Only one action runs for a wallet at a time, previews included: a second one
started meanwhile is refused the same way and can be run once the first exits.
While a record
holds a signed step the wallet stays claimed and later actions for it are refused
with "fund is already claimed". Checking the transaction on chain does not release
the claim; `almanak ax recover` does. A wallet a running strategy uses is refused
the same way.

```bash
almanak ax recover
almanak ax recover --record ~/.local/state/almanak-v3/actions/<record> --env-file /private/providers.env
almanak ax recover --record ~/.local/state/almanak-v3/actions/<record> --public-rpc publicnode
```

Without `--record` it lists the kept records: each one's `path`, `chain`, `account`
and `unresolved` step. With `--record` it resolves that record's signed step: from
its receipt when the transaction was mined, otherwise by sending the same signed
bytes again and waiting. It plans and signs nothing, and needs no wallet. Without a
transport flag it uses the endpoint the record's action ran on, which needs
`--env-file` when that was a provider key. `--env-file` or `--public-rpc <name>`,
one of them, names another transport, and the record is resolved through that
endpoint of the record's chain instead. The bytes sent are the ones that were
signed, the adapter proves the chain number and the pinned genesis on whichever
endpoint it is given and reads the outcome from the canonical receipt, and a named
public endpoint must also show a head no older than 120 seconds. A record whose
public endpoint has left the [list](#public-endpoints) for its chain is refused
with `the public endpoint <name> this record's action ran on no longer serves
<chain>; name a transport with --public-rpc or --env-file`, and resolves once one
is named. Once the
step has its outcome, whatever the request had not yet signed is dropped
(`discarded_request`), the record is removed and the claim released; state the
action again against the position as it now is. A step that cannot be resolved
leaves `resolved` false with the reason under `refused`, keeps the record and exits
1. A record a running command still uses is refused.

A record with no signed step holds nothing worth keeping. When a command is killed
before it signs, the next operator action removes that record and releases its
claim, once it can read the record and see that nothing is signed. That includes a
journal with no checkpoint yet, left by a command killed while creating it. A
record that cannot be read at all is kept with its claim, and `record_kept` can
name such a record. Do not kill a command that runs with `--execute`: it waits for each receipt
without a time limit, and stops between steps on SIGINT, SIGTERM or SIGHUP.

An action has no amount ceiling: it is bounded by the wallet's balance and the two
fee ceilings.

### Market resolution

`almanak markets --out .almanak/markets.json` reads, at one pinned block per chain (`--block`,
default `latest`; the record's `blocks` names each chain's number and hash, and every market
on a chain resolves at that block), what each market in `config.yaml` resolves to: its contracts, tokens and
ids, each under the resolver field that names it (`pool`, `currencies`, `asset`, `oracle`,
...), with every token's symbol and decimals, for the user to check on an explorer before
approving the strategy. It reads no prices, so it is quick; names that resolve to the same
contracts (two legs of one lending market) share one entry, and a market without a resolver
is listed under `skipped`. It takes the session's read gateway like `almanak ax`.

### Protocol observations

`almanak ax` reads one protocol observation at one pinned block. Every
subcommand takes one transport and its own request arguments after the
subcommand, and every one but the two HyperCore subcommands takes `--block`
(default `finalized`):

| Subcommand | `--connector` | Request |
| --- | --- | --- |
| `pool` | `uniswap_v3`, `pancakeswap_v3`, `aerodrome_slipstream`, `aerodrome`, `curve` | `--chain` and `--pool`, or `--config`; optional `--depth-bps` for the first three |
| `pool-price` | `uniswap_v4` | `--chain`, `--currency0`, `--currency1`, `--fee`, `--tick-spacing` and optional `--base`, or `--config` |
| `lending-reserve` | `aave_v3`, `sparklend`, `aave_v4`, `compound_v3`, `morpho_blue` | `--chain` and the connector's selector flags, or `--config` |
| `lending-position` | same as `lending-reserve` | the same selection, plus `--account` |
| `perp-position` | `gmx_v2` | `--config`, `--account` |
| `perp-markets` | `gmx_v2` | `--chain` and `--query`, or `--config` |
| `balance` | none | `--asset` (CAIP-19 asset type), `--account` (repeatable) |
| `resolve` | none | `--chain`, `--query` (symbol or address); optional `--limit` |
| `dex-pools` | none | `--chain`, `--token`; optional `--quote`, `--connector`, `--limit`, `--offset`, `--depth-bps` |
| `lending-markets` | `aave_v3`, `sparklend`, `aave_v4`, `compound_v3`, `morpho_blue` | `--chain`; optional `--asset` (required for `morpho_blue`), `--collateral`, `--include-unlisted` (`morpho_blue`), `--limit`, `--offset` |
| `hypercore-markets` | none | optional `--query` (a coin), `--limit`, `--offset`, `--include-delisted`; no `--block` |
| `hypercore-account` | none | `--account` (repeatable); no `--block` |

`--config` names a JSON request file whose fields are the connector's request
model; for the lending subcommands it is the market selection an author writes
under `markets:`. `--block` accepts a decimal block number, `finalized` or `latest`; any
other value is refused before a transport opens. The reader first resolves the
selector to one header, and every later read of the observation names that
header's hash with `requireCanonical`, so the output's `block` records the
concrete number and hash all values came from. A `latest` observation can be
reorganized away; a later read against a block that is no longer canonical
fails instead of mixing blocks.

On HyperEVM (`eip155:999`), `finalized` is the head, and Alchemy runs a read
pinned within about 128 to 192 blocks of its head at the next block. There a
`finalized` or `latest` selector pins the block 256 blocks (about four minutes)
behind the tag's block, for `almanak markets` too, and the output names that
block. Every HyperEVM block is proved before its first read, so a read the
provider still runs at another block is refused as `provider_context`; an
explicit number is pinned as given.

Each observation reads through exactly one transport: `--env-file` resolves the
provider credentials in the calling process, `--public-rpc` names a public endpoint
that needs no key, or `--gateway-url` with
`--gateway-auth-file` reads through `service read-gateway`, a read-only
observation gateway that keeps the credentials in its own process and serves
only whitelisted JSON-RPC reads and the discovery lookups below over
authenticated loopback. Sessions that must not receive a provider key use
the gateway transport. Without a flag, `ax` reads through the gateway named
by `ALMANAK_AX_GATEWAY_URL` and `ALMANAK_AX_GATEWAY_AUTH_FILE`; a flag always wins, and
with neither the command is refused. A public endpoint is never chosen for the
caller.

`--public-rpc <name>` reads through one named public endpoint;
[Public endpoints](#public-endpoints) lists the names, the chains each serves and
what they do not serve. The
inline `pool-price` key names a pool on the reviewed deployment for the chain,
hookless unless `--hooks` names a reviewed hook (whose dynamic-fee profiles take
`--fee 8388608`). Use each command's `--help` for the complete argument list.

`ax lending-reserve` resolves a lending market exactly as the author loader
does and reports the price sources the protocol uses at the pinned block, with
a paste-ready `config` block:

```bash
almanak ax lending-reserve --connector aave_v3 --chain eip155:1 \
  --asset eip155:1/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 \
  --block finalized --env-file .env
```

Selector flags use the author field names: `--asset` and optional `--pool` for
Aave V3 and SparkLend, `--spoke` and `--reserve-id` for Aave V4, `--comet` and
`--asset` for Compound V3, `--market-id`, `--leg` and optional `--core` for
Morpho Blue. The result carries `resolution`, `price_sources`, `config` and the
reserve `observation`. For Aave V3 and SparkLend, `config.allocation` holds the
`oracle` and `price_source` an allocation on that market states. For Aave V4,
`config.debt` holds the `oracle` and `debt_price_source` of a debt on that
reserve, and `config.collateral_price_source` is the entry for that reserve in
a debt's `collateral_price_sources`. Compound V3 and Morpho Blue resolve their
feeds and oracle themselves, so their `config.market` adds them only as
optional assertions. A deployment the connector has not reviewed is refused. A
reserve state read the protocol refuses at that block leaves `observation`
null with `observation_error`, and the price sources are still reported.

Beside the raw `observation`, `rates` states the reserve's annual rates from
that same read, with no read of its own: `supply_apr`, `supply_apy`,
`borrow_apr` and `borrow_apy` as decimal ratios per year (`0.05` is 5%),
rounded half-even to 12 places from exact arithmetic, `compounding`,
`available_liquidity` in the asset's units and the `raw` protocol values they
come from. They are the protocol's rates at the pinned block, not a forecast.

| Connector | Raw rates | APR | APY | `available_liquidity` |
| --- | --- | --- | --- | --- |
| `aave_v3`, `sparklend` | `liquidity_rate_ray`, `variable_borrow_rate_ray`: per year, in ray | rate / 1e27 | (1 + APR / Y)^Y - 1 (`per_second`) | Aave V3 `virtual_underlying_balance`; SparkLend `available_underlying` |
| `compound_v3` | the Comet curves' per-second supply and borrow rates at its recorded utilization, as `getSupplyRate` and `getBorrowRate` answer, in 1e18 | rate x Y / 1e18 | (1 + rate / 1e18)^Y - 1 (`per_second`) | `base_available`, the Comet's base balance |
| `morpho_blue` | the IRM's per-second `borrowRateView`, in 1e18; `supply_assets`, `borrow_assets`, `fee` | borrow: rate x Y / 1e18; supply: borrow APR x utilization x (1 - fee) | e^APR - 1 for each (`continuous`, as Morpho accrues): utilization and fee scale the rate before it compounds | `supply_assets - borrow_assets` |

Y is 31,536,000 seconds and utilization is borrow over supply assets.

These are the rates of the asset the market lends, named as `rates.asset`: the
reserve's own asset for Aave V3 and SparkLend, the Comet's base asset and
Morpho's loan asset. `rates.selected_asset` states what the selected asset
earns: `role` `lent` with the market's `supply_apr` and `supply_apy` when it is
that asset, or `role` `collateral` with both zero when it is collateral, which
neither a Comet nor a Morpho market lends. Selecting WBTC in a USDC Comet
reports USDC's rates under `rates` and zero under `rates.selected_asset`. The Aave
V4 reserve observation reads no Hub drawn rate or liquidity, so its `rates` is
null with `rates_unavailable`. A refused observation reports no `rates`.

`risk` labels the collateral parameters the health factor and the borrow limit
use, in basis points, one `collateral` row per collateral asset:
`ltv_bps`, `liquidation_threshold_bps` and `liquidation_bonus_bps`. Beside them
it states the formulas, at the protocol's own oracle prices:

- `health_factor`: sum(collateral_value x liquidation_threshold) / debt_value;
  the position is liquidatable below 1.
- `borrow_limit`: debt_value <= sum(collateral_value x ltv).
- `projection`: for one collateral and a debt of f times its value, the health
  factor is liquidation_threshold / f, and the largest f a floor H admits is
  min(ltv, liquidation_threshold / H).

| Connector | `ltv_bps` | `liquidation_threshold_bps` | `liquidation_bonus_bps` |
| --- | --- | --- | --- |
| `aave_v3`, `sparklend` | the reserve configuration's LTV | its liquidation threshold | its stored bonus - 10000 |
| `aave_v4` | the collateral factor of the reserve's current dynamic config | the same factor | `maxLiquidationBonus` - 10000, paid at or below `liquidation.health_factor_for_max_bonus` |
| `compound_v3` | `borrowCollateralFactor` / 1e14 | `liquidateCollateralFactor` / 1e14 | (1e18 - `liquidationFactor`) / 1e14, the share an absorbed account loses |
| `morpho_blue` | `lltv` / 1e14 | the same LLTV | `liquidation_incentive_factor` - 1, where the factor is min(1.15, 1 / (1 - 0.3 x (1 - LLTV))) |

Compound V3 and Morpho Blue keep the factors on the market, so one read covers
every collateral. On Aave V3, SparkLend and Aave V4 they belong to the
collateral's own reserve: call `lending-reserve` on the collateral asset, not
the borrowed one. Aave V3 and SparkLend values are the ordinary-mode ones; eMode
and isolation are not applied. Aave V4 also reports the reserve's
`dynamic_config_key` and the Spoke's `liquidation` config, and states that
`collateral_risk_bps` is the risk premium the collateral adds to the debt rate,
not a collateral factor. An Aave V4 risk read the Spoke refuses leaves `risk`
null with `risk_error`; a refused observation reports no `risk`.

Each row's `max_borrow_fraction.by_ltv` is the LTV. Two optional flags project a
borrow against that one collateral:

```bash
almanak ax lending-reserve --connector aave_v3 --chain eip155:1 \
  --asset eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 \
  --borrow-fraction 0.3 --borrow-fraction 0.5 --health-floor 1.6 \
  --block finalized --env-file .env
```

`--borrow-fraction f` (repeatable, above 0 and at most 1) adds a `projected` row
per fraction with `health_factor` = liquidation_threshold / f and
`within_borrow_limit` (f <= LTV). `--health-floor H` (above 1, since 1 is the
liquidation line) adds `max_borrow_fraction.at_floor` = min(LTV,
liquidation_threshold / H). Values are exact decimals rounded down to 4 places,
so neither overstates the margin. They are static ratios at the pinned block:
no price move, interest or other collateral is included, and a position with
several collaterals combines them as `health_factor` states.

`ax balance` reads every repeated `--account` at the same pinned block.

`ax pool` reads a Uniswap V3, PancakeSwap V3 or Aerodrome Slipstream pool from
only its chain and address, given inline or as a `{"chain", "pool"}` file.
Both forms validate as the same request:

```bash
almanak ax pool --connector uniswap_v3 --chain eip155:4663 --pool 0x... \
  --block latest --depth-bps 200 --gateway-url "$URL" --gateway-auth-file "$TOKEN_FILE"
```

The connector's resolver reads the pair, fee and tick spacing from the pool and
refuses a pool whose factory is not the connector's reviewed deployment on that
chain, naming the factory the pool reports. The result carries `support`
(`verified`, or `candidate` for a chain declared only for qualification), the
resolved identities, the pinned observation and the spot price of currency0 in
currency1, which is `null` when the pool has no active liquidity. With
`--depth-bps`, `depth.to_lower` is the currency1 the pool pays out before its
price falls by that many basis points and `depth.to_upper` the currency0 it pays
out before its price rises by as many; both exclude swap fees and are raw
integer units. The walk reads every initialized tick inside the band, and a band
wider than 5000 basis points or holding more than 2048 initialized ticks is
refused rather than truncated.

For an Aerodrome classic or Curve pool, `ax pool` reports the connector's
resolution (the pool's coins, decimals and, for Curve, its family and revision,
verified against the reviewed factory or registry) and the pool's prices. An
Aerodrome classic report also carries `reserves`: each currency's stored reserve, the
pool's depth, with `asset`, `amount` in base units, `decimals` and the decimal
`quantity`; a Curve report carries its balances in `pool_state`.
`--depth-bps` is refused for them. Every price, here and for the V3-style pools,
is whole units of `quote` per whole unit of `base`, fees excluded, and names its
`kind`:

| Pool | Field | `kind` |
| --- | --- | --- |
| V3-style, Aerodrome classic | `price`: currency0 in currency1 | `spot`, the marginal price at the pinned state (Aerodrome from its stored reserves, volatile or stable) |
| Curve StableSwap | `prices`: each later coin in coin 0 | `spot`, from the invariant as the pool's `get_p` computes it |
| Curve TwoCrypto, TriCrypto | `prices`: each later coin in coin 0 | `last_spot` (`last_prices`, the marginal price after the pool's last swap or liquidity change, not an execution price) and `oracle` (`price_oracle`, an EMA) |

An Aerodrome classic pool without liquidity on a priced side reports `null`.

A Curve pool also reports `pool_state`: `balances` and `total_supply` (raw amounts
with each asset and its decimals), the `fee` (Curve's 1e10-scaled integer and its
basis points; for TwoCrypto and TriCrypto the `mid_fee`, `out_fee` and `fee_gamma`
between which the swap fee moves), `amplification` (A at the pinned block), and
`virtual_price` (1e18-scaled; StableSwap computes it from the invariant as
`get_virtual_price` does, the crypto pools report their stored value). StableSwap
also reports `offpeg_fee_multiplier` and TwoCrypto and TriCrypto `gamma`.

A Curve pool is funded when every coin balance and its LP supply are positive.
An unfunded pool reports `"status": "unfunded_pool"`, a `reason` naming each empty
balance, `prices: null` and only its balances and supply: no swap into it can fill
and a first deposit would set its price. `ax dex-pools` reports such a pool as
`refused` with reason `unfunded_pool: …`, `backtest prepare` refuses a window in which
it is unfunded, and `check --online` for fork or production refuses it in its
`pool_liquidity` stage with code `unfunded_pool`, read through the operator's
provider at the latest block.

#### Public endpoints

`--public-rpc <name>` reads through one named public endpoint that needs no key.
It is for `ax` reads, for `ax supply` and `ax withdraw`, previewed or executed, and
for `ax recover`. The reviewed list is in code, and each command's `--help` prints
it:

| Name | Operator | Chains |
| --- | --- | --- |
| `publicnode` | Allnodes PublicNode | `eip155:1`, `eip155:43114` |
| `drpc` | dRPC | `eip155:1`, `eip155:8453`, `eip155:10`, `eip155:137`, `eip155:4663` |
| `base` | Base, the chain's own endpoint | `eip155:8453` |
| `arbitrum` | Arbitrum, the chain's own endpoint | `eip155:42161` |
| `bnbchain` | BNB Chain, the chain's own endpoint | `eip155:56` |
| `optimism` | Optimism, the chain's own endpoint | `eip155:10` |

Ethereum, Base and Optimism have two endpoints. Avalanche (`eip155:43114`),
Arbitrum (`eip155:42161`), BNB (`eip155:56`), Polygon (`eip155:137`) and Robinhood
(`eip155:4663`) have one, so there is no second endpoint to name when it fails.
HyperEVM (`eip155:999`) has none and needs a provider key. A name that does not
serve the command's chain is refused with the names that do.

An endpoint for a chain does not make an action available there. `ax supply` and
`ax withdraw` run only for a connector and chain that declare the action, which
`almanak support` lists as `direct_actions` and [Operator actions](#operator-actions)
describes; they run through any endpoint listed for that chain. On a chain with a
public endpoint and no declared action, the endpoint serves reads only.

Before any read the endpoint must report the chain's number and its pinned genesis
block and a head no older than 120 seconds; otherwise the command is refused with
`chain_mismatch` or `stale`. One endpoint serves a command, and a failure fails the
read: nothing falls back to another endpoint. Nobody answers for a public endpoint.
It gives no promise of availability, archive state or wide log ranges, it rate
limits and times out without notice, and its operator sees the addresses a command
reads and the transactions it sends before they are mined. An action executed
through one signs what that endpoint's reads resolve, including the market's
contract, so an endpoint that reports falsely can direct an approval or a supply
elsewhere, up to the amount of the action. Naming a public endpoint accepts that.
`ax recover` uses the endpoint its record's action ran on, or the transport it is
given instead. Strategy runs, forks, checks and backtests do not accept a public
endpoint and need a provider key or an operator RPC URL.

A pair is listed only after it passed every check of the
[qualification tool](qa.md#public-endpoint-qualification) in repeated full runs.
The tool samples the reads an observation and a direct action make: the identity
proof, pinned state, the fee quote and the receipt reads. It sends nothing, so it
proves neither every method a connector calls nor that an endpoint accepts a
transaction. A pass is a point-in-time result. A transient refusal fails a run as
it fails a command, and a listed endpoint can refuse a read it served a minute
earlier. A shared provider is absent from a chain where its keyless tier refused
one of those reads in those runs.

What a public endpoint serves beyond that sample was measured on 2026-11-19 with
the commands below, each through the listed pairs named. Nothing else is claimed:

| Command | Measured |
| --- | --- |
| `ax balance` of the native asset, at `latest` and at `finalized` | Served on every listed pair. |
| `ax pool` on a Uniswap V3 pool, `ax pool-price` on a Uniswap V4 pool | Served on every listed pair. |
| `ax lending-reserve` and `ax lending-position` for Aave V4, Compound V3 and Morpho Blue | Served on every listed pair for at least one of the three, each on chains its connector supports. |
| `ax lending-reserve --connector aave_v3` | Refused on all seven chains of the connector, through every endpoint, with `deployment_changed: Aave V3 pool has an unverified revision`. The connector's own revision pin refuses the pool; a provider key was not measured against it. |
| `ax perp-markets --connector gmx_v2`, by market address | Served on Arbitrum through `arbitrum` and on Avalanche through `publicnode`. |
| `ax perp-position`, and `ax lending-reserve` for SparkLend | Not measured. |
| `ax lending-markets` for `aave_v3`, `sparklend` and `aave_v4` | The record is read with calls at the pinned block and listed on each chain the connector supports. Three rows of each listing were read: the SparkLend and Aave V4 rows were `verified`, and every Aave V3 row was `refused` with the same `deployment_changed`. On Avalanche the Aave V4 listing named 8 Spokes in `source.unreadable`, each with `rpc: eth_call: RPC rejected the requested read (JSON-RPC -32000)`, and listed the others. A Spoke counts as holding no reserves only when its revert is reported as JSON-RPC 3, so these may be Spokes without reserves; that was not verified. |
| `ax lending-markets --connector morpho_blue` | Listed on Ethereum, Base, Optimism, Polygon, Robinhood and Arbitrum, where Morpho's API names the markets. Refused on Avalanche and BNB, where it reads the core's event history. |
| `ax lending-markets --connector compound_v3` | Refused on Ethereum, Base, Arbitrum and Optimism, through every endpoint listed there: it reads the Configurator's event history. |
| `ax resolve`, `ax dex-pools` | Refused with `this lookup uses a vendor key; give --env-file or the read gateway`, measured on Base and Arbitrum; the refusal does not depend on the chain. |

An event history is asked for from block zero in one request. Every endpoint such
a listing was run through refused it: `publicnode` on Ethereum and Avalanche, `drpc`
on Ethereum, Base and Optimism, and `base`, `arbitrum`, `optimism` and `bnbchain`. No
listing sends that request on Polygon or Robinhood, so nothing is known there. The
refusal is `eth_getLogs: the provider refused <contract>'s event history in one
request`, or `eth_getLogs: HTTP 429` or `eth_getLogs: RPC rejected the requested
read (JSON-RPC -32005)` where the endpoint answers with its rate-limit code and the
retries run out. A refused listing or lookup does not stop a direct read. Name the
market or the token yourself: the Comet address or the Morpho market ID for
`ax lending-reserve`, the pool address for `ax pool`, the token's CAIP-19 asset for
`ax balance`. A provider key serves an event-history listing where the provider
serves the range, and the lookups by name need `COINGECKO_API_KEY` as
[Discovery](#discovery) describes.

#### Discovery

`ax resolve` and `ax dex-pools` find candidates an author does not yet have.
CoinGecko on-chain names the candidates and the chain verifies them; the
vendor's liquidity and volume are reported as its figures, in USD cents, and
are absent when it omits them. They need `COINGECKO_API_KEY` (a CoinGecko pro
key) in the env file of the calling process or of the read gateway.

```bash
almanak ax resolve --chain eip155:8453 --query cbBTC
almanak ax dex-pools --chain eip155:8453 \
  --token eip155:8453/erc20:0x4200000000000000000000000000000000000006 \
  --quote eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 --depth-bps 100
```

`ax resolve` takes a symbol or an ERC-20 address. A symbol is matched against
the tokens in the pools the vendor's search returns, case-insensitively, by the
symbol the vendor lists or the token's own `symbol()`, and the candidates are
ranked by vendor liquidity. An address the vendor does not list is still reported,
with its vendor fields null. Every reported candidate is read on
chain: `decimals()` verifies the contract and
`symbol()` is reported as `onchain_symbol`, null when the token does not answer
it as a string. `source.complete` is false, with the reason in
`source.limitation`, when the vendor has not indexed part of the lookup or the
search filled the two pages (40 pools) read, since a candidate may then be missing.
`recommended` names the leading candidate only when no lookup went unindexed, it is
verified, its `onchain_symbol` matches the symbol the vendor lists, and it is
either the only candidate or its liquidity is measured at least ten times the
next one's measured liquidity, and its own is nonzero; otherwise it is null and the author chooses.
A search cut at 40 pools still recommends: liquidity is each token's total across all
its pools, and a token large enough to rival the leader has pools among the first 40.
A smaller same-symbol token can still be missed; `source.limitation` reports the cut. A symbol is never identity: the author states the CAIP-19 asset.
Native assets are `slip44` identities and are not looked up.

`ax dex-pools` lists the pools the vendor reports for an ERC-20 `--token`,
optionally only those pairing it with an ERC-20 `--quote`, in the vendor's order (24-hour
volume with liquidity), not re-ranked, so an offset continues the vendor's
list. The vendor offers no snapshot or cursor, so its order can shift between
calls as volumes move.
`--limit` (default 10, at most 20) and `--offset` page through them; `page.matched` counts
the matches in the pages read and `page.has_more` says whether a later offset
returns more. The vendor's venue label only selects a connector; a label the SDK
does not know is matched by the pool's own `factory()` against each connector's
reviewed factory, before `--connector` filters, so a factory-matched pool is kept.
Every reported pool is resolved by its connector, which refuses a pool from an
unreviewed deployment and names why. The vendor's pair is only a filter: a pool
whose resolved currencies do not include `--token` (and `--quote`, when given) is
refused with `vendor_mismatch`. Each pool reports a `status`:

| Status | Meaning |
| --- | --- |
| `verified` | The connector resolved the pool at the pinned block; `market` is a paste-ready `markets:` entry |
| `refused` | The connector refused the pool, with its reason, the pool it resolved does not trade the requested pair (`vendor_mismatch`), or a Curve pool is unfunded (`unfunded_pool`) |
| `unverified` | Uniswap V4 pools whose key neither the vendor's pair and fee nor the pool's `Initialize` event states, including a malformed event |
| `unsupported` | No SDK connector for the venue: neither its label nor a reviewed factory names one |

A Uniswap V4 key is first derived from the vendor pair and the fee in its pool
name as a hookless pool with the standard tick spacing for that fee; when its
hash differs from the vendor's pool ID, the reviewed PoolManager's `Initialize`
event for that ID states the exact key. The connector then admits the key exactly
as it does for a strategy: a hooked pool is verified only when its hook has a
reviewed profile, the profile's permission bits and fee mode, and code equal to
its pin, and the row adds a `hook` block with the `profile`, hook `address`,
`pin: match` and the current `terms` (a Fables or StablePairHook LP fee per
direction, or a Pons hook fee and creator tax in bps). Any other hook is refused
with the connector's reason. A pool holding the chain's native coin (ETH, POL, BNB
or AVAX) keeps the native coin in its PoolKey and is verified with the fund holding
its wrapped token (WETH, WPOL, WBNB or WAVAX): the reviewed Universal Router unwraps
the input or wraps the output within the swap, and the reviewed PositionManager does the
same for liquidity. The vendor files these pools under the native coin, so a `--token`
of the wrapped token also reads the vendor's native-coin list, a page of each list in
turn, and a `--quote` of the wrapped token keeps them. The native coin itself (`slip44`)
is accepted as `--token` or `--quote` and asks for its wrapped token, so it returns the
same wrapped and native pools. If the vendor has not indexed one of the two lists, the
other is still read and the source is incomplete. The row's `markets:` entry names the
native currency; keep it native, since a selection naming the wrapped token is a different
pool. A row the chain did not verify reports the vendor's claimed currencies as
`unverified_vendor_currencies` (the native coin as `slip44`, `null` when omitted): a claim
that tells a native pool from its wrapped twin, not a verified identity.
Native pools on a chain without a reviewed router route, and native/wrapped-native
pools, are refused with the connector's reason.
Vendor USD figures are cents, and absent when missing, malformed or too large to state.
`source.complete` is true only when the vendor's list ended within the pages
read. One call reads at most five vendor pages, the token's 100 busiest pools
(shared with the native coin's list for a wrapped-native token),
and the vendor does not index every pool; an incomplete list says so in
`source.limitation`, and a pool missing from it may still exist. A token the vendor
has not indexed is reported the same way, never as a complete empty list. A known pool,
for example one a reference strategy names, is checked directly with `ax pool`
or `ax pool-price`.

`ax lending-markets` lists a lending protocol's markets on a chain from the
protocol's own records at the pinned block, or, for Morpho Blue where Morpho's
API indexes the chain, from that API. None of them needs a key:

```bash
almanak ax lending-markets --connector aave_v3 --chain eip155:43114
almanak ax lending-markets --connector morpho_blue --chain eip155:8453 \
  --asset eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913
```

| Connector | Record | Order and filters |
| --- | --- | --- |
| `aave_v3`, `sparklend` | the reviewed Pool's `getReservesList()` (`source.record` `pool_reserves`) | the Pool's order; `--asset` keeps that reserve |
| `aave_v4` | the reserves of each Spoke the chain's Hubs serve (`getSpokeAddress`, `getReserve`; `hub_spokes`) | Hub, Spoke and reserve order; `--asset` keeps reserves of that asset; a Spoke that reverts `getReserveCount()` holds no reserves and is counted in `source.without_reserves`; one whose count cannot be read for any other reason (transport, throttling, an undecodable answer) is named in `source.unreadable`, as is a Hub whose Spokes cannot be enumerated (`hub`) and a reserve that cannot be read (`spoke`, `reserve_id`); the rest still list |
| `compound_v3` | the Comets the chain's Configurator recorded in `CometDeployed` events (`comet_deployments`) | deployment order; `--asset` keeps Comets whose base or collateral it is, `--collateral` those accepting it |
| `morpho_blue` | Morpho's API on Ethereum, Optimism, Polygon, HyperEVM, Robinhood, Base and Arbitrum (`source.record` `morpho_api`, with `source.skipped` counting rows it answered in another shape); elsewhere the markets the reviewed core recorded in `CreateMarket` events (`core_markets`) | `--asset` (the loan asset) is required; supplied markets only, most supplied in the loan asset first; `--collateral` narrows; from the API, only markets Morpho lists unless `--include-unlisted` (`source.listed_only`, `source.curation` `morpho_api`); from the core's events, every supplied market (`listed_only` false, `curation` null) |

A record only names a candidate. Each reported market is resolved by its
connector as `lending-reserve` resolves it, and reports `status` `verified`
with the same `config` block `lending-reserve` returns, or `refused` with the
resolver's reason. A Compound V3 selection names the asked-for `--asset`, base
or collateral, or else the base asset; a Comet whose assets cannot be read is
named in `source.unreadable`. A Morpho selection is the market's loan leg; its
collateral leg uses the same `market_id`. From the API, a Morpho row reports only
its `market_id` and the asked-for `loan_asset` as the market's own; Morpho's view
is under `vendor`: `collateral_asset`, `lltv`, `supplied` and `borrowed` in the
loan asset, `supplied_usd` and `borrowed_usd`, `supply_apy`, Morpho's `listed`
curation and its `warnings` (for example `not_whitelisted` or
`unrecognized_collateral_asset`). They come from Morpho's current index, not the
pinned block, are absent where it omits them, and are never strategy inputs; a
verified row's `config` is the resolver's reading at the pinned block, and a market
whose own loan or collateral asset contradicts the row is refused with
`vendor_mismatch`; a row naming no collateral claims an idle market, whose
collateral is the zero address. Anyone can
create a Morpho market, and an unlisted one can show supply looped against
worthless collateral that still passes the resolver's revision checks, so the API
listing keeps to Morpho's listed markets by default; `--include-unlisted` adds the
rest, each with Morpho's warnings. The core's events carry no curation, so a
listing from them is not filtered: its `source` reports `listed_only` false and
`curation` null, and `--include-unlisted` changes nothing there. From the core's events, `supplied` and
`borrowed` are the totals the core last recorded, before interest accrued since;
unsupplied markets are counted in `source.without_supply`, and a listing that
would read more than 5,000 markets is refused. A provider that caps
`eth_getLogs` ranges refuses a core's whole history in one request; ours does on
Avalanche, where the listing then fails with that reason and `lending-reserve`
still verifies a known market ID.
Every reported asset is an object of its CAIP-19 `asset` and its on-chain
`symbol()`, null where a token does not answer one as a string; the symbol is a
label, the asset is the identity. A resolver checks the deployment's revisions,
not whether a reserve takes deposits or loans now, so Aave V3 and SparkLend rows
also carry the reserve's own `reserve` flags (`active`, `frozen`, `paused`,
`borrowing_enabled`, `ltv_bps`, `liquidation_threshold_bps`,
`liquidation_bonus_bps`) and Aave V4 rows
its `frozen`, `paused` and `borrowable`; a frozen reserve, or one with no
loan-to-value, cannot back a borrow even when `verified`. A flag read that fails
reports `reserve` as null. Compound V3 rows add `collateral_factors`, one per
entry of `collateral_assets` in the same order, and Morpho rows add
`liquidation_incentive_factor` beside `lltv` (under `vendor` when the LLTV is
Morpho's), each as `lending-reserve`'s `risk` labels them. A `verified` row also carries the `rates` that
`lending-reserve` reports for that market, from the same reserve read; a read
the protocol refuses leaves `rates` null with `rates_error` and the row still
verified, and an Aave V4 row reads nothing for them (`rates_unavailable`). A
row from Morpho's API keeps Morpho's own `vendor.supply_apy` beside the
on-chain `rates`. `--limit` (default 10, at most 20) and
`--offset` (at most 500) page through the matches, with `page.matched` and
`page.has_more`. Only the page's markets are resolved. Without a filter, an Aave
V4 listing reads only the page's reserves and a Compound V3 listing only the page's
Comets: `matched` counts every one the record names, and a reserve or Comet that
cannot be read is named in `source.unreadable` only when it falls on the page. With
`--asset` or `--collateral`, each is read to count the matches, and one that cannot
be read is named and not matched. Each event history is asked for from block zero in one
request; a provider that refuses that range or result size fails the listing.

#### HyperCore

Hyperliquid's HyperCore is a venue of HyperEVM (`eip155:999`): a HyperCore
account is the same address on that chain. `ax hypercore-markets` and
`ax hypercore-account` read it from Hyperliquid's own `/info` API, which needs
no key:

```bash
almanak ax hypercore-markets --query BTC
almanak ax hypercore-account --account 0x...
```

The API has no as-of query, so these two take no `--block`: an answer is
HyperCore's state when the API served it. Each report names every query it made
in `source` or `sources`, with the time its answer was received in milliseconds
(`received_at`), never a block.

`hypercore-markets` reads the first perpetual dex's `metaAndAssetCtxs`. Each
market reports its `target` (`dex` and `coin`), its `asset` index (its position
in the dex's universe, the number every order carries), the venue's `meta` entry
(`szDecimals`, `maxLeverage`, `marginTableId`, and `isDelisted` or `marginMode`
where the venue sets them), the `margin_tiers` of its margin table, null when
`meta` lists no table of that ID, and its `context`: mark, oracle and mid prices,
`funding`, open interest and volumes, as the venue writes them. `--query` reports
the one market `meta` names so, or names so apart from letter case. Without it,
tradable markets are listed by the venue's 24-hour notional volume, most traded
first; `--include-delisted` adds delisted ones. `--limit` (default 20, at most
50) and `--offset` (at most 500) page through them; `matched` counts them all.
HIP-3 dexes are not read.

`hypercore-account` reads each `--account`'s `role` (`missing` until HyperCore
first credits the address; an approved agent reports the account it trades for),
its account abstraction mode (`abstraction`), its perp margin and positions, and
its spot balances, its open orders on the first dex (`open_orders`, spot
included, whoever placed them, each with its `cloid` when it has one) and the
agents it approved (`agents`, each with `validUntil`, its expiry in milliseconds,
or null). Under a unified account or portfolio margin, spot holds every
balance and the perp state is not meaningful: `perp.state` is then
`not_meaningful`, otherwise `as_reported`. Each position carries its market's
`asset` index. Each spot balance names its HyperEVM `asset`: the ERC-20 the
token is linked to, the USDC ERC-20 for USDC (which HyperCore links to Circle's
CoreDepositWallet), the native asset for HYPE, or null for a token linked to no
HyperEVM contract, which has no identity. Amounts are decimal strings in the
token's HyperCore units (`wei_decimals`).

Every answer must match the shape recorded from mainnet for its query; an answer
with a field added, missing or written differently is refused as `malformed`
rather than read, and an answer for another market, account, order or time range
than asked is refused as `source_mismatch`. The read gateway serves the same
queries as its `info` operation, and `ALMANAK_GATEWAY_HYPERLIQUID_WEIGHT_PER_MINUTE`
caps them by the API's own request weight: 2 for an account's perp or spot state,
60 for its role and 20 for the rest, plus one per 20 items a fill, funding or
ledger page returns. Before sending, such a page reserves the most it can cost (120
for fills, 45 for funding payments, 120 for ledger entries) and settles to the
count it returned. The ledger charge is the SDK's conservative choice: the API
documents no per-item weight for ledger entries. These reservation limits are
separate from reconciliation's truncation thresholds (2000 fills, 500 funding
payments, 500 ledger entries), which mark a page that may be cut short.
