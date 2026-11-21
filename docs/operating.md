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
The manifest a deployment was created with is retained and records the code of
each contract it covers: for Aave V3, the Pool, its implementation, the reserve
tokens and the price sources. That check compares them with the chain, so when a
protocol upgrade replaces one of them, every plan of that deployment is refused
with `manifest contract identities differ from the supported profile`, an exit
or a teardown as much as an entry, whether or not the installed grants still
fit. Every Aave V3 Pool moved from revision 11 to revision 12 on 2026-11-11: a
Safe/Roles deployment created before that is refused this way, and one created
since is served. A plain-account deployment retains no manifest and
continues across such an upgrade.
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

`almanak ax supply`, `withdraw`, `borrow`, `repay`, `collateral`, `swap` and the
liquidity commands each run one action for one wallet in one process, with no
strategy, workspace or gateway process to start. The lending commands:

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
not have been sent, and `executed` is true for it. A signed step whose deadline
passed without it being mined is released, since it can no longer take effect, and
is listed under `released_expired` with its operation and transaction hash;
`executed` is true for it and `completed` false. A signed step whose nonce another
transaction of the wallet used is released once that transaction is final, and is
listed under `released_consumed_nonce`. A command that fails after such a release
prints it in its `steps_sent` event too. A first step refused at planning
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
| `uniswap_v4` | `approve_erc20` to Permit2, `approve_permit2` to the router until the deadline, then `swap` | none from a wallet with none, and under `permit2_unexpired` the spent Permit2 allowance until its deadline; see below for a wallet with standing allowances |
| `curve` | `approve` to the pool for exactly the amount, then the exchange | none |

On Uniswap V4 a standing ERC-20 approval to Permit2 that covers the amount is used
as it stands, and a standing Permit2 allowance to the router is used only when it
outlasts the swap's deadline; otherwise it is granted again for exactly the amount.
What the swap does not spend remains. An unlimited allowance stays: Permit2 leaves
its own whole, and a token leaves an unlimited approval to Permit2 whole or lowers
it by the amount sold, depending on the token.

A swap refused or reverted after its approval was mined leaves that approval: the
result lists it, and a later swap of the same amount uses it, on Uniswap V4 after
granting the Permit2 allowance again. `ax revoke-exchange` sets the allowances to
zero; what it clears on Uniswap V4 is stated under [Liquidity](#liquidity).

`ax recover` resolves an interrupted swap as it does any action. A signed Curve swap
that may have been sent, and that the pool would now pay below its minimum, is
neither sent nor released: `ax recover` reports `signed_attempt_unsendable` and
exits 1, and the record and the wallet's claim on that chain stay. Until then the
signed bytes can still be mined at their old minimum, by `ax recover` once the pool
pays it again or by anyone who holds them.

On a network that states a [finality margin](#a-step-whose-nonce-was-used) there is
another way out, the operator's own: use the swap's nonce with another transaction of
the wallet. A node that still holds the swap accepts another transaction at its nonce
only at a higher fee, and until the replacement is final the swap can still be mined
in its place. Once it is final, `ax recover` releases the record. On a network that
states no margin, Polygon PoS today, do not do this: a used nonce releases nothing
there, the signed swap can then never be sent either, and every later `ax recover`
answers `nonce_conflict`, so the record stays for good. The refusal's message says
which of the two holds on the record's network. No SDK command sends such a
transaction, and an agent following the bundled skill never does: the choice between
waiting and replacing is the operator's, with the wallet's own tools.

A swap deployment trades one pool and holds no liquidity position. On Aerodrome and
Curve a wallet that holds the pool's LP shares, however few, is refused before
anything is signed, as a trading-only strategy there is. Anyone can send such shares
to a wallet. Shares that arrive once a step is signed do not block it, whether it
was sent yet or not: the step is sent, by the command or by `ax recover`, and
reconciled from its own receipt, and its outcome lists the shares under
`accounting_issues`. On Aerodrome a swap takes an approval and then the swap itself.
When the shares arrive after the approval is signed, the approval is sent, the swap
is refused before it is signed, no record is kept, and the result lists the
allowance that remains.

A strategy with a trading-only Aerodrome allocation, composed or not, follows a
narrower rule, since a fund that holds shares can no longer be observed, so the
strategy can neither decide nor tear down. Its signed approval or revocation, which
carries no deadline, and its signed exit swap, which lowers exposure, are sent as
above. Its signed entry swap is not: it is held back while the fund holds the
shares and is never sent past them, so the strategy takes no new exposure it could
not leave. Once its deadline has passed the swap can only be released, and a
production run releases it after the shares are removed: until then each attempt
to send it is refused for the shares first. The approval that preceded it stays
standing for as long.

#### Liquidity

Concentrated-liquidity positions on Uniswap V3, PancakeSwap V3 and Aerodrome
Slipstream are opened and managed with one command per step, on every chain where
`almanak support` lists the kind under `direct_actions`. Each names its pool with
`--pool`:

| Command | Takes | Does |
| --- | --- | --- |
| `open-liquidity` | `--tick-lower`, `--tick-upper`, `--amount0`, `--amount1`, `--slippage-bps` | Mints a position in that tick range from at most those amounts of the pool's two currencies. |
| `increase-liquidity` | `--token-id`, `--amount0`, `--amount1`, `--slippage-bps` | Adds at most those amounts to a position the wallet owns. |
| `decrease-liquidity` | `--token-id`, `--liquidity`, `--slippage-bps` | Removes that much of the position's liquidity and collects what it is owed. |
| `collect-liquidity-fees` | `--token-id` | Collects what the position is owed and leaves its liquidity. |
| `close-liquidity` | `--token-id`, `--slippage-bps` | Removes all liquidity, collects what is owed and burns the NFT. |
| `revoke-exchange` | | Sets every allowance the wallet gave the pool's router and position manager, in the two pool currencies, to zero. |

```bash
almanak ax open-liquidity --connector uniswap_v3 --chain eip155:8453 \
  --pool 0xd0b53d9277642d899df5c87a3966a349a798f224 \
  --tick-lower -198040 --tick-upper -198010 \
  --amount0 400000000000000000 --amount1 1000000000 --slippage-bps 50 \
  --wallet /private/operator --public-rpc base \
  --max-gas-price-wei 1000000000 --max-transaction-fee 2000000000000000
almanak ax liquidity-position --connector uniswap_v3 --chain eip155:8453 \
  --pool 0xd0b53d9277642d899df5c87a3966a349a798f224 --token-id 5947663 \
  --block latest --public-rpc base
```

The ticks are the operator's own and must be multiples of the pool's tick spacing;
there are no bands. `--amount0` and `--amount1` are base units of the pool's
currency0 and currency1, at least one above zero, and the wallet must hold both
stated amounts. Nothing is swapped inside a liquidity action: the mint uses what the
range takes at the mined price, never more than the liquidity the stated amounts buy
at the observed price takes within `--slippage-bps` of it, and the rest stays in the
wallet and is not approved. Swap first with `ax swap` when the wallet holds only one
side.

`--slippage-bps` is required. A mint or an increase is neither planned nor signed
while the pool price is further than this from the price at the block the command
first observed. The signed call offers no more than the liquidity the stated amounts
buy at that price takes at the edges of the tolerance, and requires what that offer
yields there, so while the tolerance stays inside the range it reverts when mined at
a price beyond the tolerance of that observed price in either direction. Where the
tolerance reaches an edge of the range, and for a range beside the price, no price
beyond that edge reverts the call: it takes there only the one currency the range
holds, at most the offered amount. For a decrease or a close, each payout may fall
at most this far below the principal at the observed block. `--deadline-seconds` is
300 unless stated; the position manager's call carries the deadline, and an approval
before it carries none.

A position is named by `--token-id`, the position manager's NFT. The command reads
it at its pinned block and refuses an NFT the wallet does not own or one of another
pool; the deployment then owns that NFT and nothing else, and its range is the
NFT's own. A position opened anywhere, by a strategy or another tool, can be
managed this way. Collecting, decreasing or closing a position with no liquidity is
refused before anything is signed, as is a collect with nothing owed; increasing an
empty position is served.

A position manager lets any account add liquidity to any NFT. A step that is mined
after another account added to the position is recorded with the position as its
block holds it and an `accounting_issues` entry, and the record resolves. A close
mined that way reverts, since the position is no longer empty when it burns, and is
stated again.

`ax liquidity-position` is the read for a position: its `owner`, `range`,
`liquidity`, whether the pool price is in range, the `principal` it would return at
the current price, and what it is `owed`, which is what a collect pays. An Aerodrome
Slipstream NFT held by its pool's gauge is reported with `staked: true`, and its
`owed` is only what the manager recorded, since a staked position earns no trading
fees. It takes
the same transports and `--block` as the other reads; `--block` defaults to
`finalized`, so read a position that was just opened at `--block latest`.

`position_before` and `position_after` of a liquidity action hold the `position`
(its `token_id`, `range` and `liquidity`, or null), what `moved` in and out of the
wallet in each currency over the mined steps, the `inventory` the action holds
beside the position (the stated amounts an open or an increase did not use, or what
a collect, a decrease or a close paid out), and the nonzero `allowances`. An open or
an increase approves the position manager for what it offers, at most the stated
amounts, and uses what the mint takes, so the unused part of each approval
remains afterwards; `revoke-exchange` clears it. A mint is the largest step: with
its 25% buffer it stays under the default gas limit on the forks rehearsed, and the
fee ceiling must cover it at the chain's gas price.

On Curve a position is the wallet's balance of the pool's LP token, so there is no
tick range and no token ID:

| Command | Takes | Does |
| --- | --- | --- |
| `open-liquidity`, `increase-liquidity` | `--amount <CAIP-19 coin>=<base units>`, once per coin deposited, `--slippage-bps` | Deposits exactly those amounts. `open-liquidity` is for a wallet that holds none of the pool's LP token, `increase-liquidity` for one that does. |
| `decrease-liquidity` | `--liquidity <LP shares>`, `--receive`, `--slippage-bps` | Burns that many shares, fewer than the balance. |
| `close-liquidity` | `--receive`, `--slippage-bps` | Burns the whole balance. |
| `revoke-exchange` | | Sets every allowance the wallet gave the pool to zero. |

`--receive` is required for a burn: `balanced` pays every coin in the pool's current
proportions, and a coin of the pool pays that coin alone, which trades against the
pool and pays its fee. The deployment owns the wallet's whole LP balance in the
pool, whoever deposited it. `--deadline-seconds` bounds only how late a step may be
signed, since Curve calls carry no deadline. A deposit approves the pool for
exactly each stated amount, so a completed one leaves no allowance on the coins it
deposited. A deposit in other proportions than the pool's, like a one-coin burn,
trades against the pool and pays its fee; `--slippage-bps` bounds how far the LP
shares minted may fall below what the amounts were worth at the observed block. The read takes
the account instead of a token ID and reports its LP shares, the total supply and
what a balanced burn would pay:

```bash
almanak ax liquidity-position --connector curve --chain eip155:1 \
  --pool 0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72 --account 0x... --block latest \
  --public-rpc publicnode
```

On Aerodrome classic a position is the wallet's balance of the pool's share token.
The commands are the V3 family's without a tick range or a token ID:
`open-liquidity` and `increase-liquidity` take `--amount0` and `--amount1`, both
above zero, and `decrease-liquidity` takes `--liquidity` in shares;
`collect-liquidity-fees` claims the fees the shares earned, and `revoke-exchange`
also clears the share approval a burn gave the router. The deployment owns the
wallet's whole share position in the pool, whoever deposited it.

An Aerodrome deposit offers what it takes of the stated amounts at the proportion of
the pool's reserves at the observed block: all of one amount and the other's share
of it. It approves the router for exactly that. `--slippage-bps` bounds it on both
sides: the call reverts when it would take less of either currency than that far
below the offer, which bounds how far the proportion may move in either direction.
The call states no minimum of shares; a shortfall is refused before signing and
reported once mined. Mined at the observed proportion a deposit leaves no allowance;
after a move inside the slippage, what the router did not take of one amount stays
approved until revoked. A signed liquidity step is sent although shares arrived in
the wallet since, and its outcome lists them under `accounting_issues`. A close is
complete for the shares it burned; shares that arrive with the burn stay in the
wallet. The read is `ax liquidity-position --connector aerodrome` with `--account`.

A wallet that holds a pool's LP shares on Curve or Aerodrome is refused an `ax swap`
in that pool, which owns no liquidity: swap first, or in another pool.

Uniswap V4 serves `open-liquidity`, `collect-liquidity-fees`, `close-liquidity` and
`revoke-exchange`, with the pool named by its key as for a swap (`--currency0`,
`--currency1`, `--fee`, `--tick-spacing` and, for a reviewed hooked pool, `--hooks`).
A collection and a close name the position by `--token-id`; a revocation names no
position. Its connector has no intent that adds to or removes part of a
position, so `increase-liquidity` and `decrease-liquidity` refuse it. A key may name
the chain's native coin: the wallet provides and receives the wrapped token, and
`--amount0` and `--amount1` follow the key's order.

A Uniswap V4 mint is for a fixed liquidity: the most the stated amounts cover at the
price observed when the mint step is planned. The most it may take of each currency
is the stated amount, trimmed to what that liquidity takes at the edge of
`--slippage-bps` around that same price: the price less the slippage for currency0,
the price plus the slippage for currency1. The position manager reverts a mint that
needs more, so a mint mined at a price further than `--slippage-bps` from the price
it was planned at reverts, and against the currency that limits the liquidity it
reverts on any move. The range bounds what the slippage cannot. Where an edge of the
slippage lies at or beyond a bound of the range, the mint may take all the range
holds of that currency, and no price in that direction reverts it. A range wholly
beside the price takes one currency only, the same amount at every price on that
side. The gateway plans the mint only while the price is within `--slippage-bps` of
the price the command decided at, and does not send it once the price has left the
slippage around the planned price; these two are the gateway's checks, not the
chain's.

Each step approves what its own observation allows the mint to take: Permit2 for
that most of each token, and for the position manager a Permit2 allowance that
expires at the mint's deadline. An approval mined at an earlier price for less than
the mint now needs is made again before the mint. What the mint does not take stays
approved: the Permit2 allowance until it expires, the ERC-20 approval to Permit2
without limit of time. A collection and a close approve nothing.
`ax liquidity-position --connector uniswap_v4` takes the key flags and `--token-id`.

On Uniswap V4 `revoke-exchange` takes the key flags and clears what a swap or a mint
left, one transaction per allowance. For each of the two tokens the wallet holds for
the pool it sets the ERC-20 approval to Permit2 to zero, then sets the Permit2
allowance to the router and the one to the position manager to zero and expires
them. It clears an allowance that holds an amount, and a Permit2 allowance that
holds none and has not expired. A completed swap leaves one of those until its
deadline: it grants nothing, the result lists it under `permit2_unexpired` apart from
`allowances`, and a strategy's start in the same wallet is refused while it stands.
With nothing of either kind the command is refused.

The ERC-20 approval to Permit2 is not Uniswap's alone: every application that spends
that token through Permit2 relies on it, so after the command each of them needs the
token approved to Permit2 again. When the command will set one to zero, its result
says so in `note`, in the preview as in the execution, naming the tokens. Permit2
allowances the wallet gave any other spender are neither read nor changed. The
command moves no tokens and touches no position: an NFT the wallet holds stays, and
collecting from it or closing it needs no approval.

The steps run in the order above and each must be mined before the next is planned.
A token whose `approve` of zero reverts therefore stops the command at that step,
and the allowances after it are not reached: they are cleared with another tool.
The same holds for `revoke-exchange` on the other pool connectors.

```bash
almanak ax revoke-exchange --connector uniswap_v4 --chain eip155:8453 \
  --currency0 eip155:8453/erc20:0x4200000000000000000000000000000000000006 \
  --currency1 eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 \
  --fee 3000 --tick-spacing 60 \
  --wallet treasury --env-file .env \
  --max-gas-price-wei 100000000 --max-transaction-fee 100000000000000
```

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
1. A signed step whose deadline passed unmined is released instead: it is listed
under `released_expired`, `resolved` is true and the wallet is free, and the command
exits 1 because the step did not happen. So is a
[step whose nonce was used](#a-step-whose-nonce-was-used) by another transaction of
the wallet. A record a running command still uses is refused.

A record with no signed step holds nothing worth keeping. When a command is killed
before it signs, the next operator action removes that record and releases its
claim, once it can read the record and see that nothing is signed. That includes a
journal with no checkpoint yet, left by a command killed while creating it. A
record that cannot be read at all is kept with its claim, and `record_kept` can
name such a record. Do not kill a command that runs with `--execute`: it waits for each receipt
without a time limit, and stops between steps on SIGINT, SIGTERM or SIGHUP.

An action has no amount ceiling: it is bounded by the wallet's balance and the two
fee ceilings.

#### A step whose nonce was used

A signed step that was never mined, and whose nonce another transaction of the same
wallet has used since, can never be mined: sending it again is refused by every node,
and waiting changes nothing. This holds for every step of every direct action,
whether it carries a deadline or not, and whether it was sent or not. `ax recover`
releases such a step once the transaction that used the nonce is final. Its result,
without the position:

```json
{
  "resolved": true,
  "discarded_request": false,
  "outcomes": [],
  "released_consumed_nonce": [
    {
      "operation": "swap",
      "transaction": "0x1afe...",
      "nonce": 7,
      "consumed_by": {
        "transaction": "0x9c02...",
        "block": 25927622,
        "block_hash": "0x5b1d..."
      }
    }
  ]
}
```

`transaction` is the step that was released and `consumed_by` the transaction that
took its nonce, with the block that holds it. The step did not happen, so the command
exits 1 although `resolved` is true; the record is removed and the wallet is free.
Nothing is planned or signed in its place: state the action again. Steps mined before
the released one stay in force. An approval that was mined still stands, the result
lists it under `position_after`, and the action stated again uses it.

The proof is recorded in the record's checkpoint by the same update that drops the
step, and the result is read from there. A command or an `ax recover` that is stopped
or fails after the drop prints it in its `steps_sent` event on standard error. A
process that is killed outright prints nothing, as for any step.

The gateway does not take a missing receipt as proof, since a lagging provider also
answers none. It finds the block in which the wallet's transaction count passed the
step's nonce, reads that block and requires exactly one transaction of the wallet
with that nonce in it, other than the recorded one. Two things are then required of
that block:

- it is at or below the block the endpoint serves as `finalized`;
- it is behind the endpoint's head by the network's margin. Some endpoints serve
  their head as `finalized`, and nothing in the answer tells that apart from fast
  finality, so the tag alone releases nothing.

What the margin stands for differs by chain. On Ethereum and BNB Smart Chain it is
the chain's own time to finality. On a rollup it is only a floor for normal
operation: a rollup block is final once its batch is final on Ethereum, the sequencer
keeps producing blocks while a batch is late, and it can reorganize a block until
that block's data is on Ethereum. Seconds behind the rollup's head do not measure
that, so on a rollup the release rests on the endpoint's `finalized` tag.

| Network | Margin | What it is |
| --- | --- | --- |
| Ethereum | 1152 s | The chain's finality time: a block is final two to three epochs after it is proposed; three epochs. |
| OP Mainnet, Base, Arbitrum One, Robinhood Chain | 1800 s | A floor. The longest documented normal case among them, about 15 to 30 minutes on OP Mainnet. The `finalized` tag is what is trusted. |
| BNB Smart Chain | 252 s | The fallback when fast finality votes are missing, taken as one turn of all 21 validators under the larger of the chain's two documented parameter sets. |
| Avalanche C-Chain | 1 s | The documented time to acceptance. The chain documents that an accepted block is never reorganized, and the release rests on that. |
| Polygon PoS, HyperEVM | none | No documented bound that holds without the tag, so no step is released there. |

Otherwise the record stays and `resolved` is false:

- with `nonce_conflict` while the other transaction is pending, above the `finalized`
  block or within the margin, because a reorganization could drop it and leave the
  signed step valid again. Ask again later.
- with `nonce_conflict` on a network that states no margin.
- with `nonce_conflict` when the count rose without exactly one such transaction, as
  it does when another account's transaction carries an authorization the wallet
  signed.
- with `inconsistent` when the block holds the recorded transaction itself although
  the endpoint serves no receipt for it, when the nonce was already used at the
  block the step was planned at, or when the block as read whole is not the one the
  endpoint holds at that height in a second read. None shows the step dead; try
  again or another endpoint.
- with the provider's own refusal when it does not serve a read the proof needs: the
  wallet's transaction count at the blocks since the step was planned, as a provider
  without archive state may not for an old record, or a transaction count at a block
  hash, which some endpoints reject. Another endpoint of the record's chain, named
  with `--env-file` or `--public-rpc`, resolves it.

This check runs in the gateway process and is not enforced on chain. It rests on one
endpoint's report of its head, its finalized block and that block's transactions. An
endpoint that lies about them can cause the release of a step that is still valid. For
a step that was sent and carries no deadline, such as a Curve swap, the signed bytes
can then still be mined at their old minimum, and an operator who states the swap
again is exposed to both.

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
subcommand but `portfolio` takes one transport and its own request arguments
after the subcommand, and every one but `portfolio` and the two HyperCore
subcommands takes `--block` (default `finalized`):

| Subcommand | `--connector` | Request |
| --- | --- | --- |
| `pool` | `uniswap_v3`, `pancakeswap_v3`, `aerodrome_slipstream`, `aerodrome`, `curve` | `--chain` and `--pool`, or `--config`; optional `--depth-bps` for the first three |
| `pool-price` | `uniswap_v4` | `--chain`, `--currency0`, `--currency1`, `--fee`, `--tick-spacing` and optional `--base`, or `--config` |
| `lending-reserve` | `aave_v3`, `sparklend`, `aave_v4`, `compound_v3`, `morpho_blue` | `--chain` and the connector's selector flags, or `--config` |
| `lending-position` | same as `lending-reserve` | the same selection, plus `--account` |
| `liquidity-position` | `uniswap_v3`, `pancakeswap_v3`, `aerodrome_slipstream`, `uniswap_v4`, `curve`, `aerodrome` | `--chain`, `--pool` and `--token-id` for the first three, `--chain`, the pool key flags and `--token-id` for `uniswap_v4`, `--chain`, `--pool` and `--account` for `curve` and `aerodrome`, or `--config` |
| `perp-position` | `gmx_v2` | `--config`, `--account` |
| `perp-markets` | `gmx_v2` | `--chain` and `--query`, or `--config` |
| `balance` | none | `--asset` (CAIP-19 asset type), `--account` (repeatable) |
| `resolve` | none | `--chain`, `--query` (symbol or address); optional `--limit` |
| `dex-pools` | none | `--chain`, `--token`; optional `--quote`, `--connector`, `--limit`, `--offset`, `--depth-bps` |
| `lending-markets` | `aave_v3`, `sparklend`, `aave_v4`, `compound_v3`, `morpho_blue` | `--chain`; optional `--asset` (required for `morpho_blue`), `--collateral`, `--include-unlisted` (`morpho_blue`), `--limit`, `--offset` |
| `hypercore-markets` | none | optional `--query` (a coin), `--limit`, `--offset`, `--include-delisted`; no `--block` |
| `hypercore-account` | none | `--account` (repeatable); no `--block` |
| `portfolio` | none | `--account` (repeatable, at most two); optional `--include-unlisted`; no `--block` and no transport |

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
optional assertions. A deployment the connector has not reviewed is refused. An
Aave V3 Pool is admitted by the code it runs: the Pool implementation of revision
11 or revision 12 on its chain, by code hash, reporting that same revision, with
the reserve's aToken and variable debt token implementations by code hash. A Pool
pin admits its code on its own chain only. A Pool
or token running other code is refused with `deployment_changed`. A
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
It is for `ax` reads, for every direct action, previewed or executed, and for
`ax recover`. The reviewed list is in code, and each command's `--help` prints
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

An endpoint for a chain does not make an action available there. A direct action
runs only for a connector and chain that declare it, which `almanak support` lists
as `direct_actions` and [Operator actions](#operator-actions) describes; it runs
through any endpoint listed for that chain. On a chain with a
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
| `ax lending-reserve --connector aave_v3`, for the chain's wrapped native coin | Measured on 2026-11-20, with every Pool at revision 12: served on every listed pair of the connector's seven chains. |
| `ax perp-markets --connector gmx_v2`, by market address | Served on Arbitrum through `arbitrum` and on Avalanche through `publicnode`. |
| `ax perp-position`, and `ax lending-reserve` for SparkLend | Not measured. |
| `ax lending-markets` for `aave_v3`, `sparklend` and `aave_v4` | The record is read with calls at the pinned block and listed on each chain the connector supports. Three rows of each listing were read: the SparkLend and Aave V4 rows were `verified`. The Aave V3 rows were read again on 2026-11-20, with every Pool at revision 12: `verified` on every listed pair except `drpc` on Base and Optimism, where each row was `refused` with `http: eth_getCode: HTTP 429` or `http: eth_getStorageAt: HTTP 429`. On Avalanche the Aave V4 listing named 8 Spokes in `source.unreadable`, each with `rpc: eth_call: RPC rejected the requested read (JSON-RPC -32000)`, and listed the others. A Spoke counts as holding no reserves only when its revert is reported as JSON-RPC 3, so these may be Spokes without reserves; that was not verified. |
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

#### Portfolio

`ax portfolio` is an overview of an account across chains, as a wallet vendor
reports it:

```bash
almanak ax portfolio --account 0x...
```

It asks Rabby's backend, per account, which chains the address has value on, the
tokens it holds on each of those chains, and its positions in the protocols the
vendor indexes on every chain. That is two requests and one more per chain. The
tokens are the vendor's current listing of each chain; its stored all-chain list,
which can predate the account's latest transactions, is not read. It needs no key, opens no chain endpoint and takes
no transport flag, and it is asked from the calling process, not through the read
gateway. `--account` repeats for up to two accounts, which keeps one command under the
vendor's throttle.

Every figure is the vendor's. The report says so in `source` (`provider` is
`rabby`, `verified` is `false`), reads no block, and names the time each answer
was received in milliseconds under `received_at`: `chains`, `positions`, and
`tokens` as the oldest of the token answers, or null when no chain was read. It is an overview for display:
no row of it is identity, a price input or the basis of an action, and a market
is read on chain with the subcommands above before anything acts on it.

Each account reports:

- `tokens`: each wallet token with its `amount` as a decimal string, `raw_amount`
  in base units, the vendor's `price_usd` and their product `value_usd`. `chain`
  and `asset` are the CAIP references the vendor's chain label and token ID
  imply, unverified. Both are null on a chain without a verified network
  definition, and `asset` is null where the vendor's token ID is not a contract
  address;
  `vendor_chain` and `vendor_token` keep the vendor's own words. `vendor_flags`
  carries the vendor's `core`, `verified`, `scam` and `suspicious` marks.
- `tokens_unlisted`: how many tokens the vendor does not mark as core. They are
  left out of the rows and the totals unless `--include-unlisted` is given. Only
  the chains read are counted.
- `chains_read`: the vendor's labels of the chains whose tokens were read, largest
  value first: every chain the vendor's summary does not value at zero, including
  one whose value it does not state, up to eight per account. `chains_unread`
  names each such chain past that bound with the vendor's `value_usd` for the
  whole chain, or null, and `tokens_usd` and `total_usd` are then null. A chain
  the vendor values at zero is not read, so a token the vendor cannot price there
  is not listed and does not make a total null.
- `positions`: each position with its `protocol`, the vendor's `kind` (such as
  `Lending` or `Rewards`), `asset_usd`, `debt_usd` and `net_usd`, the vendor's
  `health_rate` where it states one, and its `legs`: each token list of the
  vendor's detail under its own name, such as `supply`, `borrow` and `reward`. A
  leg carries a decimal `amount` and no base units. A position whose detail the
  vendor shapes otherwise has no legs, only its dollar figures.
- `totals`: `tokens_usd`, `positions_usd` and `total_usd`. A total is null when a
  row it sums has no value or a chain is unread. The vendor writes a price of zero for a token it
  cannot price; that is read as no price, never as a value of zero.

This is the vendor's own wallet backend, not a documented public interface. It
can change or refuse without notice, it sees every address asked about, and it
throttles a caller's network address: after about 25 requests in ten minutes,
every request was refused for longer than ten minutes. A throttle is the
`rate_limit` refusal, any other failed answer is `unavailable`, and an answer
that is not rows of the expected kind, a row that states no chain, token or
amount, a chain listed twice, or a token row on a chain other than the one asked,
is `malformed`. Nothing is substituted for a refused overview.
