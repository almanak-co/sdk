# Daily SDK QA

The bench runs the actual reference strategies through the existing fork harness
and backtest CLI. It validates coverage against the trusted connector support
contracts before starting any provider work.

Candidate integrations are not supported capabilities. The QA plan derives support
from trusted connector declarations. New pairs require actual reference outcomes and prepared offline
backtests before support promotion.

## Run locally

Install the locked environment and hook, then inspect the complete matrix:

```bash
uv sync --locked
uv run --locked pre-commit install
uv run --locked python -m scripts.qa_bench --plan --output artifacts/qa-plan
```

Run all cases with explicit read-only provider bindings:

```bash
make fixtures
uv run --locked python -m scripts.qa_bench \
  --env-file .env --output artifacts/qa-daily
```

`make fixtures` places the pinned evidence copies under `tests/fixtures/evidence/`
from the local evidence tree. The GMX keeper drivers read those copies, so run it
before the bench or any manual `pytest --fork-env` run in a fresh checkout and after
the manifest changes. `make qa-daily` runs it first.

Use a new output directory for each invocation. Existing output is never reused
as evidence for a new run. Plan mode performs no provider calls. Linux needs
Anvil and working Bubblewrap/unprivileged namespaces. macOS uses sandbox-exec;
Full QA runs locally with retained evidence. On a pull request, hosted CI runs
lint, types, only the test modules the pull request added or modified with
evidence-dependent modules skipped, the CRAP gate for the functions it changed
on the coverage of those modules alone, and CLI smoke checks on Linux and
macOS. It runs neither the rest of the offline suite nor the 87% statement
coverage floor, which `make test` and `make crap` cover locally; the whole
offline suite runs hosted on release tags only. The
`Fork Qualification` workflow (`.github/workflows/fork_qualification.yml`) runs
the discovered fork lifecycle and, where the checked-out connectors declare
production EOA support, the production rehearsal for named references on hosted
runners against the repository's provider key; it is the fork qualification a
reference listed in `tests/strategy_fork/rehearsal.py::UNREHEARSED` needs before
it joins the default rehearsal selection. It also runs
[weekly](#hosted-weekly-fork-qualification) on `main`. Each run retains the case evidence as a
workflow artifact and spends no funds. GMX keeper drivers settle requests with
authentic signed reports read from the pinned evidence copies in
`tests/fixtures/evidence.json`, which the workflow downloads from the staging evidence
bucket before pytest. The manual `Cell QA Matrix` workflow runs one
connector/chain cell's QA cases on hosted runners in parallel; see
[Hosted cell matrix](#hosted-cell-matrix) for what it does and does not establish.

For a focused diagnostic run, select exact case IDs from the plan:

```bash
uv run --locked python -m scripts.qa_bench \
  --env-file .env --output artifacts/qa-focused \
  --case backtest/pinned/uniswap_v4_trading/economic
```

`--lane` selects offline, pytest, fork, faults, backtest or blocked. Unselected cases remain
visible as skipped, so a focused run is never displayed as a passing full bench.
The exit code for an explicit selection describes its selected cases. The full
run exits nonzero if any required case did not pass. Unexpected pytest skips and
empty test collections fail the corresponding case.

## Inputs and coverage

The current plan validates 82 connector/chain/profile/model/action obligations
in 93 cases, including eight explicit blockers for unregistered rollout references.
The executable cases include seven references through both Safe/Roles operator variants
and EOA on pinned and newly resolved forks; twelve backtest reference/model
combinations with pinned, recent and short native-cadence variants; the full
offline gate; focused fork faults and multi-chain outages; support refusals; and
supplemental prepared-model action checks. Counts change with declared support
and reviewed proof mappings, not by hiding failed cases.

The catalog in `tests/qa/catalog.py` records exercised actions and model
revisions. Backtest mode is an explicit field checked against the declared model,
not inferred from its revision label. Required support comes from the existing
connector composition.
Adding a supported chain, profile, action or model without a proof fails plan
validation. Two exceptions hold for Across alone, each from the connector's own
list in `almanak.connectors.across.support`: the chains no reference sends from
(`declared_without_proof`), and the Safe/Roles account profile on every chain
(`profiles_without_proof`), which is declared with no fork run. The catalog
leaves those obligations and cases out by name and owes the EOA profile of the
Arbitrum cell in full. Every discovered strategy needs a catalog mapping. Supplemental
model tests are labeled component proofs; they are not additional strategy
lifecycles.

An unregistered candidate reference has an explicit blocked case and contributes
no supported-action claims. The case remains in the full result denominator;
omitting it or clearing its blocker fails catalog validation. Candidate blockers
prevent a passing full bench even when all executable cases pass. Promoting its
chain support requires replacing the blocker with reviewed proof mappings.

Author economic choices and named markets remain in each strategy's `config.yaml`.
Backtests use a separate backtest configuration. Every reference keeps providers,
account bindings and fork fixtures out of its author files: QA derives one operator
file per case from
`examples/operators/`, varying the pinned or fresh anchor, the Safe or EOA profile
and the shared or distinct operator there, and drives its fork lifecycle through the
public `almanak run`, `pause`, `resume`, `teardown` and `shutdown` commands.
QA writes and validates full resolved configurations, preserving all changes as
evidence:

- Fresh forks resolve one block identity per chain before case execution.
- Recent historical windows preserve the configured duration and cleanup period.
  The explicit source lag in `tests/qa/policy.json` is currently two hours on
  each chain, measured after the cleanup endpoint. Missing history is refused;
  the bench does not shorten the window or choose another provider.
- Native-cadence probes use a ten-minute window and ten-minute cleanup, with the
  author interval and explicit 120-second hold / 60-second collection parameters
  where those parameters apply. The ordinary pinned/recent variants retain
  their authored holding parameters and explicit backtest cadence overrides.
- Detailed Aave history uses an explicit debt-free author variant and an hourly
  override in the long runs. Its native probe has no interval override.
- The EOA rehearsal harness supplies the explicit Ethereum test gas policy in
  `tests/strategy_fork/rehearsal-policy.json` when a reference has no production
  policy. Provider selection still comes from that reference. Generated
  production configurations run only against identified local forks with
  Foundry test keys; this does not change author production authority.

Prepared backtests use a new dataset directory for cold preparation, then repeat
preparation and run execution/reproduction with OS network isolation. Local IPC
and loopback gateway calls remain available. Detailed history captures online
reads and reproduces them in the same network isolation. Successful cases must
show closure, delivered outcomes and the expected economic actions on each chain.
Uniswap prepared runs also compare integer calculations with independent Quoter
reads at the same sampled blocks, in both directions and several sizes.

Existing frozen fixture, recovery, integer boundary, vendor-source and model
regressions run in the full offline gate. The online fault lane runs `tests/fork`
and the multi-chain outage tests, including Aave liquidation/contract comparisons.

## Results and limits

`public/results.json` is the versioned complete matrix. `summary.md`, `index.html`
and `junit.xml` present the same outcomes. `private/` holds working files and
credentials generated by the harness. Both directories remain local. Evidence
export preserves dataset bytes or refuses to export a file containing known
credential material. Logs are redacted. A refused required artifact blocks the
case. The source archive and hashes retain the tested implementation, including
uncommitted changes; source changes during execution invalidate passing results.

`public/proof-ledger.json` links every declared connector, chain, action, account
profile, model and protocol variant obligation to its scheduled case IDs. An
obligation passes only when every mapped case passes. Failed, skipped, unstarted
or missing cases remain outstanding, including separate fresh-input and recovery
cases. The ledger preserves the tested source identity. It does not turn an
undeclared candidate into supported scope or replace the 96-cell rollout inventory.

Each case has isolated journals, gateways, forks and retained evidence. Provider
cases run serially initially; `--workers` controls only the offline pytest worker
count and accepts `auto` (the default) or an explicit positive integer.
The default runs the combined coverage/CRAP gate with `-n auto`. The fault lane
uses one pytest worker. The runner does not automatically
retry failures. Existing gateway collection limits retain their own explicit
bounded provider retry behavior. This is deliberately conservative until measured
complete daily runs establish useful concurrency and timeout settings.

Per-command records measure cold preparation, cached preparation, execution and
reproduction wall time independently. Quoter comparisons and source resolution
remain separate activities. The SDK's retained dataset and progress reports
provide provider activity, model and cadence detail. No throughput or ETA claim
is inferred from test counts.

The case timeout defaults to one hour. This is an operational bound, not a
measured runtime target. A backtest case's execute and reproduce steps share
that bound; dataset preparation keeps its own thirty minute bound. Timeouts, missing providers,
unstarted cases and source-resolution failures remain visible. Unknown failures
are labeled without guessing that the SDK or provider caused them.

Pass `--previous path/to/results.json` to write `changes.json`, comparing case
status/category with the previous run. Unchanged cases are absent from this file.
Supply the preceding local result explicitly. The bench sends no Slack/email
messages. An absent prior artifact is reported as a fresh comparison baseline.

## Hosted cell matrix

The manual `Cell QA Matrix` workflow (`.github/workflows/cell_qa_matrix.yml`)
runs one connector/chain cell's QA matrix on hosted runners, one job per case,
through the same `scripts.qa_bench --case` entry point as a local run. It spends
no funds and installs no production permissions: fixture accounts are public
Foundry test keys on identified Anvil forks of the repository's read-only
Alchemy provider. Dispatch it with a connector ID and a CAIP-2 chain, or with
explicit case IDs:

```bash
gh workflow run cell_qa_matrix.yml --ref <branch> \
  -f connector=uniswap_v3 -f chain=eip155:56
```

Selection. `scripts/ci/cell_qa_matrix.py` selects every catalog case whose
obligations name the connector on the chain, which are the cases the proof
ledger maps to the cell. The `fork` and `backtest` lanes run by default and the
`pytest` lane can be added. The `offline`, `faults` and `blocked` lanes are
refused: the offline gate needs the retained evidence tree, the `faults` cases
run through the local bench (`fork/faults` is every connector's fault suite, and
a cell's own fault case, such as the Across revoke proof, is dispatched with the
`nodes` input instead), and a candidate blocker cannot pass. A selected
cell case outside the chosen lanes stays listed as not run. The reverse-fault
entries are the reversal tests in `tests/fork/test_*reversal*.py` that are
parametrized by one of the cell's references, or whose module declares it as
`REFERENCE`. Each one runs as its own pytest job. When no such test exists, the
run reports that gap and does not pass. The `nodes` input adds explicit
`tests/fork` or `tests/strategy_fork` node IDs, for example a cell's
recovery-foundation nodes. An unknown case, an empty cell and more than 256
entries are refused.

Execution. Each job runs on `ubuntu-latest-l` with Bubblewrap and Anvil and
installs the locked environment on CPython 3.12.13. At most eight jobs run at
once. Every job is a separate deployment with its
own gateway, so the per-deployment provider caps apply to each job, and the
Alchemy plan limit is the only ceiling they share. Nothing is retried.

Source and input identity. Every job checks out the run's commit. Before and
after its entry, each job records the commit, the bench's tree hash, the lock
hash, the Python build and the tool versions. The aggregate job recomputes the
tree hash from a clean checkout of the same commit. Any mismatch, or any change
during a job, blocks that entry. The plan job resolves the fresh fork origins
and the input time once and passes them to every job through
`qa_bench --frozen-inputs`. All fresh forks in one run therefore start from the
same block per chain, and all recent historical windows end at the same instant,
as in one local run.

Time budgets. Fork and pytest-lane cases keep the bench's one-hour case bound,
in a 90-minute job. A backtest case gets its cold-preparation budget from
`tests/qa/policy.json` plus 7200 seconds. The policy budget is 1800 seconds by
default and 7200 seconds for the five composed references it lists; detailed
historical cases prepare nothing. The extra 7200 seconds cover cached
preparation, execution, reproduction and model comparisons, and match the bound
the local runbook recommends for slow windows. The job timeout adds 30 minutes
for setup and upload. The largest case, a 7200-second native preparation, gets
14400 seconds in a 270-minute job, inside the six-hour hosted job limit. The
planner refuses any case whose bound cannot fit. The preparation budgets are
unchanged. Each job prepares its dataset cold from the provider, exactly as a
local case does, so no retained dataset is needed or reused. These are
operational bounds, not measured runtimes, and a timeout remains a blocked case.

Evidence. Only the bench's sanitized `public/` tree leaves the runner. The
bench's export boundary redacts logs, drops credential files and refuses any
other file that contains a known secret. `private/` never leaves the runner.
Reversal-node files pass through the same export. Before upload, each job scans
file names and bytes, including gzip members, for the provider keys. The key
values come from the environment only. On a match, the job uploads nothing but
its record, and the entry is blocked as unsafe evidence. The per-case source
snapshot is dropped from the job artifacts. The aggregate archives one
`source.tar.gz` for the verified commit. All artifacts are retained for 90 days.

Results. The `aggregate` job writes `results.json`, marked
`"execution": "hosted_cell_matrix"`, and a job summary. The summary has one row
per planned entry, the cell's cases that were not run and a cell-scoped proof
ledger. The workflow fails unless every planned entry passed on one source. A
skipped, missing, timed-out, unsafe or mixed-identity entry keeps the result
`incomplete_or_failed`.

### What a hosted cell run establishes

It can satisfy the first item of the
per-cell evidence package:
the cell's fork and backtest cases and its reverse-fault node, passed on one
commit. That commit must be the frozen integrated source, or the later changes
must not touch the code paths the proof exercises. It is not the local
retained-evidence gate and does not establish:

- Pytest-lane component proofs that read retained evidence, such as
  `model-actions/uniswap_v4/eip155:56`. They cannot pass on hosted runners;
  run them locally with the evidence tree present.
- The full offline suite with retained evidence, combined coverage, CRAP at most
  30, Ruff, formatting and mypy (package item 2). Hosted CI's offline suite skips
  the evidence modules and is not a substitute.
- The recovery-foundation nodes (item 3), unless they were passed as `nodes`
  and do not need retained evidence. Otherwise they remain local work.
- Append-only sanitized evidence archival with checksum verification (item 4).
  Download the artifacts, verify their digests and seal them locally. The
  workflow writes nothing to the evidence bucket.
- GMX cells. Their keeper drivers settle requests with signed reports read from the
  pinned evidence copies, which this workflow does not fetch, so those cases fail here.
  The `Fork Qualification` workflow fetches them.

## Hosted weekly fork qualification

The `Fork Qualification` workflow also runs every Sunday at 03:17 UTC on `main`.
The scheduled run has no inputs: `scripts.ci.fork_qualification_matrix
--production-cells` selects every converted reference with a market on a
connector/chain cell that the checked-out connectors declare for production
(`almanak support`), and runs the lifecycle and the rehearsal for each. It
refuses an empty selection and a declared cell that no reference exercises.
Keeper-settled GMX references are selected like any other and replay their
signed reports from the pinned copies. At most 24 references run at once.

Each reference runs at the block its fork operator file pins. The Aave V3
references pin blocks where every Pool runs revision 11, so this workflow does not
exercise Pool revision 12, which every listed Pool has run since 2026-11-11.
Revision 12 is rehearsed locally with `--fork-env`, at blocks after the upgrade:
`tests/fork/test_ax_pool_lending.py` runs every direct action on all seven chains,
and `test_aave_v3_lifecycle_at_pool_revision_12` in
`tests/strategy_fork/test_strategies.py` runs the Safe/Roles reference lifecycle
from a copy of the operator file with only the anchor moved, on every chain but
Avalanche. There the reference supplies WAVAX, which the Pool holds above its
supply cap, so the entry is refused with `supply_cap` and the direct actions
supply WETH.e instead. Neither rehearsal is hosted Fork Qualification.

Open gates of the Aave V3 cells, on top of the ones every cell shares:

- No Aave V3 cell has a hosted Fork Qualification at Pool revision 12.
- Avalanche (`eip155:43114`) declares production for `safe_roles`, and its
  reference `aave_v3_avalanche_lending` cannot enter at any block since the
  upgrade: the Pool caps WAVAX supply at 12,000,000 and holds about 15,300,000.
  The cell has no Safe/Roles lifecycle at revision 12, local or hosted.
- `test_safe_deployment_created_before_the_pool_upgrade_plans_nothing_after_it`
  in `tests/fork/test_aave_v3_cleanup.py` holds what a Safe/Roles deployment
  created at revision 11 does once its Pool runs revision 12: the gateway refuses
  its teardown plan and the approval it held stays. The plain-account case beside
it revokes and closes. See
  [the authority check](operating.md#production).

The `select` job summary lists each cell with its references. When a scheduled
run fails, the `report` job opens the issue "Weekly Fork Qualification failed on
main", or comments on it if it is open, naming the jobs that did not pass; the
next passing scheduled run closes it. A failed run is not proof of support, and
a passing run is not a complete per-cell evidence package: it holds no
reverse-fault case, recovery-foundation node, backtest or offline gate.

## Public endpoint qualification

A (name, chain) pair is in the reviewed `--public-rpc` list only after it passed
every check of `scripts/qualify_public_rpc.py` in repeated full runs. A pass is a
point-in-time result: a transient refusal fails a run as it would fail a command,
so a healthy endpoint can fail one run and a listed one can still refuse a read.
The tool is live and explicit: it connects to the endpoints, so the offline suite
never runs it. It reads no env file and no provider key, and it signs and sends
nothing.

```bash
uv run --locked python -m scripts.qualify_public_rpc
uv run --locked python -m scripts.qualify_public_rpc --name drpc --chain eip155:8453
uv run --locked python -m scripts.qualify_public_rpc \
  --candidate bnbchain eip155:56 https://bsc-dataseed.bnbchain.org
```

Without arguments it checks every listed pair, one after another with a pause.
`--candidate NAME CHAIN URL` checks an endpoint that is not listed, and is not
combined with `--name` or `--chain`. It prints one JSON report that names hosts
only and exits 1 when any checked endpoint fails; a wrong command line exits 2
before any endpoint is opened. A run over the whole list is expected to exit 1 on
one pair: `publicnode` on Avalanche fails `reads_at_age` in most runs and stays
listed, as the operating guide states. The checks drive the SDK's own readers through a
sample of the reads an observation and a direct action make. They do not cover
every method a connector calls, and the state they ask for is a floor, not the
oldest state a command can read:

| Check | What the endpoint must serve |
| --- | --- |
| `identity` | The proof every public read asks for: the chain number, the pinned genesis block and a head no older than 120 seconds. Nothing else runs when it fails. |
| `production_guard` | What a direct action verifies before each privileged step: an HTTPS endpoint, the chain, a client that is not a test node and the genesis again. |
| `reads_at_finalized`, `reads_at_latest`, `reads_at_number` | Call, code, storage and balance reads that name the block's hash with `requireCanonical`, at `finalized`, at the default `latest` and at a numbered block 64 behind the head. A probe call returns the block timestamp the provider's EVM saw, which must be the pinned block's. |
| `reads_at_age` | The same reads at a block at least 360 seconds behind the head. A direct action reads its observation again at the observation's block and reconciles a mined step at the receipt's block, so it needs state that old: the 120 seconds an observation may age, plus an allowance of 240 seconds for one step to be planned, signed, mined and reconciled. The result states the block and its `age_seconds`. |
| `submitter_state` | The head by its number, no older than an action accepts, and the same nonce at `latest` and `pending`. |
| `fee_quote` | The fee quote of a direct action: gas estimated at the pinned block's hash, a one-block fee history with one reward percentile and the base fee the chain's policy bids on (the next block's, or the observed block's on Avalanche), and the node's gas price on BNB. |
| `op_fees` | On Base and Optimism, the pinned GasPriceOracle and L1Block revisions and their fee parameters. |
| `receipts` | A mined transaction and its receipt with the fields reconciliation reads, on a block that is canonical by number, and `null` for a transaction hash the provider does not know, as a transaction that is not mined yet answers. |

`reads_at_age` measures age by the block timestamps the endpoint itself reports,
not by a block time stated for the chain. It reads the block 64 behind the head,
estimates from the two timestamps how many blocks make 360 seconds, and reads that
block's header. A block whose timestamp is 360 to 420 seconds behind the head's is
taken; otherwise the estimate is corrected from the timestamps read so far, for at
most six header reads, and the youngest block found that is old enough is used. On a
chain with very uneven block times that block can be much older than 420 seconds, so
an endpoint that keeps only a little more than the floor can fail there. An
endpoint that keeps less state fails the check with the reader's refusal, for a
geth node `the endpoint does not serve state at that block`.

The 360 seconds are a floor the tool checks, not a promise. The allowance is a
stated figure: steps on BNB Chain over a slow link were measured at 144 to 190
seconds. A step that takes longer reads older state, and `ax recover` reads a kept
record's blocks whenever it is run, which can be hours later. A listed endpoint can
refuse those reads.

A refusal is reported in the SDK's own words, never the provider's, and an answer
of the wrong shape fails its check as `malformed`. Each failed check carries
`rate_limited` and `transient`. An endpoint whose only failures are rate limits is
checked again after a pause, up to `--attempts` times; one that stays rate limited
fails. `transient` is the reader's own mark on a failure it re-sent until its
bounds ran out, such as a transport failure, an HTTP 5xx or an HTTP 408; the tool
reports it and does not retry it. A refusal the reader does not classify as
transient, such as an HTTP 403 or a provider's own error code, is reported as
definite even when the next run passes. Read a single failure with that in mind,
and run the tool again before changing the list. Whether an endpoint accepts a submitted
transaction is not checked, because the tool sends none.

## Opt-in live checks

Two further levels run against live mainnet. Neither is part of default pytest,
`make test`, `make check`, the QA bench or any pull request or release
workflow, and neither may be added to them: public endpoints rate-limit and
stall, so their result cannot gate a merge. Both live in `scripts/live_qa/` and
run from a checkout.

They ask a different question from the
[public endpoint qualification](#public-endpoint-qualification) above. That tool
decides whether an endpoint belongs in the reviewed list, by driving the SDK's
readers through the RPC methods a command needs. The live checks take the list as
given and run the `almanak ax` commands themselves against real markets, to find
what the SDK gets wrong on today's chain state.

| Level | Costs | Needs | Run it |
| --- | --- | --- | --- |
| Live read checks | Nothing | Network access | Any time; weekly is reasonable |
| Live write check | **Real gas, on mainnet** | Your own funded wallet, fee ceilings, a typed confirmation | For a release or after a protocol upgrade. Never in CI, never scheduled, never often |

The [`almanak-qa` skill](https://github.com/almanak-co/sdk/blob/main/skills/almanak-qa/SKILL.md)
teaches an agent both levels, with the write level behind the person's
confirmation of the preview.

### Live read checks (free)

The offline suite reads pinned fixtures and the fork lanes read through a
provider key, so neither sees what a user without a key sees today: an on-chain
upgrade the SDK's pins do not know, a market the venue names differently, or a
public endpoint that answers differently. The read level runs `almanak ax`
reads as that user would, one process per command, with `--public-rpc` naming
each endpoint of the reviewed list on each chain it serves. The provider key and the session read gateway are removed from each
command's environment. It needs no key, wallet or funds.

```bash
uv run --locked python -m scripts.live_qa reads --list
uv run --locked python -m scripts.live_qa reads --output artifacts/live-reads
```

`--list` prints the plan, contacts nothing and writes nothing. Use a new output
directory for each run. `--endpoint` and `--case <text>` narrow a run. `--almanak-command` selects the
CLI under test; the default is the checkout's, and the published package runs as
a user installs it:

```bash
uv run --locked python -m scripts.live_qa reads --output artifacts/live-reads \
  --almanak-command "uvx --from almanak==<version> almanak"
```

Cases. `scripts/live_qa/read_cases.py` names known markets and public contracts.
On Ethereum and Avalanche: native and ERC-20 balances, Aave V4, Aave V3,
SparkLend and Morpho Blue reserves and listings, a Compound V3 reserve, Uniswap
V3 and V4 pools and GMX V2 markets. On every other chain of the reviewed list,
taken from the SDK's own network record so that a newly listed chain is covered
without an edit: the native balance, and the wrapped-native Aave V3 reserve where
Aave V3 has a Pool. Beside them, a HyperCore market and the refusals a public endpoint
must give (a vendor-key lookup, a chain the named endpoint does not serve, and
HyperEVM, which no endpoint serves). Each case states the correct
behavior. Where practical its output is compared with direct `eth_call`,
`eth_getBalance` and block-header reads of the same endpoint at the block the
command pinned, made by `scripts/live_qa/rpc.py` without the SDK's reader. A
reserve is held to what its Pool, Spoke, Comet or Morpho core records for it, and
each listed market to the same record: an Aave V4 market to its Spoke's reserve
under that ID, an Aave V3 or SparkLend market to the Pool's reserve list, a Morpho
market to the core's parameters for its ID. That a market is `verified` is the
SDK's own statement; the direct reads check the identity it reports, not every
fact the SDK verified. The GMX bare-label cases and the HyperCore case have no
direct read.

Outcomes. Each case on each endpoint ends as one of:

- `pass`: the command behaved as the case states and every direct read agreed.
- `fail`: it did not. A failure is not retried.
- `blocked`: the endpoint did not answer: a rate limit, a rejected read, a
  timeout, an unanswered direct read, or a command that exited 0 but reported an
  endpoint refusal for part of its output (`observation_error`). Such a case is tried three times, 5 and
  then 15 seconds apart, with 90 seconds per command (`--attempts`,
  `--backoff-seconds`, `--timeout-seconds`). After three consecutive blocked cases
  on one chain, the endpoint's remaining cases on that chain are reported blocked
  without being run. A blocked case is not a pass and not a finding about the SDK.

The run writes `results.csv` and `summary.md` and exits 0 when every case passed,
1 when a case failed and 2 when none failed but one was blocked. With
`--known-issues-ok`, a failure that reproduces its case's known issue does not
count toward the exit code, so a routine run is red only for something new; the
report lists those failures all the same.

Known issues. A defect found by hand stays in the catalog as a case with a
`KnownIssue`: the case still states the correct behavior and still fails, and the
report's `known_issue` column says whether the known defect reproduced, was not
observed, or no longer reproduces, in which case its marker is removed. The
catalog currently holds two:

- GMX V2 on Avalanche indexes its markets by `BTC.b` and `WAVAX`, so the
  `BTC/USD` and `AVAX/USD` labels match nothing. `BTC.b/USD [BTC.b-USDC]` and
  `WAVAX/USD [WAVAX-USDC]` resolve and are held as passing controls.

- `ax lending-markets --connector aave_v4` on Avalanche reports the Spokes that
  hold no reserves as `unreadable`. Such a Spoke reverts `getReserveCount()`.
  The public endpoint reports that revert with JSON-RPC code -32000, and the
  listing recognizes a revert only by code 3, so it takes the answer for an
  endpoint failure. The case confirms each revert with a direct read before it
  fails; a Spoke the endpoint really did not answer still blocks it.

Not covered: `ax lending-markets --connector compound_v3` reads the
Configurator's whole event history in one request, which the public endpoints
tried refuse; `ax resolve` and `ax dex-pools` need a vendor key; and `ax perp-position`
takes a fully resolved GMX market document that no `ax` command prints.

The same cases run under pytest with an explicit flag. Without `--live-public`
the `tests/live` directory is not collected at all, and a test file named by its
path is skipped. Run it serially, since
parallel workers share each endpoint's rate limit. A blocked case is reported as
skipped, with the same three-case breaker as the script. A known issue that
reproduces is an expected failure (`xfail`); one that no longer reproduces fails
until its marker is removed, and any other failure fails:

```bash
uv run --locked pytest tests/live --live-public -p no:xdist
```

The `Live Public Reads (optional)` workflow
(`.github/workflows/live_public_reads.yml`) runs the read level on a hosted runner
when dispatched by hand, with `--known-issues-ok`. It uses no secret, is called by
no other workflow and has no schedule; its file holds a commented weekly cron for a maintainer who
wants one. There is no workflow for the write level.

### Live write check (spends real funds)

> **This spends real funds on mainnet.** It sends up to four transactions from
> your own wallet and pays gas for each. Run it for a release or in the days
> after a protocol upgrade. Do not run it in CI, on a schedule or often. The
> repository ships no wallet and no key for it, and no workflow runs it.

Fork rehearsals force a fork to look like mainnet, which a user cannot do, so
they do not prove that `ax supply` and `ax withdraw` work against the live
protocol. The write level does: a tiny Aave V4 supply followed by a full
withdrawal through `almanak ax`, with the preview first.

It refuses to start, running no command and contacting nothing, unless all of
these hold. Every unmet gate is named at once.

1. `ALMANAK_QA_SPEND_REAL_FUNDS=1` is set, on the command line of that run.
2. `--wallet` names your own wallet directory, written by `almanak wallet create`.
   The check never creates a wallet and never opens a keystore or a password
   file; `almanak ax` signs. A wallet created without `--generate-password` or
   `--password-file` asks for its password on the terminal, once for the supply
   and once for the withdrawal. Without a terminal, as under an agent, nobody can
   answer, so the check then requires `wallet.json` to name an existing password
   file.
3. `--max-gas-price-wei` and `--max-transaction-fee` are given, as `almanak ax`
   takes them, and neither exceeds the check's own limit for the chain (100 gwei
   and 0.02 ETH on Ethereum, 200 gwei and 0.2 AVAX on Avalanche).
4. `--market` is one of the reviewed markets below and `--amount` is at most its
   largest accepted size.
5. Exactly one of `--public-rpc` and `--env-file` names the transport, and a
   public endpoint serves the market's chain. On Avalanche the one listed
   endpoint, `publicnode`, does not serve state as old as the
   [floor](operating.md#public-endpoints) requires: a step not reconciled within
   about 100 seconds can be refused, and its kept record then needs
   `ax recover` through `--env-file`.
6. Neither `CI` nor `GITHUB_ACTIONS` is set, to any value, an empty one included.
7. `--output` is a new or empty directory that can be created. A report is never written over an
   earlier one, which may hold the only record of the transactions a run sent.

| `--market` | Chain | Asset | Suggested `--amount` | Largest accepted |
| --- | --- | --- | --- | --- |
| `ethereum-usdc` | `eip155:1` | USDC | `100000` (0.1 USDC) | `1000000` (1 USDC) |
| `ethereum-weth` | `eip155:1` | WETH | `100000000000000` (0.0001 WETH) | `1000000000000000` (0.001 WETH) |
| `avalanche-usdc` | `eip155:43114` | USDC | `100000` (0.1 USDC) | `1000000` (1 USDC) |
| `avalanche-wavax` | `eip155:43114` | WAVAX | `10000000000000000` (0.01 WAVAX) | `100000000000000000` (0.1 WAVAX) |

`--max-transaction-fee` caps each transaction, so a round trip can cost up to
four times it in gas. The supplied amount returns to the wallet, plus interest.

No gate reads a balance. Before running it, check that the wallet holds the
amount in the market's asset and gas for every transaction, the withdrawal's
included. An underfunded wallet passes the gates; one short of gas after the
supply leaves the amount supplied.

Preview first. Without `--execute` the check plans the supply, prints the
preview and the confirmation phrase, and signs nothing:

```bash
ALMANAK_QA_SPEND_REAL_FUNDS=1 uv run --locked python -m scripts.live_qa writes \
  --market ethereum-usdc --amount 100000 --wallet <wallet directory> \
  --public-rpc publicnode \
  --max-gas-price-wei 20000000000 --max-transaction-fee 10000000000000000 \
  --output artifacts/live-writes
```

Then execute, on the same transport, with a new output directory. At a terminal
the check previews again and asks you to type the phrase,
`supply <amount> <market> and withdraw`; a non-interactive caller passes it with
`--confirm`. Anything else signs nothing. The phrase is predictable from the
market and amount, and a `--confirm` given at the start is accepted after the
preview without a further answer. Passing it approves the round trip before this
run's preview is shown, so pass it only after reading the first run's preview.

```bash
ALMANAK_QA_SPEND_REAL_FUNDS=1 uv run --locked python -m scripts.live_qa writes \
  --market ethereum-usdc --amount 100000 --wallet <wallet directory> \
  --public-rpc publicnode \
  --max-gas-price-wei 20000000000 --max-transaction-fee 10000000000000000 \
  --output artifacts/live-writes-executed --execute
```

After the confirmation it supplies, previews the withdrawal and withdraws the
whole supplied balance, waiting for each transaction; it does not time out or
kill an executing command. It exits 0 only when both actions completed and the
position is empty, and writes `summary.md`, with each transaction hash, and
`writes.json`.

It stops and exits 1, leaving the rest to you, when:

- the supply preview is refused, or the wallet already holds a position in the
  market, since the withdrawal would take all of it. Nothing was signed.
- the supply did not complete. Its approval may have been sent, and so may the
  supply itself. Do not run the check again: read `outcomes` and `refused`, and
  resolve a kept record with `almanak ax recover` as
  [Operating](operating.md#operator-actions) describes. Recovery can send a
  signed supply or find it mined, so read the position with
  `ax lending-position` afterwards, and withdraw with `almanak ax withdraw` if
  the amount is supplied.
- the wallet's position changed between the preview and the supply. The
  withdrawal is not run, since it would take more than the check supplied. The
  supplied amount is still in the market.
- the withdrawal preview was refused. Nothing more was signed and the supplied
  amount is still in the market; `almanak ax withdraw` for that market returns it
  once the reason is resolved.
- the withdrawal was executed but did not complete. Its outcome is not known:
  the amount may still be supplied, or the withdrawal may have been mined. Do
  not request another withdrawal yet. A signed step left unresolved keeps its
  record, which refuses later actions for the wallet: list the kept records with
  `almanak ax recover`, resolve one if kept, then read the position with
  `ax lending-position` before deciding anything.

Only the gates and the flow have tests, offline and against a fake command
(`tests/test_live_qa_writes.py`). No test and no workflow runs the write level for
real.

## Local evidence and scheduling

Generated captures under `docs/internal/reports/evidence/` and bench output under
`artifacts/` are ignored by Git and remain local. Full offline QA requires the
retained captures at their original paths. A fresh clone needs a local copy of
those inputs; missing evidence must not be converted into passing or skipped
required cases.

Hosted daily QA and its GitHub watchdog have been removed. There is no hosted
daily coverage or automatic replacement local schedule; the only hosted schedule
is the [weekly fork qualification](#hosted-weekly-fork-qualification), which
covers the fork lifecycle and the rehearsal only. The
[opt-in live checks](#opt-in-live-checks) have no schedule. Operators can invoke
the commands above with explicit provider bindings and retained results. Hosted
CI skips evidence-dependent tests and reports that scope in its job summary.
