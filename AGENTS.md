# Almanak SDK: project instructions

These are the standing rules for people and coding agents working in this
repository. They state current contracts, not their history. Record a new
maintainer constraint here when it is given, and say whether it is accepted,
proposed or unresolved.

Nothing in this file authorizes spending real funds or installing production
permissions. Each live action is an explicit operator invocation.

## Direction

- This repository is the SDK. The 2.x line is closed; treat it as domain
  evidence, not as an implementation dependency or behavior to preserve.
- There is no hosted platform. Features that serve any operator are operator
  features: the `almanak.signers` entry-point group and `--signer-file`, the
  canonical Safe 1.4.1 profile, the read gateway, the `funding` and
  `permissions` blocks of `almanak check --json`, the dashboard export, the
  backtest progress snapshot, the bundled schemas and references, and
  `backtest recent`. "Hosted CI" and "hosted Fork Qualification" mean
  GitHub-hosted runners.
- The package and public imports use the `almanak` namespace.
- Breaking APIs are allowed. Never write migration instructions; document the
  current contracts instead.
- Prefer deleting unnecessary responsibilities, then simplifying, then
  optimizing, then automating. Leave sound designs alone.
- Keep reusable reference strategies in `strategies/`. They use the public
  author root and are discovered directly from that directory. Lifecycle proofs
  execute the reference itself, never a separate test copy. The Aave V4 WETH
  supply, hold, restart and withdraw strategy is the continuing integration
  reference.
- Separate fund ownership from execution authority. Safe with Zodiac Roles is
  an optional custody and execution profile; simple EOA wallets are supported
  too.
- Support multi-wallet and multi-chain strategies natively. A strategy's
  identity never implies a single wallet or chain; every action and asset keeps
  its explicit execution and chain scope. Healthy-chain entries and exits
  continue when another chain is unavailable and their own inputs and authority
  remain valid.
- Require strict CAIP-2 chain references and CAIP-19 asset references
  throughout configuration, APIs, strategy state, persistence and reports. Use
  validated, dedicated identifier types, never untyped strings, bare numeric
  chain IDs, bare token addresses or token symbols as identity. Symbols and
  display names are presentation only. Keep fork and environment authority
  separate from CAIP identity, and convert to protocol-native values only inside
  the relevant adapter.
- Shared chain interfaces must not require EVM addresses, hexadecimal hashes,
  nonces, gas fields or ABI calls. Chain adapters own those representations.
- Bind fund accounts by explicit names within one strategy. Keep Safe owners,
  delegated operators and transaction submitters distinct in meaning. Attribute
  protocol effects to the fund account, and gas and the outer nonce to the
  submitter.

## Design and implementation

Read the guides under `docs/` and the contracts in the code you are changing
before designing a component. Keep one authoritative description of each
decision.

Understand the concrete user journey and failure cases before coding. Resolve
material ambiguity with targeted questions, then proceed. Build a complete
narrow lifecycle before expanding breadth. Do not add registries, frameworks,
services or storage tiers without a demonstrated need. Port protocol knowledge
and regression cases selectively, and verify their expected outcomes
independently.

## Correctness boundaries

- Strategies hold no secrets, do not sign, and make external calls only through
  the trusted gateway. The gateway independently enforces execution authority.
- Local strategy isolation uses macOS `sandbox-exec` or Linux Bubblewrap with
  unprivileged user namespaces and libseccomp. Every author-hook mode uses the
  shared bounded resource and output policy. Missing or unusable isolation
  refuses execution; never fall back to an unsandboxed worker.
- The gateway is a narrow stateless service for credentials, authorization,
  controlled external I/O, signing and authorized submission, served over
  authenticated HTTP on loopback. The trusted runtime owns orchestration,
  recovery and state transitions. Integrations own protocol semantics. Keep
  authoritative execution and coordination facts in durable storage, not in
  gateway memory or instance-local files.
- Keep one authorized runtime coordinator per deployment, and coordinate
  conflicting account and signer submission scopes across deployments. Persist
  identity before external actions. An unknown outcome never triggers blind
  re-execution. Recover recording and strategy transitions without duplicate
  effects.
- Missing data is not measured zero. Financial amounts carry identity, units
  and evidence. Reports expose incomplete measurements and model assumptions.
- Historical decisions use only information available at decision time.
  Simulated fills cannot use opportunities that preceded action eligibility.
- Teardown verifies remaining obligations and preserves evidence. Independent
  risk reduction can continue despite an accounting failure, with the failure
  visible.
- Simulation must never obtain production execution authority. Fork, backtest
  and production authority stay explicit and separate.

## Authoring and configuration

- A strategy is `strategy.py` and one configuration file. Generated deployment
  and credential files are runtime artifacts, not author inputs. Runtime
  artifacts are JSON; author inputs are YAML.
- Authors state each choice once. The SDK resolves and verifies the facts those
  choices imply and retains the resolved evidence. Authors must not need SDK
  implementation code to discover valid connector identifiers or other values.
- Pool and market selection belongs in author configuration. Connectors carry
  no default pools and no sample pool or token allowlists.
- Operational execution setup is operator-managed gateway configuration, not
  author configuration. Economic choices stay with the strategy, including gas
  preferences and budgets and named account requirements. The gateway resolves
  payers and enforces operator authority and fee ceilings.
- Validate strategy parameters once into one authoritative instance. Parameters
  stay fixed for a run; changes activate through restart or resume. Durable
  strategy state, position ownership and pending execution survive supported
  resumes.
- `strategy.parameters` are author knobs checked only for shape. They authorize
  nothing and never enter capabilities. `strategy.test_parameters` and
  `test run --param` change only the fork workspace's copy. An optional
  top-level `TypedParameters` model in `strategy.py` is validated in the sandbox
  before any hook and is `config.parameters` in every hook; a refusal is the
  final `invalid_config`. Never import author code in the trusted process.
- Strategy-defined configuration validation is optional and runs in the sandbox
  before deployment creation, alongside the SDK and gateway checks.
- Proposed, not accepted: `parameters.py` and `dashboard.py` as optional author
  modules beside `strategy.py`.
- Capabilities declare economic actions with typed connector targets. Trusted
  connectors derive the exact permissions and cleanup; account backends compile
  them. Missing or unsupported authorization fails closed.
- Lending intents reuse typed supply and full-withdrawal variants across
  connectors, parameterized by a validated connector target. Strategies request
  economic actions; the integration decides approval and revocation steps.
- Decision scheduling uses an explicit strategy-wide interval, runtime UTC
  time, a minimum gap between evaluation starts, immediate initial eligibility
  and no catch-up bursts. Scheduling is persisted separately from strategy
  memory. Admitted intents complete through intermediate outcomes without
  another decision. Teardown bypasses the interval.
- Ctrl-C pauses the coordinator and keeps services running.
- The gas `total_budget` is optional; absent means no lifetime gas cap. The
  `ape` preset follows an aggressive market-based fee quote within explicit hard
  caps and does not bid at those ceilings. Type-2 transactions and the other
  urgency presets remain a proposal.

## Accounts and authority

- Safe execution uses Zodiac Roles without owner-key or EOA fallbacks.
  Permissions are generated from the sandboxed strategy capability hook;
  handwritten overrides are forbidden. Safe funds own all protocol positions,
  collateral, debt and borrowed proceeds, and `onBehalfOf` is the Safe.
  Operators submit and pay gas.
- Safes are dedicated to one strategy.
- Canonical Safe profile: either Safe 1.4.1 singleton (L1 or SafeL2) by pinned
  code hash; the fallback handler is empty or the canonical
  CompatibilityFallbackHandler 1.4.1 by pinned address and code hash; a guard is
  refused.
- Permission verification reads Roles storage at the pinned block, never the
  event history. Every grant the strategy needs must be installed exactly,
  together with the Safe and Roles wiring. A partly installed partition fails.
  Not detected, and accepted: grants beyond the strategy's own, other members of
  its role, and other roles held by the operator.
- Scope-only authority: generated grants pin the contract, function, asset,
  pool or market, fee tier and hook, the Safe as recipient, beneficiary or
  owner, the exact approval spender, nonzero minimum outputs, deadlines and
  sentinels. No grant carries a cumulative allowance or an amount bound derived
  from an author field, except GMX `executionFee`, bounded by the author's
  `execution_fee`. Liquidity mint ticks are unrestricted on-chain; the gateway
  enforces the author's range or band. Accepted risk: a compromised operator or
  gateway host can lose up to the Safe's value held in the configured venues,
  but cannot move value out of the Safe or call anything ungranted. Never
  describe runtime risk or timing checks as on-chain enforcement.
- Lending exits: authors state no repayment or interest amount. Repay grants
  pin the asset, market and the Safe as beneficiary. A full repay is always
  approve the held balance, the protocol's full-repay form, then revoke the
  residual. Funding covers the entry only; an unfunded repay refuses with the
  shortfall, and a top-up resolves it without a restart. No author amount bound
  may refuse full repay, full withdraw, collateral disable or revocation.
- LP exit grants leave the NFT token ID unrestricted inside exact collect and
  close templates that pay the Safe. The gateway restricts operations to the
  retained strategy-owned NFT. The same holds for Uniswap V3 position-manager
  templates, and GMX cancellation grants may cancel any compatible request the
  Safe owns while the gateway restricts cancellation to the retained request.
- Changed generated authority means installed permissions are regenerated and
  every affected cell with production support needs a new hosted Fork
  Qualification.
- Only identified fork fixtures install permissions automatically. For
  production, the SDK exports reviewable owner transactions and verifies the
  installed state.

## Execution and recovery

- Forks and mainnet run the same protocol, account-wrapper, simulation, receipt
  and accounting logic. Environment profiles select transport identity,
  credentials and explicit execution policies only.
- Production advances on canonical mined receipts without waiting for finality
  or extra confirmations. Committed receipt identity and canonicality are
  audited before further work; invalidated history halts with retained
  evidence.
- Recovery rebroadcasts; it does not re-plan. The one exception is expired
  signed work: a recorded attempt past its connector-declared, calldata-enforced
  deadline, with no canonical receipt, no provider copy and an unused submitter
  nonce, is released and planned afresh at that nonce after the gateway
  re-proves all four facts. An attempt the gateway durably retains as never sent
  is released on a current-state refusal. A maybe-sent attempt with no calldata
  deadline that current validation refuses stops with
  `signed_attempt_unsendable`.
- Proposed, not accepted: an operator deployment's signed step whose submitter
  nonce was consumed is dropped, with or without a calldata deadline, sent or
  not. The gateway proves, when asked and again when the drop is recorded, that
  the step has no canonical receipt and that exactly one other transaction of
  the submitter with the recorded nonce is in a final block, and names that
  transaction and block. Final means both: at or below the endpoint's
  `finalized` block, and behind its head by the network's
  `finality_margin_seconds`, required because some endpoints serve their head
  as `finalized`. Each margin is stated in the network definition with its
  source and with what it stands for: the chain's own time to finality on
  Ethereum and BNB Smart Chain, and only a floor for normal operation on a
  rollup, where time on the rollup does not measure the finality of its batch
  and the endpoint's `finalized` tag is what is trusted. A network with no
  stated margin releases nothing; Polygon PoS and HyperEVM state none, and
  using a stuck step's nonce there leaves its record unresolvable. A nonce that
  is only
  occupied keeps the step with `nonce_conflict`. The proof is recorded in the
  checkpoint's `superseded` by the update that drops the step, the gateway
  admits that update only when the record equals its own proof, and reports are
  read from it. The drop plans nothing and retains nothing in `released`. A
  never-sent step is dropped only by this proof, so a connector that cannot
  release gains no release. A strategy deployment still stops at an occupied
  recorded nonce. Never describe this proof as on-chain enforcement.
- Proposed, not accepted, as the risk of the rule above: the proof rests on one
  endpoint's report of its head, its finalized block and a block's
  transactions. An endpoint that lies can cause the release of a step that is
  still valid. For a sent step with no calldata deadline, such as a Curve swap,
  the signed bytes are then still minable at their old minimum, and an operator
  who restates the swap is exposed to two swaps. This widens the accepted
  public-endpoint risk, where a false report can lose up to the amount of the
  action.
- Fee-only replacement of legacy transactions is opt-in under explicit payer
  bounds. All same-nonce candidates are retained and their maximum exposure is
  reserved.
- Asynchronous protocol requests keep their accepted request identity, escrow
  and original decision through keeper settlement, cancellation and restart. A
  missing outcome never authorizes recreating the request.
- Accepted: for a trading-only classic Aerodrome allocation, signed bytes are
  sent past pool shares that reached the fund after signing. The shares are
  reported as an accounting issue and never attributed. An operator's signed
  step is always sent. A strategy's signed step is sent unless it carries a
  calldata deadline and increases exposure. A fund that holds shares is refused
  before anything is planned or signed. Accepted consequence: the entry approval
  of a strategy frozen by such shares stays standing until the shares are
  removed.
- Unsolicited protocol events, such as a liquidation, are reconciled as
  external financial effects through bounded canonical pages, without inventing
  an SDK intent or submitter transaction.
- A zero-output exact-size quote that consumes its whole input is a
  measurement. Such counter dust stays owned, valued and reported as an exit
  residual; there is no dust threshold.
- Provider rate limits are optional operator environment variables, counted
  per deployment in gateway memory. Author settings cannot override them. Each
  deployment uses a dedicated gateway process.

## Financial data and prices

- Financial data sources are explicit in requests and configuration. A price
  identifies its venue, pool, oracle or explicitly chosen vendor, its chain and
  its base and quote units. Never substitute another source for a requested
  venue price, invent a peg, or hide missing data behind a fallback. Defaults
  never choose a financial data source, except the `ax portfolio` overview
  below.
- Strategy price series for technical analysis come directly from on-chain pool
  data through the operator's RPC provider, in backtests, forks and live runs
  alike. No vendor candle substitutes for one.
- RPC endpoints are built from the operator's `.env` provider key. Keys stay
  inside trusted provider resolution and out of author files, logs and retained
  evidence. Verify endpoint chain identity and required archive access; do not
  substitute providers on failure.
- Accepted: a named public endpoint (`--public-rpc`) serves `almanak ax` reads
  and direct actions, previewed, executed or recovered, without a provider key.
  The caller names it; it is never a default and never a fallback. The reviewed
  list is in code and excludes HyperEVM. Each reader verifies the chain number,
  the pinned genesis block and a recent head. Strategy runs, forks, checks and
  backtests refuse it.
- Accepted: the Base and BNB Chain operators' own endpoints, listed under the
  operator's name as `base` and `bnbchain`. Proposed, not accepted: the
  `arbitrum` and `optimism` entries, listed under the same rule.
- A (name, chain) pair is listed only after it passed every check of
  `scripts/qualify_public_rpc.py` in repeated full live runs. A pass is a
  point-in-time result, and a transient refusal fails a run as it would fail a
  command. The tool samples the reads an observation and a direct action make
  and sends nothing. Never relax the genesis or head proof to admit an endpoint.
- Accepted risk: a direct action executed through a public endpoint signs what
  that one endpoint's reads resolve, including the market's contract, and the
  endpoint's operator sees each transaction before it is mined. A false report
  can lose up to the amount of the action. A second endpoint as a witness was
  considered and declined.
- Proposed, not accepted: built-in request rates for public endpoints.
- An omitted lending price source is resolved once, retained as evidence and
  frozen. A later change halts as a pin does, and restart re-resolves.
- Vendor discovery names candidates, never identity. A token is verified by its
  on-chain `decimals()` and `symbol()`, a pool or market by its connector's own
  resolver at the pinned block. Vendor liquidity and volume figures are
  reported as the vendor's and never become strategy, backtest or price inputs.
- An omitted CoinGecko coin ID resolves an ERC-20 asset from its CAIP chain and
  contract. No symbol matching, provider fallback or implied historical
  coverage.
- Accepted: `almanak ax portfolio` reads an account's tokens and protocol
  positions from Rabby's backend by default, without a key. This is the one
  default that names a financial data source. Its figures are reported as the
  vendor's and are display only: never identity, a price input, a health figure
  a floor is checked against, or the basis of a planned or executed action. A
  refusal or a throttle is reported as such and nothing is substituted. The
  backend is the vendor's own, undocumented, and sees every address asked
  about.

## Backtesting

- Backtests simulate economic intents against historical observations and keep
  hypothetical balances, positions and obligations in the runtime's durable
  state. They do not replay blocks or keep state on a fork.
- A separate, required backtest configuration selects mode, simulation
  settings, collection limits, sampling and financial sources. The strategy
  configuration owns strategy parameters.
- Economic mode prepares or reuses a matching dataset, then runs offline with
  no network data calls. Datasets and effective settings are frozen across
  resume. Never replace missing history, change resolution implicitly or claim
  unmeasured speed or accuracy.
- Backtests inherit the configured decision interval. A coarser override is
  explicit, fixed for the run and reported as reduced cadence fidelity.
- Each connector and chain combination may declare a backtest starting
  datetime. An omitted boundary does not establish historical coverage.
- Hypothetical trades and liquidity do not change later external market
  history. LP fees use fixed historical fee growth without dilution. Quote
  provenance distinguishes modeled calculations from archival Quoter results.
- Band allocations run in economic mode with bracketed historical fee growth:
  the lower bracket is credited and the upper bracket reported, never
  interpolated. A liquidity band's `lower`/`upper` envelope is optional; without
  one, the band recentres around the observed tick and the backtest refuses a
  position outside its explicit data coverage.
- V3-family preparation may read ahead in bulk and carry tick state between
  samples, but sample payloads must equal a direct read of every tick.
- HyperCore backtests read the venue's own `/info` candle and funding history,
  retained as raw evidence. This is an exception for HyperCore market inputs
  only. Trade candles stand in for book, mark and oracle history, and reports
  say so.
- Progress goes to a versioned snapshot shared by the terminal and SDK
  callbacks. Leave unsupported estimates absent, and keep progress outside
  financial state.
- Measure numerical claims. Report cold preparation, cached preparation and
  execution separately.

## Connectors and support

- Connectors advertise immutable, typed per-chain support for execution modes,
  intents, account profiles, direct operator actions and historical coverage.
  Runtime and gateway enforce the same declaration. `almanak support` prints it.
  Provider availability is a separate fact.
- A candidate deployment does not establish support. A supported pair needs a
  prepared offline backtest and an actual reference lifecycle proof for every
  advertised core intent.
- Several cells declare production support ahead of their complete evidence
  package. Do not describe the open gates as complete: the full QA matrix with
  its reverse-fault case, the recovery-foundation nodes, the global offline and
  CRAP gates and the evidence review.
- Composed references that include a connector without production support on
  the same chain stay out of the production rehearsal.
- Hooked Uniswap V4 pools are admitted only through the in-code reviewed
  registry, never by strategy configuration. A hook whose code differs from its
  pin refuses new exposure, while exits and teardown continue with a fresh
  quote and strict minimum output. A pin must never trap a position.
- GMX native insolvency is excluded from advertised GMX support.
- HyperEVM (`eip155:999`) uses HYPE as its native asset and Alchemy
  `hyperliquid-mainnet` as provider. The public Hyperliquid RPC is never an SDK
  provider.

## Hyperliquid

- HyperCore is a venue of `eip155:999`, not a separate chain. The account is
  the Safe's address, spot assets are identified by their linked HyperEVM
  ERC-20 CAIP-19 references, and perpetual markets are connector targets. The
  Safe moves funds through CoreWriter under generated Roles grants; an agent
  key in the gateway signs trades through Hyperliquid's API.
- Production is declared on HyperEVM, Safe/Roles only.
- Agent keys: each approval uses a fresh key the gateway generates and holds,
  never published before the Safe approves it. The grant pins the agent name to
  the deployment's two names and leaves the address free. Renewal approves the
  next fresh key under the other name before the current approval expires, and
  never re-approves an address. Verification halts on any agent listed under
  the deployment's names that the gateway does not hold. A remote signer holds
  no agent key, so a production run with `--signer-file` is refused.
- Accepted risk: the agent can trade every market of the Safe's HyperCore
  account at any size, price and leverage. Only the gateway enforces the
  strategy's markets, sizes and prices. Teardown does not revoke the agent.
- A resting order is owned state, not pending work. Cancels are their own
  attempts, signed by the retained order ID. Teardown cancels only the
  strategy's own orders, closes positions reduce-only and stays incomplete
  while any order is open.
- Verification uses identified Anvil forks of HyperEVM mainnet and a
  synthesized HyperCore model, always labeled as synthesized and never counted
  as promotion or Fork Qualification evidence. No testnet is used.

## `almanak ax`

- `ax portfolio` takes no transport: it asks the vendor from the calling
  process, and the read gateway does not serve it.
- Every other read takes exactly one transport: a local operator `--env-file`,
  or the read-only observation gateway (`service read-gateway`), which keeps provider
  credentials in its own process and serves only whitelisted reads over
  authenticated loopback. The execution gateway's token is never reused for
  reads. Without a transport flag, `ax` uses `ALMANAK_AX_GATEWAY_URL` and
  `ALMANAK_AX_GATEWAY_AUTH_FILE`. `--block` defaults to `finalized`.
- `ax resolve`, `ax dex-pools` and `ax lending-markets` name candidates for
  authoring under the discovery rules above.
- Accepted: operator actions that do not come from a strategy are direct
  commands (such as `ax supply`, `ax borrow` and `ax swap`), each one process
  with no workspace,
  gateway process or state kept between commands. Inside the command they run
  as an operator deployment through the shared planning, simulation, signing
  and reconciliation path, with the gateway service in the same process. The
  deployment adopts the position the wallet holds at the start. Without
  `--execute` nothing is signed.
- A direct command is not resumed. A signed step left unresolved keeps its
  record and the wallet's claim, which refuses later actions for that wallet.
  `ax recover` resolves the signed step from its receipt or by sending the
  signed bytes again, drops what the request had not signed, and releases the
  claim; it plans and signs nothing. It uses the endpoint the record's action
  ran on, or the one transport the caller names with `--env-file` or
  `--public-rpc` instead, on the record's chain. A record whose public endpoint
  is no longer listed needs a named transport, so removing a listed pair never
  strands a claim. Records live in durable local state; one with no signed step
  is removed, with its claim, by the next direct command. An unreadable record
  is kept.
- Proposed, not accepted: `ax recover` also releases a signed step whose nonce
  another finalized transaction of the wallet consumed, under the proof stated
  in "Execution and recovery". The result lists it under
  `released_consumed_nonce` with the consuming transaction and its block, and
  the command exits 1 because the step did not happen. A command stopped after
  the drop reports it in its `steps_sent` event.
- A connector package that serves direct actions declares them in its
  `direct.py`, and each chain's support lists them as `direct_actions`. Every
  other installed connector is named with its reason. Accepted: GMX V2 and
  Hyperliquid serve no direct action.
- A direct lending deployment binds the wallet's whole position in the venue,
  read at the command's pinned block, not only the requested market: every Pool
  reserve the account uses as collateral or has borrowed, every leg held in a
  Comet or a Morpho market. A direct swap binds only the stated amount of the
  sold asset. Never relax a connector's ownership or risk check for an operator
  deployment; bind what the check needs, or refuse with the cause.
- A direct action a connector projects against a health floor takes the
  operator's `--min-health-factor`, one floor for the wallet's whole position in
  the venue. It is required to borrow, and for any other projected action while
  the wallet has debt there. A repayment is never projected: where a connector's
  configuration demands a floor beside repay authority, the lowest valid one is
  stated, so no floor can refuse a repayment. Never describe the floor as
  on-chain enforcement.
- A direct swap is the gateway's own exact-size quote of the operator's stated
  amount: the deployment's capital is that amount in the sold asset. Its minimum
  output is that quote less the operator's required `--slippage-bps`; no default
  chooses a slippage, and no other venue's price is consulted. A swap inside a
  liquidity action is never admitted; the operator swaps separately.
- A direct liquidity action owns only what its allocation states: the stated
  amounts of the pool's currencies, and for an existing position the one NFT
  named by token ID, which the command and the gateway each verify on chain
  (owner, pool, range). Where a position is a balance of the pool's own LP token
  (Curve, Aerodrome classic) the allocation owns the wallet's whole balance of
  it. The statement lives in the allocation's `adopt` field, which only an
  operator deployment may carry. An adopting allocation quotes and admits no
  swap.
- Declare a direct action on a cell only after its fork rehearsal passed there.
  Morpho Blue on HyperEVM declares none: the provider runs head-pinned reads at
  a later block, which the reader refuses.
- Operator actions have no amount ceiling or cumulative budget; the required
  fee ceilings and the wallet balance bound them. An operator-managed action
  policy is proposed, not accepted.

## Check output and dashboards

- `almanak check --json` states what a strategy's accounts must hold before
  starting, as the `almanak-funding-v1` block. It holds requirements from
  resolved parameters only, never observations or a funded flag, and survives a
  refused later stage.
- A production online check with Safe/Roles accounts also reports an
  `almanak-permissions-v1` block: per Safe, the installed authority or the
  owner's Safe Transaction Builder batch. The block never makes a check ready.
- The schemas and example documents are in `docs/contracts/`. `make
  contract-examples` rewrites the examples from their producers.
- Dashboards use a read-only hook, typed JSON blocks, retained snapshot data
  and a shared browser renderer. Evaluation stays outside execution.

## Almanak Code

- Each published wheel carries one native Almanak Code binary under
  `src/almanak/bin/`, selected by platform tag. A bare `almanak`, and any
  command this CLI does not define, is forwarded to it. A leading option always
  stays with argparse.
- The pinned release tag is in `.almanak-code-version`. Nothing downloads
  binaries at install or run time. The sdist carries no binary; without one,
  forwarding fails closed with exit 1.
- `make build` stays the plain `uv build`. Only `make build-platform-wheels`
  retags.
- Inside a forwarded session the SDK parser always wins. Do not build
  compatibility shims or add commands shaped like the 2.x CLI, and do not treat
  assistant output as support or proof evidence.

## Quality gates

- Use `uv` for Python commands, environments and dependencies. Install the
  hook with `uv run --locked pre-commit install`. Commits run Ruff lint and
  format checks and mypy over SDK code, strategies and scripts.
- `make check` runs every local gate. `make help` lists the targets.
- Maximum CRAP score is 30 for every repository-owned Python function. Rewrite
  a function above 30; never grandfather a violation or raise the threshold.
- Statement coverage over every Python file under `src/` stays at or above 87%
  (`[tool.crap.coverage-floor]`). Do not lower it to make a check pass.
- `make hygiene` refuses private material in the published tree: credentials,
  internal infrastructure names, workstation paths, tracker references and
  denied wallet addresses.
- Default pytest is offline. Provider access needs an explicit env-file
  argument. Strategy fork lifecycles run through the local QA bench or an
  explicit invocation with `--fork-env=.env`.
- Retained evidence under `docs/internal/reports/evidence/` is ignored by Git
  and local. Tests that read captures through `tests/retained_fixtures.py` use
  byte-for-byte copies under `tests/fixtures/evidence/`, pinned by SHA-256 in
  `tests/fixtures/evidence.json`. Add copies only through
  `scripts/evidence_fixtures.py pin`, which refuses URLs, credentials and
  operator env values. Locally, missing evidence is a failure.
- Hosted CI (`pr.yml`, `main.yml`, `release_tag.yml`) runs lint, types, the
  offline suite with `--skip-missing-evidence`, CLI smoke checks and the docs
  build. A skipped case is not a pass, and a green hosted run is not full QA.
  Pull requests also run the CRAP gate over that reduced coverage, scoped to
  functions with an executable line the pull request changed.
- The offline shards run on the org's 8-core `ubuntu-latest-l` runner. Shard
  balancing reads the committed `.test_durations`; refresh it with
  `scripts/ci/merge_test_durations.py` when shards drift.
- The macOS smoke job is advisory: macOS rejects lowering `RLIMIT_AS`, so the
  author worker refuses to start there.
- Hosted Fork Qualification runs weekly on `main` over every reference on a
  cell that declares production support, with the Safe/Roles lifecycle and the
  production EOA rehearsal. An empty selection or a declared cell no reference
  exercises fails the run. It uses identified Anvil forks and public Foundry
  test keys only. A failed run is not proof of support, and a passing run does
  not complete a cell's evidence package.
- The daily QA bench derives required coverage from connector declarations.
  Missing proofs, skipped required tests and unavailable providers never
  produce a green run. See `docs/qa.md`.
- Distinguish component tests from actual process and fork recovery evidence.
  Run the narrowest meaningful checks. Documentation changes need link and
  diff checks.

## Releases

- A release candidate tag runs `release_candidate.yml`: the gates, then the
  distributions built and verified and kept as the run's `sdk-dist` artifact.
  Release candidates publish nowhere. Keep their versions immutable.
- Publishing a non-prerelease GitHub release `vX.Y.Z` that matches
  `pyproject.toml` runs `release_tag.yml`: the gates, one build, and an upload
  of those exact files to PyPI. A PyPI version can never be uploaded again. The
  `PYPI_API_TOKEN` and `ALMANAK_CODE_TOKEN` secrets must exist first. The
  build downloads the pinned Almanak Code release from the repository named
  by the `ALMANAK_CODE_REPO` repository secret, or from the script's default
  when it is unset; the token must be able to read that repository.

## Working practices

- Never spend real funds without explicit authorization.
- Preserve unrelated changes and keep repository operations within the
  requested scope.
- Use plain language, no emojis and no em dashes. Comments explain non-obvious
  invariants, units and failure semantics instead of narrating adjacent code.
- Do not name private systems, tracker tickets or workstation paths in code,
  comments, tests or documentation.
- Local browser previews bind to `0.0.0.0`; open them through `localhost` with
  the selected port.
