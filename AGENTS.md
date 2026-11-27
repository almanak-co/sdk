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
  but cannot move value out of the Safe or call anything ungranted. The one
  exception is the bridged asset of a bridge grant, stated under "Bridges".
  Never describe runtime risk or timing checks as on-chain enforcement.
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
  re-proves all four facts. The deadline and the unused nonce are read at one
  pinned block. An attempt the gateway durably retains as never sent
  is released on a current-state refusal. A maybe-sent attempt with no calldata
  deadline that current validation refuses stops with
  `signed_attempt_unsendable`.
- Accepted: an operator deployment's signed step whose submitter
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
- Accepted risk of the rule above: the proof rests on one
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
- Accepted: the Base operator's own endpoint, listed under the operator's name
  as `base`. Accepted: BNB Chain's own endpoint is not listed, because it keeps
  less than a minute of state. Proposed, not accepted: the `arbitrum`,
  `optimism` and `blastapi` entries. The first two are their chain operators'
  own endpoints; `blastapi` is a shared provider's keyless endpoint and serves
  BNB Chain.
- A (name, chain) pair is listed only after it passed every check of
  `scripts/qualify_public_rpc.py` in repeated full live runs. A pass is a
  point-in-time result, and a transient refusal fails a run as it would fail a
  command. The tool samples the reads an observation and a direct action make
  and sends nothing. Never relax the genesis or head proof to admit an endpoint.
- Accepted: `publicnode` stays listed for Avalanche although it does not meet
  the tool's state-age check: it leaves state reads older than about 100
  seconds unanswered. No other keyless Avalanche endpoint qualified. The
  operating guide states the limit. This excepts no other pair and no other
  check.
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
- Accepted: no string of the vendor's leaves `ax portfolio` unbounded. A
  symbol, a name and a label are short runs of letters, digits and a few marks,
  an identifier is ASCII, and none holds a URL. A value that is not is
  absent, never shortened or rewritten. The agent skill reads the overview only
  when the user asks, never while it loads or as a step of an action, and
  treats its text as data. Never describe the bounds as making the text
  trustworthy.

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
  advertised core intent. Two accepted exceptions, for Across only and stated
  under "Bridges": its chains other than Arbitrum are declared on read-only
  mainnet facts and offline tests, and the Safe/Roles account profile of every
  Across cell, Arbitrum included, is declared without a lifecycle proof. No
  other connector may declare a cell or a profile this way.
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

## Bridges

- A bridge is an economic intent between two named fund accounts of one
  strategy on different chains. It is planned, signed and routed on the chain
  it leaves. Across is the first connector. It plans, signs and reconciles the
  deposit on the source chain and opens the transfer, and reconciles its arrival
  on the destination chain and its refund on the source chain.
- Current contract: Across declares fork mode, production and economic
  backtests on every chain with a pinned SpokePool:
  Arbitrum, Base, Ethereum, OP Mainnet, BNB Smart Chain, Polygon, Robinhood
  Chain and Avalanche. Its account profiles there are EOA and Safe/Roles. They
  concern fork and production runs only: a backtest account is a simulation
  profile and signs nothing. Each is the source of routes to the others. The
  declarations are derived from the pin table, never listed per chain. The
  declaration follows the chain a bridge leaves; the destination chain needs
  only its pinned SpokePool and a bound account. Every production declaration
  is ahead of its cell's evidence package; the open gates are listed at the
  end of this section.
- Accepted, for Across only: a chain is declared without a reference
  lifecycle, refund proof, production rehearsal or prepared backtest of its
  own. Arbitrum is the one source chain with those proofs. Every other pinned
  chain is declared on read-only mainnet facts and offline tests: its
  SpokePool's proxy, implementation and code hash read on chain, that code
  holding every selector, event and view the connector uses, `chainId()`
  equal to the chain, the pool's wrapped native token equal to the network
  definition's, and the installation block of the pinned implementation.
  `scripts/across_pins.py` produces these facts and sends nothing. Accepted
  risk: such a cell's path has never been run in a retained or reviewed run,
  on a fork or on mainnet, so a first bridge from it is treated as that path's
  first run. One run is known by report only: the maintainer states that an
  operator's agent ran a keyless direct bridge from Ethereum to Base on
  mainnet, approval, deposit and arrival. It is not retained evidence and
  proves no cell. `almanak support` states on each such row that no reference
  lifecycle, refund proof or production rehearsal has run from the chain. The QA catalog and the hosted Fork
  Qualification selection leave these cells out by name, from the connector's
  own list, with the reason in the code; they never pass on an empty
  selection. This excepts no other connector, and no other rule than the
  Safe/Roles profile exception below.
- Accepted, for Across only: the Safe/Roles account profile is declared on
  every Across cell, Arbitrum included, without a reference lifecycle, a
  production rehearsal or a Fork Qualification run. Basis: the maintainer
  chose on 25 November 2026 to skip fork proofs for the bridge work,
  Safe/Roles included. The profile is declared on the grants being held
  offline to the calldata the connector's own planner builds, under a test
  model of the Roles operators they use. Accepted risk: the grants were never
  installed in a Roles module, and no Safe has bridged on a fork or on
  mainnet, so a first Safe bridge is that path's first run. The connector's
  own list (`profiles_without_proof` in `connectors/across/support.py`) names
  the profile, with the reason in the code. The QA catalog leaves its
  obligations and fork cases out by that list, the hosted Fork Qualification
  summary names it as exercised by no selected case, and `almanak support`
  states it on each row. The EOA profile of the Arbitrum cell still owes every
  proof. This excepts no other connector, no other profile and no other rule.
- Current contract: HyperEVM is the one network the SDK defines that Across
  serves and that has no pin. Its SpokePool passes the same checks. A deposit
  is prepared at the head, and so is the status read that closes an arrival
  page. The chain's one provider runs a read pinned near its head at a later
  block, which the reader refuses as `provider_context`: measured read-only on
  25 November 2026, the SpokePool read was refused at the head six times of
  six and 64 blocks behind it, and served 140 blocks behind it and deeper,
  where the venue's quote is stamped after the pinned block and admission
  refuses it. So no deposit can be prepared there and no arrival recorded. A
  pin would also make HyperEVM a destination of production routes from every
  other chain, whose arrival could then not be recorded. A fork run and a
  backtest do not read the provider's head and could be declared once a route
  to a chain without production support is refused; that refusal is not
  built.
- Current contract: routes are author configuration and no chain carries a
  token list. USDC and USDT have 18 decimals on BNB Smart Chain and 6 on the
  other chains, so a route between them is refused at resolution, and the
  gateway refuses such a quote before signing. The venue serves those routes
  only on request with `allowUnmatchedDecimals`, which the connector never
  sends.
- Current contract: the reference is
  `strategies/across_usdc_arbitrum_base_bridge`. Its supervised fork proofs
  cover the arrival with a pause and resume, the refund after the destination
  proved the transfer unfilled, a coordinator killed between the mined deposit
  and the arrival, a destination outage while the source bridges, and a
  teardown that revokes an approval whose deposit was never signed. A crash
  between arrival evidence and its commit is covered at component level only:
  the runtime's own `external_evidence` and `external_commit` fault points
  around the real commit and the connector's reader over a fake chain record
  the arrival once, with the same page. It is unproven at the supervised level,
  where the commands expose no fault point and no process is killed there.
- Accepted: the fork quote. A gateway declares, by venue name,
  the transports its environment lets a connector ask a venue through. The
  production gateway declares what the connector states for production, which
  for Across is the venue's API, and takes no other. A fork gateway declares
  only what its chain's fork route names, and Across's fork binding names a
  constructed quote for the source chain: a stated fee of one basis point, the
  pinned block's own timestamp, a fill deadline one hour later, no exclusivity
  and the pinned SpokePools. The fork transport returns that as an answer
  body, which takes the same fetch, size cap, canonical form, parser and
  admission as the venue's answer. Protocol code does not branch on the
  environment, except that admission accepts the quote source
  `across_fork_constructed` on an identified fork only and refuses it on any
  other chain. A gateway that declares no transport refuses the deposit with
  `quote_unavailable`, so a fork never asks the venue. The source is retained
  with the quote and shown beside each transfer in reports and dashboards. The
  author's `quote: across_api` is unchanged. A constructed quote is never
  evidence of what the venue would quote.
- Current contract: a generated fork operator file provisions plain EOA
  accounts, with no fixture operator, for the accounts of a connector that
  supports only EOAs on its chain, and for the accounts of a bridge, the
  receiving one included. Across declares Safe/Roles accounts, but its fork
  binding asks for plain fixture accounts, because no fork run has installed a
  bridge's grants.
- Current contract: a standalone bridge has no fork run with a Safe. Every
  fork run of one, supervised or in-process, reads the connector's fork
  fixture, which funds EOA accounts only and refuses a Safe fund with that
  cause, a hand-written operator file included. So the Safe/Roles profile of a
  standalone bridge is declared for fork mode and cannot run there. A composed
  deployment's fixture takes Safe funds, so a composed Safe bridge is reachable
  on a fork only through a hand-written operator file that names Safe funds
  and their fixture operators. No such run was ever made.
  A fork fixture chain is one a position acts on or the chain of an account a
  position names as its `destination_account`; another bound account adds
  none. The fork key file names the receiving account too, which signs nothing.
- A bridge market is one direction: the sent asset on its chain, the received
  asset on the destination chain, the author's fee ceiling `max_fee_bps` and
  the named quote source. Its allocation names a source account and a
  `destination_account` on the destination chain.
- The SpokePool is pinned per chain by proxy address, implementation address
  and implementation code hash. Another implementation refuses new deposits.
  The pins are held when a market is first resolved, on both chains, and
  whenever a deposit or its approval is planned, signed or sent: the source
  pool as read at the planning block, the destination pool as retained at
  resolution, with no destination read. A route retains the two pools it was
  resolved on, and they stay the deployment's own whatever the pins name
  later, a new proxy address included: replay asks the retained reads about
  them, arrival and refund scans, `fillStatuses`, `getRelayerRefund` and the
  revocation's spender use them, and loading holds them to no pin. Replay
  verifies the retained reads for consistency only. A pool that differs from
  its pin in proxy, implementation or code refuses new deposits with the
  cause, before the venue is asked, and nothing else: arrival, refund,
  `revoke`, teardown and closure keep working, so a changed pin never traps a
  transfer in flight or a standing approval. Accepted limit: a destination
  SpokePool upgraded after resolution is seen only at the next resolution.
- The deposit's quote timestamp is its calldata deadline: the SpokePool reverts
  a deposit mined later than that timestamp plus its own quote buffer, which is
  read at the planning block. Expired signed deposits are released under the
  rule in "Execution and recovery".
- The gateway also refuses a quote whose SpokePool differs from the pin on
  either chain, whose tokens are not the route's, whose timestamp is ahead of
  the pinned block or older than the quote buffer, whose fill deadline is
  beyond the SpokePool's deadline buffer or less than ten minutes away, or
  whose exclusivity ends after the fill deadline.
- The fee ceiling compares base units of the sent and the received asset, so a
  route needs both to have the same decimals. The gateway holds the quote's
  decimals to the sent asset's on-chain decimals and refuses a difference.
- The gateway asks for and admits a quote before it plans the approval, and
  again for the deposit, which signs its own. A quote refused before the
  approval is signed leaves no approval behind. A deposit quote refused after
  the approval is mined leaves that approval standing until a later bridge or
  `revoke` clears it.
- The retained quote is its typed fields and the venue's answer as canonical
  JSON text of at most 16 kB. A larger answer or a non-finite number refuses.
- The author states no approval or revocation inside a bridge. An approval an
  earlier decision left standing, because its deposit was never mined, is
  revoked before the next bridge's own. The `revoke` action clears such an
  approval and increases no exposure, so teardown admits it and can then close.
- A route names its destination by CAIP-2 chain only. The destination's
  environment is that of the bound destination account. Two identified fork
  nodes are one environment; a fork never pairs with anything else.
- A transfer in flight is owned state, not pending work. It is neither a
  balance nor a measured zero: an open transfer carries no received amount.
- Arrival and refund are reconciled as external effects, arrival on the
  destination chain and refund on the source chain, each from the measured
  amount. A missing outcome never sends again.
- The source chain keeps working while the destination is unavailable. Only
  the destination chain records an arrival and only the source chain a refund.
- The shared transfer record keeps CAIP identity and an opaque connector
  reference. Deposit identifiers, hashes and addresses stay in the adapter.
- Accepted, and binding on every bridge connector: a bridge's output amount is
  the venue's own quote from the venue's API, named explicitly in the market
  configuration. The gateway must refuse before signing when the quote's fee
  exceeds the author's stated ceiling, so a false quote costs at most that
  ceiling, or a delay and a refund.
- Accepted risk of a Safe/Roles bridge: Roles cannot relate a deposit's output
  amount to its input. A bridge grant must pin the token, the destination
  chain, the Safe as depositor, the destination fund as recipient and an empty
  message. A compromised operator or gateway host can still move the bridged
  asset out of the Safe by depositing at a near-zero output and filling the
  deposit itself. Only the gateway enforces the fee ceiling. Never describe the
  ceiling as on-chain enforcement.
- Current contract: an Across allocation's source account is an EOA or a Safe
  with Roles, and so is its destination account, in any combination. `ax
  bridge` stays a plain-wallet command.
- Current contract: the grants of a Safe that sends, derived by the connector
  from the route capability, are two. The approval grant names the sent asset
  as contract and pins the route's SpokePool as the only spender, for any
  amount, zero included, so it serves the revocation and no other cleanup
  grant exists. The deposit grant names the route's SpokePool proxy and the
  function `deposit` in its `bytes32` form, with no native value and no
  delegate call, and pins, by position: the Safe as depositor, the destination
  fund as recipient, the sent token, the received token, the destination chain
  number and an empty message. Left free: the input and the output amount,
  each any nonzero value, because no grant bounds an amount and Roles cannot
  relate the two; the exclusive relayer and the exclusivity parameter, any
  value, because the venue chooses them per quote and states none in some; the
  quote timestamp and the fill deadline, any nonzero value, because they are
  per quote and the SpokePool holds them to its own buffers. Several routes of
  one Safe are alternatives of whole argument tuples, never mixed. A Safe that
  only receives takes no grant.
- Current contract: the permission identities of a bridge are the sent
  assets' code only. The SpokePool is left out: verification refuses every
  dispatch of an account whose retained identity changed, the revocation
  included, and a SpokePool that leaves its pin must refuse new deposits only.
  The connector's own pin check holds the pool wherever a deposit or its
  approval is planned, signed or sent. Consequence, shared with every
  connector: a sent asset whose code changes stops the Safe's dispatches,
  the revocation among them, until permissions are generated again.
- Current contract: an Across deployment may bind a Safe and an EOA together.
  Permission generation makes one partition per Safe, and a manifest holds
  exactly the Safe accounts. An EOA account has no partition and is held to
  the plain-account check at dispatch. Generation refuses a deployment that
  binds no Safe. A Safe that only receives has a partition without grants: an
  operator not yet in its role leaves the owner's `assignRoles` batch pending,
  a wired one reports as installed, other wiring refuses, and its Safe and
  Roles wiring is verified like any other. These paths are covered at
  component level only.
- Proposed, not accepted: the mixed binding above is built in the shared
  permission code, so it holds for every connector and not for Across alone.
  The maintainer has not decided that. Consequence: a deployment of other
  connectors, composed or not, with one Safe and one EOA account now generates
  a partition for the Safe only, where generation refused it before, and can
  reach production. No fork run covers that shape. One component test covers
  it, for a Comet Safe beside an EOA.
- Current contract: a wrapped native output is admitted when the destination
  account is a Safe, which is a contract and is paid the wrapped token, and
  refused when it is an EOA. The Safe is verified on its own chain with its
  permissions before a deployment is created. No fill of a wrapped native
  output to a contract was ever read by this repository's arrival reader.
- Current contract: the Safe/Roles profile is declared on every pinned chain;
  each has the canonical Safe and Roles facts other connectors declare
  Safe/Roles on. It is declared under the accepted exception stated near the
  top of this section.
- A transfer and its two accounts stay within one environment.
- Proposed, not accepted: teardown waits for open transfers on both chains and
  does not bridge back.
- Proposed, not accepted: the backtest model charges the author's full fee
  ceiling and delays arrival by a latency stated in the backtest configuration,
  both reported as model assumptions. It admits every deposit; a ceiling below
  the venue's real fee would not bridge in a live run, and no history of the
  venue's fee exists to tell, so this too is reported as an assumption.
- Built under the proposed rule above, which this does not accept: the model
  `across-full-fee-ceiling-fixed-latency-v1` is declared for `bridge` on
  every pinned chain from 15 August 2026, the first whole day after the last
  pinned SpokePool implementation was installed, on any of them. A pinned
  chain whose installation was not read would leave every chain without a
  start. It reads no history and asks no venue.
  Its dataset is the frozen statement of the routes, the window and the
  latency, prepared with no provider call. The backtest file states
  `data.arrival_latency_seconds` and `execution.models.bridge_model`; neither
  has a default, a missing one is refused by that path, and both are frozen
  across resume.
- As built: a modeled deposit debits the source wallet when it executes. The
  received amount is the amount sent less `amount * max_fee_bps // 10000`, the
  gateway's own ceiling computation, and the offer equals it. A backtest
  refuses `max_fee_bps: 0`, the one ceiling no live run admits and the one that
  makes the modeled fee vanish. The arrival is credited at the deposit's second
  plus the latency and reaches `on_outcome` as a `ModeledArrival`, an
  `ArrivalResult` without the measured identity, fill type, block and
  transaction.
- As built: every modeled transfer arrives; no refund, unfilled transfer or
  slow fill is modeled. Its fill deadline is the deposit's second plus one
  hour, as the fork's constructed quote states it, and is never a forecast of
  the arrival. A latency of one hour or more is refused, because the model has
  no refund. A transfer in flight is in neither balance, is shown as a live run
  shows it and states no received amount. A run that ends with one reports it
  in flight; the end boundary invents no arrival, a position's received total
  is absent until a transfer has arrived, and a teardown whose cleanup ends
  before the arrival fails as unresolved.
- As built: the model keeps no approval, so the historical declaration lists
  `bridge` only. A strategy that decides `revoke` in a backtest stops the run
  with an availability error, as the engine does for every action a model does
  not declare.
- As built: a modeled bridge charges gas on its source chain only, excluded or
  a fixed native amount per bridge stated in the backtest file. The fee ceiling
  stands in for every relayer cost. `connectors/gas_references.json` holds no
  Across entry, because no retained supervised run measured one, and
  `backtest recent` refuses a strategy that bridges with
  `unsupported_connector`.
- As built: every amount of a modeled bridge is labeled modeled in reports,
  with the quote source `modeled_full_fee_ceiling`, and the report lists the
  assumptions. A modeled amount is never evidence of what the venue would quote
  or when a relayer would fill.
- As built: in a composed backtest the bridge debits and credits the one shared
  cash ledger, so the arrival becomes spendable by another allocation only when
  it is recorded. The economic model has no analogue of the full-repay hold: it
  has no wallet beyond inventory and no operator top-up, so nothing can absorb
  an arrival twice.
- Current contract: a backtest evaluates `decide` once with every chain's
  observations, where a fork or live run evaluates each chain on its own turn.
  A strategy that picks its step from which allocation it sees must fall
  through to the destination's step when the source's asks for nothing.
- Proposed, not accepted: the first scope of a strategy is same-token ERC-20
  routes. The connector refuses the wrapped native asset as the sent asset,
  and as the received asset when the recipient is an EOA, which Across pays in
  the native asset. A strategy names no native asset on either side. The one
  native route is the direct command's, stated below.
- Proposed, not accepted: the native route, for the direct command only. Its
  identity is the native asset's CAIP-19 reference on both chains. The connector
  alone converts it to the wrapped native token, which each SpokePool must name
  as its own `wrappedNativeToken()` at the pinned block and which must equal the
  network definition's; the route retains both. It is admitted between chains
  whose native coin is ETH: Ethereum, Arbitrum, Base, OP Mainnet and Robinhood
  Chain. BNB Smart Chain, Polygon and Avalanche, and the native asset on one
  side only, are refused. An allocation carries the route only with the
  operator's `adopt` statement and an EOA source, so an author file, a Safe, a
  composed deployment and a backtest refuse it. The wrapped native token stated
  as an ERC-20 sent asset stays refused for everyone.
- Proposed, not accepted: a native deposit is one step with no approval. The
  operation carries its value, which must equal its amount, and the gateway
  reads the recorded operation through the same model, so a deposit of the
  wrapped token with another value or none is refused before signing. Without
  value the SpokePool would pull the wrapped token by allowance instead. The
  wallet's native balance must cover the amount plus the transaction fee
  ceiling, and accounting expects it to fall by the gas paid plus the amount.
  These are gateway checks, not on-chain enforcement.
- Proposed, not accepted: the refund asymmetry. A refund of a native deposit is
  paid in the wrapped native token on the source chain, under an executed
  refund leaf, never in the native asset. The preview and the status say so,
  and `equals_sent` keeps its meaning.
- Proposed, not accepted: the recipient code rule. A native bridge is refused
  before anything is planned when its recipient holds code on the destination
  chain, unless that code is an EIP-7702 delegation, which the SpokePool treats
  as a plain account. Nothing overrides the refusal. The SpokePool decides the
  payout when the fill lands, so an address that holds contract code other
  than an EIP-7702 delegation at that time is paid the wrapped token, an
  address that gained only a delegation is still paid the native asset, and a
  recipient that rejects native value leaves the
  transfer unfilled. The check is the command's own, made once, and never
  on-chain enforcement.
- Proposed, not accepted: the arrival evidence rule of `ax bridge-status`. A
  receipt holds no log of a native value transfer, so a fill of a wrapped
  native output is read from the pool's outflow of exactly the fill event's
  amount of its wrapped token between the fill event and the pool's next fill.
  An unwrap of that amount reports the native asset as paid: a `Withdrawal` by
  the pool, which the WETH9 source emits and which was captured on Base and OP
  Mainnet, or a transfer from the pool to the zero address, captured on
  Arbitrum and Robinhood Chain. A transfer of that amount from the pool to the
  recipient reports the wrapped token, never the native asset. Outflows of the
  wrapped token to other parties or of other amounts in that window, such as a
  refund payment after the fill, are ignored. No outflow of the amount in
  either form, or both an unwrap and a transfer to the recipient of that
  amount, stops with an integrity cause. The result names the evidence. An
  unwrap is evidence that the pool paid native value, not a measurement of the
  recipient's balance. The rule reads every fill of a pool's wrapped native
  token in `ax bridge-status`, a token route's included. No fill on Ethereum
  was captured.
- Current contract: the native route's path has never been run by anyone with
  this SDK, on a fork or on mainnet. Its readers are held to known answers
  captured read-only on 25 November 2026 and kept in
  `tests/fixtures/across-native-captures.json`: a native deposit another party
  sent from Ethereum whose calldata and value the planner reproduces up to the
  sender's trailing tag, its Base fill, fills on Arbitrum, Robinhood Chain and
  OP Mainnet, a fill to a delegating account and a fill to a contract on Base,
  and a refund on Base paid in the wrapped token. The fill to a contract
  carried a message, so it is no relay this connector deposits; no fill of a
  relay without a message to a contract was found. `almanak support` states
  the route as unrun on each chain it leaves. A first native bridge is that
  path's first run.
- Proposed, not accepted: the quote comes from Across's keyless
  `suggested-fees` endpoint, which the vendor labels legacy. An optional
  operator key is read from `ALMANAK_ACROSS_API_KEY` in the gateway's
  environment. If the vendor retires the endpoint, deposits refuse until the
  quote reader is rebuilt on its successor.
- Current contract: a two-chain market is resolved on its source chain and
  completed on its destination chain at that chain's own anchor. The received
  asset is verified there by its on-chain `decimals()` and `symbol()`, with
  those reads retained and replayed separately. A route that was not completed
  forms no allocation.
- Current contract: the transfer's reference keeps the deposit identifier, the
  relay hash and the quote's timestamp, because the destination chain sees no
  source outcome.
- Proposed, not accepted: a relay is hashed under the chain number each
  SpokePool reads from its own node, so on identified forks both sides use the
  isolated signing chain. The production hash is held to a known answer from a
  real deposit and its fill.
- Current contract: an arrival is a `FilledRelay` whose whole relay hashes to
  the retained hash, never a matching deposit identifier alone. It must pay
  the destination fund. The received amount is the value of the token transfer
  to the fund that follows the event in the fill transaction, required to
  equal the event's amount; a difference stops with an accounting cause. A
  slow fill must be paid by the SpokePool. A fast fill's payer is whoever
  called the SpokePool, which the event does not name, so it is not checked.
  An arrival below the offer is a measurement.
- Current contract: `fillStatuses` is a cross-check only. A fill needs a
  filled status, and a filled status with no fill in the scanned pages halts.
- Accepted: the page rule. An arrival scan starts at the last
  destination block not after the quote's timestamp, and a refund scan at the
  last source block not after the earliest fill deadline among the transfers
  proven unfilled. Nothing is read before the head has passed that time. Later
  pages continue at the block after the last one and span at most 2,000
  blocks. A page is committed when it starts its scan, is full, holds an
  outcome or an unattributed payment, or ends an arrival scan past the fill
  deadline; a shorter empty page is read again. The gateway re-derives every
  page before it is recorded.
- Accepted: the unfilled proof. A refund alone does not show
  that a transfer was not delivered. The destination chain proves a transfer
  unfilled when its arrival scan has committed contiguous pages past the fill
  deadline with no fill, and records that on the transfer; only the
  destination chain may. A transfer proven unfilled is still open and owned
  value in flight until its refund is measured. A transfer goes from open to
  unfilled or to arrived on the destination chain and from unfilled to
  refunded on the source chain, and never otherwise.
- Accepted, as part of the unfilled proof and the attribution rule: a refund
  is read only for transfers proven unfilled and never inferred from time. With the destination chain unavailable refunds
  wait, and the source chain keeps working. The refund scan also waits while
  its page would pass the fill deadline of a transfer of the same allocation
  that is in flight and not yet proven unfilled.
- Accepted: the refund attribution and tie rule. A payment of the sent asset
  from the SpokePool to the fund counts only in a transaction whose
  `ExecutedRelayerRefundRoot` leaves name the fund for that asset with the
  same amounts, entry by entry. Its total is attributed to a set of the
  allocation's transfers proven unfilled, past their fill deadline at that
  block, whose sent amounts sum to it exactly. Sets of equal sum are
  interchangeable, so the one with the oldest fill deadlines is taken, then
  the lowest references. Consequence: a foreign payment of an equal amount can
  resolve a transfer, and that transfer's own later refund is then the
  unattributed one. Within one block the amount credited equals the amount of
  the attributed payments. Across blocks it need not equal what the fund's own
  refunds paid: a payment attributed to an older set of equal sum can leave a
  later own refund with no set, and that refund is then an unattributed
  payment, in the wallet and in no inventory, while a transfer it was meant
  for stays open and its deployment unclosed.
- Unresolved, a candidate accepted risk the maintainer has not decided: dust
  in the same leaf. A stranger's expired deposit that names the fund as
  depositor can be summed into the same refund leaf entry as the fund's own
  refund. The payment then matches no set, the own refund is unattributed and
  the transfer stays open.
- Proposed, not accepted: unsolicited inflow. Anyone can deposit naming the
  fund as depositor and let it expire. A refund-leaf payment that matches no
  set is retained in its committed page with its transaction and amount,
  listed in the report as an unattributed refund payment, never attributed,
  and the scan continues. It is no outcome and raises no accounting issue of
  one, because such an issue stops new exposure on the chain and a stranger's
  dust would then freeze a strategy.
- Proposed, not accepted: attribution of one payment considers the 12 oldest
  transfers proven unfilled of an allocation that no other payment of the
  block took.
- Accepted: a refund page ends at the first block that holds a refund-leaf
  payment to the fund, and the payments it names are in that block. The
  gateway refuses a page that runs past it, so no scan continues beyond a
  refund it never read.
- Accepted: several refund payments to one fund in one block are settled in
  that block's page and stop nothing. The payments that fit some set on their
  own are assigned together: when some assignment gives each of them its own
  disjoint set of the transfers waiting, the first such in the tie rule's
  order, payment by payment in log order, is taken, and the other payments
  then take in log order what is left. Without such an assignment, or past
  the search's fixed budget of 10,000 sets tried, each payment in log order
  takes the oldest set the earlier ones left. No payment takes a transfer another paid. The page's one refund outcome names the
  first attributed transaction, retains the receipts of the others, resolves
  every transfer they pay and returns their sum. A payment of the block that
  matches no set is an unattributed refund payment.
- Accepted: a refund transaction whose leaves name the fund for other amounts
  than the SpokePool paid it there is reported and attributed to nothing. It
  is retained in its committed page with its transaction, the paid amount and
  the leaf amount, listed in reports and dashboards as an unattributed refund
  payment with both amounts, and the scan continues. It is no outcome and
  raises no accounting issue. Nothing is credited: the paid tokens are in the
  fund's wallet and in no inventory. Consequence, shared with every
  unattributed payment: a transfer whose refund arrived this way stays proven
  unfilled and open, so its deployment does not close. The report lists such
  transfers under `awaiting_refund` and says that no unattributed payment is
  ever applied. No command resolves the transfer; the operator runs teardown,
  which unwinds everything else, and the fund's owner moves the paid tokens.
  Resolving a transfer by an operator's statement is not built.
- Current contract: an allocation is the only Across allocation of its source
  fund and sent asset. A second one is refused at configuration.
- Current contract: a refund the SpokePool deferred because its transfer
  failed is detected by `getRelayerRefund` and stops the source chain as
  unresolved. Claiming it is not built.
- Current contract: a transfer is resolved at most once, and a deployment
  closes only when every transfer is arrived or refunded and no allowance
  stands. A fill found for a transfer marked unfilled or refunded halts the
  destination chain with both named.
- Proposed, not accepted: a transfer opened towards a chain that had already
  closed reopens that chain.
- Current contract: the destination chain's tick runs `decide` with no
  observation of the allocation and owns nothing there. A bridge decided on
  that tick is refused as routed to another chain.
- Proposed, not accepted: fork settlement. A fill is the destination
  SpokePool's own `fillRelay` by a relayer the fixture funds. A refund is a
  fork-constructed bundle: the impersonated admin relays a root of one leaf
  the fixture wrote, executed through the real contract code, and the fixture
  moves the destination fork past the fill deadline. Label it so wherever it
  is reported; it is never promotion or Fork Qualification evidence. It is
  defined for an Arbitrum source only.
- Current contract: a bridge allocation composes with allocations of other
  connectors. Its deposit debits the source account's owned inventory of the
  sent asset. A transfer in flight is in no wallet's inventory. The measured
  arrival credits the destination account's inventory of the received asset,
  and the measured refund credits the source account's. Nothing spends the
  offered amount: the gateway checks every spend except a full repay against
  retained inventory, which holds the amount only after its arrival is
  measured. A full repay follows the rule below.
- Proposed, not accepted: a full repay in a composed deployment may spend loan
  asset sent to the fund beyond retained inventory, the operator top-up of
  "Lending exits". While a wallet asset may hold tokens of a bridge transfer
  that no record credits yet, a full repay of that wallet asset is held to
  retained inventory: its funding check, its approval and its approved upper
  bound all stay within it, so an arrival or a refund is never absorbed as a
  top-up and credited again. A wallet asset is held while an open transfer
  names it as source or destination, and, in a chain's view, while another
  chain has planned or signed bridge work that delivers into it. The view's
  `inbound` field is derived by the gateway from durable state and never
  retained. Consequences: during the hold a full repay that retained inventory
  covers proceeds, one it does not cover refuses with `repayment_funding`
  naming the hold, a top-up counts only once the transfer is recorded as
  arrived or refunded, with no restart, and a repay approval larger than
  retained inventory that was standing before the hold refuses the repay until
  then. The hold is not an author amount bound: it is derived from the
  checkpoint and ends by evidence. It is a gateway check, not on-chain
  enforcement. It is proven at component level only.
- Current contract: composed `inventory` covers the sent asset on the source
  account and the received asset on the destination account, and another
  allocation on the destination account must hold the received asset, because
  only its observation measures that wallet. A configuration without either is
  refused, before resolution where the author file already names the assets
  and at the resolved configuration otherwise. Accepted consequence: a composed bridge cannot deliver to an account
  that holds no other allocation.
- Current contract: the composed checkpoint retains `transfers`, omitted while
  there are none, and its `external_scans` hold GMX liquidation pages and
  Across settlement pages, each decoded by its own kind. Each component sees
  only its own pages, outcomes, requests and transfers. Composed external
  effects read, verify and audit a page through the component that owns it,
  starting after the connector of the last committed page. Composed requests
  stay GMX only.
- Current contract: on the source chain's tick a composed bridge allocation's
  owned record reports the source account's owned cash and its transfers in
  flight, with sent, offered, fill deadline and the unfilled mark, and no
  received amount. Retained records carry neither. The destination tick has no
  bridge record; arrival and refund reach `on_outcome` there and on the source
  chain as in a single-connector deployment.
- Current contract: an open transfer holds closure of both of its chains in a
  composed deployment too, including a chain where the bridge has no record.
  Composed teardown unwinds the other allocations, revokes a standing bridge
  approval and does not bridge back.
- Current contract: composed reports and dashboards list transfers with their
  quote source, and unattributed refund payments, apart from fund cash. Value
  in flight is never added to a balance and never shown as zero.
- Current contract: the composed reference is
  `strategies/arbitrum_base_bridge_and_lend`: Across from Arbitrum to Base,
  then a Compound V3 supply of exactly the measured arrival, EOA accounts,
  fork runs and an economic backtest. Its supervised fork proofs cover the
  lifecycle with a pause and resume, each stage with owned inventory asserted, and the refund with
  nothing supplied. Its economic backtest models the bridge under the proposed
  model rule and reads Comet history for the supply. It is part of the
  production rehearsal: Across declares production on Arbitrum and Compound V3
  on Base.
- Current contract: a runtime process killed around the arrival commit is
  proven on the composed reference with real processes and fork nodes
  (`tests/fork/test_composed_bridge_fault.py`): killed at the runtime's
  `external_evidence` and `external_commit` fault points, a new runtime
  records the arrival once, credits the destination inventory once and
  supplies exactly that amount. The proof drives the runtime and gateway
  processes directly, not the supervised commands, which expose no fault
  point.
- Current contract: the in-process fork workspace provisions an account that
  only receives a bridge on its own chain's node.
- Current contract: a composed fork route names the constructed quote on the
  bridge's source chain only, under the fork quote rule above, whose status
  this does not change.
- Proposed, not accepted: the composed fork keeper used by `almanak test` runs
  settles a composed deployment's transfers in flight beside GMX requests. It
  is covered at component level only; no fork run exercises it.
- Accepted: the venue answer rule. A venue's answer is not
  block-addressable: asked twice, the venue names another relayer, amount or
  time. The gateway verifies a reserved plan by preparing it again, so it keeps
  its own answer in memory, by venue, decision, pinned block, target and
  amount, and reads that for the same question. A caller never supplies an
  answer. Admission runs on every preparation, retained answer or not. An
  answer lost with the gateway process is asked again, the submitted plan then
  differs and is refused before anything is signed, the coordinator stops with
  `invalid_request`, and a resumed run plans afresh. So a gateway restarted
  between a deposit's preparation and its record costs that decision and signs
  nothing. A refused answer is retained for its block like any other; a
  transport failure is never retained. Without this rule no deposit quoted by
  the venue's API can be recorded. The answer is not authority and is never
  durable state. This is the one fact the gateway keeps in memory between two
  calls of one decision; an operator who restarts the gateway in that window
  resumes the run.
- Current contract: the readers are held to known answers from Arbitrum and
  Base mainnet, captured read-only on 24 November 2026 and kept in
  `tests/fixtures/across-mainnet-captures.json`: a venue answer parsed and
  admitted at its own block, a plain `deposit` whose calldata the planner
  reproduces up to the sender's trailing integrator tag, its receipt, its fast
  fill matched by the whole relay hash, and one transaction that executes
  three refund roots. The relay hash is also held to the contract: the Base
  SpokePool's `fillStatuses` of the computed hash reads unfilled one block
  before that fill and filled after it. The arrival page and the refund page
  run through the settlement entry point over the reads retained from both
  chains. The refund is a real payment under a real refund leaf to a relayer;
  the transfer it is attributed to is constructed, because no deposit of this
  repository was ever refunded. No slow fill was found in the Base blocks
  searched, so a slow fill is proven against constructed logs only.
- Current contract: routes that leave Base and Ethereum are held to known
  answers captured read-only on 25 November 2026 and kept in
  `tests/fixtures/across-route-captures.json`: Base to Ethereum and Ethereum
  to Arbitrum, each with a venue answer admitted at its own block against both
  pinned pools, a plain `deposit` whose calldata the planner reproduces up to
  the sender's trailing tag, its receipt, the relay hash computed from that
  receipt read as unfilled and then filled by the destination SpokePool's
  `fillStatuses`, and the fill measured from its receipt; and one execution
  of refund roots on Ethereum and on Base, kept to the logs of the SpokePool
  and of the asset. The venue states exclusivity as an offset in some
  answers and as an absolute time in others, whatever the source chain; both
  forms occur and both are admitted. No
  settlement page was replayed for these routes. OP Mainnet, BNB Smart Chain,
  Polygon, Robinhood Chain and Avalanche have no known answer of a deposit, a
  fill or a refund: only the facts of the accepted exception above.
- Current contract: the production EOA rehearsal has an in-process fixture
  for a standalone bridge: one fork per chain of the route and one funded EOA
  per named account. Both bridge references are in the default rehearsal
  selection and are subjects of the hosted Fork Qualification for the cell
  `across` on `eip155:42161`.
- Accepted: a rehearsed bridge asks the venue's live API, as the production
  gateway does, with the fixture's public test addresses. The forks' clocks
  are moved ten minutes ahead of the present first: the venue stamps a quote
  some minutes behind the present, a fork mines only when the run sends, and
  the gateway refuses a quote stamped after its pinned block. The fill is the
  fixture relayer's, and a refund is not rehearsed. A rehearsal, local or
  hosted, therefore depends on the venue's API answering: an outage or a
  refused request fails the run, and a failed run is not proof of support.
- Open gates of every Across cell declared under the accepted exception,
  stated once and holding for each of them: no reference sends from the chain;
  no fork lifecycle, refund proof, production rehearsal, prepared backtest or
  Fork Qualification run exists for it; no fork refund is defined for a
  transfer it sends, so a fork run from it that is not filled stays open; a
  fork refuses authored gas ceilings on Base and OP Mainnet; a signed step
  whose nonce was consumed is never released on Polygon, which states no
  finality margin; its
  QA obligations are left out of the catalog rather than proven; the page span
  and the provider's log ranges were not exercised on its block time; and
  every gate of the Arbitrum cell below is open for it too, the Safe/Roles
  gates among them.
- Open gates of the production cell `across` on `eip155:42161`, none of which
  may be described as complete: no hosted Fork Qualification run has rehearsed
  either reference; no reverse-fault case exists for the cell, so a hosted
  cell QA run reports that gap and does not pass; the recovery-foundation
  nodes, the global offline and CRAP gates over retained evidence, and the
  evidence review are open; the `revoke` action is proven on forks only,
  after a constructed quote, because no rehearsal reaches a revoke; the
  composed reference's refund credit, its fork keeper and the full-repay hold
  are unrehearsed, the hold being proven at component level only; a refund
  has been read from another party's
  mainnet refund execution and from fork-constructed bundles, and a slow fill
  from constructed logs only, never for a transfer of this repository; no gas
  reference exists for an EOA bridge, so `almanak check` compares a bridge's
  gas with nothing; an author file with gas policies on Arbitrum and on Base
  has no fork run, because a fork refuses authored ceilings on Base and leaves
  them unenforced only when every policy is on an OP chain, so the standalone
  reference keeps its production gas policy in `config.production.yaml` and
  its fork runs are gasless; and no retained run or other evidence of this
  repository holds a mainnet transaction of this connector, from this cell or
  any other.
- Open gates of the Safe/Roles profile on every Across cell, Arbitrum
  included, none of which may be described as complete: no Safe/Roles bridge
  has run on a fork or on mainnet; the generated grants were never installed
  anywhere, so their acceptance by the Roles integrity check, their evaluation
  of a real deposit and the dynamic comparison of the empty message are
  unproven outside the offline model; the generated authority is new, so every
  production cell owes a hosted Fork Qualification that exercises it, and none
  has run; the weekly selection's lifecycle nodes for the bridge references run
  their EOA fork operator files and the rehearsal is an EOA rehearsal, so a
  passing run is no Safe/Roles evidence; no fork fixture, reference, operator
  example or rehearsal binds a Safe to a bridge, and the fork fixture of a
  standalone bridge refuses one; the mixed Safe and EOA
  manifest, the grantless partition of a receiving Safe, a refund paid to a
  Safe, a wrapped native output paid to a Safe, the composed Safe bridge and
  the Safe's revocation are each covered at component level or not at all; and
  the QA obligations of the profile are left out of the catalog rather than
  proven.
- Not built: a recent-window rule, without which `backtest recent` refuses a
  bridge with `unsupported_connector`; a claim for a deferred refund, which
  stops the source chain as unresolved; and an operator's resolution of a
  transfer whose refund was paid unattributed. The rules above still marked
  proposed are ones the production declaration rests on.
- Current contract: the Across checkpoint is schema version 2. A version 1
  checkpoint, which retained one unattributed payment per refund page, is
  audit-only and refused for execution. A composed checkpoint carries no
  version of its components: one written before with an unattributed Across
  payment fails validation. Such checkpoints exist on forks only.
- Known limit: a full empty scan page is retained, and every retained page of
  a chain is audited, two block reads and one log query each, whenever that
  chain's history is audited. GMX liquidation pages share this. A scan that
  waits commits a full page every 2,000 blocks, so a transfer that waits for a
  refund that never comes grows the checkpoint, and the audit by as many log
  queries, at a rate set by the chain's block time. From the block times read
  on 25 November 2026:

  | Chain | Block time | One page spans | Pages a day |
  | --- | --- | --- | --- |
  | Robinhood Chain | 0.105 s | 3.5 min | 411 |
  | Base | 0.2 s | 6.7 min | 216 |
  | Arbitrum | 0.285 s | 9.5 min | 152 |
  | BNB Smart Chain | 0.451 s | 15 min | 96 |
  | Avalanche | 0.995 s | 33 min | 43 |
  | Polygon | 1.5 s | 50 min | 29 |
  | OP Mainnet | 2 s | 67 min | 22 |
  | Ethereum | 12 s | 6.7 h | 4 |

  An arrival scan grows the same way on the destination chain until the fill
  or the fill deadline. After a pause a scan catches up one page per tick,
  before signed work is admitted on that chain. Nothing bounds the growth;
  pages are not dropped.
- Current contract: a page may end at the head, and the first page of a scan
  is committed at once, so a chain whose head can be replaced, Ethereum and
  Polygon among them, can replace a committed page's last block. A scan never
  continues after a block that is no longer the chain's: the coordinator's
  audit before each tick of the chain, the scan's own next page and the
  gateway's commit each stop with `reorganization`, never with the status
  cross-check's accounting cause. The halt is of that chain only. The page is
  retained, nothing is recorded and the transfer stays open and owned; a fill
  mined in the replacement block is on chain and in the fund's wallet, in no
  inventory. No command drops a page that is no longer canonical, so the
  transfer is not resolved by the SDK after such a halt. A head replaced
  before its page was committed leaves nothing stale and is read again.

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
  `ALMANAK_AX_GATEWAY_AUTH_FILE`. `--block` defaults to `latest`, the head;
  `finalized` is stated explicitly, for `ax bridge-status` too.
- Current contract: every `ax` read that pins a block states `read_at`: `block`,
  the selector that chose the block, the resolved block's `number` and
  `timestamp`, and `age_seconds`, the command's clock less that timestamp. It
  is added by the calling process on every transport and reads no head.
  `ax bridge-status` keeps its own `read_at`, the tag and a block of each
  chain, with no age.
- Current contract: a direct action's preview states `steps_note`, and `steps`
  where the connector declares them: the kinds of the transactions the request
  sends, in order. A connector declares `steps` in its `direct.py` only where
  the observation the first step was planned on decides every step; the list
  is reported only when its first entry is the step the gateway planned.
  Across, Aave V4, Aave V3 and SparkLend declare it, and none for a full
  repayment. Only the first step's gas is quoted. Never estimate the gas of a
  later step, and never describe the list as a promise: each later step is
  planned again once the earlier one is mined.
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
- Accepted: once a step of a direct command's request is mined, a transient
  provider or storage failure before the next step is sent is held and the
  check repeated, for at most 60 seconds per request. A failure met at signing
  may leave a signature the record does not hold, so the repeat can sign the
  same recorded plan again: the same transaction at the same nonce, of which
  only the recorded one is sent and only one can be mined. Before any step is
  mined such a failure ends the command. Never describe the hold as on-chain
  enforcement.
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
- Accepted: `ax recover` also releases a signed step whose nonce
  another finalized transaction of the wallet consumed, under the proof stated
  in "Execution and recovery". The result lists it under
  `released_consumed_nonce` with the consuming transaction and its block, and
  the command exits 1 because the step did not happen. A command stopped after
  the drop reports it in its `steps_sent` event.
- Proposed, not accepted: the native route of `ax bridge`. The direct command
  sends native ETH and delivers native ETH through Across when `--asset` and
  `--destination-asset` both name the native asset by its CAIP-19 reference, on
  two of Ethereum, Arbitrum, Base, OP Mainnet and Robinhood Chain, from a plain
  wallet to a plain recipient. The rules are stated under "Bridges". It is a
  route of the direct command only: a strategy, a Safe, a composed deployment
  and a backtest refuse it.
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
- Accepted: the Across direct actions, `ax bridge` and `ax revoke-bridge`, are the
  one exception to the rule above. They are declared on offline tests and
  read-only mainnet checks, with no fork rehearsal of the direct command on any
  cell. Unproven at process level: the signed approval and deposit of a direct
  bridge, their receipt reconciliation, the record kept for an unresolved step
  and `ax recover` on it. The exception covers no other connector.
- Current contract: Across declares its direct actions on every chain it declares
  production on as the source of a route, from the same list in code.
- Accepted: a direct bridge asks Across's quote API from the command's own
  gateway, under the quote rules of "Bridges", on every transport. The API
  needs no key, and no RPC endpoint stands in for it.
- Accepted on 25 November 2026, in the maintainer's words "I want it to work
  without a key yes, both quote and execution": the direct bridge commands take
  named public endpoints. The maintainer asked for the keyless quote and
  execution of `ax bridge`. `ax bridge-status` and `ax revoke-bridge` are
  included as the read and the cleanup an execution needs, a transfer's outcome
  and a standing approval, and were not named. A bridge reads two chains, so
  the caller names two endpoints: `--public-rpc` for the chain the
  asset leaves and `--destination-public-rpc` for the chain it arrives on,
  required with it for `ax bridge` and `ax bridge-status`. `ax revoke-bridge`
  reads the source chain only and takes the first alone. Neither is a default
  and neither is a fallback; a (name, chain) pair outside the reviewed list is
  refused before any read with the names that serve the chain, and HyperEVM has
  none. Each endpoint gives the full public proof before its chain is resolved:
  chain number, pinned genesis block and a recent head. The destination reader
  of a keyed bridge is still held to chain number and genesis.
- Accepted consequence of the entry above: a direct bridge previewed, executed
  or queried through public endpoints trusts two endpoints, one per chain. The
  accepted public-endpoint risk extends to it: the bridge signs what the source
  endpoint's reads resolve, that endpoint's operator sees each transaction
  before it is mined, each operator sees the addresses read on its chain, and a
  false report can lose up to the amount of the action. A false destination
  report sends nothing by itself. At a bridge's start it can pass a received
  asset or a recipient that the checks there would refuse, within the amount of
  the action. At a status read it can misstate an arrival and moves no funds.
  The fee ceiling, the SpokePool pins and the recipient check stay checks of the
  command and its gateway over what the endpoints report. Never describe them
  as on-chain enforcement.
- Current contract: quote admission is the same on a public endpoint. A quote
  stamped after the block the gateway pinned on the source chain is refused
  with `quote_time` before the step is signed. The venue stamped its quotes 79
  to 269 seconds behind the present in five answers read on 25 November 2026,
  and a public head may be up to 120 seconds old, so a lagging endpoint can
  cause that refusal. It costs the attempt and signs nothing. Never loosen the
  check to admit a lagging endpoint.
- Current contract: a direct bridge deployment has one chain, the one it sends
  from, and binds only the wallet. The operator states the recipient as an
  address on the destination chain with the required `--recipient`; no default
  chooses it, and the wallet's own address is stated to keep the funds. The
  statement lives in the allocation's `adopt` field, which only an operator
  deployment may carry. The route is resolved on the source chain and completed
  on the destination chain before the deployment exists, through a second
  reader: from the same env file, held to that chain's number and pinned genesis
  block, or the named destination endpoint, held to the public proof. Both
  SpokePools are held to their pins. The deployment sends exactly the stated
  amount, under the operator's required `--max-fee-bps`. A bridge, its
  revocation and its recovery read no event logs.
- Current contract: `ax revoke-bridge` reads the source chain only and holds
  neither SpokePool to its pin, so a destination outage or a pin difference never
  keeps an approval standing. Its route takes the sent token and the SpokePool
  from the source read; the destination pool is the pin as written in code and
  the received asset is as stated, both unread and unused. The spender it clears
  is the proxy address the pins name today, whatever implementation it runs. It
  takes no other spender, as `ax revoke-exchange` takes none: an allowance to a
  proxy address the pins no longer name is not cleared by this command.
- Known limit: a revocation, direct or a strategy's, observes the source
  SpokePool as every Across step does, reading its quote buffer, fill deadline
  buffer and deposit pause. It is held to no pin, but a proxy whose
  implementation no longer serves those views refuses the observation, and the
  approval then stands until the owner clears it with another tool.
- Accepted: the recipient checks of a direct bridge. The preview
  and the result state `recipient` and `recipient_is_sender` beside the quote.
  The `note` says when the recipient is not the sending wallet, and when it was
  stated in one case, which carries no checksum. A recipient that is a contract
  on the source chain and holds no code on the destination chain is refused
  with `recipient_not_deployed` before anything is planned, since a contract
  account of one chain does not exist at the same address on another. A plain
  account that delegates its code (EIP-7702) is not a contract here. The refusal
  has no override. It is a check of the command at its start, never on-chain
  enforcement, and shows nothing about who controls the address.
- Current contract: a direct bridge ends with its deposit's receipt and does not
  wait for the arrival. Its result names the transfer by its reference and
  states no received amount. Nothing is kept of a finished command. A preview
  states the gateway's admitted quote with its source, fee and ceiling; the
  deposit signs a later quote under the same ceiling.
- Current contract: a transfer has two handles. The reference exists once the
  deposit is mined, because the SpokePool assigns the deposit identifier then.
  The deposit's transaction hash exists from signing: every outcome names it,
  and so do the `unresolved` step of a kept record and of the `steps_sent`
  event. `ax bridge-status --deposit-tx` rebuilds the reference from the one
  deposit that transaction made through the pinned SpokePool towards the
  destination chain, in a block that is still canonical.
- Current contract: `ax bridge-status` is a read. It finds the deposit on the
  source SpokePool by its identifier and holds it to the reference's relay hash
  and to the deposit event's own quote timestamp, which the hash does not cover,
  then reports `arrived` with the amount measured in the fill transaction,
  `in_flight`, or `unfilled`. For an unfilled transfer it lists the refund-leaf
  payments the depositor received since and attributes none: a refund names the
  fund and the token, never a deposit. It never reports an unmeasured amount as
  zero.
- Accepted: the blocks of `ax bridge-status`. The deposit is found
  at the source chain's latest block, because the operator holds its receipt and
  it decides no outcome. The outcome is read at the block `--block` names on each
  chain, `latest` by default or `finalized`; the command takes no block number,
  since one number names no block of two chains. A fill is therefore reported
  as soon as it is mined, in a block that can still be replaced; with
  `finalized` it is reported as `in_flight` until its block is final. The
  result names the tag and the blocks read.
- Current contract: `unfilled` is a destination block stamped after the fill
  deadline in which `fillStatuses` does not hold the relay as filled. It rests on
  the SpokePool refusing a late fill: `fillRelay` reverts with
  `ExpiredFillDeadline` when the relay's fill deadline is before the block's
  time. That was read from the pinned implementations on Arbitrum and Base by a
  simulated call on 25 November 2026: a relay one second past its deadline
  reverted with that error, and the same relay with its deadline at the block's
  time, or later, passed that check and failed further on for another cause. A
  block stamped with the deadline itself can still fill, so it reads as
  `in_flight`. Unproven here, and accepted as such: that a slow fill,
  `executeSlowRelayLeaf`, takes the same check. It needs a proof against a relayed root and was not simulated; it
  rests on the published contract source, where both paths share one internal
  fill function. A read at `latest` is as good as that block: a replaced block
  can still bring a fill stamped before the deadline.
- Current contract: `ax recover` releases an expired signed deposit of a direct
  bridge under the expiry rule of "Execution and recovery". The approval before
  it stands until `ax revoke-bridge` or a later bridge of the same amount.
- Proposed, not accepted: `ax recover` resolves a kept bridge record through a
  public endpoint of its source chain, on the footing of every other direct
  action: the endpoint the bridge ran on while that pair is listed, or the one
  the caller names with `--public-rpc`, which gives the public proof first. It
  is built as a consequence of the keyless execution accepted above: a keyless
  bridge that leaves a record must be resolvable without a key, or the wallet's
  claim is stranded. The maintainer named the quote and the execution, not the
  recovery, and has not decided it. Risk, which the decision would accept: a
  false report from the source endpoint can release a deposit that was mined,
  under the expiry rule or the consumed-nonce proof of "Execution and
  recovery", and a bridge stated again would then send twice. No connector
  refuses a public endpoint for its records any more. Nodes behind one
  load-balanced endpoint can disagree, so the expiry release reads the nonce
  again at the pinned block that shows the deadline passed, named by its hash
  and required canonical: an unused nonce in
  that block's own state shows the deposit was not mined up to it, and a node
  that does not hold the block refuses the read. The receipt and the
  provider's copy stay separate reads. This holds for every connector and
  every endpoint, and is a gateway check, not on-chain enforcement. Recovery
  through the record's own endpoint, with no flag, is held to the chain number
  and genesis only, not to a recent head, as for every direct action.
- Proposed, not accepted: `ax bridge-status` pages its log scans by what the
  named public endpoint serves in one query. The span is stated in code beside
  the reviewed list (`PUBLIC_LOG_SPAN`): 100 blocks through `drpc`, and a
  settlement page of 2,000 through an endpoint with no entry. The command keeps
  no page, so the span changes no fact a completed scan reports. It changes
  what a read costs, and how far a refund scan reaches in one command: that
  scan is capped at 200 pages, one request each, so it covers 400,000 blocks
  with whole pages and 20,000 through `drpc`, then stops with `complete` false
  and is continued with `--refund-from-block` at `scanned.to` plus one. A block
  budget instead of the page cap would make one `drpc` command 4,000 requests.
  Strategy scans never use it: they refuse a public endpoint. A read a public
  endpoint refuses or leaves unanswered ends the command with the endpoint's
  own refusal, its kind unchanged, and the endpoint and chain named; nothing is
  reported from a partial read and nothing falls back.
- Current contract: what the listed pairs served a status read was measured
  read-only on 25 November 2026 against each chain's SpokePool, and is stated
  with its refusals in the operating guide. `arbitrum`, `optimism` and
  `publicnode` on Avalanche served 2,000-block log pages up to a day back;
  `publicnode` on Ethereum served them two hours back and refused them a day
  back; `drpc` refused 2,000 blocks and served 100 on its five chains; `base`
  and `blastapi` refused every log query, down to one block. Every pair served
  `fillStatuses` at its `finalized` block. So through public endpoints Base
  needs `drpc` for a status read, and on BNB Smart Chain no arrival can be
  measured and no refund listed. A measurement is a point-in-time result.
- Known limit: no listed pair was added or removed for bridges, and
  `scripts/qualify_public_rpc.py` does not sample a log query, so a pair's
  listing says nothing about what it serves a status read.
- Proposed, not accepted: the agent skill bridges only to its own wallet's
  address and refuses any other recipient, although the command takes one.
- Operator actions have no amount ceiling or cumulative budget; the required
  fee ceilings and the wallet balance bound them. An operator-managed action
  policy is proposed, not accepted.
- Accepted: `ax revoke-exchange` on Uniswap V4 sets the wallet's ERC-20
  approval of each pool token to Permit2 to zero, beside the Permit2
  allowances to the router and the position manager. That approval is shared
  with every other application that spends the token through Permit2 from the
  same wallet, and each needs it approved again afterwards. When it will zero
  one, the result states this in `note` before anything is signed.

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
  (`[tool.crap.coverage-floor]`). Do not lower it to make a check pass. Only
  the local `make crap` scores it.
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
- Hosted CI (`pr.yml`, `release_candidate.yml`, `release_tag.yml`) runs lint,
  types, CLI smoke checks and the docs build. Nothing runs these gates on a
  push to `main`. A skipped case is not a pass, and a green hosted run is not
  full QA.
- A pull request runs only the test modules it added or modified
  (`make test-changed`), with `--skip-missing-evidence`, in one job on a
  standard runner. A changed conftest or support module selects nothing. The
  same job gates CRAP for functions with an executable line the pull request
  changed, on the coverage of those modules alone, so a changed function that
  no changed test module exercises scores as uncovered. The coverage floor is
  not scored there. A green pull request does not show that the rest of the
  offline suite passes: run `make test` and `make crap` locally before merging.
- The whole offline suite runs hosted on release candidate and release tags
  only, in shards on the org's 8-core `ubuntu-latest-l` runner. Shard
  balancing reads the committed `.test_durations`; refresh it with
  `scripts/ci/merge_test_durations.py` when shards drift.
- The macOS smoke job is advisory: macOS rejects lowering `RLIMIT_AS`, so the
  author worker refuses to start there.
- Hosted Fork Qualification runs weekly on `main` over every reference on a
  cell that declares production support, with the Safe/Roles lifecycle and the
  production EOA rehearsal. An empty selection or a declared cell no reference
  exercises fails the run, except the Across cells declared without proof
  under "Bridges", which the selection leaves out by name. No selected case
  exercises the Safe/Roles profile of a bridge, which the run's summary states.
  It uses identified Anvil forks and public Foundry
  test keys only. Accepted exception: the rehearsal of an Across bridge asks
  the venue's live quote API, keyless and with public test addresses, so a
  venue outage fails that run. A failed run is not proof of support, and a
  passing run does not complete a cell's evidence package.
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
