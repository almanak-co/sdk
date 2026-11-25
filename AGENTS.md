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
  re-proves all four facts. An attempt the gateway durably retains as never sent
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

## Bridges

- A bridge is an economic intent between two named fund accounts of one
  strategy on different chains. It is planned, signed and routed on the chain
  it leaves. Across is the first connector. It plans, signs and reconciles the
  deposit on the source chain and opens the transfer, and reconciles its arrival
  on the destination chain and its refund on the source chain.
- Current contract: Across declares fork mode and economic backtests on
  Arbitrum only, for EOA accounts, as the source of a route to Base. Base
  declares no run mode: no reference lifecycle sends from it and no fork refund
  is defined there, so it is a destination only. No chain declares production.
  The declaration follows the chain a bridge leaves; the destination chain
  needs only its pinned SpokePool and a bound account.
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
- Proposed, not accepted: the fork quote. A gateway declares, by venue name,
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
  supports only EOAs on its chain, the receiving account of a bridge included.
  A fork fixture chain is one a position acts on or the chain of an account a
  position names as its `destination_account`; another bound account adds
  none. The fork key file names the receiving account too, which signs nothing.
- A bridge market is one direction: the sent asset on its chain, the received
  asset on the destination chain, the author's fee ceiling `max_fee_bps` and
  the named quote source. Its allocation names a source account and a
  `destination_account` on the destination chain.
- The SpokePool is pinned per chain by proxy address, implementation address
  and implementation code hash. Another implementation refuses new deposits.
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
  Arbitrum from 15 August 2026, the first whole day after the pinned SpokePool
  implementation was installed there. It reads no history and asks no venue.
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
- Proposed, not accepted: the first scope is same-token ERC-20 routes. The
  connector refuses the wrapped native asset as the sent asset, and as the
  received asset when the recipient is an EOA, which Across pays in the native
  asset.
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
- Proposed, not accepted: the page rule. An arrival scan starts at the last
  destination block not after the quote's timestamp, and a refund scan at the
  last source block not after the earliest fill deadline among the transfers
  proven unfilled. Nothing is read before the head has passed that time. Later
  pages continue at the block after the last one and span at most 2,000
  blocks. A page is committed when it starts its scan, is full, holds an
  outcome or an unattributed payment, or ends an arrival scan past the fill
  deadline; a shorter empty page is read again. The gateway re-derives every
  page before it is recorded.
- Proposed, not accepted: the unfilled proof. A refund alone does not show
  that a transfer was not delivered. The destination chain proves a transfer
  unfilled when its arrival scan has committed contiguous pages past the fill
  deadline with no fill, and records that on the transfer; only the
  destination chain may. A transfer proven unfilled is still open and owned
  value in flight until its refund is measured. A transfer goes from open to
  unfilled or to arrived on the destination chain and from unfilled to
  refunded on the source chain, and never otherwise.
- Proposed, not accepted: a refund is read only for transfers proven unfilled
  and never inferred from time. With the destination chain unavailable refunds
  wait, and the source chain keeps working. The refund scan also waits while
  its page would pass the fill deadline of a transfer of the same allocation
  that is in flight and not yet proven unfilled.
- Proposed, not accepted: the refund attribution and tie rule. A payment of
  the sent asset from the SpokePool to the fund counts only in a transaction
  whose `ExecutedRelayerRefundRoot` leaves name the fund for that asset with
  the same amounts. Its total is attributed to a set of the allocation's
  transfers proven unfilled, past their fill deadline at that block, whose
  sent amounts sum to it exactly. Sets of equal sum are interchangeable, so
  the one with the oldest fill deadlines is taken, then the lowest references.
  Consequence: a foreign payment of an equal amount can resolve a transfer,
  and that transfer's own later refund is then the unattributed one. The
  totals stay right.
- Proposed, not accepted: unsolicited inflow. Anyone can deposit naming the
  fund as depositor and let it expire. A refund-leaf payment that matches no
  set is retained in its committed page with its transaction and amount,
  listed in the report as an unattributed refund payment, never attributed,
  and the scan continues. It is no outcome and raises no accounting issue of
  one, because such an issue stops new exposure on the chain and a stranger's
  dust would then freeze a strategy.
- Proposed, not accepted: attribution considers the 12 oldest transfers proven
  unfilled of an allocation.
- Proposed, not accepted: two refund payments to one fund in one block stop
  the source chain with an ordering cause.
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
  fork runs and an economic backtest. Its supervised fork proofs cover the lifecycle with a pause
  and resume, each stage with owned inventory asserted, and the refund with
  nothing supplied. Its economic backtest models the bridge under the proposed
  model rule and reads Comet history for the supply. It stays out of the production rehearsal because
  Across declares no production support.
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
