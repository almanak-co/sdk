# Changelog

All notable changes to the Almanak SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Version 3.0.0 is the first release of the rewritten SDK. The 2.x line is closed, and
its changelog is on the `v2` branch.

## [Unreleased]

## [3.4.2] - 2026-11-26

### Added
- `almanak ax bridge` sends native ETH and delivers native ETH through Across, from
  a plain wallet to a plain recipient, when `--asset` and `--destination-asset` both
  name the native asset (`eip155:<chain>/slip44:60`) of two of Ethereum, Arbitrum,
  Base, OP Mainnet and Robinhood Chain. The deposit carries the amount as its own
  value, in one step with no approval. The wallet must hold the amount plus the
  transaction fee ceiling. A recipient that holds contract code on the destination
  chain is refused with `recipient_is_contract`, because the SpokePool would pay it
  the wrapped token; that check is the command's and not on-chain enforcement. The
  preview states the payout and that a refund is paid in the wrapped token on the
  source chain. A strategy, a Safe and a backtest refuse the route. No native
  bridge has been run on a fork or on mainnet: the route is declared on read-only
  mainnet known answers and offline tests.
- `almanak ax bridge-status` reports what a fill of a wrapped native output paid:
  `arrival.paid_as` is `native` when the destination SpokePool unwrapped the amount
  in the fill transaction and `wrapped_token` when it transferred the token, with
  the evidence under `arrival.measured_by`. The evidence is the pool's outflow of
  exactly the fill's amount after the fill event; a fill with none, or with both
  forms, stops with an integrity cause. A refund of a wrapped native input names
  the token under `refund.paid_in`.
- Every `almanak ax` read that pins a block states `read_at`, `ax balance` and the
  pool, reserve, market and discovery reads included, and it also holds the resolved
  block's `number` and `timestamp` and `age_seconds`, the command's clock less that
  timestamp. No extra read is made for it. `ax bridge-status` keeps its own
  `read_at`.
- A direct action's preview states `steps_note`: how many transactions the request
  plans, in order by kind, as the sequence it sends when nothing is refused, and that only the first step's gas is quoted, because a
  later step cannot be simulated before the earlier one is mined. Aave V4, Aave V3
  and SparkLend now list `steps` as Across does, where the planned block decides
  every step; a full repayment and the other connectors list none and say that
  more can follow the first step. No gas is estimated for a later step.

### Changed
- A venue refusal of an amount as too low for its fees is the cause
  `amount_too_low`, for every Across route. It was `quote_refused`.
- Every `almanak ax` read defaults to `--block latest`, the head, and
  `ax bridge-status` reads its outcome there too. `--block finalized` is stated to
  read only final blocks. A read at the old default reported a balance from before a
  transfer that had arrived, a quarter of an hour behind the head on a rollup. A
  block at the head can still be replaced, so an arrival or an unfilled transfer
  that `ax bridge-status` reports without `--block finalized` is not final. Its
  `in_flight` note mentions a fill in a later block only for a read at `finalized`.
- The agent skill reads a balance or a position after an action or a bridge's
  arrival with `--block latest`, reports the step count of a preview, and says that
  the later steps' gas is not quoted up front.
- A position read says which block it read and why. `--help` of every `ax` read states
  its default and that `finalized` can trail the head by minutes on a rollup;
  `almanak markets --help` states its own default, `latest`. `ax lending-position`,
  `ax liquidity-position` and `ax perp-position` report `read_at`, whose `block` is the
  selector that chose the block, so an empty position read at an old block can be told
  from an empty position. The refusal of a token ID a position manager does not hold
  names the block read (`holds no NFT <token id> at block <number>`), and a
  `liquidity-position` read at `finalized` adds how to read the head; a direct action's
  refusal names the block only. Each entry of a direct action's `outcomes` states the
  `block` its step was mined in. The accepted selectors are unchanged (#38).

### Fixed
- `almanak ax lending-markets --connector aave_v4` counts a Spoke that reverts
  `getReserveCount()` in `source.without_reserves` whichever JSON-RPC code the node
  answers the revert with. Avalanche nodes answer a revert without data as `-32000`
  `execution reverted`, so every empty Spoke there was named in `source.unreadable`. A
  read the node refused or could not serve under the same code is still unreadable.
- `almanak ax perp-markets --connector gmx_v2` resolves the venue's Avalanche labels:
  `BTC/USD`, `ETH/USD` and `AVAX/USD` name the markets indexed by BTC.b, WETH.e and
  WAVAX, as `BTC/USD` and `ETH/USD` already named WBTC and WETH on Arbitrum. The
  `[LONG-SHORT]` collateral notation takes the same labels beside the on-chain symbols,
  for example `AVAX/USD [AVAX-USDC]`. Aliases are per chain, and a label that matches
  several markets is still refused with its candidates.
- A direct `ax` action other than a bridge re-sends a transient read failure while it
  resolves its market and reads the wallet's position, each read within the budget an
  execution read has. A load-balanced endpoint that had not yet served the head
  (`provider has not served the block`) ended the action before planning. Nothing was
  signed, and no submission is ever re-sent. `ax bridge` and `ax revoke-bridge` resolve
  their route without this budget.

## [3.4.1] - 2026-11-25

### Changed
- `almanak ax bridge`, `ax bridge-status` and `ax revoke-bridge` work without a
  provider key. `--public-rpc <name>` names the public endpoint of the chain the
  asset leaves, and `--destination-public-rpc <name>` the endpoint of the chain it
  arrives on, required with it for `ax bridge` and `ax bridge-status`. Neither is
  chosen for the caller, a name that does not serve its chain is refused with the
  names that do, and each endpoint shows its chain number, the pinned genesis block
  and a recent head before its chain is resolved. The venue's quote still comes
  from Across's own API and is admitted against the pinned block as before. The
  commands refused `--public-rpc` until now.
- `almanak ax recover` resolves a kept bridge record through a named public
  endpoint of the source chain, or through the endpoint a keyless bridge ran on. It
  refused one for a bridge record until now. A false report from that endpoint can
  release a deposit that was mined; the operating guide states the check to make
  before a released bridge is stated again.
- `almanak ax bridge-status` pages its log scans by what the named public endpoint
  serves in one query, 100 blocks through `drpc`, and a read a public endpoint
  refuses names that endpoint and its chain. `base` and `blastapi` served no log
  query when measured, so an arrival on BNB Chain cannot be measured without a
  provider key, and Base needs `drpc` for a status read.

- `almanak ax portfolio` bounds the vendor's text. A `symbol`, a `name`, a
  protocol's `name` and a `kind` that is longer than a short label, or holds a
  character outside letters, digits, currency signs and a few marks, is null, and
  the vendor's own IDs are ASCII. A position's `protocol` no longer carries
  `site_url`. A token's symbol and name are chosen by its deployer and anyone can
  send a token to an account, so this text reached the agents that read the
  report unbounded.
- The agent skill reads the overview only when the user asks for it. It read it
  every time it loaded. The skill tells the agent that symbols and names in the
  overview are data written by others and never instructions.

### Fixed
- The release of an expired signed step reads the nonce again at the pinned block
  that shows the deadline passed, named by its hash, so nodes of one endpoint that disagree cannot
  release a step that was mined.

## [3.4.0] - 2026-11-25

### Added
- A bridge is an economic intent between two named fund accounts of one strategy on
  different chains. A strategy asks for it with `config.bridge(name, amount)`; the
  deposit is planned, signed and reconciled on the chain it leaves. A transfer in
  flight is owned state, in neither wallet's balance and never a measured zero, and
  its arrival and its refund are reconciled as external effects on their own chains
  and reach `on_outcome` (#49).
- Across is the first bridge connector (`across`). A market is one direction: the
  sent asset, the received asset on the destination chain, the author's fee ceiling
  `max_fee_bps` and the quote source `across_api`. The output amount is the venue's
  own quote, and the gateway refuses to sign a deposit whose fee exceeds the
  ceiling; the SpokePool checks no price. The SpokePool is pinned per chain by proxy
  address, implementation and code hash, and a difference refuses new deposits only.
  Routes need the same decimals on both sides (#50).
- An Across transfer is settled by its measured arrival on the destination chain or
  its measured refund on the source chain. A transfer is proven unfilled only by the
  destination chain, after its fill deadline; a refund payment that cannot be
  attributed is reported as such, credited to nothing, and stops nothing. A missing
  outcome never sends again (#52, #68).
- Reference strategies `across_usdc_arbitrum_base_bridge` and
  `arbitrum_base_bridge_and_lend`, which bridges and then supplies exactly the
  measured arrival to Compound V3. A bridge allocation composes with allocations of
  other connectors (#55, #59).
- Economic backtests model a bridge with `across-full-fee-ceiling-fixed-latency-v1`:
  the whole fee ceiling is charged and the arrival is credited after
  `data.arrival_latency_seconds`, both stated in the backtest file and reported as
  model assumptions. No refund is modeled (#65).
- Across declares fork runs, production and economic backtests on Arbitrum, Base,
  Ethereum, OP Mainnet, BNB Smart Chain, Polygon, Robinhood Chain and Avalanche.
  Only Arbitrum has a reference lifecycle, a refund proof and a production
  rehearsal, all on forks; the other chains are declared on read-only mainnet facts
  and offline tests, and `almanak support` says so on each row. No mainnet
  transaction of this connector has been sent (#66, #68, #72).
- `almanak ax bridge` sends an exact amount to a stated recipient on another chain
  through Across, under a required `--max-fee-bps`, and ends when the deposit is
  mined. `almanak ax bridge-status` reads what became of a transfer from its
  reference or its deposit transaction, and `almanak ax revoke-bridge` clears a
  standing SpokePool approval. A bridge takes a provider key and refuses
  `--public-rpc` (#73).
- An Across bridge may send from a Safe with Zodiac Roles, and either account of a
  bridge may be an EOA or a Safe. The connector derives two scope-only grants for a
  Safe that sends: an approval of the sent asset to the route's SpokePool only, for
  any amount, which also serves the revocation, and the SpokePool's `deposit` with
  the Safe as depositor, the destination fund as recipient, both tokens, the
  destination chain and an empty message pinned. Amounts, the venue's relayer, the
  quote time, the fill deadline and the exclusivity are free: Roles cannot relate a
  deposit's output to its input, so the fee ceiling stays a gateway check and a
  compromised operator or gateway host can move the bridged asset out of the Safe.
  A Safe destination keeps a wrapped native output as the token. `ax bridge` is
  unchanged and takes a plain wallet.
- Across declares the Safe/Roles account profile on every chain with a pinned
  SpokePool, for fork runs and production. It is declared with no fork proof: the
  grants are held to the planner's calldata by offline tests and were never installed
  in a Roles module, and no Safe has bridged on a fork or on mainnet. `almanak
  support` says so on each row, the QA catalog leaves the profile out by the
  connector's own list, and a generated fork operator file still provisions plain
  accounts for a bridge. A standalone bridge has no fork run with a Safe: its fork
  fixture funds EOA accounts only and refuses a Safe fund with that cause.
- One deployment may bind Safe and EOA accounts together. Permission generation
  makes one partition per Safe, an EOA account has none, and a deployment that
  binds no Safe is refused. The change is in the shared permission code, so it
  applies to every connector.

### Changed
- The agent skill is named `almanak-defi` and lives in `skills/almanak-defi/`. It was
  `almanak` in `skills/almanak/`. An agent finds a skill by its name and description,
  and the name now says what the skill is for.

- `--public-rpc blastapi` is the public endpoint for BNB Chain (`eip155:56`), and
  `bnbchain` is no longer a name. BNB Chain's own endpoint keeps less than a minute of
  state, so a direct action that took longer than that between a mined step and its
  reconciliation ended with `missing trie node` and a record that `ax recover` could
  not resolve through it. A record kept on `bnbchain` is resolved by naming a
  transport, such as `ax recover --record <path> --public-rpc blastapi` (#42).

- `scripts/qualify_public_rpc.py` has a tenth check, `reads_at_age`: pinned state must
  be served at a block at least 360 seconds behind the head, measured by the block
  timestamps the endpoint itself reports. That is the 120 seconds an observation may
  age plus an allowance of 240 seconds for one step of a direct action to be planned,
  signed, mined and reconciled. The read 64 blocks behind the head stays, but 64 blocks
  are about 29 seconds on BNB Chain, so an endpoint that keeps a minute of state passed
  every check. The 360 seconds are a floor the tool checks: a slower step or a later
  `ax recover` reads older state. One listed pair does not meet it and stays listed
  with the limit stated in the operating guide: `publicnode` on Avalanche leaves
  state reads older than about 100 seconds unanswered (#42).

### Fixed
- The provider guard's `web3_clientVersion` read is sent again after a transient
  failure, as its `eth_chainId` read already was. The method was missing from the
  reads the execution reader may send again, so one refused connection, timeout, HTTP
  5xx or HTTP 429 on that read before a privileged gateway request ended a direct
  action run with `--execute`, even with a signed step waiting, and left the record to
  `ax recover`. A failure that outlasts the reader's retry bounds is now marked transient
  like any other read. Other HTTP 4xx answers are unchanged (#64).
- An execution read answered with HTTP 408 Request Timeout is sent again within the
  reader's retry bounds, and a 408 that outlasts them is marked transient, as an HTTP
  5xx already was. It was a final `http` refusal, so one such answer ended a direct
  action run with `--execute` even with a signed step waiting and left the record to
  `ax recover`. Every other HTTP 4xx answer, HTTP 400 with a JSON-RPC error included,
  still ends the command at once and is sent once: it does not pass on a repeat, and
  the hold for a signed step has no time limit. A transaction submission answered 408
  is still sent once, not marked transient, and keeps its signed step, as one answered
  with a 5xx does; its message now reads `eth_sendRawTransaction: HTTP 408` instead of
  calling the submission refused (#64).
- A direct action run with `--execute` no longer ends over one failed read between two
  of its steps once a step is mined. A transient provider or storage failure was held
  only while a signed step was unresolved, so between a reconciled step and the next
  signature it ended the command with exit 1, the approvals standing and no position.
  The command now runs the check again until the next signed step is recorded, for at
  most 60 seconds per request counted over the checks that ended in such a failure,
  and then ends as before, with a message that says the mined steps stand. A check
  repeated at signing signs the same recorded plan again: both signatures are of one
  transaction, only one can be mined, and nothing unrecorded is sent. A failure before
  any step is mined and a refusal that waiting cannot change still end the command at
  once, and `ax recover` is unchanged (#42).
- A direct action run with `--execute` no longer ends over its own step that was just
  mined. When the endpoint served neither the receipt nor the transaction of that step
  in one check, the command took the wallet's advanced count for another transaction
  at the step's nonce, asked for the consumed-nonce proof, and ended on its refusal
  with exit 1 and the record kept although every step was mined, until `ax recover`
  resolved it. The command now reads the receipt again and asks again while the proof
  refuses, until those checks have taken 60 seconds for the step, and then ends as
  before. A refusal that waiting cannot change therefore ends the command up to a
  minute later. `ax recover` is unchanged, and only the full proof drops a step (#35).
- `almanak ax recover` sends a read of its public endpoint proof again after a
  transient failure, within the 8 seconds execution reads have. The reader it proves
  a named `--public-rpc` endpoint with had no retry budget, so one failed read of the
  chain number, the genesis block or the head refused the recovery (#42).
- A single execution read whose first attempt timed out is sent once more. The
  execution reader's 8 second retry budget starts with the first attempt and the
  provider timeout is 15 seconds, so a timeout had spent the budget and the read failed
  after one attempt, while a refused connection was tried about five times. The re-send
  follows a 0.5 second wait and has an 8 second provider timeout. The timeouts apply to
  each phase of an attempt and not to its total. Batched reads are unchanged, and a
  transaction submission is still never sent again (#42).
- A read that geth refuses with `missing trie node` is reported as `the endpoint does
  not serve state at that block`. It was not a known reason, so the refusal said only
  `RPC rejected the requested read (JSON-RPC -32000)`. The usual cause is a pinned
  block whose state the endpoint has pruned. The read is not sent again. Other
  clients' wordings for the same condition are not recognised (#42).
- A direct `almanak ax --execute` action, or a strategy run, no longer ends with
  `gateway coordinator heartbeat failed` because its own send was slow. A broadcast
  sends while the gateway holds the journal's write lock, and no lease renewal can be
  written until it returns; a send slower than the lease (30 s for a direct action) cost
  the coordinator its lease, although the transaction had often been mined. Time under
  that lock no longer counts against the lease: it runs for its full duration from the
  release of the lock, whether the request committed or failed, and the renewal waits
  for the coordinator's own request. The send still runs under the lock, so a signed
  step is recorded and sent exactly as before (#46).
- A lost coordinator lease is reported with the renewal failure that lost it, and a
  direct action names `almanak ax recover --record <path>`, with its record's path,
  where it said `resume to recover`.

## [3.3.1] - 2026-11-21

### Fixed
- `almanak ax portfolio` reads the vendor's current token listing of each chain the
  account has value on. It read the vendor's stored all-chain list, which kept
  balances from before an account's latest transactions. The report names the chains
  read as `chains_read` and any past the bound of eight as `chains_unread`, and
  `raw_amount` now comes from the vendor's hexadecimal base units. One command reads at
  most two accounts, where it read five, since each account now costs more requests
  (#39).

## [3.3.0] - 2026-11-20

### Added
- `almanak ax portfolio --account` reports an account's wallet tokens and protocol
  positions on every chain, with US dollar values, as Rabby's backend reports them. It
  needs no key and takes no transport. Every figure is the vendor's and unverified: the
  report is an overview, never identity, a price input or the basis of an action. The
  agent skill reads it when it loads.
- Each connector declares the direct operator actions it serves, and `almanak support`
  lists them per chain as `direct_actions`. `ax supply` and `ax withdraw` take their
  connector choices from these declarations, refuse an undeclared connector, chain or
  action before any read, and the gateway admits and plans an operator request only
  for a declared action.
- `ax supply` and `ax withdraw` take an optional `--max-gas-limit`.
- `ax supply` and `ax withdraw` serve Aave V3, SparkLend, Compound V3 and Morpho Blue
  beside Aave V4, on every chain where the connector has production support for a
  plain account, except Morpho Blue on HyperEVM. The deployment covers the wallet's
  whole position in the venue: the Pool reserves the account uses as collateral or
  has borrowed, and every leg it holds in a Comet or a Morpho market. An action the
  protocol would check against a health floor is refused while the wallet has debt
  there, and the refusal says so.
- `ax borrow`, `ax repay` and `ax collateral`, and an optional `--amount` on
  `ax withdraw` and `ax repay`, for the forms each lending connector serves.
  `--min-health-factor` states the lowest health factor a projected action may leave
  the account with: it is required to borrow, and for any other projected action
  while the wallet has debt in the venue. No floor can refuse a repayment.
- `ax swap` sells an exact amount of one pool currency for another in one named pool,
  on Uniswap V3, Uniswap V4, PancakeSwap V3, Aerodrome, Aerodrome Slipstream and
  Curve, with a required `--slippage-bps` below the pool's own quote at the observed
  block. The result lists what was sold and bought and any allowance left.
- `ax open-liquidity`, `ax increase-liquidity`, `ax decrease-liquidity`,
  `ax collect-liquidity-fees` and `ax close-liquidity` open and manage a
  concentrated-liquidity position on Uniswap V3, PancakeSwap V3 and Aerodrome
  Slipstream, in a tick range and from amounts the operator states. An existing
  position is named by `--token-id`, whoever opened it. Nothing is swapped inside a
  liquidity action. A mint or an increase reverts when mined at a price further than
  `--slippage-bps` from the observed one in either direction, within what the range
  itself holds. `ax revoke-exchange`
  sets the allowances a command left to zero, and `ax liquidity-position` reads a
  position.
- A liquidity step mined after another account added to the position's NFT is
  recorded with the position as its block holds it and an accounting issue, for
  strategies and direct actions alike, so the step resolves.
- The liquidity commands also serve Curve, whose position is the wallet's balance of
  the pool's LP token: `open-liquidity` and `increase-liquidity` deposit an exact
  `--amount` per named coin, and `decrease-liquidity` and `close-liquidity` state with
  `--receive` whether the burn pays every coin in proportion or one coin. They serve
  Aerodrome stable and volatile pools too, whose position is the wallet's balance of
  the pool's share token. An Aerodrome deposit offers the stated amounts in the
  proportion of the pool's reserves and reverts when that proportion moved further
  than `--slippage-bps` in either direction.
- `ax open-liquidity`, `ax collect-liquidity-fees` and `ax close-liquidity` serve
  Uniswap V4, with the pool named by its key and a native pool provided in the wrapped
  token. A mint reverts when mined at a price further than `--slippage-bps` from the
  price its step was planned at, within what the range itself holds.
- `ax revoke-exchange` serves Uniswap V4. For each of the pool's two tokens it sets the
  ERC-20 approval to Permit2 to zero, then the Permit2 allowances to the router and
  the position manager. The approval to Permit2 is shared with every other
  application that spends the token through Permit2, which needs it approved again
  afterwards; the result says so in `note` before anything is signed. A Uniswap V4
  result lists a Permit2 allowance that holds no amount and has not expired under
  `permit2_unexpired`, and the revocation expires it.
- `--public-rpc` serves `almanak ax` reads on Base, Arbitrum, BNB, Optimism, Polygon and
  Robinhood beside Ethereum and Avalanche. `drpc` adds Base, Optimism, Polygon and
  Robinhood, and four chain operators' own endpoints are named `base`, `arbitrum`,
  `bnbchain` and `optimism`. Arbitrum, BNB, Polygon and Robinhood have one endpoint
  each, and HyperEVM has none. A direct action also runs through one, on the chains
  where `almanak support` lists it.
  Compound V3 listings, Morpho Blue listings on Avalanche and BNB, and the lookups by
  name are refused through a public endpoint; `docs/operating.md` says what works.
- `scripts/qualify_public_rpc.py` checks a public endpoint, live, against a sample of
  the reads an observation and a direct action make of it: the pinned genesis, pinned
  state at `finalized`, `latest` and a numbered block, the fee quote and the receipt
  reads. It sends nothing. A (name, chain) pair is listed only after it passed every
  check in repeated full runs; a pass is a point-in-time result, and a transient
  refusal fails a run as it would fail a command.
- `ax recover --record` takes one named transport, `--public-rpc <name>` or
  `--env-file`, and resolves the record through that endpoint of the record's chain
  instead of the endpoint its action ran on. A named public endpoint must show the
  pinned genesis and a recent head first. A record whose public endpoint is no longer
  listed for its chain is refused until a transport is named, so a pair that leaves
  the list strands no wallet claim.

### Changed
- The Aave V3 backtest model serves history under Pool revision 11 and 12 and keeps
  its identifiers (`aave-v3-revision11-economic-indices-v1`,
  `aave-v3-revision11-prepared-reserves-v1` and the liquidation model
  `immediate_underlying_v3_revision11`): they name the accounting model, which
  revision 12 runs unchanged. A dataset prepared earlier stays valid, and a window
  may span a Pool's upgrade.
- A direct action bids as an author's gas policy defaults on its chain: the observed
  block's base fee on Avalanche, 1.25 times the base fee on Arbitrum and Robinhood
  Chain, and a gas limit of at most 1,200,000 unless `--max-gas-limit` states another.
- The agent skill (`skills/almanak/SKILL.md`) covers every direct action: lending with
  borrowing, repayment and collateral, swaps, and liquidity in a tick range or as
  Curve or Aerodrome LP tokens. It holds no list of chains: the agent reads `direct_actions` from
  `almanak support` before each action. It asks the user for the health floor, the
  slippage, the tick range and the payout of a Curve burn, and explains what each one
  does not protect against. Its public endpoint table names
  each chain's native asset, and its `settings.json` notes the markets, the liquidity
  positions and the tokens the wallet holds. It resolves with `ax recover` only a
  record of its own wallet, and asks before a signed transaction that is not from an
  action the user has just confirmed is sent again. `tests/test_agent_skill.py` holds
  the skill's command lines, the flags it names, and its command, selector, public
  endpoint, position field, health floor and result field tables to the parser, the
  declarations and the models. A refusal the skill names is a kind the SDK raises or
  part of a message it raises. The rules an agent must keep are checked only as text.
- The agent skill reads `almanak support` filtered, by `direct_actions` and the
  lines of `restrictions` that name pools or markets, and treats an entry without
  `direct_actions` as serving nothing. It states the Curve limit (pools of the
  reviewed StableSwap NG, TwoCrypto NG and TriCrypto NG factories, no metapool),
  names a Curve coin by its CAIP-19 asset, and has rows for `unsupported_pool`,
  `unsupported_revision` and a refused event-history listing, which it does not
  retry through a public endpoint. It creates the wallet when a request it can
  serve first needs the agent's own address, never reads `gateway-auth.token`, and
  takes no address from its own examples. It reports `rates` from
  `ax lending-reserve` and the reason where there is none, computes prices with a
  tool, reports a health factor of 2^256 - 1 as no debt, copies a Uniswap V4 key
  whole, and asks for both limits of a tick-range open inside the price, since no
  read reports the ratio a range needs. It previews a supply, a swap or a deposit
  only once the wallet holds the funds, and gives a preview the time limit of an
  execution.

### Removed
- `--public-rpc drpc` no longer serves Avalanche. Its keyless tier intermittently
  refuses reads pinned to a block and the gas estimate a direct action makes at a block
  hash, and it failed qualification in repeated runs. Avalanche keeps `publicnode` as
  its one public endpoint. A record kept from an action that ran through `drpc` on
  Avalanche resolves with `ax recover --record <record> --public-rpc publicnode`.

### Fixed
- Aave V3 Pool revision 12 is admitted on all seven chains. On 2026-11-11 every Aave V3
  Pool the connector lists moved to Pool revision 12 (Aave v3.8), whose code the
  connector had not verified, so each read, check, run, fork at a later block and
  direct action stopped with `deployment_changed: Aave V3 pool has an unverified
  revision`. The connector admits each chain's revision 12 Pool implementation by code
  hash beside the revision 11 one, each on its own chain only, and refuses a Pool that
  reports another revision than the one its code was verified as. Reads, backtests,
  direct actions, plain-account deployments and Safe/Roles deployments created while
  the Pool runs revision 12 are served. Revision 12 adds a reentrancy guard to the
  Pool's entry points. Its function signatures, events and storage layout are those of
  revision 11, it links the same supply, borrow, liquidation and pool logic
  libraries, and the reserve tokens are unchanged, so calldata, receipt accounting,
  the health projection and the generated Safe/Roles grants are the same under either
  revision.
  A Safe/Roles deployment keeps the permission manifest of its creation,
  which records the code of each contract it covers. One created while the Pool ran
  revision 11 is refused on every plan, its repayments, withdrawals and teardown
  included, with `manifest contract identities differ from the supported profile`,
  although its installed grants are the ones revision 12 needs. A plain-account
  deployment retains no manifest and continues across the upgrade.
- A signed step of `ax swap` on Aerodrome is sent although pool shares reached the
  wallet after the signature. The step is reconciled from its own receipt with the
  shares listed under `accounting_issues`, and they are never attributed to the
  deployment. Such a step was held back before its first send for as long as the
  wallet held the shares, which kept the record and the wallet's claim: a signed
  approval carries no deadline to release it by. A strategy's trading-only Aerodrome
  allocation, composed or not, sends its signed approval, revocation and exit swap
  the same way. Its signed entry swap is still held back while the fund holds the
  shares and is never sent past them; past its deadline it can only be released,
  which follows the removal of the shares. A fund that holds shares is still refused
  before anything is signed.
- `ax withdraw` on Aave V4 refuses a reserve that backs the wallet's open debt on the
  Spoke. It was held back only by the Spoke's own limit of a health factor of 1,
  while the documentation said every withdrawal of collateral was refused. A
  withdrawal of collateral with no debt open works, and the documentation says so.
- The `--public-rpc` help no longer says the endpoint is for reads and previews only:
  direct actions execute through it.
- `ax recover` releases a signed step that was never mined and whose nonce another
  transaction of the wallet used, once that transaction is final: at or below the
  endpoint's `finalized` block and behind its head by the network's margin, which
  `docs/operating.md` lists. Such a record answered `nonce_conflict` on every
  `ax recover` and kept the wallet's claim for good, for every direct action and step.
  The result lists the step under `released_consumed_nonce` with the transaction that
  used the nonce and its block, and the command exits 1 because the step did not
  happen. While that transaction is not final the answer is still `nonce_conflict`.
  Polygon PoS and HyperEVM state no margin: nothing is released there, and using the
  nonce of a stuck step there leaves its record unresolvable. Strategy deployments are
  unchanged.

## [3.2.1] - 2026-11-19

### Changed
- `ax supply` and `ax withdraw` execute through a named public endpoint
  (`--public-rpc`), so a direct action needs no provider key. `ax recover` uses the
  endpoint its record's action ran on and takes `--env-file` only for a record that
  used a provider key.

## [3.2.0] - 2026-11-19

### Added
- `almanak ax supply` and `almanak ax withdraw` run one operator action for one wallet in
  one process, without a strategy, workspace or gateway process. Without `--execute` the
  first step is planned and nothing is signed. They support Aave V4 with an EOA wallet,
  take required `--max-gas-price-wei` and `--max-transaction-fee` ceilings, and adopt the
  position the wallet already holds.
- `almanak ax recover` lists the records of operator actions that stopped with a signed
  step unresolved, and resolves one from its receipt or by sending the signed bytes
  again. It plans and signs nothing, and releases the wallet's claim once resolved.
- `--public-rpc <name>` reads through a named public endpoint that needs no provider key,
  for `almanak ax` reads and previews of `ax supply` and `ax withdraw` on Ethereum and
  Avalanche. The endpoint must show the chain's pinned genesis and a recent head.
  Execution and every other command still need a provider key or an operator RPC URL.
- An agent skill, `skills/almanak/SKILL.md`, teaches an AI agent to manage positions from
  plain requests with the operator actions: setup on load, then preview, confirm and
  execute. `docs/agents.md` covers installing it.
- `almanak wallet create` takes `--password-file` or `--generate-password`, so a keystore
  can be created without a terminal. A generated password is written to a private
  `keystore-password` file beside the keystore.

### Removed
- Publishing release candidates to a package index. Stable releases go to PyPI only; a
  release candidate builds and verifies its distributions as a workflow artifact and
  publishes nothing.

### Changed
- A coordinator stopped on `execution_failed` or `accounting`, which a recorded revert or
  accounting issue makes every resume repeat, is reported with the failure
  `teardown_required` instead of `execution`. Its next command is `almanak teardown --run`
  and its diagnostic says teardown is required instead of "explicitly resume". Every other
  coordinator stop stays `execution`.
- A supervisor that `almanak teardown` relaunches reports `provisioned` from its first
  status until the teardown control arrives, instead of the previous supervisor's `failed`
  or `shutdown` stage, which hosts treat as final. A teardown of a workspace with no
  execution state is refused before any service starts and never creates a coordinator.
- The production check's decision-interval warning speaks of a live deployment.
- A Hyperliquid production run given `--signer-file` is refused with "a remote signer
  holds no agent keys": the gateway alone generates and holds agent keys.

## [3.1.1] - 2026-11-05

### Added
- `almanak ax hypercore-markets` and `almanak ax hypercore-account` read Hyperliquid's
  HyperCore, a venue of HyperEVM, from Hyperliquid's `/info` API: each first-dex
  perpetual market's asset index, `meta` entry, margin tiers, prices and funding, and an
  account's role, abstraction mode, perp margin and positions, spot balances, each named
  by its HyperEVM CAIP-19 asset, open orders and approved agents with their expiries.
  The API has no as-of query, so these take no `--block` and report when each answer was
  received. Every answer must match the shape recorded from mainnet, or it is refused as
  `malformed`, and one for another market, account, order or time range is refused as
  `source_mismatch`. The read gateway serves the same queries as its `info` operation,
  capped by `ALMANAK_GATEWAY_HYPERLIQUID_WEIGHT_PER_MINUTE` in the API's request weight;
  a fill, funding or ledger page reserves the most it can cost until its answer is
  counted.
- `almanak markets --out .almanak/markets.json`: what each market in `config.yaml`
  resolves to at one pinned block, its contracts, tokens and ids with each token's symbol
  and decimals, for checking before approval. It reads no prices.
- `ax pool` reports an Aerodrome classic pool's `reserves` beside its price: each
  currency's stored reserve with `asset`, `amount` in base units, `decimals` and the
  decimal `quantity`.
- `context.decision(intent, memory=..., reason=...)`: a strategy can say in its own words
  why it holds or acts. Only a test run logs it; the runtime never acts on it and the
  checkpoint never keeps it. Every published reference states its hold and act reasons.
- Test runs (`almanak test run`) log two more coordinator lines: `run_metadata` once at
  start (each verified asset's symbol and decimals, each allocation's connector) and
  `transaction_executed` after each committed transaction (allocation, operation,
  success, gas paid, the fund's token changes, a decoded revert); `decision_evaluated`
  carries the strategy's `reason`. Live runs log none of them.

- Dashboard line charts show the reading under the pointer (or a finger on a touch
  screen): a guide snaps to the nearest reading and a label gives its time, each series'
  value at the display precision, and any marker there ("Sell"), so a trade reads with its
  time and price.

### Changed
- Every connector's dashboard tables follow one convention:
  - Exchange dashboards (Uniswap V3/V4, PancakeSwap V3, Aerodrome, Slipstream, Curve) list
    token inventory one row per token under Position / Token / Amount, labelled by symbol,
    instead of "Currency 0/1 inventory" columns. A composed strategy's `fund_inventory`
    does the same under Token / Amount.
  - A fund account (or GMX fund address) column appears only when the strategy uses more
    than one: in `exchange_inventory`, `payer_costs`, `fund_costs`, `lending_positions`,
    `supply_positions`, `debt_positions` and GMX's `positions`, `gm_shares` and `requests`.
    Allocation headers read "Position".
  - Every connector projects a `transactions` dataset: each committed transaction the
    strategy submitted (newest 200), named in its own terms ("Swap USDC → XDP", "Open LP
    position", "Supply USDC", "Use WETH as collateral", "Open long ETH"), with "(reverted)"
    when it failed, its receipt gas with the native symbol and its UTC time. Aave V4's
    replaces its Action / Result / Transaction / "Block time (Unix)" columns and keeps
    "Asset effect". Lending `fund_costs` gas carries the native symbol.
  - Lending, repayment and composed cash amounts carry their token symbol ("0.009 WETH",
    not "asset units"). GMX tables read in tokens and time: the market by its pair
    ("WETH/USDC"), GM shares and request fees in tokens ("0.02 WETH"; the separate fee
    asset column is gone), admission deadlines in UTC and submitter gas in ETH;
    `requests` loses `asset` and `submitters` loses `asset`.
  - Dashboards that read rows by removed keys must change: `exchange_inventory`'s
    `inventory0`/`inventory1`/`asset` and `fund_inventory`'s `asset` are now `token` and
    `amount`; Aave V4 `transactions` rows lose `operation`, `result` and `hash` for
    `transaction` and `gas`; a single-account table no longer has `account` (or GMX
    `fund`).
- A transaction that approves zero (a teardown revoke, or the reset before re-approving on
  Uniswap V3, PancakeSwap, Aerodrome, Curve and GMX) is stated as `revoke` in the test
  run's `transaction_executed` lines and the exported history, so the fork-test view no
  longer reads it as a new approval.
- The loop and borrow references word their capped-round and repay-shortfall messages in
  plain terms, with the owed amount in tokens instead of base units and parameter names.
- `almanak check`'s `gas_amount_units` ceiling follows the chain: a Polygon gas fee,
  budget or reserve warns above 10,000 whole POL (a real budget there is many cheap
  units) instead of 10; every other chain keeps 10 whole native units.

### Fixed
- `almanak markets` and `almanak ax` resolve HyperEVM at their default block. `finalized`
  is HyperEVM's head, and Alchemy runs a read pinned within about 128 to 192 blocks of
  its head at the next block, so the block-context proof refused every read at
  `finalized` or `latest` as `provider_context`. On HyperEVM those tags now pin the block
  256 blocks (about four minutes) behind, through `--env-file` or the read gateway; an
  explicit `--block` is unchanged, and a read run at any other block is still refused.
  The runtime's own reads keep pinning the head.
- Base transactions are accounted again. Base upgraded its L1Block fee contract to 1.8.0 at
  block 53508490 (2026-11-04 15:32 UTC), and the SDK refused the new implementation as an
  unverified fee revision, so every Base transaction failed its receipt accounting after it
  was sent. The new implementation differs from the accepted 1.7.0 in one byte, its
  version string, so its code hash joins the accepted L1Block revisions.
- A line chart's value axis prints enough decimals to tell its two ends apart. A token
  priced under a cent (0.0201 over a 0.0171 stop) read "0.02" at both ends, so a moving
  price looked flat.
- A line chart outside a group keeps the same 30 px below it as a table does, instead of
  touching the block under it.

## [3.1.0] - 2026-10-28

### Added
- Composed lending decisions read owned cash: in `decide`, each Aave V3, Aave V4,
  SparkLend, Morpho Blue and Compound V3 record in `ctx.owned` reports the fund's owned
  inventory of its market asset as `inventory`, after the last committed outcome, in
  live, fork and backtest runs alike. A rotation can withdraw from one market and then
  supply exactly what the fund now owns, interest included, to another. Checkpoints and
  single-connector strategies are unchanged (`inventory` stays `None`).
- Typed `strategy.parameters`: a strategy may subclass `almanak.api.TypedParameters`
  once (a frozen pydantic model refusing undeclared keys) to declare its knobs with
  types, constraints and defaults. The SDK validates the authored values, after any
  `test_parameters` or `--param`, before any hook runs; a refused value is the final
  `invalid_config` naming `strategy.parameters.<field>`, with that field as the
  refusal's `field`. Every hook then reads `config.parameters` as the validated
  instance (`config.parameters.borrow_quantity`), and `--param` and `test_parameters`
  values keep their YAML form for the model's types to convert; as without a model, a
  `bool` field takes only `true` or `false` and a number field refuses a boolean or a
  `_`. The model is a top-level class; one the source can't show (under `if`, built
  with `type(...)`, or with a plain mixin base) is read from the strategy loaded in
  the sandbox. With a model,
  `--param` and `test_parameters` may set any field the model declares, including one
  config.yaml leaves to its default (fork copy only); a key the model doesn't declare
  is refused with the model's fields listed. `almanak.api` adds
  `PositiveDecimal` and `UnitFraction`. A strategy without a model keeps the mapping.
- Optional `strategy.test_parameters` in config.yaml: fork-test values for declared
  `strategy.parameters` keys, in the declared type. `almanak test run` applies them by
  default, `--param` overrides one on top, and `--production-parameters` turns them off;
  the verdict adds `parameter_sources` (`test_parameters` or `--param`) next to
  `parameter_overrides`. `almanak check` validates the section. Strategy code never
  sees it in production or backtests, and it leaves every digest unchanged.
- Optional `strategy.parameters` in config.yaml: author knobs (scalars or flat lists of
  scalars, keys `^[a-z][a-z0-9_]*$`) that strategies read as the read-only
  `config.parameters` in every hook, in forks, production and backtests alike. Checks
  validate only their shape; strategies validate values in `validate_config`. An omitted
  section leaves every digest unchanged. `almanak test run --param KEY=VALUE`
  (repeatable) overrides a declared parameter in the fork workspace's copy only, in its
  declared type (a string knob keeps the text as written, a float knob takes any finite
  number, an integer refuses `_` and exponents, `null` only for a `null` knob), and the
  verdict records it under `parameter_overrides`. Fork fixture deployments built
  directly from a fork config (`ForkSet.prepare`, as the strategy-fork QA harness does)
  carry the parameters too.
- `almanak backtest recent` rewrites an economic backtest file for the most recent
  window (30 days by default): hourly decisions and sampling, limits and tick coverage
  sized for the window, and `fixed_native` costs from measured gas at the window's gas
  price. Connectors that read every block, unvalued runs and operations without a
  measured gas reference are refused with a reason.
- HyperEVM (`eip155:999`) is a supported network, with gas paid in HYPE
  (`eip155:999/slip44:2457`) and WHYPE as its wrapped native token, read through
  Alchemy's `hyperliquid-mainnet` route. Morpho Blue is its first connector, declared for
  fork, backtest (from 2026-10-20 04:00 UTC) and production with EOA and Safe/Roles
  accounts. `ax lending-markets` lists its markets from Morpho's API, and `ax resolve`
  and `ax dex-pools` name candidates through CoinGecko's `hyperevm` network. The
  `morpho_blue_hyperevm_lending` reference supplies USDC and borrows it against WHYPE.
- `UnixTime` (price-series `source_time`, `available_at` and the other Unix-second time fields) gains
  `timestamp()`, returning the seconds as an int, and `datetime()`, returning the UTC datetime. Code
  written for a datetime field such as `effective_at` (`int(x.timestamp())`) now also reads a
  `UnixTime` field instead of raising `AttributeError`.
- The `almanak test run` verdict reports how teardown went and what it left. Additive;
  existing fields are unchanged. `teardown_decisions` lists every teardown decision
  with its result and facts, so a teardown that held with obligations open is visible
  (`closure: not_closed` with its `refusal`). `teardown_sequence` lists every intent
  teardown executed, in order with repeats, which `teardown_executed` lists once.
  `holdings` gives each fund's final balance of every configured asset, read from the
  fork, so an exit that left the fund in the wrong asset shows in a passing verdict.
- `almanak.api.InvalidConfig`: a `validate_config` refusal is now a final
  `invalid_config` with the author's message and line, at `check --online` (code
  `strategy_config_refused`) and at `test run`/`run` setup, which records the
  non-retryable failure `invalid_config` instead of a retryable `start`. A plain
  `raise ValueError(...)` in `validate_config` itself counts too; unpacking,
  conversion and pydantic ValueErrors stay crashes. Strategy worker crashes now report
  the traceback from the strategy's first frame, with its source lines.
- `ax lending-reserve` reports a `risk` block: per collateral asset its `ltv_bps`,
  `liquidation_threshold_bps` and `liquidation_bonus_bps`, decoded from the Aave V3 and
  SparkLend configuration word, the Aave V4 reserve's current dynamic config (with the
  Spoke's liquidation config, and `collateral_risk_bps` labelled as the risk premium it
  is), Compound V3's collateral factors and Morpho Blue's LLTV and liquidation incentive
  factor, with the `health_factor` and `borrow_limit` definitions. Optional
  `--borrow-fraction f` (repeatable) and `--health-floor H` project one collateral's
  health factor, liquidation_threshold / f, and its largest safe borrow fraction,
  min(ltv, liquidation_threshold / H). `ax lending-markets` adds the Aave V3 and
  SparkLend `liquidation_bonus_bps`, Compound V3 `collateral_factors` and the Morpho
  `liquidation_incentive_factor`. The Aave V4 dynamic and liquidation config reads move
  from the backtest into the connector reader, unchanged.
- The `almanak test run` verdict names why a lifecycle held, failed or ran out of time.
  Additive; `passed` and the exit code are unchanged. A new `status` is `entered`,
  `not_entered` (passed, but the strategy only held, so only admission and teardown ran)
  or `failed`. Decision rows carry `observation_errors`, each `*_error` field an
  observation recorded instead of a measurement (`allocation`, `field`, `message`), such
  as the `entry_quote_error` of an empty pool. Transaction rows carry `trigger`
  (`decision` or `teardown`), and `teardown_executed` lists the intent kinds teardown
  executed. A refusal's structured `details` reach its failure entry: a lending
  teardown that cannot close (`remaining_obligations`, on Aave, Spark, Compound and
  Morpho) lists each allocation's remaining supply, debt, collateral and allowance with
  amounts and asset, in `details.remaining` and in its message. `repayment` reports the
  runtime's latest debt, available balance and shortfall per borrowing allocation.
- A teardown that refuses an exposure-increasing intent (it is still dropped, never
  admitted) now logs `exposure_refused_in_teardown` with the intent kind and
  allocation. `almanak test run` ends teardown as `teardown_refused` (class
  `strategy`) after 3 such refusals with no teardown outcome between them, naming the
  intent and allocation in `details`, instead of waiting out the 900-second teardown.
- `ax pool` and `ax dex-pools` report a Curve pool's state beside its prices, from the
  reads the connector already makes: `pool_state` with `balances` and `total_supply`,
  the `fee` (StableSwap base fee, or TwoCrypto/TriCrypto `mid_fee`, `out_fee`,
  `fee_gamma`) as Curve's 1e10 integer and in bps, `amplification`, `virtual_price`,
  and StableSwap's `offpeg_fee_multiplier` or the crypto pools' `gamma`.
- A Curve pool whose coin balances or LP supply include a zero is reported as unfunded:
  `ax pool` returns `"status": "unfunded_pool"` with the reason and balances (an empty
  TwoCrypto or TriCrypto pool was refused with `invalid_config` before), `ax dex-pools`
  lists it as `refused` with reason `unfunded_pool: …`, and `check --online` for fork and
  production refuses it in a new `pool_liquidity` stage with code `unfunded_pool`, read at
  the latest block. The rule is the one `backtest prepare` already applied: Curve
  execution requires an existing funded pool.
- `almanak ax lending-reserve` and every verified `ax lending-markets` row report a
  `rates` block for Aave V3, SparkLend, Compound V3 and Morpho Blue: `supply_apr`,
  `supply_apy`, `borrow_apr` and `borrow_apy` as decimal ratios per year (0.05 is 5%),
  their `compounding` convention, `available_liquidity` in the asset's units and the
  `raw` protocol values. Aave ray rates are per-year APRs compounded per second for
  APY; Compound V3's per-second curve rates at the Comet's utilization likewise
  compound per second; Morpho's IRM borrow rate compounds continuously, and so does its
  supply rate, borrow rate x utilization x (1 - fee). `rates.asset` names the lent asset
  the rates belong to (the reserve's asset, the Comet's base, Morpho's loan asset) and
  `rates.selected_asset` what the selected asset itself earns: the market's supply
  rates when it is lent, zero when it is Compound or Morpho collateral. The conversion uses exact arithmetic over values
  the reserve reader already observes and adds no contract read; `lending-markets`
  makes that same reserve read for each verified row on the page. Aave V4 reports
  `rates` null with `rates_unavailable`, as its reserve observation reads no Hub drawn
  rate or liquidity. Additive.
- `almanak check --online` warns `gas_ceiling_oversized` on `max_transaction_fee` when it
  is more than 20 times the executable fee: `max_gas_limit` x the policy's highest bid over
  the last 100 blocks (`eth_feeHistory` at its `reward_percentile` and
  `base_fee_multiplier_bps`, floored by `eth_gasPrice` on BNB), plus on Base and Optimism
  the SDK's L1 data and operator fee bound for a transaction of `max_gas_limit` gas. The
  payer's entry minimum is entry transactions x `max_transaction_fee` + `exit_reserve`, so
  fees sized for 10 to 1000 gwei on chains quoting 0.001 to 0.2 gwei put funding
  requirements 20 to 1000x above the real cost. The message states the factor, the bid
  and the L1 part, and suggests `max_transaction_fee` = 5 x the executable fee rounded up
  to two significant digits, with `exit_reserve` = exit transactions x that fee. A node
  that refuses the 100-block window falls back to the single-block quote, which the
  message says. Advisory only, and never reported with `gas_price` or `gas_fee_quote`. The
  `uniswap_v3_optimism_lp` reference gas policy is resized to October 2026 quotes.
- `almanak test run` decision rows state what the runtime knew when it evaluated each
  decision: whether the strategy ran (`evaluated`, false with `price_error` when an
  unavailable price series held it first), the `observed` allocations, each declared
  `price_series`' completeness (`complete`, `points` of `window`, `gaps`, `age_seconds`)
  and the chains not ready (`availability`). Only a test run's coordinator logs them, as
  one `decision_evaluated` line per decision after the decision is recorded; production
  deployments never do. Additive; a hold's reason is not inferred.
- The generated support page lists each connector's strategy actions: the methods
  bound to `ctx.actions` with their signatures and what each does (for example what
  `exit` closes, converts and revokes), read from the connector bindings and their
  docstrings, under one "<connector> actions" heading per connector.
- Uniswap V4 liquidity allocations, fixed-range and banded, on pools holding the chain's
  native coin, on every chain with a reviewed native route. The fund deposits and
  receives the wrapped native (WETH, WPOL, WBNB, WAVAX) and never sends native value: the
  reviewed PositionManager unwraps the deposit, returns what the mint did not use, and
  wraps the native proceeds of collect and close within the same call. A strategy cannot
  combine native and ERC-20 Uniswap V4 liquidity on one chain, and holds native liquidity
  in one token pair per chain (fee tiers of that pair may differ): a larger grant exceeds
  the Roles 2.1 condition limit and is refused when permissions are generated. A native
  mint needs about 1.1M gas, above the 1.2M default limit once its 25% buffer is added, so
  `check` warns a native liquidity strategy to raise `max_gas_limit`; native-pool gas
  references are kept apart (`variant: native`) and bound no trading or ERC-20 strategy.
- `almanak support --json` reports each connector's `gas_lifecycle`: per allocation kind,
  the transactions one entry and one exit submit, the counts the `funding` block and
  `almanak check` use. A lending allocation granted `borrow` exits in 3 (approve,
  repay, revoke; `exit_when_granted`), an Aave V4 debt in 3 plus 1 per collateral it
  disables (`exit_per_item`), as the rig's fork teardowns submit; `check`'s
  `gas_exit_reserve` counts them instead of 1, so a reserve sized for 1 per debt now
  warns. `exit_per_unwind_cycle` (3 on Aave V3, SparkLend, Compound V3 and Morpho Blue)
  is what one partial deleverage cycle submits on the lending connector; a leverage loop
  unwound in C cycles exits in the summed exits + 1 + (C - 1) x (that + the swap's
  exit). `check` cannot see C and counts none. A V3-style pool liquidity exit (Uniswap
  V3, PancakeSwap V3, Aerodrome Slipstream) is 5: close, approve, swap and a revoke for
  each position-manager allowance the mint left, which rounding can leave on both
  currencies. A GMX position exits in 3: approve the order fee, create the close order,
  claim accrued funding; the keeper's execution is not the payer's.

### Changed
- Every published reference states a `strategy.gas` policy in its `config.yaml`, so a
  copy deploys in production without first writing one: the 38 that had none get
  example amounts from October 2026 mainnet fees, with `max_transaction_fee` sized as
  `almanak check --online` suggests at the default 1.2M gas limit (2.3M on GMX V2),
  `exit_reserve` covering the exit transactions `almanak check --production` counts
  and `total_budget` adding the entry transactions plus 20 later ones. Their fork
  operator examples carry matching gas ceilings, or `author_gas: unenforced` on Base.
- A single-evaluation strategy worker imports the request's connector before its
  author budget starts, under the 30s startup bound, so the 20s wall and CPU budgets
  cover only strategy code; limits and syscall filters still apply before strategy
  source loads. Connectors import their historical and economic backtest bindings only
  when a backtest selects one, so a live or fork decision no longer imports backtest,
  gateway-service or signing modules: a cold composed Aave V3 + Uniswap V3 decision
  uses 2.5 instead of 4.7 CPU-seconds and 257 instead of 365 MiB.
- Reference strategies and the authoring templates declare their `strategy.parameters`
  as a `StrategyParams(TypedParameters)` model instead of module constants or
  hand-written parsers: `borrow_quantity` on the lending references; period, window
  and levels on the TA references; Aave V4's `unwind_margin`. Rules between knobs sit
  in the model; rules that read the rest of the configuration stay in
  `validate_config`. Behaviour at the shipped values is unchanged; each converted
  reference's configuration digest changes.
- The loan allocation of `aave_v3_arbitrum_lending`, `aave_v3_base_sma_borrow`,
  `compound_v3_{base,ethereum}_lending`, `morpho_blue_{base,ethereum}_lending`,
  `sparklend_ethereum_lending` and the managed-borrow template accepts an optional
  `capital`, a loan-asset reserve for the full repay's interest, which these refused
  before. The other `aave_v3_*_lending` references still refuse it. The managed-borrow
  template ships `capital: '1'` there.
- `almanak.api` exports `MAX_WINDOW` and `parameter_base_units`, which converts a
  quantity knob to base units and refuses one finer than the asset's decimals, naming
  the field. The lending references refuse such a `borrow_quantity` in
  `validate_config` instead of truncating it to 0. Rules between knobs on the TA
  references name the field they refuse, and the GMX price-stop template refuses
  acceptable prices that don't suit its `side`.
- `almanak support --references` lists the six authoring templates (Aave V3 and Morpho
  Blue leverage loops, Aave V3 borrow-then-LP, lending short and health-factor-managed
  borrow, GMX V2 price stop) as published references. The loop templates declare
  `loops: 1` in `strategy.test_parameters`.
- The gateway and `almanak test run` decode the execution checkpoint less often, since
  it grows with every outcome (about 1 MB after 18 transactions). `renew` and `state`
  decode and check the stored checkpoint only when its stored body differs from the
  last one accepted, and `state` returns the stored checkpoint unchanged. A lease-fenced
  request decodes its checkpoint once. The test run's keeper poll decodes only for a
  connector with a fork keeper, and only when the checkpoint changed. Replies and
  refusals are unchanged.
- `almanak test run` classes `interrupt`, `decision_timeout`, the new
  `teardown_timeout` (teardown not closed within its limit, formerly `stopped`) and
  `worker_timeout` as `infra` failures, the three wall-time timeouts as `transient`.
  `worker_timeout` is a preloaded strategy worker that did not answer during setup: it
  raises a typed timeout, and the supervisor still records the retryable `start`
  failure, now with `cause: worker_timeout` in its status.
- A transaction plan the gateway's re-preparation does not reproduce when it is reserved
  is still refused and still stops the coordinator, but the refusal now names the parts
  that differ (`snapshot`, or the plan fields such as `gas_price`), never their values.
  A fork test's teardown no longer advances fork time while a decision awaits its plan,
  a cause of the mismatch.
- Three author fields are no longer required where they have no effect. Configurations
  that set them validate and behave as before.
  - `collect_seconds` is optional on Uniswap V4 liquidity allocations. Only strategy code
    reads it, to time its own fee collection; connectors never collect on a schedule.
    Omitted means no scheduled collection, and closing the position still takes its fees.
    A trading allocation still refuses it.
  - `entry_swap_bps` accepts 0 on liquidity allocations (Uniswap V3 and V4, PancakeSwap
    V3, Aerodrome, Aerodrome Slipstream, Curve): no entry quote is offered, which fits a
    deposit without an entry swap such as a single-sided Curve deposit. Trading
    allocations still require at least 1. The concentrated-pool schema descriptions note
    that a band entering through `rebalance_quote` does not use it, since that quote sizes
    itself from the centred position.
  - `minimum_health_factor` is optional on Aave V3, SparkLend, Morpho and Compound V3
    allocations that cannot borrow, in a fund and Pool, market or Comet where no
    allocation borrows. It stays required with `borrow: true`, and every allocation in that
    scope still states the same value. An omitted floor tolerates no debt: while the fund
    carries any debt in that scope, an action the connector risk-checks is refused.
- A liquidity `band` on Uniswap V3 and V4, PancakeSwap V3 and Aerodrome Slipstream no
  longer requires a tick envelope: `band: {width_bps: 500}` recentres around the price
  for the whole deployment instead of holding out of range once the price leaves a fixed
  `[lower, upper]`. `lower` and `upper` remain an optional author bound, given together,
  and a band with them behaves as before. The gateway admits an envelope-less mint only
  at the aligned range of exactly the band width centred on the retained decision tick.
  Economic backtests of such a band take the explicit `data.coverage`, which must hold
  an aligned band width and the first centred position, and stop with an error if a
  position would leave it.
- `hold_seconds` is optional on Uniswap V3 and V4, PancakeSwap V3, Aerodrome,
  Aerodrome Slipstream, Curve and GMX V2 allocations, as it already was on Aave V4.
  Connectors never act on it; only strategy code reads it for its own timed exit, so
  omitting it means no scheduled exit and the allocation holds until teardown. Uniswap
  V4 checks that fee collection precedes the hold only when a hold is set. Files that
  set it validate and behave as before.

### Fixed
- Market resolution waits out a provider's rate limit instead of failing the run it
  starts. The bootstrap gateway resolves a run's markets with a reader that had no retry
  budget, so one HTTP 429 during resolution ended a backtest with `eth_getCode: HTTP 429`
  (seen when many recent-window backtests start at once on one provider key). Its reader
  now re-sends a transient read within the observation readers' budget: a throttle, an
  HTTP 5xx, a body that is not JSON, a transport failure or a block the provider serves
  late. Refusals and malformed answers still fail at once.
- A provider answer that is not JSON (an overloaded provider's HTML page with HTTP 200)
  is now a provider failure a later attempt may pass, like an HTTP 5xx, instead of a
  malformed answer. A reader with a retry budget sends the read again, the backtest
  gateway re-sends a single read or a swap-log window within its retries, and a live
  deployment holds the tick instead of stopping. A recent-window Base backtest had
  failed with "eth_getLogs: response is not JSON". JSON that answers wrongly is still
  refused at once, and no submission is ever re-sent.
- `almanak test run` stops on a coordinator that is not progressing instead of waiting
  out the decision and teardown clocks. A `strategy_worker_timeout` or
  `transient_read_hold` hold the coordinator has kept for 300 seconds ends the wait as a
  failure of that kind (class `infra`, transient) with the hold's reason, attempts and
  elapsed time; a `decision_timeout` or `teardown_timeout` is named by such a hold when
  the coordinator is still on one. After a lifecycle that settled no decision, teardown
  waits at most 180 seconds and says so, rather than 900: 600 plus 900 seconds outlast a
  20-minute lifecycle budget, and a starved run ended as an anonymous `decision_timeout`
  and then an `interrupt`, with no closure.
- Backtest history collection rides out provider pushback instead of failing the run.
  A price pool's resolution reads, sent one at a time beside the archive batches,
  now pass the provider's gate and wait out a rate limit (HTTP 429), an HTTP 5xx or a
  transport failure as the batches do; a recent-window run on Base had stopped with
  `eth_getBlockByNumber: HTTP 429`. A batch answered with HTTP 200 but no JSON (an
  overloaded provider's HTML page) is sent again within the batch's retries rather
  than failing with "historical batch response is not JSON".
- HyperEVM reads are pinned by block number, and the reader proves each pinned block
  with one probe call (its number, timestamp and parent hash) before reading at it.
  Alchemy runs reads pinned within about 128 blocks of its head at head + 1, and some
  dRPC upstreams ignore block-hash pins entirely, so observations could mix blocks: a
  Morpho deployment stopped with "Morpho observed IRM rate differs from the verified
  revision math". A provider that runs a pinned read elsewhere is now refused with
  `provider_context`. Other networks and local forks are unchanged.
- HyperEVM fee quotes pinned on a 30M-gas large block now read the small-block fee
  market from the pin's parent. A large block's own fee history predicted its 0.1 gwei
  market, and a third of such quotes bid below the small block the transaction lands
  in. Other chains are unchanged.
- `almanak test run` no longer counts a GMX order its keeper cancelled as an entry.
  `entered`, `executed` and `after_entry` now require the connector's result to succeed,
  not only the keeper transaction, so a lifecycle whose only entry was cancelled passes
  as `not_entered` instead of `entered`, and the idle stop that follows an entry no
  longer cuts its decisions short. Each `keepers` row of a mined execution whose receipt
  carries the request's lifecycle event names the `request_status` GMX gave the request and, for a cancellation, its `reason` and
  `reason_bytes` (for example `OrderNotFulfillableAtAcceptablePrice`).
- An interrupted `almanak test run` (Ctrl-C or SIGTERM, such as a harness budget)
  reports what the journal holds at the interrupt. `transactions`, `teardown_executed`,
  `closed` and `evidence_clean` were left as the last decision wrote them, so a run
  interrupted during teardown reported no teardown transactions however many had
  executed. A new `in_flight` lists each decision left unsettled, with its trigger,
  intent kind, allocation and broadcast transaction.
- `almanak ax dex-pools` reports Uniswap V4 pools holding the native coin under a
  wrapped-native filter. The vendor files them under the native coin, so `--token WETH`
  never listed them and `--quote WETH` dropped them; a wrapped-native `--token` now also
  reads the vendor's native-coin list, a page of each in turn within the same five-page
  cap, and a wrapped-native `--quote` keeps a pool whose native leg Uniswap V4 holds as
  that token. The native coin (`slip44`) is accepted as `--token` or `--quote` and asks
  for its wrapped token instead of being refused. A verified native pool's `market`
  names the native coin as its currency, as before. When the vendor has not indexed one
  of the two lists, the other is still read and `source.limitation` says which pools are
  unknown. A row the chain did not verify (unverified, refused or unsupported) also
  reports `unverified_vendor_currencies`, the vendor's claimed currencies with the
  native coin as `slip44`, so a native pool and its wrapped-token twin are told apart
  before verification. Other filters and refusals are unchanged.
- The Uniswap V4 support restriction no longer calls native-coin pools trading-only:
  on each chain with a reviewed native route it states trading and liquidity, one native
  token pair per chain, no ERC-20 V4 liquidity beside it, and the `check` gas warning
  for the larger native mint.
- An idle multi-chain coordinator no longer ticks every second. Once every chain in a
  scheduler cycle has nothing to do, it waits like a single-chain coordinator: until the
  next decision is due, at most 5 seconds, so a teardown is still seen within one cycle.
- The coordinator rewrites the retained dashboard input only when the displayed record
  changes, or every 5 minutes to refresh its capture time, instead of on every tick. The
  record hash leaves out the multi-chain scheduler cursor, so a cursor step no longer
  re-evaluates the dashboard hook.
- A Uniswap V4 swap, mint, fee collection or close whose signed attempt outlives its
  deadline no longer stops the deployment for good, standalone or in a composed
  deployment. Uniswap V4 did not declare its router and PositionManager deadlines, so
  once the provider dropped such an attempt every rebroadcast was refused as
  `stale_plan` and every resume repeated it. It is now released and planned afresh like
  the V3 pool and Aerodrome attempts: the gateway proves the deadline passed at the
  canonical head, with no receipt, no provider copy and an unused nonce, and the
  released attempt stays recorded until an attempt at its nonce is reconciled. A signed
  Uniswap V4 approval, which has no protocol deadline, is likewise rebroadcast after its
  intent expires instead of being refused for the intent's window.
- Composed deployments release an expired signed attempt as its component connector
  would. They kept the rebroadcast-only rule, so a Uniswap V3, PancakeSwap V3, Aerodrome
  Slipstream, Aerodrome or Uniswap V4 action held past its deadline stopped a composed
  coordinator on every resume. Lending and GMX V2 actions declare no deadline and keep
  the rebroadcast-only rule. Composed checkpoints record released attempts in an
  optional `released` field, omitted when empty, so existing checkpoints read and
  serialize unchanged.
- A signed attempt is released as expired only when its calldata enforces the deadline
  and the canonical head is strictly past it. Curve pool calls and a Uniswap V3,
  PancakeSwap V3 or Aerodrome Slipstream fee collection encode no deadline, yet were
  released once the SDK's prepared deadline passed, and a head exactly at the deadline
  counted as expired although the periphery still accepts that block and on Arbitrum,
  Robinhood Chain or BNB a later block can share the head's timestamp. In each case a
  released copy could still execute and stop the deployment at its nonce with an effect
  the journal never recorded. Such Curve attempts and V3-pool collections are now
  rebroadcast past the prepared deadline; it still bounds their preparation and signing.
  Preparing or signing at a head exactly at the deadline is still refused.
- A signed attempt whose bytes never left is released when current validation refuses
  it, whatever its deadline, so a teardown's first send refused by the pre-broadcast
  simulation no longer stops the deployment for good. Signing on Uniswap V3 and V4,
  PancakeSwap V3, Aerodrome Slipstream, Aerodrome and Curve marks the attempt never
  sent (`unsent` on the pending attempt, omitted once cleared), and the gateway
  durably clears the mark in its own commit before the first send. The runtime then
  releases a never-sent attempt on `stale_plan`, `simulation_reverted`,
  `price_movement`, `hook_terms` or `precondition_changed`, retains nothing for it, and
  the next decision plans afresh at the same nonce. Each such release counts against
  the re-plan limit, which now resets only once an attempt is sent or reconciled
  rather than after every signature, so a plan that signs and is refused again stops
  after `REPLAN_ATTEMPTS`. An operator (`ax`) request whose signed step is released
  this way fails with the refusal, saying nothing executed and the request should be
  submitted again, instead of finishing without an outcome. A crash between the
  commit and the send leaves the attempt maybe sent, as are attempts signed by earlier
  versions. Signers, remote and plugin signers included, must only sign and never
  broadcast or keep the bytes for broadcast; the never-sent proof relies on it.
- A maybe-sent Uniswap V3 or V4, PancakeSwap V3, Aerodrome Slipstream, Aerodrome or
  Curve attempt whose calldata enforces no deadline (an approval, a Curve call or a
  V3-pool fee collection) that current validation refuses now stops with
  `signed_attempt_unsendable`, naming its nonce, submitter and the underlying refusal;
  a lagging provider head or a transient read keeps its own refusal and is retried.
  Its bytes may still land once the refusal clears, so the remedy is to restore what
  validation needs (for example fund the gas payer, or wait for the market) and
  resume. Cancelling the nonce instead retires the deployment, which then stops at
  the occupied nonce on every resume until a later release reconciles it, and the
  cancel needs a higher fee than the signed attempt to replace any copy a provider
  holds. In a multi-chain deployment the stop is a chain-local fault retried on each
  pass. Previously it stopped with the underlying refusal. Other connectors are
  unchanged.
- Checkpoints written by this version can hold `released` attempts (Uniswap V4 and
  composed deployments) and never-sent attempts. An earlier SDK refuses such a
  checkpoint when it reads it and does not act on it, so a rollback fails closed.
- A gas payer funded at the funding statement's entry minimum could not finish entry:
  the minimum was one `max_transaction_fee` plus `exit_reserve`, enough for a single
  exposure-increasing transaction, while every entry transaction pays its fee from the
  payer and fee admission requires the fee ceiling plus `exit_reserve` before each one
  that adds exposure. An Aave supply-and-borrow strategy funded at the stated 0.0011 ETH
  paid for its approval and supply, then was refused at the borrow with `gas_budget`
  "payer balance cannot cover execution and exit reserve". The minimum is now
  `max(entry_transactions x max_transaction_fee + exit_reserve, largest
  minimum_native_balance)`, where each connector states an upper bound on the
  transactions one entry of each allocation submits through the last that adds
  exposure (for example Aave V3 approve, supply, enable collateral and borrow where
  granted; a V3 pool trade approve and swap once per `entry_swap_bps` slice of its
  capital, and a liquidity allocation the same slices before its approvals and mint;
  a single-slice Uniswap V4 liquidity entry eight, including the other currency's
  approvals), summed over the payer's accounts and a composed strategy's components.
  Slices a composed trade takes from shared or borrowed inventory beyond its `capital`
  are not counted. `almanak check` warns `gas_entry_slices` when an allocation's
  `entry_swap_bps` splits capital into more than 10 slices, with the fees they add.
  The `almanak-funding-v1` payer entry adds an optional `entry_transactions` and
  production preflight adds `native_entry_transactions`; `entry_minimum_base_units` and
  `native_entry_minimum_base_units` keep their names and now cover the whole entry, so
  most stated minimums rise. Fee admission is unchanged.
- A composed strategy's full repay no longer stays refused with `repayment_funding`
  after the operator sends the shortfall to the fund. It was checked against the
  strategy's retained inventory, which only its own execution changes, so a transfer
  never counted although the refusal said it would resolve it; the repayment coverage
  on the dashboard kept showing the old balance for the same reason. A full repay and
  its coverage now read the fund's measured wallet balance, as non-composed lending
  does, so the retained inventory no longer caps what a full repay consumes. The
  repay's receipt absorbs the top-up it spent into the inventory before debiting it,
  so the ledger stays within the wallet, and the checkpoint records the absorbed
  amount, reported as `repayment_top_ups`, which the gateway verifies like the
  inventory. Partial repays and every other spending remain bounded by the retained
  inventory.
- A node's refusal of a transaction submission read as an unexplained
  `eth_sendRawTransaction: RPC rejected the requested read (JSON-RPC -32000)`, or as a
  bare `HTTP 400` when the provider refused it at the HTTP level. It now reads `RPC
  refused the transaction submission` (`did not accept` for a throttle code) and names
  the standard transaction-pool refusals (nonce too low or too high, already known,
  replacement or transaction underpriced, intrinsic gas too low, insufficient funds, max
  fee below the block base fee, block gas limit, fee cap, full pool, invalid sender or
  chain id) by a fixed phrase, in go-ethereum wording as geth, reth and anvil report it
  and in erigon's and Nethermind's own wording. Provider text is still never echoed, and
  reads keep their message.
- A second `almanak shutdown` (with or without `--force`) on a workspace that is already
  shut down now prints the recorded status and exits 0 instead of an httpx traceback; the
  first shutdown left the stopped supervisor's control URL in `workspace.json`, and the
  CLI posted to it. Shutdown now withdraws that URL, and a repeated shutdown never sends
  the control token to a URL an older supervisor left behind. A workspace a teardown has
  relaunched still receives the shutdown while its supervisor holds the workspace lock;
  a repeated shutdown waits up to 2 seconds for the supervisor that shut down to release
  that lock, and a starting supervisor retries the lock for up to 1 second.
  A later `pause` is refused as `invalid_input` ("workspace is shut down and has no ready
  supervisor"). `almanak run` ends with the recorded status, exit 0 (130 after Ctrl-C),
  when another command shuts the workspace down while it follows. A `pause` or `shutdown`
  whose supervisor cannot be reached or answers with an HTTP error status, and a `resume`
  or `teardown` control that fails after a successful relaunch, print one
  `supervisor_unavailable` error document with the `workspace`, the `action` and, for an
  HTTP error, its `status_code`, and exit 1. A relaunch that fails still reports
  `runtime`, and refusals the supervisor reports in its answer are unchanged.
- The Uniswap V4 `entry_swap_bps` description said each entry swaps that share of
  capital and repeats. That holds for trading; for liquidity the share sizes the entry
  quote, which swaps only the part the mint pairs with and is offered only while no
  position is held, while `open_liquidity` mints with all inventory held. With the usual
  single entry swap, values below 10000 leave about the rest of capital idle (on the V3
  pools 5000 swaps half and invests about all); banded allocations that recentre through
  `rebalance_quote` and strategies that swap repeatedly before minting are not limited
  this way. The schema and configuration docs now say so, and `almanak check` warns
  `entry_budget_partial` with the idle amount for an unbanded V4 liquidity allocation
  below 10000. No configuration field or execution behaviour changes; a check that
  carries the new warning is no longer clean.
- Aave V3 and SparkLend allocations with `collateral: false` and no `borrow` on a reserve
  whose LTV is above zero are refused when a new run's parameters are prepared
  (`almanak check --online`, and fresh production or fork preparation), naming the
  allocation's `collateral` field and the remedy (set `collateral: true`). On the supported
  ordinary-mode reserves the Pool enables a first supply into an empty position as
  collateral automatically, so that allocation could never supply and previously failed
  only in the fork lifecycle with `collateral_authority`. Resuming or exiting a deployment
  from retained parameters and backtest parameter construction do not apply this check.
  It replays the same `first_supply_enables_collateral` predicate that execution, receipts
  and backtests use; the position schemas now describe `collateral`, and the execution
  refusal names the remedy too.
- The Uniswap V4 support declaration no longer says LP ranges are fixed: allocations
  may use a recentring `band`, which backtests in the economic model only.
- Reference strategies that decided every 5 seconds now decide every 60 seconds, so a
  copied reference no longer draws the `decision_interval` warning (minimum 30 seconds);
  references with longer intervals are unchanged.
- A market fee quote above a per-transaction ceiling of the gas policy no longer stops
  a single-chain coordinator, which then stayed stopped after gas fell until someone
  resumed it. The ceilings are `max_gas_price_wei` (refusal `gas_price`) and
  `max_transaction_fee` for the quoted maximum fee including OP Stack L1 data and
  operator fees. That fee refusal is now its own kind, `gas_fee_quote`, where it was
  `gas_budget`, and a replacement family blocked on it records that kind. Preparation
  holds instead: it signs nothing, keeps the decision, quotes again with the
  transient-read back-off (5 seconds doubling to a minute) and proceeds once the quote
  is within the ceilings. The hold has no time limit, in running and teardown alike,
  because the ceilings are the author's spending choice and a gas spike can outlast the
  30-minute transient-read horizon. Market refusals of the retained decision at
  preparation between held attempts no longer add up to the re-plan limit; a count that
  includes a refusal at signing is kept, so those still stop after five. When the fixed
  OP Stack operator fees alone exceed `max_transaction_fee`, no market change can admit
  the transaction and the refusal stays `gas_budget`, which stops. A ceiling set below
  the chain's floor gas price still holds without end; status and the log show it. While it holds, status stays
  `running` with a diagnostic naming the kind and the policy field, and the
  coordinator log has one `gas_ceiling_hold` line per attempt with the quote, then
  `gas_ceiling_recovered` once a plan is admitted or `gas_ceiling_dropped` when the
  decision is dropped. `almanak test run` stops at once on such a hold with its kind
  and quote, since fork gas need not fall. Gas limit, payer balance, exit reserve and
  lifetime `total_budget` refusals still stop, as does a ceiling refusal at signing or
  submission, and an operator request (`ax`) is still refused at once. Multi-chain
  deployments already retried the chain on each pass with the refusal retained as the
  chain's fault; they log no hold events and their status has no hold diagnostic.

## [3.0.1] - 2026-10-27

## [3.0.0] - 2026-10-15

### Added
- First release of the v3 SDK: `strategy.py` plus one configuration file, checked,
  run in explicit fork or production mode and backtested against explicit models.
- Aave V4 lending, Uniswap V4 trading and fixed-range liquidity references on
  Ethereum and Avalanche, plus the rollout connectors declared by `almanak support`.
- Uniswap V4 trading allocations on pools holding the chain's native coin: ETH on
  Ethereum, Base, Arbitrum, Optimism and Robinhood, POL on Polygon, BNB on BNB Chain and
  AVAX on Avalanche, hookless or with a reviewed hook (such as Fables WETH/USDG). The fund
  holds the wrapped native (WETH, WPOL, WBNB, WAVAX) and never sends native value: the
  reviewed Universal Router unwraps the input or wraps the native output within the same
  call, and receipts read each chain's wrap evidence. Capital names the wrapped token.
  Native/wrapped-native pools, LP on native pools and chains without a reviewed router
  route are refused, and a strategy cannot combine a native and an ERC-20 Uniswap V4 pool
  on one chain.
- `strategy.gas` defaults the fields that tune how execution bids, so a policy needs
  only `accounts`, `chain`, `max_transaction_fee` and `exit_reserve` (and
  `total_budget` for a lifetime cap); the amounts never default.
  `max_gas_price_wei` defaults to `max_transaction_fee` in wei over `max_gas_limit`,
  the highest price the fee ceiling admits, so omitted ceilings can no longer
  contradict the fee the author chose. `source`, `reward_percentile` (50),
  `base_fee_basis` (`observed_block` on Avalanche), `base_fee_multiplier_bps` (20000;
  12500 on Arbitrum and Robinhood Chain), `max_gas_limit` (1200000) and
  `gas_limit_buffer_bps` (2500) default as listed in the configuration guide. An
  Avalanche file that omitted `base_fee_basis` now gets `observed_block`, the only
  basis its fee history supports, which changes that file's digest.
  `almanak check` reports every applied default under `gas_defaults`. Existing files
  that state every field are unchanged, digest included.
- Strategy price series for technical analysis, read from on-chain pool state. A
  strategy declares a series in `data_dependencies` with a strategy-purpose
  `PriceBinding`: one Uniswap V3, PancakeSwap V3, Aerodrome Slipstream, Uniswap V4,
  Aerodrome classic (volatile or stable) or Curve (StableSwap NG, TwoCrypto NG,
  TriCrypto NG) pool, a `1m` to `1d` timeframe and a `window`. `PoolPrice` delivers one
  close per candle, the pool's price at the last block at or before each boundary, so
  quiet pools leave no gaps: the V3/V4 pool price, the Aerodrome reserves' spot price,
  the StableSwap invariant's marginal price or a Curve crypto pool's own `last_prices`.
  `PoolSwapCandles` adds open, high, low and quote-token volume built from the pool's
  trade logs: Swap events' resulting prices, Aerodrome `Sync` events' spot prices, or
  Curve `TokenExchange` execution prices of the declared pair, fees included. Prices are
  the declared asset in the pool's other coin, or in the `quote` coin a Curve pool of
  more than two coins must name, decimal-adjusted; the connector's pool resolution
  verifies the pool and that both are its coins. Every decision receives the points
  available by its cutoff in `ctx.prices` (`ctx.price(asset)`). Every economic backtest
  connector, composed strategies and `historical_path` for Aave V4 and Uniswap V4 read
  each series through the operator's archive provider from `window` points before the
  start, with compact evidence that rebuilds every point; fork and live gateways read it
  from their chain before each decision, as of the earlier of the decision and block
  time, reading only boundaries they have not seen, and retain what they served in
  `strategy-prices.sqlite`. No data service or key is added. A backtest file never
  authors a strategy series; Aave V4 economic single strategy prices (coin price or
  Spoke oracle, delivered as `PriceObservation`) are unchanged. A failed read holds that
  live decision, teardown reads no series, provisioning refuses a series on a chain the
  deployment cannot read, and the gateway refuses to start on a pool that does not
  resolve or lacks the asset. The SDK computes no indicator: `almanak[ta]` installs
  numpy and TA-Lib, and the worker runs numeric libraries single-threaded. Four Base
  references use it: `uniswap_v3_base_rsi` (RSI crossings), `uniswap_v3_base_rsi_lp`
  (RSI-gated banded liquidity), `aave_v3_base_sma_borrow` (a loan held only above a
  moving average) and `uniswap_v3_base_atr_breakout` (full candles: channel entry, ATR
  trailing stop).
- `almanak test run --through-exit` keeps deciding after the first entry, up to
  `--decisions`, so the strategy's own later intents execute on the fork before
  teardown: an LP mint after its entry swap, collection, recentring and its own
  exit, stopping early once the entered strategy holds for `--idle` consecutive
  decisions (default 3). Without it the lifecycle still stops at the first entry. The verdict adds
  `executed`, the intent kinds that ran, and `after_entry`, those that ran after
  the entry.
- `almanak test run` settles GMX orders, deposits and withdrawals on the fork. A
  fork runs no GMX keeper, so a GMX strategy's first request never executed and
  the lifecycle stopped at it. The run now replays a real keeper: an execution
  mined on the origin chain after the request, by the same keeper, with its
  unchanged signed Chainlink Data Streams reports, submitted to the fork's handler
  at the reports' time. No price, signature, provider or role is synthesized; a
  request without an eligible report stops the run with that reason. The verdict
  lists each settlement under `keepers`. `almanak test config` anchors chains with
  a GMX market at least three hours behind the head so those reports exist, at a start
  where the market's keeper publications are dense, and the run lengthens advances
  when needed so each decision lands shortly before a publication. Report sources
  are the order, deposit and withdrawal handlers of the reviewed deployment record. The report source
  the QA suite used moved from `tests/` to `almanak.connectors.gmx_v2`.
- Uniswap V4 liquidity allocations, fixed-range and banded, on pools with a
  reviewed Fables or StablePairHook hook. Neither hook can block a withdrawal: their
  address bits encode no remove-liquidity callback, and a profile that did could not
  admit LP. LP fees are the hook's override fee as the PoolManager credits it, read
  from fee growth like any V4 position. A changed hook refuses new mints while
  collect, burn and teardown run. Pons pools stay trading only, since they pay LPs
  no fee. Measured Safe/Roles gas references cover hooked mint, collect and burn on
  Ethereum and Robinhood.
- `almanak ax resolve` names ERC-20 token candidates for a symbol or address and
  `almanak ax dex-pools` lists the pools that trade a token. CoinGecko on-chain
  names the candidates with its liquidity and volume; each checked token is read on
  chain and each checked pool is resolved by its connector, which refuses pools from
  unreviewed deployments. Verified pools carry a paste-ready `markets:` entry. Both
  need `COINGECKO_API_KEY` (pro) in the env file of the caller or the read gateway,
  which serves them as a `lookup` operation under the existing CoinGecko rate cap.
  `dex-pools` pages through the vendor's order with `--limit` (default 10) and
  `--offset`, reporting `matched` and `has_more`; an incomplete vendor list says so in
  `source.limitation`. `resolve` reports `source.complete` the same way and
  recommends nothing when the vendor has not indexed a lookup. A Uniswap V4 pool
  is admitted by the connector's own resolution: a hooked pool is verified only with
  a reviewed hook profile whose code matches its pin, and reports a `hook` block with
  the profile, address, pin and current terms; any other hook is refused with the
  connector's reason. Vendor pools labelled `pons-v2-dex` are tried as Uniswap V4
  pools. A pool holding the native coin keeps it in its PoolKey and `markets:` entry and is
  verified with the fund holding its wrapped token, and a native
  `--token` or `--quote` is refused in favour of the wrapped token. A pool whose
  resolved currencies do not trade the requested `--token`/`--quote` is refused with
  `vendor_mismatch`; `--connector` keeps pools matched by their reviewed factory.
- `almanak ax lending-markets` lists Aave V3, SparkLend, Aave V4, Compound V3 and
  Morpho Blue markets on a chain from each protocol's own records: the Pool's reserve
  list, the reserves of the Spokes the Aave V4 Hubs serve and the Comets the
  chain's Configurator deployed. Morpho Blue markets, one loan asset at a time and
  most supplied first, come from Morpho's public API where it indexes the chain,
  only Morpho-listed markets unless `--include-unlisted`, with Morpho's warnings, and from the core's `CreateMarket`
  events on BSC and Avalanche, which carry no curation: there every supplied market is
  listed, `source.listed_only` is false and `source.curation` null, and
  `--include-unlisted` changes nothing. Each reported market is resolved as `lending-reserve` resolves it
  and is `verified` with its `config` block or `refused` with the reason; Aave V3,
  SparkLend and Aave V4 rows also carry the reserve's frozen, paused and borrowing
  flags (and loan-to-value for V3 and SparkLend), and every asset carries its
  on-chain symbol. A Morpho market whose own loan or collateral asset contradicts
  the API row is refused with `vendor_mismatch`, and an Aave V4 Spoke whose reserve
  count cannot be read (anything but the revert a Spoke without reserves answers) is
  named in `source.unreadable` rather than counted as empty, as are a Hub that cannot
  be enumerated and a reserve that cannot be read; the rest still list. No key
  is needed; the read gateway serves the Morpho lookup under its own rate cap
  (`ALMANAK_GATEWAY_MORPHO_REQUESTS_PER_MINUTE`).
- `ax pool` takes `--connector aerodrome` and `--connector curve` and reports the
  connector's resolution of the pool against its reviewed factory or registry, with
  the pool's prices: an Aerodrome spot `price` from its reserves, Curve StableSwap
  spot `prices` from the invariant, and Curve crypto `last_spot` (the spot price
  after the pool's last operation) and `oracle` prices. Every `ax pool` price, the V3-style one included, now names its `kind`.
  `dex-pools` verifies those pools too and matches a venue label it does not know
  by the pool's own `factory()`.
- `ax perp-markets` takes `--chain` and `--query` inline, and `ax pool-price` takes a
  pool key inline (`--chain`, `--currency0`, `--currency1`, `--fee`,
  `--tick-spacing`, optional `--base` and `--hooks` for a reviewed hook) on the
  reviewed deployment.
- Uniswap V4 admits pools with a reviewed hook for trading allocations: the ten
  Fables hooks and three PonsV2MemeHook instances on Robinhood and Uniswap's
  StablePairHook proxies on Ethereum and Arbitrum. The registry is in code, keyed
  by chain and hook address, and pins each hook's runtime code (and a proxy's
  implementation), its permission bits and its fee mode; a market sets `fee` to
  `8388608` (the dynamic-fee flag) for Fables and StablePairHook. Every other hook
  is still refused. Fork and live quotes come from the V4 Quoter, which runs the
  hook; economic backtests read the hook's fee view or Pons launch rates at each
  sample and apply the override fee or output haircut; receipts reconcile the
  Pons take. A reviewed hook whose code later changes refuses new entries
  (`hook_changed`) and still lets exits and teardown run with a fresh quote and
  strict minimum output. An entry on a Pons pool whose terms could not be read is
  refused (`hook_terms`) and re-planned like a reverted simulation; only a
  throttled read fails the snapshot. `almanak ax pool-price` reports a hooked pool's profile,
  pin status and terms.
- Economic Uniswap V3, PancakeSwap V3 and Aerodrome Slipstream backtests report their
  result in the funding asset. The report gains an equity curve, the pool price series,
  total return, maximum drawdown and Sharpe ratio, and a `performance` block: fees
  earned, swap cost and volume, the remaining price effect, time in market and in range,
  holding benchmarks and one row per position with its range, deposits, withdrawals and
  fees. Prices come only from the run's own prepared pool samples. Balances the pools do
  not price, such as gas, stay in asset units.
- Market resolution reads each ERC-20 asset's `symbol()` and names the asset with
  it in `asset_metadata` unless the author declares a symbol. The symbol is display
  text outside the verified facts.
- `almanak report --format html` leads a valued run with its result and lists its
  liquidity positions.
- `almanak test config` writes a fork operator file from the author configuration:
  one Alchemy provider per authored chain, placeholder account bindings, the latest
  block (or `--block`) as anchor, fixture funds sized from an online fork check's
  `funding` block plus 1%, and `fork.gas` ceilings equal to `strategy.gas`. It
  replaces an existing `--output` only with `--force`.
- `almanak test run` runs one supervised fork lifecycle through the workspace
  `run --fork` creates: admission, up to `--decisions` decisions with fork time
  advanced by the decision interval (or `--advance`, raised to the next due decision
  when shorter), teardown and shutdown, releasing the run's fund claims. It prints
  and retains a JSON verdict, `test-verdict.json`, and exits 0 only when the
  verdict `passed`: admission passed, teardown closed, `evidence_clean` (no pending
  or reserved attempt, undelivered outcome, accounting issue, integrity error or
  chain fault) and no failures.
- `almanak check` reports advisory `warnings` with a `code`: `gas_limit` when a
  `max_gas_limit` is below the largest measured Safe/Roles gas of the policy's
  connectors (shipped as reviewed fork receipts in `gas_references.json`),
  and `gas_amount_units` when a gas amount exceeds 10 whole native units. Warnings
  never fail a check.
- `almanak check` warns about gas policies that cannot work: `gas_fee_ceiling`
  when `max_transaction_fee` is below `max_gas_limit` x `max_gas_price_wei`,
  `gas_budget` when `total_budget` cannot pay one fee per entry transaction plus
  `exit_reserve`, `gas_exit_reserve` when the reserve cannot pay one fee per exit
  transaction and, online, `gas_price` and `gas_fee_quote` when the price ceiling
  or fee ceiling is below what the policy quotes now.
- A fork operator file may set `fork.author_gas: unenforced` on Base and Optimism:
  the fork runs gasless and the authored gas policy is retained but not enforced.
  Preparation retains that choice as a `fork_gas_unenforced` record, which admission
  and the prepared configuration (`unenforced_gas`) require. Without it those forks
  still refuse authored ceilings.
- `almanak check` warns with code `decision_interval` when a production or mode-agnostic
  check finds a `strategy.decision_interval` shorter than 30 seconds: every live
  decision starts a fresh sandboxed worker, and a shorter interval can starve a
  deployment of CPU. Fork and backtest checks do not report it.
- `almanak check --fork|--production --online --json` reports a `funding` block
  (`almanak-funding-v1`, schema in `docs/contracts/funding-v1.schema.json`): per bound
  account the CAIP-19 assets and base units it must hold before its first entry, and per
  gas payer the native entry minimum and gas policy amounts. It holds requirements only,
  no observations. A refused operator policy or fixture stage keeps it in the failure
  detail. Backtest checks omit it.
- `almanak ax pool --chain <CAIP-2> --pool <address>` names the pool inline, as an
  alternative to a `--config` request file; both forms validate as the same
  request.
- The wheel carries a read-only copy of every reference strategy's author files
  (`strategy.py`, `config.yaml`, backtest YAML files and README) from
  `strategies/`. `almanak support --references` lists them with their connector
  and chain markets and installed path, filtered by `--connector` and `--chain`.
- Gateway request log lines carry `duration_ms`, the time from receipt to the
  encoded reply, including any wait for the deployment's checkpoint lock.
- `almanak ax pool --connector uniswap_v3|pancakeswap_v3|aerodrome_slipstream`
  reads a V3-style pool from a `{chain, pool}` request: the connector's resolver
  verifies the pool against its reviewed factory, and the result reports the
  declared support level, resolved identities, pinned state and spot price.
  `--depth-bps` adds the output available on each side before the price moves
  by that band, from every initialized tick inside it, fees excluded.
- `almanak service read-gateway` serves a read-only observation gateway: it holds the
  provider credentials from `--env-file` in its own process and answers only whitelisted
  JSON-RPC reads over authenticated loopback HTTP. Every `almanak ax` command takes
  exactly one transport, `--env-file` or `--gateway-url` with `--gateway-auth-file`, so
  a session that cannot hold a provider key can still read.
- `almanak ax perp-markets --connector gmx_v2` resolves a GMX V2 market by GM
  share address or label (for example `BTC/USD`): it enumerates the venue's own
  DataStore market list at the pinned block and returns the verified resolved
  market record, with no hosted-API dependency.
- Remote signing seam: `--signer-file` on `run`, `check` and `service gateway` binds a
  signer plugin from the `almanak.signers` entry-point group; every returned envelope
  is verified against the request and the bound submitter before it is used.
- `almanak support --schema strategy|backtest` prints the bundled JSON schema set of the
  installed version.
- The coordinator retains the projected dashboard input (`dashboard/input.json`) after
  every cycle for a host to display; `almanak support --schema dashboard` prints the
  schema of the dashboard response a host renders.

### Changed
- The distribution, console script and import module are named `almanak`.
- Generated Safe permissions carry scope only (blueprint 27): the exact contract,
  function, asset, pool or market, the Safe as recipient or beneficiary, exact
  approval spenders, nonzero minimum outputs, deadlines and sentinels. No grant
  carries a cumulative allowance or an amount bound sized from an author field, and
  liquidity mint ticks are enforced by the gateway rather than on-chain. The
  budget-only author fields are removed (`maximum0`/`maximum1`, Curve `maximums`,
  the GMX maxima, lending `supply_limit`/`borrow_limit`/`entry_amount`, Aave V4
  `amount`); lending allocations take `capital` and `borrow: true`. An exchange
  allocation is offered an entry quote whenever it holds funding inventory, one
  `capital × entry_swap_bps` slice at a time, so a strategy that should enter once
  checks `entry_complete`. `strategy.gas.total_budget` is optional unless the
  operator's ceiling states one. Installed permissions change for every Safe.
- `ax` reads through the read gateway named by `ALMANAK_AX_GATEWAY_URL` and
  `ALMANAK_AX_GATEWAY_AUTH_FILE` when neither `--env-file` nor `--gateway-url` is
  given, and `--block` defaults to `finalized`. Transport flags still take precedence.
- An `ax` or read gateway observation read the provider throttles (HTTP 429, or
  JSON-RPC 429 or -32005) is re-sent within the bounds batched reads already used:
  at most six times, after the provider's `Retry-After` or a bounded backoff, each
  attempt admitted under the gateway's rate cap again, and no wait ending more than
  30 seconds after the first attempt. Concurrent `ax lending-markets` reads no longer drop an Aave V4
  Hub or fail a Morpho listing on one throttle. Execution, fork fixture and
  backtest preparation reads still fail on a throttle.
- `almanak service read-gateway` refuses to start when a provider rate cap
  variable is invalid, rather than refusing each read or lookup it would govern as
  an invalid request.
- Fork fixtures fund ERC-20s outside the reviewed list instead of refusing them.
  The wrapped native asset is still funded only by wrapping. Every other ERC-20
  tries, in order, a reviewed token funder, a transfer from a listed holder, the
  balance slots and seed recipe ported from SDK 2.x, `anvil_dealERC20`, then,
  without a seed recipe, probing of common balance slots; a token with a reviewed funder or a listed
  holder never falls through. Each route must leave the exact balance and pass a
  reverted transfer check, storage routes are refused when `totalSupply` is below
  the amount, and a failed route is rolled back before the next. When no route
  passes the fixture refuses and names each outcome. A token address with no code
  is refused, and an `anvil_dealERC20` still running after 20 s stops funding and
  asks for a new fork. `simulated_initial_capital` evidence records the route in
  `method` with its `slot`, `seed` or `holder` (see `docs/operating.md`); the
  plan's `local_storage` method now means any ERC-20 route. `almanak check --fork`
  accepts any ERC-20 fixture asset. BTC.b on Avalanche, USD₮0 on Arbitrum and
  wstETH on Ethereum were refused before.
- A V3-family economic report names each pool's continuity proof by its dataset
  digest, block interval and event count instead of embedding it. On a 14-day
  Robinhood window the embedded proof was 8.3 MB of a 10.4 MB report, and the HTML
  report, which also repeated the finished report, was 25 MB.
- Preparing Uniswap V3, PancakeSwap V3 and Aerodrome Slipstream pool samples no
  longer pays one provider round trip per read or reads every initialized tick at
  every sample. Collection resolves the block anchors of the samples ahead one
  search level at a time, asks for the reads the previous sample repeated in bulk,
  and reads liquidity log windows in batches. A sample reads only the ticks that
  can have changed since the one before (new, named by a Mint or Burn, crossed by
  the price, and the nearest ticks outward until one has unchanged fee growth) and
  carries the others. The first sample of each range, every 256th carried sample
  and the last sample read every tick and refuse a difference. Sample payloads are
  unchanged: on Uniswap V3 (Robinhood, Ethereum, Arbitrum, Base), PancakeSwap V3
  (BNB) and Slipstream (Base) windows the new collection produced the same payloads
  as reading every tick. A 14-day window at 5-minute samples on a Robinhood pool
  (4,033 samples, about 360 ticks each) prepared in 6 minutes 23 seconds with
  230,801 provider reads. A 1-day window took 32 seconds and 15,657 reads against
  251 seconds and 120,763 reads before.
- Every V3 target now retains the pool's Mint and Burn events, trading targets
  included, so `max_liquidity_log_requests` and `max_liquidity_events` bound trading
  backtests too. A window that needs more log requests is refused with the number
  it needs.
- The historical gateway accepts up to 1,024 reads in one batch and overlaps its
  provider batches within `collection.max_in_flight`. Each provider batch retries
  on its own. A provider rate limit is waited out up to eight times apart from
  `max_retries`, holds every later request and halves the overlap, which recovers
  one batch at a time. Batches of a provider with an operator rate cap stay
  sequential.
- A prepared dataset hashes each retained evidence body once per writer and once
  more at publication, instead of once for every record that names it, and a
  collection pass serializes its continuity proof once. Publishing and opening the
  14-day dataset above took 3 and 10 seconds.
- The preloaded strategy worker also imports the configuration types the strategy
  helpers dispatch on, so a forked evaluation no longer imports them itself. In a
  prepared Uniswap V3 band backtest one decision fell from about 136 ms to about
  30 ms.
- The timestamp-to-block search reads its next four levels of headers in one
  batch when the reader can prefetch, so each search takes about a quarter of the
  provider round trips. It reads and retains the same headers as before.
- The dashboard renderer takes every colour and font from CSS custom
  properties, and `AlmanakDashboard.mount` accepts `{theme}` to set them, so a
  host page can render the dashboard in its own palette and typefaces with the
  stylesheet and its CSP hash unchanged. `AlmanakDashboard.themeTokens` lists the
  accepted names. Without a theme the dashboard keeps its current colours and fonts.
- The dashboard no longer draws its own `ALMANAK / STRATEGY OBSERVATORY` header
  bar; the page or report that embeds it provides the frame.
- `almanak check` runs its strategy, capability and data dependency hooks in one
  preloaded sandbox worker, each in a fresh forked child as backtests already do,
  instead of starting a sandboxed interpreter per hook. An online check starts that
  worker while its read gateway starts. In a 3.0.0rc43 container with a Uniswap V3
  Robinhood fork check, the median wall time fell from 6.6s to 4.1s with 2 CPUs and from
  6.9s to 4.7s with 1 CPU. A sandbox that cannot start now fails the `sandbox` stage,
  with the wrapper's own error, instead of the `strategy` stage. Permission generation
  for an installed Safe also reads its Safe/Roles wiring once instead of twice.
- The read-only funding preflight that production admission runs before a
  production start requires each gas payer to hold the native entry minimum
  `max(max_transaction_fee + exit_reserve, max minimum_native_balance)` over the
  payer's accounts, instead of the fee reserve alone. It reports the value as
  `native_entry_minimum_base_units` beside `native_admission_reserve_base_units`.
  An allocation threshold above the fee reserve was previously caught only at
  the first entry.
- Every connector computes its entry token requirements with
  `Execution.requirements(config, account)` from parameters alone, and preflight
  compares them with measured balances. Preflight rows are one per asset (Compound
  V3 and Morpho Blue previously emitted one per observation and under-counted two
  allocations on one asset), every row states its `requirement` basis, and a
  missing balance reports `observed_token_base_units: null` and `funded: false`.
- Uniswap V3, PancakeSwap V3 and Aerodrome Slipstream pool resolution names the
  factory a refused pool reports alongside the reviewed deployment factory.
- Python imports use `almanak`, including the public author API, sandbox workers and
  bundled schemas.

### Fixed
- The gateway no longer refuses a stored plan as differing from its own preparation
  because the node's gas-price suggestion moved between `plan` and `update`, e.g. when
  a fork's teardown mined a block. Without a gas policy, preparation now prices a
  legacy transaction from the pinned block's fee history, 12.5% above the higher of
  its and the next block's base fee, instead of the node's current `eth_gasPrice`;
  every fork mines one empty local block at start, so the first plan reads this
  node's base fee rather than the upstream one. On BNB, whose fee history reports
  no tips, plan revalidation reuses the quote's retained node gas price, as signing
  and replacement already did, and the policy still bounds the resulting price.
- A live deployment no longer stops in error when its provider briefly fails a read.
  A load-balanced provider could serve a just-mined receipt and
  then answer its block with null or "header not found", or throttle an execution read
  with HTTP 429; the gateway refused the request and the coordinator exited with a
  successful transaction unreconciled. Execution, receipt and authority readers now
  re-send an idempotent read after a throttle (HTTP 429, JSON-RPC 429 or -32005), a
  transport failure, an HTTP 5xx, an empty block read or a node's unknown-block error,
  within an 8 s budget per read and a 40 s read deadline per gateway request; observation
  readers keep their 30 s budget and now cover the same failures. Local fork readers fail
  at once, as before. A failure that outlasts the budget and a client timeout on a
  read-only gateway request are marked transient: the coordinator holds that tick and
  tries again with the worker-timeout back-off, logging `transient_read_hold` and
  `transient_read_recovered`.
  A running deployment stops after 30 minutes of held ticks; teardown never stops, but
  logs `transient_read_hold_escalated` once past that horizon. A held tick commits
  nothing, so a pending transaction is only looked up again and never replaced by a new
  economic action. Submissions, refusals and every other failure keep their behaviour.
- A fork lifecycle over a contract a local Anvil fork cannot execute is refused before
  funding or any decision. Base's B20 stock tokens (`0xb2…` addresses)
  answer `eth_getCode` with `0xef`, code the Base node implements natively; mainnet
  reads and resolution pass, but every fork call failed with `EVM error OpcodeNotFound`
  and stopped the run at its first decision. Every market token, verified market
  contract and funded token is read in one batch per chain. `almanak check --fork
  --online` fails its `fork_compatibility` stage with code `unsupported_on_fork`. Fork
  setup checks the fork nodes before funding, on every launch including the reuse of
  an already funded fixture, and stops `failed` with the non-retryable failure
  `unsupported_on_fork`: status offers `shutdown` rather than `resume`, `almanak run`
  exits 1 and `test run` reports the kind as the admission refusal's `error`. The
  refusal names every such contract and states that this is a fork simulation
  limitation, not a strategy fault. EIP-7702 delegated accounts and empty accounts are
  unaffected.
- Every refusal and failure in the `almanak test run` verdict now carries a structured
  `kind` from its source, a `class` (`infra` or `strategy`) and `transient`, and the
  verdict a top-level `failure_class`, so a caller decides whether a re-run can help
  without reading error text. A stopped coordinator's kind is the error kind of the
  refusal it logged. A refused command's error document now carries
  `"transient": true` when its source marked the failure transient and
  `"reverted": true` for a contract revert, which keeps the `rpc` kind and is a
  `strategy` failure; gateway refusals carry the same `reverted` mark. The fields are
  additive.
- A node's JSON-RPC rejection that names a known failure now adds a fixed phrase
  beside the code, such as `(JSON-RPC -32603: EVM error OpcodeNotFound)` or
  `header not found`. The provider's own text is still never echoed: an unrecognised
  message, and every revert, reports the code alone.
- The economic exchange report no longer reads a pool before an interval-aligned
  window start whose first block precedes it by a block interval.
- A composed strategy can cancel its pending GMX request in teardown. The cancellation
  was prepared through GMX's own component configuration, so the gateway refused it
  because the permission manifest belongs to the composed configuration, and the
  deployment stayed in teardown with funds in GMX escrow. GMX now only reads its
  request and snapshot through its view; the composed backend prepares the plan.
- Uniswap V3, PancakeSwap V3 and Aerodrome Slipstream liquidity additions no longer
  revert on the position manager's `Price slippage check` when the pool moves a few
  ticks between planning and inclusion. The amount minimums were
  `slippage_bps` below the amounts at the observed price, and in a narrow band each
  tick shifts the token mix by about 1%, so a ±200-tick band at 100 bps tolerated
  less than one tick. Minimums and `minimum_liquidity` are now what a mint takes at
  the edges of the price window the price-movement gate already admits,
  P·(1 ± `slippage_bps`), so any price that passes the gate also passes the mint.
  The non-binding token's minimum is correspondingly lower; a strategy wanting
  tighter protection lowers `slippage_bps`, which also tightens the gate. Plans and
  recorded checkpoints made by earlier releases replan to different minimums.
- A mined Safe/Roles transaction that reverts records why. The receipt holds only
  `ModuleTransactionFailed()`, so the gateway replays the inner call at the parent
  block with `shouldRevert` cleared and keeps the decoded reason as the outcome's
  `revert_reason`. The coordinator's `transaction reverted` and `execution_failed`
  stops name it, and so the deployment's diagnostic does too. A failure that
  depended on an earlier transaction in the same block is reported as not
  reproduced.
- A coordinator or operator request stopped by recorded accounting issues names the
  first issue instead of reporting unexplained wallet movements, which it also
  said when every movement was explained, e.g. after a Uniswap V4 hook's code
  changed. Approvals and revokes on a hooked pool no longer carry that hook's
  changed-code note, since they never call it.
- A stopped coordinator now reports why. Exceeding a non-refilling spending quota
  (Uniswap V3 and V4, PancakeSwap V3, Aerodrome, Aerodrome Slipstream and Curve) is a
  `quota` refusal naming the asset, the requested amount, what was already spent, the
  quota and what remains, instead of a generic `invalid_request: gateway request
  refused` whose reason only reached `logs/gateway.log`. The `gas_limit`, `gas_price`
  and `gas_budget` refusals name the buffered gas estimate and `max_gas_limit`, the
  market quote and `max_gas_price_wei`, the transaction's maximum cost and
  `max_transaction_fee`, the spent, fee and `exit_reserve` against `total_budget` or the
  payer's balance, and the entry minimum against the payer's balance. The supervisor's
  `failed` diagnostic is the refusal the coordinator printed in this run, and offline
  `almanak status` keeps returning it after the supervisor exits; without one the
  generic instruction to inspect the log remains.
- A pre-signing simulation that reverts no longer stops a deployment with an unexplained
  `rpc` refusal. Deployments on PancakeSwap V3 on BSC and on Robinhood stopped at
  signing with "eth_call: RPC rejected the requested read (JSON-RPC 3)": a narrow-range
  mint reverted "Price slippage check" after the pool moved about 3 ticks between
  preparation and signing. The gateway now refuses a reverting simulation as
  `simulation_reverted` with the decoded reason (`Error(string)`, `Panic(uint256)` or
  the custom error selector; behind a Safe it reads the inner protocol reason that Roles
  replaces with `ModuleTransactionFailed()`), and names it in its request log. Provider
  error text is still never kept. The coordinator treats `simulation_reverted` and
  `price_movement` from preparation or signing like an expired plan: nothing was signed,
  so it logs an `unsigned_plan_refused` event, discards the plan and decides afresh,
  backing off 15 seconds more each time. The fifth consecutive refusal on a chain stops
  the coordinator with the reason.
- Production signing and broadcast on OP Stack chains (Base, Optimism) no longer
  fail with `block_mismatch` whenever the head has moved since the plan's fee
  quote. Revalidation pins a fresh head and then reads the OP fee parameters at
  the older quote block; that read left the reader pinned at the quote block, so
  the Compound V3, Morpho Blue and other revision checks that follow refused the
  head. The fee parameter read now restores the caller's pinned block. Forks did
  not show it because their head does not advance on its own.
- A deployment no longer stops for good when one decision or outcome evaluation runs
  past its 20-second wall budget. A worker within its 20 CPU seconds can still miss that
  deadline on a CPU-starved host: one deployment stopped after 15 hours when its host
  ran at 80 to 90 percent of its available CPU, and another, deciding every 5 seconds on
  BSC, stopped before its exit ran. The coordinator now logs a degraded
  `strategy_worker_timeout` event and evaluates the same due decision again after
  backing off for 5 seconds, doubling to at most 60; nothing an evaluation produces is
  recorded before its commit, so the retry keeps the original deadline, and a teardown
  requested meanwhile is evaluated at the next attempt. The first evaluation that
  completes logs `strategy_worker_recovered`; a tick that evaluates nothing, such as a
  multi-chain tick on a chain that is not due, leaves the streak running. A worker that
  misses its 30-second startup bound is retried the same way. A running deployment stops
  only after its evaluations have timed out without a success for 30 minutes; a
  deployment in teardown never stops for timeouts. The CPU limit and any other worker
  failure stay immediately fatal.
- An idle coordinator no longer reads its whole checkpoint from the gateway twice a
  second. Between decisions it now reads once per wait, waking at the next deadline or
  after at most 5 seconds, so a requested teardown or an unsolicited external effect is
  seen within 5 seconds instead of 1. The dashboard export reuses that read instead of
  loading the checkpoint again. Ctrl-C and pause are still honored within a second.
  Multi-chain deployments keep the one-second cadence. One deployment's gateway served
  425 checkpoint reads, averaging 490 ms, for every 3 observations.
- The gateway re-verifies a checkpoint's durable authority only when it changes.
  Every checkpoint read used to recompare the configuration and re-derive every
  Safe/Roles grant of the retained permission manifest; an unchanged authority now
  reuses the verdict this gateway process already reached, and any change is
  checked in full. Locally, re-deriving the one-partition Uniswap V3 manifest of
  that deployment took 11.5 ms per read, against 24 ms for a whole checkpoint read
  of the 12 KB reference fixture.
- ERC-20 tokens whose `approve` returns no data, such as USDT on Ethereum, can be
  approved and revoked. Every connector's approve and allowance-cleanup simulation
  now follows OpenZeppelin SafeERC20: an empty return or exactly ABI `true` passes;
  ABI `false`, any other length and reverts are still refused. Uniswap V4, Aave V3
  and V4, SparkLend, Morpho Blue, Compound V3 and GMX refused an empty return
  before. Uniswap V4 clears a residual ERC-20 allowance to Permit2 (left by a
  mint) to zero before approving a larger amount for Ethereum USDT, which reverts a
  nonzero-to-nonzero approve; other tokens approve as before, so retained executions
  replay unchanged. The other connectors already reset to zero first.
- A fork node starts at the fork block's base fee without Anvil's 1 gwei minimum
  tip, so its gas quote follows the forked chain and a mainnet-calibrated
  `strategy.gas` fits; chains whose fee history reports no tips (BNB) start at the
  upstream gas price.
- Backtest preparation that refuses a stale `resolution.json` names the file and
  the recovery: prepare into a fresh directory, or remove it and prepare again.
- Fork runs on Arbitrum-family chains (Arbitrum One, Robinhood Chain) no longer
  fail when a read precedes the first transaction. Their upstream block headers
  carry no Cancun blob fields, so Anvil refused every call at the forked block
  with "Excess blob gas not set."; an EOA-account fork on Arbitrum One stopped at
  start in the fixture's zero-balance check. A fork whose head lacks
  `excessBlobGas` now mines one empty local block once it is ready; the origin
  and its anchors are unchanged, and other chains are untouched.
- `almanak check` refuses a zero `hold_seconds`, `deadline_seconds` or Uniswap V4
  `collect_seconds` at the configuration stage, naming the field. The resolved
  parameters always required them to be positive, so a zero passed the offline
  check and failed only at backtest preparation or deployment. The editor schemas
  require them to be positive too.
- Economic backtests of a pool holding a beacon-proxy token, such as WBTC on
  Arbitrum, no longer fail at preparation with "gateway request refused". The
  historical gateway refused the token verification's reads of the beacon. A
  beacon pointer captured from a configured contract at the same block now
  permits exactly the beacon's code, its `implementation()` call and the code that
  call names; the MATIC proxy slot is followed the same way.
- A deployment survives a slow service start. The supervisor waits for a starting
  gateway or read gateway for as long as its process lives, up to 300 seconds, instead
  of a fixed 25, and says whether the process exited or never answered. A setup failure
  is now reported as `failure: "start"` on the failed stage, since no coordinator ran; a
  coordinator that stopped reports `failure: "execution"` and waits for an explicit
  resume. The supervisor log records how long the gateway and setup took.
- `almanak status` and the other workspace controls no longer import the backtest, model
  and Ethereum stacks: the CLI imports its error types only to report an error. Each
  call takes about a third less CPU.
- The configuration and operating guides no longer say Base and Optimism refuse
  authored gas caps everywhere. Only fork runs refuse them; production runs bound
  the L1 data and operator fees at quote time within the payer ceilings and
  reconcile the exact charges from each receipt. Agents reading the bundled docs
  told users that Base could not deploy with a gas policy.
- A multi-step Uniswap V3, PancakeSwap V3, Aerodrome Slipstream, Aerodrome or Curve
  action whose intent deadline passes partway through no longer stops the deployment for
  good. A recorded signed approval has no protocol deadline, so it is rebroadcast after
  its intent expires instead of being refused as `stale_plan` on every resume; the
  intent's window still bounds planning and signing. Preparing the next step of an
  expired intent drops the decision, as a stale signing already did, and the next
  decision plans afresh; before, the `stale_plan` refusal stopped the coordinator. A
  Robinhood Chain LP rebalance signed its fourth approval before the 300-second window
  closed and broadcast it just after, and every later start failed the same way.
- Permission export writes a Safe Transaction Builder batch only for a Safe that
  still lacks authority, as the check statement reports it. An installed Safe got
  a `safe-<n>.json` with no transactions, so a production deployment held only for
  funding exported an empty batch that hosts offered the owner to sign.
- GMX V2 on Arbitrum and Avalanche names the deployment that GMX currently
  authorizes. GMX revoked the RoleStore roles of the previously pinned
  ExchangeRouter and handlers on 16 and 17 September 2026, after which every
  request was refused at simulation with an unnamed RPC rejection. The compiled
  deployment table now carries the live router, Reader, Oracle, handlers and
  executors, verified against their compiler inputs, and records the roles each
  contract holds. Deployment verification reads those roles and refuses a
  retired deployment by contract, address and role. Pending-request decoding and
  creation receipt checks follow the new revision's `uiFeeFactor` field.
- The GMX economic funding model applies the deployed rules: minimum and maximum
  funding factors are read per paying side, an adaptive increase is floored by
  the market's minimum increase rate, and a zero next saved factor pays nothing.
  On 17 Arbitrum and 4 Avalanche markets it equals the live Reader's next
  funding, including per-size payment and claim deltas. The other GMX economic
  models have not been compared with the new revision.
- Clicking a section link in an embedded dashboard no longer blanks it. The links were
  plain `#block-…` anchors, which a `srcdoc` frame resolves against the host page's URL,
  so the frame navigated away. They now scroll to the section in place and mark it as
  the current one; previously the first link was always highlighted.
- Production plan preparation no longer fails with `block_mismatch` on any
  account. The production gateway's provider guard pins the genesis block to
  check the chain's identity, and operation validation runs that guard after
  preparation has pinned the observation block. The contract revision checks
  that follow (for example the Uniswap V3 pool and position manager checks)
  then compared the observation block with genesis and refused. The guard now
  restores the caller's pinned block. The earlier Safe authority fix covered
  only the second of these two pins, so 3.0.0rc32 still failed. A
  `block_mismatch` error from a revision check or a pinned contract call now
  names the contract role or call, the address, the block being checked and
  the block the reader has pinned.
- Planning an operation for a Safe account no longer fails with
  `block_mismatch` ("revision checks require the reader's pinned block").
  Safe authority verification pins a fresh chain head, and that pin replaced
  the observation block the plan had pinned. The contract revision checks that
  follow (for example the Uniswap V3 pool and periphery checks) then compared
  the observation block with the newer head and refused. Verification now
  restores the caller's pinned block when it finishes, so those checks read
  code at the observation block. Any chain whose head advanced during
  verification was affected; Robinhood Chain, with a block about every 0.26
  seconds, hit it on the first plan. EOA accounts were not affected.
- `almanak resume` on a provisioned workspace no longer runs admission inside the
  supervisor's control request. The control server answers one request at a time, and
  production admission reads the full Roles history, which could take about two minutes.
  The CLI request timed out after 35 seconds and status polls queued behind it.
  Admission, gateway start and coordinator start now run in the same background worker
  as initial provisioning: `resume` returns once accepted with `provisioning: true`,
  status keeps answering, other controls except `pause` are refused until it finishes,
  and a failure is persisted as the `failed` stage with its diagnostic. A workspace
  without execution state reports `provisioned` while its admission runs instead of the
  stale `action_required` diagnostic, and a pause requested meanwhile keeps the
  coordinator from starting.
- A first start runs production admission once. `setup()` admitted the workspace
  and then `start_coordinator` admitted it again, so the funding preflight, the
  full Roles history read and permission verification ran twice back to back
  (about 8 minutes each on Robinhood Chain) while the started gateway sat idle.
  The coordinator start now skips admission only when the same worker has just
  admitted; every other path still admits, and nothing is cached across starts.
- Production admission accepts standard Safe 1.4.1 deployments. Safe authority
  verification pinned only the L1 Safe 1.4.1 singleton, refused any fallback handler and
  required a Roles module with no operator, so such deployments failed with `Safe
  implementation is outside the supported profile`. It now also admits the SafeL2 1.4.1
  singleton and Safe's canonical CompatibilityFallbackHandler 1.4.1, each by pinned code
  hash, and a Roles history whose setup transaction already enabled and assigned the
  bound operator's role; the exported bundle then omits `assignRoles`. A guard, any
  other handler or singleton, and any other pre-installation event are still refused.
- Production admission reads the Roles history far faster. It proved the module's log
  history by fetching every block header since the Roles setup one request at a time,
  fetched it all again for `verify` after `generate`, and again on every later
  admission: on Robinhood Chain one admission for a Safe set up about 13,000 blocks
  earlier took well over five minutes, growing with the Safe's age. Headers and receipts
  are now read in JSON-RPC batches of up to 100, one admission shares its verified
  history between `generate` and `verify`, and the workspace keeps it in
  `private/deployment/roles-history.json` so the next admission reads only newer blocks.
  A retained history is used only after its last block is confirmed canonical under the
  new observation; otherwise the full history is read. Every existing refusal is
  unchanged. A batch with a missing, duplicated or rejected member fails the read, and a
  provider's HTTP 429 on a batch is re-sent at most six times.
- The first production start no longer fails with `gateway_unavailable` about a
  minute after it reports running. The gateway's `create` request generated and
  verified the Roles permissions with two more full history walks, which took
  minutes on an older Safe, while the coordinator waited 60 seconds for any
  gateway reply. The gateway now starts `create` from the admission checkpoint
  (passed as `--roles-history`, read and never written by the gateway), so it
  reads only the blocks since admission, and the coordinator waits up to 10
  minutes for `create` alone. A `create` that outlives the wait still cannot be
  applied twice: the deployment database is created exclusively.
- `almanak check --production` refuses an author file without `strategy.gas` in
  the offline configuration stage, with or without `--gateway-config`. The rule
  was enforced only during online resolution, so an offline production check
  passed and the missing gas policy first failed inside a started deployment.
  A check without a mode reports the unmet rule under `warnings`; its exit code
  is unchanged.
- Production runs with a remote signer (`signing.signer_file`) no longer ask for a
  keystore password. `almanak run`, `resume` and `teardown` prompted whenever the
  operator configured no `password_file`, so an unattended production run died with
  `EOFError` before its supervisor started, and a gateway restart would have passed
  `--password-stdin` to a remote signer. Only a keystore without a password file prompts
  now.
- `almanak ax --block` accepts a decimal block number, `finalized` or `latest` on
  every subcommand and refuses any other value at parse time with a message
  naming those forms. `latest` pins one concrete header through either
  transport, like `finalized`, and every read of the observation names its hash.
- Composed strategies decode their checkpoint about eight times faster. Each
  retained decision, plan, pending transaction and outcome is validated as the
  connector type of the allocation it acts on, instead of against every
  connector's record type, and the composed codec is built once. The gateway
  and runtime decode the checkpoint on every request, so composed lifecycle
  steps had slowed as outcomes accumulated; on a local BNB fork the
  `bnb_aave_borrow_and_trade_v4` Safe lifecycle fell from 30 to 11 minutes.
- Multi-chain composed checkpoints route each chain's active decision, plan and
  pending transaction the same way. Chain work is declared before the
  configuration, so those records were still validated against every
  connector's record type, at about 70 ms each, and every gateway request and
  runtime step paid it again while a chain had work in flight. They now route
  by the connector of their own target, with the same full-union fallback. For
  a retained `ethereum_base_borrow_and_trade` checkpoint with a decision and
  pending transaction on both chains, one decode fell from 321 ms to 131 ms and
  an in-process gateway `update` from 1.43 s to 0.78 s.
- `almanak ax perp-markets` enumerates the GMX V2 DataStore market list through
  its address-set accessors and the Reader's market pages. The previous bytes32
  accessors answered an empty list, so every label query reported `not_found`.
- GMX V2 market resolution accepts synthetic markets, whose index token is a
  venue-derived address with no contract (Arbitrum BTC/USD, DOGE/USD and most
  other listings). The index precision is read from the venue's data stream
  multiplier instead of an ERC-20 `decimals()` call, which failed with an ABI
  error. `perp-markets` reports `index_kind`, verifies a synthetic label against
  the venue's address derivation, and accepts GMX's `[LONG-SHORT]` collateral
  notation, for example `BTC/USD [WBTC-USDC]`, to select one of several
  markets sharing an index.
- Configuration validation uses the same resolved parameter contract in public
  checks, runtime creation and backtesting, with execution setup excluded.
