# Execution API

For author code use the [public authoring guide](../authoring.md); for commands use
the [operating guide](../operating.md). This page describes trusted integration contracts.

## Author configuration and support

A strategy owns `strategy.py` and one configuration file. The current
[reference strategy catalog](https://github.com/almanak-co/sdk/blob/main/strategies/README.md) selects explicit markets,
assets, capital, timing and gas policies in that file. Operator account
bindings, credentials and generated deployment files are runtime artifacts.

`connector_for(config).support_for(ChainId(...))` returns an immutable
`ConnectorChainSupport`. `supported_chains(RunMode.FORK)`,
`RunMode.PRODUCTION` and `RunMode.BACKTEST` enumerate static support. Aave and
Uniswap V4 advertise Ethereum and Avalanche for fork and production execution;
only Ethereum has historical models. Aave historical actions exclude borrowing
and repayment. Historical start times constrain an existing model; an omitted
time grants no additional market or data coverage.

Support records expose modes, typed `EconomicAction` values, account profiles,
restrictions and historical model identity. Configuration validation and gateway
operation admission independently check them. A declaration does not establish
provider availability, wallet funding or installed Safe authority.

Configured Aave reserves identify the Spoke, Hub, reserve ID and CAIP asset.
Admission verifies their observed relationship, reserve metadata and supported
implementation bytecode. Proxy verification normalizes only the verified
immutable administrator operand. Uniswap configuration identifies protocol
infrastructure and a full PoolKey. Admission verifies infrastructure revisions,
periphery relationships and initialized pool state. Asset and pool selection do
not consult sample token or market lists. Historical implementation code reads
require a captured proxy pointer from a configured contract at the same canonical
block; that evidence grants no calls or storage access to the implementation. A Uniswap V4
hook is supported only with a reviewed profile pinned by code (Fables, Uniswap StablePairHook and
PonsV2MemeHook instances; Fables and StablePairHook pools also admit LP, Pons pools only
trading); other hooks and protocol revisions fail. A
reviewed hook whose code later differs refuses new exposure and still admits exits. A native-coin
pool trades with the wrapped native held by the fund, wrapped and unwrapped by the reviewed
router within the swap; its liquidity is minted and closed with the wrapped native, which the
reviewed PositionManager unwraps and wraps within the same call. Simulation and receipt accounting require standard ERC-20
balance effects; unexplained effects prevent further exposure.
Historical admission retains its captured model and revision constraints;
compatibility for live execution does not establish backtest coverage.

Uniswap capital may identify either pool currency. `target.funding_currency` and
`target.counter_currency` preserve that orientation through quotes, inventory,
completion and pool-marked reporting. NFT exits retain a
broader Roles grant: the token ID is unrestricted inside exact collect and close templates
that pay the Safe, and the gateway restricts operations to the retained strategy-owned NFT.

## Production binding

Every reference binds through the operator configuration. `almanak run
--production --gateway-config` reads the public author file with
`configuration.unresolved.read_inputs`, resolves named markets through the
read-only gateway and constructs authoritative parameters with the operator's
account bindings before admission. The author retains economic gas preferences;
production networks, payer ceilings, account profiles and credentials belong to
the operator file. The SDK exposes no legacy author-loader or complete-config
production fallback. Archived roots under `scripts/legacy_configuration/` serve
only baseline and component evidence tooling.

Each binding has a name and an EOA or Safe/Roles profile. Its fund, submitter and
policy retain full chain and environment scope. The prepared deployment freezes
the author digest, resolved facts, account identities, strategy hooks and
compatible author/operator gas policy before the execution gateway can start.
The supervised run generates and verifies these deployment artifacts. See the
[operating guide](../operating.md#production).

Gas policies explicitly select `base_fee_basis`: `observed_block` in the
Avalanche samples and `next_block` for Ethereum. The retained quote has the
corresponding `observed_base_fee` or `next_base_fee`, plus its pinned block and
priority fee. Requested missing data fails; there is no automatic fee-basis or
provider fallback. The policy multiplier and hard caps apply to that observation.

The resulting configuration contains production authority only. Fork funding
and local signing keys do not transfer into it. The
[production operator guide](https://github.com/almanak-co/sdk/blob/main/strategies/aave_v4_weth_supply/PRODUCTION.md)
describes encrypted credentials, `production-check`, gateway launch and runtime
control. `production-check` reports each network separately. A configured URL
provider uses only its named environment variable; there is no fallback provider.

## Chain boundary and gateway routes

`Chain` combines a validated CAIP-2 identity and explicit environment authority.
New forks retain the origin CAIP identity and use an `anvil:<instance>`
environment. `ForkBinding` retains the pinned origin and separate local signing
chain `eip155:31337`. Avalanche native fees use
`eip155:43114/slip44:9005`; wrapping uses the verified WAVAX mapping.

`ChainAdapter` owns network guards, checkpoints, submission references,
preparation, signing, broadcast, receipts, reconciliation and execution costs.
`CheckpointReference` and `TransactionReference` carry a chain and an opaque
validated `ChainReference`. EVM references require canonical hashes and EVM
checkpoints require block heights. Other families preserve case and delegate
semantic validation to their adapters. EVM nonce, ABI, gas and signed-transaction
fields stay in the EVM implementation and connector execution types.

One authenticated loopback gateway contains explicitly composed adapter routes.
`ChainRouter` creates a route when accessed, so one unavailable network does not
prevent another route from operating. Multi-chain gateway requests to `observe`,
`audit`, `plan`, `sign`, `receipt`, `broadcast`, `reconcile` and execution `update`
must include the complete `chain` object. Omitted or mismatched routes are refused.
`GatewayClient` and the runtime's `ScopedGateway` provide those routes.

Use `gateway --fork-routes /private/deployment/fork-routes.json` for a generated
fork collection. Its entries contain `chain` and loopback `endpoint`. The `fork`
command starts one node per configured target, writes one deployment and routing
file, and installs only generated Safe authority on the identified fixtures.
Production routes come from `production.networks`. Both environments share
protocol, account wrapping, simulation, receipt and accounting code.

## Durable work and strategy observations

A multi-chain checkpoint contains `work`, one record per configured chain, and a
round-robin `scheduler_cursor`. Each record retains initialization, decision,
plan, pending attempt, fault and closure. One economic intent may be active on
each chain. The checkpoint has one configuration, strategy memory, outcome order
and delivery cursor. Decisions and outcome delivery remain serialized.

Single-chain checkpoints retain their scalar execution slot, including previously
recorded Ethereum fork identities and signed bytes. The runtime never rewrites a
pending attempt into another signing domain. Chain-scoped updates are projected
and independently validated under the same durable journal lock; updates that
alter another chain's execution facts are refused.

`DecisionInput.availability` exposes `unobserved`, `ready`, `unavailable`,
`invalidated` or `closed`, with retained fault information. A decision receives
fresh observations for its selected chain. It must skip unavailable allocations.
`Decision.reason` is an optional string the test run's coordinator logs with the
decision and nothing else reads.
`Decision.required_observations` declares `(chain, account, allocation)` scopes.
The runtime and gateway reject dependencies absent from the decision evidence;
the connector additionally verifies its economic dependencies. The new reference
strategies declare the observations used by each economic decision. No missing
observation is interpreted as zero. Cross-chain dependent decisions have no
combined observation acquisition workflow in this increment and fail closed.

Committed receipt canonicality is audited before advancing a route. Unknown
attempts reconcile using their retained signed identity before new affected
work. A chain-local outage or invalidated history blocks that chain; independent
healthy entries and exits continue. A shared database, gateway, coordinator or
strategy failure stops the runtime. Creating a Safe deployment requires all its
permissions to be generated and verified; independent progress begins after that
admission. Gateway replacement uses the same database and credentials.

Teardown persists at deployment scope. Each available chain repays, exits and
verifies its remaining obligations independently. Overall `closed` requires all
chains closed. Invalidated history remains halted for operator investigation;
automatic reorganization recovery is unsupported. Reports retain per-chain work,
readiness, balances, debt, permissions and payer gas. They do not sum unlike
assets or infer a consolidated valuation source.
