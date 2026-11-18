# How the SDK works

An Almanak strategy proposes economic actions. It does not talk directly to a
chain, hold credentials, sign transactions or decide whether an operation was
completed. Trusted SDK components resolve those responsibilities and retain the
evidence needed to recover safely.

## The complete path

```text
strategy.py + config.yaml
          |
          v
sandboxed strategy hook  -->  decision + proposed memory
          |                            |
          v                            v
trusted runtime  <-->  connector  <-->  authenticated gateway  <-->  provider / chain
          |
          v
durable state, outcomes, accounting and reports
```

Backtests use the same strategy hooks and economic intents, but replace live
execution with an explicit historical model. Fork and production runs use the
shared execution, reconciliation and accounting path with different operator
authority.

## What each part owns

| Part | Owns | Does not own |
| --- | --- | --- |
| Strategy | Economic decisions and proposed memory changes | Secrets, network access, signing, persistence or authoritative holdings |
| Author configuration | Markets, accounts, allocations, quantities, timing and selected financial sources | Providers, wallet addresses, signing material or fork fixtures |
| Runtime | Scheduling, durable work, recovery, outcome delivery and teardown | Protocol-specific transaction construction or credentials |
| Connector | Protocol observations, actions, transaction plans, position semantics and cleanup | Strategy scheduling or independent durable state |
| Gateway | Controlled reads, authorization, credentials, signing and submission | Strategy decisions or lifecycle progression |
| Operator configuration | Providers, account bindings, fork fixtures and production authority | Economic strategy choices |
| Durable record | Captured inputs, pending work, outcomes, positions and evidence | A second scheduler or an alternative view of completion |

This separation lets the gateway reject an unauthorized request even if
strategy code proposes it, and lets the runtime recover admitted work without
asking the strategy to guess what happened.

## The decision cycle

1. The runtime checks whether a strategy evaluation is eligible.
2. Connectors acquire the explicitly requested observations through the gateway,
   or read retained historical inputs in a backtest.
3. The sandbox receives resolved configuration, strategy memory, owned positions,
   observations, chain availability and the current trigger.
4. `decide(ctx)` returns at most one typed economic intent and an optional memory
   update.
5. The runtime records the admitted decision before external execution.
6. The connector expands the economic intent into the protocol steps it owns,
   such as approvals, a swap, repayment or allowance cleanup.
7. The gateway independently checks the exact operation and its execution
   authority before a live or fork submission.
8. The runtime reconciles the result, updates connector-owned positions and
   delivers the retained outcome to `on_outcome(ctx)`.

An approval or another setup step can produce an `intermediate` outcome. The
runtime continues that already admitted economic action without calling
`decide()` again. Only economic completion produces `completed`; a refused or
unsuccessful operation produces `failed`.

## State, positions and recovery

Strategy memory records decision state such as a phase or start time. It is not
an authoritative account balance or proof that a position exists. Connectors and
the durable runtime record own economic positions, pending transactions,
allowances, debt and other obligations.

Before resuming, the runtime reconciles recorded external work. An unknown
outcome cannot authorize a replacement action. Captured strategy source and
configuration remain fixed for the run. Persisted strategy memory evolves
through accepted updates and remains available for recovery, so editing the
original files does not change an open deployment.

Teardown is also a durable lifecycle state. It bypasses the ordinary decision
interval and asks the strategy and connectors to reduce risk and close their
known obligations. A successful transaction receipt alone is not proof that all
debt, positions or spending authority are clear.

## Three environments

| Environment | What it proves | Authority and data |
| --- | --- | --- |
| Backtest | Strategy behavior under a named historical model and frozen assumptions | Hypothetical inventory; preparation may use read-only providers, execution and reproduction are offline |
| Fork | Real contract behavior at an identified chain state with simulated funds | Generated fixture accounts and authority on retained local fork processes |
| Production | Supported execution against a live chain | Explicit operator account bindings, payer ceilings and signing authority |

Results from one environment do not imply guarantees from another. A passing
backtest does not prove contract execution, and a fork lifecycle does not
authorize real spending.

## Identity and scope

Chains use strict CAIP-2 identifiers such as `eip155:1`. Assets use CAIP-19
identifiers such as
`eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`.
Symbols such as USDC and WETH are display metadata, not identity.

Every action remains scoped to a named fund account, chain, connector target and
allocation. The gas payer can be different from the account that owns the
position. Likewise, two allocation names do not create separate ownership of the
same account and protocol balance.

## Continue from here

- Follow [Getting started](getting-started.md) for a complete reference journey.
- Use [Strategy authoring](authoring.md) while writing `strategy.py`.
- Use [Configuration](configuration.md) while writing `config.yaml`.
- Check [Supported connectors](support.md) before selecting a connector, chain,
  action or environment.
- Browse [Example strategies](examples.md) to find the nearest executable
  starting point.
