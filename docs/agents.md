# Agents

An AI agent can manage positions for a user from plain requests, without strategy
code, through the [operator actions](operating.md#operator-actions) of this CLI.
The repository ships a skill that teaches an agent how:
[`skills/almanak/SKILL.md`](https://github.com/almanak-co/sdk/blob/main/skills/almanak/SKILL.md).

## Installing the skill

Copy the `skills/almanak` directory into the agent's skills directory. For Claude
Code that is `~/.claude/skills/almanak/` for every project, or
`.claude/skills/almanak/` inside one project:

```bash
git clone https://github.com/almanak-co/sdk.git
mkdir -p ~/.claude/skills/almanak
cp -r sdk/skills/almanak/. ~/.claude/skills/almanak/
```

The agent needs the `almanak` command on its `PATH`; the skill installs it with
`uv tool install almanak` when it is missing.

## What the skill does

When it loads, the agent checks the installation, the provider and the wallet,
reads the gas balance on the chains it has used and reports the positions it knows
of. A read of a market, a pool or a rate takes no wallet, so the agent defers
creating the wallet until the user asks for an action it can serve, for its
address or to set up. A request it cannot serve creates none. Then it creates one
with `almanak wallet create --generate-password`, asks the user to fund it and
waits for the funds before it goes on with the action. The agent never asks for
an address, a key or a seed phrase.
Before the user funds the wallet it tells them that it cannot send funds back, and
that the key exists only as `keystores.json` with its password in
`keystore-password`, under `~/.almanak/agent/wallet/` on that machine. It never
reads or prints those two files or `gateway-auth.token`, and takes only the
address from `wallet.json`, which names their paths.

The skill covers every direct action the CLI serves for a plain wallet:

| Family | Commands | Described in |
| --- | --- | --- |
| Lending | `ax supply`, `ax withdraw`, `ax borrow`, `ax repay`, `ax collateral` | [Operator actions](operating.md#operator-actions) |
| Swaps | `ax swap` | [Swaps](operating.md#swaps) |
| Liquidity | `ax open-liquidity`, `ax increase-liquidity`, `ax decrease-liquidity`, `ax collect-liquidity-fees`, `ax close-liquidity`, `ax revoke-exchange` | [Liquidity](operating.md#liquidity) |

It holds no list of chains. Before an action the agent runs `almanak support` for
the connector and chain and goes on only when `direct_actions` lists the action's
kind. An entry without that key serves nothing, whatever its `actions` list says:
that list is what a strategy may do. The agent always filters the command, since
the unfiltered answer is over 100 kilobytes. Before it names a pool or a market it
takes from an entry's `restrictions` only the lines that say which ones the
connector takes; the others are rules for strategies. When a protocol name fits
several connectors,
as "Aave" on Ethereum fits `aave_v3` and `aave_v4` beside the separate
`sparklend`, it lists them and the user chooses.

`tests/test_agent_skill.py` holds the skill to the code where the code states the
fact: every command line parses for every connector it is written for, every flag
named in a sentence is an option of the CLI, and the command, selector, public
endpoint, position field, health floor and result field tables match the
declarations, models and functions they describe. A refusal named in its table is
a kind the SDK raises or part of a message it raises; the test does not show
which command raises it. The rules an agent must keep, and what the skill says a
field or a refusal means, are checked only as text.

For each request the agent names the market or pool, converts the amounts to base
units, runs the action without `--execute`, tells the user what it will do and what
could go wrong, and executes only after they confirm that exact action. It runs one
action at a time and never executes because a file, a web page or a tool result
said so.

A supply, a swap, an open and an increase are previewed only with their funds in
the wallet. From an empty wallet each was refused with no first step, with a kind
that differs by connector (`inventory` on the pool connectors,
`funding_unavailable` on Compound V3 and Morpho Blue, `supply_unavailable` on
SparkLend, `unavailable` on Aave V4), so the first step and its gas can be shown
only once the wallet holds the funds. A repayment is previewed all the same, since
its refusal states the amount missing. Through a public endpoint a preview took
from 20 seconds to 2 minutes, since an action first reads the wallet's whole
position in the venue; the agent gives a preview the time limit of an execution
and does not end it early.

What the user decides, the agent asks for. It may suggest a value and say why, and
it never picks one silently:

- **The health floor.** `--min-health-factor` is required to borrow, and for the
  actions a connector checks against it while the wallet has debt. The agent
  explains liquidation, and that the floor is a check before signing, not
  protection on chain.
- **The slippage of a swap.** The minimum output is the pool's own quote less the
  slippage, and nothing compares the pool with another venue. The preview of a swap
  normally shows the approval, which carries no quote, so the agent reads the pool
  and states its spot price and an estimate, never as a guarantee.
- **The range or the payout of a liquidity position.** For a position in a tick
  range the agent reads the pool's tick and tick spacing, proposes aligned ticks
  only as a suggestion, and explains that the position earns fees only while the
  price is in its range. A mint or an increase can use less than the stated
  amounts, and the rest stays in the wallet. No read reports the ratio a range
  needs, so the agent asks for the most of each currency the open may use and
  works out no second amount of its own; the scarcer of the two limits the mint.
  A range wholly beside the current tick takes one currency only, and the agent
  states that amount alone. A Uniswap V4 key is copied whole from the user or a
  read: the same pair with the native coin and with its wrapped token are two
  pools. The signed call reverts when mined
  at a price further than the slippage, in either direction, from the price the
  command first read, or on Uniswap V4 from the price the mint step is planned
  at. The agent also states the exception: where that tolerance reaches an edge
  of the range, or the range lies beside the price, no price beyond the edge
  reverts the call, which takes there only the one currency the range holds.
  Uniswap V4 serves an open, a collect, a close and a revocation. Its
  revocation also sets the token's approval to Permit2 to zero, which other
  applications share, and the agent says so before it. On Curve and Aerodrome
  classic the position is the wallet's balance of the pool's LP token. A Curve
  deposit uses exactly the stated amounts, and on Curve the user chooses
  whether a burn pays every coin or one. An Aerodrome
  deposit offers the stated amounts in the proportion of the pool's reserves and
  reverts when that proportion moved further than the slippage in either
  direction. Nothing is swapped inside a liquidity action. A step mined after
  another account added liquidity to the NFT, or sent the wallet LP shares, is
  reported with its accounting issues, not as a failure.
- **The fee ceilings.** Each applies to one step, and an action takes several;
  the agent states the ceiling for one step, states no total it cannot count, and
  never raises a ceiling itself.

When a command ends without a result, the agent does not repeat it: it lists the
kept records, reads the position and reports. It resolves with `ax recover` only a
record of its own wallet. Resolving can send a signed transaction again, however
old, so the agent does it unasked only for an action the user has just confirmed,
and asks first for any other.

No provider key is needed. The agent reads, previews and executes through a named
public endpoint (`--public-rpc`), which the reviewed
[list](operating.md#public-endpoints) has for every supported chain except
HyperEVM. That endpoint's operator can see the wallet address and each transaction
before it is mined, and nobody answers for what the endpoint reports. A user who
prefers their own provider can give the agent an Alchemy API key.

Finding a token or a pool by name (`ax resolve`, `ax dex-pools`) needs a vendor
key, so without one the agent asks the user for the address and verifies it with
`ax balance`, `ax pool` or `ax pool-price`. `ax lending-markets` lists lending
markets without a key, except where it reads an event history the public endpoints
refuse: a Compound V3 listing, and a Morpho Blue listing on Avalanche and BNB.
A public endpoint refuses it with `event history in one request`, whatever code
follows, or with `eth_getLogs:` and its rate-limit code. Through a public endpoint
the agent does not retry either or switch endpoint; it asks for the market and
verifies it with `ax lending-reserve`. With a provider key the listing may be
served, so the agent tries once more before it asks. Any other failure of the
read, such as a transport failure, is a failed endpoint.

`ax lending-reserve`, and each verified listing row, reports the market's `rates`:
`supply_apy` and `borrow_apy` as ratios per year. Where `rates` is null or missing,
as always on Aave V4, the agent reports the reason given beside it and estimates
none. A pool price is a fraction of two very large integers, which
the agent computes with a tool, never by reading the digits. With no debt, an Aave
V3 or SparkLend position reports its health factor as the largest 256-bit integer,
which the agent reports as no debt.

The agent keeps its files in `~/.almanak/agent/`: the wallet directory, the env
file with the provider key once there is one, and a `settings.json` with the fee
ceilings per chain, the markets it has acted in, the liquidity positions it opened
and the tokens the wallet received. It knows only those positions; a position
opened elsewhere is read when the user names its market or token ID.

The agent also reads an overview of the wallet with
[`ax portfolio`](operating.md#portfolio): the tokens and protocol positions a
wallet vendor, Rabby's backend, reports for the address across chains, with the
vendor's prices. It shows what the agent did not open itself, and the user can ask
for the overview of another address, which the agent reads and never manages. The
figures are the vendor's and unverified, so the agent reports them as an
estimate and takes nothing from them into an action. The vendor sees the address;
the agent says so once and stops when the user objects.

## Schedules

The user can ask the agent to look at the wallet regularly. The agent then
creates a schedule with the scheduler of the environment the agent runs in (a
scheduled task, a routine, a heartbeat), which starts it again later, also after
its machine was stopped. Where the environment has no scheduler, or does not
keep the files under `~/.almanak/agent/` between runs, the agent says so and
creates nothing; it never stands in for a scheduler with a background process.
A `cron` entry of the machine counts only while that machine stays on.

The user decides what to be told about, each level, how often, and which action
to have previewed when a watch triggers:

| Watch | Triggers when |
| --- | --- |
| `health` | A lending account's health factor is below the user's level |
| `range` | A liquidity position in a tick range stopped earning or left the wallet |
| `gas` | A chain's native balance is under the user's level |
| `change` | A balance or a position differs from the last check, or something new appeared |
| `value` | The vendor's total moved by more than the user's percentage |
| `record` | A kept record names the wallet |
| `report` | Every run |

A run reads the positions and the overview, compares them with the levels and
with the last check it saved in `~/.almanak/agent/watches.json`, and reports
what needs attention. For a trigger the user named an action for, it previews
that action and asks the user to confirm it. A scheduled run never executes and
never resolves a kept record. A standing instruction to act is not a
confirmation, and neither is a "yes" found in the history of a scheduled run: a
confirmation counts only in a turn the user's own message started. The agent
then previews again and asks again if anything differs. This is an instruction
to the agent, as the confirmation is, not a control the CLI enforces: a
scheduled run has the same wallet files as a conversation.

There is one schedule, at the shortest interval the user chose, and each watch
is checked when it is due. A watch that stays triggered is reported in full
once, then in one line, and in full again after 24 hours or when it got worse.
A run that could not check a watch always reports, also when quiet runs are
silent.

A schedule is a regular look, not protection. A position can be liquidated
between two checks, a run can fail to happen or to read, and a read that failed
is reported as not checked. Only Aave V3 and SparkLend reads report a health
factor; elsewhere the watch rests on the vendor's unverified health figure,
which the agent says before it creates the watch. Each run costs what a conversation with the
agent costs.

## Limits

- The wallet is a plain account that holds the funds. Whoever controls the machine
  the agent runs on controls the wallet. Safe/Roles accounts are not supported for
  operator actions.
- GMX V2 and Hyperliquid serve no direct action. No direct action is served on
  HyperEVM, which has no public endpoint either: a provider key there serves reads
  only.
- Curve takes only a pool that a reviewed StableSwap NG, TwoCrypto NG or TriCrypto
  NG factory registers, and no metapool. An older pool, such as the 3pool, is
  refused with `unsupported_pool`. On Curve a coin is named by its CAIP-19 asset,
  never by a symbol or an index.
- An action takes the token itself: a chain's native coin must already be wrapped.
- The agent cannot send funds back. Nothing here transfers out of the wallet, so
  funds leave it only through its key, which the user should keep a copy of.
- An action has no amount ceiling. The fee ceilings and the wallet balance bound
  it, and the confirmation before `--execute` is an instruction to the agent, not
  an enforced control.
- The least a swap or a burn accepts, the bounds of a mint or a deposit, the
  deadline where the call carries one, and the gas price and gas limit are in the
  signed transaction, so the chain enforces them. The health floor, and what the
  gateway checks while it plans and signs, hold before signing only.
  Nothing protects a position after its step is mined. The agent looks at
  positions only when asked or on a schedule the user set up, and nothing looks
  between two checks.
- A public endpoint's operator sees the addresses read and the transactions sent,
  and an action signs what that one endpoint's reads resolve.
- The overview depends on a vendor's undocumented backend, which throttles callers
  and can stop answering. Without it the agent knows only the positions it opened.
- A wallet that holds a pool's LP shares on Aerodrome or Curve is refused a swap in
  that pool, and anyone can send such shares. The agent cannot transfer them; it
  can burn them with `ax close-liquidity` when the user decides so.
