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
of. If there is no wallet it creates one with
`almanak wallet create --generate-password` and asks the user to fund it. The agent
never asks for an address, a key or a seed phrase. Before the user funds the
wallet it tells them that it cannot send funds back, and that the key exists only
as `keystores.json` with its password in `keystore-password`, under
`~/.almanak/agent/wallet/` on that machine.

The skill covers every direct action the CLI serves for a plain wallet:

| Family | Commands | Described in |
| --- | --- | --- |
| Lending | `ax supply`, `ax withdraw`, `ax borrow`, `ax repay`, `ax collateral` | [Operator actions](operating.md#operator-actions) |
| Swaps | `ax swap` | [Swaps](operating.md#swaps) |
| Liquidity | `ax open-liquidity`, `ax increase-liquidity`, `ax decrease-liquidity`, `ax collect-liquidity-fees`, `ax close-liquidity`, `ax revoke-exchange` | [Liquidity](operating.md#liquidity) |

It holds no list of chains. Before an action the agent runs `almanak support` for
the connector and chain and goes on only when `direct_actions` lists the action's
kind.

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
  price is in its range. On Curve and Aerodrome classic the position is the
  wallet's balance of the pool's LP token, and on Curve the user chooses whether a
  burn pays every coin or one.
  Nothing is swapped inside a liquidity action.
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
refuse; there the agent asks for the market and verifies it with
`ax lending-reserve`.

The agent keeps its files in `~/.almanak/agent/`: the wallet directory, the env
file with the provider key once there is one, and a `settings.json` with the fee
ceilings per chain, the markets it has acted in, the liquidity positions it opened
and the tokens the wallet received. It knows only those positions; a position
opened elsewhere is read when the user names its market or token ID.

## Limits

- The wallet is a plain account that holds the funds. Whoever controls the machine
  the agent runs on controls the wallet. Safe/Roles accounts are not supported for
  operator actions.
- GMX V2 and Hyperliquid serve no direct action. No direct action is served on
  HyperEVM, which has no public endpoint either: a provider key there serves reads
  only.
- An action takes the token itself: a chain's native coin must already be wrapped.
- The agent cannot send funds back. Nothing here transfers out of the wallet, so
  funds leave it only through its key, which the user should keep a copy of.
- An action has no amount ceiling. The fee ceilings and the wallet balance bound
  it, and the confirmation before `--execute` is an instruction to the agent, not
  an enforced control.
- The minimum a swap, a deposit or a burn accepts, the deadline where the call
  carries one, and the gas price and gas limit are in the signed transaction, so
  the chain enforces them. The health floor is a check before signing only.
  Nothing protects a position after its step is mined, and the agent does not
  watch positions.
- A public endpoint's operator sees the addresses read and the transactions sent,
  and an action signs what that one endpoint's reads resolve.
- A wallet that holds a pool's LP shares on Aerodrome or Curve is refused a swap in
  that pool, and anyone can send such shares. The agent cannot transfer them; it
  can burn them with `ax close-liquidity` when the user decides so.
