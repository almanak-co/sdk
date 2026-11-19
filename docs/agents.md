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
reads gas balances on the supported chains and reports the positions in the markets
it has acted in. If there is no wallet it creates one with
`almanak wallet create --generate-password` and asks the user to fund it.

For each request it finds the market, converts the amount to base units, runs the
action without `--execute`, shows the user the preview, and executes only after
they confirm.

No provider key is needed. The agent reads, previews and executes through a named
public endpoint (`--public-rpc`). That endpoint's operator can see the wallet
address and each transaction before it is mined, and nobody answers for what the
endpoint reports. A user who prefers their own provider can give the agent an
Alchemy API key.

The agent can also read a balance, a market or a position on any other supported
chain at an address the user names. Public endpoints cover every supported chain
except HyperEVM, which needs a provider key; the
[list](operating.md#public-endpoints) names the endpoints of each chain. Finding a
token or a pool by name (`ax resolve`, `ax dex-pools`) needs a vendor key, so
without one the agent asks the user for the address.

The agent keeps its files in `~/.almanak/agent/`: the wallet directory, the env
file with the provider key once there is one, and a `settings.json` with fee ceilings and the markets
it has acted in.

## Limits

- The wallet is a plain account that holds the funds. Whoever controls the machine
  the agent runs on controls the wallet. Safe/Roles accounts are not supported for
  operator actions.
- Actions are Aave V4 supply and full withdrawal on Ethereum and Avalanche. On
  the other chains the agent only reads.
- A supply that is enabled as collateral cannot be withdrawn: the withdrawal is
  refused, and no direct command disables collateral.
- An action has no amount ceiling. The fee ceilings and the wallet balance bound
  it, and the confirmation before `--execute` is an instruction to the agent, not
  an enforced control.
