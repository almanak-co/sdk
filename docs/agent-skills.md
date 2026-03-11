# Agent Skills

The Almanak SDK ships with an **agent skill** -- a structured knowledge file that teaches AI coding agents how to build DeFi strategies. Install it into your project so your AI assistant understands the IntentStrategy API, intent vocabulary, market data methods, and CLI commands.

## Quick Install

The fastest way to get started:

```bash
almanak agent install
```

This auto-detects which AI coding tools you use (by checking for `.claude/`, `.cursor/`, `.github/`, etc.) and installs the skill into each platform's native skills directory.

## Platform-Specific Instructions

### Claude Code

Claude Code discovers skills from `.claude/skills/` subdirectories.

```bash
almanak agent install -p claude
```

**Result:** `.claude/skills/almanak-strategy-builder/SKILL.md`

---

### OpenAI Codex

Codex discovers project skills from `.codex/skills/` subdirectories.

```bash
almanak agent install -p codex
```

**Result:** `.codex/skills/almanak-strategy-builder/SKILL.md`

---

### Cursor

Cursor reads custom rules from `.cursor/rules/` as `.mdc` files with glob-based scoping.

```bash
almanak agent install -p cursor
```

The installed file includes YAML frontmatter that activates the skill when editing `strategy.py` or `config.json` files:

```yaml
---
globs:
  - "**/strategy.py"
  - "**/config.json"
---
```

**Result:** `.cursor/rules/almanak-strategy-builder.mdc`

---

### GitHub Copilot

Copilot reads scoped instructions from `.github/instructions/` with `applyTo` frontmatter.

```bash
almanak agent install -p copilot
```

The installed file includes frontmatter scoped to strategy files:

```yaml
---
applyTo: "**/strategy.py"
---
```

**Result:** `.github/instructions/almanak-strategy-builder.instructions.md`

---

### Windsurf

Windsurf reads rules from `.windsurf/rules/`.

```bash
almanak agent install -p windsurf
```

**Result:** `.windsurf/rules/almanak-strategy-builder.md`

---

### Cline

Cline reads rules from `.clinerules/`.

```bash
almanak agent install -p cline
```

**Result:** `.clinerules/almanak-strategy-builder.md`

---

### Roo Code

Roo Code reads rules from `.roo/rules/`.

```bash
almanak agent install -p roo
```

**Result:** `.roo/rules/almanak-strategy-builder.md`

---

### Aider

Aider discovers skills from `.aider/skills/` subdirectories.

```bash
almanak agent install -p aider
```

**Result:** `.aider/skills/almanak-strategy-builder/SKILL.md`

---

### Amazon Q

Amazon Q reads rules from `.amazonq/rules/`.

```bash
almanak agent install -p amazonq
```

**Result:** `.amazonq/rules/almanak-strategy-builder.md`

---

### OpenClaw

OpenClaw discovers skills from `.openclaw/skills/` subdirectories.

```bash
almanak agent install -p openclaw
```

**Result:** `.openclaw/skills/almanak-strategy-builder/SKILL.md`

---

## Install All Platforms

To install for every supported platform at once:

```bash
almanak agent install -p all
```

## Global Install

Install into your home directory (`~/`) so the skill is available in every project without per-project setup:

```bash
almanak agent install -g
```

This writes to `~/.claude/skills/`, `~/.codex/skills/`, `~/.cursor/rules/`, etc. Platforms that only support project-scoped rules (Copilot, Cline, Roo Code, Amazon Q) are automatically skipped.

You can also target specific platforms:

```bash
almanak agent install -g -p claude
```

To check or update global installs:

```bash
almanak agent status -g
almanak agent update -g
```

!!! note
    Most platforms let local (project) skills override global ones. If you have both, the project-level skill takes precedence.

## Install via npx (skills.sh)

If you use the [skills.sh](https://skills.sh) registry, you can install directly from GitHub:

```bash
npx skills add almanak-co/sdk
```

This discovers the `almanak-strategy-builder` skill from the public repo and installs it for your detected platform.

## Managing Installed Skills

### Check Status

See which platforms have the skill installed and whether they are up to date:

```bash
almanak agent status
```

Example output:

```text
Agent skill status (SDK v2.0.0):

  claude      up to date (v2.0.0)
  codex       not installed
  cursor      outdated (v1.9.0 -> v2.0.0)
  copilot     not installed
  ...

Installed: 1  Outdated: 1  Missing: 7
```

### Update

After upgrading the SDK (`pip install --upgrade almanak`), update your installed skill files to match:

```bash
almanak agent update
```

This scans for all installed platform files and replaces their content with the current SDK version.

### Dry Run

Preview what `install` would do without writing any files:

```bash
almanak agent install --dry-run
```

### Custom Directory

Install into a specific project directory:

```bash
almanak agent install -d /path/to/my-project -p claude
```

## Per-Strategy Guides

When you scaffold a new strategy with `almanak strat new`, an `AGENTS.md` file is automatically generated inside the strategy directory. This lightweight guide is tailored to the template you chose -- it lists only the intent types and patterns relevant to that specific strategy.

Each scaffolded strategy is a self-contained Python project with `pyproject.toml`, `.venv/`, and `uv.lock`, so the per-strategy `AGENTS.md` also documents adding dependencies (`uv add`) and running tests (`uv run pytest`).

```bash
almanak strat new --template mean_reversion --name my_rsi --chain arbitrum
# Creates my_rsi/AGENTS.md alongside strategy.py, config.json, pyproject.toml, etc.
```

## What the Skill Teaches

The bundled skill covers:

| Section | What it covers |
|---------|---------------|
| Quick Start | Install, scaffold, run |
| Core Concepts | IntentStrategy, decide(), MarketSnapshot |
| Intent Reference | All intent types with signatures and examples |
| Market Data API | price(), balance(), rsi(), macd(), bollinger_bands(), and 10+ indicators |
| State Management | self.state persistence, on_intent_executed callback |
| Configuration | config.json format, .env secrets, anvil_funding |
| Token Resolution | get_token_resolver(), resolve_for_swap() |
| Backtesting | PnL simulation, paper trading, parameter sweeps |
| CLI Commands | strat new/run/demo/backtest, gateway, agent |
| Chains & Protocols | 12 chains, 10 protocols with enum names |
| Common Patterns | Rebalancing, alerting, teardown, IntentSequence |
| Troubleshooting | Common errors and fixes |

## Reading the Skill Directly

To view or grep the raw skill content:

```bash
# Print the file path
almanak docs agent-skill

# Dump the full content
almanak docs agent-skill --dump

# Search for specific topics
almanak docs agent-skill --dump | grep "Intent.swap"
```
