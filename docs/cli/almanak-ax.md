# almanak ax

Execute DeFi actions directly from the command line. One-shot commands for swaps, balance checks, price queries, and more -- no strategy files needed.

Auto-starts a gateway if none is running. Use `--network anvil` for safe local testing.

## Usage

```
Usage: almanak ax [OPTIONS] COMMAND [ARGS]...
```

`almanak ax` supports two modes:

- **Structured mode** (default) -- deterministic, scriptable, no LLM needed
- **Natural language mode** (`--natural` / `-n`) -- describe what you want in plain English, LLM interprets it

## Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--gateway-host` | | `localhost` | Gateway hostname (env: `GATEWAY_HOST`) |
| `--gateway-port` | | `50051` | Gateway gRPC port (env: `GATEWAY_PORT`) |
| `--chain` | `-c` | `arbitrum` | Default chain (env: `ALMANAK_CHAIN`) |
| `--wallet` | `-w` | auto | Wallet address (env: `ALMANAK_WALLET_ADDRESS`). Auto-derived from `ALMANAK_PRIVATE_KEY` if not set |
| `--max-trade-usd` | | `10000` | Max single trade size in USD |
| `--dry-run` | | `false` | Simulate only, do not submit transactions |
| `--json` | | `false` | Output results as JSON instead of human-readable tables |
| `--yes` | `-y` | `false` | Skip confirmation prompts (for non-interactive / AI agent use) |
| `--natural` | `-n` | | Natural language mode (see below) |
| `--network` | | auto | `mainnet` or `anvil`. Auto-starts a gateway if none is running |

## Structured Mode

Use subcommands with explicit parameters. Deterministic, scriptable, zero latency overhead.

### `almanak ax price`

Get the current USD price of a token.

```bash
almanak ax price ETH
almanak ax price USDC --chain base
almanak ax price ETH --json
```

### `almanak ax balance`

Get the balance of a token in your wallet.

```bash
almanak ax balance USDC
almanak ax balance ETH --chain base
almanak ax balance WETH --json
```

### `almanak ax swap`

Swap tokens on a DEX.

```bash
almanak ax swap USDC ETH 100                    # Swap 100 USDC to ETH
almanak ax swap USDC ETH 100 --dry-run           # Simulate only
almanak ax swap USDC ETH 100 --slippage 100      # 1% slippage
almanak ax swap USDC ETH 100 --chain base --yes  # Skip confirmation
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--slippage` | `50` | Max slippage in basis points (50 = 0.5%) |
| `--protocol` | auto | Specific DEX protocol (default: best available) |

### `almanak ax lp-info`

Get details about an existing LP position (range, liquidity, accrued fees, in-range status).

```bash
almanak ax lp-info 123456                      # View LP position #123456
almanak ax lp-info 123456 --json               # JSON output
almanak ax lp-info 123456 --protocol uniswap_v3
```

### `almanak ax lp-close`

Close (fully withdraw) a liquidity position and collect accrued fees.

```bash
almanak ax lp-close 123456                     # Close LP #123456
almanak ax lp-close 123456 --dry-run           # Simulate only
almanak ax lp-close 123456 --no-collect-fees   # Skip fee collection
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--protocol` | `uniswap_v3` | LP protocol |
| `--no-collect-fees` | `false` | Skip collecting accrued fees |

### `almanak ax pool`

Get details about a liquidity pool (current price, tick, liquidity, volume, fees, TVL).

```bash
almanak ax pool WBTC WETH                      # Pool state
almanak ax pool USDC ETH --fee-tier 500        # 0.05% fee tier
almanak ax pool WBTC WETH --json               # JSON output
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--fee-tier` | `3000` | Pool fee tier in hundredths of a bip (3000 = 0.3%) |
| `--protocol` | `uniswap_v3` | DEX protocol |

### `almanak ax tools`

List all available tools in the catalog.

```bash
almanak ax tools                    # List all tools
almanak ax tools --category action  # Only action tools
almanak ax tools --json             # JSON output
```

### `almanak ax run`

Run any tool from the catalog by name. Generic fallback for tools without a dedicated subcommand.

```bash
almanak ax run get_price '{"token": "ETH"}'
almanak ax run get_balance '{"token": "USDC"}' --json
almanak ax run compile_intent '{"intent_type": "swap", ...}'
```

## Natural Language Mode

!!! info "Requires LLM API key"
    Natural language mode uses the same `AGENT_LLM_*` environment variables as
    the [agentic trading](../agentic/index.md) path. Set `AGENT_LLM_API_KEY` before use.

Describe what you want in plain English. The LLM interprets your request into a structured tool call, shows what it understood, then executes through the same pipeline.

```bash
almanak ax -n "what's the price of ETH?"
almanak ax -n "check my USDC balance on base"
almanak ax -n "swap 5 USDC to WETH on base" --dry-run
almanak ax --natural "open an LP position with 1000 USDC and 0.5 ETH"
```

### How it works

1. Your text is sent to the LLM along with the full tool catalog
2. The LLM returns exactly one tool call (single-shot, not an agent loop)
3. The interpreted action is **always shown** before execution (even with `--yes`)
4. The same safety gate applies -- write actions require confirmation

### Example output

```
$ almanak ax -n "swap about 5 bucks of USDC to WETH on base"

Interpreted as:
  Action:   swap_tokens
  Chain:    base
  Token In: USDC
  Token Out: WETH
  Amount:   5
  Slippage Bps: 50

Execute this transaction? [y/N]
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_LLM_API_KEY` | -- | API key (required) |
| `AGENT_LLM_BASE_URL` | `https://api.openai.com/v1` | LLM endpoint URL |
| `AGENT_LLM_MODEL` | `gpt-4o` | Model name |

Any OpenAI-compatible provider works (OpenAI, Anthropic via proxy, Ollama, etc.).

### Error handling

| Scenario | Behavior |
|----------|----------|
| No `AGENT_LLM_API_KEY` | Clear error with setup instructions |
| LLM unreachable | Error with structured syntax suggestion as fallback |
| LLM returns no tool call | Shows what the LLM said and suggests structured syntax |
| LLM returns unknown tool | Error listing available tools |

## Safety Model

`almanak ax` enforces a TTY safety matrix for all write actions (swaps, LP, lending):

| Context | Behavior |
|---------|----------|
| Interactive terminal (TTY) | Simulate, preview, confirm, execute |
| Non-interactive + `--yes` | Simulate, execute (no prompt) |
| Non-interactive without `--yes` | Fails with error |
| `--dry-run` | Simulate only, never submit |

For natural language mode, the interpreted action is **always displayed** regardless of `--yes`. This is the "Safety Always" guarantee -- you always see what the LLM understood before anything executes.

## Mainnet vs Anvil (Local Testing)

`almanak ax` **auto-starts a gateway** if none is running. Use `--network` to control mainnet vs Anvil:

```bash
# Auto-start Anvil gateway, swap on local fork (free, safe)
almanak ax --network anvil swap USDC ETH 100

# Auto-start mainnet gateway (real transactions)
almanak ax swap USDC ETH 100

# Or connect to an already-running gateway (skips auto-start)
almanak ax --gateway-port 50051 swap USDC ETH 100
```

!!! tip "Start with Anvil"
    Always test with `--network anvil` first. Anvil forks mainnet state so
    balances and prices are real, but transactions are local and free.

The `--chain` flag selects which chain to query/execute on (e.g. `arbitrum`, `base`, `ethereum`). The `--network` flag determines whether those chains resolve to mainnet RPCs or local Anvil forks.

**How auto-start works:**

1. `almanak ax` tries to connect to the gateway at `--gateway-host`:`--gateway-port`
2. If no gateway is running, it starts a `ManagedGateway` in a background thread
3. For `--network anvil`, it also starts an Anvil fork for the selected chain
4. The gateway shuts down automatically when the command finishes

## Workflow Example: Prepare Wallet for Strategy Testing

Real-world scenario: your assets are in a WBTC-WETH LP position on Uniswap V3, but you need 10 USDC for a strategy test.

```bash
# Step 1: Check your LP position (auto-starts Anvil gateway)
almanak ax --network anvil lp-info 123456 --chain ethereum

# Step 2: Close the LP position (withdraws WBTC + WETH, collects fees)
almanak ax --network anvil lp-close 123456 --chain ethereum

# Step 3: Swap WBTC proceeds to USDC
almanak ax --network anvil swap WBTC USDC 0.0005 --chain ethereum

# Step 4: Swap WETH proceeds to USDC
almanak ax --network anvil swap WETH USDC 0.01 --chain ethereum

# Step 5: Verify you have enough USDC
almanak ax --network anvil balance USDC --chain ethereum
```

Or with natural language mode (single commands, but one at a time):

```bash
almanak ax --network anvil -n "show me LP position 123456 on ethereum"
almanak ax --network anvil -n "close LP position 123456 on ethereum"
almanak ax --network anvil -n "swap all my WBTC to USDC on ethereum"
almanak ax --network anvil -n "swap all my WETH to USDC on ethereum"
almanak ax --network anvil -n "check my USDC balance on ethereum"
```

Each step shows what will happen and asks for confirmation before executing write actions.

## Structured vs Natural Language

| | Structured | Natural Language |
|---|---|---|
| **Syntax** | `almanak ax swap USDC ETH 100` | `almanak ax -n "swap 100 USDC to ETH"` |
| **LLM required** | No | Yes |
| **Deterministic** | Yes | No (LLM interprets) |
| **Scriptable** | Yes | Not recommended |
| **Latency** | Instant | +1-2s (LLM round-trip) |
| **Best for** | CI/CD, automation, AI agents | Interactive exploration, humans |
