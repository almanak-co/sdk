# Almanak Code

Almanak Code is Almanak's AI coding assistant. Every published wheel carries the
binary for its own platform, so it is available as soon as the SDK is installed.

## Running it

Running `almanak` with no command starts the assistant:

```bash
almanak
```

Any command this CLI does not define is passed to it unchanged, so its own
options work directly:

```bash
almanak code -p "supply WETH to Aave on Ethereum"
```

A command the SDK does define always belongs to the SDK, and so does anything
starting with an option:

```bash
almanak check strategies/aave_v4_weth_supply   # SDK
almanak --help                                 # SDK help
almanak --unknown-option                       # SDK error, exit 2
```

## The Python LSP

The assistant drives `pyright-langserver` from `PATH` and falls back to `npx`
when it is absent. To install it alongside the SDK:

```bash
uv add "almanak[code]"
```

## Platform coverage

Wheels are published for macOS on arm64 and x86_64, Linux on arm64 and x86_64,
and Windows on x86_64. Installing from the source distribution, or on any other
platform, gives a working SDK with no assistant; `almanak` then reports
`almanak_code_unavailable` and exits 1.

The pinned release is recorded in `.almanak-code-version`. Nothing is downloaded
at install time or at run time.

## The SDK v3 authoring lane

Forwarding sets `ALMANAK_SDK_GENERATION=v3` and `ALMANAK_SDK_V3_PYTHON` to the
running interpreter unless they are already set, so the assistant works in its
SDK v3 lane: its prompts, sub-agents and strategy workflow target a
`strategy.py` plus one `config.yaml`, validated with `almanak check --json`, and
its `sdk-docs`/`sdk-src` references resolve to this installed package. Worked
examples are listed with `almanak support --references`.

Inside an assistant session the SDK parser always wins, so a command line this
CLI does not define, such as a V2 `almanak strat new`, reports the commands it
does define rather than reopening the assistant.

`almanak ax` exists in both generations with different meanings: V2's `ax`
runs DeFi actions such as swaps, while this SDK's `ax` is read-only protocol
observations (`pool`, `pool-price`, `lending-reserve`, `lending-position`,
`perp-position`, `perp-markets`, `balance`), read through `--env-file` or a
`service read-gateway` sidecar, plus `ax supply` and `ax withdraw`, which preview
or execute one [operator action](operating.md#operator-actions). V2 `ax` command
lines, such as `almanak ax swap`, are rejected by the SDK parser.
