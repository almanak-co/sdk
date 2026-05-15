# Accounting matrix row configs

Per-row `config.json` overrides for `scripts/qa/run_accounting_matrix.py`.
One file per matrix row that needs to deviate from the algo folder's
default config.

## How it fits together

```
strategies/accounting/<algo>/              ← protocol- and chain-agnostic algo
  strategy.py                              ← the only Python in the loop
  config.json                              ← the canonical/default row

tests/fixtures/accounting/matrix-configs/  ← per-row config overrides (this dir)
  <row_id>.json                            ← matches scripts/qa/accounting-matrix.yml row id

scripts/qa/accounting-matrix.yml           ← matrix
  rows:
    - id: <row_id>
      fixture: <algo-folder>               ← strategies/accounting/<this>
      class_name: <strategy class>
      config: tests/fixtures/.../<row_id>.json   ← optional override (this dir)
```

The matrix runner copies the fixture folder to a temp dir, then — if the
row defines a `config:` path — overwrites the copied `config.json` with
the override. Rows that don't deviate from the algo's default can omit
the `config:` field entirely.

## File naming

`<row_id>.json`, where `<row_id>` is the YAML row id verbatim. Examples:

- `lp-uniswap_v3-ethereum.json` — drives `lp/` algo on Ethereum mainnet
- `lp_pancakeswap-arbitrum.json` — drives `lp/` algo with PancakeSwap V3 on Arbitrum
- `lp_pancakeswap_dual-arbitrum.json` — drives `lp_dual/` algo with PancakeSwap V3

## Required fields per algo

| Algo | Required keys |
|---|---|
| `lp` | chain, protocol, pool, starting_asset, total_value_usd, swap_split_pct, range_width_pct, max_slippage, anvil_funding |
| `lp_dual` | chain, protocol, pool, starting_asset, total_value_usd, swap_split_pct, lp1_range_width_pct, lp2_range_width_pct, lp_capital_split_pct, max_slippage, anvil_funding |
| `lp_triple` | (TBD — Phase 2 follow-up) |
| `looping` | (TBD — Phase 3) |
| `loop_lp_*` | (TBD — Phase 4) |

Exceptions (own algo folder, own default config, no override needed):
`lp_aerodrome`, `lp_aerodrome_dual`, `lp_curve` — protocol-specific
lifecycles (integer-tick math, dual swap/LP keys, stableswap mechanics).
