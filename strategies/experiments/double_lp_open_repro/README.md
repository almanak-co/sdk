# double_lp_open_repro — S2 negative-path repro

**Scope**: prove that two `LP_OPEN` intents into the same pool with
`registry_handle=None` will (today) both land on-chain and only fail at
post-tx accounting persistence — producing an orphan NFT.

This is the negative-path companion to
`docs/internal/AccountingStagingMay18.md` §S2 and is sized to drive the
preflight design in `docs/internal/S2-LP-Registry-Preflight-Proposal.md`.

## What this strategy does

1. Iteration 1 — emits `LP_OPEN(pool="WETH/USDC/500", registry_handle=None)`.
2. Iteration 2 — emits the same `LP_OPEN` again, also `registry_handle=None`.
3. Iteration ≥3 — emits `HOLD` forever, waiting for a teardown signal.

`registry_handle` is omitted on purpose; this is the bug.

## Run instructions (managed Anvil, continuous, NOT `--once`)

```bash
cd "$(git rev-parse --show-toplevel)"
set -a; source .env; set +a

# Deterministic deployment_id — used for the separate teardown signal.
# ClassName is the Python class, NOT the @almanak_strategy(name=...) decorator.
ID="repro-s2-$(date +%s)"

# Terminal 1 — runner + managed Anvil + managed Gateway, continuous.
uv run almanak strat run \
  -d strategies/experiments/double_lp_open_repro \
  --network anvil \
  --id "$ID" \
  --gateway-port 50112 \
  --fresh \
  --verbose
```

When you've observed both LP_OPENs in the runner log, fire teardown from a
second terminal:

```bash
# Terminal 2 — separate teardown signal (same shell env).
uv run almanak strat teardown request \
  -s "DoubleLpOpenReproStrategy:$ID" \
  --wait
```

The teardown should walk `acknowledged → started → progress → completed`
and close any `LP_CLOSE`able positions the strategy tracked. Today that's
both NFTs; after the preflight ships it's exactly one.

## Assertions (today — no preflight)

The strategy is launched via `almanak strat run -d strategies/experiments/double_lp_open_repro`,
so `ALMANAK_STRATEGY_FOLDER` anchors the local DB at
`strategies/experiments/double_lp_open_repro/almanak_state.db` (per
`almanak/framework/local_paths.py:local_db_path()`). The registry table
is named `position_registry`.

| # | Assertion | How to check |
|---|---|---|
| 1 | First `LP_OPEN` succeeds and `position_registry` has 1 row | `sqlite3 strategies/experiments/double_lp_open_repro/almanak_state.db "select count(*) from position_registry where deployment_id='DoubleLpOpenReproStrategy:$ID' and accounting_category='lp' and status='open';"` → 1 |
| 2 | Second `LP_OPEN` emitted with `registry_handle IS NULL` | Runner log: `Emitting LP_OPEN #2 (no registry_handle)` |
| 3 | Second `LP_OPEN` lands on-chain — different token ID | Runner log: two distinct `Extracted LP position ID from receipt: <id>` lines |
| 4 | Accounting then fails with `RegistryAutoCollisionError` | Runner log: `Auto-mode registry collision on accounting_category='lp' semantic_grouping_key='arbitrum:0x…'`; `select count(*) from position_registry … status='open'` is still 1 |
| 5 | The runner now has two NFTs on-chain but only one `position_registry` row | Use `almanak ax lp-list --chain arbitrum` (or query the V3 position manager via RPC) → 2 NFTs; `position_registry` count → 1 |

Assertion 5 is the orphan condition. That's the harm the preflight prevents.

## Assertions (after the proposed preflight)

| # | Assertion | How to check |
|---|---|---|
| 1 | First `LP_OPEN` succeeds and `position_registry` has 1 row | unchanged |
| 2 | Second `LP_OPEN` fails BEFORE sign/submit | `ExecutionResult.success == False`, `error_phase == VALIDATION`, message names `semantic_grouping_key` and suggests `registry_handle=` |
| 3 | No second NFT is minted | `almanak ax lp-list --chain arbitrum` → 1 |
| 4 | `position_registry` still has exactly 1 open row | `select count(*) from position_registry … status='open'` → 1 |
| 5 | Pre-execution failure produces no `accounting_events` row for a phantom second open | `select count(*) from accounting_events where deployment_id='DoubleLpOpenReproStrategy:$ID' and event_type='LP_OPEN'` → 1 |

## Cleanup

```bash
# Stop runner (Ctrl-C) and clear the strategy-anchored DB if iterating
rm -f strategies/experiments/double_lp_open_repro/almanak_state.db
```

## Why this directory

- Not a demo (would land in `almanak/demo_strategies/`).
- Not a fixture (would land in `strategies/accounting/`).
- It is an internal repro for an open bug, so it lives under
  `strategies/experiments/`.

The strategy does not need to be portable across chains, does not need a
default-chain matrix, and is intentionally **excluded from**:

- `make test-demo-quick` / `make test-demo-strategies` (not a demo).
- `.github/sidecar-demos.yml` (not a demo).
- `scripts/ci/check_connector_registry.py` (no new connector).
