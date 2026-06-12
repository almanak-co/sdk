# Backtest Trust Matrix (VIB-5081)

The backtesting analog of the Accountant Test
(`almanak/framework/accounting/accountant_test.py`): a scored matrix of
conservation and math invariants - rows are the invariants from
`docs/internal/blueprints/31-backtesting.md` section 4.3 plus closed-form
math checks, columns are strategy types (swap/TA, LP, lending, perp).

Every cell exercises the REAL engine, portfolio, and adapter code. Nothing
under test is mocked; synthetic price providers replace only the data feed.
This exists because the 9-phase manual trust protocol
(`docs/internal/notes/backtesting/Backtesting-TrustTest.md`) was executed
once in February 2026 and never automated - which is how the VIB-5082
conservation bug survived under an "L3 certified" banner.

## Two tiers

| Tier | When | Command | Needs |
|---|---|---|---|
| Network-free (the matrix) | every PR | `uv run pytest tests/validation/backtesting -m "not validation"` | nothing - no keys, no network |
| Keyed (`validation` marker) | nightly | `uv run pytest tests/validation/backtesting -m validation` | `COINGECKO_API_KEY` (keyed tests skip cleanly without it) |

The network-free tier lives in `test_trust_matrix.py` (cells registered in
`trust_matrix.py`). The keyed tier is the historical-accuracy benchmarks
(`test_accuracy_benchmarks.py`, `test_{lp,lending,perp}_historical_accuracy.py`)
plus the trust-protocol Phase 3/4 checks (`test_keyed_data_integrity.py`:
CoinGecko reference-price integrity, fixed-seed reproducibility).

## The scoreboard

The conftest aggregates every `@pytest.mark.trust_cell` outcome and prints
the matrix at the end of the run, so CI logs always carry the current state.
Set `TRUST_MATRIX_JSON=/path/out.json` to also write the JSON artifact.

## The rule (blueprint 31 section 9)

**Every backtesting PR must move this matrix forward on the affected
surface, or explain why it cannot.**

- A PASS -> FAIL transition is a stop-the-line event: the change lost a
  conservation property. Revert or escalate; do not adjust the test.
- Known-bug cells are `xfail(strict=True)` with the tracking reference in
  the reason string. NEVER weaken an assertion to make a cell pass - the
  assertion is the spec; xfail documents the gap. Fixing the bug flips the
  strict xfail to XPASS (a hard failure), forcing the fixing PR to remove
  the marker and claim the cell.
- New invariants or strategy columns register in `trust_matrix.py`; the
  meta-test in `test_trust_matrix.py` enforces registry/test lockstep.

## Conventions

- Conservation assertions are Decimal-exact wherever the math permits
  (zero-cost paths). LP adapter cells allow a relative dust bound of 1e-9
  for Decimal sqrt round-trips - numeric dust only, never economic
  tolerance.
- Engine timing: an intent returned by `decide()` at tick T executes at
  tick T+1, even with `inclusion_delay_blocks=0`. Closed forms in the cells
  account for this.
