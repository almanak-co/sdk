# Bug #2130 — local-mode acceptance test fixtures

Minimal, deterministic fixtures backing
`tests/integration/state/test_bug_2130_local.py` (VIB-4199 / T13).

## Provenance

Every numeric value here traces to the original GitHub issue body
(`almanak-co/almanak-sdk-private` issue #2130, opened 2026-05-05). The
fixture data is intentionally a *recipe*, NOT a captured Anvil round-trip
— the test pins the **structural** recoverability claim through the
SQLite-backed StateManager, which is sufficient to demonstrate that the
bug class is impossible after T11 (atomic primitive) + T12 (UniV3 cutover)
+ T14 (typed collision exception). A future ticket (T20 / VIB-4206) ships
the hosted-Postgres equivalent, and the optional Anvil round-trip lives
in the strategy-tester harness rather than this unit/integration test.

## Files

- `lp_open.json` — LP_OPEN registry-row payload mirroring issue #2130's
  on-chain landing: `token_id = 5468420`, Arbitrum WETH/USDC ultra-tight
  pool, Uniswap V3, fee tier 500 (0.05%).
- `lp_close.json` — LP_CLOSE registry-row payload (same `physical_identity_hash`,
  `status='closed'`, populated `closed_tx` / `closed_at_block`).
- `deployment.json` — deployment + chain identity scalars (deployment_id,
  chain, accounting_category, semantic_grouping_key, NPM address).

## What this fixture is NOT

- Not a real Anvil receipt. The fixture's identity scalars are
  hand-derived to be **deterministic** so the test runs on a vanilla CI
  runner with SQLite + Python 3.12+ alone, no fork RPC.
- Not a parser test. The receipt-parser path is exercised in
  `tests/fixtures/multi-position-tracking/univ3-arbitrum/` and the L1
  golden tests under `tests/accounting/L1/`. This fixture is solely the
  **runtime registry-row shape** for the bug-class structural test.
