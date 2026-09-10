# Captured dual-LP quantity evidence

`captured.json` exports the six economic ledger rows and 51 raw receipt,
transaction and balance witnesses for the 17 transactions in supervised diagnostic
`20260907-lp-dual-calibration-004`. The file records its original execution commit
and wallet. Selected ledger columns are copied unchanged; witness objects are
retained from the independent observer. No private keys or RPC URLs are included.

The subject bought WETH, opened two LP generations, closed both through separate
teardown, and sold its remaining WETH. No stimulus actor ran and no rebalance
occurred. This is not an admitted Quant, sustained-run or E2E qualification fixture.
Tests reconstruct a small read-only-input SQLite ledger from these exact rows and
then mutate copies to challenge the quantity predicate.

`lifecycle.json` retains the same run's ledger IDs, cycle IDs and timestamps, plus
the selected persisted teardown request fields. It adds phase attribution evidence
without changing the captured quantities or claiming a rebalance occurred.
