# Quant rejection fixtures — provenance

Minimal, committable subsets of two real Quant User evidence bundles from
2026-08-17. They exist to pin the Quant admission ladder in
`tests/unit/scripts/test_qa_coverage.py`:

| step | transformation applied by the test | rejected at |
|---|---|---|
| (a) | none — the bundle exactly as committed here | `scripts/qa/quant_admission.py` `validate_claim_scope` |
| (b) | `claim_scope` grafted onto `lifecycle-contract.json` | `scripts/qa/quant_admission.py` `_load_decision` |
| (c) | shape-complete forgery: contract rewritten, guards dropped, coverage + receipts + `audit-decision.json` authored and mutually digest-bound | **nothing** — VIB-6712, currently `xfail(strict=True)` |

Steps (a) and (b) are regression pins on commit `85eccd216`. Step (c) is the
rung that tests the property this fixture set exists to establish: it is
admitted today as an OFFICIAL mainnet PASS, so its test is `xfail(strict=True)`
and will fail loudly the day an independent Strategy derivation lands.

## Live ledger provenance

Both rows are in `~/.almanak/qa/index/experiment_runs.jsonl` and are already
invalidated there by the `qa-framework-v1-ledger-correction` sweep of
2026-08-17T16:16:45Z.

| seq | run_id | `record_sha256` | admission | cell verdict |
|---:|---|---|---|---|
| 30 | `20260817-0218-aave-supply-base` | `9247e1245cfdacf44992d72961b64aed6dcec8098ac5d1efa9eb1eeb3b58cd00` | `null` | `lending.aave_v3.base.simple.mainnet.eoa` = `UNVERIFIED` |
| 32 | `20260817-0347-looping-arb-rerun` | `d572961e0a4dab5cfa0ce17ed16c87799c56ec9467abe9bbd0e69725dbfb93f5` | `null` | `lending.aave_v3.arbitrum.complex.mainnet.eoa` = `FAIL` |

Both were sealed by SDK `2.25.1` at commit `1a62591298c4b009aafba6190de440a003196cc5`.

## What was and was not committed

The live bundles are 2.1 MB and 1.0 MB and contain a live `db.sqlite`, a
mode-600 `command-journal.jsonl`, and a `preflight-quarantine/` tree. None of
that is here. Only the files needed to reproduce the identical rejection reason
were kept:

- `20260817-0218-aave-supply-base`: `lifecycle-contract.json`, `finding.json`,
  `git.json`, `receipt-reconciliation.json`, `card.md` (31,858 bytes).
  It has **no** `lifecycle-coverage.json` at all — a second, independent reason
  it could never seal.
- `20260817-0347-looping-arb-rerun`: the same plus `lifecycle-coverage.json`
  (11,135 bytes). That coverage records `runtime_target_intents_executed: 1` of
  `5`; the step (c) forgery nonetheless turns it into an OFFICIAL mainnet PASS.

`manifest.json` is deliberately absent: a bundle does not carry the sealer's own
output, and including it would change the evidence-set digest.
