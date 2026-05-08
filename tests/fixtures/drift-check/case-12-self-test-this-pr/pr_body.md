# feat(skills): /pr-merger Stage 6.5 step-back drift check (VIB-4141)

## Summary

Adds Stage 6.5: Step-Back / Drift Check to /pr-merger.

## Architecture

- `.claude/commands/pr-merger.md` Stage 6.5 — protocol prose
- `tests/fixtures/drift-check/_stage65.sh` — sourceable Bash function
- `tests/fixtures/drift-check/run.sh` — fixture-based test driver
- 16 fixture cases cover all 5 verdicts + auditor-lies override + edge cases

## UAT Gate Status: skipped — bootstrapping

Reason: this PR ships the very gate Phase 4/5 would run.

## Test plan

- [x] make lint passes
- [x] tests/fixtures/drift-check/run.sh --all returns all PASS
- [x] CI green
