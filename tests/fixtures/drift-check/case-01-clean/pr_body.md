# feat(skills): add Stage 6.5 drift check to /pr-merger

## Summary

Adds a holistic drift-check stage to `/pr-merger` that runs after deletion check and before merge. Catches scope drift, quick patches, untracked follow-ups, and production risks via meta-history reasoning.

## Test plan

- [x] make lint
- [x] tests/fixtures/drift-check/run.sh --all
