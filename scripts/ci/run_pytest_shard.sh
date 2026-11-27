#!/usr/bin/env bash
# Run one shard of the offline suite.
#
# Arguments: total shard count and this shard's 1-based index. Produces
# test-results-<K>.xml and pytest-<K>.log.
set -euo pipefail

SPLITS="${1:?total shard count}"
GROUP="${2:?shard index}"

rm -f "test-results-${GROUP}.xml" "pytest-${GROUP}.log"

set +e
# .test_durations is committed so every shard partitions identically; tests
# without a recorded duration get the average, so drift only costs balance.
# Refresh it from a run's test-durations artifact (see
# scripts/ci/merge_test_durations.py) so the weights match hosted conditions.
uv run --locked pytest -n auto -p no:cacheprovider \
  --splits "$SPLITS" --group "$GROUP" \
  --splitting-algorithm least_duration --durations-path .test_durations \
  --skip-missing-evidence -rfEs \
  --junitxml="test-results-${GROUP}.xml" 2>&1 | tee "pytest-${GROUP}.log"
STATUS=${PIPESTATUS[0]}
set -e

test -f "test-results-${GROUP}.xml" || { echo "::error::shard ${GROUP} produced no junit"; exit 1; }
exit "$STATUS"
