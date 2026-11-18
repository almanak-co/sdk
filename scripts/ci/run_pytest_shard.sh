#!/usr/bin/env bash
# Run one shard of the offline suite, optionally under the CRAP coverage
# configuration.
#
# Arguments: total shard count, this shard's 1-based index, and an optional
# coverage switch ("true"/"false", default "true"). With coverage on the shard
# records .coverage.shard-<K> and coverage.shard-<K>.sources.json so the
# aggregator can combine the data for the CRAP gate; every shard uses the same
# include list, written by check_crap.py, so the combined data scores exactly
# the files a single run would measure. With coverage off only
# test-results-<K>.xml and pytest-<K>.log are produced; the PR CRAP gate is the
# only coverage consumer, so runs that never score CRAP (main, releases may
# choose) skip the tracing overhead.
set -euo pipefail

SPLITS="${1:?total shard count}"
GROUP="${2:?shard index}"
COVERAGE="${3:-true}"

CONFIG="shard-${GROUP}.coveragerc"
DATA=".coverage.shard-${GROUP}"
MANIFEST="coverage.shard-${GROUP}.sources.json"
rm -f "$CONFIG" "$DATA" "$MANIFEST" "test-results-${GROUP}.xml" "pytest-${GROUP}.log"

COV_ARGS=()
if [ "$COVERAGE" = "true" ]; then
  uv run --locked python scripts/check_crap.py --write-coveragerc "$CONFIG" --data-file "$DATA"
  COV_ARGS=(--cov "--cov-config=${CONFIG}" --cov-report=)
fi

set +e
# .test_durations is committed so every shard partitions identically; tests
# without a recorded duration get the average, so drift only costs balance.
# Refresh it from a coverage-on run's test-durations artifact (see
# scripts/ci/merge_test_durations.py) so the weights match hosted conditions.
uv run --locked pytest -n auto -p no:cacheprovider \
  --splits "$SPLITS" --group "$GROUP" \
  --splitting-algorithm least_duration --durations-path .test_durations \
  --skip-missing-evidence -rfEs \
  "${COV_ARGS[@]}" \
  --junitxml="test-results-${GROUP}.xml" 2>&1 | tee "pytest-${GROUP}.log"
STATUS=${PIPESTATUS[0]}
set -e

test -f "test-results-${GROUP}.xml" || { echo "::error::shard ${GROUP} produced no junit"; exit 1; }
if [ "$COVERAGE" = "true" ]; then
  test -f "$DATA" || { echo "::error::shard ${GROUP} produced no coverage data"; exit 1; }
  # Hits only count for the sources they were collected from.
  uv run --locked python scripts/check_crap.py --verify-sources "$MANIFEST"
fi
exit "$STATUS"
