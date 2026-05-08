#!/usr/bin/env bash
# Stage 6.5 fixture-based test driver.
#
# Usage:
#   ./tests/fixtures/drift-check/run.sh <case-name>
#   ./tests/fixtures/drift-check/run.sh --all
#
# Each case lives at tests/fixtures/drift-check/<case-name>/ and contains:
#   - pr_body.md, commits.txt, git_log_stat.txt   (REQUIRED)
#   - linear_mock.json, gh_issue_mock.json        (optional)
#   - expected_verdict                            (REQUIRED, e.g. "SHIP")
#   - expected_recommendation_contains            (optional, substring)
#   - head_sha_at_start.txt, current_head_sha.txt (optional, for STALE_HEAD)
#   - .actual/                                    (gitignored, written by run.sh)

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/_stage65.sh"

run_case() {
    local case_name="$1"
    local fixture="$HERE/$case_name"
    if [ ! -d "$fixture" ]; then
        echo "FAIL: fixture not found: $fixture" >&2
        return 1
    fi

    local actual_dir="$fixture/.actual"
    mkdir -p "$actual_dir"
    rm -f "$actual_dir/verdict" "$actual_dir/log" "$actual_dir/recommendation"

    # Run the gate; capture stdout (verdict + recommendation) + stderr (log).
    local stdout_file="$actual_dir/stdout.txt"
    local stderr_file="$actual_dir/log"
    pr_merger_stage_65_check "$fixture" >"$stdout_file" 2>"$stderr_file" || {
        local rc=$?
        echo "FAIL [$case_name]: pr_merger_stage_65_check exited $rc" >&2
        cat "$stderr_file" >&2
        return 1
    }

    # Extract verdict + recommendation. Fail fast if either input is missing
    # — empty strings on both sides would otherwise spuriously pass.
    if ! grep -m1 '^Verdict:' "$stdout_file" > "$actual_dir/verdict.raw"; then
        echo "FAIL [$case_name]: missing 'Verdict:' line in stage output" >&2
        echo "  stdout:" >&2; sed 's/^/    /' "$stdout_file" >&2
        return 1
    fi
    sed 's/^Verdict:[[:space:]]*//' "$actual_dir/verdict.raw" > "$actual_dir/verdict"
    sed -n '/^## Recommendation/,$p' "$stdout_file" > "$actual_dir/recommendation"

    local actual_verdict expected_verdict
    if [ ! -s "$fixture/expected_verdict" ]; then
        echo "FAIL [$case_name]: missing or empty expected_verdict" >&2
        return 1
    fi
    actual_verdict=$(tr -d '[:space:]' < "$actual_dir/verdict")
    expected_verdict=$(tr -d '[:space:]' < "$fixture/expected_verdict")

    if [ "$actual_verdict" != "$expected_verdict" ]; then
        echo "FAIL [$case_name]: verdict mismatch — expected '$expected_verdict', got '$actual_verdict'" >&2
        echo "  log:" >&2; sed 's/^/    /' "$stderr_file" >&2
        return 1
    fi

    # Optional: substring check on recommendation.
    if [ -f "$fixture/expected_recommendation_contains" ]; then
        local expected_sub
        expected_sub=$(cat "$fixture/expected_recommendation_contains")
        if [ -n "$expected_sub" ] && ! grep -qF "$expected_sub" "$actual_dir/recommendation"; then
            echo "FAIL [$case_name]: recommendation missing expected substring" >&2
            echo "  expected: $expected_sub" >&2
            echo "  actual:" >&2; sed 's/^/    /' "$actual_dir/recommendation" >&2
            return 1
        fi
    fi

    # Optional: substring check on log (e.g., "Linear unreachable" warnings).
    if [ -f "$fixture/expected_log_contains" ]; then
        local expected_log_sub
        expected_log_sub=$(cat "$fixture/expected_log_contains")
        if [ -n "$expected_log_sub" ] && ! grep -qF "$expected_log_sub" "$stderr_file"; then
            echo "FAIL [$case_name]: log missing expected substring" >&2
            echo "  expected: $expected_log_sub" >&2
            echo "  actual log:" >&2; sed 's/^/    /' "$stderr_file" >&2
            return 1
        fi
    fi

    echo "PASS [$case_name]: $actual_verdict"
    return 0
}

run_all() {
    local fail=0 pass=0 discovered=0 case_dir
    for case_dir in "$HERE"/case-*; do
        [ -d "$case_dir" ] || continue
        discovered=$((discovered + 1))
        local case_name
        case_name=$(basename "$case_dir")
        if run_case "$case_name"; then
            pass=$((pass + 1))
        else
            fail=$((fail + 1))
        fi
    done
    if [ "$discovered" -eq 0 ]; then
        echo "FAIL: no fixtures found under $HERE (expected case-* directories)" >&2
        return 1
    fi
    echo
    echo "Stage 6.5 fixture summary: $pass passed, $fail failed"
    [ "$fail" -eq 0 ]
}

main() {
    if [ "${1:-}" = "--all" ]; then
        run_all
    else
        run_case "${1:?case name required, or use --all}"
    fi
}

main "$@"
