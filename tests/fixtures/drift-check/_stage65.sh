#!/usr/bin/env bash
# Stage 6.5 drift-check logic — sourceable, testable.
#
# Used both by `.claude/commands/pr-merger.md` Stage 6.5 in production and by
# `tests/fixtures/drift-check/run.sh` in CI.
#
# The function is pure: same inputs, same outputs. No live network unless
# LINEAR_MOCK_PATH / GH_ISSUE_MOCK_PATH are unset (production path).

# Use pipefail only — `set -u` would propagate to a sourcing shell and
# break callers on benign unbound-var refs. The function uses `${var:-}`
# defaults internally instead.
set -o pipefail

# ---------------------------------------------------------------------------
# Constants — kept inline so the function is self-contained.
# ---------------------------------------------------------------------------

# Quick-patch markers in commit messages (case-insensitive). Each match
# overrides an auditor SHIP to QUICK_PATCH_DETECTED.
readonly STAGE65_QUICK_PATCH_REGEX='\b(silence|silenced|xfail|skip(ped)?[[:space:]]+flaky?|noqa|type:[[:space:]]*ignore|TODO[[:space:]]+later|HACK)\b'

# Cleanup verbs in commit subjects — these REMOVE quick-patch markers
# (positive refactors) and must NOT trigger QUICK_PATCH_DETECTED. Allows an
# optional conventional-commit prefix (`refactor:`, `chore(scope):`, etc.).
readonly STAGE65_CLEANUP_VERB_REGEX='^([a-z]+(\([^)]+\))?:[[:space:]]+)?(revert|remove|removes|removed|delete|deletes|deleted|replace|replaces|replaced|drop|drops|dropped|undo|undoes|undone|rip[[:space:]]+out|unsilence|unskip|unxfail)\b'

# Money-path file patterns. When any of these appear in the changed-files list AND
# there's a risk pattern in the diff, override to PRODUCTION_RISK.
readonly STAGE65_MONEY_PATH_REGEX='(framework/execution|framework/accounting|framework/connectors/[^/]+/sdk\.py|framework/state)'

# Risk patterns that, paired with a money-path file, indicate production risk.
# These mirror CLAUDE.md "Hard Architecture Rules" silent-error class.
readonly STAGE65_RISK_HINT_REGEX='(return[[:space:]]+Decimal\("0"\)|except[[:space:]]+Exception|except[[:space:]]*:|^\+?[[:space:]]+pass[[:space:]]*$|TODO|FIXME|hardcoded|placeholder|silently[[:space:]]+swallow)'

# Placeholder Linear ticket title patterns (case-insensitive, anchored at start).
readonly STAGE65_PLACEHOLDER_TITLE_REGEX='^(Track all|Misc|Miscellaneous|Followups for|Follow-ups for|TBD|Generic|All follow-ups)\b'

# Closed Linear states (case-insensitive). Includes the four canonical closed
# states plus common Linear workspace renames.
readonly STAGE65_CLOSED_STATES_REGEX='^(done|cancelled|canceled|duplicate|completed|complete|won.?t do|wont do|will not do|released|shipped|archived|merged|closed|abandoned)$'

# H2 headers that introduce a deferred-followup section in PR body. Match ANY
# header containing one of these keywords (case-insensitive) — the closed
# allowlist was a loophole; renaming the section bypassed the gate.
# Each keyword is matched as a STANDALONE WORD (delimited by whitespace,
# `-`, `_`, or end-of-string) to avoid false-positives like
# `## Future-Proofing Considerations`. Literal dots are escaped.
readonly STAGE65_FOLLOWUP_KEYWORDS='(^|[[:space:]_-])(deferred?|follow[[:space:]_-]?ups?|future[[:space:]_-]?work|todo|to[[:space:]_-]?do|backlog|out[[:space:]_-]of[[:space:]_-]scope|next[[:space:]_-]?steps?|known[[:space:]_-]limitations?|tech[[:space:]_-]debt|later[[:space:]_-]?work|punted?|won.t[[:space:]_-]?fix)([[:space:]_-]|$)'

# ---------------------------------------------------------------------------
# Logging helpers (stderr).
# ---------------------------------------------------------------------------

stage65_log() { echo "$@" >&2; }
stage65_warn() { echo "WARN: $*" >&2; }
stage65_error() { echo "ERROR: $*" >&2; }

# ---------------------------------------------------------------------------
# Input validation.
# ---------------------------------------------------------------------------

stage65_require_file() {
    local label="$1" path="$2"
    [ -f "$path" ] || { stage65_error "Stage 6.5 input missing: $label ($path)"; return 2; }
}

# ---------------------------------------------------------------------------
# Mechanical detectors.
# Each returns 0 with a non-empty stdout when a blocker is found, 1 otherwise.
# ---------------------------------------------------------------------------

stage65_detect_quick_patch() {
    local commits="$1"
    # Two-pass detection (fail-closed on adversarial bypass):
    #
    # 1. Find every line matching the QUICK_PATCH regex (could be a real
    #    band-aid OR a cleanup of one — we don't know yet).
    # 2. For each, check if it ALSO matches the CLEANUP_VERB regex AND does
    #    NOT have a "back" / "instead" / "swap" / "and add" suffix that
    #    indicates the cleanup is incomplete (e.g.,
    #    `refactor: drop xfail and add noqa back` should still trigger).
    # 3. A line that's purely cleanup is suppressed; everything else stays.
    local match
    match=$(grep -Ei "$STAGE65_QUICK_PATCH_REGEX" "$commits" | while IFS= read -r line; do
        if echo "$line" | grep -Eqi "$STAGE65_CLEANUP_VERB_REGEX" \
            && ! echo "$line" | grep -Eqi '\b(back|instead|swap[[:space:]]+in|and[[:space:]]+add|after[[:space:]]+adding)\b'
        then
            continue  # purely cleanup — suppress
        fi
        printf '%s\n' "$line"
    done | head -3)
    if [ -n "$match" ]; then
        echo "$match"
        return 0
    fi
    return 1
}

stage65_detect_production_risk() {
    local changed_files="$1"
    local money_paths
    money_paths=$(grep -Eo "$STAGE65_MONEY_PATH_REGEX" "$changed_files" | sort -u | head -5 || true)
    if [ -z "$money_paths" ]; then
        return 1
    fi
    # Money-path file touched. Look for risk hints in risk_hints.txt (populated
    # by the production protocol from `git diff`, or by fixtures directly).
    local hints_file="${changed_files%/*}/risk_hints.txt"
    if [ -f "$hints_file" ]; then
        local hint
        hint=$(grep -Ei "$STAGE65_RISK_HINT_REGEX" "$hints_file" | head -3 || true)
        if [ -n "$hint" ]; then
            echo "money-path: $money_paths"
            echo "risk: $hint"
            return 0
        fi
    fi
    return 1
}

# Tokenize PR title into noun-phrase scope tokens.
# Strips conventional-commit prefixes (`feat:`, `fix(scope):`, etc.) and
# splits on common separators. Lowercased.
stage65_title_tokens() {
    local title="$1"
    # Capture conventional-commit scope inside parens and append it as tokens
    # (so `feat(skills): ...` keeps `skills` as a meaningful scope token).
    local scope
    scope=$(echo "$title" | sed -nE 's/^[a-z]+\(([^)]+)\):.*/\1/Ip')
    # Strip the type+scope+colon prefix from the title body.
    title=$(echo "$title" | sed -E 's/^[a-z]+(\([^)]*\))?:[[:space:]]*//I')
    title="$title $scope"
    # Strip residual punctuation (parens, brackets, braces, colons, semicolons)
    # before splitting — otherwise `4141)` survives as a token.
    title=$(echo "$title" | tr -d '()[]{};:!?"`')
    # Lowercase + split on whitespace, comma, slash, plus, hyphen, dot.
    # Note: `-` is at the end of the character class (no escape) for BSD/GNU portability.
    # Length filter ≥2 keeps domain tokens like `lp`, `ax`, `ts`.
    echo "$title" | tr '[:upper:]' '[:lower:]' | tr -s '[:space:],/+. -' '\n' \
        | grep -Ev '^(the|a|an|and|or|of|to|for|in|on|with|by|from|at|be|is|are|was|were|been|being|via)$' \
        | grep -Ev '^[[:space:]]*$' \
        | grep -Ev '^[0-9]+$' \
        | awk 'length($0) >= 2' || true
}

# Every changed file path. The protocol writes one path per line (from
# `git diff --name-only`); legacy fixtures may include `git log --stat`
# format with a trailing `| N changes` separator — we strip that.
# Accepts files with extension (`x.py`), nested paths (`a/b/c`), and root
# files without extension (`Dockerfile`, `Makefile`).
stage65_changed_files() {
    local path_list="$1"
    sed -E 's/[[:space:]]+\|.*$//; s/^[[:space:]]+//' "$path_list" \
        | grep -E '^[A-Za-z0-9._/-]+\.[A-Za-z0-9_-]+$|^[A-Za-z0-9._/-]+/[A-Za-z0-9._-]+$|^[A-Za-z0-9._-]+$' \
        | sort -u || true
}

stage65_detect_scope_drift() {
    local pr_title="$1" git_log_stat="$2"
    local title_tokens changed_files
    title_tokens=$(stage65_title_tokens "$pr_title")
    changed_files=$(stage65_changed_files "$git_log_stat")
    if [ -z "$title_tokens" ] || [ -z "$changed_files" ]; then
        return 1
    fi
    # Support paths are auto-aligned (any feature can have tests/docs/notes).
    # Feature paths are the ones that must match the title scope.
    local feature_total=0 feature_matched=0
    while IFS= read -r file; do
        [ -z "$file" ] && continue
        local file_lower
        file_lower=$(echo "$file" | tr '[:upper:]' '[:lower:]')
        # Auto-aligned: tests/, docs/, docs/internal/notes/, .gitignore, *.lock, fixtures.
        if echo "$file_lower" | grep -Eq '^(tests/|docs/|docs/internal/notes/|\.gitignore$|.*\.lock$)'; then
            continue
        fi
        feature_total=$((feature_total + 1))
        # File matches if ANY title token appears as a substring of the path.
        while IFS= read -r tok; do
            [ -z "$tok" ] && continue
            if echo "$file_lower" | grep -qF "$tok"; then
                feature_matched=$((feature_matched + 1)); break
            fi
        done <<< "$title_tokens"
    done <<< "$changed_files"
    # If no feature files at all, no drift to flag (PR is pure tests/docs).
    if [ "$feature_total" -eq 0 ]; then
        return 1
    fi
    local threshold=$(( (feature_total + 1) / 2 ))
    if [ "$feature_matched" -lt "$threshold" ]; then
        echo "title-tokens: $(echo "$title_tokens" | tr '\n' ' ')"
        echo "feature-files: $feature_total (excluding tests/, docs/, docs/internal/notes/)"
        echo "matched: $feature_matched / $feature_total (threshold $threshold)"
        return 0
    fi
    return 1
}

# Extract deferred-followup bullets from PR body. Each output line is one
# bullet's full text (without the leading `-` or `*`).
#
# Header detection is keyword-based (any H2 matching the FOLLOWUP_KEYWORDS
# regex), not a closed allowlist — synonyms like `## Future Work`, `## TODO`,
# `## Backlog`, `## Tech Debt` etc. all qualify. The previous closed-list
# approach was bypassable by renaming the section.
stage65_extract_followup_bullets() {
    local pr_body="$1"
    awk -v kw_re="$STAGE65_FOLLOWUP_KEYWORDS" '
        BEGIN { in_section = 0 }
        /^##[[:space:]]+/ {
            line_lower = tolower($0)
            if (line_lower ~ kw_re) { in_section = 1; next }
            in_section = 0
            next
        }
        in_section && /^[[:space:]]*[-*][[:space:]]+/ {
            sub(/^[[:space:]]*[-*][[:space:]]+/, "")
            print
        }
    ' "$pr_body"
}

# Resolve a Linear ticket. Three input paths:
#   1. LINEAR_RESOLVED_PATH — pre-resolved by the protocol caller. Format
#      is one ticket per line: `<id>|<state>|<title>`. Production path.
#   2. LINEAR_MOCK_PATH — JSON map for fixture tests.
#   3. LINEAR_UNREACHABLE=1 — simulate API down.
# Outputs `<state>|<title>` on success or empty on failure.
stage65_resolve_linear() {
    local ticket="$1"
    if [ -n "${LINEAR_RESOLVED_PATH:-}" ] && [ -f "$LINEAR_RESOLVED_PATH" ]; then
        local line
        line=$(grep -E "^${ticket}\|" "$LINEAR_RESOLVED_PATH" | head -1 || true)
        if [ -n "$line" ]; then
            # `<id>|<state>|<title>` → strip the leading `<id>|`.
            echo "$line" | cut -d'|' -f2-
        fi
        return 0
    fi
    if [ -n "${LINEAR_MOCK_PATH:-}" ] && [ -f "$LINEAR_MOCK_PATH" ]; then
        jq -r --arg t "$ticket" '
            if .[$t] then "\(.[$t].state)|\(.[$t].title)" else empty end
        ' "$LINEAR_MOCK_PATH"
        return 0
    fi
    if [ -n "${LINEAR_UNREACHABLE:-}" ]; then
        # Warn once per run so degraded-mode use is visible in logs.
        if [ -z "${STAGE65_LINEAR_UNREACHABLE_WARNED:-}" ]; then
            stage65_warn "Linear unreachable; falling back to GitHub issue verification when bullets carry a /issues/ URL"
            STAGE65_LINEAR_UNREACHABLE_WARNED=1
        fi
        return 0
    fi
    # Live path unconfigured — treat as unreachable (the caller's UNTRACKED
    # path handles this, blocking PRs that cite tickets we can't verify).
    # Loud-warn so production callers know they forgot to pre-resolve.
    stage65_warn "Linear lookup not pre-resolved (set LINEAR_RESOLVED_PATH or LINEAR_MOCK_PATH); ticket $ticket treated as unresolvable"
    return 0
}

# Resolve a GitHub issue via mock or live `gh issue view`.
# Mock format: { "<owner>/<repo>#<n>": { "state": "open|closed" } }
stage65_resolve_gh_issue() {
    local url="$1"
    local key
    key=$(echo "$url" | sed -E 's|https?://github\.com/([^/]+)/([^/]+)/issues/([0-9]+).*|\1/\2#\3|')
    if [ -n "${GH_ISSUE_MOCK_PATH:-}" ] && [ -f "$GH_ISSUE_MOCK_PATH" ]; then
        jq -r --arg k "$key" '
            if .[$k] then "\(.[$k].state)" else empty end
        ' "$GH_ISSUE_MOCK_PATH"
        return 0
    fi
    # Note: LINEAR_UNREACHABLE no longer short-circuits here — gh and Linear
    # are independent services. If Linear is down but gh works, the bullet's
    # GH fallback should still resolve.
    if command -v gh >/dev/null 2>&1; then
        local repo issue state
        repo=$(echo "$url" | sed -E 's|https?://github\.com/([^/]+/[^/]+)/issues/[0-9]+.*|\1|')
        issue=$(echo "$url" | grep -oE '/issues/[0-9]+' | grep -oE '[0-9]+')
        if [ -n "$repo" ] && [ -n "$issue" ]; then
            state=$(gh issue view "$issue" -R "$repo" --json state --jq '.state' 2>/dev/null \
                | tr '[:upper:]' '[:lower:]' || true)
            [ -n "$state" ] && echo "$state"
            return 0
        fi
    fi
    stage65_warn "gh issue lookup unavailable for $url"
    return 0
}

stage65_detect_untracked_followup() {
    local pr_body="$1"
    local bullets bullet ticket gh_url result missing=0
    local missing_bullets=()
    # Track which tickets have already been cited so duplicates flag.
    local seen_tickets=""
    bullets=$(stage65_extract_followup_bullets "$pr_body")
    if [ -z "$bullets" ]; then
        return 1  # No follow-up section ⇒ nothing to validate.
    fi
    while IFS= read -r bullet; do
        [ -z "$bullet" ] && continue
        ticket=$(echo "$bullet" | grep -oE 'VIB-[0-9]+' | head -1 || true)
        gh_url=$(echo "$bullet" | grep -oE 'https?://github\.com/[^[:space:])\].,]+/issues/[0-9]+' | head -1 || true)
        if [ -n "$ticket" ]; then
            # Cross-bullet uniqueness check: each follow-up needs its own ticket.
            if echo "$seen_tickets" | tr ' ' '\n' | grep -Fxq "$ticket"; then
                missing_bullets+=("$bullet [reason: ticket $ticket already cited by another bullet — each follow-up needs its own ticket]")
                missing=$((missing + 1)); continue
            fi
            seen_tickets="$seen_tickets $ticket"
            # Linear lookup
            result=$(stage65_resolve_linear "$ticket" || true)
            if [ -z "$result" ]; then
                # Linear down or ticket not found. Check GH fallback in same bullet.
                if [ -n "$gh_url" ]; then
                    local gh_state
                    gh_state=$(stage65_resolve_gh_issue "$gh_url" || true)
                    if [ "$gh_state" = "open" ]; then continue; fi
                fi
                missing_bullets+=("$bullet [reason: ticket $ticket not resolvable]")
                missing=$((missing + 1)); continue
            fi
            local state title
            state=$(echo "$result" | cut -d'|' -f1 | tr '[:upper:]' '[:lower:]')
            title=$(echo "$result" | cut -d'|' -f2-)
            if echo "$state" | grep -Eq "$STAGE65_CLOSED_STATES_REGEX"; then
                missing_bullets+=("$bullet [reason: cited ticket is closed (state: $state)]")
                missing=$((missing + 1)); continue
            fi
            if echo "$title" | grep -Eqi "$STAGE65_PLACEHOLDER_TITLE_REGEX"; then
                missing_bullets+=("$bullet [reason: placeholder ticket — each follow-up must have its own ticket]")
                missing=$((missing + 1)); continue
            fi
        elif [ -n "$gh_url" ]; then
            local gh_state
            gh_state=$(stage65_resolve_gh_issue "$gh_url" || true)
            if [ "$gh_state" != "open" ]; then
                missing_bullets+=("$bullet [reason: GitHub issue not open or not resolvable]")
                missing=$((missing + 1)); continue
            fi
        else
            # Bullet has neither a valid VIB-[0-9]+ nor a /issues/[0-9]+.
            missing_bullets+=("$bullet [reason: requires VIB-[0-9]+ or full GitHub issue URL with numeric id]")
            missing=$((missing + 1))
        fi
    done <<< "$bullets"
    if [ "$missing" -gt 0 ]; then
        printf '%s\n' "${missing_bullets[@]}"
        return 0
    fi
    return 1
}

# ---------------------------------------------------------------------------
# Main entry point.
# ---------------------------------------------------------------------------

# Usage:
#   pr_merger_stage_65_check <fixture-dir>
# where <fixture-dir> contains pr_body.md, commits.txt, git_log_stat.txt, etc.
pr_merger_stage_65_check() {
    local fixture="${1:?fixture dir required}"
    stage65_require_file "pr_body" "$fixture/pr_body.md" || return 2
    stage65_require_file "commits" "$fixture/commits.txt" || return 2
    stage65_require_file "git_log_stat" "$fixture/git_log_stat.txt" || return 2

    # Load mocks / pre-resolutions if present. Production callers populate
    # linear_resolved.txt; fixtures use linear_mock.json. Reset env vars
    # first so a previous fixture's mock paths don't leak into this run.
    unset LINEAR_RESOLVED_PATH LINEAR_MOCK_PATH GH_ISSUE_MOCK_PATH
    [ -f "$fixture/linear_resolved.txt" ] && export LINEAR_RESOLVED_PATH="$fixture/linear_resolved.txt"
    [ -f "$fixture/linear_mock.json" ] && export LINEAR_MOCK_PATH="$fixture/linear_mock.json"
    [ -f "$fixture/gh_issue_mock.json" ] && export GH_ISSUE_MOCK_PATH="$fixture/gh_issue_mock.json"

    # Extract PR title from first H1 in pr_body.md (or from first non-blank line).
    local pr_title
    pr_title=$(grep -m1 '^# ' "$fixture/pr_body.md" | sed 's/^# //' || true)
    [ -z "$pr_title" ] && pr_title=$(head -1 "$fixture/pr_body.md")

    # Mechanical detectors.
    local quick_patch="" production_risk="" scope_drift="" untracked_followup=""
    quick_patch=$(stage65_detect_quick_patch "$fixture/commits.txt" || true)
    production_risk=$(stage65_detect_production_risk "$fixture/git_log_stat.txt" || true)
    scope_drift=$(stage65_detect_scope_drift "$pr_title" "$fixture/git_log_stat.txt" || true)
    untracked_followup=$(stage65_detect_untracked_followup "$fixture/pr_body.md" || true)

    # Auditor verdict (mocked in tests via $EVALUATOR_VERDICT, default "SHIP").
    # In production, Stage 6.5 invokes an Agent; this script is the deterministic
    # fallback / override layer.
    local auditor_verdict="${EVALUATOR_VERDICT:-SHIP}"
    [ -n "${EVALUATOR_MUST_LIE:-}" ] && auditor_verdict="SHIP"

    # Determine mechanical verdict (highest priority wins).
    local mechanical_verdict="" mechanical_evidence=""
    if [ -n "$production_risk" ]; then
        mechanical_verdict="PRODUCTION_RISK"
        mechanical_evidence="$production_risk"
    elif [ -n "$scope_drift" ]; then
        mechanical_verdict="SCOPE_DRIFT"
        mechanical_evidence="$scope_drift"
    elif [ -n "$quick_patch" ]; then
        mechanical_verdict="QUICK_PATCH_DETECTED"
        mechanical_evidence="$quick_patch"
    elif [ -n "$untracked_followup" ]; then
        mechanical_verdict="UNTRACKED_FOLLOWUP"
        mechanical_evidence="$untracked_followup"
    fi

    # STALE_HEAD sub-reason: if fixture provides head_sha_at_start.txt and
    # current_head_sha.txt that differ, force PRODUCTION_RISK.
    if [ -f "$fixture/head_sha_at_start.txt" ] && [ -f "$fixture/current_head_sha.txt" ]; then
        local sha_start sha_now
        sha_start=$(tr -d '[:space:]' < "$fixture/head_sha_at_start.txt")
        sha_now=$(tr -d '[:space:]' < "$fixture/current_head_sha.txt")
        if [ "$sha_start" != "$sha_now" ]; then
            stage65_log "HEAD changed mid-check"
            mechanical_verdict="PRODUCTION_RISK"
            mechanical_evidence="STALE_HEAD: HEAD SHA changed during Stage 6.5 (start=$sha_start, now=$sha_now)"
        fi
    fi

    # Final verdict: mechanical override beats auditor self-report.
    local final_verdict="$auditor_verdict"
    if [ -n "$mechanical_verdict" ]; then
        if [ "$auditor_verdict" = "SHIP" ]; then
            stage65_error "Auditor reported SHIP but mechanical check found $mechanical_verdict; overriding to $mechanical_verdict"
        fi
        final_verdict="$mechanical_verdict"
    fi

    # Output.
    echo "Verdict: $final_verdict"
    echo
    echo "## Recommendation"
    case "$final_verdict" in
        SHIP)
            echo "Proceed to merge. All deferred follow-ups tracked; no scope drift, quick patches, or production risks detected."
            ;;
        SCOPE_DRIFT)
            echo "Diff materially exceeds the PR's stated intent. Recommendation: split adjacent improvements into follow-ups; merge core feature only."
            echo
            echo "Evidence:"; echo "$mechanical_evidence"
            ;;
        QUICK_PATCH_DETECTED)
            echo "At least one fix is a band-aid. Required action: replace with a real fix, or annotate the line with \`# tech-debt: <ticket>\` pointing at a tracked Linear ticket."
            echo
            echo "Offending commits / lines:"; echo "$mechanical_evidence"
            ;;
        UNTRACKED_FOLLOWUP)
            echo "PR body lists deferred follow-ups without tracked tickets. Required action: file the missing tickets via Linear MCP \`save_issue\` (or GitHub issue if Linear unavailable) and update the PR body to cite \`VIB-[0-9]+\` or full \`/issues/[0-9]+\` URL for each bullet."
            echo
            echo "Untracked / malformed bullets:"; echo "$mechanical_evidence"
            ;;
        PRODUCTION_RISK)
            echo "Hard block: money-critical path has silent-error / hardcoded / missing-validation issue, OR HEAD changed during the check."
            echo
            echo "Evidence:"; echo "$mechanical_evidence"
            ;;
        *)
            echo "Unknown verdict from auditor: $final_verdict"
            return 3
            ;;
    esac
    return 0
}

# When sourced, only define functions. When invoked directly, run main.
if [ "${BASH_SOURCE[0]:-}" = "${0:-}" ] && [ -n "${BASH_SOURCE[0]:-}" ]; then
    pr_merger_stage_65_check "${1:-}"
fi
