"""Tests for ``scripts/ci/check_xfail_hygiene.py``.

Issue #1694: untagged ``@pytest.mark.xfail(strict=False)`` muted intent-test
coverage across ~24 sites without ticket refs or dated rationales. The
hygiene script enforces (ticket, date, explicit strict=) on every xfail
under ``tests/intents/``. These tests pin the script's grammar against
inputs we know it must accept and reject.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_xfail_hygiene.py"
    spec = importlib.util.spec_from_file_location("check_xfail_hygiene", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register in sys.modules so dataclasses.fields() can resolve forward refs
    # during ``Violation``/``XfailSite`` construction.
    sys.modules["check_xfail_hygiene"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def hygiene():
    return _load_module()


def _evaluate_source(hygiene, source: str, tmp_path: Path) -> list:
    """Helper: write *source* to a temp file under ``tests/intents/`` shape and check it."""
    target = tmp_path / "test_demo.py"
    target.write_text(source, encoding="utf-8")
    sites = hygiene._collect_sites(target)
    return [hygiene._evaluate(s) for s in sites]


def test_compliant_xfail_passes(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(\n"
        '    reason="VIB-1234: pool drained on Anvil fork (as of 2026-05-04)",\n'
        "    strict=True,\n"
        ")\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    results = _evaluate_source(hygiene, source, tmp_path)
    assert results == [None], f"Expected no violations, got {results}"


def test_missing_ticket_ref_is_flagged(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(\n"
        '    reason="Flaky on Anvil fork (as of 2026-05-04)",\n'
        "    strict=False,\n"
        ")\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    [violation] = _evaluate_source(hygiene, source, tmp_path)
    assert violation is not None
    assert any("ticket-ref" in m for m in violation.missing)


def test_missing_date_is_flagged(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(reason='VIB-9000: bug', strict=True)\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    [violation] = _evaluate_source(hygiene, source, tmp_path)
    assert violation is not None
    assert any("date stamp" in m for m in violation.missing)


def test_missing_strict_is_flagged(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(reason='VIB-1: stuff (as of 2026-05-04)')\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    [violation] = _evaluate_source(hygiene, source, tmp_path)
    assert violation is not None
    assert any("explicit strict=" in m for m in violation.missing)


def test_dynamic_strict_is_flagged(hygiene, tmp_path):
    """``strict=condition`` (non-literal) must not pass; reviewers can't reason about runtime values."""
    source = (
        "import pytest\n"
        "\n"
        "FLAKY = True\n"
        "@pytest.mark.xfail(reason='VIB-1: stuff (as of 2026-05-04)', strict=FLAKY)\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    [violation] = _evaluate_source(hygiene, source, tmp_path)
    assert violation is not None
    assert any("literal True/False" in m for m in violation.missing)


def test_grandfathered_above_decorator_is_skipped(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "# xfail-grandfathered: #1694\n"
        "@pytest.mark.xfail(reason='no ticket no date', strict=False)\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    results = _evaluate_source(hygiene, source, tmp_path)
    assert results == [None]


def test_grandfathered_below_marker_module_level(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "# xfail-grandfathered: #1694 (module pytestmark)\n"
        "pytestmark = pytest.mark.xfail(reason='legacy module-level mute')\n"
        "\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    results = _evaluate_source(hygiene, source, tmp_path)
    assert results == [None]


def test_github_issue_ref_alone_satisfies_ticket(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(reason='#42: bug (as of 2026-01-01)', strict=False)\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    [v] = _evaluate_source(hygiene, source, tmp_path)
    assert v is None


def test_implicit_string_concat_in_reason_is_parsed(hygiene, tmp_path):
    """Multi-line implicit-concatenated reason strings must still be inspected."""
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(\n"
        "    reason=(\n"
        '        "VIB-7777: '
        '"\n'
        '        "long reason continues "\n'
        '        "(as of 2026-05-04)"\n'
        "    ),\n"
        "    strict=True,\n"
        ")\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    [v] = _evaluate_source(hygiene, source, tmp_path)
    assert v is None, f"Implicit concat should fold and pass; got {v}"


def test_mark_alias_form_is_detected(hygiene, tmp_path):
    """``from pytest import mark`` + ``@mark.xfail(...)`` must NOT slip past
    the hygiene gate (Codex review on PR #2033 -- the original AST matcher
    only handled ``pytest.mark.xfail`` and silently skipped this alias).
    """
    # Compliant alias-form decorator -> no violation.
    compliant = (
        "from pytest import mark\n"
        "\n"
        "@mark.xfail(\n"
        '    reason="VIB-1234: aliased mark form (as of 2026-05-04)",\n'
        "    strict=True,\n"
        ")\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    assert _evaluate_source(hygiene, compliant, tmp_path) == [None]

    # Non-compliant alias-form decorator -> the gate must catch it.
    sloppy_path = tmp_path / "test_sloppy.py"
    sloppy_path.write_text(
        "from pytest import mark\n"
        "\n"
        "@mark.xfail(reason='no ticket no date', strict=False)\n"
        "def test_bar():\n"
        "    assert False\n",
        encoding="utf-8",
    )
    sites = hygiene._collect_sites(sloppy_path)
    assert len(sites) == 1, "alias-form xfail site was not collected"
    [violation] = [hygiene._evaluate(s) for s in sites]
    assert violation is not None
    assert any("ticket-ref" in m for m in violation.missing)
    assert any("date stamp" in m for m in violation.missing)


# ---------------------------------------------------------------------------
# Ticket liveness (VIB-5965) — ref extraction + status classification.
# All network interaction is stubbed via the injectable run/post/resolver
# hooks; these tests must never hit the gh CLI or the Linear API.
# ---------------------------------------------------------------------------


def _collect_single_site(hygiene, source: str, tmp_path: Path):
    target = tmp_path / "test_refs.py"
    target.write_text(source, encoding="utf-8")
    sites = hygiene._collect_sites(target)
    assert len(sites) == 1
    return sites[0]


def test_refs_extracted_from_reason_in_order_deduped(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(\n"
        "    reason='VIB-4426 V0 (PR #2335) needs VIB-4478; see #2335 again "
        "(as of 2026-05-17)',\n"
        "    strict=True,\n"
        ")\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    site = _collect_single_site(hygiene, source, tmp_path)
    assert site.refs == ("VIB-4426", "#2335", "VIB-4478")


def test_refs_include_grandfather_marker_line(hygiene, tmp_path):
    """Grandfathered sites often have no ref in the reason; the marker's
    tracking issue (#1694) is their only tracker and must be liveness-checked."""
    source = (
        "import pytest\n"
        "\n"
        "# xfail-grandfathered: #1694\n"
        "@pytest.mark.xfail(reason='flaky on Anvil fork', strict=False)\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    site = _collect_single_site(hygiene, source, tmp_path)
    assert site.grandfathered
    assert site.refs == ("#1694",)


def test_rule_number_citations_are_not_refs(hygiene, tmp_path):
    """``per intent-tests rule #12`` cites a doc section, not a tracker."""
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(\n"
        "    reason='VIB-4309: route flake; needs 10/10 runs per intent-tests "
        "rule #12 (as of 2026-05-13)',\n"
        "    strict=False,\n"
        ")\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    site = _collect_single_site(hygiene, source, tmp_path)
    assert site.refs == ("VIB-4309",)


def test_adjacent_grandfathered_sites_do_not_share_marker_refs(hygiene, tmp_path):
    """Two grandfathered sites within a few lines of each other must each keep
    only their own marker's tracker ref (PR #3391 review): site A's forward
    scan window overlaps site B's marker line, and without ownership
    attribution B's ref would leak into A — a borrowed OPEN ref could then
    mask a genuinely closed tracker on A."""
    source = (
        "import pytest\n"
        "\n"
        "# xfail-grandfathered: #1111\n"
        "@pytest.mark.xfail(reason='flaky (as of 2026-01-01)', strict=False)\n"
        "def test_a():\n"
        "    assert False\n"
        "\n"
        "# xfail-grandfathered: #2222\n"
        "@pytest.mark.xfail(reason='flaky (as of 2026-01-01)', strict=False)\n"
        "def test_b():\n"
        "    assert False\n"
    )
    target = tmp_path / "test_refs.py"
    target.write_text(source, encoding="utf-8")
    sites = hygiene._collect_sites(target)
    assert len(sites) == 2
    by_line = {site.lineno: site for site in sites}
    assert by_line[4].refs == ("#1111",)
    assert by_line[9].refs == ("#2222",)


def test_marker_inside_multiline_decorator_owned_by_its_site(hygiene, tmp_path):
    """A marker on a line inside the decorator's argument list (below the
    anchor) is attributed to that decorator via the preceding-anchor fallback
    in _marker_owner."""
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(\n"
        "    # xfail-grandfathered: #3333\n"
        "    reason='flaky (as of 2026-01-01)',\n"
        "    strict=False,\n"
        ")\n"
        "def test_a():\n"
        "    assert False\n"
    )
    site = _collect_single_site(hygiene, source, tmp_path)
    assert site.grandfathered
    assert site.refs == ("#3333",)


def test_neighbouring_code_does_not_leak_refs(hygiene, tmp_path):
    """Only the reason and grandfather-marker lines contribute refs — a
    comment on an adjacent line must not be attributed to the site."""
    source = (
        "import pytest\n"
        "\n"
        "# unrelated context: VIB-9999 lives here\n"
        "@pytest.mark.xfail(reason='VIB-1111: bug (as of 2026-01-01)', strict=True)\n"
        "def test_foo():\n"
        "    assert False\n"
    )
    site = _collect_single_site(hygiene, source, tmp_path)
    assert site.refs == ("VIB-1111",)


def test_resolve_github_ref_open_issue(hygiene):
    def fake_run(args):
        assert args[:2] == ["issue", "view"]
        return 0, '{"state": "OPEN"}'

    status = hygiene.resolve_github_ref("#42", run=fake_run)
    assert (status.status, status.detail) == (hygiene.STATUS_OPEN, "issue OPEN")


def test_resolve_github_ref_closed_issue(hygiene):
    status = hygiene.resolve_github_ref("#42", run=lambda args: (0, '{"state": "CLOSED"}'))
    assert (status.status, status.detail) == (hygiene.STATUS_CLOSED, "issue CLOSED")


def test_resolve_github_ref_merged_pr_via_issue_endpoint(hygiene):
    """gh issue view resolves PR numbers too, reporting state=MERGED; the
    detail must say PR, and MERGED must count as CLOSED."""
    status = hygiene.resolve_github_ref("#3347", run=lambda args: (0, '{"state": "MERGED"}'))
    assert (status.status, status.detail) == (hygiene.STATUS_CLOSED, "PR MERGED")


def test_resolve_github_ref_pr_fallback(hygiene):
    calls = []

    def fake_run(args):
        calls.append(args[:2])
        if args[0] == "issue":
            return 1, "GraphQL: Could not resolve to an Issue"
        return 0, '{"state": "MERGED"}'

    status = hygiene.resolve_github_ref("#2335", run=fake_run)
    assert calls == [["issue", "view"], ["pr", "view"]]
    assert (status.status, status.detail) == (hygiene.STATUS_CLOSED, "PR MERGED")


def test_resolve_github_ref_degrades_to_unknown(hygiene):
    status = hygiene.resolve_github_ref("#7", run=lambda args: (1, "boom"))
    assert status.status == hygiene.STATUS_UNKNOWN

    # Malformed JSON on both endpoints must degrade, not raise.
    status = hygiene.resolve_github_ref("#7", run=lambda args: (0, "not json"))
    assert status.status == hygiene.STATUS_UNKNOWN


def test_resolve_linear_ref_without_key_is_unknown(hygiene):
    status = hygiene.resolve_linear_ref("VIB-1", api_key=None, post=None)
    assert status.status == hygiene.STATUS_UNKNOWN
    assert "LINEAR_API_KEY" in status.detail


def _linear_response(state_type, state_name):
    import json

    return json.dumps(
        {"data": {"issue": {"identifier": "VIB-1", "state": {"name": state_name, "type": state_type}}}}
    )


@pytest.mark.parametrize(
    ("state_type", "state_name", "expected"),
    [
        ("completed", "Done", "CLOSED"),
        ("canceled", "Canceled", "CLOSED"),
        ("started", "In Progress", "OPEN"),
        ("backlog", "Backlog", "OPEN"),
        ("triage", "Triage", "OPEN"),
    ],
)
def test_resolve_linear_ref_state_mapping(hygiene, state_type, state_name, expected):
    status = hygiene.resolve_linear_ref(
        "VIB-1", api_key="key", post=lambda key, payload: _linear_response(state_type, state_name)
    )
    assert status.status == expected
    assert status.detail == state_name


def test_resolve_linear_ref_degrades_to_unknown(hygiene):
    def raising_post(key, payload):
        raise OSError("connection refused")

    status = hygiene.resolve_linear_ref("VIB-1", api_key="key", post=raising_post)
    assert status.status == hygiene.STATUS_UNKNOWN
    assert "API error" in status.detail

    # Issue not found / no access -> data.issue is null.
    status = hygiene.resolve_linear_ref(
        "VIB-1", api_key="key", post=lambda key, payload: '{"data": {"issue": null}}'
    )
    assert status.status == hygiene.STATUS_UNKNOWN

    # Malformed body -> UNKNOWN, never a crash.
    status = hygiene.resolve_linear_ref("VIB-1", api_key="key", post=lambda key, payload: "<html>")
    assert status.status == hygiene.STATUS_UNKNOWN


def _ref_status(hygiene, ref, status):
    return hygiene.RefStatus(ref=ref, kind="github", status=status, detail=status.lower())


def test_site_verdict_folding(hygiene):
    open_ = _ref_status(hygiene, "#1", hygiene.STATUS_OPEN)
    closed = _ref_status(hygiene, "#2", hygiene.STATUS_CLOSED)
    unknown = _ref_status(hygiene, "#3", hygiene.STATUS_UNKNOWN)

    assert hygiene.site_verdict(()) == hygiene.VERDICT_NO_REF
    # Any live tracker keeps the mute justified.
    assert hygiene.site_verdict((closed, open_)) == hygiene.STATUS_OPEN
    # UNKNOWN must never escalate to CLOSED (no false positives on degradation).
    assert hygiene.site_verdict((closed, unknown)) == hygiene.STATUS_UNKNOWN
    assert hygiene.site_verdict((closed, closed)) == hygiene.STATUS_CLOSED


def test_build_liveness_rows_resolves_each_ref_once(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(reason='VIB-1: a (as of 2026-01-01)', strict=True)\n"
        "def test_a():\n"
        "    assert False\n"
        "\n"
        "@pytest.mark.xfail(reason='VIB-1 again, plus #2 (as of 2026-01-01)', strict=True)\n"
        "def test_b():\n"
        "    assert False\n"
    )
    target = tmp_path / "test_cache.py"
    target.write_text(source, encoding="utf-8")
    sites = hygiene._collect_sites(target)

    seen = []

    def resolver(ref):
        seen.append(ref)
        return _ref_status(hygiene, ref, hygiene.STATUS_CLOSED)

    rows, cache = hygiene.build_liveness_rows(sites, resolver)
    assert sorted(seen) == ["#2", "VIB-1"], "each unique ref must be resolved exactly once"
    assert set(cache) == {"VIB-1", "#2"}
    assert [r.verdict for r in rows] == [hygiene.STATUS_CLOSED, hygiene.STATUS_CLOSED]


def test_format_liveness_report_markdown_escapes_pipes(hygiene, tmp_path):
    source = (
        "import pytest\n"
        "\n"
        "@pytest.mark.xfail(reason='VIB-1: a | b pipe (as of 2026-01-01)', strict=True)\n"
        "def test_a():\n"
        "    assert False\n"
    )
    target = tmp_path / "test_md.py"
    target.write_text(source, encoding="utf-8")
    sites = hygiene._collect_sites(target)
    rows, cache = hygiene.build_liveness_rows(
        sites, lambda ref: _ref_status(hygiene, ref, hygiene.STATUS_CLOSED)
    )
    report = hygiene.format_liveness_report(rows, cache, markdown=True)
    assert "| Site | Ref | Status | Detail | Reason (excerpt) |" in report
    assert "a \\| b pipe" in report
    assert "Sites whose every tracker is closed: 1 of 1." in report


def test_run_liveness_exit_codes(hygiene, capsys):
    """Exit 1 only with --fail-on-closed AND a closed-tracker site; the
    default liveness invocation always exits 0 (report-only)."""
    all_closed = lambda ref: _ref_status(hygiene, ref, hygiene.STATUS_CLOSED)  # noqa: E731
    all_unknown = lambda ref: _ref_status(hygiene, ref, hygiene.STATUS_UNKNOWN)  # noqa: E731

    assert hygiene.run_liveness(fail_on_closed=False, markdown=False, resolver=all_closed) == 0
    assert hygiene.run_liveness(fail_on_closed=True, markdown=False, resolver=all_closed) == 1
    # Graceful degradation (everything UNKNOWN) never fails the job.
    assert hygiene.run_liveness(fail_on_closed=True, markdown=False, resolver=all_unknown) == 0
    capsys.readouterr()


def test_real_repo_passes_check(hygiene):
    """The repo's current state must pass the hygiene check.

    Every existing xfail under ``tests/intents/`` should either be compliant
    or carry the grandfather marker tied to issue #1694. If this test fails,
    a new xfail was added without satisfying the rule -- either fix it or
    explicitly grandfather it (and keep the count shrinking).
    """
    files = hygiene.find_intent_test_files()
    sites = []
    for f in files:
        sites.extend(hygiene._collect_sites(f))
    violations = [v for s in sites if (v := hygiene._evaluate(s))]
    assert not violations, (
        "xfail hygiene violations in tests/intents/:\n"
        + hygiene.format_report(
            violations,
            total_sites=len(sites),
            grandfathered=sum(1 for s in sites if s.grandfathered),
        )
    )
