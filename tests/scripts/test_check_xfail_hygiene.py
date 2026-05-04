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
