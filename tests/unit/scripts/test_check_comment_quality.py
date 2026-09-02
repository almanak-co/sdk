from __future__ import annotations

from collections import Counter

import pytest

from scripts.ci import check_comment_quality as gate


def test_parse_added_lines_tracks_only_new_ranges() -> None:
    diff = """diff --git a/almanak/x.py b/almanak/x.py
--- a/almanak/x.py
+++ b/almanak/x.py
@@ -10,2 +10,3 @@
+one
+two
+three
diff --git a/tests/y.py b/tests/y.py
--- a/tests/y.py
+++ b/tests/y.py
@@ -0,0 +1,2 @@
+one
+two
"""

    assert gate._parse_added_lines(diff) == {
        "almanak/x.py": {10, 11, 12},
        "tests/y.py": {1, 2},
    }


def test_parse_baseline_paths_tracks_renames() -> None:
    diff = """diff --git a/almanak/old.py b/almanak/new.py
similarity index 90%
rename from almanak/old.py
rename to almanak/new.py
--- a/almanak/old.py
+++ b/almanak/new.py
@@ -1 +1 @@
-old
+new
"""

    assert gate._parse_baseline_paths(diff) == {"almanak/new.py": "almanak/old.py"}


@pytest.mark.parametrize(
    ("comment", "reason"),
    [
        ("# VIB-123 changed this", "ticket or pull-request history"),
        ("# Follow-up from PR #42", "ticket or pull-request history"),
        ("# Tracked as issue 42", "ticket or pull-request history"),
        ("# Follow-up to #3534", "ticket or pull-request history"),
        ("# CodeRabbit review", "reviewer attribution"),
        ("# Phase 3 migration", "rollout phase or numbered step"),
        ("# Step 2: call the helper", "rollout phase or numbered step"),
        ("# See the accounting blueprint", "external spec or blueprint reference"),
        ("# See docs/internal/accounting.md", "external spec or blueprint reference"),
        ("# ==================", "decorative separator"),
        ("# value = legacy_call()", "commented-out Python"),
        ("# if enabled:", "commented-out Python"),
        ("# @legacy_decorator", "commented-out Python"),
        ("# continue", "commented-out Python"),
        ("# obj.legacy_call()", "commented-out Python"),
        ("# await legacy_call()", "commented-out Python"),
        ("# async with session:", "commented-out Python"),
        ("# match value:", "commented-out Python"),
        ("# case 1:", "commented-out Python"),
        ("# crap-allowlist: VIB-123 temporary", "ticket or pull-request history"),
    ],
)
def test_rejects_high_confidence_comment_debt(comment: str, reason: str) -> None:
    violations = gate._violations_in_source("almanak/x.py", f"{comment}\nvalue = 1\n", {1})

    assert [violation.reason for violation in violations] == [reason]


def test_ignores_existing_bad_comments() -> None:
    source = "# VIB-123 old debt\nvalue = 1\n"

    assert gate._violations_in_source("almanak/x.py", source, {2}) == []


def test_ignores_unchanged_inline_comment_when_code_changes() -> None:
    comment = "# VIB-123 old debt"
    source = f"value = new_call()  {comment}\n"

    assert gate._violations_in_source("almanak/x.py", source, {1}, Counter({comment: 1})) == []


def test_rejects_new_duplicate_of_an_existing_comment() -> None:
    comment = "# VIB-123 old debt"
    source = f"value = 1  {comment}\nvalue = 2  {comment}\n"

    violations = gate._violations_in_source("almanak/x.py", source, {2}, Counter({comment: 1}))

    assert [violation.line for violation in violations] == [2]


def test_ignores_hashes_inside_strings() -> None:
    source = 'message = "# VIB-123 is data"\n'

    assert gate._violations_in_source("almanak/x.py", source, {1}) == []


def test_allows_timeless_rationale_and_machine_directives() -> None:
    source = """# Preserve measured zero; None means unmeasured.
# The JSON-RPC specification requires a hexadecimal quantity.
# amount / 10**decimals
# token0 < token1
# O(n * m)
value = call()  # type: ignore[arg-type]
other = call()  # noqa: F401
third = call()  # pragma: no cover
fourth = call()  # crap-allowlist: generated dispatch
"""

    assert gate._violations_in_source("almanak/x.py", source, set(range(1, 10))) == []


def test_generated_marker_requires_a_file_header() -> None:
    assert gate._is_generated_source("# This file is auto-generated.\nvalue = 1\n")
    assert not gate._is_generated_source('EXPECTED = """\n# This file is auto-generated.\n"""\n')
    assert not gate._is_generated_source("value = 1\n# This file is auto-generated.\n")
    assert not gate._is_generated_source("# IDs are auto-generated for each request.\n")


def test_scan_uses_rename_source_for_existing_comments(tmp_path, monkeypatch) -> None:
    comment = "# VIB-123 old debt"
    package = tmp_path / "almanak"
    package.mkdir()
    (package / "new.py").write_text(f"value = new_call()  {comment}\n")
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        gate,
        "_changed_python_lines",
        lambda _base: (
            "base",
            {"almanak/new.py": {1}},
            {"almanak/new.py": "almanak/old.py"},
        ),
    )
    requested: list[tuple[str, str]] = []

    def comments_at_revision(revision: str, path: str) -> Counter[str]:
        requested.append((revision, path))
        return Counter({comment: 1})

    monkeypatch.setattr(gate, "_comments_at_revision", comments_at_revision)

    assert gate.scan("base") == []
    assert requested == [("base", "almanak/old.py")]


def test_tokenizer_errors_fail_closed() -> None:
    with pytest.raises(gate.CommentQualityError, match="could not tokenize"):
        gate._violations_in_source("almanak/x.py", 'value = """unterminated\n', {1})
