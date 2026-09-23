"""The money-lane audit must name every ambient edit it excludes, including the first."""

import subprocess

import pytest

from qa_lab import mainnet_intent_coordinator as coordinator


@pytest.fixture
def repo(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "qa@example.test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "qa"], cwd=tmp_path, check=True)
    for name in ("AGENTS.md", "second.md"):
        (tmp_path / name).write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=tmp_path, check=True)
    monkeypatch.setattr(coordinator, "REPO", tmp_path)
    # `_git` binds `cwd=REPO` as a default at import, so patching the module
    # attribute alone would leave it reading the real checkout.
    monkeypatch.setattr(
        coordinator,
        "_git",
        lambda *args, cwd=tmp_path, strip=True: coordinator._run(["git", *args], cwd=cwd, strip=strip),
    )
    return tmp_path


def test_first_unstaged_path_is_not_shifted(repo):
    """`stdout.strip()` ate the leading space of the first porcelain row.

    An unstaged modification renders as ``" M AGENTS.md"``. Stripping the whole
    output shifts only that first line, so the audit recorded ``GENTS.md`` with
    a ``not-a-file`` digest -- unverifiable, and silently so, because every
    later row was unaffected.
    """
    (repo / "AGENTS.md").write_text("edited\n", encoding="utf-8")
    (repo / "second.md").write_text("edited\n", encoding="utf-8")

    rows = coordinator._operator_changes()
    by_path = {row["path"]: row for row in rows}

    assert "GENTS.md" not in by_path, f"first porcelain row was shifted: {rows}"
    assert "AGENTS.md" in by_path, f"first modified file missing: {rows}"
    assert by_path["AGENTS.md"]["status"] == " M"
    assert by_path["AGENTS.md"]["sha256"] != "not-a-file"
    assert by_path["second.md"]["sha256"] != "not-a-file"


def test_untracked_first_row_also_survives(repo):
    """An untracked first row (``?? x``) has no leading space, but must still resolve."""
    (repo / "AAA-untracked.md").write_text("new\n", encoding="utf-8")

    rows = coordinator._operator_changes()
    by_path = {row["path"]: row for row in rows}

    assert "AAA-untracked.md" in by_path, f"untracked row missing: {rows}"
    assert by_path["AAA-untracked.md"]["status"] == "??"
    assert by_path["AAA-untracked.md"]["sha256"] != "not-a-file"


def test_every_recorded_path_resolves_to_a_real_file(repo):
    """No row may carry a digest the audit could not compute."""
    (repo / "AGENTS.md").write_text("edited\n", encoding="utf-8")
    (repo / "zz-new.md").write_text("new\n", encoding="utf-8")

    rows = coordinator._operator_changes()

    assert rows
    unverifiable = [r for r in rows if r["sha256"] == "not-a-file"]
    assert not unverifiable, f"audit could not digest: {unverifiable}"
