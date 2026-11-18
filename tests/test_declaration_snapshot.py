"""Public connector declarations stay byte-identical to the retained snapshot.

Promotions regenerate the snapshot with `scripts/declaration_snapshot.py`
(`--head` and `--scope` replace the recorded values); `--check` fails on any
byte difference between the rendering and the committed file.
"""

import json
from pathlib import Path

from almanak import reports
from scripts import declaration_snapshot

ROOT = Path(__file__).parents[1]
SNAPSHOT = ROOT / "tests/fixtures/connector-chain-declarations.json"


def snapshot_support():
    expected: dict[str, list[dict]] = {}
    for entry in json.loads(SNAPSHOT.read_text())["entries"]:
        expected.setdefault(entry["connector"], []).append(
            {key: value for key, value in entry.items() if key != "connector"}
        )
    return expected


def test_public_declarations_match_the_retained_snapshot():
    # Candidate declarations live in separate candidate functions; only trusted
    # promotion regenerates this snapshot together with the declaration change.
    assert json.dumps(reports.support(), sort_keys=True) == json.dumps(
        snapshot_support(), sort_keys=True
    )


def test_generator_check_mode_accepts_the_committed_snapshot():
    assert declaration_snapshot.SNAPSHOT == SNAPSHOT
    assert declaration_snapshot.main(["--check"]) == 0


def test_generator_rewrites_a_drifted_copy_byte_for_byte(tmp_path, capsys):
    committed = SNAPSHOT.read_text(encoding="utf-8")
    drifted = json.loads(committed)
    drifted["entries"].pop()
    copy = tmp_path / "declarations.json"
    copy.write_text(json.dumps(drifted, indent=1) + "\n", encoding="utf-8")

    assert declaration_snapshot.main(["--check", "--path", str(copy)]) == 1
    assert "out of date" in capsys.readouterr().err
    assert declaration_snapshot.main(["--path", str(copy)]) == 0
    assert copy.read_text(encoding="utf-8") == committed


def test_generator_replaces_head_and_scope_only_when_given(tmp_path):
    copy = tmp_path / "declarations.json"
    copy.write_text(SNAPSHOT.read_text(encoding="utf-8"), encoding="utf-8")
    original = json.loads(SNAPSHOT.read_text(encoding="utf-8"))

    assert declaration_snapshot.main(["--path", str(copy), "--head", "abc"]) == 0
    rewritten = json.loads(copy.read_text(encoding="utf-8"))
    assert (rewritten["head"], rewritten["scope"]) == ("abc", original["scope"])
    assert declaration_snapshot.main(["--check", "--path", str(copy)]) == 0

    assert declaration_snapshot.main(["--path", str(copy), "--scope", "new scope"]) == 0
    rewritten = json.loads(copy.read_text(encoding="utf-8"))
    assert (rewritten["head"], rewritten["scope"]) == ("abc", "new scope")
    assert rewritten["entries"] == original["entries"]
