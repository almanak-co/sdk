"""Pinned copies of retained evidence captures, so hosted CI runs the tests that read them.

Every file under tests/fixtures/evidence/ is a committed byte-for-byte copy of the retained
capture at the same path relative to the evidence tree, and tests/fixtures/evidence.json pins
each copy's SHA-256; `make fixtures` verifies them and restores a changed copy from a local
evidence tree. test_retained_fixtures checks the copies against the manifest in every run;
test_retained_fixture_audit checks the manifest against the retained tree where it is
present. scripts/evidence_fixtures.py pins new copies after refusing any that carry a URL,
credential marker or operator env value.

Paths given here are relative to the evidence tree, for example
"2026-09-07-rollout/curve-initial-discovery/curve-reader-capture.json.gz".
"""

import json
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
ROOT = FIXTURES / "evidence"
MANIFEST = FIXTURES / "evidence.json"


def manifest() -> dict[str, str]:
    return json.loads(MANIFEST.read_text())


def retained(relative: str) -> Path:
    """The committed copy of a retained file, or a directory of copies."""
    return ROOT / relative
