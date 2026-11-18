"""Every pinned evidence copy is byte-identical to its retained original.

This module reads the git-ignored evidence tree, so hosted CI does not collect it; locally a
missing original or a changed byte fails.
"""

import hashlib
from pathlib import Path

import pytest

from tests.retained_fixtures import manifest

EVIDENCE = Path(__file__).parents[1] / "docs/internal/reports/evidence"


@pytest.mark.parametrize("relative,digest", sorted(manifest().items()))
def test_committed_copy_digest_equals_retained_original(relative, digest):
    assert hashlib.sha256((EVIDENCE / relative).read_bytes()).hexdigest() == digest
