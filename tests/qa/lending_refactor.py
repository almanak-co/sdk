"""Keep historical execution source distinct from the reviewed current reference."""

import gzip
import hashlib
import json

from almanak.strategy import StrategyDefinition
from tests.legacy_references import before_hold_guard_digests
from tests.qa.catalog import ROOT
from tests.scope_only_sources import before_scope_only_digests

PACKAGE = (
    ROOT / "docs/internal/reports/evidence/2026-09-09-lending-rollout/arbitrum-reference-refactor"
)


def archived_reference(reference):
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    path = f"strategies/{reference}/strategy.py"
    row = next(item for item in manifest["files"] if item["path"] == path)
    raw = (PACKAGE / row["archived_path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == row["archived_sha256"]
    before = StrategyDefinition(filename="strategy.py", source=gzip.decompress(raw).decode())
    after = StrategyDefinition.read(ROOT / path)
    assert before.sha256 == row["before"]
    # The refactored source predates the reviewed optional-hold and scope-only edits.
    assert row["after"] in before_hold_guard_digests(after) | before_scope_only_digests(
        reference, after
    )
    return before
