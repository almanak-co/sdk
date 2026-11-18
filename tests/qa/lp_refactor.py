"""Bind historical captures to the exact source of the bounded LP helper extraction."""

import ast
import gzip
import hashlib
import json

from almanak.strategy import StrategyDefinition
from tests.legacy_references import (
    before_hold_guard_digests,
    historical_strategy_current_imports,
    reviewed_current_imports,
    reviewed_hold_guard,
)
from tests.qa.catalog import ROOT

PACKAGE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/lp-exit-helper-refactor"


def archived_reference(reference):
    path = f"strategies/{reference}/strategy.py"
    rows = json.loads((PACKAGE / "manifest.json").read_text())["files"]
    row = next(item for item in rows if item["path"] == path)
    raw = (PACKAGE / row["archived_path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == row["archived_sha256"]
    before = StrategyDefinition(filename="strategy.py", source=gzip.decompress(raw).decode())
    after = StrategyDefinition.read(ROOT / path)
    assert before.sha256 == row["before"]
    assert row["after"] in before_hold_guard_digests(after)
    assert_helper_extraction(before.source, after.source)
    return before


def assert_reference_source(recorded, reference):
    current = StrategyDefinition.read(ROOT / "strategies" / reference / "strategy.py")
    if reviewed_current_imports(recorded) != current:
        assert recorded == archived_reference(reference)


def assert_helper_extraction(before, after):
    before = reviewed_hold_guard(
        historical_strategy_current_imports(
            StrategyDefinition(filename="strategy.py", source=before)
        ).source
    )
    original = ast.parse(before)
    changed = ast.parse(after)
    helper = next(node for node in changed.body if getattr(node, "name", None) == "exit_allocation")
    assert ast.dump(helper.args) == ast.dump(
        ast.parse("def f(context, allocation, observed): pass").body[0].args
    )
    changed.body.remove(helper)
    decision = next(
        node for node in changed.body if getattr(node, "name", None) == "decide_allocation"
    )
    selection = decision.body[1]
    expected = ast.parse("return exit_allocation(context, allocation, observed)").body
    assert [ast.dump(node) for node in selection.body] == [ast.dump(node) for node in expected]
    selection.body = helper.body
    assert ast.dump(original) == ast.dump(changed)
