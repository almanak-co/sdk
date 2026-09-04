"""Protect user-ticket counterexamples as named QA contracts."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY = REPO_ROOT / "qa_lab/docs/catalog/v1/ticket-counterexamples.json"
# ALM-3242, ALM-3254 and ALM-3263 are absent on purpose. Their counterexample
# nodes travelled out with the production capabilities they pin -- the strat-test
# lifecycle coverage gate (sibling patch A) and the Aerodrome Slipstream closure
# authority (sibling patch C). A registry row naming a node this branch does not
# contain is exactly the vacuous coverage this registry exists to prevent, so the
# rows return with their capabilities rather than being kept alive here.
EXPECTED_TICKETS = {
    "ALM-3041",
    "ALM-3227",
    "ALM-3241",
    "ALM-3250",
    "ALM-3251",
    "ALM-3252",
    "ALM-3267",
    "ALM-3276",
    "ALM-3277",
}


def test_all_user_tickets_have_live_permanent_counterexamples() -> None:
    payload = json.loads(REGISTRY.read_text())

    assert payload["schema_version"] == 1
    assert set(payload["tickets"]) == EXPECTED_TICKETS
    for ticket, contract in payload["tickets"].items():
        assert contract["claim"].strip(), ticket
        assert contract["surfaces"], ticket
        assert len(contract["tests"]) >= 2, ticket
        for nodeid in contract["tests"]:
            relative_path, *qualname = nodeid.split("::")
            path = REPO_ROOT / relative_path
            assert path.is_file(), f"{ticket}: missing counterexample file {relative_path}"
            function_name = qualname[-1]
            source = path.read_text()
            assert re.search(rf"^\s*def\s+{re.escape(function_name)}\s*\(", source, re.MULTILINE), (
                f"{ticket}: missing counterexample node {nodeid}"
            )
