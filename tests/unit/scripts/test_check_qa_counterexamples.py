from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts/ci/check_qa_counterexamples.py"
SPEC = importlib.util.spec_from_file_location("check_qa_counterexamples", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gate
SPEC.loader.exec_module(gate)


def _payload(nodeids: list[str]) -> dict:
    return {
        "schema_version": 1,
        "tickets": {
            "ALM-1": {
                "claim": "A permanent claim",
                "surfaces": ["quant"],
                "tests": nodeids,
            }
        },
    }


def test_registry_resolves_exact_unique_repository_test_nodes() -> None:
    nodeids = [
        "tests/unit/scripts/test_derived_claims.py::test_pass_requires_measurement_and_byte_bound_artifact",
        "tests/unit/scripts/test_derived_claims.py::test_tampered_artifact_invalidates_claim",
    ]

    assert gate.registered_nodeids(_payload(nodeids)) == nodeids


def test_registry_rejects_a_text_reference_that_is_not_a_test_node() -> None:
    payload = _payload(["docs/internal/qa/README.md::claim", "tests/unit/scripts/missing.py::test_missing"])

    with pytest.raises(ValueError, match="not a repository test"):
        gate.registered_nodeids(payload)


def test_registry_rejects_shared_nodes_that_hide_ticket_denominators() -> None:
    nodeid = "tests/unit/scripts/test_derived_claims.py::test_tampered_artifact_invalidates_claim"
    payload = _payload(
        [nodeid, "tests/unit/scripts/test_derived_claims.py::test_pass_requires_measurement_and_byte_bound_artifact"]
    )
    payload["tickets"]["ALM-2"] = {
        "claim": "A different permanent claim",
        "surfaces": ["data"],
        "tests": [
            nodeid,
            "tests/unit/scripts/test_quant_books.py::test_missing_mechanical_books_evidence_is_unmeasured",
        ],
    }

    with pytest.raises(ValueError, match="shared by ALM-1 and ALM-2"):
        gate.registered_nodeids(payload)


def test_pytest_command_enforces_execution_and_strict_xfail() -> None:
    command = gate.pytest_command(["tests/unit/example.py::test_case"])

    assert "--collect-only" not in command
    assert "xfail_strict=true" in command
    assert command[:3] == [sys.executable, "-m", "pytest"]
