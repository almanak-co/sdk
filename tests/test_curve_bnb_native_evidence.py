"""Retained BNB proofs keep family, fund and operator claims independently auditable."""

import gzip
import json
from dataclasses import replace
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from tests.qa.catalog import CURVE_LP, Case
from tests.qa.worker import assert_fork_actions

BASE = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-07-rollout"
CASES = tuple(
    (family, record)
    for family in ("stable", "twocrypto")
    for record in json.loads((BASE / f"curve-bnb-{family}-native/manifest.json").read_text())[
        "cases"
    ]
)


@pytest.mark.parametrize("family,record", CASES, ids=[record["case"] for _, record in CASES])
def test_native_family_and_profile_claims_match_actual_closed_effects(family, record, tmp_path):
    directory = BASE / f"curve-bnb-{family}-native/native" / record["case"]
    state = json.loads(gzip.decompress((directory / "checkpoint.json.gz").read_bytes()))
    assert state["mode"] == "closed" and state["pending"] is None
    assert state["outcomes"] and state["delivered"] == len(state["outcomes"])
    assert {binding["profile"]["kind"] for binding in state["config"]["accounts"]["bindings"]} == {
        record["profile"]
    }
    case = Case(
        id=record["case"],
        lane="fork",
        reference=f"curve_bnb_{family}_lp",
        connector=ConnectorId("curve"),
        chains=(ChainId("eip155:56"),),
        actions=CURVE_LP,
        profile=record["profile"],
        mode="fork",
        operator_mode=record["operator_mode"],
        variants=(ProtocolVariant("stableswap_ng" if family == "stable" else "twocrypto_ng"),),
    )
    source = Path(__file__).parents[1] / "strategies" / case.reference / "config.yaml"
    config = tmp_path / "config.yaml"
    config.write_bytes(source.read_bytes())
    assert_fork_actions(case, state, config)
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(
            case=replace(case, variants=(ProtocolVariant("tricrypto_ng"),)),
            state=state,
            config=config,
        )


@pytest.mark.parametrize("family", ["stable", "twocrypto"])
def test_interrupted_approval_remains_a_cleanup_obligation_in_both_profiles(family):
    directory = BASE / f"curve-bnb-{family}-approval-cleanup"
    manifest = json.loads((directory / "manifest.json").read_text())
    assert {case["profile"] for case in manifest["cases"]} == {"eoa", "safe_roles"}
    for case in manifest["cases"]:
        record = json.loads(
            gzip.decompress((directory / "native" / case["case"] / "result.json.gz").read_bytes())
        )
        assert record["status"] == "passed"
        assert record["scenario"] == "approval_commit_then_teardown"
        state = json.loads(
            gzip.decompress(
                (directory / "native" / case["case"] / "checkpoint.json.gz").read_bytes()
            )
        )
        assert state["mode"] == "closed" and state["pending"] is None
        assert state["delivered"] == len(state["outcomes"])
