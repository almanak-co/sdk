"""Sealed band-reference supplements to the 8 September native QA packages.

Banded liquidity arrived after the PancakeSwap BNB and Slipstream Base native
packages were sealed, so their band cases are sealed separately from one later
qa_bench run. A case is read from whichever package holds it, and every declared
case must be held exactly once across a base package and its supplement.
"""

import json
from pathlib import Path

import yaml

from tests.exchange_scope import without_exchange_budgets
from tests.qa.catalog import ROOT

SUPPLEMENTS = ROOT / "docs/internal/reports/evidence/2026-09-24-band-native"


def manifest(package: Path) -> dict:
    return json.loads((package / "manifest.json").read_text())


def holding(packages, case_id):
    """The package and manifest row sealing one case."""
    return next(
        (package, row)
        for package in packages
        for row in manifest(package)["cases"]
        if row["case"] == case_id
    )


def sealed(packages):
    return [row["case"] for package in packages for row in manifest(package)["cases"]]


def assert_supplement_audit(package, artifact, checkpoint):
    """Every supplement EOA fund read empty code at its retained fork origin block."""
    rows = json.loads(artifact(package, "audit/account-code-audit.json.gz"))
    cases = {row["case"]: row for row in manifest(package)["cases"]}
    eoa = {case for case, row in cases.items() if row["profile"] == "eoa"}
    assert {row["case"] for row in rows} == eoa
    for row in rows:
        case = cases[row["case"]]
        retained = json.loads(artifact(package, case["checkpoint_path"]))
        state = checkpoint.model_validate_json(json.dumps(without_exchange_budgets(retained)))
        origins = json.loads(artifact(package, case["fork_origins_path"]))
        origin = origins["forks"][0]["block"]
        assert row["result"] == "0x" and row["plain_eoa"]
        assert row["origin"] == origin
        assert configured_anchor(package, artifact, case, origin["chain"]["chain_id"]) == (
            origin["number"],
            origin["hash"],
        )
        assert row["request"] == {
            "method": "eth_getCode",
            "params": [row["address"], hex(origin["number"])],
        }
        assert row["address"] == state.config.accounts.bindings[0].profile.fund.address
    return eoa


def configured_anchor(package, artifact, case, chain):
    """The fork anchor the case's operator bundle configured for one chain."""
    operator = yaml.safe_load(artifact(package, case["operator_path"]))
    anchor = next(a for a in operator["fork"]["anchors"] if a["chain"] == chain)
    assert anchor["fresh"] is False
    return anchor["height"], anchor["reference"]


def assert_generated_authority(packages, artifact, case_id, record, state):
    """A Safe case installed and verified its generated grants before execution.

    The supervised public CLI driver admits Safe execution only after `verify`
    wrote its record, so that record is bound to the run's retained grants.
    """
    if record.get("driver") != "public_cli_supervised_fork":
        assert "generated_permissions_installed_and_verified" in record["phases"]
        return
    assert record["phases"][0] == "supervised_resolution_fixture_hooks_and_admission"
    package, row = holding(packages, case_id)
    verification = json.loads(artifact(package, row["permission_verification_path"]))
    retained = state.permissions.partitions
    # Packages sealed before scope-only authority (blueprint 27) also verified quotas; the
    # current state retains none, so the partition accounts are what binds them.
    for verified, partition in zip(verification["partitions"], retained, strict=True):
        assert verified["account"] == partition.account


def assert_supplement_source(package, reference, definition):
    """The executed author source is the one in the sealed run's source identity."""
    files = manifest(package)["identity"]["files"]
    assert definition.sha256 == files[f"strategies/{reference}/strategy.py"]
