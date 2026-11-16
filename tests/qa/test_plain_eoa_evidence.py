"""Plain EOA qualification uses actual fund identities and independently read code."""

import hashlib
import io
import json
import tarfile

import pytest

from almanak.connectors.composition import checkpoint_from_json, connector_for
from tests.qa.catalog import ROOT, catalog
from tests.qa.eoa_evidence import QUALIFIED
from tests.qa.frozen_reference import archived_reference
from tests.qa.retained import current_json
from tests.qa.test_uniswap_v3 import artifact
from tests.qa.worker import assert_fork_actions

AUDIT = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/plain-eoa-origin-audit"
REFERENCES = {
    "aave_v3_base_lending",
    "curve_base_stable_lp",
    "curve_base_twocrypto_lp",
    "curve_base_tricrypto_lp",
    "uniswap_v3_base_trading",
    "uniswap_v3_base_lp",
    "uniswap_v3_optimism_trading",
    "uniswap_v3_optimism_lp",
}
CASES = tuple(
    c
    for c in catalog()
    if c.reference in REFERENCES
    and c.lane == "fork"
    and c.input_policy == "pinned"
    and c.profile == "eoa"
    and not c.scenario
)


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
def test_replacement_funds_match_empty_code_origins_and_complete_economic_actions(tmp_path, case):
    audit = json.loads(artifact(AUDIT, "plain-eoa-actual-origin-audit.json.gz"))["accounts"]
    assert_capture(tmp_path, case, QUALIFIED, audit)


def assert_capture(tmp_path, case, package, audit):
    manifest = json.loads((package / "manifest.json").read_text())
    row = next(r for r in manifest["cases"] if r["case"] == case.id)
    state = checkpoint_from_json(current_json(artifact(package, row["checkpoint_path"])))
    record = json.loads(artifact(package, row["result_path"]))
    assert record["status"] == "passed" and record["strategy"] == case.reference
    source = manifest.get("tested_source")
    archive = package / (source["path"] if source else "tested-source.tar.gz")
    if source:
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == source["sha256"]
    assert state.strategy == archived_reference(archive.parent, archive.name, case.reference)
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert all(o.success and not o.accounting_issues for o in state.outcomes)
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    assert_fund_origins(case, state, audit)
    config = tmp_path / "config.yaml"
    config.write_bytes(artifact(package, "native/" + row["directory"] + "/config.yaml.gz"))
    assert_fork_actions(case, state.model_dump(mode="json"), config)


def assert_fund_origins(case, state, audit):
    rows = [r for r in audit if r["case"] == case.id]
    assert len(rows) == len(state.config.accounts.bindings)
    for binding in state.config.accounts.bindings:
        assert binding.profile.kind == "eoa"
        fund = binding.profile.fund
        origin = next(b.origin for b in state.config.fork_bindings if b.chain == fund.chain)
        matching = [
            r for r in rows if r["account"] == binding.name and r["address"] == fund.address
        ]
        assert len(matching) == 1
        observed = matching[0]
        assert observed["code"] == "0x" and observed["block"] == origin.model_dump(mode="json")
        assert observed["request"] == {
            "method": "eth_getCode",
            "params": [fund.address, hex(origin.number)],
        }


def test_all_qualified_cases_keep_their_exact_frozen_source():
    manifest = json.loads((QUALIFIED / "manifest.json").read_text())
    assert {r["case"] for r in manifest["cases"]} == {c.id for c in CASES}
    assert {c.reference for c in CASES} == REFERENCES
    hashes = json.loads(artifact(QUALIFIED, "source-hashes.json.gz"))
    with tarfile.open(fileobj=io.BytesIO(artifact(QUALIFIED, "tested-source.tar.gz"))) as archive:
        for name, expected in hashes.items():
            stream = archive.extractfile(name)
            assert stream is not None
            assert hashlib.sha256(stream.read()).hexdigest() == expected
