"""Fresh and additional-chain EOA evidence retains actual fund and source identities."""

import hashlib
import json

import pytest

from tests.qa.catalog import ROOT, catalog
from tests.qa.eoa_evidence import QUALIFIED
from tests.qa.test_plain_eoa_evidence import REFERENCES, assert_capture
from tests.qa.test_uniswap_v3 import artifact

CURVE_REFERENCES = {
    "curve_bnb_stable_lp",
    "curve_bnb_twocrypto_lp",
    "curve_optimism_stable_lp",
    "curve_optimism_twocrypto_lp",
    "curve_optimism_tricrypto_lp",
    "curve_polygon_stable_lp",
    "curve_polygon_twocrypto_lp",
    "curve_polygon_tricrypto_lp",
}
PACKAGES = (
    (QUALIFIED.parent / "plain-eoa-qualified-fresh", "fresh", REFERENCES),
    (QUALIFIED.parent / "curve-plain-eoa-qualified-pinned", "pinned", CURVE_REFERENCES),
)
CASES = tuple(
    (case, package)
    for package, policy, references in PACKAGES
    for case in catalog()
    if case.reference in references
    and case.lane == "fork"
    and case.profile == "eoa"
    and case.input_policy == policy
    and not case.scenario
)


@pytest.mark.parametrize("case,package", CASES, ids=[case.id for case, _ in CASES])
def test_additional_reference_uses_its_actual_empty_code_fund_and_origin(tmp_path, case, package):
    audit = json.loads(artifact(package, "origin-code-audit.json.gz"))["accounts"]
    assert_capture(tmp_path, case, package, audit)


@pytest.mark.parametrize("package,policy,references", PACKAGES)
def test_additional_captures_keep_exact_original_source(package, policy, references):
    manifest = json.loads((package / "manifest.json").read_text())
    assert {r["case"] for r in manifest["cases"]} == {
        case.id for case, selected in CASES if selected == package
    }
    assert {r["directory"] for r in manifest["cases"]} == references
    assert all(r["case"].startswith("fork/" + policy + "/") for r in manifest["cases"])
    assert json.loads(artifact(package, "source-hashes.json.gz")) == json.loads(
        artifact(QUALIFIED, "source-hashes.json.gz")
    )
    source = manifest["tested_source"]
    archive = (package / source["path"]).resolve()
    assert archive == (QUALIFIED / "tested-source.tar.gz").resolve()
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == source["sha256"]


def test_earlier_curve_captures_keep_their_coded_fund_qualification_gap():
    package = QUALIFIED.parent / "curve-original-eoa-origin-audit"
    audit = json.loads(artifact(package, "curve-all-origin-code-audit.json.gz"))
    rows = audit["records"]
    assert len(rows) == 96 and audit["unique_observations"] == 18
    assert sum(row["code"] != "0x" for row in rows) == 38
    for row in rows:
        assert (
            hashlib.sha256((ROOT / row["checkpoint"]).read_bytes()).hexdigest()
            == row["checkpoint_sha256"]
        )
        assert row["request"] == {
            "method": "eth_getCode",
            "params": [row["fund"]["address"], hex(row["origin"]["number"])],
        }
