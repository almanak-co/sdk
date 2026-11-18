"""Retained Slipstream manager grants separate Roles scope from gateway ownership."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.caip import AssetId, ChainId

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/slipstream-base-safe-lp"


def artifact(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".json.gz")).read_bytes()))


def test_sealed_safe_lp_authority_artifacts_have_exact_checksums():
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for entry in manifest["files"]:
        data = (EVIDENCE / entry["path"]).read_bytes()
        assert (len(data), hashlib.sha256(data).hexdigest()) == (entry["bytes"], entry["sha256"])


@pytest.mark.parametrize("authority", ["owns", "approved"])
def test_broader_nft_roles_scope_is_distinct_from_gateway_retained_ownership(authority):
    record = artifact("native/base/foreign-" + authority)
    assert record["authority"] == authority
    assert record["gateway_refused"] and record["truncated_exit_refused"]
    assert AssetId(record["nft"]).chain == ChainId("eip155:8453")
    assert all(record[step]["status"] == "0x1" for step in ("mint", "collect", "close"))
    assert any(value["amount"] > 0 for value in record["safe_cash_received"])


def test_direct_lp_roles_attacks_have_mined_reverted_receipts():
    rejected = artifact("native/base/forbidden-lp")
    assert len(rejected) == len({entry["case"] for entry in rejected}) == 17
    assert all(entry["receipt"]["status"] == "0x0" for entry in rejected)
    assert "wrong mint spacing" in {entry["case"] for entry in rejected}
    assert "nonzero initialization price" in {entry["case"] for entry in rejected}
