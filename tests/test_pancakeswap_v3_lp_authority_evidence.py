"""Sealed BNB Safe LP evidence remains reproducible through current connector checks."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.caip import AssetId, ChainId
from almanak.connectors.pancakeswap_v3 import permissions
from almanak.connectors.pancakeswap_v3.state import Checkpoint
from tests.exchange_scope import without_exchange_budgets
from tests.pancake_frozen_source import recaptured_reference
from tests.scope_trees import assert_same_scope
from tests.test_pancakeswap_v3_native import (
    candidate_binding as candidate_binding,
)
from tests.test_pancakeswap_v3_native import (
    validate_recorded_case,
)

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/pancakeswap-v3-bnb-safe-lp"
# The native lifecycle recaptured after #160 moved the mint and increase
# minimums to the edge of the admitted price window; the authority captures stay sealed.
RECAPTURED = EVIDENCE.with_name(EVIDENCE.name + "-2026-10-20")


def artifact(name, package=EVIDENCE):
    return json.loads(gzip.decompress((package / (name + ".json.gz")).read_bytes()))


def test_sealed_safe_lp_source_and_execution_artifacts_have_exact_checksums():
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for entry in manifest["files"]:
        data = (EVIDENCE / entry["path"]).read_bytes()
        assert (len(data), hashlib.sha256(data).hexdigest()) == (entry["bytes"], entry["sha256"])


def test_actual_safe_lp_reference_revalidates_all_operations_and_reports(candidate_binding):
    validate_recorded_case(
        RECAPTURED / "native",
        ROOT / "strategies/pancakeswap_v3_bnb_lp/strategy.py",
        recaptured_reference(RECAPTURED),
    )
    result = artifact("native/result", RECAPTURED)
    assert result["status"] == "passed"
    assert {
        "generated_permissions_installed_and_verified",
        "submitted_pending_identity_survived_runtime_and_gateway_replacement",
        "entered_and_held",
        "runtime_resumed",
        "gateway_replaced_and_resumed",
        "closed",
    } <= set(result["phases"])


def test_installed_lp_grants_keep_every_scope_word():
    """Scope-only derivation (blueprint 27) keeps the installed scope by value.

    Only author deposit and swap bounds with their allowances, and the two pinned mint tick
    words, differ from what the fork installed.
    """
    raw = artifact("native/checkpoint")
    state = Checkpoint.model_validate_json(json.dumps(without_exchange_budgets(raw)))
    assert state.permissions is not None
    for partition in state.permissions.partitions:
        grants = permissions.derive(partition.profile, state.capabilities)
        assert any(grant.signature.startswith("mint(") for grant in grants)
        assert_same_scope(grants, partition.grants)


@pytest.mark.parametrize("authority", ["owns", "approved"])
def test_broader_nft_roles_scope_is_distinct_from_gateway_retained_ownership(authority):
    record = artifact("authority/foreign-" + authority)
    assert record["authority"] == authority
    assert record["gateway_refused"] and record["truncated_exit_refused"]
    assert AssetId(record["nft"]).chain == ChainId("eip155:56")
    assert all(record[step]["status"] == "0x1" for step in ("mint", "collect", "close"))
    assert any(value["amount"] > 0 for value in record["safe_cash_received"])


def test_direct_lp_roles_attacks_have_mined_reverted_receipts():
    rejected = artifact("authority/forbidden-lp")
    assert len(rejected) == len({entry["case"] for entry in rejected}) == 16
    assert all(entry["receipt"]["status"] == "0x0" for entry in rejected)
