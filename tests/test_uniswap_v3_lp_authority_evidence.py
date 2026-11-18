"""Retained installed Roles outcomes preserve broader NFT authority and cleanup."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.caip import AssetId, AssetType, ChainId
from almanak.connectors.uniswap_v3 import permissions
from almanak.connectors.uniswap_v3.state import Checkpoint
from tests.exchange_scope import without_exchange_budgets
from tests.fork.test_uniswap_lp_cleanup import assert_allocation_cleanup, assert_closed_effects
from tests.qa.frozen_reference import archived_reference
from tests.scope_trees import assert_same_scope
from tests.strategy_fork.v3_pool_assertions import (
    assert_closed_position,
    assert_operations,
    assert_payer,
    assert_successful_outcomes,
    assert_wallet_effects,
    position_effects,
)

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/uniswap-v3-lp-authority"
MANIFEST = json.loads((EVIDENCE / "manifest.json").read_text())
CASES = tuple(row for row in MANIFEST["cases"] if row["status"] == "passed")


def artifact(name):
    row = next(row for row in MANIFEST["files"] if row["path"] == name)
    raw = (EVIDENCE / name).read_bytes()
    assert len(raw) == row["bytes"] and hashlib.sha256(raw).hexdigest() == row["sha256"]
    return gzip.decompress(raw)


@pytest.mark.parametrize("case", CASES, ids=lambda row: row["case"])
def test_actual_lp_and_cleanup_references_keep_exact_fund_and_payer_effects(case):
    prefix = "native/" + case["case"] + "/"
    state = Checkpoint.model_validate_json(
        json.dumps(without_exchange_budgets(json.loads(artifact(prefix + "checkpoint.json.gz"))))
    )
    record = json.loads(artifact(prefix + "result.json.gz"))
    assert state.strategy == archived_reference(EVIDENCE, "tested-source.tar.gz", case["strategy"])
    assert (state.mode, state.pending) == ("closed", None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    if case["account_profile"] is not None:
        assert_cleanup_case(state, record)
    else:
        assert_lp_case(state, record)


@pytest.mark.parametrize(
    "case", [row for row in CASES if row["account_profile"] != "eoa"], ids=lambda row: row["case"]
)
def test_installed_lp_grants_keep_every_scope_word(case):
    """Scope-only derivation (blueprint 27) keeps the installed scope by value.

    Only author deposit and swap bounds with their allowances, and the two pinned mint tick
    words, differ from what the fork installed.
    """
    raw = json.loads(artifact("native/" + case["case"] + "/checkpoint.json.gz"))
    state = Checkpoint.model_validate_json(json.dumps(without_exchange_budgets(raw)))
    assert state.permissions is not None
    for partition in state.permissions.partitions:
        grants = permissions.derive(partition.profile, state.capabilities)
        assert any(grant.signature.startswith("mint(") for grant in grants)
        assert_same_scope(grants, partition.grants)


def assert_cleanup_case(state, record):
    before = Checkpoint.model_validate_json(
        json.dumps(without_exchange_budgets(record["interrupted_checkpoint"]))
    )
    assert_closed_effects(before, state)
    for owned, initial, final in zip(state.owned, state.baseline, state.closure, strict=True):
        assert_allocation_cleanup(owned, initial, final)
        assert_payer(state, owned.target, initial, final, position_effects(state, owned.target))


def assert_lp_case(state, record):
    assert state.permissions is not None
    assert "generated_permissions_installed_and_verified" in record["phases"]
    assert "submitted_pending_identity_survived_runtime_and_gateway_replacement" in record["phases"]
    for owned, initial, final in zip(state.owned, state.baseline, state.closure, strict=True):
        effects = position_effects(state, owned.target)
        assert_closed_position(owned, final)
        assert_operations(owned.target, effects)
        assert_wallet_effects(effects, initial, final)
        assert_payer(state, owned.target, initial, final, effects)


@pytest.mark.parametrize("authority", ["owns", "approved"])
def test_real_foreign_nft_succeeds_on_chain_but_gateway_refuses_it(authority):
    record = json.loads(artifact("authority/foreign-" + authority + ".json.gz"))
    assert record["authority"] == authority
    assert record["gateway_refused"] and record["truncated_exit_refused"]
    nft = AssetId(record["nft"])
    assert (nft.chain, nft.asset_type.namespace) == (ChainId("eip155:1"), "erc721")
    for step in ("mint", "collect", "close"):
        assert int(record[step]["status"], 16) == 1
    received = record["safe_cash_received"]
    assert len({AssetType(item["asset"]) for item in received}) == 2
    assert all(item["amount"] > 0 for item in received)
