"""Contract tests for the local QA Coverage store, planner, seal, and Lab."""

from __future__ import annotations

import ast
import hashlib
import html
import importlib.util
import io
import json
import re
import shutil
import sqlite3
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from eth_utils import keccak

from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES
from almanak.framework.data.qa.production_identity import (
    ObservationProvenance,
    TokenObservation,
    TokenRequirement,
    derive_production_requirements,
    requirements_digest,
)
from scripts.qa.permission_attestation import derive_permission_attestation

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_DIR = REPO_ROOT / "scripts" / "quant-test"
REAL_CATALOG = REPO_ROOT / "docs" / "internal" / "qa" / "catalog" / "v1" / "cells.yaml"
REJECTION_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "quant_rejection_bundles"
TEST_COMMIT = "a" * 40
TEST_SDK = {
    "commit": TEST_COMMIT,
    "branch": "test",
    "dirty": False,
    "sdk_version": "0.0-test",
    "source": "executing-worktree",
}


def _safe_permission_attestation(chain: str = "arbitrum") -> dict:
    target = "0x" + "55" * 20
    selector = "0xabcdef01"
    return derive_permission_attestation(
        transactions=[{"to": target, "data": selector}],
        manifest={
            "permissions": [
                {
                    "target": target,
                    "function_selectors": [{"selector": selector}],
                    "operation": 0,
                    "send_allowed": False,
                }
            ]
        },
        chain=chain,
    )


def _topic_address(address: str) -> str:
    return "0x" + address.removeprefix("0x").rjust(64, "0")


def _aave_supply_receipt_payload(*, exec_path: str = "safe") -> dict:
    account = "0x" + "11" * 20
    asset = "0x" + "22" * 20
    pool = AAVE_V3_POOL_ADDRESSES["arbitrum"]
    recipient = "0x" + "44" * 20
    amount = 100
    tx_hash = "0x" + "ab" * 32
    payload = {
        "intent_cell_id": f"intent.aave_v3.arbitrum.SUPPLY.anvil.{exec_path}",
        "protocol": "aave_v3",
        "intent": "SUPPLY",
        "chain": "arbitrum",
        "network": "anvil",
        "exec_path": exec_path,
        "artifact_kind": "almanak.intent_receipt_evidence",
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "SUPPLY",
            "asset_reference": asset,
            "amount": "0.0001",
        },
        "status": "PASS",
        "fidelity": {"hard": True, "flags": {"amount_match": True}, "witnesses": [], "notes": []},
        "balance_checks": {"wallet_delta_matches_requested_amount": True},
        "layers": {"receipt": "PASS", "permissions": "PASS" if exec_path == "safe" else "NOT_APPLICABLE"},
        "tx": {"hash": tx_hash, "block_number": 123, "status": 1, "gas_used": 456},
        "raw_receipt": {
            "transactionHash": tx_hash,
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "address": pool,
                    "topics": [
                        "0x" + keccak(text="Supply(address,address,address,uint256,uint16)").hex(),
                        _topic_address(asset),
                        _topic_address(account),
                        _topic_address(account),
                    ],
                    "data": "0x" + _topic_address(account).removeprefix("0x") + f"{amount:064x}" + f"{0:064x}",
                },
                {
                    "address": asset,
                    "topics": [
                        "0x" + keccak(text="Transfer(address,address,uint256)").hex(),
                        _topic_address(account),
                        _topic_address(recipient),
                    ],
                    "data": "0x" + f"{amount:064x}",
                },
            ],
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "lending.v1",
            "intent": "SUPPLY",
            "account": account,
            "asset_address": asset,
            "asset_decimals": 6,
            "resource_address": pool,
            "requested_amount_raw": amount,
            "wallet_before_raw": 1_000,
            "wallet_after_raw": 900,
            "position_before": 0,
            "position_after": amount,
            "parser_amount_raw": amount,
        },
    }
    if exec_path == "safe":
        payload["permission_attestation"] = _safe_permission_attestation()
    return payload


def _write_state_database(bundle: Path, tx_ids: list[str], *, statuses: list[int] | None = None) -> Path:
    """Give a fixture bundle the run-produced artifact a Strategy PASS needs.

    VIB-6712: every other input to the Strategy claim is JSON a producer can
    author by hand, so the sealer confirms the declared lifecycle transactions
    against the run's own state database.  Bundles that legitimately pass carry
    one; forgery fixtures deliberately do not.
    """
    statuses = statuses or [1] * len(tx_ids)
    db_path = bundle / "db.sqlite"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "CREATE TABLE transaction_ledger "
            "(tx_hash TEXT, success INTEGER, extracted_data_json TEXT, deployment_id TEXT)"
        )
        connection.executemany(
            "INSERT INTO transaction_ledger VALUES (?, ?, ?, ?)",
            [(tx_id, status, "{}", "deployment:test") for tx_id, status in zip(tx_ids, statuses, strict=True)],
        )
        connection.commit()
    finally:
        connection.close()
    return db_path


def _write_receipt_reconciliation(
    bundle: Path,
    tx_ids: list[str],
    *,
    statuses: list[int] | None = None,
    async_order_ids: list[str] | None = None,
) -> dict:
    statuses = statuses or [1] * len(tx_ids)
    transactions = []
    for tx_id, status in zip(tx_ids, statuses, strict=True):
        transactions.append(
            {
                "tx_hash": tx_id,
                "receipt_status": status,
                "execution_outcome": "SUCCESS" if status == 1 else "REVERTED",
                "raw_receipt": {
                    "transactionHash": tx_id,
                    "blockNumber": "0x64",
                    "blockHash": "0x" + "ab" * 32,
                    "gasUsed": "0x5208",
                    "effectiveGasPrice": "0x3b9aca00",
                    "status": hex(status),
                    "logs": [],
                },
            }
        )
    successful = sorted(tx_id for tx_id, status in zip(tx_ids, statuses, strict=True) if status == 1)
    reverted = sorted(tx_id for tx_id, status in zip(tx_ids, statuses, strict=True) if status == 0)
    payload = {
        "canonical_hashes": sorted(tx_ids),
        "intents": [{"intent_id": "intent-1", "transactions": transactions}],
        "submission_receipt_integrity": {
            "async_order_ids": sorted(async_order_ids or []),
            "reverted_transaction_hashes": reverted,
            "status": "PASS" if not reverted else "FAIL",
            "submitted_transaction_count": len(tx_ids),
            "successful_transaction_hashes": successful,
            "terminal_receipt_count": len(tx_ids),
        },
    }
    (bundle / "receipt-reconciliation.json").write_text(json.dumps(payload))
    return payload


def _write_quant_audit_decision(
    qa,
    bundle: Path,
    *,
    status: str = "PASS",
    required_claims: list[str] | None = None,
) -> dict:
    required_claims = required_claims or ["strategy"]
    transcript = bundle / "audit.md"
    if not transcript.is_file():
        transcript.write_text("Independent quant audit transcript\n")
    evidence_digest, inventory = qa.evidence_set_digest(bundle)
    inventory_by_path = {row["path"]: row for row in inventory}
    claims = {}
    for axis in required_claims:
        claim = {"status": status, "measurements": 1, "reason_codes": []}
        if status == "FAIL":
            witness = inventory_by_path["finding.json"]
            claim.update(reason_codes=["measured_product_failure"], failure_evidence=[witness])
        claims[axis] = claim
    contract = bundle / "lifecycle-contract.json"
    payload = {
        "schema_version": 1,
        "evidence_kind": "almanak.qa.quant-audit-decision",
        "audit_verdict": "AUDIT_CONFIRMED",
        "seal_eligible": True,
        "experiment_completed": True,
        "evidence_set_sha256": evidence_digest,
        "lifecycle_contract_sha256": hashlib.sha256(contract.read_bytes()).hexdigest(),
        "required_claims": required_claims,
        "auditor": {
            "role": "quant-admission-auditor",
            "run_id": "independent-audit-1",
            "identity": "test-quant-auditor",
            "transcript_path": "audit.md",
            "transcript_sha256": hashlib.sha256(transcript.read_bytes()).hexdigest(),
        },
        "claims": claims,
    }
    (bundle / "audit-decision.json").write_text(json.dumps(payload))
    return payload


def _write_minimal_official_quant_bundle(qa, bundle: Path) -> None:
    bundle.mkdir()
    tx_id = "0x" + "51" * 32
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "claim_scope": {
            "required": ["strategy"],
            "not_applicable": ["books", "dashboard", "harness"],
        },
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategy.py"],
            "transition_sequence": ["idle -> open"],
        },
        "requirements": [{"id": "open", "phase": "runtime", "intent_type": "LP_OPEN", "min_executed": 1}],
        "teardown": {"required": False},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    contract_digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    (bundle / "lifecycle-coverage.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract_sha256": contract_digest,
                "observations": [
                    {
                        "requirement_id": "open",
                        "phase": "runtime",
                        "intent_type": "LP_OPEN",
                        "executed": 1,
                        "transaction_ids": [tx_id],
                    }
                ],
                "teardown": {"coverage": "not_requested"},
            }
        )
    )
    _write_receipt_reconciliation(bundle, [tx_id])
    _write_state_database(bundle, [tx_id])
    (bundle / "finding.json").write_text(json.dumps({"verdicts": {"strategy": "PASS"}}))
    (bundle / "git.json").write_text(json.dumps(TEST_SDK))
    _write_quant_audit_decision(qa, bundle)


def _write_dedicated_evidence(bundle: Path, *, status: str = "FAIL") -> dict:
    chain = bundle / "chain"
    chain.mkdir(parents=True, exist_ok=True)
    artifacts = []
    tx_hash = "0x" + "ab" * 32
    wallet = "0x" + "11" * 20
    for name, kind, witness in (
        (f"receipt-{tx_hash}.json", "rpc-receipt", {"transactionHash": tx_hash, "status": "0x1"}),
        (f"transaction-{tx_hash}.json", "rpc-transaction", {"hash": tx_hash, "from": wallet}),
        (
            f"balances-{tx_hash}.json",
            "rpc-balance-witness",
            [
                {
                    "status": "PASS",
                    "balance_before_raw": "10",
                    "balance_after_raw": "15",
                    "actual_delta_raw": "5",
                    "transfer_log_delta_raw": "5",
                    "reconciliation_mode": "exact_transfer",
                }
            ],
        ),
    ):
        path = chain / name
        path.write_text(json.dumps(witness) + "\n")
        artifacts.append(
            {
                "kind": kind,
                "path": path.relative_to(bundle).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    stage_names = (
        "compile",
        "execute",
        "receipt_balance",
        "accounting_persistence",
        "snapshot_metrics",
        "teardown",
        "accountant",
    )
    stages = {
        name: {
            "status": "FAIL" if name == "accountant" and status == "FAIL" else "PASS",
            "anchor": f"independent {name} anchor",
            "facts": {"measured": True},
            "artifacts": [],
            "diagnostic": "accountant red" if name == "accountant" and status == "FAIL" else "",
        }
        for name in stage_names
    }
    stages["receipt_balance"]["facts"] = {
        "transaction_count": 1,
        "balance_check_count": 1,
        "ledger_tx_hashes": [tx_hash],
        "witnessed_tx_hashes": [tx_hash],
        "receipt_set_complete": True,
    }
    stages["receipt_balance"]["artifacts"] = artifacts
    payload = {
        "schema_version": 1,
        "artifact_kind": "almanak.accounting_dedicated_evidence",
        "row_id": "lp-uniswap_v3-arbitrum",
        "fixture": "lp",
        "chain": "arbitrum",
        "primitive": "lp",
        "network": "anvil",
        "exec_path": "eoa",
        "deployment_id": "deployment:test",
        "expected_shape": {"LP_OPEN": 1},
        "stage_order": list(stage_names),
        "stages": stages,
        "status": status,
    }
    (bundle / "chain-witnesses.json").write_text(json.dumps({"artifacts": artifacts}) + "\n")
    (bundle / "dedicated-evidence.json").write_text(json.dumps(payload))
    return payload


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def modules():
    qa = _load("qa_coverage", "qa_coverage.py")
    planner = _load("plan_coverage_batch", "plan_coverage_batch.py")
    sync_catalog = _load("sync_qa_catalog", "sync_qa_catalog.py")
    return qa, planner, sync_catalog


def test_composite_intent_receipt_set_requires_every_unique_role(modules) -> None:
    qa, _, _ = modules
    receipt_set = {"decrease": "0x" + "11" * 32, "collect": "0x" + "22" * 32, "burn": "0x" + "33" * 32}
    receipts = [
        {
            "receipt_role": role,
            "transaction_hash": tx_hash.removeprefix("0x"),
            "semantic_verification": {"receipt_set": receipt_set},
        }
        for role, tx_hash in receipt_set.items()
    ]

    qa._validate_composite_semantic_receipts(receipts, nodeid="exact-close")

    with pytest.raises(ValueError, match="bijective and complete"):
        qa._validate_composite_semantic_receipts(receipts[:-1], nodeid="missing-burn")
    duplicate_role = [dict(receipts[0]), dict(receipts[0]), dict(receipts[2])]
    with pytest.raises(ValueError, match="repeats receipt role"):
        qa._validate_composite_semantic_receipts(duplicate_role, nodeid="duplicate-role")


def _identity_bundle(bundle: Path, captured_at: datetime) -> TokenRequirement:
    requirement = next(item for item in derive_production_requirements() if isinstance(item, TokenRequirement))
    raw = {
        "schema_version": 1,
        "requirement": requirement.to_canonical_dict(),
        "pinned_block": {"number": 123, "hash": "0x" + "ab" * 32},
        "calls": [
            {
                "method": "eth_call",
                "chain": requirement.chain,
                "to": requirement.address,
                "data": "0x313ce567",
                "block": "0x7b",
                "result": "0x" + requirement.decimals.to_bytes(32, "big").hex(),
            }
        ],
        "observed": {"decimals": requirement.decimals},
    }
    encoded = (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode()
    artifact_digest = hashlib.sha256(encoded).hexdigest()
    (bundle / "raw").mkdir(parents=True)
    (bundle / "raw" / f"{artifact_digest}.json").write_bytes(encoded)
    observation = TokenObservation(
        requirement_id=requirement.requirement_id,
        chain=requirement.chain,
        address=requirement.address,
        decimals=requirement.decimals,
        provenance=ObservationProvenance(
            collector="gateway_rpc",
            captured_at=captured_at,
            block_number=123,
            block_hash="0x" + "ab" * 32,
            artifact_sha256=artifact_digest,
        ),
    )
    digest = requirements_digest([requirement])
    (bundle / "requirements.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scope": {"chain": requirement.chain, "complete_chain_inventory": False},
                "requirements_sha256": digest,
                "requirements": [requirement.to_canonical_dict()],
            }
        )
    )
    (bundle / "observations.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "chain": requirement.chain,
                "requirements_sha256": digest,
                "pinned_block": {"number": 123, "hash": "0x" + "ab" * 32},
                "captured_at": captured_at.isoformat().replace("+00:00", "Z"),
                "observations": [observation.to_canonical_dict()],
            }
        )
    )
    return requirement


@pytest.fixture
def catalog_path(tmp_path: Path) -> Path:
    path = tmp_path / "cells.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "chains": ["arbitrum", "base"],
                "chain_logos": {"arbitrum": "chains/arbitrum.webp"},
                "defaults": {"networks": ["anvil", "mainnet"], "exec_paths": ["eoa", "safe"]},
                "cells": [
                    {
                        "id": "lp.uniswap_v3.arbitrum.simple",
                        "primitive": "lp",
                        "protocol": "uniswap_v3",
                        "chain": "arbitrum",
                        "lifecycle": "simple",
                        "strategy_path": "strategies/accounting/lp",
                        "intent_surface": "LP_OPEN -> LP_CLOSE",
                        "cost_class": "standard",
                        "protocol_logo": "protocols/uniswap-v3.webp",
                    }
                ],
            },
            sort_keys=False,
        )
    )
    (tmp_path / "assets" / "protocols").mkdir(parents=True)
    (tmp_path / "assets" / "chains").mkdir(parents=True)
    (tmp_path / "assets" / "protocols" / "uniswap-v3.webp").write_bytes(b"RIFFfakeWEBP")
    (tmp_path / "assets" / "chains" / "arbitrum.webp").write_bytes(b"RIFFfakeWEBP")
    return path


def test_expand_cells_includes_exec_path(modules, catalog_path: Path) -> None:
    _, planner, _ = modules
    catalog = planner._load_catalog(catalog_path)

    cells = planner._expand_cells(catalog, "mainnet")

    assert {cell["cell_id"] for cell in cells} == {
        "lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
        "lp.uniswap_v3.arbitrum.simple.mainnet.safe",
    }
    assert {cell["exec_path"] for cell in cells} == {"eoa", "safe"}


def test_radius_protocol_mapping_is_catalog_derived_and_separator_bound(modules) -> None:
    _, planner, _ = modules
    known = {"gmx_v2", "hyperliquid", "aster", "curve", "morpho_blue"}
    paths = [
        "docs/internal/gmx-readiness/plan.md",
        "almanak/connectors/hyperliquid/provider.py",
        "tests/intents/arbitrum/test_aster_perp.py",
        "almanak/framework/yield_curve_model.py",
    ]
    assert planner._protocols_for_changed_paths(paths, known) == {"gmx_v2", "hyperliquid", "aster"}


def _intent_catalog_stub(
    *,
    line: int = 24,
    nodeid: str = "tests/intents/arbitrum/test_supply.py::TestSupply::test_supply_safe",
    contract: str = "supply.v1",
    extra_cells: list | None = None,
) -> dict:
    """A minimal catalog shaped like the real Intent catalog's hashed fields."""
    return {
        "schema_version": 1,
        "generated_at": "2026-08-19T00:00:00Z",
        "source": "scripts/ci/check_intent_coverage.py (VIB-4303)",
        "chains": ["arbitrum"],
        "cells": [
            {
                "id": "intent.aave_v3.arbitrum.SUPPLY",
                "protocol": "aave_v3",
                "intent": "SUPPLY",
                "chain": "arbitrum",
                "presence": "covered",
                "test_paths": {"safe": ["tests/intents/arbitrum/test_supply.py"], "eoa": []},
                "proof_recipe": {
                    "schema_version": 1,
                    "roles": [
                        {
                            "role": "positive_runtime",
                            "cardinality": "exactly_one",
                            "nodes": [
                                {
                                    "proof_id": "intent.aave_v3.arbitrum.SUPPLY.safe.positive_runtime.v1",
                                    "nodeid": nodeid,
                                    "line": line,
                                    "protocol": "aave_v3",
                                    "intent": "SUPPLY",
                                    "chain": "arbitrum",
                                    "exec_path": "safe",
                                    "role": "positive_runtime",
                                    "recipe_version": 1,
                                    "contract_profile": contract,
                                }
                            ],
                        }
                    ],
                },
            },
            *(extra_cells or []),
        ],
    }


def test_intent_catalog_fingerprint_ignores_source_position_but_not_identity(modules) -> None:
    qa, _, _ = modules
    baseline = qa.intent_catalog_fingerprint(_intent_catalog_stub())

    # A declaration that merely moved down its file is the same cell. Hashing the
    # line number made every unrelated test-tree edit paint MAP DRIFT.
    assert qa.intent_catalog_fingerprint(_intent_catalog_stub(line=91)) == baseline
    rebuilt = _intent_catalog_stub()
    rebuilt["generated_at"] = "2027-01-01T00:00:00Z"
    assert qa.intent_catalog_fingerprint(rebuilt) == baseline

    # Liveness: the gate must still fire on anything that redefines the universe.
    renamed = _intent_catalog_stub(nodeid="tests/intents/arbitrum/test_supply.py::TestSupply::test_renamed")
    assert qa.intent_catalog_fingerprint(renamed) != baseline
    recontracted = _intent_catalog_stub(contract="supply.v2")
    assert qa.intent_catalog_fingerprint(recontracted) != baseline
    widened = _intent_catalog_stub(extra_cells=[{"id": "intent.aave_v3.arbitrum.WITHDRAW", "presence": "gap"}])
    assert qa.intent_catalog_fingerprint(widened) != baseline

    # The locator stays in the catalog document; the Lab deep-links source with it.
    catalog = _intent_catalog_stub()
    qa.intent_catalog_fingerprint(catalog)
    assert catalog["cells"][0]["proof_recipe"]["roles"][0]["nodes"][0]["line"] == 24

    # A catalog without cells is refused, never fingerprinted as an empty universe.
    with pytest.raises(ValueError, match="empty universe"):
        qa.intent_catalog_fingerprint({"schema_version": 1, "chains": ["arbitrum"]})


def test_accounting_score_requires_complete_pass_set(modules) -> None:
    qa, _, _ = modules
    assert qa._score_status({"passed": 1, "failed": 0, "xfailed": 0, "total": 22}) == "ERROR"
    assert qa._score_status({"passed": 22, "failed": 0, "xfailed": 0, "total": 22}) == "PASS"


def test_fee_extract_preserves_unmeasured_and_unknown_lane(modules) -> None:
    qa, _, _ = modules
    fees = qa._fee_extract(
        {
            "ledger": [
                {"cycle_id": None, "gas_usd": None, "gas_used": 21_000, "tx_hash": "0x1"},
                {"cycle_id": "teardown-1", "gas_usd": "0.0100", "gas_used": 30_000, "tx_hash": "0x2"},
            ],
            "events": [],
        }
    )
    assert fees["rows"][0]["lane"] == "unknown"
    assert fees["rows"][0]["usd"] is None
    assert fees["rows"][0]["measured"] is False
    assert fees["gas_by_lane"]["unknown"]["null_rows"] == 1
    assert fees["gas_by_lane"]["teardown"]["measured_usd"] == "0.0100"


def test_missing_pnl_is_explicit_absent_evidence(modules) -> None:
    qa, _, _ = modules
    pnl = qa._pnl_extract({})
    assert pnl["evidence_status"] == "ABSENT"
    assert pnl["wallet_pnl_usd"] is None
    assert pnl["null_buckets"] == ["g6_decomposition"]


def test_lab_home_is_reachable_from_every_board_and_states_the_rules(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"

    qa.bootstrap_store(store, catalog_path)
    qa.render_lab(store=store, catalog_path=store / "catalog" / "cells.yaml")

    home = (store / "lab" / "home.html").read_text(encoding="utf-8")
    for anchor in ('id="philosophy"', 'id="guide"', 'id="architecture"'):
        assert anchor in home
    # The rules the store actually enforces, not decorative prose.
    assert "Refusal beats a false green" in home
    assert "Evidence is forward-only" in home
    assert "A result belongs to one cell universe" in home
    assert "MAP DRIFT" in home
    assert "intent-run" in home
    # The home page carries the canonical Lab chrome like every other board.
    assert 'data-active-page="home.html"' in home
    assert 'href="evidence.html">Evidence</a>' in home
    assert 'href="index.html">Today</a>' in home

    # The logo is the route to it, from every rendered board.
    boards = sorted(path.name for path in (store / "lab").glob("*.html"))
    assert "home.html" in boards
    for name in boards:
        page = (store / "lab" / name).read_text(encoding="utf-8")
        assert '<a class="logo" href="home.html"' in page, f"{name} cannot reach the Lab home"


def test_bootstrap_renders_empty_lab_with_all_four_views(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"

    qa.bootstrap_store(store, catalog_path)
    output = qa.render_lab(store=store, catalog_path=store / "catalog" / "cells.yaml")

    for name in qa.INDEX_NAMES:
        assert json.loads((store / "index" / name).read_text()) == {}
    assert json.loads((store / "index" / qa.TICKET_INDEX_NAME).read_text()) == {
        "schema_version": 1,
        "rebuilt_at": None,
        "linear_synced_at": None,
        "tickets": {},
    }
    assert stat.S_IMODE(store.stat().st_mode) == 0o700
    # Quant is one lane of eight and no longer owns the Lab entry point; Today
    # does. Its four axis views moved with it to quant.html.
    assert output.name == "index.html"
    page = (store / "lab" / "quant.html").read_text()
    assert 'data-network="mainnet"' in page
    assert 'data-network="anvil"' in page
    assert 'data-exec="eoa"' in page
    assert 'data-exec="safe"' in page
    assert "No sealed experiment yet" in page
    assert "Local tamper-evident QA Coverage Lab" in page
    assert "Ledger SHA-256" in page
    # Reachability is the property that matters, and it is now carried by the
    # lane switcher rather than by one top-level tab per lane. Asserting the
    # hrefs keeps the guarantee ("every lane is one click away") while letting
    # the chrome stop growing with the lane count.
    for lane_page in (
        "quant.html",
        "intent.html",
        "accounting.html",
        "data.html",
        "demo.html",
        "protocol.html",
        "backtesting.html",
        "ax.html",
    ):
        assert f'href="{lane_page}"' in page, f"{lane_page} unreachable from the Quant board"
    assert 'href="index.html">Today</a>' in page
    assert 'href="evidence.html">Evidence</a>' in page
    assert 'href="readiness.html">Readiness</a>' in page
    # History left the bar; the ledger chip is now its route.
    assert 'class="chip-ledger" href="history.html"' in page
    assert '<a class="logo" href="home.html"' in page
    assert ">AQA</a>" in page
    assert "Almanak QA Lab" in page
    assert '<span class="nav-separator" aria-hidden="true"></span>' in page
    assert 'class="btn ticket-btn" href="tickets.html">QA Tickets</a>' in page
    assert "Tickets filed by QA" not in page
    assert "Strategy + Books + Dashboard" in page
    assert "STRATEGY · execution only" in page
    assert "CAVEAT · not an exact Strategy PASS" in page
    assert "S = exact Strategy PASS" in page
    assert "QA operator pulse" in page
    assert "Observed claims × Urgency × Stability" in page
    assert "this is a count, not a coverage grade" in page
    assert "Needs attention" in page
    assert "Ticketed" in page
    assert "unsupported / not declared" in page
    assert "linked QA tickets" in page
    assert "Prepare re-check" in page
    # The panel prepares a request and never executes; a re-run still moves the
    # cell, by sealing. Both halves must survive rewording.
    assert "This panel only prepares a request" in page
    assert "supersedes the seal shown above" in page
    assert "Force queue" not in page
    assert "u==='PASS'?'pass':u.startsWith('PASS')?'caveat'" in page
    assert '"chains":["arbitrum","base"]' in page
    assert (store / "assets" / "protocols" / "uniswap-v3.webp").read_bytes() == b"RIFFfakeWEBP"
    intent_page = store / "lab" / "intent.html"
    assert intent_page.is_file()
    intent_html = intent_page.read_text()
    assert "Intent Tests answer one question" in intent_html
    assert "How to use this page" in intent_html
    assert "Follow the CTA" in intent_html
    assert 'class="btn ticket-btn" href="tickets.html">QA Tickets</a>' in intent_html
    assert '"source_root":"sources/"' in intent_html
    assert 'target="_blank" rel="noopener"' in intent_html
    assert "Testing gaps" in intent_html
    assert "NO EXACT TEST" in intent_html
    assert "NOT CERTIFIABLE" in intent_html
    assert "NO TEST PATH" in intent_html
    assert "MAINNET RUNNER MISSING" in intent_html
    assert "Broad test source" in intent_html
    assert "Atomic proof recipe" in intent_html
    assert "Sealed runtime result" in intent_html
    assert "Run this exact cell" in intent_html
    assert "Transaction proof" in intent_html
    assert "Audit details" in intent_html
    assert "Review or link an issue" in intent_html
    assert "Issue assessment" in intent_html
    assert "Prepare re-check" in intent_html
    assert ".c-gap,.c-proof-gap{background:linear-gradient(#704019,#3e250f);color:#ffc27d}" in intent_html
    assert '<i class="sw c-proof-gap"></i>NO EXACT TEST' in intent_html
    assert "/test-intent intent.aave_v3.arbitrum.SUPPLY.anvil.safe" in intent_html
    assert "Exact proof recipe" in intent_html
    assert "Broad file coverage cannot paint this cell" in intent_html
    intent_catalog = json.loads((store / "catalog" / qa.INTENT_CATALOG_NAME).read_text())
    source_path = next(
        path
        for cell in intent_catalog["cells"]
        for paths in cell.get("test_paths", {}).values()
        for path in paths
        if (REPO_ROOT / path).is_file()
    )
    source_page = store / "lab" / "sources" / f"{source_path}.html"
    assert source_page.is_file()
    assert "Read-only snapshot · SHA-256" in source_page.read_text()
    ticket_page = store / "lab" / "tickets.html"
    assert ticket_page.is_file()
    assert "QA issue register" in ticket_page.read_text()
    history_page = store / "lab" / "history.html"
    assert history_page.is_file()
    assert "No reproducible experiments sealed yet" in history_page.read_text()
    # Four destinations, not one tab per lane. Lanes are reached through the
    # Evidence switcher, so promoting one back into this tuple is the
    # regression that put a growth axis in a fixed-width bar.
    canonical_nav = (
        ("index.html", "Today"),
        ("evidence.html", "Evidence"),
        ("readiness.html", "Readiness"),
        ("tickets.html", "QA Tickets"),
    )
    for page_name in (
        "index.html",
        "evidence.html",
        "quant.html",
        "demo.html",
        "intent.html",
        "accounting.html",
        "data.html",
        "protocol.html",
        "backtesting.html",
        "ax.html",
        "readiness.html",
        "history.html",
        "tickets.html",
    ):
        rendered = (store / "lab" / page_name).read_text()
        header = rendered.split("</header>", 1)[0].rsplit("<header", 1)[1]
        assert '<a class="logo" href="home.html"' in header
        assert ">AQA</a>" in header
        assert 'class="brand-copy"' in header
        assert 'aria-label="QA Lab"' in header
        # At most one highlighted destination: two would make "you are here"
        # ambiguous. Zero is correct for a page routed outside the bar — home
        # via the logo, History via the ledger chip — and those pages assert
        # their own route below.
        expected_active = 0 if qa._PAGE_DESTINATION[page_name] is None else 1
        assert header.count(' active"') == expected_active, page_name
        positions = [header.index(f'href="{href}">{label}</a>') for href, label in canonical_nav]
        assert positions == sorted(positions)
    for unbarred in ("history.html", "home.html"):
        header = (store / "lab" / unbarred).read_text().split("</header>", 1)[0]
        assert qa._PAGE_DESTINATION[unbarred] is None
        assert '<a class="logo" href="home.html"' in header
    assert 'href="history.html"' in (store / "lab" / "quant.html").read_text()
    assert intent_catalog["summary"]["required"] > 0
    recheck_catalog = json.loads((store / "catalog" / "recheck_routes.json").read_text())
    assert recheck_catalog["mode"] == "render_only_no_dispatch"
    assert recheck_catalog["invariant"] == "A re-check route never changes evidence or support state."
    accounting_page = store / "lab" / "accounting.html"
    assert accounting_page.is_file()
    accounting_html = accounting_page.read_text()
    assert "Dedicated Accounting truth" in accounting_html
    assert "Neither can create a Dedicated PASS" in accounting_html
    # The board still identifies itself, but through the destination map and the
    # lane switcher rather than a top-level tab of its own.
    assert 'data-active-page="accounting.html"' in accounting_html
    assert "Evidence · Accounting" in accounting_html
    accounting_catalog = json.loads((store / "catalog" / qa.ACCOUNTING_CATALOG_NAME).read_text())
    matrix = yaml.safe_load(qa.DEFAULT_ACCOUNTING_MATRIX.read_text())
    matrix_rows = matrix["rows"]
    expected_matrix_cell_count = sum(
        len(json.loads((qa.REPO_ROOT / row["baseline"]).read_text())["cells"]) for row in matrix_rows
    )
    assert accounting_catalog["summary"]["matrix_rows"] == len(matrix_rows)
    expected_profiles = {
        path.parent.name
        for path in (qa.REPO_ROOT / "tests" / "fixtures" / "accounting").glob("*/expected_cells.json")
        if (path.parent / "expected_baseline.sqlite").is_file()
    }
    assert {profile["profile"] for profile in accounting_catalog["profiles"]} == expected_profiles
    assert accounting_catalog["summary"]["frozen_profiles"] == len(expected_profiles)
    assert sum(accounting_catalog["summary"]["matrix_cell_statuses"].values()) == expected_matrix_cell_count
    assert all(profile["total"] > 0 for profile in accounting_catalog["profiles"])
    assert all(sum(profile["counts"].values()) == profile["total"] for profile in accounting_catalog["profiles"])
    # Reference baselines are inventory only; they never paint latest execution.
    assert json.loads((store / "index" / qa.ACCOUNTING_DEDICATED_INDEX).read_text()) == {}
    assert json.loads((store / "index" / qa.ACCOUNTING_CORROBORATION_INDEX).read_text()) == {}
    data_page = store / "lab" / "data.html"
    assert data_page.is_file()
    assert "First-class Data Tests" in data_page.read_text()
    # Same self-identification contract as the Accounting board: the page names
    # itself through the destination map and the lane switcher, not a tab.
    data_html = data_page.read_text()
    assert 'data-active-page="data.html"' in data_html
    assert "Evidence · Data" in data_html
    data_catalog = json.loads((store / "catalog" / qa.DATA_CATALOG_NAME).read_text())
    assert data_catalog["summary"]["contract_cells"] == 24
    assert data_catalog["summary"]["contract_checks"] == 8
    assert data_catalog["summary"]["qa_data_categories"] == 5
    assert data_catalog["summary"]["scheduled_cells"] == 8
    assert data_catalog["summary"]["identity_requirements"] > 0
    assert set(data_catalog["summary"]["identity_requirement_kinds"]) == {
        "direct_chainlink_feed",
        "token",
        "v3_pool",
    }
    assert json.loads((store / "index" / "data_latest.json").read_text()) == {}
    ax_page = store / "lab" / "ax.html"
    assert ax_page.is_file()
    protocol_page = store / "lab" / "protocol.html"
    assert protocol_page.is_file()
    protocol_html = protocol_page.read_text()
    assert "Protocol QA tests the exception" in protocol_html
    assert "SHARED CONTRACT" in protocol_html
    assert "TEST PIPE MISSING" in protocol_html
    protocol_catalog = json.loads((store / "catalog" / "protocol_cells.json").read_text())
    assert protocol_catalog["summary"]["supported_protocols"] > 0
    assert protocol_catalog["summary"]["family_contracts"] == protocol_catalog["summary"]["supported_protocols"]
    assert protocol_catalog["summary"]["custom_pipeline_gaps"] > 0
    assert all(
        (store / "lab" / row["detail_path"]).is_file()
        for row in protocol_catalog["protocols"]
        if row["strategy_supported"]
    )
    assert json.loads((store / "index" / "protocol_latest.json").read_text()) == {}
    backtest_page = store / "lab" / "backtesting.html"
    assert backtest_page.is_file()
    assert "Backtesting truth by evidence tier" in backtest_page.read_text()
    backtest_catalog = json.loads((store / "catalog" / "backtest_cells.json").read_text())
    assert backtest_catalog["summary"] == {
        "registered_cells": len(backtest_catalog["cells"]),
        "tiers": len(backtest_catalog["tiers"]),
    }
    assert backtest_catalog["summary"]["registered_cells"] >= 40
    assert json.loads((store / "index" / "backtest_latest.json").read_text()) == {}
    assert "ax product evidence, fail-closed" in ax_page.read_text()
    ax_catalog = json.loads((store / "catalog" / "ax_cells.json").read_text())
    assert ax_catalog["summary"]["tools"] > 0
    assert json.loads((store / "index" / "ax_latest.json").read_text()) == {}


def test_lab_refuses_to_render_an_invalid_history_ledger(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    (store / "index" / "experiment_runs.jsonl").write_text('{"not":"a valid chained record"}\n')

    with pytest.raises(ValueError):
        qa.render_lab(store=store, catalog_path=store / "catalog" / "cells.yaml")


def test_history_lab_surfaces_legacy_volatile_warning(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    history = qa._load_history_module()
    store = tmp_path / "store"
    run = store / "quant" / "2026" / "08" / "10" / "legacy-benqi"
    run.mkdir(parents=True)
    (run / "result.txt").write_text("PASS\n")
    manifest = run / "manifest.json"
    manifest.write_text(json.dumps({"run_id": "legacy-benqi", "sdk": TEST_SDK}) + "\n")
    record = history.append_experiment(
        store=store,
        surface="quant",
        run_id="legacy-benqi",
        run_dir=run,
        manifest_path=manifest,
        sdk=TEST_SDK,
        cell_verdicts={"lending.benqi.avalanche": "PASS"},
        completed_at="2026-08-10T01:00:00Z",
        sealed_at="2026-08-10T01:00:00Z",
        admission={"status": "OFFICIAL"},
    )
    sidecar = run / "db.sqlite-shm"
    sidecar.write_bytes(b"legacy")
    sidecar_row = {
        "relpath": sidecar.relative_to(store).as_posix(),
        "bytes": sidecar.stat().st_size,
        "sha256": history._sha256_file(sidecar),
    }
    record["artifacts"] = sorted([*record["artifacts"], sidecar_row], key=lambda row: row["relpath"])
    record["artifact_set_sha256"] = history._sha256_bytes(history._canonical_bytes(record["artifacts"]))
    record["record_sha256"] = history._record_digest(record)
    (store / "index" / history.HISTORY_LEDGER_NAME).write_text(
        json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
    )
    sidecar.unlink()

    page = qa.render_history_lab(store=store).read_text()

    assert "PASS WITH WARNINGS" in page
    assert "Legacy volatile warning" in page
    assert "db.sqlite-shm" in page
    assert "not reproducibility-verified" in page
    assert "Stable artifacts verified" in page


def test_history_lab_renders_unadmitted_partial_rows_as_forensic_never_scientific(modules, tmp_path: Path) -> None:
    """The page carried one banner -- "Scientific record" -- over every row.

    An unadmitted PARTIAL/UNVERIFIED observation was rendered with a verdict
    badge, folded into the cell's run count, and allowed to break the PASS
    streak: three grades on a result that was never admitted as official
    history. Reproduced at 21256be6f.
    """
    qa, _, _ = modules
    history = qa._load_history_module()
    store = tmp_path / "store"

    def seal(run_id: str, verdict: str, hour: int) -> None:
        run = store / "quant" / "2026" / "08" / "10" / run_id
        run.mkdir(parents=True)
        (run / "result.txt").write_text(f"{verdict}\n")
        manifest = run / "manifest.json"
        manifest.write_text(json.dumps({"run_id": run_id, "sdk": TEST_SDK}) + "\n")
        stamp = f"2026-08-10T{hour:02d}:00:00Z"
        history.append_experiment(
            store=store,
            surface="intent",
            run_id=run_id,
            run_dir=run,
            manifest_path=manifest,
            sdk=TEST_SDK,
            cell_verdicts={"lending.aave_v3.base": verdict},
            completed_at=stamp,
            sealed_at=stamp,
            admission=None,
        )

    seal("graded-pass", "PASS", 1)
    seal("unadmitted-partial", "PARTIAL", 3)

    page = qa.render_history_lab(store=store).read_text()

    # The blanket "Scientific record." claim over the whole page is gone.
    assert "<b>Scientific record.</b>" not in page
    # The forensic row is named as such, at the row and at the page level.
    assert "PARTIAL · FORENSIC" in page
    assert "Forensic record — not official history." in page
    assert "1 forensic record(s) on this page." in page
    # ...and it is not graded.
    assert "1 graded runs · 1 forensic records · 0 regressions · PASS streak 1" in page
    # The admitted-shaped row keeps its grade.
    assert "PASS · FORENSIC" not in page


def test_intent_junit_seal_maps_results_and_repaints_lab(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    inventory = qa.build_intent_catalog()
    cell = next(
        cell
        for cell in inventory["cells"]
        if cell["protocol"] == "aave_v3" and cell["intent"] == "SUPPLY" and cell["chain"] == "arbitrum"
    )
    nodeid = "tests/intents/arbitrum/test_aave_v3_lending.py::TestAaveV3SupplyIntent::test_supply_usdc_using_intent"
    junit = tmp_path / "results.xml"
    junit.write_text(
        '<testsuite tests="1"><testcase '
        'classname="tests.intents.arbitrum.test_aave_v3_lending.TestAaveV3SupplyIntent" '
        'name="test_supply_usdc_using_intent" time="0.5" /></testsuite>'
    )
    evidence = tmp_path / "evidence"
    receipts = evidence / "receipts"
    receipts.mkdir(parents=True)
    cell_id = f"{cell['id']}.anvil.safe"
    receipt_base = {
        **_aave_supply_receipt_payload(),
        "balance_deltas": {"token": {"symbol": "USDC", "before": 1000, "after": 900, "delta": -100}},
        "source_provenance": {
            "kind": "sealed-quant-runtime",
            "quant_run_id": "quant-source-run",
            "quant_store_path": "quant/2026/08/04/quant-source-run",
            "ledger_entry_id": "ledger-1",
            "accounting_event_id": "accounting-1",
        },
    }
    (receipts / "01-hard.json").write_text(
        json.dumps(
            {
                **receipt_base,
                "fidelity": {
                    "hard": True,
                    "flags": {"amount_match": True},
                    "witnesses": [{"kind": "wallet_balance_delta"}],
                    "notes": [],
                },
            }
        )
    )
    (receipts / "02-soft.json").write_text(
        json.dumps(
            {
                **receipt_base,
                "fidelity": {
                    "hard": True,
                    "flags": {"amount_match": True},
                    "witnesses": [{"kind": "independent_value_flow"}],
                    "notes": [],
                },
            }
        )
    )
    (evidence / "evidence-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "nodes": [
                    {
                        "nodeid": nodeid,
                        "outcome": "PASS",
                        "duration_seconds": 0.5,
                        "intents": [
                            {
                                "intent_cell_id": cell_id,
                                "protocol": "aave_v3",
                                "intent": "SUPPLY",
                                "chain": "arbitrum",
                                "network": "anvil",
                                "exec_path": "safe",
                                "outcome_class": "hard-pass",
                                "receipt_expected": True,
                                "receipt_artifacts": [
                                    {"path": "receipts/02-soft.json"},
                                    {"path": "receipts/01-hard.json"},
                                ],
                            }
                        ],
                    }
                ],
            }
        )
    )

    target = qa.seal_intent_junit(
        junit=junit,
        store=store,
        catalog_path=catalog_path,
        chain=cell["chain"],
        network="anvil",
        exec_path="safe",
        evidence_dir=evidence,
        run_id="intent-test-run",
        sdk_provenance=TEST_SDK,
        now=datetime(2026, 8, 4, 12, 0, tzinfo=UTC),
    )

    latest = json.loads((store / "index" / "intent_latest.json").read_text())
    assert latest[cell_id]["status"] == "PASS"
    assert latest[cell_id]["schema_version"] == 2
    assert latest[cell_id]["attribution_mode"] == "exact-runtime"
    intent_catalog = json.loads((store / "catalog" / "intent_cells.json").read_text())
    assert latest[cell_id]["catalog_sha256"] == intent_catalog["catalog_sha256"]
    assert latest[cell_id]["nodeids"] == [nodeid]
    assert latest[cell_id]["receipt_counts"] == {"fail": 0, "hard": 2, "soft": 0}
    assert latest[cell_id]["evidence_status"] == "COMPLETE"
    assert latest[cell_id]["contract_status"] == "VERIFIED"
    assert latest[cell_id]["layers"] == {"permissions": "PASS", "receipt": "PASS", "semantic_contract": "PASS"}
    assert len(latest[cell_id]["receipt_paths"]) == 2
    assert latest[cell_id]["receipt_path"].endswith("/receipts/01-hard.json")
    assert latest[cell_id]["receipts"][0]["decode_path"].endswith("/receipts/01-hard-decode.html")
    assert latest[cell_id]["receipts"][0]["source_provenance"]["quant_run_id"] == "quant-source-run"
    assert not any(".BORROW." in key or ".REPAY." in key or ".WITHDRAW." in key for key in latest)
    assert latest[cell_id]["last_pass_at"] == "2026-08-04T12:00:00Z"
    assert latest[cell_id]["report_path"].endswith("/report.html")
    assert "Intent test report" in {artifact["label"] for artifact in latest[cell_id]["artifacts"]}
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["catalog_sha256"] == intent_catalog["catalog_sha256"]
    assert "Intent test report" in {artifact["label"] for artifact in manifest["artifacts"]}
    assert (target / "results.xml").is_file()
    assert (target / "summary.json").is_file()
    assert (target / "report.html").is_file()
    run_report = (target / "report.html").read_text()
    assert "Claim boundary" in run_report
    assert "Exact verification nodes" in run_report
    assert "Observed bilateral balances" in run_report
    assert "Human receipt verdict" in run_report
    assert "Claim-level fidelity checklist" in run_report
    assert "Back to Intent Lab" in run_report
    assert "wallet_balance_delta" in run_report
    assert "0x" + "ab" * 32 in run_report
    assert 'href="receipts/01-hard-decode.html"' in run_report
    assert 'href="manifest.json"' in run_report
    assert "sealed quant run quant-source-run" in run_report
    assert "Open source quant report" in run_report
    decode_html = (target / "receipts" / "01-hard-decode.html").read_text()
    assert "Almanak parser" in decode_html
    assert "Independent explorer-style log walk" in decode_html
    assert "Claim-level fidelity checklist" in decode_html
    assert "Human receipt verdict" in decode_html
    assert "Inspect raw parser and explorer payloads" in decode_html
    assert 'class="raw-detail"' in decode_html
    assert "Back to Intent Lab" in decode_html
    assert 'href="01-hard.json"' in decode_html
    # The tx hash sits on its own line; on Anvil there is no explorer to link to.
    assert 'class="txline"' in decode_html
    assert "no public explorer URL" in decode_html
    assert "Open chain explorer" not in decode_html
    intent_html = (store / "lab" / "intent.html").read_text()
    assert cell_id in intent_html
    assert "DECODE ×${decodeCount}" in intent_html
    assert "JUnit PASS alone does not create DECODE" in intent_html
    assert "VERIFIED" in intent_html
    assert "LEGACY" in intent_html
    assert "LIVE ENVELOPE GAP" in intent_html
    assert "row.mainnet_envelope_status==='VERIFIED'" in intent_html
    assert "Non-green sealed" in intent_html
    assert "status==='PASS'&&exact&&evidence==='COMPLETE'" in intent_html
    assert "Protocol sign-off · strongest exact chain" in intent_html
    assert "EOA + Safe" in intent_html
    assert "latest-per-cell aggregate" in intent_html
    assert '"human_summary"' in intent_html
    assert "Open source quant report" in intent_html
    mainnet_cell_id = f"{cell['id']}.mainnet.safe"
    mainnet_manifest = json.loads((evidence / "evidence-manifest.json").read_text())
    mainnet_intent = mainnet_manifest["nodes"][0]["intents"][0]
    mainnet_intent["intent_cell_id"] = mainnet_cell_id
    mainnet_intent["network"] = "mainnet"
    (evidence / "evidence-manifest.json").write_text(json.dumps(mainnet_manifest))
    for name in ("01-hard.json", "02-soft.json"):
        payload = json.loads((receipts / name).read_text())
        payload["intent_cell_id"] = mainnet_cell_id
        payload["network"] = "mainnet"
        payload["tx"] = {**payload["tx"], "explorer_url": "https://arbiscan.io/tx/0xabc"}
        (receipts / name).write_text(json.dumps(payload))
    bad_payload = json.loads((receipts / "02-soft.json").read_text())
    bad_payload["tx"] = {
        **bad_payload["tx"],
        "explorer_url": "http://not-an-explorer-proof.invalid/tx/0xabc",
    }
    (receipts / "02-soft.json").write_text(json.dumps(bad_payload))
    # A fork qa_proof node must never be reused to paint a Mainnet cell. The
    # recipe gate runs before individual receipt URL validation.
    with pytest.raises(ValueError, match="not a registered qa_proof recipe"):
        qa.seal_intent_junit(
            junit=junit,
            store=tmp_path / "invalid-mainnet-store",
            catalog_path=catalog_path,
            chain=cell["chain"],
            network="mainnet",
            exec_path="safe",
            evidence_dir=evidence,
            run_id="invalid-mainnet-evidence",
        )

    with pytest.raises(ValueError, match="Mainnet Intent sealing requires --evidence-dir"):
        qa.seal_intent_junit(
            junit=junit,
            store=tmp_path / "mainnet-store",
            catalog_path=catalog_path,
            chain=cell["chain"],
            network="mainnet",
            exec_path="safe",
            run_id="unproven-mainnet-run",
            sdk_provenance=TEST_SDK,
            now=datetime(2026, 8, 4, 12, 0, tzinfo=UTC),
        )


def test_safe_intent_seal_rejects_missing_permission_closure(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    intent = {
        "intent_cell_id": "intent.aave_v3.arbitrum.SUPPLY.anvil.safe",
        "protocol": "aave_v3",
        "intent": "SUPPLY",
        "chain": "arbitrum",
        "network": "anvil",
        "exec_path": "safe",
    }
    payload = _aave_supply_receipt_payload()
    payload.pop("permission_attestation")

    with pytest.raises(ValueError, match="permission proof is invalid"):
        qa._validate_receipt_payload(
            payload,
            source=tmp_path / "receipt.json",
            intent=intent,
            network="anvil",
            contract_profile="lending.v1",
        )


def test_safe_intent_seal_recomputes_permission_closure(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    intent = {
        "intent_cell_id": "intent.aave_v3.arbitrum.SUPPLY.anvil.safe",
        "protocol": "aave_v3",
        "intent": "SUPPLY",
        "chain": "arbitrum",
        "network": "anvil",
        "exec_path": "safe",
    }
    tampered = _safe_permission_attestation()
    tampered["manifest_grants"][0]["target"] = "0x" + "66" * 20
    payload = _aave_supply_receipt_payload()
    payload["permission_attestation"] = tampered

    with pytest.raises(ValueError, match="not entailed"):
        qa._validate_receipt_payload(
            payload,
            source=tmp_path / "receipt.json",
            intent=intent,
            network="anvil",
            contract_profile="lending.v1",
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["raw_receipt"].update(status=0), "did not succeed"),
        (lambda p: p["tx"].update(hash="0x" + "cd" * 32), "hash is not entailed"),
        (lambda p: p["tx"].update(block_number=124), "block is not entailed"),
        (lambda p: p["fidelity"].update(flags={}), "fidelity.hard is not entailed"),
        (lambda p: p.update(balance_checks={}), "non-empty all-true predicate"),
    ],
    ids=("status-zero", "hash-mismatch", "block-mismatch", "empty-fidelity", "empty-balances"),
)
def test_intent_seal_rederives_transaction_and_measurement_envelope(
    modules, tmp_path: Path, mutation, message: str
) -> None:
    qa, _, _ = modules
    payload = _aave_supply_receipt_payload(exec_path="eoa")
    intent = {key: payload[key] for key in ("intent_cell_id", "protocol", "intent", "chain", "network", "exec_path")}
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        qa._validate_receipt_payload(
            payload,
            source=tmp_path / "receipt.json",
            intent=intent,
            network="anvil",
            contract_profile="lending.v1",
        )


def test_intent_receipt_human_summary_surfaces_parser_metadata_divergence(modules) -> None:
    qa, _, _ = modules
    payload = {
        "intent": "SWAP",
        "tx": {"hash": "0xabc", "block_number": 123},
        "almanak": {
            "parser_method": "parse_swap_receipt",
            "result": {
                "transaction_hash": "",
                "block_number": 0,
                "swap_result": {
                    "amount_in_decimal": "100",
                    "amount_out_decimal": "0.053",
                    "token_in_symbol": "USDC",
                    "token_out_symbol": "WETH",
                },
            },
        },
        "fidelity": {"hard": True, "flags": {"amount_match": True}},
    }

    summary = qa._intent_receipt_human_summary(payload)

    assert summary["headline"] == "Parsed 100 USDC → 0.053 WETH"
    assert summary["checks"] == {"passed": 1, "total": 1, "hard": True}
    assert len(summary["warnings"]) == 2
    assert "transaction_hash" in summary["warnings"][0]
    assert "block number" in summary["warnings"][1]


def test_intent_junit_nodeid_normalizes_real_classname_and_keeps_param_suffix(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    junit = tmp_path / "param.xml"
    junit.write_text(
        '<testsuite tests="1"><testcase '
        'classname="tests.intents.arbitrum.test_aave_v3_lending.TestAaveV3SupplyIntent" '
        'name="test_supply_usdc_using_intent[USDC-100]" time="0.1" /></testsuite>'
    )

    rows, _ = qa._parse_intent_junit(junit)

    assert rows[0]["nodeid"] == (
        "tests/intents/arbitrum/test_aave_v3_lending.py::"
        "TestAaveV3SupplyIntent::test_supply_usdc_using_intent[USDC-100]"
    )


def test_intent_catalog_exec_paths_respect_effective_node_intent_markers(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    mixed = tmp_path / "test_mixed.py"
    mixed.write_text(
        """
import pytest
from almanak.framework.intents.vocabulary import IntentType

@pytest.mark.intent(IntentType.SWAP)
async def test_safe_swap():
    pass

@pytest.mark.no_zodiac(reason="native wrapper")
@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
async def test_eoa_lp_roundtrip():
    pass
"""
    )

    path_intents = qa._intent_file_exec_path_intents(mixed)

    assert path_intents == {"safe": {"SWAP"}, "eoa": {"LP_OPEN", "LP_CLOSE"}}


def test_intent_proof_node_is_atomic_versioned_and_axis_exact(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    proof = tmp_path / "tests" / "intents" / "arbitrum" / "test_exact.py"
    proof.parent.mkdir(parents=True)
    proof.write_text(
        """
import pytest
from almanak.framework.intents.vocabulary import IntentType

class TestSupply:
    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="axis control")
    @pytest.mark.intent(IntentType.SUPPLY)
    async def test_supply(self, intent_evidence):
        pass
"""
    )
    original_root = qa.REPO_ROOT
    qa.REPO_ROOT = tmp_path
    try:
        nodes = qa._intent_file_proof_nodes(proof)
    finally:
        qa.REPO_ROOT = original_root

    assert nodes == [
        {
            "proof_id": "intent.aave_v3.arbitrum.SUPPLY.eoa.positive_runtime.v1",
            "nodeid": "tests/intents/arbitrum/test_exact.py::TestSupply::test_supply",
            "line": 9,
            "protocol": "aave_v3",
            "intent": "SUPPLY",
            "chain": "arbitrum",
            "exec_path": "eoa",
            "role": "positive_runtime",
            "recipe_version": 1,
            "contract_profile": "lending.v1",
        }
    ]


def test_intent_proof_node_rejects_multi_intent_cross_paint(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    proof = tmp_path / "tests" / "intents" / "arbitrum" / "test_ambiguous.py"
    proof.parent.mkdir(parents=True)
    proof.write_text(
        """
import pytest
from almanak.framework.intents.vocabulary import IntentType

@pytest.mark.qa_proof(protocol="aave_v3")
@pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
async def test_roundtrip(intent_evidence):
    pass
"""
    )
    original_root = qa.REPO_ROOT
    qa.REPO_ROOT = tmp_path
    try:
        with pytest.raises(ValueError, match="exactly one effective IntentType"):
            qa._intent_file_proof_nodes(proof)
    finally:
        qa.REPO_ROOT = original_root


def test_intent_proof_node_allows_one_explicit_target_with_setup_intents(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    proof = tmp_path / "tests" / "intents" / "base" / "test_lifecycle.py"
    proof.parent.mkdir(parents=True)
    proof.write_text(
        """
import pytest
from almanak.framework.intents.vocabulary import IntentType

@pytest.mark.qa_proof(protocol="euler_v2", target="WITHDRAW", contract="lending.v1")
@pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
def test_round_trip(intent_evidence):
    pass
"""
    )
    original_root = qa.REPO_ROOT
    qa.REPO_ROOT = tmp_path
    try:
        recipes = qa._intent_file_proof_nodes(proof)
    finally:
        qa.REPO_ROOT = original_root

    assert len(recipes) == 1
    assert recipes[0]["intent"] == "WITHDRAW"
    assert recipes[0]["contract_profile"] == "lending.v1"


def test_aave_supply_exact_cell_plan_selects_one_node_per_execution_path(modules) -> None:
    qa, _, _ = modules
    safe = qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.anvil.safe")
    eoa = qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.anvil.eoa")

    assert safe["proof_recipe"]["nodeids"] == [
        "tests/intents/arbitrum/test_aave_v3_lending.py::TestAaveV3SupplyIntent::test_supply_usdc_using_intent"
    ]
    assert eoa["proof_recipe"]["nodeids"] == [
        "tests/intents/arbitrum/test_aave_v3_lending.py::TestAaveV3SupplyIntent::test_supply_usdc_using_intent_eoa"
    ]
    assert safe["proof_recipe"]["proof_id"] != eoa["proof_recipe"]["proof_id"]


@pytest.mark.parametrize("chain", ["base", "arbitrum", "ethereum"])
@pytest.mark.parametrize("intent", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
@pytest.mark.parametrize("exec_path", ["safe", "eoa"])
def test_aave_lending_exact_cells_have_one_atomic_recipe(
    modules,
    chain: str,
    intent: str,
    exec_path: str,
) -> None:
    qa, _, _ = modules
    cell_id = f"intent.aave_v3.{chain}.{intent}.anvil.{exec_path}"

    plan = qa.intent_cell_plan(cell_id=cell_id)

    assert plan["cell_id"] == cell_id
    assert plan["protocol"] == "aave_v3"
    assert plan["intent"] == intent
    assert plan["proof_recipe"]["contract_profile"] == "lending.v1"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("chain", ["base", "arbitrum", "ethereum"])
@pytest.mark.parametrize("exec_path", ["safe", "eoa"])
def test_uniswap_v3_swap_exact_cells_have_one_atomic_recipe(modules, chain: str, exec_path: str) -> None:
    qa, _, _ = modules
    cell_id = f"intent.uniswap_v3.{chain}.SWAP.anvil.{exec_path}"

    plan = qa.intent_cell_plan(cell_id=cell_id)

    assert plan["cell_id"] == cell_id
    assert plan["proof_recipe"]["contract_profile"] == "swap.v1"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("exec_path", ["safe", "eoa"])
def test_traderjoe_v2_avalanche_swap_has_one_atomic_recipe(modules, exec_path: str) -> None:
    qa, _, _ = modules
    cell_id = f"intent.traderjoe_v2.avalanche.SWAP.anvil.{exec_path}"

    plan = qa.intent_cell_plan(cell_id=cell_id)

    assert plan["cell_id"] == cell_id
    assert plan["proof_recipe"]["contract_profile"] == "liquidity_book_swap.v1"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


def test_traderjoe_v2_avalanche_mainnet_eoa_uses_reviewed_recipe(modules) -> None:
    qa, _, _ = modules
    inventory = qa.build_intent_catalog()
    cell = next(
        row
        for row in inventory["cells"]
        if row["protocol"] == "traderjoe_v2" and row["chain"] == "avalanche" and row["intent"] == "SWAP"
    )
    recipes = [row for row in cell["mainnet_recipes"] if row["exec_path"] == "eoa"]

    assert len(recipes) == 1
    assert recipes[0]["contract_profile"] == "liquidity_book_swap.v1"
    assert recipes[0]["source"] == "scripts/quant-test/run_mainnet_intent.py"


@pytest.mark.parametrize("chain", ["base", "arbitrum", "ethereum"])
@pytest.mark.parametrize("intent", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
# ``safe`` is deliberately absent. The Intent catalog derives proof nodes by
# AST-scanning ``qa_proof`` markers, so the Euler safe cells were backed by the
# exact-Safe proofs that ship with the connector's synthetic permission
# discovery. That capability was lifted out of this PR, so asserting a safe
# recipe here would declare coverage this branch cannot back. Both return
# together in docs/internal/qa/pr-3582-sibling-patches/B-euler-v2-permission-discovery.patch.
@pytest.mark.parametrize("exec_path", ["eoa"])
def test_euler_v2_lending_cells_have_one_atomic_recipe(
    modules,
    chain: str,
    intent: str,
    exec_path: str,
) -> None:
    qa, _, _ = modules
    cell_id = f"intent.euler_v2.{chain}.{intent}.anvil.{exec_path}"

    plan = qa.intent_cell_plan(cell_id=cell_id)

    assert plan["cell_id"] == cell_id
    assert plan["proof_recipe"]["contract_profile"] == "lending.v1"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


def test_exact_cell_plan_rejects_broad_coverage_and_live_without_runner(modules) -> None:
    qa, _, _ = modules
    with pytest.raises(ValueError, match="requires exactly one positive_runtime proof node"):
        qa.intent_cell_plan(cell_id="intent.across.arbitrum.BRIDGE.anvil.safe")
    with pytest.raises(ValueError, match="requires exactly one positive_runtime proof node"):
        qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.mainnet.safe")


def test_aave_arbitrum_supply_mainnet_eoa_has_one_versioned_runner_recipe(modules) -> None:
    qa, _, _ = modules

    plan = qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa")

    assert plan["proof_recipe"]["proof_id"] == "aave_v3.supply.arbitrum.eoa"
    assert plan["proof_recipe"]["contract_profile"] == "lending.v1"
    assert plan["proof_recipe"]["runner"] == "scripts/quant-test/run_mainnet_intent.py"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("intent", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
def test_aave_arbitrum_mainnet_eoa_cells_have_exact_runner_recipes(modules, intent: str) -> None:
    qa, _, _ = modules

    plan = qa.intent_cell_plan(cell_id=f"intent.aave_v3.arbitrum.{intent}.mainnet.eoa")

    assert plan["proof_recipe"]["contract_profile"] == "lending.v1"
    assert plan["proof_recipe"]["runner"] == "scripts/quant-test/run_mainnet_intent.py"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("chain", ["arbitrum", "base"])
@pytest.mark.parametrize("intent", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
def test_aave_mainnet_eoa_cells_are_routable_only_with_exact_recipes(modules, chain: str, intent: str) -> None:
    qa, _, _ = modules

    plan = qa.intent_cell_plan(cell_id=f"intent.aave_v3.{chain}.{intent}.mainnet.eoa")

    assert plan["proof_recipe"]["contract_profile"] == "lending.v1"
    assert plan["proof_recipe"]["runner"] == "scripts/quant-test/run_mainnet_intent.py"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("chain", ["arbitrum", "base"])
def test_uniswap_v3_swap_mainnet_eoa_is_routable_only_with_exact_recipe(modules, chain: str) -> None:
    qa, _, _ = modules

    plan = qa.intent_cell_plan(cell_id=f"intent.uniswap_v3.{chain}.SWAP.mainnet.eoa")

    assert plan["proof_recipe"]["contract_profile"] == "swap.v1"
    assert plan["proof_recipe"]["runner"] == "scripts/quant-test/run_mainnet_intent.py"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("chain", ["arbitrum", "base"])
@pytest.mark.parametrize("intent", ["LP_OPEN", "LP_CLOSE"])
def test_uniswap_v3_lp_mainnet_eoa_is_routable_only_with_exact_lifecycle_recipe(
    modules, chain: str, intent: str
) -> None:
    qa, _, _ = modules

    plan = qa.intent_cell_plan(cell_id=f"intent.uniswap_v3.{chain}.{intent}.mainnet.eoa")

    assert plan["proof_recipe"]["contract_profile"] == "v3_lp.v1"
    assert plan["proof_recipe"]["runner"] == "scripts/quant-test/run_mainnet_intent.py"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("intent", ["PERP_OPEN", "PERP_CLOSE"])
def test_gmx_v2_mainnet_eoa_is_routable_only_through_keeper_settlement_certifier(
    modules, chain: str, intent: str
) -> None:
    qa, _, _ = modules

    plan = qa.intent_cell_plan(cell_id=f"intent.gmx_v2.{chain}.{intent}.mainnet.eoa")

    assert plan["proof_recipe"]["contract_profile"] == "async_perp.v1"
    assert plan["proof_recipe"]["runner"] == "scripts/quant-test/intent_mainnet.py"
    assert len(plan["proof_recipe"]["nodeids"]) == 1


def test_exact_cell_runner_executes_only_planned_node_then_seals(monkeypatch, modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    cell_id = "intent.aave_v3.arbitrum.SUPPLY.anvil.safe"
    nodeid = "tests/intents/arbitrum/test_aave_v3_lending.py::TestAaveV3SupplyIntent::test_supply_usdc_using_intent"
    plan = {
        "schema_version": 1,
        "cell_id": cell_id,
        "protocol": "aave_v3",
        "intent": "SUPPLY",
        "chain": "arbitrum",
        "network": "anvil",
        "exec_path": "safe",
        "proof_recipe": {"nodeids": [nodeid]},
    }
    monkeypatch.setattr(qa, "intent_cell_plan", lambda **_kwargs: plan)
    monkeypatch.setattr(qa, "fork_upstream_is_public_rpc", lambda _chain: False)

    class History:
        @staticmethod
        def provenance_from_worktree(_root):
            return TEST_SDK

    monkeypatch.setattr(qa, "_load_history_module", lambda: History)
    launched: dict = {}

    class Process:
        def __init__(self, command, **kwargs):
            launched.update(command=command, kwargs=kwargs)
            junit = Path(next(arg.split("=", 1)[1] for arg in command if arg.startswith("--junitxml=")))
            evidence = Path(next(arg.split("=", 1)[1] for arg in command if arg.startswith("--intent-evidence-dir=")))
            junit.write_text('<testsuite tests="1"/>')
            evidence.mkdir()
            (evidence / "evidence-manifest.json").write_text('{"schema_version": 1, "nodes": []}')
            self.stdout = io.StringIO("one exact node\n")

        @staticmethod
        def wait():
            return 0

    monkeypatch.setattr(qa.subprocess, "Popen", Process)
    sealed = tmp_path / "sealed"
    seal_call: dict = {}

    def fake_seal(**kwargs):
        seal_call.update(kwargs)
        return sealed

    monkeypatch.setattr(qa, "seal_intent_junit", fake_seal)

    result = qa.run_intent_cell(
        cell_id=cell_id,
        store=tmp_path / "store",
        catalog_path=tmp_path / "catalog.yaml",
        run_id="exact-run",
    )

    assert result == sealed
    assert launched["command"][3:4] == [nodeid]
    assert not any(arg == "tests/intents/arbitrum" for arg in launched["command"])
    assert launched["kwargs"]["env"]["ANVIL_FORK_CACHE_PATH"] == "/tmp/anvil-cache/arbitrum"
    # The seal lane must run the proof nodes STRICT: without this env the known-red
    # excuses (VIB-6212/VIB-6810) xfail and the board would stop saying red.
    assert launched["kwargs"]["env"]["ALMANAK_QA_STRICT_PROOFS"] == "1"
    assert seal_call["network"] == "anvil"
    assert seal_call["exec_path"] == "safe"
    assert seal_call["sdk_provenance"] == TEST_SDK


def test_exact_cell_runner_rejects_public_rpc_before_creating_attempt(monkeypatch, modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    cell_id = "intent.aave_v3.arbitrum.SUPPLY.anvil.safe"
    monkeypatch.setattr(
        qa,
        "intent_cell_plan",
        lambda **_kwargs: {
            "cell_id": cell_id,
            "chain": "arbitrum",
            "proof_recipe": {"nodeids": ["tests/intents/example.py::test_supply"]},
        },
    )
    monkeypatch.setattr(qa, "fork_archive_required_chains", lambda: frozenset({"arbitrum"}))
    monkeypatch.setattr(qa, "fork_upstream_is_public_rpc", lambda _chain: True)
    store = tmp_path / "store"

    with pytest.raises(RuntimeError, match="archive-capable fork upstream"):
        qa.run_intent_cell(
            cell_id=cell_id,
            store=store,
            catalog_path=tmp_path / "catalog.yaml",
        )

    assert not store.exists()


def test_real_intent_catalog_does_not_cross_product_mixed_file_exec_paths(modules) -> None:
    qa, _, _ = modules
    inventory = qa.build_intent_catalog()
    cells = {(cell["protocol"], cell["intent"], cell["chain"]): cell for cell in inventory["cells"]}
    # The mixed Fluid file has Safe LP_CLOSE coverage and a separate EOA
    # LP_OPEN class. File-level cross-product attribution used to claim both.
    assert not cells[("fluid_dex_lp", "LP_CLOSE", "arbitrum")]["test_paths"]["eoa"]
    assert cells[("fluid_dex_lp", "LP_OPEN", "arbitrum")]["test_paths"]["eoa"]

    # V4 collect-fees lifecycle nodes deliberately mark their EOA setup
    # intents too; VIB-4303 union semantics therefore retain these sources.
    explicit_eoa_setup_claims = {
        ("uniswap_v4", "SWAP", "avalanche"),
        ("uniswap_v4", "SWAP", "bsc"),
        ("uniswap_v4", "SWAP", "polygon"),
        ("uniswap_v4", "LP_OPEN", "polygon"),
    }
    assert all(cells[triple]["test_paths"]["eoa"] for triple in explicit_eoa_setup_claims)

    # Exact-runtime golden scenarios deliberately expose the same assertions
    # as separately collectible Safe and EOA nodes. Neither axis may be
    # inferred from the other.
    for triple in {
        ("uniswap_v3", "SWAP", "arbitrum"),
        ("aave_v3", "SUPPLY", "arbitrum"),
    }:
        assert cells[triple]["test_paths"]["safe"]
        assert cells[triple]["test_paths"]["eoa"]


def test_intent_catalog_gate_isolated_from_loaded_connector_modules(modules) -> None:
    qa, _, _ = modules
    import almanak.connectors.aerodrome.connector  # noqa: F401, PLC0415

    module_name = "almanak.connectors.aerodrome.connector"
    loaded_module = sys.modules[module_name]

    qa.build_intent_catalog()

    assert sys.modules[module_name] is loaded_module


def test_legacy_intent_seal_never_sets_official_freshness_or_decode(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    qa, _, _ = modules
    inventory = qa.build_intent_catalog()
    cell = next(cell for cell in inventory["cells"] if cell["chain"] == "arbitrum" and cell["test_paths"]["safe"])
    test_file = cell["test_paths"]["safe"][0]
    classname = test_file.removesuffix(".py").replace("/", ".") + ".TestIntent"
    junit = tmp_path / "legacy.xml"
    junit.write_text(
        f'<testsuite tests="1"><testcase classname="{classname}" name="test_roundtrip" time="0.5" /></testsuite>'
    )
    store = tmp_path / "legacy-store"

    target = qa.seal_intent_junit(
        junit=junit,
        store=store,
        catalog_path=catalog_path,
        chain="arbitrum",
        network="anvil",
        exec_path="safe",
        run_id="legacy-run",
        sdk_provenance=TEST_SDK,
    )

    assert json.loads((store / "index" / "intent_latest.json").read_text()) == {}
    summary = json.loads((target / "summary.json").read_text())
    assert summary["attribution_mode"] == "legacy-file-inferred"
    assert all(cell["evidence_status"] == "LEGACY_UNVERIFIED" for cell in summary["cells"])


def test_data_seal_keeps_contract_and_provider_health_independent(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    bundle = tmp_path / "data-output"
    bundle.mkdir()
    checks = {check: {"ok": True} for check in qa._data_contract_checks()}
    price_tokens = qa._literal_strings_assigned(qa.DATA_CONTRACT_TEST, "PRICE_TOKENS")
    stablecoins = set(qa._literal_strings_assigned(qa.DATA_CONTRACT_TEST, "STABLECOIN_TOKENS"))
    prices = {token: "1" if token in stablecoins else "3000" for token in price_tokens}
    price_sources = {
        token: {
            "source": "coingecko" if token == "WBTC" else "binance",
            "confidence": 0.9 if token == "WBTC" else 1.0,
            "stale": False,
            "observed_at": "2026-08-04T12:00:00+00:00",
            "pair": f"{token.upper()}/USD",
        }
        for token in price_tokens
    }
    price_sources["WBTC"]["source_details"] = {"sources_failed": {"onchain": "empty eth_call result"}}
    summary = {
        "status": "pass",
        "timestamp_utc": "2026-08-04T12:00:00+00:00",
        "completed_at_utc": "2026-08-04T12:00:00+00:00",
        "chain": "arbitrum",
        "checks": checks,
        "errors": [],
        "timings_ms": dict.fromkeys(checks, 1.5),
        "metrics": {
            "prices": prices,
            "providers": {
                "price_sources": price_sources,
                "ohlcv_source": {
                    "source": "binance",
                    "confidence": 1.0,
                    "stale": False,
                    "observed_at": "2026-08-04T12:00:00+00:00",
                    "pair": "WETH/USD",
                    "timeframe": "4h",
                    "candle_count": 120,
                    "first_candle_at": "2026-07-15T12:00:00+00:00",
                    "last_candle_at": "2026-08-04T08:00:00+00:00",
                    "timestamps_strictly_increasing": True,
                },
            },
        },
    }
    (bundle / "summary.json").write_text(json.dumps(summary))
    (bundle / "chart_prices_with_sma.png").write_bytes(b"png")
    log = tmp_path / "market.log"
    log.write_text("provider contract log")

    target = qa.seal_data_bundle(
        bundle=bundle,
        store=tmp_path / "store",
        catalog_path=catalog_path,
        log=log,
        sdk_provenance=TEST_SDK,
        run_id="data-run-001",
        now=datetime(2026, 8, 4, 12, 0, tzinfo=UTC),
    )

    latest = json.loads((tmp_path / "store/index/data_latest.json").read_text())
    assert len(latest) == 8
    assert latest["data.market_snapshot.price_sanity.arbitrum"]["status"] == "PASS"
    provider = latest["data.market_snapshot.provider_attribution.arbitrum"]
    assert provider["status"] == "DEGRADED"
    assert provider["contract_status"] == "UNMEASURED"
    assert provider["last_pass_at"] is None
    assert "WBTC: onchain source failed" in provider["degradations"][0]
    assert (target / "report.html").is_file()
    report = (target / "report.html").read_text()
    assert "Full Data Test Report" in report
    assert f"Observed prices and selected providers · {len(price_tokens)}" in report
    assert "WBTC: onchain source failed" in report
    assert "coingecko" in report
    assert 'src="chart_prices_with_sma.png"' in report
    assert 'loading="lazy"' not in report
    assert 'href="market_data_api_contract.log"' in report
    assert 'href="manifest.json"' in report
    assert "1.5 ms" in report
    assert (target / "market_data_api_contract.log").is_file()
    manifest = json.loads((target / "manifest.json").read_text())
    assert {artifact["kind"] for artifact in manifest["artifacts"]} >= {"json", "png", "log", "report.html"}
    assert all("sha256" in artifact for artifact in manifest["artifacts"])
    page = (tmp_path / "store/lab/data.html").read_text()
    assert "data-run-001" in page
    assert "chart_prices_with_sma.png" in page


def test_data_seal_rejects_missing_expected_check_despite_declared_pass(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    """A truncated producer summary cannot certify the checks it omitted."""
    qa, _, _ = modules
    bundle = tmp_path / "truncated-data-output"
    bundle.mkdir()
    checks = {check: {"ok": True} for check in qa._data_contract_checks()}
    omitted = next(iter(checks))
    checks.pop(omitted)
    (bundle / "summary.json").write_text(json.dumps({"status": "pass", "chain": "arbitrum", "checks": checks}))

    with pytest.raises(ValueError, match="missing executable catalog checks"):
        qa.seal_data_bundle(
            bundle=bundle,
            store=tmp_path / "store",
            catalog_path=catalog_path,
            sdk_provenance=TEST_SDK,
        )


def test_data_seal_ignores_producer_check_boole_without_measurements(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    """Producer ok booleans cannot create either PASS or FAIL without observations."""
    qa, _, _ = modules
    bundle = tmp_path / "false-green-data-output"
    bundle.mkdir()
    checks = {check: {"ok": True} for check in qa._data_contract_checks()}
    checks[next(iter(checks))]["ok"] = False
    (bundle / "summary.json").write_text(json.dumps({"status": "pass", "chain": "arbitrum", "checks": checks}))

    qa.seal_data_bundle(
        bundle=bundle,
        store=tmp_path / "store",
        catalog_path=catalog_path,
        sdk_provenance=TEST_SDK,
    )
    latest = json.loads((tmp_path / "store/index/data_latest.json").read_text())
    assert {row["status"] for row in latest.values()} == {"UNMEASURED"}


def test_data_authoritative_feed_mismatch_is_an_explicit_failure(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    identity = {
        "provider": "chainlink",
        "chain": "ethereum",
        "address": "0x" + "12" * 20,
        "pair": "RETH/ETH",
        "decimals": 18,
        "kind": "eth",
    }
    declared = {**identity, "pair": "RETH/USD", "decimals": 8, "kind": "usd"}
    (tmp_path / "resource-identities.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "declared": declared,
                        "authority": {
                            "identity": identity,
                            "authority_uri": "https://data.chain.link/ethereum/mainnet/crypto-eth/reth-eth",
                        },
                    }
                ]
            }
        )
    )

    status, reasons, count = qa._data_resource_identity_status(tmp_path, chain="ethereum")

    assert status == "FAIL"
    assert count == 1
    assert reasons == ["feed_decimals_mismatch", "feed_kind_mismatch", "feed_pair_mismatch"]


def test_data_identity_seal_indexes_only_replayed_exact_resource(
    modules, catalog_path: Path, tmp_path: Path, monkeypatch
) -> None:
    qa, _, _ = modules
    captured_at = datetime(2026, 8, 15, 12, tzinfo=UTC)
    bundle = tmp_path / "identity-bundle"
    requirement = _identity_bundle(bundle, captured_at)
    store = tmp_path / "store"
    monkeypatch.setattr(qa, "render_lab", lambda **_kwargs: store / "lab" / "data.html")

    target = qa.seal_data_identity_bundle(
        bundle=bundle,
        store=store,
        catalog_path=catalog_path,
        sdk_provenance=TEST_SDK,
        now=captured_at,
    )

    latest = json.loads((store / "index" / "data_identity_latest.json").read_text())
    row = latest[requirement.requirement_id]
    assert row["status"] == "PASS"
    assert row["captured_at"] == "2026-08-15T12:00:00Z"
    assert row["expected"] == requirement.to_canonical_dict()
    assert row["observed"]["provenance"]["artifact_sha256"]
    assert (target / "raw" / f"{row['observed']['provenance']['artifact_sha256']}.json").is_file()


def test_failed_run_does_not_satisfy_recency(modules, tmp_path: Path, monkeypatch) -> None:
    _, planner, _ = modules
    store = tmp_path / "store"
    (store / "index").mkdir(parents=True)
    cell_id = "lp.uniswap_v3.arbitrum.simple.mainnet.eoa"
    (store / "index" / "cell_latest.json").write_text(
        json.dumps(
            {
                cell_id: {
                    "sealed_at": "2026-08-03T00:00:00Z",
                    "verdicts": {"strategy": "FAIL"},
                }
            }
        )
    )
    (store / "index" / "runs.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "cell_id": cell_id,
                        "sealed_at": "2026-08-01T00:00:00Z",
                        "verdicts": {"strategy": "PASS"},
                    }
                ),
                json.dumps(
                    {
                        "cell_id": cell_id,
                        "sealed_at": "2026-08-03T00:00:00Z",
                        "verdicts": {"strategy": "FAIL"},
                    }
                ),
            ]
        )
        + "\n"
    )
    monkeypatch.setattr(planner, "_utc_now", lambda: datetime(2026, 8, 3, tzinfo=UTC))

    recency = planner._load_recency(store)

    assert recency[cell_id] == pytest.approx(2.0)


def test_only_failed_history_is_never_green(modules, tmp_path: Path, monkeypatch) -> None:
    _, planner, _ = modules
    store = tmp_path / "store"
    (store / "index").mkdir(parents=True)
    cell_id = "lp.uniswap_v3.arbitrum.simple.mainnet.safe"
    failed = {
        "cell_id": cell_id,
        "sealed_at": "2026-08-03T00:00:00Z",
        "verdicts": {"strategy": "FAIL"},
    }
    (store / "index" / "cell_latest.json").write_text(json.dumps({cell_id: failed}))
    (store / "index" / "runs.jsonl").write_text(json.dumps(failed) + "\n")
    monkeypatch.setattr(planner, "_utc_now", lambda: datetime(2026, 8, 3, tzinfo=UTC))

    assert cell_id not in planner._load_recency(store)


def test_strategy_caveat_and_product_freshness_require_exact_passes(modules) -> None:
    qa, _, _ = modules
    claims = {
        "strategy": {"status": "PASS", "observer": "quant-sealer"},
        "books": {"status": "PASS", "observer": "quant-books-sealer"},
        "dashboard": {"status": "PASS", "observer": "quant-dashboard-sealer"},
    }

    assert not qa.strategy_is_green({"strategy": "PASS (CAVEAT)"})
    assert not qa.product_is_green({"strategy": "PASS (CAVEAT)", "books": "PASS", "dashboard": "PASS"})
    assert not qa.product_is_green({"strategy": "PASS", "books": "FAIL", "dashboard": "PASS"})
    assert not qa.product_is_green({"strategy": "PASS", "books": "PASS", "dashboard": "PASS"})
    assert qa.product_is_green({"strategy": "PASS", "books": "PASS", "dashboard": "PASS"}, claims)
    claims["dashboard"]["observer"] = "dashboard-verifier"
    assert not qa.product_is_green({"strategy": "PASS", "books": "PASS", "dashboard": "PASS"}, claims)


def test_books_index_preserves_last_pass_without_cross_painting_dedicated(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    quant_cell_id = "lp.uniswap_v3.arbitrum.simple.mainnet.eoa"
    books_cell_id = "books.uniswap_v3.lp_simple.arbitrum.mainnet.eoa"
    old_pass = {
        "cell_id": quant_cell_id,
        "sealed_at": "2026-08-01T00:00:00Z",
        "verdicts": {"books": "PASS"},
    }
    latest_fail = {
        "cell_id": quant_cell_id,
        "run_id": "latest-fail",
        "sealed_at": "2026-08-04T00:00:00Z",
        "store_path": "quant/2026/08/04/latest-fail",
        "verdicts": {"books": "FAIL"},
        "artifacts": [],
    }
    (store / "index" / "runs.jsonl").write_text(json.dumps(old_pass) + "\n" + json.dumps(latest_fail) + "\n")
    (store / "index" / "cell_latest.json").write_text(json.dumps({quant_cell_id: latest_fail}))
    catalog = qa._load_catalog(store / "catalog" / "cells.yaml")

    latest = qa.rebuild_books_index(store, catalog)

    assert latest[books_cell_id]["status"] == "FAIL"
    assert latest[books_cell_id]["last_pass_at"] == "2026-08-01T00:00:00Z"

    dedicated = {"source": "accounting", "status": "PASS", "run_id": "accountant-001"}
    (store / "index" / qa.ACCOUNTING_DEDICATED_INDEX).write_text(json.dumps({books_cell_id: dedicated}))
    assert qa.rebuild_books_index(store, catalog)[books_cell_id]["source"] == "quant-test"
    assert json.loads((store / "index" / qa.ACCOUNTING_DEDICATED_INDEX).read_text())[books_cell_id] == dedicated


def test_accounting_seal_is_immutable_fail_closed_and_repaints_lab(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    bundle = tmp_path / "accounting-row"
    strategy = bundle / "strategy"
    strategy.mkdir(parents=True)
    (bundle / "status.json").write_text(
        json.dumps(
            {
                "row_id": "lp-uniswap_v3-arbitrum",
                "chain": "arbitrum",
                "status": "PASS",
            }
        )
    )
    accountant = {
        "scores": {"passed": 20, "failed": 1, "xfailed": 1, "total": 22},
        "cell_details": [
            {"id": "G1", "description": "Money trail", "status": "PASS", "diagnostic": "4 rows"},
            {
                "id": "G6",
                "description": "Reconciliation",
                "status": "FAIL",
                "diagnostic": "gap=$0.42 > epsilon=$0.10",
            },
        ],
        "g6_decomposition": {
            "wallet_pnl_usd": "-1.00",
            "component_pnl_usd": "-1.42",
            "gap_usd": "0.42",
            "epsilon_threshold_usd": "0.10",
        },
    }
    (bundle / "accountant.json").write_text(json.dumps(accountant))
    _write_dedicated_evidence(bundle)
    (bundle / "strat.log").write_text("strategy evidence\n")
    (bundle / "teardown.log").write_text("completed\n")
    db = strategy / "almanak_state.db"
    connection = sqlite3.connect(db)
    connection.executescript(
        """
        CREATE TABLE transaction_ledger (
          id TEXT, timestamp TEXT, cycle_id TEXT, intent_type TEXT, token_in TEXT,
          amount_in TEXT, token_out TEXT, amount_out TEXT, effective_price TEXT,
          slippage_bps TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
              chain TEXT, protocol TEXT, success INTEGER, error TEXT, deployment_id TEXT
        );
        CREATE TABLE accounting_events (
          id TEXT, timestamp TEXT, cycle_id TEXT, event_type TEXT, position_key TEXT,
              tx_hash TEXT, confidence TEXT, payload_json TEXT, schema_version INTEGER,
              deployment_id TEXT
        );
        CREATE TABLE position_events (
          id TEXT, timestamp TEXT, cycle_id TEXT, position_id TEXT, position_type TEXT,
          event_type TEXT, protocol TEXT, chain TEXT, token0 TEXT, token1 TEXT,
          amount0 TEXT, amount1 TEXT, value_usd TEXT, tick_lower INTEGER,
          tick_upper INTEGER, liquidity TEXT, in_range INTEGER, fees_token0 TEXT,
          fees_token1 TEXT, gas_usd TEXT, tx_hash TEXT, attribution_json TEXT
        );
        CREATE TABLE portfolio_snapshots (
          id INTEGER, timestamp TEXT, cycle_id TEXT, iteration_number INTEGER,
          total_value_usd TEXT, available_cash_usd TEXT, value_confidence TEXT,
              positions_json TEXT, wallet_balances_json TEXT, deployment_id TEXT
            );
            CREATE TABLE portfolio_metrics (id INTEGER, deployment_id TEXT);
        INSERT INTO transaction_ledger VALUES (
          'l1','2026-08-04T00:00:00Z','cycle-1','LP_OPEN','WETH','1','USDC','2',
              '2','0','100','0.1','0xabc','arbitrum','uniswap_v3',1,'','deployment:test'
        );
        INSERT INTO accounting_events VALUES (
          'a1','2026-08-04T00:00:00Z','cycle-1','LP_OPEN','lp:key','0xabc','HIGH',
              '{"cost_basis_usd":"2","fees_total_usd":"0"}',1,'deployment:test'
        );
        INSERT INTO position_events VALUES (
          'p1','2026-08-04T00:00:00Z','cycle-1','42','LP','OPEN','uniswap_v3',
          'arbitrum','WETH','USDC','1','2','2','-1','1','100',1,'0','0','0.1',
          '0xabc','{}'
        );
        INSERT INTO portfolio_snapshots VALUES (
          1,'2026-08-04T00:00:00Z','cycle-1',1,'2','98','HIGH',
              '{"schema_version":1,"positions":[{"position_type":"LP"}],"metadata":{},"reconciliation":{}}','[]',
              'deployment:test'
            );
            INSERT INTO portfolio_metrics VALUES (1, 'deployment:test');
        """
    )
    connection.commit()
    connection.close()
    store = tmp_path / "store"
    now = datetime(2026, 8, 4, 12, 30, tzinfo=UTC)

    target = qa.seal_accounting_bundle(
        bundle=bundle,
        store=store,
        catalog_path=catalog_path,
        books_id="books.uniswap_v3.lp_simple.arbitrum",
        network="anvil",
        exec_path="eoa",
        sdk_provenance=TEST_SDK,
        now=now,
    )

    assert target.is_dir()
    books_history = (store / "index" / "accounting_dedicated_runs.jsonl").read_text().splitlines()
    assert [json.loads(row)["run_id"] for row in books_history] == [target.name]
    assert (target / "almanak_state.db").is_file()
    assert (target / "positions.json").is_file()
    assert (target / "costs.json").is_file()
    assert (target / "fees.json").is_file()
    assert (target / "pnl.json").is_file()
    fees = json.loads((target / "fees.json").read_text())
    assert fees["gas_by_lane"]["iteration"] == {
        "measured_usd": "0.1",
        "measured_rows": 1,
        "null_rows": 0,
        "complete": True,
    }
    assert fees["gas_by_lane"]["unknown"]["measured_usd"] == "0"
    assert json.loads((target / "pnl.json").read_text())["evidence_status"] == "MEASURED"
    report = (target / "report.html").read_text()
    assert "Sealed dedicated Accounting proof" in report
    assert "dedicated stage contract" in report
    assert "Money trail" in report
    assert "Positions and lifecycle" in report
    assert "PnL reconciliation" in report
    assert "gap=$0.42" in report
    # The snapshots table must carry the fixture's position count. The original
    # guard was the bare cell position_count() rendered (`>1</td>`); the report
    # formatting commits replaced that with _positions_cell(), which shows the
    # same count plus its type breakdown — assert the same fact in that markup.
    assert "<td>1<small>LP" in report
    cell_id = "books.uniswap_v3.lp_simple.arbitrum.anvil.eoa"
    latest = json.loads((store / "index" / qa.ACCOUNTING_DEDICATED_INDEX).read_text())[cell_id]
    assert latest["source"] == "accounting"
    assert latest["status"] == "FAIL"
    assert latest["matrix_gate_status"] == "PASS"
    assert latest["last_pass_at"] is None
    assert latest["report_path"].endswith("/report.html")
    lab = (store / "lab" / "accounting.html").read_text()
    assert "Accounting Test report" in lab
    assert "Latest dedicated proofs" in lab
    assert "matrix row" in lab
    with pytest.raises(FileExistsError, match="Immutable Accounting dedicated run"):
        qa.seal_accounting_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            books_id="books.uniswap_v3.lp_simple.arbitrum",
            network="anvil",
            exec_path="eoa",
            sdk_provenance=TEST_SDK,
            now=now,
        )


def test_plan_markdown_requires_quant_test_only(modules) -> None:
    _, planner, _ = modules
    plan = {
        "lane": "daily",
        "network": "mainnet",
        "created_at": "2026-08-03T00:00:00Z",
        "slots_requested": 1,
        "store": "/tmp/store",
        "radius_protocols": [],
        "policy": {"exclude_chains": ["ethereum"], "allow_ethereum": False, "allow_costly": False},
        "slots": [
            {
                "cell_id": "lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
                "strategy_path": "strategies/accounting/lp",
                "network": "mainnet",
                "exec_path": "eoa",
                "chain": "arbitrum",
                "lifecycle": "simple",
                "selection_bucket": "recency",
                "selection_reason": "oldest",
                "intent_surface": "LP_OPEN -> LP_CLOSE",
                "cost_class": "standard",
            }
        ],
    }

    markdown = planner._render_md(plan, hold="2h")

    assert "/quant-test" in markdown
    assert "Or manual" not in markdown
    assert "continuous `uv run almanak" not in markdown
    assert "standalone strategy run is not Coverage evidence" in markdown
    assert "exec_path=eoa" in markdown


def test_picker_never_schedules_same_strategy_folder_twice(modules) -> None:
    _, planner, _ = modules
    pool = [
        {
            "id": "lp.uniswap_v3.arbitrum.simple",
            "cell_id": f"lp.uniswap_v3.arbitrum.simple.mainnet.{exec_path}",
            "strategy_path": "strategies/accounting/lp",
            "protocol": "uniswap_v3",
        }
        for exec_path in ("eoa", "safe")
    ]

    selected = planner._pick(
        pool,
        2,
        recency={cell["cell_id"]: 1e9 for cell in pool},
        radius_protocols=set(),
        pins=[],
        rng=planner.random.Random(7),
        radius_slots=0,
        recency_slots=2,
    )

    assert len(selected) == 1


def test_catalog_only_capability_is_visible_but_not_plannable(modules) -> None:
    _, planner, _ = modules
    catalog_only = {
        "id": "swap.across.arbitrum.simple",
        "cell_id": "swap.across.arbitrum.simple.mainnet.eoa",
        "primitive": "swap",
        "protocol": "across",
        "chain": "arbitrum",
        "lifecycle": "simple",
        "strategy_path": None,
        "testability": "catalog_only",
    }

    assert (
        planner._filter_pool(
            [catalog_only],
            exclude_chains=set(),
            include_chains=None,
            allow_costly=False,
            allow_ethereum=False,
            only_lifecycle=None,
            only_primitive=None,
        )
        == []
    )


def test_catalog_sync_builds_full_support_universe_and_preserves_executable_overlay(modules) -> None:
    _, _, sync_catalog = modules
    current = {
        "defaults": {"networks": ["anvil", "mainnet"], "exec_paths": ["eoa", "safe"]},
        "cells": [
            {
                "id": "swap.uniswap_v3.arbitrum.simple",
                "primitive": "swap",
                "protocol": "uniswap_v3",
                "chain": "arbitrum",
                "lifecycle": "simple",
                "strategy_path": "strategies/incubating/buy_the_dip",
                "protocol_logo": "protocols/uniswap-v3.png",
                "chain_logo": "chains/arbitrum.png",
            }
        ],
    }
    matrix = {
        "chains": ["arbitrum", "base"],
        "protocols": [
            {
                "name": "uniswap_v3",
                "category": "swap",
                "chains": ["arbitrum", "base"],
                "chainsByIntent": {"SWAP": ["arbitrum", "base"]},
            }
        ],
    }

    catalog = sync_catalog.build_catalog(current, matrix)

    assert [row["id"] for row in catalog["cells"]] == [
        "swap.uniswap_v3.arbitrum.simple",
        "swap.uniswap_v3.base.simple",
    ]
    assert catalog["cells"][0]["testability"] == "executable"
    assert catalog["cells"][0]["protocol_logo"] == "protocols/uniswap-v3.webp"
    assert catalog["cells"][0]["chain_logo"] == "chains/arbitrum.webp"
    assert catalog["cells"][1]["testability"] == "catalog_only"
    assert catalog["chains"] == ["arbitrum", "base"]


def _write_minimal_quant_bundle(qa, bundle: Path) -> Path:
    """Build the canonical admitted Quant bundle shared by the seal and migration tests."""
    (bundle / "dashboard").mkdir(parents=True)
    (bundle / "report.html").write_text("<h1>sealed report</h1>")
    (bundle / "run.log").write_text("runner evidence\n")
    # Was 15 bytes of the literal text "sqlite-evidence".  VIB-6712 confirms the
    # lifecycle against this file, so the fixture has to be a real database.
    _write_state_database(bundle, ["0x" + "1" * 64]).rename(bundle / "almanak_state.db")
    (bundle / "dashboard" / "h01-full.png").write_bytes(b"fake-png-evidence")
    (bundle / "finding.json").write_text(
        json.dumps(
            {
                "verdicts": {
                    "strategy": "PASS",
                    "books": "FAIL",
                    "dashboard": "PASS",
                    "harness": "PASS",
                },
                "headline": "Accounting NAV disagrees with wallet cash",
                "axes": {"nav": "books", "ux": "dashboard"},
                "outcomes": {"nav": "zero NAV with positive cash", "ux": "chart rendered"},
                "tickets": [
                    {"id": "QA-9001", "status": "filed", "covers": "dashboard value divergence"},
                    {"id": "QA-9001", "status": "filed", "covers": "missing chart evidence"},
                ],
            }
        )
    )
    (bundle / "git.json").write_text(
        json.dumps(
            {
                "commit": TEST_COMMIT,
                "branch": "feat/test",
                "dirty": False,
                "sdk_version": "0.0-test",
            }
        )
    )
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "claim_scope": {
            "required": ["strategy"],
            "not_applicable": ["books", "dashboard", "harness"],
        },
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategies/example/strategy.py"],
            "transition_sequence": ["idle -> open"],
        },
        "requirements": [{"id": "open", "phase": "runtime", "intent_type": "LP_OPEN", "min_executed": 1}],
        "teardown": {"required": False},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    (bundle / "lifecycle-coverage.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract_sha256": digest,
                "observations": [
                    {
                        "requirement_id": "open",
                        "phase": "runtime",
                        "intent_type": "LP_OPEN",
                        "executed": 1,
                        "transaction_ids": ["0x" + "1" * 64],
                    }
                ],
                "teardown": {"coverage": "not_requested"},
            }
        )
    )
    _write_receipt_reconciliation(bundle, ["0x" + "1" * 64])
    (bundle / "audit.md").write_text(
        f"LIFECYCLE_COVERAGE_CONFIRMED: yes\nLIFECYCLE_CONTRACT_SHA256: {digest}\nRECEIPT_INTEGRITY_CONFIRMED: yes\n"
    )
    _write_quant_audit_decision(qa, bundle)
    return bundle


def test_seal_links_every_file_and_repaints_lab(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    bundle = _write_minimal_quant_bundle(qa, tmp_path / "bundle")
    digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    cell_id = "lp.uniswap_v3.arbitrum.simple.mainnet.eoa"
    sealed_at = datetime(2026, 8, 3, 12, 30, tzinfo=UTC)

    target = qa.seal_bundle(
        bundle=bundle,
        store=store,
        catalog_path=catalog_path,
        cell_id=cell_id,
        network="mainnet",
        exec_path="eoa",
        lane="daily",
        run_id="run-001",
        selection={"bucket": "recency", "reason": "oldest"},
        now=sealed_at,
    )

    assert target == store / "quant/2026/08/03/run-001"
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["cell_id"] == cell_id
    assert manifest["exec_path"] == "eoa"
    assert {row["relpath"] for row in manifest["artifacts"]} == {
        "quant/2026/08/03/run-001/dashboard/h01-full.png",
        "quant/2026/08/03/run-001/almanak_state.db",
        "quant/2026/08/03/run-001/finding.json",
        "quant/2026/08/03/run-001/git.json",
        "quant/2026/08/03/run-001/report.html",
        "quant/2026/08/03/run-001/run.log",
        "quant/2026/08/03/run-001/lifecycle-contract.json",
        "quant/2026/08/03/run-001/lifecycle-coverage.json",
        "quant/2026/08/03/run-001/receipt-reconciliation.json",
        "quant/2026/08/03/run-001/audit.md",
        "quant/2026/08/03/run-001/audit-decision.json",
    }
    assert all(len(row["sha256"]) == 64 for row in manifest["artifacts"])

    latest = json.loads((store / "index" / "cell_latest.json").read_text())[cell_id]
    assert latest["last_green_at"] == "2026-08-03T12:30:00Z"
    assert latest["last_product_green_at"] is None
    assert latest["report_path"].endswith("/report.html")
    assert {row["kind"] for row in latest["artifacts"]} >= {"manifest", "report.html", "png", "log"}
    page = (store / "lab" / "quant.html").read_text()
    assert "run-001" in page
    assert "h01-full.png" in page
    books_id = "books.uniswap_v3.lp_simple.arbitrum.mainnet.eoa"
    books = json.loads((store / "index" / qa.ACCOUNTING_CORROBORATION_INDEX).read_text())
    assert books_id not in books
    accounting_page = (store / "lab" / "accounting.html").read_text()
    assert "run-001" not in accounting_page
    tickets = json.loads((store / "index" / qa.TICKET_INDEX_NAME).read_text())["tickets"]
    assert list(tickets) == ["QA-9001"]
    assert tickets["QA-9001"]["reports"][0]["covers"] == [
        "dashboard value divergence",
        "missing chart evidence",
    ]
    tx_id = "0x" + "1" * 64
    assert manifest["lifecycle_evidence"] == {
        "contract_sha256": digest,
        "receipt_integrity": {
            "async_order_ids": [],
            "submitted_transaction_count": 1,
            "status": "PASS",
        },
        "requirements_proved": 1,
        "lifecycle_transaction_ids": [tx_id],
        # VIB-6712: the Strategy PASS now records which run-produced database
        # confirmed each declared lifecycle transaction.
        "state_database": {
            "path": "almanak_state.db",
            "sha256": hashlib.sha256((bundle / "almanak_state.db").read_bytes()).hexdigest(),
            "confirmed_transactions": 1,
            "deployment_id": "deployment:test",
        },
    }

    with pytest.raises(FileExistsError):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            cell_id=cell_id,
            network="mainnet",
            exec_path="eoa",
            lane="daily",
            run_id="run-001",
            now=sealed_at,
        )


def test_seal_rejects_secret_filenames(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / ".env.mainnet").write_text("SECRET=do-not-copy\n")

    with pytest.raises(ValueError, match="secret-bearing"):
        qa.seal_bundle(
            bundle=bundle,
            store=tmp_path / "store",
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="daily",
        )


def test_seal_explicit_strategy_pass_cannot_bypass_lifecycle_gate(modules, catalog_path: Path, tmp_path: Path) -> None:
    """A CLI verdict override is not an alternate path around evidence validation."""
    qa, _, _ = modules
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "finding.json").write_text(json.dumps({"verdicts": {"strategy": "FAIL"}}))
    (bundle / "git.json").write_text(json.dumps(TEST_SDK))

    with pytest.raises(ValueError, match="requires lifecycle-contract.json"):
        qa.seal_bundle(
            bundle=bundle,
            store=tmp_path / "store",
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="daily",
            verdicts={"strategy": "PASS"},
        )


@pytest.mark.parametrize("producer_strategy", ["FAIL", "PASS (CAVEAT)"])
def test_incomplete_attempt_never_changes_official_state(
    modules,
    catalog_path: Path,
    tmp_path: Path,
    producer_strategy: str,
) -> None:
    """The Aug-17 incomplete bundle shapes remain attempts, never experiments."""
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    ledger = store / "index" / "experiment_runs.jsonl"
    latest = store / "index" / "cell_latest.json"
    ledger_before = ledger.read_bytes() if ledger.exists() else b""
    latest_before = latest.read_bytes()
    bundle = tmp_path / producer_strategy.replace(" ", "-")
    bundle.mkdir()
    (bundle / "finding.json").write_text(json.dumps({"verdicts": {"strategy": producer_strategy}}))
    (bundle / "git.json").write_text(json.dumps(TEST_SDK))
    (bundle / "report.html").write_text("incomplete attempt")

    with pytest.raises(ValueError, match="requires lifecycle-contract.json"):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id=f"incomplete-{producer_strategy.lower().replace(' ', '-')}",
        )

    assert (ledger.read_bytes() if ledger.exists() else b"") == ledger_before
    assert latest.read_bytes() == latest_before
    attempts = (store / "index" / "attempts.jsonl").read_text().splitlines()
    assert len(attempts) == 1
    assert json.loads(attempts[0])["status"] == "REJECTED"


def test_audit_overturn_cannot_enter_official_ledger(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    bundle = tmp_path / "overturned"
    bundle.mkdir()
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "claim_scope": {
            "required": ["strategy"],
            "not_applicable": ["books", "dashboard", "harness"],
        },
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategy.py"],
            "transition_sequence": ["idle -> target"],
        },
        "requirements": [{"id": "target", "phase": "runtime", "intent_type": "SWAP", "min_executed": 1}],
        "teardown": {"required": False},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    (bundle / "finding.json").write_text(json.dumps({"verdicts": {"strategy": "PASS"}}))
    (bundle / "git.json").write_text(json.dumps(TEST_SDK))
    _write_quant_audit_decision(qa, bundle)
    decision_path = bundle / "audit-decision.json"
    decision = json.loads(decision_path.read_text())
    decision.update(audit_verdict="AUDIT_OVERTURNED", seal_eligible=False)
    decision_path.write_text(json.dumps(decision))

    with pytest.raises(ValueError, match="did not declare.*seal-eligible"):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id="overturned-attempt",
        )

    assert json.loads((store / "index" / "cell_latest.json").read_text()) == {}
    assert not (store / "index" / "experiment_runs.jsonl").exists()


def test_evidence_complete_product_failure_can_become_official_red(modules, catalog_path: Path, tmp_path: Path) -> None:
    qa, _, _ = modules
    bundle = tmp_path / "measured-failure"
    bundle.mkdir()
    tx_id = "0x" + "fa" * 32
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "claim_scope": {
            "required": ["strategy"],
            "not_applicable": ["books", "dashboard", "harness"],
        },
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategy.py"],
            "transition_sequence": ["idle -> failed target"],
        },
        "requirements": [{"id": "target", "phase": "runtime", "intent_type": "LP_OPEN", "min_executed": 1}],
        "teardown": {"required": False},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    (bundle / "lifecycle-coverage.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract_sha256": digest,
                "observations": [
                    {
                        "requirement_id": "target",
                        "phase": "runtime",
                        "intent_type": "LP_OPEN",
                        "executed": 1,
                        "transaction_ids": [tx_id],
                    }
                ],
                "teardown": {"coverage": "not_requested"},
            }
        )
    )
    _write_receipt_reconciliation(bundle, [tx_id], statuses=[0])
    (bundle / "finding.json").write_text(json.dumps({"verdicts": {"strategy": "PASS"}, "failure": "revert"}))
    (bundle / "git.json").write_text(json.dumps(TEST_SDK))
    _write_quant_audit_decision(qa, bundle, status="FAIL")

    target = qa.seal_bundle(
        bundle=bundle,
        store=tmp_path / "store",
        catalog_path=catalog_path,
        cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
        network="mainnet",
        exec_path="eoa",
        lane="adhoc",
        run_id="official-product-fail",
    )

    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["producer_verdicts"]["strategy"] == "PASS"
    assert manifest["verdicts"]["strategy"] == "FAIL"
    latest = json.loads((tmp_path / "store/index/cell_latest.json").read_text())
    assert latest["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]["verdicts"]["strategy"] == "FAIL"


def test_crash_before_ledger_commit_quarantines_attempt_without_changing_official_state(
    modules, catalog_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qa, _, _ = modules
    history = qa._load_history_module()
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    bundle = tmp_path / "bundle-before-ledger"
    _write_minimal_official_quant_bundle(qa, bundle)
    latest_before = (store / "index/cell_latest.json").read_bytes()

    def crash_before_commit(**_kwargs):
        raise OSError("injected-before-ledger")

    monkeypatch.setattr(history, "append_experiment", crash_before_commit)
    with pytest.raises(OSError, match="injected-before-ledger"):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id="crash-before-ledger",
        )

    assert not (store / "index/experiment_runs.jsonl").exists()
    assert (store / "index/cell_latest.json").read_bytes() == latest_before
    assert (store / "attempts/crash-before-ledger").is_dir()
    assert not list((store / "index/seal-intents").glob("*.json"))


def test_crash_after_ledger_commit_is_recovered_by_projection_rebuild(
    modules, catalog_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    bundle = tmp_path / "bundle-after-ledger"
    _write_minimal_official_quant_bundle(qa, bundle)
    original_rebuild = qa.rebuild_quant_cell_index
    calls = 0

    def crash_first_projection(target_store: Path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected-after-ledger")
        return original_rebuild(target_store)

    monkeypatch.setattr(qa, "rebuild_quant_cell_index", crash_first_projection)
    with pytest.raises(OSError, match="injected-after-ledger"):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id="crash-after-ledger",
        )

    assert len((store / "index/experiment_runs.jsonl").read_text().splitlines()) == 1
    assert list((store / "index/seal-intents").glob("*.json"))
    qa.bootstrap_store(store, catalog_path)
    latest = json.loads((store / "index/cell_latest.json").read_text())
    assert latest["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]["run_id"] == "crash-after-ledger"
    assert not list((store / "index/seal-intents").glob("*.json"))


def test_product_is_green_liveness_the_predicate_can_still_return_true(modules) -> None:
    """VIB-6707: product_is_green is unreachable in practice; prove it still works.

    derive_observed_claim (scripts/qa/derived_claims.py:151) hard-codes the
    dashboard axis to UNMEASURED, so dashboard must be `not_applicable` on every
    sealable run and this predicate can never be True end to end at this HEAD.
    A predicate that can never be True can never fail, so it would rot silently
    between now and the day the Dashboard axis becomes derivable.  This test
    constructs the inputs directly rather than through a seal.
    """
    qa, _, _ = modules
    verdicts = {"strategy": "PASS", "books": "PASS", "dashboard": "PASS", "harness": "N/A"}
    claims = {
        "strategy": {"status": "PASS", "observer": "quant-sealer"},
        "books": {"status": "PASS", "observer": "quant-books-sealer"},
        "dashboard": {"status": "PASS", "observer": "quant-dashboard-sealer"},
    }

    assert qa.product_is_green(verdicts, claims) is True

    # Each authority is load-bearing: demoting any one axis, or letting any axis
    # be claimed by a non-mechanical observer, must take the predicate back down.
    for axis in ("strategy", "books", "dashboard"):
        assert qa.product_is_green({**verdicts, axis: "PASS (CAVEAT)"}, claims) is False
        assert qa.product_is_green(verdicts, {**claims, axis: {"status": "PASS", "observer": "producer"}}) is False
        assert (
            qa.product_is_green(verdicts, {**claims, axis: {"status": "N/A", "observer": "prelaunch-claim-scope"}})
            is False
        )
    assert qa.product_is_green(verdicts, {k: v for k, v in claims.items() if k != "dashboard"}) is False


def test_strategy_only_official_pass_is_labelled_and_never_sets_product_freshness(
    modules, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VIB-6707: a Strategy-only green must be visibly Strategy-only.

    A Strategy-only claim_scope refreshes last_green_at exactly like a full
    product run, so the matrix colour cannot tell them apart.  Assert the scope
    survives into the Lab payload and is rendered, and that product freshness is
    withheld -- with the projection line proven live so this fails if
    product_is_green is loosened, not merely because nothing is ever green.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, REAL_CATALOG)
    bundle = tmp_path / "strategy-only"
    _write_minimal_official_quant_bundle(qa, bundle)
    cell_id = "lp.uniswap_v3.arbitrum.simple.anvil.eoa"
    qa.seal_bundle(
        bundle=bundle,
        store=store,
        catalog_path=REAL_CATALOG,
        cell_id=cell_id,
        network="anvil",
        exec_path="eoa",
        lane="adhoc",
        run_id="strategy-only-pass",
    )
    row = json.loads((store / "index" / "cell_latest.json").read_text())[cell_id]

    # The run is officially green on Strategy and carries its pre-launch scope.
    assert row["official_verdict"] == "PASS"
    assert row["last_green_at"] == row["sealed_at"]
    assert row["admission"]["claim_scope"] == {
        "required": ["strategy"],
        "not_applicable": ["books", "dashboard", "harness"],
    }
    assert row["last_product_green_at"] is None

    # The scope reaches the Lab payload and is rendered as a badge.
    qa.render_lab(store=store, catalog_path=REAL_CATALOG)
    lab = (store / "lab" / "quant.html").read_text()
    payload = json.loads(lab.split("const LAB_DATA=", 1)[1].split(";const RECHECK_ROUTES", 1)[0])
    assert payload["index"][cell_id]["admission"]["claim_scope"]["required"] == ["strategy"]
    # Both render sites are wired, and the badge rule exists to style them.
    assert "${scopeBadge(row)}${triageMarker(triage)}" in lab
    assert '${scopeLine(row)}<div class="verdicts">' in lab
    assert ".scope-badge{" in lab
    runs = [json.loads(line) for line in (store / "index" / "runs.jsonl").read_text().splitlines()]
    assert runs[-1]["admission"]["claim_scope"]["required"] == ["strategy"]

    # product_is_green is the only thing withholding product freshness for this
    # exact row, and the projection line that consumes it is live.
    assert qa.product_is_green(row["verdicts"], row["derived_claims"]) is False
    monkeypatch.setattr(qa, "product_is_green", lambda *_args, **_kwargs: True)
    qa.rebuild_quant_cell_index(store)
    loosened = json.loads((store / "index" / "cell_latest.json").read_text())[cell_id]
    assert loosened["last_product_green_at"] == row["sealed_at"]


REJECTION_LADDER_BUNDLES = (
    ("20260817-0218-aave-supply-base", "lending.aave_v3.base.simple.mainnet.eoa"),
    ("20260817-0347-looping-arb-rerun", "lending.aave_v3.arbitrum.complex.mainnet.eoa"),
)
_MINIMAL_CLAIM_SCOPE = {"required": ["strategy"], "not_applicable": ["books", "dashboard", "harness"]}


def _seal_known_good_baseline(qa, store: Path, tmp_path: Path, *, run_id: str) -> dict[str, bytes]:
    """Seal one legitimately shaped admission so byte-identity has something to lose.

    Every "official state unchanged" assertion in the ladder below passes
    vacuously on a fresh store: `cell_latest.json` is `{}` and the ledger does
    not exist, so byte-identity compares absent to absent.  This positive
    control makes both files non-empty first, and asserts they moved.
    """
    empty_latest = (store / "index" / "cell_latest.json").read_bytes()
    bundle = tmp_path / f"known-good-{run_id}"
    _write_minimal_official_quant_bundle(qa, bundle)
    qa.seal_bundle(
        bundle=bundle,
        store=store,
        catalog_path=REAL_CATALOG,
        cell_id="lp.uniswap_v3.arbitrum.simple.anvil.eoa",
        network="anvil",
        exec_path="eoa",
        lane="adhoc",
        run_id=run_id,
    )
    baseline = {
        "ledger": (store / "index" / "experiment_runs.jsonl").read_bytes(),
        "cell_latest": (store / "index" / "cell_latest.json").read_bytes(),
    }
    # The control itself must have moved both files, or the assertions are free.
    assert baseline["cell_latest"] != empty_latest
    assert json.loads(baseline["cell_latest"])["lp.uniswap_v3.arbitrum.simple.anvil.eoa"]["run_id"] == run_id
    assert baseline["ledger"]
    # And the run-directory probe used by assertion 4 must be able to find one.
    assert list((store / "quant").rglob(run_id))
    return baseline


def _assert_official_state_unchanged(
    store: Path, baseline: dict[str, bytes], *, run_id: str, attempt_count: int
) -> None:
    """The five rejection assertions, scoped so none of them is free."""
    # 1/2: the official ledger and the cell projection are byte-identical to a
    # NON-EMPTY baseline established by the positive control above.
    assert (store / "index" / "experiment_runs.jsonl").read_bytes() == baseline["ledger"]
    assert (store / "index" / "cell_latest.json").read_bytes() == baseline["cell_latest"]

    # 3: freshness and the pass streak are pure projections of the ledger, so
    # assert them explicitly rather than inferring them from 1/2.
    latest = json.loads(baseline["cell_latest"])
    for cell_id, row in json.loads((store / "index" / "cell_latest.json").read_text()).items():
        assert row["last_green_at"] == latest[cell_id]["last_green_at"]
        assert row["last_product_green_at"] == latest[cell_id]["last_product_green_at"]
        assert row["last_failure"] == latest[cell_id]["last_failure"]
    projection = json.loads((store / "index" / "experiment_history.json").read_text())
    for cell in projection["cells"].values():
        assert cell["pass_streak"] == 1
        assert cell["regressions"] == 0

    # 4: no run directory for the rejected run.  Scoped to runs/**/<run_id>,
    # NEVER to the runs/ tree: seal_bundle creates the empty day directory
    # before validation, so a whole-tree comparison fails for the wrong reason.
    assert not list((store / "quant").rglob(run_id))

    # 5: the forensic record IS present, and the run was never quarantined.
    attempts = [json.loads(row) for row in (store / "index" / "attempts.jsonl").read_text().splitlines()]
    assert len(attempts) == attempt_count
    assert attempts[-1]["attempt_id"] == run_id
    assert attempts[-1]["status"] == "REJECTED"
    assert attempts[-1]["reason_code"] == "official_admission_rejected"
    assert not (store / "attempts" / run_id).exists()


@pytest.mark.parametrize(("bundle_name", "cell_id"), REJECTION_LADDER_BUNDLES)
def test_live_aug17_bundle_is_rejected_before_any_official_state_moves(
    modules, tmp_path: Path, bundle_name: str, cell_id: str
) -> None:
    """VIB-6707 step (a): a legacy contract without claim_scope cannot seal.

    Regression pin on 85eccd216.  Provenance of these fixtures is recorded in
    tests/unit/scripts/fixtures/quant_rejection_bundles/README.md.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, REAL_CATALOG)
    baseline = _seal_known_good_baseline(qa, store, tmp_path, run_id=f"known-good-a-{bundle_name}")
    bundle = tmp_path / f"step-a-{bundle_name}"
    shutil.copytree(REJECTION_FIXTURES / bundle_name, bundle)
    assert "claim_scope" not in json.loads((bundle / "lifecycle-contract.json").read_text())

    with pytest.raises(ValueError, match="requires a pre-launch claim_scope"):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=REAL_CATALOG,
            cell_id=cell_id,
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id=f"replay-a-{bundle_name}",
        )

    _assert_official_state_unchanged(store, baseline, run_id=f"replay-a-{bundle_name}", attempt_count=1)


@pytest.mark.parametrize(("bundle_name", "cell_id"), REJECTION_LADDER_BUNDLES)
def test_grafting_a_claim_scope_still_cannot_seal_without_an_audit_decision(
    modules, tmp_path: Path, bundle_name: str, cell_id: str
) -> None:
    """VIB-6707 step (b): satisfying the shallowest guard reaches the next one.

    Step (a) alone only proves one isinstance call fires.  Grafting a minimal
    claim_scope onto the legacy contract advances the bundle to
    quant_admission._load_decision; neither live bundle contains an
    audit-decision.json at all.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, REAL_CATALOG)
    baseline = _seal_known_good_baseline(qa, store, tmp_path, run_id=f"known-good-b-{bundle_name}")
    bundle = tmp_path / f"step-b-{bundle_name}"
    shutil.copytree(REJECTION_FIXTURES / bundle_name, bundle)
    contract = json.loads((bundle / "lifecycle-contract.json").read_text())
    contract["claim_scope"] = _MINIMAL_CLAIM_SCOPE
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract, indent=1))
    assert not (bundle / "audit-decision.json").exists()

    with pytest.raises(ValueError, match="requires an owned audit-decision.json"):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=REAL_CATALOG,
            cell_id=cell_id,
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id=f"replay-b-{bundle_name}",
        )

    _assert_official_state_unchanged(store, baseline, run_id=f"replay-b-{bundle_name}", attempt_count=1)


def _forge_shape_complete_step_c_bundle(qa, fixture_dir: Path, dest: Path) -> str:
    """Upgrade a rejected fixture into a fully self-consistent OFFICIAL-PASS forgery.

    Steps (a) and (b) die on the two shallowest shape checks, which proves
    nothing about admission depth.  This helper does what a forger with only a
    text editor can do: graft the pre-launch claim_scope, rewrite the contract
    to a single runtime requirement, drop the declared guards, and author
    coverage, receipts, and an audit decision that are mutually digest-bound.
    Every declared transaction id is one the run itself declared, so the bundle
    is internally perfect.  Nothing in it is re-derived from the sealed
    db.sqlite, the run log, or a chain.
    """
    shutil.copytree(fixture_dir, dest)
    contract_path = dest / "lifecycle-contract.json"
    contract = json.loads(contract_path.read_text())
    contract["claim_scope"] = dict(_MINIMAL_CLAIM_SCOPE)
    contract["requirements"] = [
        {"id": "runtime-supply", "phase": "runtime", "intent_type": "SUPPLY", "min_executed": 1}
    ]
    contract["teardown"] = {"required": False}
    contract.pop("guards", None)
    contract_path.write_text(json.dumps(contract, indent=1))
    contract_digest = hashlib.sha256(contract_path.read_bytes()).hexdigest()

    raw_receipts = json.loads((dest / "receipt-reconciliation.json").read_text())
    declared_hashes = raw_receipts.get("canonical_hashes") or [
        row.get("tx_hash") or row.get("hash") for row in raw_receipts.get("observed_transactions", [])
    ]
    tx_id = sorted(value for value in declared_hashes if value)[0]

    (dest / "lifecycle-coverage.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract_sha256": contract_digest,
                "observations": [
                    {
                        "requirement_id": "runtime-supply",
                        "phase": "runtime",
                        "intent_type": "SUPPLY",
                        "executed": 1,
                        "transaction_ids": [tx_id],
                    }
                ],
                "teardown": {"coverage": "not_requested"},
            }
        )
    )
    _write_receipt_reconciliation(dest, [tx_id])
    _write_quant_audit_decision(qa, dest)
    return tx_id


@pytest.mark.parametrize(("bundle_name", "cell_id"), REJECTION_LADDER_BUNDLES)
def test_hand_authored_audit_decision_is_rejected_by_an_independent_derivation(
    modules, tmp_path: Path, bundle_name: str, cell_id: str
) -> None:
    """VIB-6707 step (c): the only rung that tests what this PR exists to establish.

    Steps (a) and (b) are regression pins on the shape checks.  This step makes
    every author-supplied document self-consistent, so the only thing left that
    could refuse the seal is a derivation from a source the author did not
    write.

    VIB-6712 supplied that source.  The Strategy claim is now derived from the
    bundle rather than read out of audit-decision.json, and a PASS additionally
    requires the declared lifecycle transactions to appear in the run's own
    state database.  Neither forged bundle carries one, so both are refused
    before any official state moves -- including 20260817-0347-looping-arb-rerun,
    whose own live coverage records that four of its five required runtime
    intents never executed.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, REAL_CATALOG)
    baseline = _seal_known_good_baseline(qa, store, tmp_path, run_id=f"known-good-c-{bundle_name}")
    bundle = tmp_path / f"step-c-{bundle_name}"
    _forge_shape_complete_step_c_bundle(qa, REJECTION_FIXTURES / bundle_name, bundle)

    with pytest.raises(ValueError):
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=REAL_CATALOG,
            cell_id=cell_id,
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id=f"replay-c-{bundle_name}",
        )

    _assert_official_state_unchanged(store, baseline, run_id=f"replay-c-{bundle_name}", attempt_count=1)


def test_official_seal_records_a_readable_admission_trail(modules, catalog_path: Path, tmp_path: Path) -> None:
    """VIB-6712: every green carries what it derived from, and proof it discriminates.

    The manifest and the Lab rail are one contract.  If the sealer stops
    recording the admission trail, or the rail stops rendering it, a reader can
    no longer tell a measured green from a declared one -- which is the whole
    property this work exists to establish.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    bundle = tmp_path / "bundle-admission-trail"
    qa.bootstrap_store(store, catalog_path)
    _write_minimal_official_quant_bundle(qa, bundle)

    target = qa.seal_bundle(
        bundle=bundle,
        store=store,
        catalog_path=catalog_path,
        cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
        network="mainnet",
        exec_path="eoa",
        lane="adhoc",
        run_id="admission-trail-001",
    )
    strategy = json.loads((target / "manifest.json").read_text())["derived_claims"]["strategy"]
    control = strategy["admission_control"]

    assert control["declared_status"] == "PASS", "the auditor's declaration must still be recorded"
    assert control["derived_status"] == "PASS", "the official status must be the derived one"
    assert {row["source"] for row in strategy["authorities"]} == {
        "lifecycle-contract.json",
        "lifecycle-coverage.json",
        "receipt-reconciliation.json",
        "db.sqlite",
    }
    # The negative control is the point: corrupting any cited artifact must
    # move the verdict off PASS.  An empty mutation list would mean nothing
    # was tested, so assert coverage of every authority, not merely a status.
    assert control["liveness"]["status"] == "PASS"
    assert {row["source"] for row in control["liveness"]["mutations"]} == {
        row["source"] for row in strategy["authorities"]
    }
    assert all(row["mutant_status"] != "PASS" for row in control["liveness"]["mutations"])

    lab = (store / "lab" / "quant.html").read_text()
    assert "admissionBlock" in lab, "the rail no longer renders the admission trail"
    assert '"admission_control"' in lab, "the rail has no admission data to render"


def test_projection_failure_inside_the_append_never_quarantines_a_committed_run(
    modules, catalog_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VIB-6707: the fsync/committed window must not orphan an OFFICIAL record.

    ``append_experiment`` fsyncs the ledger line and then rebuilds projections.
    Before the fix, a failure in that rebuild propagated as a plain exception
    while ``seal_bundle`` still had ``committed = False``, so the handler moved
    the run directory into ``attempts/`` out from under a durable OFFICIAL
    ledger row.  ``rebuild_quant_cell_index`` then raised on the dangling
    manifest and every later seal AND render failed permanently.
    """
    qa, _, _ = modules
    history = qa._load_history_module()
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    bundle = tmp_path / "bundle-projection-failure"
    _write_minimal_official_quant_bundle(qa, bundle)
    real_rebuild = history.rebuild_projections

    def explode(target_store, records=None):
        # Only the post-append call passes the in-memory record list; the
        # bootstrap call inside seal_bundle must still succeed.
        if records is None:
            return real_rebuild(target_store)
        raise OSError("injected-projection-rebuild-failure")

    monkeypatch.setattr(history, "rebuild_projections", explode)
    # The state assertions below are the contract; the exception type is checked
    # last so that a regression fails on observable damage, not on a missing name.
    failure: Exception | None = None
    try:
        qa.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id="projection-failure-window",
        )
    except Exception as exc:  # noqa: BLE001 - the injected failure must still surface
        failure = exc
    monkeypatch.setattr(history, "rebuild_projections", real_rebuild)
    assert failure is not None, "an injected projection failure must still reach the caller"

    # 1. The ledger row exists and is OFFICIAL.
    ledger_lines = (store / "index/experiment_runs.jsonl").read_text().splitlines()
    assert len(ledger_lines) == 1
    record = json.loads(ledger_lines[0])
    assert record["run_id"] == "projection-failure-window"
    assert record["admission"]["status"] == "OFFICIAL"

    # 2. The run directory still exists at its ledger-recorded store_path.
    assert (store / record["store_path"]).is_dir()
    assert (store / record["store_path"] / "manifest.json").is_file()
    assert not (store / "attempts" / "projection-failure-window").exists()

    # 3. No contradictory attempt row claims the same run was rejected.
    attempts_path = store / "index" / "attempts.jsonl"
    attempts = attempts_path.read_text().splitlines() if attempts_path.exists() else []
    assert not [row for row in attempts if json.loads(row).get("attempt_id") == "projection-failure-window"]

    # 4. The store is still usable: projections rebuild and the Lab renders.
    qa.rebuild_quant_cell_index(store)
    latest = json.loads((store / "index/cell_latest.json").read_text())
    assert latest["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]["run_id"] == "projection-failure-window"
    assert qa.render_lab(store=store, catalog_path=catalog_path).is_file()

    # 5. And the failure is typed, so no caller can mistake it for a failed append.
    assert isinstance(failure, history.ProjectionRebuildError)
    assert "injected-projection-rebuild-failure" in str(failure)

    # POSITIVE CONTROL for assertion 3: the same store DOES record a rejection
    # row when a seal fails before the ledger commit, so "no attempt row" above
    # is a live measurement rather than a file that is never written.
    rejected = tmp_path / "bundle-rejected"
    rejected.mkdir()
    (rejected / "finding.json").write_text(json.dumps({"verdicts": {"strategy": "PASS"}}))
    (rejected / "git.json").write_text(json.dumps(TEST_SDK))
    with pytest.raises(ValueError, match="requires lifecycle-contract.json"):
        qa.seal_bundle(
            bundle=rejected,
            store=store,
            catalog_path=catalog_path,
            cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
            network="mainnet",
            exec_path="eoa",
            lane="adhoc",
            run_id="pre-commit-rejection",
        )
    recorded = [json.loads(row) for row in (store / "index" / "attempts.jsonl").read_text().splitlines()]
    assert [row["attempt_id"] for row in recorded] == ["pre-commit-rejection"]
    assert recorded[0]["reason_code"] == "official_admission_rejected"


def test_strategy_pass_rejects_missing_required_intent_observation(modules, tmp_path: Path) -> None:
    """ALM-3254: declared PASS cannot replace the requested action's receipt."""
    qa, _, _ = modules
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategies/example/strategy.py"],
            "transition_sequence": ["idle -> supply"],
        },
        "requirements": [{"id": "supply", "phase": "runtime", "intent_type": "SUPPLY", "min_executed": 1}],
        "teardown": {"required": False},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    (bundle / "lifecycle-coverage.json").write_text(
        json.dumps({"schema_version": 1, "contract_sha256": digest, "observations": [], "teardown": {}})
    )
    (bundle / "audit.md").write_text(f"LIFECYCLE_COVERAGE_CONFIRMED: yes\nLIFECYCLE_CONTRACT_SHA256: {digest}\n")

    with pytest.raises(ValueError, match="missing lifecycle observation: supply"):
        qa._validate_lifecycle_evidence(bundle, {"strategy": "PASS"})


def test_strategy_pass_rejects_empty_teardown_when_unwind_was_required(modules, tmp_path: Path) -> None:
    """ALM-3242: nothing-to-unwind is not proof of a requested closure."""
    qa, _, _ = modules
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategies/example/strategy.py"],
            "transition_sequence": ["open -> teardown"],
        },
        "requirements": [{"id": "open", "phase": "runtime", "intent_type": "LP_OPEN", "min_executed": 1}],
        "teardown": {"required": True, "coverage": "proved"},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    (bundle / "lifecycle-coverage.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract_sha256": digest,
                "observations": [
                    {
                        "requirement_id": "open",
                        "phase": "runtime",
                        "intent_type": "LP_OPEN",
                        "executed": 1,
                        "transaction_ids": ["0x" + "1" * 64],
                    }
                ],
                "teardown": {"coverage": "nothing_to_unwind", "intents_executed": 0},
            }
        )
    )
    (bundle / "audit.md").write_text(f"LIFECYCLE_COVERAGE_CONFIRMED: yes\nLIFECYCLE_CONTRACT_SHA256: {digest}\n")

    with pytest.raises(ValueError, match="teardown coverage is nothing_to_unwind; required proved"):
        qa._validate_lifecycle_evidence(bundle, {"strategy": "PASS"})


def test_signal_validation_can_explicitly_prove_nothing_to_unwind(modules, tmp_path: Path) -> None:
    """A measured no-op remains valid when that is the sealed test objective."""
    qa, _, _ = modules
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    contract = {
        "schema_version": 1,
        "goal": "signal-validation",
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategies/example/strategy.py"],
            "transition_sequence": ["idle -> hold", "hold -> teardown"],
        },
        "requirements": [],
        "teardown": {"required": True, "coverage": "nothing_to_unwind"},
    }
    (bundle / "lifecycle-contract.json").write_text(json.dumps(contract))
    digest = hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest()
    (bundle / "lifecycle-coverage.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract_sha256": digest,
                "observations": [],
                "teardown": {"coverage": "nothing_to_unwind", "intents_executed": 0},
            }
        )
    )
    (bundle / "audit.md").write_text(f"LIFECYCLE_COVERAGE_CONFIRMED: yes\nLIFECYCLE_CONTRACT_SHA256: {digest}\n")

    assert qa._validate_lifecycle_evidence(
        bundle,
        {"strategy": "PASS"},
        catalog_cell={"claim_kind": "true_negative"},
    ) == {
        "contract_sha256": digest,
        "receipt_integrity": {
            "async_order_ids": [],
            "submitted_transaction_count": 0,
            "status": "NOT_APPLICABLE",
        },
        "requirements_proved": 0,
        # A true-negative proves nothing executed, so there is no transaction
        # for the state-database confirmation to look for (VIB-6712).
        "lifecycle_transaction_ids": [],
    }


def test_signal_validation_cannot_refresh_an_action_bearing_catalog_cell(modules, tmp_path: Path) -> None:
    """A true-negative contract is valid only for a catalog cell that declares that claim kind."""
    qa, _, _ = modules
    contract = {
        "schema_version": 1,
        "goal": "signal-validation",
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategies/example/strategy.py"],
            "transition_sequence": ["idle -> hold"],
        },
        "requirements": [],
        "teardown": {"required": False},
    }

    with pytest.raises(ValueError, match="true_negative"):
        qa._validate_lifecycle_feasibility(contract, catalog_cell={"primitive": "swap"})


def test_alm_3267_quant_pass_rejects_status_zero_as_explicit_failure(modules, tmp_path: Path) -> None:
    """A mined revert is terminal evidence, but it can never support Strategy PASS."""
    qa, _, _ = modules
    bundle = tmp_path / "alm-3267"
    bundle.mkdir()
    tx_id = "0x" + "67" * 32
    _write_receipt_reconciliation(bundle, [tx_id], statuses=[0])

    with pytest.raises(ValueError, match=r"explicitly reverted transaction .*status=0"):
        qa._validate_submission_receipt_integrity(bundle, transaction_ids=[tx_id], async_order_ids=[])


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "mismatched"])
def test_alm_3276_quant_pass_rejects_non_bijective_receipt_sets(modules, tmp_path: Path, mutation: str) -> None:
    """Every submitted hash needs exactly one identity-matching terminal receipt."""
    qa, _, _ = modules
    bundle = tmp_path / mutation
    bundle.mkdir()
    tx_id = "0x" + "76" * 32
    report = _write_receipt_reconciliation(bundle, [tx_id])
    transactions = report["intents"][0]["transactions"]
    if mutation == "missing":
        transactions.clear()
    elif mutation == "duplicate":
        transactions.append(dict(transactions[0]))
    else:
        transactions[0]["raw_receipt"]["transactionHash"] = "0x" + "99" * 32
    (bundle / "receipt-reconciliation.json").write_text(json.dumps(report))

    with pytest.raises(ValueError, match="membership mismatch|duplicates terminal receipt|identity does not match"):
        qa._validate_submission_receipt_integrity(bundle, transaction_ids=[tx_id], async_order_ids=[])


def test_async_order_identity_is_not_treated_as_an_evm_transaction(modules, tmp_path: Path) -> None:
    qa, _, _ = modules
    bundle = tmp_path / "async"
    bundle.mkdir()
    order_id = "0x" + "ab" * 32
    _write_receipt_reconciliation(bundle, [], async_order_ids=[order_id])

    assert qa._validate_submission_receipt_integrity(bundle, transaction_ids=[], async_order_ids=[order_id]) == {
        "async_order_ids": [order_id],
        "submitted_transaction_count": 0,
        "status": "PASS",
    }


def test_zero_identity_receipt_reconciliation_cannot_support_an_official_strategy_verdict(
    modules, tmp_path: Path
) -> None:
    """EMPTY != ZERO on the path that paints ``last_green_at``.

    A ``receipt-reconciliation.json`` binding neither a canonical EVM
    transaction nor an async order identity satisfies every cardinality check by
    comparing 0 against 0, and previously returned receipt-integrity ``PASS``.
    That verdict reaches ``cell_latest.json`` freshness via
    ``seal_bundle`` -> ``_derive_quant_claims`` -> ``rebuild_quant_cell_index``.
    Omitting the file is the honest NOT_APPLICABLE; publishing an empty one is
    not evidence.
    """
    qa, _, _ = modules
    bundle = tmp_path / "zero-identity"
    bundle.mkdir()
    _write_receipt_reconciliation(bundle, [])

    with pytest.raises(ValueError, match="binds no canonical transaction and no async order identity"):
        qa._validate_submission_receipt_integrity(bundle, transaction_ids=[], async_order_ids=[])


def test_absent_receipt_reconciliation_remains_honestly_not_applicable(modules, tmp_path: Path) -> None:
    """LIVENESS: the guard must not swallow the legitimate unmeasured lane."""
    qa, _, _ = modules
    bundle = tmp_path / "absent"
    bundle.mkdir()

    assert qa._validate_submission_receipt_integrity(bundle, transaction_ids=[], async_order_ids=[]) == {
        "async_order_ids": [],
        "submitted_transaction_count": 0,
        "status": "NOT_APPLICABLE",
    }


def test_ticket_index_groups_reporting_experiments_and_imports_linear_metadata(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    history = qa._load_history_module()
    cell_id = "lp.uniswap_v3.arbitrum.simple.mainnet.eoa"
    for sequence, sealed_at in enumerate(
        (datetime(2026, 8, 3, 10, tzinfo=UTC), datetime(2026, 8, 4, 10, tzinfo=UTC)),
        start=1,
    ):
        run_id = f"run-{sequence}"
        run = store / "quant" / "2026" / "08" / f"0{sequence + 2}" / run_id
        run.mkdir(parents=True)
        (run / "report.html").write_text(f"<h1>report {sequence}</h1>")
        (run / "finding.json").write_text(
            json.dumps(
                {
                    "verdicts": {"strategy": "FAIL"},
                    "tickets": [
                        {
                            "id": "QA-4242",
                            "status": "filed",
                            "covers": f"finding from experiment {sequence}",
                        }
                    ],
                }
            )
        )
        admission = {
            "status": "OFFICIAL",
            "evidence_set_sha256": "e" * 64,
            "audit_decision_sha256": "f" * 64,
            "claim_scope": {
                "required": ["strategy"],
                "not_applicable": ["books", "dashboard", "harness"],
            },
        }
        manifest = {
            "schema_version": 2,
            "run_id": run_id,
            "cell_id": cell_id,
            "network": "mainnet",
            "exec_path": "eoa",
            "lane": "daily",
            "git": TEST_SDK,
            "verdicts": {"strategy": "FAIL", "books": "N/A", "dashboard": "N/A", "harness": "N/A"},
            "derived_claims": {},
            "admission": admission,
            "artifacts": [
                {
                    "kind": "report.html",
                    "relpath": (run / "report.html").relative_to(store).as_posix(),
                }
            ],
            "sealed_at": sealed_at.isoformat().replace("+00:00", "Z"),
        }
        (run / "manifest.json").write_text(json.dumps(manifest))
        history.append_experiment(
            store=store,
            surface="quant",
            run_id=run_id,
            run_dir=run,
            manifest_path=run / "manifest.json",
            sdk=TEST_SDK,
            cell_verdicts={cell_id: "FAIL"},
            completed_at=manifest["sealed_at"],
            sealed_at=manifest["sealed_at"],
            admission=admission,
        )
    qa.rebuild_quant_cell_index(store)

    index = qa.rebuild_ticket_index(
        store,
        linear_issues={
            "issues": [
                {
                    "id": "QA-4242",
                    "title": "QA detected accounting divergence",
                    "priority": {"value": 1, "name": "Urgent"},
                    "status": "Triage",
                    "statusType": "triage",
                    "url": "https://linear.app/almanak/issue/QA-4242/example",
                    "updatedAt": "2026-08-04T12:00:00Z",
                }
            ]
        },
        now=datetime(2026, 8, 4, 12, tzinfo=UTC),
    )

    ticket = index["tickets"]["QA-4242"]
    assert ticket["priority"] == {"value": 1, "name": "Urgent"}
    assert ticket["status"] == {"name": "Triage", "type": "triage"}
    assert index["linear_synced_at"] == "2026-08-04T12:00:00Z"
    assert [report["run_id"] for report in ticket["reports"]] == ["run-2", "run-1"]
    assert all(report["report_relpath"].endswith("report.html") for report in ticket["reports"])

    qa.render_lab(store=store, catalog_path=store / "catalog" / "cells.yaml")
    page = (store / "lab" / "tickets.html").read_text()
    assert "Tickets filed by QA" in page
    assert "https://linear.app/almanak/issue/QA-4242/example" in page
    assert "run-2" in page and "run-1" in page

    quant_page = (store / "lab" / "quant.html").read_text()
    assert "Tickets filed by QA" not in quant_page


def test_linear_refresh_fetches_exact_referenced_issue_metadata(modules) -> None:
    qa, _, _ = modules
    calls: list[str] = []

    def post(api_key: str, payload: bytes) -> str:
        assert api_key == "secret-not-persisted"
        request = json.loads(payload)
        identifier = request["variables"]["id"]
        calls.append(identifier)
        return json.dumps(
            {
                "data": {
                    "issue": {
                        "identifier": identifier,
                        "title": f"Live {identifier}",
                        "priority": 2,
                        "priorityLabel": "High",
                        "url": f"https://linear.app/almanak/issue/{identifier}/live",
                        "updatedAt": "2026-08-04T13:00:00Z",
                        "state": {"name": "In Progress", "type": "started"},
                    }
                }
            }
        )

    raw = qa.fetch_linear_issues(
        ["VIB-2", "VIB-1", "VIB-2"],
        api_key="secret-not-persisted",
        post=post,
    )

    assert calls == ["VIB-1", "VIB-2"]
    mapped = qa._linear_issue_map(raw)
    assert mapped["VIB-1"]["priority"] == {"value": 2, "name": "High"}
    assert mapped["VIB-1"]["status"] == {"name": "In Progress", "type": "started"}


def test_ticket_metadata_rejects_non_linear_links(modules) -> None:
    qa, _, _ = modules

    with pytest.raises(ValueError, match="must use https://linear.app"):
        qa._linear_issue_map(
            [
                {
                    "id": "QA-4042",
                    "title": "Untrusted metadata",
                    "url": "javascript:alert(1)",
                }
            ]
        )


def _seal_one_quant_run(qa, catalog_path: Path, tmp_path: Path) -> Path:
    """Seal a minimal admitted Quant bundle so a migration has real evidence to move."""
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    bundle = tmp_path / "migration-bundle"
    _write_minimal_quant_bundle(qa, bundle)
    qa.seal_bundle(
        bundle=bundle,
        store=store,
        catalog_path=catalog_path,
        cell_id="lp.uniswap_v3.arbitrum.simple.mainnet.eoa",
        network="mainnet",
        exec_path="eoa",
        lane="daily",
        run_id="migrate-001",
        selection={"bucket": "recency", "reason": "oldest"},
        now=datetime(2026, 8, 3, 12, 30, tzinfo=UTC),
    )
    return store


def _demote_store_to_legacy_layout(qa, store: Path) -> None:
    """Rebuild the store under the pre-schema-2 layout using the chain's own digest rule.

    Written with plain string surgery rather than the migrator's remap so the forward
    migration is not being tested against its own inverse.
    """
    history = qa._load_history_module()
    (store / "quant").rename(store / "runs")
    ledger = store / "index" / history.HISTORY_LEDGER_NAME
    rows = [json.loads(line) for line in ledger.read_text().splitlines() if line.strip()]
    previous = None
    rebuilt = []
    for row in rows:
        row = json.loads(json.dumps(row).replace('"quant/', '"runs/'))
        if "artifact_set_sha256" in row and row.get("artifacts") is not None:
            row["artifact_set_sha256"] = history._sha256_bytes(history._canonical_bytes(row["artifacts"]))
        row["previous_record_sha256"] = previous
        row.pop("record_sha256", None)
        previous = history._record_digest(row)
        row["record_sha256"] = previous
        rebuilt.append(row)
    ledger.write_text("".join(json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n" for r in rebuilt))
    (store / "store.json").unlink(missing_ok=True)


def test_bootstrap_refuses_a_store_whose_evidence_predates_the_current_layout(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    """The guard exists so a rename can never leave two plausible evidence roots.

    Without it, bootstrap would create an empty ``quant/`` beside a populated
    ``runs/`` and the Quant board would render "no sealed run on this machine yet"
    over real sealed evidence.
    """
    qa, _, _ = modules
    store = _seal_one_quant_run(qa, catalog_path, tmp_path)
    _demote_store_to_legacy_layout(qa, store)

    with pytest.raises(qa.StoreSchemaError) as excinfo:
        qa.bootstrap_store(store, catalog_path)

    assert "store-migrate" in str(excinfo.value)
    assert not (store / "quant").exists(), "refusal must not leave a second evidence root behind"
    assert list((store / "runs").rglob("migrate-001")), "refusal must not disturb the legacy evidence"


def test_layout_migration_moves_evidence_and_leaves_the_chain_verifiable(
    modules, catalog_path: Path, tmp_path: Path
) -> None:
    """Renaming an evidence directory rewrites every dependent digest.

    The assertions below are made against ``verify_history`` — the chain's own
    validator — rather than against a reversal of the migrator, so a migrator that
    corrupts the record cannot satisfy them by being self-consistent.
    """
    qa, _, _ = modules
    history = qa._load_history_module()
    store = _seal_one_quant_run(qa, catalog_path, tmp_path)
    _demote_store_to_legacy_layout(qa, store)
    legacy_terminal = json.loads((store / "index" / history.HISTORY_LEDGER_NAME).read_text().splitlines()[-1])[
        "record_sha256"
    ]
    legacy_count = len(history.read_history(store))

    result = qa.migrate_store_layout(store, apply=True)

    assert result["status"] == "MIGRATED"
    assert result["pre_migration_terminal_sha256"] == legacy_terminal
    assert not (store / "runs").exists()
    assert list((store / "quant").rglob("migrate-001"))

    migrated = history.read_history(store)  # raises on any chain, digest, or sequence break
    assert len(migrated) == legacy_count
    assert history.verify_history(store)["status"] in {"PASS", "PASS_WITH_WARNINGS"}

    ledger_text = (store / "index" / history.HISTORY_LEDGER_NAME).read_text()
    assert '"runs/' not in ledger_text, "a migrated ledger must not still point at the old root"
    for record in migrated:
        for artifact in record.get("artifacts") or []:
            assert (store / artifact["relpath"]).exists(), f"migrated artifact is missing: {artifact['relpath']}"

    preserved = store / "index" / f"{Path(history.HISTORY_LEDGER_NAME).stem}.pre-schema2.jsonl"
    assert '"runs/' in preserved.read_text(), "the pre-migration ledger must be preserved verbatim"
    manifest = json.loads((store / "store.json").read_text())
    assert manifest["schema_version"] == qa.STORE_SCHEMA_VERSION
    assert manifest["migrations"][-1]["pre_migration_terminal_sha256"] == legacy_terminal

    assert qa.migrate_store_layout(store, apply=True)["status"] == "ALREADY_CURRENT"


def test_the_intent_board_legend_help_and_painter_share_one_vocabulary(modules) -> None:
    """One declaration must feed the swatch legend, the operator help, and the JS.

    These were three lists that disagreed. The legend advertised VERIFIED, FAILED,
    NO LIVE RUNNER and NOT SUPPORTED — four labels the board never rendered, so an
    engineer matching a swatch to a cell was looking up words that did not exist.
    The help text defined four labels out of thirteen, and every label it omitted
    (MAP DRIFT, CONTRACT GAP, LIVE ENVELOPE GAP, SOFT, CONTROL, LEGACY) was one that
    can be mistaken for a pass. A board that explains only its obviously-unfinished
    states is at its least helpful exactly where it matters most.
    """
    qa, _, _ = modules
    declared = {row[0] for row in qa.INTENT_STATUSES}
    painted = set(re.findall(r"INTENT_STATUS\.(\w+)", qa.INTENT_JS))

    assert painted, "the board must paint its labels from the declaration, not from literals"
    assert painted == declared, (
        f"painted-but-undeclared: {sorted(painted - declared)}; declared-but-never-painted: {sorted(declared - painted)}"
    )
    assert not re.search(r"label:'[A-Z][A-Z ]*'", qa.INTENT_JS), "a hardcoded label can drift from the legend"

    legend = qa._intent_legend_html()
    help_html = qa._intent_status_help_html()
    for _id, key, group, label, meaning in qa.INTENT_STATUSES:
        assert group in qa.INTENT_STATUS_GROUPS, f"{label} is in an undeclared group: {group}"
        assert label in legend, f"{label} is paintable but missing from the colour legend"
        assert label in help_html, f"{label} is paintable but has no definition in the help"
        assert html.escape(meaning) in help_html, f"{label} is listed but its meaning is not rendered"
        assert html.escape(meaning) in legend, f"{label}'s legend swatch has no hover definition"
        # the swatch is painted by the same class as the cell, so it cannot drift
        assert f'class="sw c-{key}"' in legend, f"{label}'s swatch does not use its own cell colour"

    for group in qa.INTENT_STATUS_GROUPS:
        assert group in help_html, f"the help omits the {group!r} group entirely"


# --- Fidelity vocabulary ------------------------------------------------------


def _intent_tree_fidelity_flag_names() -> set[str]:
    """Every key passed as ``flags={...}`` to ``record_fidelity`` under tests/intents, via AST."""
    names: set[str] = set()
    for path in (REPO_ROOT / "tests" / "intents").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or getattr(node.func, "attr", None) != "record_fidelity":
                continue
            for keyword in node.keywords:
                if keyword.arg == "flags" and isinstance(keyword.value, ast.Dict):
                    names.update(key.value for key in keyword.value.keys if isinstance(key, ast.Constant))
    return names


def test_every_intent_tree_fidelity_flag_is_registered_with_a_question(modules) -> None:
    qa_coverage, _, _ = modules
    names = _intent_tree_fidelity_flag_names()
    assert names, "AST sweep found no record_fidelity(flags=...) sites; the sweep itself is broken"
    unregistered = sorted(name for name in names if name not in qa_coverage.FIDELITY_CHECKS)
    assert not unregistered, f"fidelity flags used by tests/intents without a registered question: {unregistered}"
    group_keys = {key for key, _, _ in qa_coverage.FIDELITY_GROUPS}
    for name, (group, question) in qa_coverage.FIDELITY_CHECKS.items():
        assert group in group_keys, name
        assert question and question != name, name


def test_fidelity_flag_groups_follow_reasoning_order_and_keep_raw_names(modules) -> None:
    qa_coverage, _, _ = modules
    flags = {
        "output_transfer_unambiguous": True,
        "amount_eq_wallet_delta": False,
        "beneficiary_match": True,
        "parse_success": True,
        "brand_new_predicate_nobody_described": True,
    }
    groups = qa_coverage.fidelity_flag_groups(flags)
    assert [group["key"] for group in groups] == ["parser", "witness", "quality"]
    parser_rows = groups[0]["rows"]
    assert [row["name"] for row in parser_rows] == [
        "parse_success",
        "beneficiary_match",
        "brand_new_predicate_nobody_described",
    ]
    assert parser_rows[-1]["registered"] is False
    assert parser_rows[-1]["question"] == "Brand new predicate nobody described"
    witness_row = groups[1]["rows"][0]
    assert witness_row["passed"] is False
    assert witness_row["question"] == "Parsed amount = the test wallet's balance change"


def test_fidelity_verdict_sentence_names_failures_and_witness_count(modules) -> None:
    qa_coverage, _, _ = modules
    witnesses = [{"kind": "wallet_balance_delta"}, {"kind": "independent_transfer_logs"}]
    assert qa_coverage.fidelity_verdict_sentence({"a": True, "b": True}, witnesses, True) == (
        "All 2 checks passed, checked against 2 independent witnesses — receipt graded HARD."
    )
    assert qa_coverage.fidelity_verdict_sentence({"a": True}, [], False) == (
        "All 1 checks passed, with no independent witness recorded — receipt graded SOFT."
    )
    failing = qa_coverage.fidelity_verdict_sentence({"amount_eq_transfer": False, "user_match": True}, witnesses, False)
    assert failing.startswith("1 of 2 checks failed (amount = transfer)")
    assert failing.endswith("receipt capped at SOFT.")
    assert "cannot be graded HARD" in qa_coverage.fidelity_verdict_sentence({}, witnesses, False)


def test_fidelity_checklist_html_marks_failures_and_unregistered_names(modules) -> None:
    qa_coverage, _, _ = modules
    html_out = qa_coverage.fidelity_checklist_html(
        {"amount_eq_wallet_delta": False, "mystery_flag": True},
        esc=lambda value: str(value),
    )
    assert '<li class="fail"><span class="mark">✗</span>' in html_out
    assert "Parsed amount = the test wallet's balance change" in html_out
    assert "<code>amount_eq_wallet_delta</code>" in html_out
    assert "no description registered" in html_out
    assert "Can the witnesses be trusted?" not in html_out
    witness_html = qa_coverage.fidelity_witness_html(
        [{"kind": "wallet_balance_delta", "token": "0x" + "ab" * 20, "amount_raw": 500}, {"kind": "novel_kind"}],
        esc=lambda value: str(value),
    )
    assert "Wallet balance change" in witness_html
    assert "amount raw 500" in witness_html
    assert "0xabab…abab" in witness_html
    assert "Novel kind" in witness_html
    assert "nothing outside the parser corroborates" in qa_coverage.fidelity_witness_html([], esc=str)


def test_harness_failure_seal_retracts_a_stale_green(modules, catalog_path: Path, tmp_path: Path) -> None:
    """A run that fails before producing evidence must retract the cell, not vanish.

    Before seal_intent_harness_failure existed, this exact shape — pytest exits, no
    evidence-manifest.json — raised without sealing, and intent_latest.json kept the
    previous PASS with no trace that a newer run failed. Observed live 2026-08-28:
    a base fork failure left the board unchanged. The instrument could add green but
    never retract it.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    plan = qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.anvil.safe")
    cell_id = plan["cell_id"]

    # a previously sealed green for the same cell
    latest_path = store / "index" / "intent_latest.json"
    latest_path.write_text(
        json.dumps(
            {
                cell_id: {
                    "intent_cell_id": cell_id,
                    "status": "PASS",
                    "evidence_status": "COMPLETE",
                    "contract_status": "VERIFIED",
                    "attribution_mode": "exact-runtime",
                    "last_pass_at": "2026-08-01T00:00:00Z",
                    "sealed_at": "2026-08-01T00:00:00Z",
                    "catalog_sha256": plan["catalog_sha256"],
                }
            }
        )
    )

    # yesterday's failure shape: a run.log and plan.json, pytest produced NO junit
    # and NO evidence manifest
    workspace = tmp_path / "work"
    workspace.mkdir()
    (workspace / "run.log").write_text("fixture skip: Anvil could not start\n")
    (workspace / "plan.json").write_text(json.dumps(plan))

    target = qa.seal_intent_harness_failure(
        store=store,
        catalog_path=catalog_path,
        plan=plan,
        run_id="hf-retract-001",
        workspace=workspace,
        returncode=1,
        sdk=TEST_SDK,
    )

    row = json.loads(latest_path.read_text())[cell_id]
    assert row["status"] == "FAIL", "the stale green must be retracted"
    assert row["evidence_status"] == "HARNESS_FAIL"
    assert row["attribution_mode"] == "harness-failure"
    assert row["last_pass_at"] == "2026-08-01T00:00:00Z", "retraction removes the claim, not the history"
    assert "missing: results.xml, evidence-manifest.json" in row["failure_reason"]

    # the retraction is a first-class immutable seal: report + preserved inputs + ledger row
    assert (target / "report.html").is_file()
    assert (target / "run.log").is_file()
    history = qa._load_history_module()
    records = history.read_history(store)
    assert records, "the retraction must enter the hash-chained ledger"
    last = records[-1]
    assert last["surface"] == "intent"
    assert last["cell_verdicts"] == {cell_id: "FAIL"}


def test_harness_failure_seal_can_never_write_a_pass(modules, catalog_path: Path, tmp_path: Path) -> None:
    """Even a green JUnit cannot turn a harness failure into a PASS.

    The exact live shape: pytest exits 0 with a passing-looking results.xml (a skip),
    but the evidence manifest was never published. Evidence publication failed, so
    nothing was proven; the seal must be FAIL/HARNESS_FAIL regardless of what the
    JUnit says. PASS admission belongs exclusively to seal_intent_junit's
    fail-closed path.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    plan = qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.anvil.safe")
    workspace = tmp_path / "work"
    workspace.mkdir()
    nodeid = plan["proof_recipe"]["nodeids"][0]
    classname = nodeid.split("::")[0].removesuffix(".py").replace("/", ".") + "." + nodeid.split("::")[1]
    (workspace / "results.xml").write_text(
        f'<testsuite tests="1"><testcase classname="{classname}" name="{nodeid.split("::")[-1]}" time="1.0" /></testsuite>'
    )
    (workspace / "run.log").write_text("1 passed\n")

    target = qa.seal_intent_harness_failure(
        store=store,
        catalog_path=catalog_path,
        plan=plan,
        run_id="hf-nopass-001",
        workspace=workspace,
        returncode=0,
        sdk=TEST_SDK,
    )

    summary = json.loads((target / "summary.json").read_text())
    assert [cell["status"] for cell in summary["cells"]] == ["FAIL"]
    assert [cell["evidence_status"] for cell in summary["cells"]] == ["HARNESS_FAIL"]
    row = json.loads((store / "index" / "intent_latest.json").read_text())[plan["cell_id"]]
    assert row["status"] == "FAIL"
    assert row.get("last_pass_at") is None


def test_the_harness_canary_cell_resolves_to_a_real_failing_node(modules) -> None:
    """The canary is only an alarm if its wiring cannot rot silently.

    Three ways it could disarm without anyone noticing: the synthesized cell
    drops out of the catalog, the plan stops resolving, or the nodeid points at
    a test that no longer exists (an unpainted canary must mean "never run",
    never "file was deleted"). Each is pinned here; the runtime failure itself
    is pinned by test_the_canary_fails_when_armed_and_skips_when_not.
    """
    qa, _, _ = modules
    catalog = qa.build_intent_catalog()
    cell = next(c for c in catalog["cells"] if c["id"] == qa.HARNESS_CANARY_CELL_ID)
    assert cell["protocol"] == "harness_control"
    assert cell["presence"] == "covered"
    assert cell["mainnet_recipes"] == [], "the canary must never grow a mainnet runner"

    plan = qa.intent_cell_plan(cell_id=f"{qa.HARNESS_CANARY_CELL_ID}.anvil.safe")
    nodeids = plan["proof_recipe"]["nodeids"]
    assert len(nodeids) == 1
    path, function = nodeids[0].split("::")
    source = (Path(qa.REPO_ROOT) / path).read_text()
    assert f"def {function}(" in source, "canary nodeid points at a test that no longer exists"
    assert "_fail_if_armed()" in source


def test_the_canary_fails_when_armed_and_skips_when_not(monkeypatch: pytest.MonkeyPatch) -> None:
    """Negative control on the canary itself: armed, it MUST raise.

    If someone 'fixes' the canary, its cells would seal PASS and paint CANARY
    BROKEN — but only after a run. This catches the disarm at unit speed.
    """
    from tests.qa_lab import test_harness_canary as canary

    monkeypatch.setenv("ALMANAK_QA_CANARY", "1")
    with pytest.raises(AssertionError, match="fails by design"):
        canary.test_harness_canary_must_fail_safe()

    monkeypatch.delenv("ALMANAK_QA_CANARY")
    with pytest.raises(pytest.skip.Exception):
        canary.test_harness_canary_must_fail_eoa()


def test_seal_admission_refusal_retracts_instead_of_keeping_the_stale_green(
    modules, catalog_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A completed run whose evidence is refused at admission must retract the cell.

    This is the keep-stale-green hole one layer below the incomplete-evidence
    shape: pytest exits 0, evidence-manifest.json exists, but seal_intent_junit
    raises (a semantic-contract violation, a JUnit/evidence mismatch). The old
    behaviour propagated the exception with no seal, leaving the previous PASS
    standing — a run that PROVED its evidence inadmissible left the board
    claiming the cell was fine.
    """
    qa, _, _ = modules
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    plan = qa.intent_cell_plan(cell_id="intent.aave_v3.arbitrum.SUPPLY.anvil.safe")
    cell_id = plan["cell_id"]
    latest_path = store / "index" / "intent_latest.json"
    latest_path.write_text(
        json.dumps(
            {
                cell_id: {
                    "intent_cell_id": cell_id,
                    "status": "PASS",
                    "evidence_status": "COMPLETE",
                    "attribution_mode": "exact-runtime",
                    "catalog_sha256": plan["catalog_sha256"],
                }
            }
        )
    )

    class _CompletedRun:
        """Stands in for pytest: writes complete-looking seal input, exits 0."""

        def __init__(self, command):
            junit = next(arg.split("=", 1)[1] for arg in command if arg.startswith("--junitxml="))
            evidence = next(arg.split("=", 1)[1] for arg in command if arg.startswith("--intent-evidence-dir="))
            Path(junit).write_text('<testsuite tests="1"><testcase classname="t" name="n" time="1"/></testsuite>')
            Path(evidence).mkdir(parents=True, exist_ok=True)
            (Path(evidence) / "evidence-manifest.json").write_text("{}")
            self.stdout = io.StringIO("")

        def wait(self):
            return 0

    real_popen = qa.subprocess.Popen

    def _popen(command, **kwargs):
        # Only the proof-run pytest is faked; the catalog worker and any other
        # subprocess the seal path spawns must stay real.
        if any(str(arg).startswith("--junitxml=") for arg in command):
            return _CompletedRun(command)
        return real_popen(command, **kwargs)

    def _refusing_seal(**_: object):
        raise ValueError("semantic contract parser amount 1 does not equal request 2")

    history = qa._load_history_module()
    monkeypatch.setattr(qa, "intent_cell_plan", lambda *, cell_id: plan)
    monkeypatch.setattr(qa, "fork_upstream_is_public_rpc", lambda _chain: False)
    monkeypatch.setattr(qa, "seal_intent_junit", _refusing_seal)
    monkeypatch.setattr(qa.subprocess, "Popen", _popen)
    monkeypatch.setattr(history, "provenance_from_worktree", lambda *a, **k: TEST_SDK)

    with pytest.raises(RuntimeError, match="seal admission refused"):
        qa.run_intent_cell(cell_id=f"{cell_id}", store=store, catalog_path=catalog_path, run_id="admission-001")

    row = json.loads(latest_path.read_text())[cell_id]
    assert row["status"] == "FAIL", "an admission refusal must retract the stale green"
    assert row["evidence_status"] == "HARNESS_FAIL"
    assert "seal admission refused" in row["failure_reason"]
    assert "parser amount 1 does not equal request 2" in row["failure_reason"]
    retraction_dirs = list((store / "intents").rglob("admission-001-retraction"))
    assert len(retraction_dirs) == 1
    assert (retraction_dirs[0] / "admission-refusal.txt").is_file()


def test_mainnet_failure_seal_retracts_the_cell_from_a_failed_bundle(
    modules, catalog_path: Path, tmp_path: Path, monkeypatch
) -> None:
    """A funded mainnet failure must paint FAIL on the board, not vanish.

    Observed live 2026-08-30: three funded Uniswap mainnet runs failed (open
    positions, quarantined wallets) and intent_latest.json had NO entry for any
    of them — the mainnet runner sealed only on PASS while the Anvil lane
    already retracted through seal_intent_harness_failure. The retraction must
    carry the runner's own measured error and the runner.log as run.log.
    """
    qa, _, _ = modules
    # The live runner seals only from the clean SHA its plan bound; the unit test
    # runs in whatever tree the developer has, so pin the provenance instead.
    monkeypatch.setattr(qa._load_history_module(), "provenance_from_worktree", lambda root: dict(TEST_SDK))
    store = tmp_path / "store"
    qa.bootstrap_store(store, catalog_path)
    cell_id = "intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa"

    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "plan.json").write_text(json.dumps({"cell_id": cell_id, "plan_sha256": "irrelevant"}))
    (bundle / "result.json").write_text(
        json.dumps({"overall": "FAIL", "error": "TypeError: missing 1 required keyword-only argument"})
    )
    (bundle / "runner.log").write_text("boom\n")

    target = qa.seal_mainnet_intent_failure(store=store, catalog_path=catalog_path, bundle=bundle)

    row = json.loads((store / "index" / "intent_latest.json").read_text())[cell_id]
    assert row["status"] == "FAIL"
    assert row["evidence_status"] == "HARNESS_FAIL"
    assert row["failure_reason"] == "TypeError: missing 1 required keyword-only argument"
    summary = json.loads((target / "summary.json").read_text())
    assert summary["failure_reason"] == "TypeError: missing 1 required keyword-only argument"
    assert summary["network"] == "mainnet"
    assert (target / "run.log").is_file(), "runner.log must reach the retraction as run.log"
    assert target.name.endswith("-retraction")
