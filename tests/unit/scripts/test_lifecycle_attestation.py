"""Permanent counterexamples for Quant lifecycle close and guard evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from qa_lab.lifecycle_attestation import validate_lifecycle_attestations

REPO_ROOT = Path(__file__).resolve().parents[3]
QA_COVERAGE_PATH = REPO_ROOT / "qa_lab" / "qa_coverage.py"
TX_ID = "0x" + "ab" * 32


def _close_contract(*, amount_mode: str = "exact_pre_state") -> dict[str, Any]:
    amount_binding: dict[str, str] = {"mode": amount_mode}
    if amount_mode == "full_close_sentinel":
        amount_binding["sentinel_raw"] = str(2**256 - 1)
    return {
        "schema_version": 1,
        "goal": "action-density",
        "feasibility": {
            "status": "feasible",
            "source_files": ["strategies/example/strategy.py"],
            "transition_sequence": ["open -> teardown"],
        },
        "requirements": [
            {
                "id": "close-nft-5763809",
                "phase": "teardown",
                "intent_type": "LP_CLOSE",
                "min_executed": 1,
                "close_obligation": {
                    "position_identity": "eip155:8453/erc721:0x03a520b32c04bf3beef7beeb72e919cf822ed34f1/5763809",
                    "resource_identity": "eip155:8453/uniswap-v3-pool:0x8c7080564b5a792a33ef2fd473fba6364d5495e5",
                    "amount_binding": amount_binding,
                    "maximum_terminal_residual_raw": "0",
                },
            }
        ],
        "teardown": {"required": True, "coverage": "proved"},
    }


def _close_coverage(bundle: Path, *, compiled_raw: str = "4235110629", residual_raw: str = "0") -> dict[str, Any]:
    for name in ("pre-state.json", "decoded-transaction.json", "terminal-state.json"):
        (bundle / name).write_text("{}\n")
    return {
        "schema_version": 1,
        "observations": [
            {
                "requirement_id": "close-nft-5763809",
                "phase": "teardown",
                "intent_type": "LP_CLOSE",
                "executed": 1,
                "transaction_ids": [TX_ID],
                "close_attestation": {
                    "position_identity": "eip155:8453/erc721:0x03a520b32c04bf3beef7beeb72e919cf822ed34f1/5763809",
                    "resource_identity": "eip155:8453/uniswap-v3-pool:0x8c7080564b5a792a33ef2fd473fba6364d5495e5",
                    "pre_state": {
                        "block_number": 34200000,
                        "amount_raw": "4235110629",
                        "evidence_refs": ["pre-state.json"],
                    },
                    "compiled": {
                        "amount_raw": compiled_raw,
                        "source": "decoded_transaction_calldata",
                        "transaction_id": TX_ID,
                        "evidence_refs": ["decoded-transaction.json"],
                    },
                    "terminal": {
                        "block_number": 34200001,
                        "residual_raw": residual_raw,
                        "closed": True,
                        "evidence_refs": ["terminal-state.json"],
                    },
                },
            }
        ],
        "teardown": {"coverage": "proved", "intents_executed": 1},
    }


def _load_qa_coverage() -> Any:
    name = "qa_coverage_lifecycle_attestation_test"
    spec = importlib.util.spec_from_file_location(name, QA_COVERAGE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_alm_3277_rejects_successful_lp_close_with_terminal_position_residual(tmp_path: Path) -> None:
    """ALM-3277: receipt success is not proof that the bound NFT fully closed."""
    contract = _close_contract()
    coverage = _close_coverage(tmp_path, residual_raw="4235110629")

    with pytest.raises(ValueError, match="terminal residual 4235110629 exceeds allowed 0"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


def test_alm_3277_rejects_close_amount_not_bound_to_block_pinned_pre_state(tmp_path: Path) -> None:
    """ALM-3277: decoded decrease amount must match the pinned live position amount."""
    contract = _close_contract()
    coverage = _close_coverage(tmp_path, compiled_raw="1")

    with pytest.raises(ValueError, match="compiled amount does not equal its block-pinned pre-state amount"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


def test_full_close_rejects_different_canonical_position_identity(tmp_path: Path) -> None:
    contract = _close_contract()
    coverage = _close_coverage(tmp_path)
    coverage["observations"][0]["close_attestation"]["position_identity"] += "-different"

    with pytest.raises(ValueError, match="position identity mismatch"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


def test_full_close_positive_control_proves_identity_amount_and_terminal_state(tmp_path: Path) -> None:
    contract = _close_contract()
    coverage = _close_coverage(tmp_path)

    assert validate_lifecycle_attestations(contract, coverage, bundle=tmp_path) == {
        "closes_proved": 1,
        "guards_proved": 0,
    }


def test_full_close_sentinel_positive_control(tmp_path: Path) -> None:
    contract = _close_contract(amount_mode="full_close_sentinel")
    coverage = _close_coverage(tmp_path, compiled_raw=str(2**256 - 1))

    assert validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)["closes_proved"] == 1


def test_alm_3041_rejects_mandatory_production_guard_skipped_on_anvil(tmp_path: Path) -> None:
    """ALM-3041: a local-environment skip cannot attest mainnet deployability."""
    contract = {
        "requirements": [],
        "guards": [{"id": "price-impact", "required_for": "production", "mandatory": True}],
    }
    coverage = {
        "observations": [],
        "guard_attestations": [
            {
                "guard_id": "price-impact",
                "status": "skipped_environment",
                "measured": True,
            }
        ],
    }

    with pytest.raises(ValueError, match="mandatory guard price-impact.*got skipped_environment"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


def test_mandatory_production_guard_executed_pass_positive_control(tmp_path: Path) -> None:
    (tmp_path / "price-impact.json").write_text("{}\n")
    contract = {
        "requirements": [],
        "guards": [{"id": "price-impact", "required_for": "production", "mandatory": True}],
    }
    coverage = {
        "observations": [],
        "guard_attestations": [
            {
                "guard_id": "price-impact",
                "status": "executed_pass",
                "measured": True,
                "evidence_refs": ["price-impact.json"],
            }
        ],
    }

    assert validate_lifecycle_attestations(contract, coverage, bundle=tmp_path) == {
        "closes_proved": 0,
        "guards_proved": 1,
    }


def test_alm_3041_mandatory_production_guard_rejects_unmeasured_executed_pass(tmp_path: Path) -> None:
    """ALM-3041: a producer's unmeasured PASS is not production guard evidence."""
    (tmp_path / "price-impact.json").write_text("{}\n")
    contract = {
        "requirements": [],
        "guards": [{"id": "price-impact", "required_for": "production", "mandatory": True}],
    }
    coverage = {
        "observations": [],
        "guard_attestations": [
            {
                "guard_id": "price-impact",
                "status": "executed_pass",
                "measured": False,
                "evidence_refs": ["price-impact.json"],
            }
        ],
    }

    with pytest.raises(ValueError, match="mandatory guard price-impact to be measured executed_pass"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


def test_quant_sealer_invokes_close_and_guard_attestation_gate(tmp_path: Path) -> None:
    """The pure obligation validator is part of the actual Strategy PASS seal path."""
    qa = _load_qa_coverage()
    contract = _close_contract()
    contract["guards"] = [{"id": "price-impact", "required_for": "production", "mandatory": True}]
    coverage = _close_coverage(tmp_path)
    coverage["guard_attestations"] = [
        {
            "guard_id": "price-impact",
            "status": "skipped_environment",
            "measured": True,
        }
    ]
    (tmp_path / "lifecycle-contract.json").write_text(json.dumps(contract))
    digest = hashlib.sha256((tmp_path / "lifecycle-contract.json").read_bytes()).hexdigest()
    coverage["contract_sha256"] = digest
    (tmp_path / "lifecycle-coverage.json").write_text(json.dumps(coverage))
    (tmp_path / "audit.md").write_text(f"LIFECYCLE_COVERAGE_CONFIRMED: yes\nLIFECYCLE_CONTRACT_SHA256: {digest}\n")

    with pytest.raises(ValueError, match="mandatory guard price-impact.*got skipped_environment"):
        qa._validate_lifecycle_evidence(tmp_path, {"strategy": "PASS"})


def _created_position_case(bundle):
    contract = _close_contract()
    coverage = _close_coverage(bundle)
    obligation = contract["requirements"][0]["close_obligation"]
    obligation.pop("position_identity")
    identity = "eip155:8453/erc721:0x" + "22" * 20 + "/5763809"
    coverage["observations"][0]["close_attestation"]["position_identity"] = identity
    obligation["position_binding"] = {
        "mode": "unique_created_erc721",
        "requirement_id": "open",
        "namespace": identity.rsplit("/", 1)[0],
    }
    contract["requirements"].insert(0, {"id": "open", "phase": "runtime", "intent_type": "LP_OPEN", "min_executed": 1})
    mint_tx = "0x" + "cd" * 32
    (bundle / "mint.json").write_text("{}\n")
    coverage["observations"].insert(
        0,
        {
            "requirement_id": "open",
            "phase": "runtime",
            "intent_type": "LP_OPEN",
            "executed": 1,
            "transaction_ids": [mint_tx],
            "created_positions": [
                {
                    "position_identity": identity,
                    "resource_identity": obligation["resource_identity"],
                    "transaction_id": mint_tx,
                    "block_number": 34199999,
                    "source": "decoded_mint_receipt",
                    "evidence_refs": ["mint.json"],
                }
            ],
        },
    )
    return contract, coverage


def test_close_binds_to_unique_runtime_minted_position(tmp_path):
    contract, coverage = _created_position_case(tmp_path)
    assert validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)["closes_proved"] == 1


@pytest.mark.parametrize(
    "mutation,expected_error",
    [
        ("different_nft", "position identity mismatch"),
        ("wrong_contract", "created position namespace mismatch"),
        ("wrong_resource", "created resource identity mismatch"),
        ("unbound_transaction", "creation is not bound"),
        ("missing_receipt", "does not exist in the evidence bundle"),
        ("two_positions", "exactly one created position"),
        ("missing_positions", "exactly one created position"),
        ("two_actions", "exactly one executed creation action"),
        ("future_mint", "creation postdates"),
        ("self_reference", "earlier creation requirement"),
        ("both_identity_modes", "exactly one fixed identity or creation binding"),
        ("wrong_mode", "has invalid mode"),
        ("noncanonical_token", "canonical non-negative base-unit string"),
        ("unmeasured_source", "decoded mint receipt evidence"),
    ],
)
def test_runtime_mint_binding_rejects_ambiguous_or_unrelated_evidence(tmp_path, mutation, expected_error):
    contract, coverage = _created_position_case(tmp_path)
    obligation = contract["requirements"][1]["close_obligation"]
    creation = coverage["observations"][0]
    position = creation["created_positions"][0]
    if mutation == "different_nft":
        position["position_identity"] = position["position_identity"].rsplit("/", 1)[0] + "/2"
    elif mutation == "wrong_contract":
        obligation["position_binding"]["namespace"] = "eip155:8453/erc721:0x" + "11" * 20
    elif mutation == "wrong_resource":
        position["resource_identity"] = "another-pool"
    elif mutation == "unbound_transaction":
        position["transaction_id"] = TX_ID
    elif mutation == "missing_receipt":
        position["evidence_refs"] = ["missing.json"]
    elif mutation == "two_positions":
        creation["created_positions"].append(dict(position))
    elif mutation == "missing_positions":
        creation["created_positions"] = []
    elif mutation == "two_actions":
        creation["executed"] = 2
    elif mutation == "future_mint":
        position["block_number"] = 34200002
    elif mutation == "self_reference":
        obligation["position_binding"]["requirement_id"] = contract["requirements"][1]["id"]
    elif mutation == "both_identity_modes":
        obligation["position_identity"] = position["position_identity"]
    elif mutation == "wrong_mode":
        obligation["position_binding"]["mode"] = "guess"
    elif mutation == "noncanonical_token":
        position["position_identity"] = position["position_identity"].rsplit("/", 1)[0] + "/01"
    elif mutation == "unmeasured_source":
        position["source"] = "strategy_state"
    with pytest.raises(ValueError, match=expected_error):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


@pytest.mark.parametrize("executed", [True, "1", None])
def test_creation_execution_count_must_be_measured_integer(tmp_path, executed):
    contract, coverage = _created_position_case(tmp_path)
    coverage["observations"][0]["executed"] = executed
    with pytest.raises(ValueError, match="exactly one executed"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


@pytest.mark.parametrize(
    "field,value,expected_error",
    [
        ("namespace", "eip155:1/erc721:0x" + "22" * 20, "namespace and resource chain mismatch"),
        ("requirement_id", "missing", "earlier creation requirement"),
    ],
)
def test_creation_binding_requires_same_chain_and_declared_source(tmp_path, field, value, expected_error):
    contract, coverage = _created_position_case(tmp_path)
    contract["requirements"][1]["close_obligation"]["position_binding"][field] = value
    with pytest.raises(ValueError, match=expected_error):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)


def test_creation_transaction_ids_must_be_an_array(tmp_path):
    contract, coverage = _created_position_case(tmp_path)
    creation = coverage["observations"][0]
    creation["transaction_ids"] = creation["transaction_ids"][0]
    with pytest.raises(ValueError, match="not bound"):
        validate_lifecycle_attestations(contract, coverage, bundle=tmp_path)
