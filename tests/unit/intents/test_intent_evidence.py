from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.qa.permission_attestation import derive_permission_attestation
from tests.intents.intent_evidence import (
    APPROVAL_TOPIC,
    TRANSFER_TOPIC,
    DisabledIntentEvidenceRecorder,
    IntentEvidenceRecorder,
    build_evidence_manifest,
    decode_explorer_view,
    json_safe,
)


def _address_topic(address: str) -> str:
    return "0x" + address.removeprefix("0x").lower().rjust(64, "0")


def _receipt() -> dict:
    return {
        "tx_hash": "0xabc",
        "block_number": 42,
        "status": 1,
        "logs": [
            {
                "log_index": 3,
                "address": "0x" + "33" * 20,
                "topics": [
                    TRANSFER_TOPIC,
                    _address_topic("0x" + "11" * 20),
                    _address_topic("0x" + "22" * 20),
                ],
                "data": "0x" + (123).to_bytes(32).hex(),
            },
            {
                "log_index": 4,
                "address": "0x" + "44" * 20,
                "topics": [
                    TRANSFER_TOPIC,
                    _address_topic("0x" + "00" * 20),
                    _address_topic("0x" + "22" * 20),
                    "0x" + (987).to_bytes(32).hex(),
                ],
                "data": "0x",
            },
            {"logIndex": 5, "address": "0xdead", "topics": ["0x1234"], "data": "0x"},
            {"logIndex": 6, "address": "0xbeef", "topics": [APPROVAL_TOPIC], "data": "0x01"},
        ],
    }


def _permission_attestation() -> dict:
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
        chain="arbitrum",
    )


class Kind(Enum):
    PASS = "PASS"


@dataclass
class Result:
    success: bool
    amount: Decimal
    missing: None = None


def test_json_safe_preserves_null_and_exact_scalar_types() -> None:
    assert json_safe(Result(True, Decimal("1.2300"))) == {
        "success": True,
        "amount": "1.2300",
        "missing": None,
    }


def test_disabled_recorder_calls_parser_once_without_writes(tmp_path: Path) -> None:
    calls = 0

    def parser(receipt: dict) -> dict:
        nonlocal calls
        calls += 1
        return receipt

    recorder = DisabledIntentEvidenceRecorder()
    result = recorder.capture_parse(
        intent=object(), transaction_result=SimpleNamespace(receipt=_receipt()), parser=parser
    )
    recorder.bind(object())
    recorder.record_fidelity(hard=True)
    recorder.record_balance_deltas(token={"delta": "1"})
    assert calls == 1
    assert result["tx_hash"] == "0xabc"
    assert not list(tmp_path.iterdir())
    assert json_safe({"kind": Kind.PASS, "raw": b"\x01\x02"}) == {
        "kind": "PASS",
        "raw": "0x0102",
    }


def test_explorer_decoder_is_lossless_for_unknown_and_malformed_logs() -> None:
    view = decode_explorer_view(_receipt())
    transfer = view["logs"][0]
    assert transfer["name"] == "Transfer"
    assert transfer["args"] == {
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "value": "123",
    }
    assert transfer["standard"] == "ERC20"
    nft_transfer = view["logs"][1]
    assert nft_transfer["standard"] == "ERC721"
    assert nft_transfer["args"] == {
        "from": "0x" + "00" * 20,
        "to": "0x" + "22" * 20,
        "value": "987",
    }
    assert view["logs"][2]["topic0"] == "0x1234"
    assert view["logs"][2]["name"] is None
    assert "decode_error" in view["logs"][3]
    assert view["logs"][3]["topics"] == [APPROVAL_TOPIC]


def test_recorder_calls_parser_once_and_emits_exact_cell_manifest(tmp_path: Path) -> None:
    intent = SimpleNamespace(protocol="uniswap_v3", chain="arbitrum", intent_type=Kind("PASS"))
    # Use the real marker value after constructing the lightweight object.
    intent.intent_type = SimpleNamespace(value="SWAP")
    transaction = SimpleNamespace(
        receipt=_receipt(),
        tx_hash="0xabc",
        gas_used=99,
        qa_permission_attestation=_permission_attestation(),
    )
    result = Result(True, Decimal("123"))
    calls: list[dict] = []

    def parser(receipt: dict) -> Result:
        calls.append(receipt)
        return result

    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_uniswap_swap.py::TestSwap::test_swap[param]",
        network="anvil",
        exec_path="safe",
        git_sha="abc1234",
        declared_intents={"SWAP"},
        observed_intents=[intent],
        source_provenance={"kind": "sealed-quant-runtime", "run_id": "quant-1"},
    )
    returned = recorder.capture_parse(intent=intent, transaction_result=transaction, parser=parser)
    assert returned is result
    assert len(calls) == 1
    recorder.record_fidelity(
        hard=True,
        flags={"amount_in_eq_transfer": True},
        witnesses=[{"parser": "123", "log": "123"}],
    )
    recorder.record_balance_deltas(checks={"parser_amount_eq_wallet_delta": True}, token_in={"delta": "-123"})
    recorder.finalize(outcome="PASS", duration_seconds=1.25)
    manifest_path = build_evidence_manifest(tmp_path)

    manifest = json.loads(manifest_path.read_text())
    node = manifest["nodes"][0]
    claim = node["intents"][0]
    assert node["nodeid"].endswith("test_swap[param]")
    assert claim["intent_cell_id"] == "intent.uniswap_v3.arbitrum.SWAP.anvil.safe"
    assert len(claim["receipt_artifacts"]) == 1
    artifact = json.loads((tmp_path / claim["receipt_artifacts"][0]).read_text())
    assert artifact["almanak"]["result"]["amount"] == "123"
    assert artifact["fidelity"]["hard"] is True
    assert artifact["layers"] == {
        "compile": "PASS",
        "execute": "PASS",
        "receipt": "PASS",
        "balances": "PASS",
        "permissions": "PASS",
    }
    assert artifact["source_provenance"] == {
        "kind": "sealed-quant-runtime",
        "run_id": "quant-1",
    }


def test_recorder_keeps_fidelity_and_balances_scoped_to_receipt_role(tmp_path: Path) -> None:
    intent = SimpleNamespace(protocol="gmx_v2", chain="arbitrum", intent_type=SimpleNamespace(value="PERP_CLOSE"))
    transaction = SimpleNamespace(receipt=_receipt(), tx_hash="0xabc", gas_used=99)
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_gmx.py::test_close",
        network="anvil",
        exec_path="safe",
        declared_intents={"PERP_CLOSE"},
        observed_intents=[intent],
    )
    recorder.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: receipt,
        receipt_role="submission",
    )
    recorder.capture_parse(
        intent=intent,
        transaction_result=transaction,
        parser=lambda receipt: receipt,
        receipt_role="settlement",
    )
    recorder.record_fidelity(receipt_role="submission", hard=True, flags={"order_created": True})
    recorder.record_fidelity(receipt_role="settlement", hard=True, flags={"position_decreased": True})
    recorder.record_balance_deltas(
        receipt_role="submission", checks={"fee_delta_observed": True}, execution_fee={"delta": "-1"}
    )
    recorder.record_balance_deltas(
        receipt_role="settlement", checks={"collateral_delta_observed": True}, collateral={"delta": "100"}
    )
    recorder.finalize(outcome="PASS", duration_seconds=1.0)

    artifacts = [json.loads(path.read_text()) for path in sorted((tmp_path / "receipts").rglob("*.json"))]
    by_role = {artifact["receipt_role"]: artifact for artifact in artifacts}
    assert by_role["submission"]["fidelity"]["flags"] == {"order_created": True}
    assert by_role["submission"]["balance_deltas"] == {"execution_fee": {"delta": "-1"}}
    assert by_role["settlement"]["fidelity"]["flags"] == {"position_decreased": True}
    assert by_role["settlement"]["balance_deltas"] == {"collateral": {"delta": "100"}}


def test_hard_fidelity_is_derived_and_empty_flags_stay_soft(tmp_path: Path) -> None:
    intent = SimpleNamespace(protocol="gmx_v2", chain="arbitrum", intent_type=SimpleNamespace(value="PERP_OPEN"))
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_gmx.py::test_open",
        network="anvil",
        exec_path="eoa",
        declared_intents={"PERP_OPEN"},
    )
    recorder.capture_parse(
        intent=intent,
        transaction_result=SimpleNamespace(receipt=_receipt()),
        parser=lambda receipt: receipt,
    )
    recorder.record_fidelity(hard=True, flags={})
    recorder.finalize(outcome="PASS", duration_seconds=1.0)

    artifact = json.loads(next((tmp_path / "receipts").rglob("*.json")).read_text())
    assert artifact["fidelity"]["declared_hard"] is True
    assert artifact["fidelity"]["hard"] is False
    assert artifact["layers"]["receipt"] == "SOFT"


def test_parser_failure_cannot_be_upgraded_by_hard_fidelity(tmp_path: Path) -> None:
    intent = SimpleNamespace(protocol="gmx_v2", chain="arbitrum", intent_type=SimpleNamespace(value="PERP_OPEN"))
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_gmx.py::test_open",
        network="anvil",
        exec_path="eoa",
        declared_intents={"PERP_OPEN"},
    )
    with pytest.raises(RuntimeError, match="decode failed"):
        recorder.capture_parse(
            intent=intent,
            transaction_result=SimpleNamespace(receipt=_receipt()),
            parser=lambda _receipt: (_ for _ in ()).throw(RuntimeError("decode failed")),
        )
    recorder.record_fidelity(hard=True, flags={"independent_witness": True})
    recorder.finalize(outcome="FAIL", duration_seconds=1.0)

    artifact = json.loads(next((tmp_path / "receipts").rglob("*.json")).read_text())
    assert artifact["fidelity"]["hard"] is True
    assert artifact["layers"]["receipt"] == "FAIL"


def test_finalize_records_early_failure_without_intent_binding(tmp_path: Path) -> None:
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_gmx.py::test_early_failure",
        network="anvil",
        exec_path="eoa",
    )
    fragment = recorder.finalize(outcome="FAIL", duration_seconds=0.1)
    node = json.loads(fragment.read_text())
    assert node["outcome"] == "FAIL"
    assert node["intents"] == []
    assert "compiled/bound no intents" in node["evidence_error"]


def test_recorder_rejects_invalid_receipt_role(tmp_path: Path) -> None:
    intent = SimpleNamespace(protocol="gmx_v2", chain="arbitrum", intent_type=SimpleNamespace(value="PERP_OPEN"))
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_gmx.py::test_open",
        network="anvil",
        exec_path="safe",
        declared_intents={"PERP_OPEN"},
    )
    with pytest.raises(ValueError, match="receipt_role"):
        recorder.capture_parse(
            intent=intent,
            transaction_result=SimpleNamespace(receipt=_receipt()),
            parser=lambda receipt: receipt,
            receipt_role="Keeper settlement",
        )


def test_recorder_records_and_reraises_same_parser_exception(tmp_path: Path) -> None:
    intent = SimpleNamespace(protocol="aave_v3", chain="arbitrum", intent_type=SimpleNamespace(value="SUPPLY"))
    transaction = SimpleNamespace(receipt=_receipt())
    failure = RuntimeError("decode failed")
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_aave.py::test_supply",
        network="anvil",
        exec_path="eoa",
        declared_intents={"SUPPLY"},
    )

    def parser(_receipt: dict) -> None:
        raise failure

    with pytest.raises(RuntimeError) as caught:
        recorder.capture_parse(intent=intent, transaction_result=transaction, parser=parser)
    assert caught.value is failure
    recorder.finalize(outcome="FAIL", duration_seconds=0.1)
    artifact_path = next((tmp_path / "receipts").rglob("*.json"))
    artifact = json.loads(artifact_path.read_text())
    assert artifact["layers"]["receipt"] == "FAIL"
    assert artifact["almanak"]["error"]["message"] == "decode failed"


def test_recorder_rejects_marker_mismatch(tmp_path: Path) -> None:
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_aave.py::test_supply",
        network="anvil",
        exec_path="safe",
        declared_intents={"BORROW"},
    )
    intent = SimpleNamespace(protocol="aave_v3", chain="arbitrum", intent_type=SimpleNamespace(value="SUPPLY"))
    with pytest.raises(ValueError, match="absent"):
        recorder.bind(intent)


def test_recorder_rejects_unobserved_compiler_claim(tmp_path: Path) -> None:
    claimed = SimpleNamespace(protocol="aave_v3", chain="arbitrum", intent_type=SimpleNamespace(value="SUPPLY"))
    observed = SimpleNamespace(protocol="aave_v3", chain="arbitrum", intent_type=SimpleNamespace(value="BORROW"))
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_aave.py::test_supply",
        network="anvil",
        exec_path="safe",
        declared_intents={"SUPPLY"},
        observed_intents=[observed],
    )
    recorder.bind(claimed, receipt_expected=False)
    with pytest.raises(ValueError, match="bound source requests IntentCompiler.compile did not compile"):
        recorder.finalize(outcome="PASS", duration_seconds=0.1)


def test_recorder_rejects_equivalent_but_uncompiled_source_request(tmp_path: Path) -> None:
    observed = SimpleNamespace(
        protocol="aave_v3",
        chain="arbitrum",
        intent_type=SimpleNamespace(value="SUPPLY"),
        token="0x" + "11" * 20,
        amount=Decimal("1"),
    )
    substituted = SimpleNamespace(**vars(observed))
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_aave.py::test_supply",
        network="anvil",
        exec_path="eoa",
        declared_intents={"SUPPLY"},
        observed_intents=[observed],
    )
    recorder.bind(substituted, receipt_expected=False)

    with pytest.raises(ValueError, match="bound source requests IntentCompiler.compile did not compile"):
        recorder.finalize(outcome="PASS", duration_seconds=0.1)


def test_swap_source_request_binds_both_assets_and_exact_amount(tmp_path: Path) -> None:
    intent = SimpleNamespace(
        protocol="uniswap_v3",
        chain="base",
        intent_type=SimpleNamespace(value="SWAP"),
        from_token="0x" + "11" * 20,
        to_token="0x" + "22" * 20,
        amount=Decimal("1.25"),
    )
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/base/test_uniswap.py::test_swap",
        network="anvil",
        exec_path="eoa",
        declared_intents={"SWAP"},
        observed_intents=[intent],
    )

    recorder.bind(intent, receipt_expected=False)
    fragment = recorder.finalize(outcome="FAIL", duration_seconds=0.1)

    node = json.loads(fragment.read_text())
    assert node["intents"][0]["source_request"] == {
        "schema_version": 1,
        "captured_by": "compiler_observer",
        "intent": "SWAP",
        "asset_reference": intent.from_token,
        "target_asset_reference": intent.to_token,
        "amount": "1.25",
    }
