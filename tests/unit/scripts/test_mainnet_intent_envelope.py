"""Adversarial controls for the live-money Intent envelope."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS
from scripts.qa.mainnet_intent_envelope import (
    MainnetEnvelopeError,
    artifact_reference,
    validate_mainnet_envelope,
)
from scripts.qa.mainnet_intent_recipe import (
    AAVE_V3_ARBITRUM_SUPPLY_EOA,
    build_approval,
    build_run_plan,
)

WALLET = "0x" + "11" * 20
MASTER = "0x" + "22" * 20
BLOCK_HASH = "0x" + "ab" * 32


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def _receipt(*, tx: str, sender: str, block: int, logs: list[dict] | None = None) -> dict:
    return {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_phase_receipt",
        "chain_id": 42161,
        "raw_receipt": {
            "tx_hash": tx,
            "block_hash": BLOCK_HASH,
            "block_number": block,
            "status": 1,
            "from_address": sender,
            "to_address": AAVE_V3_ARBITRUM_SUPPLY_EOA.resource_address,
            "gas_used": 100_000,
            "effective_gas_price": "20000000",
            "logs": logs or [],
        },
    }


def _topic_address(address: str) -> str:
    return "0x" + address.removeprefix("0x").lower().zfill(64)


def _ref(bundle: Path, path: Path) -> dict[str, str]:
    return artifact_reference(bundle=bundle, path=path)


def _bundle(tmp_path: Path) -> Path:
    recipe = AAVE_V3_ARBITRUM_SUPPLY_EOA
    funding = {
        "schema_version": 1,
        "cell_id": recipe.cell_id,
        "pool_index": 7,
        "wallet": WALLET,
        "funding": {"native": "0.0003", "tokens": ["USDC:1"]},
        "caps_usd": {"trading": "1.10", "gas": "1.50", "total_wallet": "4.00"},
    }
    plan = build_run_plan(recipe=recipe, funding_plan=funding, git_sha="a" * 40)
    approval = build_approval(plan=plan, approver="qa-owner", approved_at="2026-08-16T12:00:00Z")
    plan_path = _write(tmp_path / "plan.json", plan)
    approval_path = _write(tmp_path / "approval.json", approval)

    hashes = {name: "0x" + f"{index:064x}" for index, name in enumerate(("fund", "target", "cleanup", "sweep"), 1)}
    funding_receipt = _write(tmp_path / "raw/funding.json", _receipt(tx=hashes["fund"], sender=MASTER, block=10))
    amount = 1_000_000
    supply_log = {
        "address": recipe.resource_address,
        "topics": [
            EVENT_TOPICS["Supply"],
            _topic_address(recipe.asset_address),
            _topic_address(WALLET),
            _topic_address(WALLET),
        ],
        "data": "0x" + f"{0:064x}" + f"{amount:064x}" + f"{0:064x}",
        "logIndex": 0,
    }
    transfer_log = {
        "address": recipe.asset_address,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            _topic_address(WALLET),
            _topic_address(recipe.resource_address),
        ],
        "data": "0x" + f"{amount:064x}",
        "logIndex": 1,
    }
    target_payload = _receipt(tx=hashes["target"], sender=WALLET, block=11, logs=[supply_log, transfer_log])
    target_payload.update(
        {
            "artifact_kind": "almanak.intent_receipt_evidence",
            "intent_cell_id": recipe.cell_id,
            "network": "mainnet",
            "chain": "arbitrum",
            "protocol": "aave_v3",
            "intent": "SUPPLY",
            "exec_path": "eoa",
            "source_request": {
                "schema_version": 1,
                "captured_by": "compiler_observer",
                "intent": "SUPPLY",
                "asset_reference": recipe.asset_address,
                "amount": "1",
            },
            "fidelity": {"hard": True},
            "semantic_contract": {
                "schema_version": 1,
                "profile": "lending.v1",
                "intent": "SUPPLY",
                "account": WALLET,
                "asset_address": recipe.asset_address,
                "asset_decimals": recipe.asset_decimals,
                "resource_address": recipe.resource_address,
                "requested_amount_raw": amount,
                "wallet_before_raw": 2_000_000,
                "wallet_after_raw": 1_000_000,
                "position_before": "0",
                "position_after": "1000000",
                "parser_amount_raw": amount,
            },
        }
    )
    target_receipt = _write(tmp_path / "raw/target.json", target_payload)
    withdraw_log = {
        "address": recipe.resource_address,
        "topics": [
            EVENT_TOPICS["Withdraw"],
            _topic_address(recipe.asset_address),
            _topic_address(WALLET),
            _topic_address(WALLET),
        ],
        "data": "0x" + f"{999999:064x}",
        "logIndex": 1,
    }
    cleanup_receipt = _write(
        tmp_path / "raw/cleanup.json",
        _receipt(tx=hashes["cleanup"], sender=WALLET, block=12, logs=[withdraw_log]),
    )
    sweep_receipt = _write(tmp_path / "raw/sweep.json", _receipt(tx=hashes["sweep"], sender=WALLET, block=13))

    config_words = [6, 8000, 8500, 10500, 1000, 1, 1, 0, 1, 0]
    guard = _write(
        tmp_path / "raw/guard.json",
        {
            "asset": recipe.asset_address,
            "raw_result": "0x" + "".join(f"{word:064x}" for word in config_words),
        },
    )
    terminal = _write(
        tmp_path / "raw/terminal.json",
        {
            "chain_id": 42161,
            "block_number": 12,
            "block_hash": BLOCK_HASH,
            "wallet": WALLET,
            "asset": recipe.asset_address,
            "raw_result": "0x" + "0" * (9 * 64),
        },
    )
    release = _write(tmp_path / "raw/release.json", {"pool_index": 7, "funded": False})
    anchors = _write(
        tmp_path / "anchors.json",
        {
            "legs": {
                "aave-mainnet": {
                    "funded_txs": {"USDC": hashes["fund"]},
                    "funded_usd": "1.5",
                    "legs": {
                        "USDC": {"usd": "1", "price": "1", "native": False},
                        "ETH": {"usd": "0.5", "price": "2000", "native": True},
                    },
                }
            }
        },
    )
    sweep = _write(
        tmp_path / "sweep.json",
        {
            "aave-mainnet": {
                "sweep": {
                    "pre_usd": "1.4",
                    "post_usd": "0.4",
                    "swept_usd": "1.0",
                    "txs": {"ETH": hashes["sweep"]},
                }
            }
        },
    )
    envelope = {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_envelope",
        "cell_id": recipe.cell_id,
        "git_sha": "a" * 40,
        "recipe_sha256": recipe.digest,
        "chain_id": 42161,
        "wallet": WALLET,
        "plan": _ref(tmp_path, plan_path),
        "approval": _ref(tmp_path, approval_path),
        "phases": {
            "setup": {"obligations": [], "receipts": []},
            "target": {
                "obligations": list(recipe.target),
                "receipts": [
                    {"action": recipe.target[0], "role": "primary", "artifact": _ref(tmp_path, target_receipt)}
                ],
            },
            "cleanup": {
                "obligations": ["WITHDRAW_ALL:USDC"],
                "receipts": [
                    {"action": "WITHDRAW_ALL:USDC", "role": "primary", "artifact": _ref(tmp_path, cleanup_receipt)}
                ],
            },
        },
        "guards": [
            {
                "id": "aave_reserve_active_unfrozen:USDC",
                "required_for_production": True,
                "measured": True,
                "status": "executed_pass",
                "artifact": _ref(tmp_path, guard),
            }
        ],
        "terminal": [
            {"id": "AAVE_ATOKEN_BALANCE_ZERO:USDC", "artifact": _ref(tmp_path, terminal)},
            {"id": "POOL_WALLET_RELEASED", "artifact": _ref(tmp_path, release)},
        ],
        "capital": {
            "leg": "aave-mainnet",
            "target_asset_price_usd": "1",
            "native_price_usd": "2000",
            "funding": _ref(tmp_path, anchors),
            "funding_receipts": [_ref(tmp_path, funding_receipt)],
            "sweep": _ref(tmp_path, sweep),
            "sweep_receipts": [_ref(tmp_path, sweep_receipt)],
        },
    }
    return _write(tmp_path / "envelope.json", envelope)


def test_complete_mainnet_envelope_is_independently_verified(tmp_path: Path) -> None:
    result = validate_mainnet_envelope(_bundle(tmp_path))

    assert result["status"] == "VERIFIED"
    assert result["transaction_count"] == 4
    assert result["target_usd"] == "1"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value["phases"]["cleanup"].update(receipts=[]), "cleanup primary receipts"),
        (lambda value: value["guards"][0].update(status="skipped_environment"), "not measured PASS"),
        (lambda value: value["terminal"].pop(0), "Missing terminal obligation"),
        (lambda value: value["capital"].update(target_asset_price_usd="2"), "trading cap"),
        (lambda value: value["capital"].update(sweep_receipts=[]), "Sweep receipts"),
        (lambda value: value["phases"]["target"].update(obligations=[]), "target obligations"),
    ],
)
def test_false_green_mutations_are_rejected(tmp_path: Path, mutation, message: str) -> None:
    path = _bundle(tmp_path)
    value = json.loads(path.read_text())
    mutation(value)
    _write(path, value)

    with pytest.raises(MainnetEnvelopeError, match=message):
        validate_mainnet_envelope(path)


def test_tampered_raw_artifact_is_rejected_before_interpretation(tmp_path: Path) -> None:
    path = _bundle(tmp_path)
    value = json.loads(path.read_text())
    target_path = tmp_path / value["phases"]["target"]["receipts"][0]["artifact"]["path"]
    payload = json.loads(target_path.read_text())
    payload["semantic_contract"]["requested_amount_raw"] = 2_000_000
    _write(target_path, payload)

    with pytest.raises(MainnetEnvelopeError, match="digest mismatch"):
        validate_mainnet_envelope(path)


def test_target_lending_claim_cannot_disagree_with_authoritative_receipt(tmp_path: Path) -> None:
    path = _bundle(tmp_path)
    envelope = json.loads(path.read_text())
    target = envelope["phases"]["target"]["receipts"][0]
    target_path = tmp_path / target["artifact"]["path"]
    payload = json.loads(target_path.read_text())
    payload["semantic_contract"]["parser_amount_raw"] += 1
    _write(target_path, payload)
    target["artifact"] = _ref(tmp_path, target_path)
    _write(path, envelope)

    with pytest.raises(MainnetEnvelopeError, match="not independently valid"):
        validate_mainnet_envelope(path)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda log: log.update(address="0x" + "33" * 20), "authoritative Aave Pool event"),
        (lambda log: log.update(data="0x" + f"{0:064x}"), "account and amount obligation"),
    ],
)
def test_cleanup_labels_cannot_replace_authoritative_event_proof(tmp_path: Path, mutation, message: str) -> None:
    path = _bundle(tmp_path)
    envelope = json.loads(path.read_text())
    reference = envelope["phases"]["cleanup"]["receipts"][0]["artifact"]
    receipt_path = tmp_path / reference["path"]
    payload = json.loads(receipt_path.read_text())
    mutation(payload["raw_receipt"]["logs"][0])
    _write(receipt_path, payload)
    envelope["phases"]["cleanup"]["receipts"][0]["artifact"] = _ref(tmp_path, receipt_path)
    _write(path, envelope)

    with pytest.raises(MainnetEnvelopeError, match=message):
        validate_mainnet_envelope(path)


@pytest.mark.parametrize("value", [1.5, 0.5, 2.7])
def test_qa_coverage_admission_paths_refuse_a_non_integral_status(value: float) -> None:
    """Both official-admission paths in qa_coverage must refuse a fractional status.

    `_validate_successful_terminal_receipt` admits official PASS and
    `_receipt_status` admits official FAIL. They carried the same coercion
    twice, so fixing one and missing the other is precisely how this rule
    survived several review rounds; they share `_integral_receipt_status` now.
    """
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("_qa_cov_probe", "scripts/quant-test/qa_coverage.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["_qa_cov_probe"] = module
    spec.loader.exec_module(module)

    with pytest.raises(ValueError, match="integral"):
        module._integral_receipt_status(value)
    # The FAIL-admission caller must translate it into a refusal, not admit it.
    with pytest.raises(ValueError, match="terminal receipt status"):
        module._receipt_status({"status": value}, tx_id="0xdead")
    # The PASS-admission caller must not treat it as a successful receipt.
    with pytest.raises(ValueError):
        module._validate_successful_terminal_receipt("0xdead", {"status": value})

    # Liveness: real statuses still admit.
    assert module._integral_receipt_status(1) == 1
    assert module._integral_receipt_status("0x1") == 1
    assert module._receipt_status({"status": 0}, tx_id="0xdead") == 0


@pytest.mark.parametrize("value", [1.5, 0.5, 2.7])
def test_non_integral_receipt_status_is_refused(value: float) -> None:
    """int(1.5) == 1, so a fractional status must never reach a `== 1` check.

    This rule has several entrypoints across the QA evidence validators, and it
    was fixed at one site at a time across three review rounds. Pin it here for
    every validator that coerces an integer evidence field on a verdict path,
    so the next one cannot be missed silently.
    """
    from scripts.qa.mainnet_intent_envelope import MainnetEnvelopeError
    from scripts.qa.mainnet_intent_envelope import _quantity as envelope_quantity
    from scripts.qa.quant_books import _quantity as books_quantity

    with pytest.raises(MainnetEnvelopeError, match="must be an integer quantity"):
        envelope_quantity(value, label="raw receipt status")
    with pytest.raises(ValueError, match="non-integral quantity"):
        books_quantity(value, source="raw receipt")


def test_integral_receipt_status_still_passes_every_validator() -> None:
    """Liveness: the refusal above is not simply refusing everything."""
    from scripts.qa.mainnet_intent_envelope import _quantity as envelope_quantity
    from scripts.qa.quant_books import _quantity as books_quantity

    for accepted in (1, 1.0, "0x1", "1"):
        assert envelope_quantity(accepted, label="raw receipt status") == 1
        assert books_quantity(accepted, source="raw receipt") == 1
