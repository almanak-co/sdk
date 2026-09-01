"""The sealed mainnet report renders setup / cleanup / sweep context around the target.

The target intent is the only verified claim; the lifecycle section is a rendering of
the same sealed envelope so a human can see at a glance whether every obligation produced
exactly one confirmed primary transaction, the position closed, and the funds came back.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPT_DIR = Path(__file__).resolve().parents[3] / "scripts" / "quant-test"
APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
SUPPLY_TOPIC = "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61"
REPAY_TOPIC = "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051"
WALLET = "0x" + "11" * 20
POOL = "0x" + "22" * 20


@pytest.fixture(scope="module")
def qa():
    spec = importlib.util.spec_from_file_location("qa_coverage_lifecycle", SCRIPT_DIR / "qa_coverage.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["qa_coverage_lifecycle"] = module
    spec.loader.exec_module(module)
    return module


def _write(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True))
    return path.name


def _phase_receipt(
    tx: str, block: int, *, status: int = 1, topics: list[str] | None = None, action: str | None = None
) -> dict[str, Any]:
    return {
        "artifact_kind": "almanak.mainnet_intent_phase_receipt",
        "action": action,
        "raw_receipt": {
            "tx_hash": tx,
            "block_number": block,
            "status": status,
            "gas_used": 100_000,
            "from_address": WALLET,
            "to_address": POOL,
            "logs": [{"address": POOL, "topics": [topic]} for topic in (topics or [])],
        },
    }


def _web3_receipt(block: int) -> dict[str, Any]:
    return {
        "action": None,
        "raw_receipt": {"blockNumber": block, "status": 1, "gasUsed": 21_000, "from": WALLET, "logs": []},
    }


def _run_dir(tmp_path: Path, *, cleanup_status: int = 1, drop_cleanup_primary: bool = False) -> Path:
    run = tmp_path / "run"
    env_root = run / "mainnet-envelope"
    receipts = env_root / "mainnet-envelope-artifacts" / "receipts"

    def ref(name: str) -> dict[str, str]:
        return {"path": f"mainnet-envelope-artifacts/receipts/{name}", "sha256": "0" * 64}

    funding = {**_web3_receipt(10), "action": "FUNDING:USDC"}
    sweep = {**_web3_receipt(60), "action": "SWEEP:USDC"}
    _write(receipts / "funding-usdc.json", funding)
    _write(receipts / "sweep-usdc.json", sweep)
    _write(receipts / "setup-approve.json", _phase_receipt("aa" * 32, 20, topics=[APPROVAL_TOPIC]))
    _write(
        receipts / "setup-supply.json",
        _phase_receipt("bb" * 32, 21, topics=[APPROVAL_TOPIC, SUPPLY_TOPIC], action="SUPPLY:USDC:2"),
    )
    _write(receipts / "setup-toggle.json", _phase_receipt("cc" * 32, 22))
    _write(
        receipts / "target-repay.json", _phase_receipt("dd" * 32, 30, topics=[REPAY_TOPIC], action="REPAY:WETH:0.00005")
    )
    _write(
        receipts / "cleanup-withdraw.json",
        _phase_receipt("ee" * 32, 40, status=cleanup_status, action="WITHDRAW_ALL:USDC"),
    )
    _write(
        env_root / "anchors.json",
        {"legs": {"leg": {"funded_txs": {"USDC": "f1" * 32}, "funded_usd": "3.75", "cap_usd": "6.00"}}},
    )
    _write(
        env_root / "sweep.json",
        {
            "leg": {
                "sweep": {
                    "txs": {"USDC": "f2" * 32},
                    "pre_usd": "3.70",
                    "post_usd": "0.42",
                    "swept_usd": "3.28",
                    "retained": {"ETH": {"balance": "0.0001"}},
                }
            }
        },
    )
    _write(env_root / "terminal-usdc.json", {"block_number": 61, "wallet": WALLET})
    _write(env_root / "verification.json", {"status": "VERIFIED", "gas_usd": "0.0649"})
    cleanup_receipts = (
        []
        if drop_cleanup_primary
        else [{"action": "WITHDRAW_ALL:USDC", "role": "primary", "artifact": ref("cleanup-withdraw.json")}]
    )
    envelope = {
        "wallet": WALLET,
        "capital": {
            "leg": "leg",
            "funding": {"path": "anchors.json", "sha256": "0" * 64},
            "sweep": {"path": "sweep.json", "sha256": "0" * 64},
            "funding_receipts": [ref("funding-usdc.json")],
            "sweep_receipts": [ref("sweep-usdc.json")],
        },
        "guards": [{"id": "aave_reserve_active_unfrozen:USDC", "status": "executed_pass"}],
        "phases": {
            "setup": {
                "obligations": ["SUPPLY:USDC:2"],
                "receipts": [
                    {"action": None, "role": "auxiliary", "artifact": ref("setup-approve.json")},
                    {"action": "SUPPLY:USDC:2", "role": "primary", "artifact": ref("setup-supply.json")},
                    {"action": None, "role": "auxiliary", "artifact": ref("setup-toggle.json")},
                ],
            },
            "target": {
                "obligations": ["REPAY:WETH:0.00005"],
                "receipts": [{"action": "REPAY:WETH:0.00005", "role": "primary", "artifact": ref("target-repay.json")}],
            },
            "cleanup": {"obligations": ["WITHDRAW_ALL:USDC"], "receipts": cleanup_receipts},
        },
        "terminal": [
            {"id": "AAVE_ATOKEN_BALANCE_ZERO:USDC", "artifact": {"path": "terminal-usdc.json", "sha256": "0" * 64}}
        ],
    }
    _write(env_root / "envelope.json", envelope)
    return run


def test_lifecycle_section_renders_every_phase_from_the_sealed_envelope(qa, tmp_path: Path) -> None:
    run = _run_dir(tmp_path)
    page = qa._mainnet_lifecycle_html(run, "mainnet-envelope/envelope.json", "arbitrum")

    assert "Lifecycle context" in page
    assert "every leg confirmed" in page
    assert "Something is off" not in page
    # Funding and sweep hashes come from the anchor/sweep documents, not the receipt.
    assert f"https://arbiscan.io/tx/0x{'f1' * 32}" in page
    assert f"https://arbiscan.io/tx/0x{'f2' * 32}" in page
    # The protocol event outranks the Approval that precedes it in the same receipt.
    assert "SUPPLY:USDC:2" in page and ">Approve<" in page
    # A leg with no events is flagged as such rather than silently labelled.
    assert "no events emitted" in page
    assert "3/3 met" in page
    assert "$3.75 (cap $6.00)" in page and "$3.28 of $3.70" in page
    assert "AAVE_ATOKEN_BALANCE_ZERO:USDC" in page
    assert "claim under test" in page


def test_lifecycle_section_flags_a_reverted_cleanup_leg(qa, tmp_path: Path) -> None:
    run = _run_dir(tmp_path, cleanup_status=0)
    page = qa._mainnet_lifecycle_html(run, "mainnet-envelope/envelope.json", "arbitrum")
    assert "Something is off" in page
    assert "cleanup: WITHDRAW_ALL:USDC did not confirm" in page
    assert "2/3 met" in page
    assert 'class="phase fail"' in page


def test_lifecycle_section_flags_an_obligation_without_a_primary_receipt(qa, tmp_path: Path) -> None:
    run = _run_dir(tmp_path, drop_cleanup_primary=True)
    page = qa._mainnet_lifecycle_html(run, "mainnet-envelope/envelope.json", "arbitrum")
    assert "obligation WITHDRAW_ALL:USDC has no primary receipt" in page
    assert "no receipts referenced" in page


def test_lifecycle_section_orders_legs_by_block_within_a_phase(qa, tmp_path: Path) -> None:
    run = _run_dir(tmp_path)
    page = qa._mainnet_lifecycle_html(run, "mainnet-envelope/envelope.json", "arbitrum")
    assert page.index("aa" * 32) < page.index("bb" * 32) < page.index("cc" * 32)


def test_anvil_cells_render_no_lifecycle_section(qa, tmp_path: Path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    assert qa._mainnet_lifecycle_html(run, "", "arbitrum") == ""
    assert qa._mainnet_lifecycle_html(run, "mainnet-envelope/envelope.json", "arbitrum") == ""
