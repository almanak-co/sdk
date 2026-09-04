"""Adversarial controls for exact-pair Trader Joe Mainnet evidence."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2
from qa_lab.mainnet_intent_envelope import (
    MainnetEnvelopeError,
    _validate_traderjoe_guard,
    _validate_traderjoe_phase_action,
    artifact_reference,
    validate_mainnet_envelope,
)
from qa_lab.mainnet_intent_recipe import TRADERJOE_V2_AVALANCHE_SWAP_EOA, build_approval, build_run_plan

RECIPE = TRADERJOE_V2_AVALANCHE_SWAP_EOA
WALLET = "0x" + "11" * 20
MASTER = "0x" + "22" * 20
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _word(value: int) -> str:
    return f"{value:064x}"


def _topic(address: str) -> str:
    return "0x" + address.removeprefix("0x").lower().zfill(64)


def _factory_raw() -> str:
    return "0x" + _word(RECIPE.fee_tier) + RECIPE.resource_address.removeprefix("0x").lower().zfill(64) + _word(0) * 2


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def _receipt(
    *,
    tx_hash: str,
    sender: str,
    token_in: str | None = None,
    token_out: str | None = None,
    amount_in: int = 0,
    amount_out: int = 0,
) -> dict:
    logs = []
    if token_in and token_out:
        logs = [
            {
                "address": token_in,
                "topics": [TRANSFER_TOPIC, _topic(WALLET), _topic(RECIPE.resource_address)],
                "data": "0x" + _word(amount_in),
                "logIndex": 0,
            },
            {
                "address": token_out,
                "topics": [TRANSFER_TOPIC, _topic(RECIPE.resource_address), _topic(WALLET)],
                "data": "0x" + _word(amount_out),
                "logIndex": 1,
            },
        ]
    return {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_phase_receipt",
        "chain_id": 43114,
        "raw_receipt": {
            "tx_hash": tx_hash,
            "block_hash": "0x" + "ab" * 32,
            "block_number": 2,
            "status": 1,
            "from_address": sender,
            "to_address": TRADERJOE_V2["avalanche"]["router"],
            "gas_used": 100_000,
            "effective_gas_price": 10_000_000,
            "logs": logs,
        },
    }


def _identity() -> dict:
    return {
        "chain_id": 43114,
        "block_number": 2,
        "block_hash": "0x" + "ab" * 32,
        "factory": RECIPE.factory_address,
        "pair": RECIPE.resource_address,
        "expected_pair": RECIPE.resource_address,
        "token_x": RECIPE.asset_address,
        "token_y": RECIPE.output_asset_address,
        "bin_step": RECIPE.fee_tier,
        "raw_result": _factory_raw(),
        "pair_code_sha256": "cd" * 32,
    }


def _compile_guard() -> dict:
    return {
        "managed_fork": False,
        "max_price_impact": RECIPE.max_price_impact,
        "max_slippage": RECIPE.max_slippage,
        "compile_metadata": {
            "router": TRADERJOE_V2["avalanche"]["router"],
            "bin_step": RECIPE.fee_tier,
            "amount_out_min_wei": "61000",
            "oracle_expected_wei": "63000",
            "quoter_amount_wei": "62500",
        },
    }


def _bundle(tmp_path: Path) -> Path:
    funding = {
        "schema_version": 1,
        "cell_id": RECIPE.cell_id,
        "pool_index": 7,
        "wallet": WALLET,
        "funding": {"native": RECIPE.native_funding, "tokens": list(RECIPE.funding_tokens)},
        "caps_usd": {
            "trading": RECIPE.trading_cap_usd,
            "gas": RECIPE.gas_budget_usd,
            "total_wallet": RECIPE.total_wallet_cap_usd,
        },
    }
    plan = build_run_plan(recipe=RECIPE, funding_plan=funding, git_sha="a" * 40)
    approval = build_approval(plan=plan, approver="qa-owner", approved_at="2026-08-16T12:00:00Z")
    plan_path = _write(tmp_path / "plan.json", plan)
    approval_path = _write(tmp_path / "approval.json", approval)
    hashes = {name: "0x" + f"{index:064x}" for index, name in enumerate(("fund", "target", "cleanup", "sweep"), 1)}
    fund = _write(tmp_path / "raw/fund.json", _receipt(tx_hash=hashes["fund"], sender=MASTER))
    target_payload = _receipt(
        tx_hash=hashes["target"],
        sender=WALLET,
        token_in=RECIPE.asset_address,
        token_out=RECIPE.output_asset_address,
        amount_in=10**16,
        amount_out=62_000,
    )
    target_payload.update(
        {
            "artifact_kind": "almanak.intent_receipt_evidence",
            "intent_cell_id": RECIPE.cell_id,
            "network": "mainnet",
            "chain": RECIPE.chain,
            "protocol": RECIPE.protocol,
            "intent": RECIPE.intent,
            "exec_path": RECIPE.exec_path,
            "tx": {"to": TRADERJOE_V2["avalanche"]["router"]},
            "source_request": {
                "schema_version": 1,
                "captured_by": "compiler_observer",
                "intent": "SWAP",
                "asset_reference": RECIPE.asset_address,
                "target_asset_reference": RECIPE.output_asset_address,
                "amount": RECIPE.target_amount,
            },
            "fidelity": {"hard": True},
            "semantic_contract": {
                "schema_version": 1,
                "profile": RECIPE.semantic_profile,
                "intent": "SWAP",
                "account": WALLET,
                "asset_address": RECIPE.asset_address,
                "asset_decimals": RECIPE.asset_decimals,
                "output_asset_address": RECIPE.output_asset_address,
                "output_asset_decimals": RECIPE.output_asset_decimals,
                "resource_address": RECIPE.resource_address,
                "factory_address": RECIPE.factory_address,
                "router_address": TRADERJOE_V2["avalanche"]["router"],
                "bin_step": RECIPE.fee_tier,
                "requested_amount_raw": 10**16,
                "wallet_before_raw": 10**16,
                "wallet_after_raw": 0,
                "output_wallet_before_raw": 0,
                "output_wallet_after_raw": 62_000,
                "parser_amount_raw": 10**16,
                "parser_output_amount_raw": 62_000,
                "factory_witness": {
                    "block_number": 2,
                    "block_hash": "0x" + "ab" * 32,
                    "to": RECIPE.factory_address,
                    "calldata": (
                        "0x704037bd"
                        + RECIPE.asset_address.removeprefix("0x").lower().zfill(64)
                        + RECIPE.output_asset_address.removeprefix("0x").lower().zfill(64)
                        + _word(RECIPE.fee_tier)
                    ),
                    "raw_result": _factory_raw(),
                },
            },
        }
    )
    target = _write(tmp_path / "raw/target.json", target_payload)
    cleanup = _write(
        tmp_path / "raw/cleanup.json",
        _receipt(
            tx_hash=hashes["cleanup"],
            sender=WALLET,
            token_in=RECIPE.output_asset_address,
            token_out=RECIPE.asset_address,
            amount_in=62_000,
            amount_out=9_800_000_000_000_000,
        ),
    )
    sweep_receipt = _write(tmp_path / "raw/sweep.json", _receipt(tx_hash=hashes["sweep"], sender=WALLET))
    identity = _write(tmp_path / "raw/identity.json", _identity())
    compile_guard = _write(tmp_path / "raw/compile.json", _compile_guard())
    terminal = _write(
        tmp_path / "raw/terminal.json",
        {
            "chain_id": 43114,
            "block_number": 3,
            "block_hash": "0x" + "cd" * 32,
            "wallet": WALLET,
            "asset": RECIPE.output_asset_address,
            "balance_raw": 0,
        },
    )
    release = _write(tmp_path / "raw/release.json", {"pool_index": 7, "funded": False})
    allowances = _write(
        tmp_path / "raw/allowances.json",
        {"chain_id": 43114, "block_number": 3, "block_hash": "0x" + "cd" * 32, "wallet": WALLET, "allowances": []},
    )
    anchors = _write(
        tmp_path / "anchors.json",
        {
            "legs": {
                RECIPE.recipe_id: {
                    "funded_txs": {"WAVAX": hashes["fund"]},
                    "funded_usd": "0.09",
                    "legs": {
                        "WAVAX": {"usd": "0.06", "price": "6", "native": False},
                        "AVAX": {"usd": "0.03", "price": "6", "native": True},
                    },
                }
            }
        },
    )
    sweep = _write(
        tmp_path / "sweep.json",
        {
            RECIPE.recipe_id: {
                "sweep": {"pre_usd": "0.08", "post_usd": "0.01", "swept_usd": "0.07", "txs": {"WAVAX": hashes["sweep"]}}
            }
        },
    )

    def ref(path: Path) -> dict[str, str]:
        return artifact_reference(bundle=tmp_path, path=path)

    envelope = {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_envelope",
        "cell_id": RECIPE.cell_id,
        "git_sha": "a" * 40,
        "recipe_sha256": RECIPE.digest,
        "chain_id": 43114,
        "wallet": WALLET,
        "plan": ref(plan_path),
        "approval": ref(approval_path),
        "phases": {
            "setup": {"obligations": [], "receipts": []},
            "target": {
                "obligations": list(RECIPE.target),
                "receipts": [{"action": RECIPE.target[0], "role": "primary", "artifact": ref(target)}],
            },
            "cleanup": {
                "obligations": [RECIPE.cleanup[0]],
                "receipts": [{"action": RECIPE.cleanup[0], "role": "primary", "artifact": ref(cleanup)}],
            },
        },
        "guards": [
            {
                "id": "traderjoe_v2_exact_pair_identity",
                "required_for_production": True,
                "measured": True,
                "status": "executed_pass",
                "artifact": ref(identity),
            },
            {
                "id": "traderjoe_v2_production_quote_and_min_out",
                "required_for_production": True,
                "measured": True,
                "status": "executed_pass",
                "artifact": ref(compile_guard),
            },
        ],
        "terminal": [
            {"id": RECIPE.terminal[0], "artifact": ref(terminal)},
            {"id": "NO_RESIDUAL_ALLOWANCES", "artifact": ref(allowances)},
            {"id": "POOL_WALLET_RELEASED", "artifact": ref(release)},
        ],
        "capital": {
            "leg": RECIPE.recipe_id,
            "target_asset_price_usd": "6",
            "native_price_usd": "6",
            "funding": ref(anchors),
            "funding_receipts": [ref(fund)],
            "sweep": ref(sweep),
            "sweep_receipts": [ref(sweep_receipt)],
        },
    }
    return _write(tmp_path / "envelope.json", envelope)


def test_complete_traderjoe_mainnet_envelope_is_independently_verified(tmp_path: Path) -> None:
    result = validate_mainnet_envelope(_bundle(tmp_path))

    assert result["status"] == "VERIFIED"
    assert result["target_usd"] == "0.06"
    assert result["guard_ids"] == [
        "traderjoe_v2_exact_pair_identity",
        "traderjoe_v2_production_quote_and_min_out",
    ]


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("pair",), "0x" + "33" * 20),
        (("token_y",), "0x" + "44" * 20),
        (("bin_step",), 25),
        (("pair_code_sha256",), "00" * 32),
    ],
)
def test_exact_pair_guard_rejects_identity_drift(path: tuple[str, ...], value: object) -> None:
    observation = _identity()
    observation[path[0]] = value
    with pytest.raises(MainnetEnvelopeError, match="exact pair"):
        _validate_traderjoe_guard(
            guard_id="traderjoe_v2_exact_pair_identity", observation=observation, recipe=RECIPE, chain_id=43114
        )


def test_cleanup_rejects_wrong_router_and_partial_target_output(tmp_path: Path) -> None:
    path = _bundle(tmp_path)
    envelope = json.loads(path.read_text())
    cleanup_ref = envelope["phases"]["cleanup"]["receipts"][0]["artifact"]
    cleanup_path = tmp_path / cleanup_ref["path"]
    cleanup = json.loads(cleanup_path.read_text())
    wrong_router = deepcopy(cleanup)
    wrong_router["raw_receipt"]["to_address"] = "0x" + "55" * 20
    with pytest.raises(MainnetEnvelopeError, match="canonical router"):
        _validate_traderjoe_phase_action(
            payload=wrong_router, action_spec=RECIPE.cleanup[0], recipe=RECIPE, wallet=WALLET
        )

    cleanup["raw_receipt"]["logs"][0]["data"] = "0x" + _word(61_999)
    _write(cleanup_path, cleanup)
    envelope["phases"]["cleanup"]["receipts"][0]["artifact"] = artifact_reference(bundle=tmp_path, path=cleanup_path)
    _write(path, envelope)
    with pytest.raises(MainnetEnvelopeError, match="entire measured target output"):
        validate_mainnet_envelope(path)
