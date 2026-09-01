"""Adversarial controls for exact-pool Uniswap V3 Mainnet evidence."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from scripts.qa.mainnet_intent_envelope import (
    MainnetEnvelopeError,
    _validate_uniswap_guard,
    _validate_uniswap_phase_action,
    _validate_uniswap_terminal,
    artifact_reference,
    validate_mainnet_envelope,
)
from scripts.qa.mainnet_intent_recipe import (
    UNISWAP_V3_ARBITRUM_SWAP_EOA,
    build_approval,
    build_run_plan,
)

RECIPE = UNISWAP_V3_ARBITRUM_SWAP_EOA
WALLET = "0x" + "11" * 20
MASTER = "0x" + "22" * 20


def _identity() -> dict:
    return {
        "chain_id": 42161,
        "block_number": 1,
        "block_hash": "0x" + "ab" * 32,
        "factory": RECIPE.factory_address,
        "pool": RECIPE.resource_address,
        "factory_pool": RECIPE.resource_address,
        "token0": min(RECIPE.asset_address, RECIPE.output_asset_address),
        "token1": max(RECIPE.asset_address, RECIPE.output_asset_address),
        "fee_tier": RECIPE.fee_tier,
        "pool_code_sha256": "cd" * 32,
    }


def _compile_guard() -> dict:
    return {
        "managed_fork": False,
        "max_price_impact": RECIPE.max_price_impact,
        "max_slippage": RECIPE.max_slippage,
        "compile_metadata": {
            "pinned_pool": RECIPE.resource_address,
            "selected_fee_tier": RECIPE.fee_tier,
            "fee_selection_source": "intent_pinned",
            "min_amount_out": "123",
        },
    }


def test_exact_pool_and_production_compile_guards_are_verified() -> None:
    _validate_uniswap_guard(
        guard_id="uniswap_v3_exact_pool_identity",
        observation=_identity(),
        recipe=RECIPE,
        chain_id=42161,
    )
    _validate_uniswap_guard(
        guard_id="uniswap_v3_production_quote_and_min_out",
        observation=_compile_guard(),
        recipe=RECIPE,
        chain_id=42161,
    )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("chain_id",), 8453),
        (("factory_pool",), "0x" + "22" * 20),
        (("token0",), "0x" + "33" * 20),
        (("fee_tier",), 3000),
        (("pool_code_sha256",), "00" * 32),
    ],
)
def test_exact_pool_guard_rejects_every_identity_mismatch(path: tuple[str, ...], value: object) -> None:
    observation = _identity()
    observation[path[0]] = value

    with pytest.raises(MainnetEnvelopeError):
        _validate_uniswap_guard(
            guard_id="uniswap_v3_exact_pool_identity",
            observation=observation,
            recipe=RECIPE,
            chain_id=42161,
        )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("managed_fork",), True),
        (("max_price_impact",), "1"),
        (("compile_metadata", "pinned_pool"), "0x" + "22" * 20),
        (("compile_metadata", "selected_fee_tier"), 3000),
        (("compile_metadata", "fee_selection_source"), "auto"),
        (("compile_metadata", "min_amount_out"), 0),
    ],
)
def test_compile_guard_rejects_skips_route_drift_and_unprotected_output(path: tuple[str, ...], value: object) -> None:
    observation = deepcopy(_compile_guard())
    cursor = observation
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value

    with pytest.raises(MainnetEnvelopeError, match="quote/min-out"):
        _validate_uniswap_guard(
            guard_id="uniswap_v3_production_quote_and_min_out",
            observation=observation,
            recipe=RECIPE,
            chain_id=42161,
        )


def test_terminal_output_token_must_be_zero_at_a_pinned_block() -> None:
    observation = {
        "chain_id": 42161,
        "block_number": 2,
        "block_hash": "0x" + "ef" * 32,
        "wallet": WALLET,
        "asset": RECIPE.output_asset_address,
        "balance_raw": 0,
    }
    _validate_uniswap_terminal(observation=observation, recipe=RECIPE, wallet=WALLET, chain_id=42161)

    observation["balance_raw"] = 1
    with pytest.raises(MainnetEnvelopeError, match="not measured zero"):
        _validate_uniswap_terminal(observation=observation, recipe=RECIPE, wallet=WALLET, chain_id=42161)


def _word(value: int, *, signed: bool = False) -> str:
    if signed and value < 0:
        value += 1 << 256
    return f"{value:064x}"


def _topic(address: str) -> str:
    return "0x" + address.removeprefix("0x").lower().zfill(64)


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def _receipt(*, tx_hash: str, sender: str, logs: list[dict] | None = None) -> dict:
    return {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_phase_receipt",
        "chain_id": 42161,
        "raw_receipt": {
            "tx_hash": tx_hash,
            "block_hash": "0x" + "ab" * 32,
            "block_number": 2,
            "status": 1,
            "from_address": sender,
            "gas_used": 100_000,
            "effective_gas_price": 10_000_000,
            "logs": logs or [],
        },
    }


def _cleanup_logs() -> list[dict]:
    amount_in = 10**14
    amount_out = 300_000
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    return [
        {
            "address": RECIPE.output_asset_address,
            "topics": [transfer_topic, _topic(WALLET), _topic(RECIPE.resource_address)],
            "data": "0x" + _word(amount_in),
            "logIndex": 0,
        },
        {
            "address": RECIPE.resource_address,
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                _topic(WALLET),
                _topic(WALLET),
            ],
            "data": "0x"
            + _word(amount_in, signed=True)
            + _word(-amount_out, signed=True)
            + _word(2**96)
            + _word(1)
            + _word(0, signed=True),
            "logIndex": 1,
        },
        {
            "address": RECIPE.asset_address,
            "topics": [transfer_topic, _topic(RECIPE.resource_address), _topic(WALLET)],
            "data": "0x" + _word(amount_out),
            "logIndex": 2,
        },
    ]


def _target_logs() -> list[dict]:
    amount_in = 500_000
    amount_out = 250_000_000_000_000
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    return [
        {
            "address": RECIPE.asset_address,
            "topics": [transfer_topic, _topic(WALLET), _topic(RECIPE.resource_address)],
            "data": "0x" + _word(amount_in),
            "logIndex": 0,
        },
        {
            "address": RECIPE.resource_address,
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                _topic(WALLET),
                _topic(WALLET),
            ],
            "data": "0x"
            + _word(-amount_out, signed=True)
            + _word(amount_in, signed=True)
            + _word(2**96)
            + _word(1)
            + _word(0, signed=True),
            "logIndex": 1,
        },
        {
            "address": RECIPE.output_asset_address,
            "topics": [transfer_topic, _topic(RECIPE.resource_address), _topic(WALLET)],
            "data": "0x" + _word(amount_out),
            "logIndex": 2,
        },
    ]


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
    fund_receipt = _write(tmp_path / "raw/fund.json", _receipt(tx_hash=hashes["fund"], sender=MASTER))
    target_payload = _receipt(tx_hash=hashes["target"], sender=WALLET, logs=_target_logs())
    target_payload.update(
        {
            "artifact_kind": "almanak.intent_receipt_evidence",
            "intent_cell_id": RECIPE.cell_id,
            "network": "mainnet",
            "chain": RECIPE.chain,
            "protocol": "uniswap_v3",
            "intent": "SWAP",
            "exec_path": RECIPE.exec_path,
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
                "profile": "swap.v1",
                "intent": "SWAP",
                "account": WALLET,
                "asset_address": RECIPE.asset_address,
                "asset_decimals": RECIPE.asset_decimals,
                "output_asset_address": RECIPE.output_asset_address,
                "output_asset_decimals": RECIPE.output_asset_decimals,
                "resource_address": RECIPE.resource_address,
                "factory_address": RECIPE.factory_address,
                "fee_tier": RECIPE.fee_tier,
                "requested_amount_raw": 500_000,
                "wallet_before_raw": 1_000_000,
                "wallet_after_raw": 500_000,
                "output_wallet_before_raw": 0,
                "output_wallet_after_raw": 250_000_000_000_000,
                "parser_amount_raw": 500_000,
                "parser_output_amount_raw": 250_000_000_000_000,
            },
        }
    )
    target_receipt = _write(tmp_path / "raw/target.json", target_payload)
    cleanup_receipt = _write(
        tmp_path / "raw/cleanup.json",
        _receipt(tx_hash=hashes["cleanup"], sender=WALLET, logs=_cleanup_logs()),
    )
    sweep_receipt = _write(tmp_path / "raw/sweep.json", _receipt(tx_hash=hashes["sweep"], sender=WALLET))
    identity = _write(tmp_path / "raw/identity.json", _identity())
    compile_guard = _write(tmp_path / "raw/compile.json", _compile_guard())
    terminal = _write(
        tmp_path / "raw/terminal.json",
        {
            "chain_id": 42161,
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
        {"chain_id": 42161, "block_number": 3, "block_hash": "0x" + "cd" * 32, "wallet": WALLET, "allowances": []},
    )
    anchors = _write(
        tmp_path / "anchors.json",
        {
            "legs": {
                RECIPE.recipe_id: {
                    "funded_txs": {"USDC": hashes["fund"]},
                    "funded_usd": "1.5",
                    "legs": {
                        "USDC": {"usd": "0.5", "price": "1", "native": False},
                        "ETH": {"usd": "1", "price": "2000", "native": True},
                    },
                }
            }
        },
    )
    sweep = _write(
        tmp_path / "sweep.json",
        {
            RECIPE.recipe_id: {
                "sweep": {
                    "pre_usd": "1.4",
                    "post_usd": "0.4",
                    "swept_usd": "1.0",
                    "txs": {"USDC": hashes["sweep"]},
                }
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
        "chain_id": 42161,
        "wallet": WALLET,
        "plan": ref(plan_path),
        "approval": ref(approval_path),
        "phases": {
            "setup": {"obligations": [], "receipts": []},
            "target": {
                "obligations": list(RECIPE.target),
                "receipts": [{"action": RECIPE.target[0], "role": "primary", "artifact": ref(target_receipt)}],
            },
            "cleanup": {
                "obligations": [RECIPE.cleanup[0]],
                "receipts": [{"action": RECIPE.cleanup[0], "role": "primary", "artifact": ref(cleanup_receipt)}],
            },
        },
        "guards": [
            {
                "id": "uniswap_v3_exact_pool_identity",
                "required_for_production": True,
                "measured": True,
                "status": "executed_pass",
                "artifact": ref(identity),
            },
            {
                "id": "uniswap_v3_production_quote_and_min_out",
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
            "target_asset_price_usd": "1",
            "native_price_usd": "2000",
            "funding": ref(anchors),
            "funding_receipts": [ref(fund_receipt)],
            "sweep": ref(sweep),
            "sweep_receipts": [ref(sweep_receipt)],
        },
    }
    return _write(tmp_path / "envelope.json", envelope)


def test_cleanup_requires_authoritative_pool_event_and_bilateral_transfers() -> None:
    amount_in = 10**14
    amount_out = 300_000
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    payload = {
        "raw_receipt": {
            "status": 1,
            "transactionHash": "0x" + "12" * 32,
            "blockNumber": 2,
            "logs": [
                {
                    "address": RECIPE.output_asset_address,
                    "topics": [transfer_topic, _topic(WALLET), _topic(RECIPE.resource_address)],
                    "data": "0x" + _word(amount_in),
                    "logIndex": 0,
                },
                {
                    "address": RECIPE.resource_address,
                    "topics": [
                        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                        _topic(WALLET),
                        _topic(WALLET),
                    ],
                    "data": "0x"
                    + _word(amount_in, signed=True)
                    + _word(-amount_out, signed=True)
                    + _word(2**96)
                    + _word(1)
                    + _word(0, signed=True),
                    "logIndex": 1,
                },
                {
                    "address": RECIPE.asset_address,
                    "topics": [transfer_topic, _topic(RECIPE.resource_address), _topic(WALLET)],
                    "data": "0x" + _word(amount_out),
                    "logIndex": 2,
                },
            ],
        }
    }
    _validate_uniswap_phase_action(
        payload=payload,
        action_spec=RECIPE.cleanup[0],
        recipe=RECIPE,
        wallet=WALLET,
    )

    payload["raw_receipt"]["logs"][1]["address"] = "0x" + "44" * 20
    with pytest.raises(MainnetEnvelopeError, match="authoritative exact-pool"):
        _validate_uniswap_phase_action(
            payload=payload,
            action_spec=RECIPE.cleanup[0],
            recipe=RECIPE,
            wallet=WALLET,
        )


def test_complete_uniswap_mainnet_envelope_is_independently_verified(tmp_path: Path) -> None:
    result = validate_mainnet_envelope(_bundle(tmp_path))

    assert result["status"] == "VERIFIED"
    assert result["target_usd"] == "0.5"
    assert result["guard_ids"] == [
        "uniswap_v3_exact_pool_identity",
        "uniswap_v3_production_quote_and_min_out",
    ]


def test_target_semantic_claim_cannot_disagree_with_raw_bilateral_flow(tmp_path: Path) -> None:
    path = _bundle(tmp_path)
    envelope = json.loads(path.read_text())
    target = envelope["phases"]["target"]["receipts"][0]
    target_path = tmp_path / target["artifact"]["path"]
    payload = json.loads(target_path.read_text())
    payload["semantic_contract"]["parser_output_amount_raw"] += 1
    _write(target_path, payload)
    target["artifact"] = artifact_reference(bundle=tmp_path, path=target_path)
    _write(path, envelope)

    with pytest.raises(MainnetEnvelopeError, match="not independently valid"):
        validate_mainnet_envelope(path)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value["guards"].pop(), "production guard"),
        (lambda value: value["phases"]["cleanup"].update(receipts=[]), "cleanup primary receipts"),
        (lambda value: value["terminal"].pop(0), "Missing terminal obligation"),
        (
            lambda value: value["phases"]["target"]["receipts"][0].update(
                artifact=value["phases"]["cleanup"]["receipts"][0]["artifact"]
            ),
            "counted in more than one phase",
        ),
    ],
)
def test_uniswap_full_envelope_false_green_mutations_are_rejected(tmp_path: Path, mutation, message: str) -> None:
    path = _bundle(tmp_path)
    value = json.loads(path.read_text())
    mutation(value)
    _write(path, value)

    with pytest.raises(MainnetEnvelopeError, match=message):
        validate_mainnet_envelope(path)
