"""Fail-closed controls for synchronous Mainnet Uniswap V3 NFT lifecycles."""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.qa.mainnet_intent_envelope import (
    MainnetEnvelopeError,
    _validate_uniswap_close_components,
    _validate_uniswap_guard,
    _validate_uniswap_phase_action,
    artifact_reference,
    validate_mainnet_envelope,
)
from scripts.qa.mainnet_intent_recipe import (
    UNISWAP_V3_ARBITRUM_LP_CLOSE_EOA,
    UNISWAP_V3_ARBITRUM_LP_OPEN_EOA,
    UNISWAP_V3_BASE_LP_CLOSE_EOA,
    UNISWAP_V3_BASE_LP_OPEN_EOA,
    build_approval,
    build_run_plan,
)
from tests.unit.scripts.test_intent_semantic_contract import (
    ACCOUNT,
    _v3_lp_close_payloads,
    _v3_lp_open_payload,
)

LP_RECIPES = (
    UNISWAP_V3_ARBITRUM_LP_OPEN_EOA,
    UNISWAP_V3_ARBITRUM_LP_CLOSE_EOA,
    UNISWAP_V3_BASE_LP_OPEN_EOA,
    UNISWAP_V3_BASE_LP_CLOSE_EOA,
)


@pytest.mark.parametrize("recipe", LP_RECIPES)
def test_lp_recipe_is_one_exact_dynamic_nft_lifecycle(recipe) -> None:
    assert recipe.exec_path == "eoa"
    assert recipe.semantic_profile == "v3_lp.v1"
    assert recipe.resource_address
    assert recipe.pool_address
    assert recipe.factory_address
    assert recipe.fee_tier == 500
    assert recipe.terminal == ("UNISWAP_V3_NFT_BALANCE_ZERO", "POOL_WALLET_RELEASED")
    if recipe.intent == "LP_OPEN":
        assert recipe.setup == ()
        assert recipe.target[0].startswith("LP_OPEN:")
        assert recipe.cleanup[0] == "LP_CLOSE:SETUP_POSITION:FULL"
    else:
        assert recipe.setup[0].startswith("LP_OPEN:")
        assert recipe.target == ("LP_CLOSE:SETUP_POSITION:FULL",)
        assert recipe.cleanup == ("SWEEP_TO_MASTER",)


def _lp_identity(recipe) -> dict:
    return {
        "chain_id": 42161 if recipe.chain == "arbitrum" else 8453,
        "block_number": 1,
        "block_hash": "0x" + "ab" * 32,
        "factory": recipe.factory_address,
        "pool": recipe.pool_address,
        "factory_pool": recipe.pool_address,
        "token0": min(recipe.asset_address, recipe.output_asset_address),
        "token1": max(recipe.asset_address, recipe.output_asset_address),
        "fee_tier": recipe.fee_tier,
        "pool_code_sha256": "cd" * 32,
        "position_manager": recipe.resource_address,
        "position_manager_code_sha256": "ef" * 32,
    }


def _lp_mint_guard(recipe) -> dict:
    return {
        "managed_fork": False,
        "max_slippage": recipe.max_slippage,
        "compile_metadata": {
            "pool": recipe.pool_address,
            "position_manager": recipe.resource_address,
            "fee_tier": recipe.fee_tier,
            "amount0_min": "1",
            "amount1_min": "1",
        },
    }


@pytest.mark.parametrize("recipe", LP_RECIPES)
def test_lp_identity_and_two_sided_mint_bounds_are_mandatory(recipe) -> None:
    chain_id = 42161 if recipe.chain == "arbitrum" else 8453
    _validate_uniswap_guard(
        guard_id="uniswap_v3_exact_pool_identity",
        observation=_lp_identity(recipe),
        recipe=recipe,
        chain_id=chain_id,
    )
    _validate_uniswap_guard(
        guard_id="uniswap_v3_production_lp_mint_bounds",
        observation=_lp_mint_guard(recipe),
        recipe=recipe,
        chain_id=chain_id,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("position_manager", "0x" + "77" * 20),
        ("position_manager_code_sha256", "00" * 32),
    ],
)
def test_lp_identity_rejects_wrong_or_empty_position_manager(field: str, value: str) -> None:
    recipe = UNISWAP_V3_BASE_LP_OPEN_EOA
    observation = _lp_identity(recipe)
    observation[field] = value
    with pytest.raises(MainnetEnvelopeError, match="position manager"):
        _validate_uniswap_guard(
            guard_id="uniswap_v3_exact_pool_identity",
            observation=observation,
            recipe=recipe,
            chain_id=8453,
        )


@pytest.mark.parametrize("field", ["amount0_min", "amount1_min"])
def test_lp_mint_guard_rejects_a_zero_leg_minimum(field: str) -> None:
    recipe = UNISWAP_V3_BASE_LP_CLOSE_EOA
    observation = _lp_mint_guard(recipe)
    observation["compile_metadata"][field] = "0"
    with pytest.raises(MainnetEnvelopeError, match="mint bounds"):
        _validate_uniswap_guard(
            guard_id="uniswap_v3_production_lp_mint_bounds",
            observation=observation,
            recipe=recipe,
            chain_id=8453,
        )


def test_lp_close_setup_receipt_proves_the_exact_bounded_nft_mint() -> None:
    recipe = UNISWAP_V3_BASE_LP_CLOSE_EOA
    payload = _v3_lp_open_payload()
    _validate_uniswap_phase_action(
        payload=payload,
        action_spec=recipe.setup[0],
        recipe=recipe,
        wallet=ACCOUNT,
    )

    payload = deepcopy(payload)
    payload["raw_receipt"]["logs"][0]["topics"][2] = "0x" + ("77" * 20).zfill(64)
    with pytest.raises(MainnetEnvelopeError, match="NFT mint"):
        _validate_uniswap_phase_action(
            payload=payload,
            action_spec=recipe.setup[0],
            recipe=recipe,
            wallet=ACCOUNT,
        )


def test_lp_open_cleanup_requires_the_complete_close_component_set() -> None:
    recipe = UNISWAP_V3_BASE_LP_OPEN_EOA
    payloads = _v3_lp_close_payloads()
    _validate_uniswap_close_components(
        payloads=payloads,
        recipe=recipe,
        wallet=ACCOUNT,
        expected_position_id=42,
    )

    payloads.pop("burn")
    with pytest.raises(MainnetEnvelopeError, match="decrease/collect/burn"):
        _validate_uniswap_close_components(
            payloads=payloads,
            recipe=recipe,
            wallet=ACCOUNT,
            expected_position_id=42,
        )


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def _mined(payload: dict, *, tx_hash: str, sender: str = ACCOUNT) -> dict:
    result = deepcopy(payload)
    result["chain_id"] = 8453
    raw = result.setdefault("raw_receipt", {})
    raw.update(
        {
            "transactionHash": tx_hash,
            "status": 1,
            "from": sender,
            "gasUsed": 100_000,
            "effectiveGasPrice": 1_000_000,
            "blockNumber": raw.get("blockNumber", 100),
            "blockHash": raw.get("blockHash", "0x" + "ab" * 32),
        }
    )
    return result


def _lp_close_envelope(tmp_path: Path) -> Path:
    recipe = UNISWAP_V3_BASE_LP_CLOSE_EOA
    funding_plan = {
        "schema_version": 1,
        "cell_id": recipe.cell_id,
        "pool_index": 7,
        "wallet": ACCOUNT,
        "funding": {"native": recipe.native_funding, "tokens": list(recipe.funding_tokens)},
        "caps_usd": {
            "trading": recipe.trading_cap_usd,
            "gas": recipe.gas_budget_usd,
            "total_wallet": recipe.total_wallet_cap_usd,
        },
    }
    plan = build_run_plan(recipe=recipe, funding_plan=funding_plan, git_sha="a" * 40)
    approval = build_approval(plan=plan, approver="qa-owner", approved_at="2026-08-16T12:00:00Z")
    plan_path = _write(tmp_path / "plan.json", plan)
    approval_path = _write(tmp_path / "approval.json", approval)

    setup = _mined(_v3_lp_open_payload(), tx_hash="0x" + "44" * 32)
    setup["artifact_kind"] = "almanak.mainnet_intent_phase_receipt"
    setup_path = _write(tmp_path / "raw/setup.json", setup)
    targets: list[tuple[str, Path]] = []
    for role, payload in _v3_lp_close_payloads().items():
        target = _mined(payload, tx_hash=payload["raw_receipt"]["transactionHash"])
        target.update(
            {
                "artifact_kind": "almanak.intent_receipt_evidence",
                "intent_cell_id": recipe.cell_id,
                "network": "mainnet",
                "chain": recipe.chain,
                "exec_path": recipe.exec_path,
                "fidelity": {"hard": True},
            }
        )
        targets.append((role, _write(tmp_path / f"raw/target-{role}.json", target)))

    identity_path = _write(tmp_path / "raw/identity.json", _lp_identity(recipe))
    mint_guard_path = _write(tmp_path / "raw/mint-guard.json", _lp_mint_guard(recipe))
    terminal_path = _write(
        tmp_path / "raw/terminal.json",
        {
            "chain_id": 8453,
            "block_number": 200,
            "block_hash": "0x" + "cd" * 32,
            "wallet": ACCOUNT,
            "collection": recipe.resource_address,
            "balance_raw": 0,
        },
    )
    release_path = _write(
        tmp_path / "raw/release.json",
        {"pool_index": 7, "wallet": ACCOUNT, "funded": False, "swept_at": "2026-08-16T12:01:00Z"},
    )
    fund_hash = "0x" + "55" * 32
    sweep_hash = "0x" + "66" * 32
    fund_receipt_path = _write(
        tmp_path / "raw/fund.json",
        _mined({"artifact_kind": "almanak.mainnet_intent_phase_receipt"}, tx_hash=fund_hash, sender="0x" + "99" * 20),
    )
    sweep_receipt_path = _write(
        tmp_path / "raw/sweep.json",
        _mined({"artifact_kind": "almanak.mainnet_intent_phase_receipt"}, tx_hash=sweep_hash),
    )
    anchors_path = _write(
        tmp_path / "anchors.json",
        {
            "legs": {
                recipe.recipe_id: {
                    "funded_usd": "4.4",
                    "funded_txs": {"fund": fund_hash},
                    "legs": {
                        "ETH": {"native": True, "price": "2000", "usd": "1.4"},
                        "WETH": {"native": False, "price": "2000", "usd": "2"},
                        "USDC": {"native": False, "price": "1", "usd": "1"},
                    },
                }
            }
        },
    )
    sweep_path = _write(
        tmp_path / "sweep.json",
        {
            recipe.recipe_id: {
                "sweep": {"txs": {"sweep": sweep_hash}, "pre_usd": "3", "post_usd": "0", "swept_usd": "3"}
            }
        },
    )
    envelope = {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_envelope",
        "cell_id": recipe.cell_id,
        "git_sha": "a" * 40,
        "recipe_sha256": recipe.digest,
        "chain_id": 8453,
        "wallet": ACCOUNT,
        "plan": artifact_reference(bundle=tmp_path, path=plan_path),
        "approval": artifact_reference(bundle=tmp_path, path=approval_path),
        "phases": {
            "setup": {
                "obligations": list(recipe.setup),
                "receipts": [
                    {
                        "action": recipe.setup[0],
                        "role": "primary",
                        "artifact": artifact_reference(bundle=tmp_path, path=setup_path),
                    }
                ],
            },
            "target": {
                "obligations": list(recipe.target),
                "receipts": [
                    {
                        "action": recipe.target[0],
                        "role": "primary",
                        "receipt_role": role,
                        "artifact": artifact_reference(bundle=tmp_path, path=path),
                    }
                    for role, path in targets
                ],
            },
            "cleanup": {"obligations": [], "receipts": []},
        },
        "guards": [
            {
                "id": "uniswap_v3_exact_pool_identity",
                "required_for_production": True,
                "measured": True,
                "status": "executed_pass",
                "artifact": artifact_reference(bundle=tmp_path, path=identity_path),
            },
            {
                "id": "uniswap_v3_production_lp_mint_bounds",
                "required_for_production": True,
                "measured": True,
                "status": "executed_pass",
                "artifact": artifact_reference(bundle=tmp_path, path=mint_guard_path),
            },
        ],
        "terminal": [
            {"id": "UNISWAP_V3_NFT_BALANCE_ZERO", "artifact": artifact_reference(bundle=tmp_path, path=terminal_path)},
            {"id": "POOL_WALLET_RELEASED", "artifact": artifact_reference(bundle=tmp_path, path=release_path)},
        ],
        "capital": {
            "leg": recipe.recipe_id,
            "target_asset_price_usd": "2000",
            "target_asset_prices_usd": {"WETH": "2000", "USDC": "1"},
            "native_price_usd": "2000",
            "funding": artifact_reference(bundle=tmp_path, path=anchors_path),
            "funding_receipts": [artifact_reference(bundle=tmp_path, path=fund_receipt_path)],
            "sweep": artifact_reference(bundle=tmp_path, path=sweep_path),
            "sweep_receipts": [artifact_reference(bundle=tmp_path, path=sweep_receipt_path)],
        },
    }
    return _write(tmp_path / "envelope.json", envelope)


def test_complete_lp_close_envelope_rederives_setup_target_terminal_and_cap(tmp_path: Path) -> None:
    result = validate_mainnet_envelope(_lp_close_envelope(tmp_path))

    assert result["status"] == "VERIFIED"
    assert result["cell_id"] == UNISWAP_V3_BASE_LP_CLOSE_EOA.cell_id
    assert Decimal(result["target_usd"]) == Decimal("3")


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value["phases"]["target"]["receipts"].pop(), "composite LP_CLOSE"),
        (lambda value: value["phases"]["target"]["receipts"][0].update(receipt_role="burn"), "composite LP_CLOSE"),
        (lambda value: value["terminal"][0].update(id="WRONG"), "Missing terminal obligation"),
    ],
)
def test_lp_close_envelope_cannot_green_with_missing_roles_or_terminal(tmp_path: Path, mutation, message: str) -> None:
    path = _lp_close_envelope(tmp_path)
    value = json.loads(path.read_text())
    mutation(value)
    _write(path, value)

    with pytest.raises(MainnetEnvelopeError, match=message):
        validate_mainnet_envelope(path)
