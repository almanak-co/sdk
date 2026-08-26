"""Fail-closed controls for synchronous Mainnet Intent recipes."""

from __future__ import annotations

import importlib.util
import json
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.sdk import compute_pool_address
from scripts.qa.mainnet_intent_recipe import (
    AAVE_V3_ARBITRUM_SUPPLY_EOA,
    MAINNET_ASSET_DECIMALS,
    TRADERJOE_V2_AVALANCHE_SWAP_EOA,
    UNISWAP_V3_ARBITRUM_SWAP_EOA,
    UNISWAP_V3_BASE_SWAP_EOA,
    asset_decimals,
    build_approval,
    build_run_plan,
    resolve_recipe,
    verify_approval,
    verify_run_plan,
)
from scripts.qa.operator_gateway import OperatorGatewayClient

REPO = Path(__file__).resolve().parents[3]


def _runner_module():
    path = REPO / "scripts" / "quant-test" / "run_mainnet_intent.py"
    spec = importlib.util.spec_from_file_location("run_mainnet_intent", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _funding() -> dict:
    return {
        "schema_version": 1,
        "cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id,
        "pool_index": 7,
        "wallet": "0x" + "11" * 20,
        "funding": {"native": "0.0003", "tokens": ["USDC:1"]},
        "caps_usd": {"trading": "1.10", "gas": "1.50", "total_wallet": "4.00"},
    }


def _plan() -> dict:
    return build_run_plan(recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA, funding_plan=_funding(), git_sha="a" * 40)


def test_aave_arbitrum_supply_recipe_has_no_setup_and_mandatory_cleanup() -> None:
    recipe = resolve_recipe("intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa")

    assert recipe.target == ("SUPPLY:USDC:1",)
    assert "WITHDRAW_ALL:USDC" in recipe.cleanup
    assert recipe.setup == ()


@pytest.mark.parametrize("intent", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
def test_aave_arbitrum_eoa_has_one_exact_live_recipe(intent: str) -> None:
    recipe = resolve_recipe(f"intent.aave_v3.arbitrum.{intent}.mainnet.eoa")

    assert recipe.target == (f"{intent}:{recipe.asset_symbol}:{recipe.target_amount}",)
    assert recipe.nodeid.endswith("_eoa")
    assert "SWEEP_TO_MASTER" in recipe.cleanup
    assert "POOL_WALLET_RELEASED" in recipe.terminal


@pytest.mark.parametrize("chain", ["arbitrum", "base"])
@pytest.mark.parametrize("intent", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
def test_aave_live_recipe_axes_and_dependencies_are_exact(chain: str, intent: str) -> None:
    recipe = resolve_recipe(f"intent.aave_v3.{chain}.{intent}.mainnet.eoa")

    assert recipe.chain == chain
    assert recipe.intent == intent
    assert recipe.exec_path == "eoa"
    assert recipe.semantic_profile == "lending.v1"
    assert recipe.target == (f"{intent}:{recipe.asset_symbol}:{recipe.target_amount}",)
    assert "SWEEP_TO_MASTER" in recipe.cleanup


def test_aave_dependency_recipes_separate_setup_target_and_cleanup() -> None:
    borrow = resolve_recipe("intent.aave_v3.arbitrum.BORROW.mainnet.eoa")
    repay = resolve_recipe("intent.aave_v3.arbitrum.REPAY.mainnet.eoa")

    assert borrow.setup == ("SUPPLY:USDC:2",)
    assert borrow.target == ("BORROW:WETH:0.0001",)
    assert borrow.cleanup == ("REPAY_ALL:WETH", "WITHDRAW_ALL:USDC", "SWEEP_TO_MASTER")
    assert repay.setup == ("SUPPLY:USDC:2", "BORROW:WETH:0.0001")
    assert repay.target == ("REPAY:WETH:0.00005",)
    assert set(repay.setup).isdisjoint(repay.target)


@pytest.mark.parametrize(
    "recipe",
    [UNISWAP_V3_ARBITRUM_SWAP_EOA, UNISWAP_V3_BASE_SWAP_EOA],
)
def test_uniswap_swap_recipe_is_exact_pool_and_inverse_cleanup(recipe) -> None:
    assert recipe.semantic_profile == "swap.v1"
    assert recipe.target == (f"SWAP:USDC:WETH:0.5:{recipe.fee_tier}:{recipe.resource_address}",)
    assert recipe.cleanup == (
        f"SWAP_ALL:WETH:USDC:ALL:{recipe.fee_tier}:{recipe.resource_address}",
        "SWEEP_TO_MASTER",
    )
    assert recipe.terminal == ("TOKEN_BALANCE_ZERO:WETH", "POOL_WALLET_RELEASED")
    assert recipe.resource_address == compute_pool_address(
        recipe.factory_address,
        recipe.asset_address,
        recipe.output_asset_address,
        recipe.fee_tier,
    )


def test_traderjoe_swap_recipe_is_exact_pair_and_inverse_cleanup() -> None:
    recipe = TRADERJOE_V2_AVALANCHE_SWAP_EOA

    assert recipe.semantic_profile == "liquidity_book_swap.v1"
    assert recipe.target == (f"SWAP:WAVAX:USDT:0.01:20:{recipe.resource_address}",)
    assert recipe.cleanup == (f"SWAP_ALL:USDT:WAVAX:ALL:20:{recipe.resource_address}", "SWEEP_TO_MASTER")
    assert recipe.terminal == ("TOKEN_BALANCE_ZERO:USDT", "POOL_WALLET_RELEASED")


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("intent", ["PERP_OPEN", "PERP_CLOSE"])
def test_gmx_mainnet_recipe_requires_one_full_keeper_settled_lifecycle(chain: str, intent: str) -> None:
    recipe = resolve_recipe(f"intent.gmx_v2.{chain}.{intent}.mainnet.eoa")

    assert recipe.semantic_profile == "async_perp.v1"
    assert recipe.quant_cell_id == f"perp.gmx_v2.{chain}.simple.mainnet.eoa"
    assert recipe.required_lifecycle == ("PERP_OPEN", "PERP_CLOSE")
    assert recipe.terminal == ("NO_OPEN_GMX_POSITIONS", "NO_PENDING_GMX_ORDERS")
    assert recipe.source == "scripts/quant-test/intent_mainnet.py"


def test_gmx_does_not_fabricate_unsupported_base_or_unproved_cancel_routes() -> None:
    with pytest.raises(ValueError, match="No Mainnet Intent recipe"):
        resolve_recipe("intent.gmx_v2.base.PERP_OPEN.mainnet.eoa")
    with pytest.raises(ValueError, match="No Mainnet Intent recipe"):
        resolve_recipe("intent.gmx_v2.arbitrum.PERP_CANCEL_ORDER.mainnet.eoa")


def test_plan_and_approval_bind_recipe_wallet_funding_and_sha() -> None:
    plan = _plan()
    approval = build_approval(plan=plan, approver="qa-owner", approved_at="2026-08-16T12:00:00Z")

    assert verify_run_plan(plan) == AAVE_V3_ARBITRUM_SUPPLY_EOA
    verify_approval(plan=plan, approval=approval)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("git_sha",), "b" * 40, "digest"),
        (("wallet",), "0x" + "22" * 20, "digest"),
        (("funding", "funding", "tokens"), ["USDC:2"], "digest"),
        (("recipe", "target_amount"), "2", "digest"),
    ],
)
def test_any_plan_mutation_invalidates_approval_surface(path: tuple[str, ...], value, message: str) -> None:
    plan = deepcopy(_plan())
    cursor = plan
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value

    with pytest.raises(ValueError, match=message):
        verify_run_plan(plan)


def test_approval_for_another_plan_is_rejected() -> None:
    plan = _plan()
    approval = build_approval(plan=plan, approver="qa-owner", approved_at="2026-08-16T12:00:00Z")
    approval["plan_sha256"] = "b" * 64

    with pytest.raises(ValueError, match="does not bind"):
        verify_approval(plan=plan, approval=approval)


def test_mainnet_runner_emits_pytest_canonical_junit_identity(tmp_path: Path) -> None:
    runner = _runner_module()
    junit = tmp_path / "results.xml"

    runner._write_junit(
        junit,
        nodeid=AAVE_V3_ARBITRUM_SUPPLY_EOA.nodeid,
        outcome="PASS",
        duration=1.25,
        error=None,
    )

    case = ET.parse(junit).getroot().find("testcase")
    assert case is not None
    assert case.get("file") == "tests/intents/arbitrum/test_aave_v3_lending.py"
    assert case.get("classname") == ("tests.intents.arbitrum.test_aave_v3_lending.TestAaveV3SupplyIntent")
    assert case.get("name") == "test_supply_usdc_using_intent_eoa"


def test_operator_gateway_read_seam_is_fail_closed_and_rpc_compatible() -> None:
    target = "0x" + "ab" * 20

    class Eth:
        @staticmethod
        def call(call, block_identifier):
            assert call == {"to": Web3.to_checksum_address(target), "data": "0x1234"}
            assert block_identifier == "latest"
            return bytes.fromhex("01")

    class Provider:
        @staticmethod
        def make_request(method, params):
            if method == "eth_call":
                assert params == [{"to": Web3.to_checksum_address(target), "data": "0x1234"}, "latest"]
                result = "0x01"
            else:
                assert method == "eth_getTransactionCount"
                assert params == [target, "latest"]
                result = "0x7"
            return {"jsonrpc": "2.0", "id": 1, "result": result}

    client = OperatorGatewayClient(type("Web3Stub", (), {"eth": Eth(), "provider": Provider()})(), "arbitrum")
    good = client.Call(
        type(
            "Request",
            (),
            {
                "chain": "arbitrum",
                "method": "eth_call",
                "params": json.dumps([{"to": Web3.to_checksum_address(target), "data": "0x1234"}, "latest"]),
            },
        )()
    )
    wrong_chain = client.Call(type("Request", (), {"chain": "base", "method": "eth_call", "params": "[]"})())

    assert good.success is True
    assert json.loads(good.result) == "0x01"
    nonce = client.Call(
        type(
            "Request",
            (),
            {"chain": "arbitrum", "method": "eth_getTransactionCount", "params": json.dumps([target, "latest"])},
        )()
    )
    assert nonce.success is True
    assert json.loads(nonce.result) == "0x7"
    assert wrong_chain.success is False
    assert client.eth_call(chain="arbitrum", to=target, data="0x1234") == "0x01"
    assert client.eth_call(chain="base", to=target, data="0x1234") is None


@pytest.mark.parametrize(
    "method",
    [
        "eth_estimateGas",
        "eth_feeHistory",
        "eth_gasPrice",
        "eth_getBlockByNumber",
        "eth_maxPriorityFeePerGas",
    ],
)
def test_operator_gateway_allows_transaction_construction_reads(method: str) -> None:
    class Provider:
        @staticmethod
        def make_request(actual_method, params):
            assert actual_method == method
            assert params == []
            return {"jsonrpc": "2.0", "id": 1, "result": "0x7"}

    client = OperatorGatewayClient(type("Web3Stub", (), {"provider": Provider()})(), "avalanche")
    response = client.Call(type("Request", (), {"chain": "avalanche", "method": method, "params": "[]"})())

    assert response.success is True
    assert json.loads(response.result) == "0x7"


@pytest.mark.parametrize(
    "method",
    ["eth_sendRawTransaction", "eth_sendTransaction", "personal_sign", "eth_signTransaction"],
)
def test_operator_gateway_rejects_signing_and_submission_methods(method: str) -> None:
    client = OperatorGatewayClient(type("Web3Stub", (), {"provider": object()})(), "avalanche")

    response = client.Call(type("Request", (), {"chain": "avalanche", "method": method, "params": "[]"})())

    assert response.success is False
    assert "unsupported operator read" in response.error


@pytest.mark.parametrize(
    ("obligation", "expected"),
    [
        ("SUPPLY:USDC:2", ("SUPPLY", "USDC", 2)),
        ("BORROW:WETH:0.0001", ("BORROW", "WETH", 0.0001)),
        ("REPAY_ALL:WETH", ("REPAY_ALL", "WETH", None)),
        ("WITHDRAW_ALL:USDC", ("WITHDRAW_ALL", "USDC", None)),
    ],
)
def test_live_obligation_parser_is_typed_and_exact(obligation: str, expected: tuple) -> None:
    runner = _runner_module()

    action, symbol, amount = runner._parse_action(obligation)

    assert (action, symbol) == expected[:2]
    assert (float(amount) if amount is not None else None) == expected[2]


@pytest.mark.parametrize("obligation", ["SUPPLY:USDC", "REPAY_ALL:WETH:1", "SWAP:USDC:1", "SUPPLY"])
def test_live_obligation_parser_rejects_ambiguous_or_unsupported_actions(obligation: str) -> None:
    runner = _runner_module()

    with pytest.raises(ValueError, match="obligation"):
        runner._parse_action(obligation)


@pytest.mark.parametrize(
    ("obligation", "expected"),
    [
        (
            UNISWAP_V3_ARBITRUM_SWAP_EOA.target[0],
            ("SWAP", "USDC", "WETH", 0.5, 500, UNISWAP_V3_ARBITRUM_SWAP_EOA.resource_address),
        ),
        (
            UNISWAP_V3_ARBITRUM_SWAP_EOA.cleanup[0],
            ("SWAP_ALL", "WETH", "USDC", None, 500, UNISWAP_V3_ARBITRUM_SWAP_EOA.resource_address),
        ),
    ],
)
def test_uniswap_live_obligation_parser_is_typed_and_exact(obligation: str, expected: tuple) -> None:
    runner = _runner_module()

    action, token_in, token_out, amount, fee, pool = runner._parse_swap_action(obligation)

    assert (action, token_in, token_out, float(amount) if amount is not None else None, fee) == expected[:5]
    assert pool.lower() == expected[5].lower()


@pytest.mark.parametrize(
    "obligation",
    [
        "SWAP:USDC:WETH:0.5:500",
        "SWAP_ALL:WETH:USDC:1:500:0x" + "11" * 20,
        "SWAP:USDC:WETH:0:500:0x" + "11" * 20,
        "SWAP:USDC:WETH:1:0:0x" + "11" * 20,
    ],
)
def test_uniswap_live_obligation_parser_rejects_ambiguous_shapes(obligation: str) -> None:
    runner = _runner_module()

    with pytest.raises((ValueError, TypeError), match="swap|Swap|fee|amount"):
        runner._parse_swap_action(obligation)


def test_recover_seal_is_read_only_and_revalidates_completed_bundle(tmp_path: Path, monkeypatch) -> None:
    runner = _runner_module()
    plan = _plan()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "evidence").mkdir()
    (bundle / "results.xml").write_text("<testsuite/>", encoding="utf-8")
    (bundle / "envelope.json").write_text("{}", encoding="utf-8")
    (bundle / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    (bundle / "result.json").write_text(
        json.dumps(
            {
                "target": "PASS",
                "cleanup": "PASS",
                "terminal_position_zero": True,
                "sweep": "PASS",
                "overall": "FAIL",
                "error": "seal crashed",
            }
        ),
        encoding="utf-8",
    )
    calls = []
    monkeypatch.setattr(runner, "_git_sha", lambda: plan["git_sha"])
    monkeypatch.setattr(runner, "validate_mainnet_envelope", lambda path: calls.append(("validate", path)))
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **kwargs: (
            calls.append(("subprocess", command)),
            SimpleNamespace(returncode=0, stdout="/immutable/seal\n", stderr=""),
        )[1],
    )

    assert runner.recover_seal_command(SimpleNamespace(bundle=bundle)) == 0
    result = json.loads((bundle / "result.json").read_text(encoding="utf-8"))

    assert result["overall"] == "PASS"
    assert result["seal_path"] == "/immutable/seal"
    command = next(value for kind, value in calls if kind == "subprocess")
    assert "intent-seal" in command
    assert not ({"run", "fund", "sweep"} & set(command))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("target", "FAIL"),
        ("cleanup", "NOT_RUN"),
        ("terminal_position_zero", False),
        ("sweep", "FAIL"),
    ],
)
def test_recover_seal_refuses_incomplete_money_lifecycle(
    tmp_path: Path, monkeypatch, field: str, value: object
) -> None:
    runner = _runner_module()
    plan = _plan()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    result = {
        "target": "PASS",
        "cleanup": "PASS",
        "terminal_position_zero": True,
        "sweep": "PASS",
    }
    result[field] = value
    (bundle / "result.json").write_text(json.dumps(result), encoding="utf-8")
    (bundle / "envelope.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(runner, "_git_sha", lambda: plan["git_sha"])

    with pytest.raises(ValueError, match="refuses an incomplete"):
        runner._seal_completed_bundle(
            output=bundle,
            plan=plan,
            envelope_path=bundle / "envelope.json",
        )


def test_asset_decimals_refuses_an_unrecorded_symbol() -> None:
    """An unknown asset must refuse, never default to 18.

    The expression this replaced was `6 if symbol == "USDC" else 18`, which
    silently called every non-USDC asset 18 decimals. WBTC is 8 and USDT is 6,
    so adding either to a recipe would have mis-scaled a real mainnet spend cap
    by 10**10 or 10**12 with nothing to notice it. The guard is only worth
    anything if it actually fires, so pin that it does.
    """
    for symbol in ("WBTC", "DAI", "", "usdc"):
        with pytest.raises(ValueError, match="No decimals recorded for mainnet asset"):
            asset_decimals(symbol)


@pytest.mark.parametrize(("symbol", "expected"), [("USDC", 6), ("USDT", 6), ("WETH", 18), ("WAVAX", 18)])
def test_asset_decimals_returns_the_recorded_value(symbol: str, expected: int) -> None:
    """Liveness: the refusal above is not simply refusing everything."""
    assert asset_decimals(symbol) == expected


def test_every_recipe_decimal_agrees_with_the_map() -> None:
    """No recipe may carry a decimals value the map disagrees with.

    Recipes used to hardcode the number next to the symbol. Correct today, but
    a second source that can drift; both sides of the cap check must agree.
    """
    for recipe in (
        AAVE_V3_ARBITRUM_SUPPLY_EOA,
        UNISWAP_V3_ARBITRUM_SWAP_EOA,
        UNISWAP_V3_BASE_SWAP_EOA,
        TRADERJOE_V2_AVALANCHE_SWAP_EOA,
    ):
        assert recipe.asset_decimals == MAINNET_ASSET_DECIMALS[recipe.asset_symbol]
        output_symbol = getattr(recipe, "output_asset_symbol", None)
        if output_symbol:
            assert recipe.output_asset_decimals == MAINNET_ASSET_DECIMALS[output_symbol]
