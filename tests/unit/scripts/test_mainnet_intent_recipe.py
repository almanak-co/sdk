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
from qa_lab.mainnet_intent_recipe import (
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
from qa_lab.operator_gateway import OperatorGatewayClient

REPO = Path(__file__).resolve().parents[3]


def _runner_module():
    path = REPO / "qa_lab" / "run_mainnet_intent.py"
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
        f"SWAP_BACK:WETH:USDC:MEASURED:{recipe.fee_tier}:{recipe.resource_address}",
        "SWEEP_TO_MASTER",
    )
    assert recipe.terminal == ("TOKEN_BALANCE_ZERO:WETH", "NO_RESIDUAL_ALLOWANCES", "POOL_WALLET_RELEASED")
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
    assert recipe.cleanup == (f"SWAP_BACK:USDT:WAVAX:MEASURED:20:{recipe.resource_address}", "SWEEP_TO_MASTER")
    assert recipe.terminal == ("TOKEN_BALANCE_ZERO:USDT", "NO_RESIDUAL_ALLOWANCES", "POOL_WALLET_RELEASED")


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("intent", ["PERP_OPEN", "PERP_CLOSE"])
def test_gmx_mainnet_recipe_requires_one_full_keeper_settled_lifecycle(chain: str, intent: str) -> None:
    recipe = resolve_recipe(f"intent.gmx_v2.{chain}.{intent}.mainnet.eoa")

    assert recipe.semantic_profile == "async_perp.v1"
    assert recipe.quant_cell_id == f"perp.gmx_v2.{chain}.simple.mainnet.eoa"
    assert recipe.required_lifecycle == ("PERP_OPEN", "PERP_CLOSE")
    assert recipe.terminal == ("NO_OPEN_GMX_POSITIONS", "NO_PENDING_GMX_ORDERS")
    assert recipe.source == "qa_lab/intent_mainnet.py"


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
            ("SWAP_BACK", "WETH", "USDC", None, 500, UNISWAP_V3_ARBITRUM_SWAP_EOA.resource_address),
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
        "SWAP_ALL:WETH:USDC:ALL:500:0x" + "11" * 20,
        "SWAP_BACK:WETH:USDC:ALL:500:0x" + "11" * 20,
        "SWAP_BACK:WETH:USDC:1:500:0x" + "11" * 20,
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


def test_run_diagnostics_carry_the_runners_own_log_into_the_bundle(tmp_path: Path) -> None:
    import logging

    runner = _runner_module()
    root = logging.getLogger()
    before = (list(root.handlers), root.level)
    diagnostics = runner._RunDiagnostics(tmp_path)
    logging.getLogger("almanak.test.compile").error("Gateway balance query failed: no attribute")
    logging.getLogger("almanak.test.parse").warning("token_resolution_error token=0xabc chain=ethereum")
    logging.getLogger("almanak.test.parse").info("Parsed Aave V3: REPAY 0.00005")
    summary = diagnostics.finish(tmp_path)

    assert (list(root.handlers), root.level) == before, "handlers and level are restored"
    assert summary == {"log": "runner.log", "errors": 1, "warnings": 1}
    written = json.loads((tmp_path / "diagnostics.json").read_text(encoding="utf-8"))
    assert written["errors"] == 1 and written["warnings"] == 1
    assert [row["level"] for row in written["records"]] == ["ERROR", "WARNING"]
    log = (tmp_path / "runner.log").read_text(encoding="utf-8")
    assert "Gateway balance query failed" in log and "Parsed Aave V3" in log


def test_recover_seal_refuses_error_diagnostics_unless_a_human_accepts_them(tmp_path: Path, monkeypatch) -> None:
    runner = _runner_module()
    plan = _plan()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "evidence").mkdir()
    (bundle / "results.xml").write_text("<testsuite/>", encoding="utf-8")
    (bundle / "envelope.json").write_text("{}", encoding="utf-8")
    (bundle / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    (bundle / "result.json").write_text(
        json.dumps({"target": "PASS", "cleanup": "PASS", "terminal_position_zero": True, "sweep": "PASS"}),
        encoding="utf-8",
    )
    (bundle / "diagnostics.json").write_text(json.dumps({"errors": 1, "warnings": 3}), encoding="utf-8")
    monkeypatch.setattr(runner, "_git_sha", lambda: plan["git_sha"])
    monkeypatch.setattr(runner, "validate_mainnet_envelope", lambda path: None)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(returncode=0, stdout="/immutable/seal\n", stderr=""),
    )

    with pytest.raises(ValueError, match="ERROR-level diagnostic"):
        runner.recover_seal_command(SimpleNamespace(bundle=bundle))
    assert runner.recover_seal_command(SimpleNamespace(bundle=bundle, accept_error_diagnostics=True)) == 0


def test_runner_call_sites_bind_every_required_proof_helper_argument() -> None:
    """The reverse-cleanup helper gained a required ``amount_in_raw`` (60dd623cfb)
    and the runner's call site silently kept the old shape; the TypeError surfaced
    only AFTER live funding (2026-08-30 SWAP cleanup). Bind every runner call site
    to its helper's signature so helper drift breaks in CI, not on mainnet.
    """
    import ast
    import inspect

    runner = _runner_module()
    helpers = {
        name: getattr(runner, name)
        for name in (
            "execute_uniswap_v3_exact_reverse_cleanup",
            "execute_traderjoe_v2_reverse_cleanup",
            "run_uniswap_v3_swap_exact_proof",
            "run_traderjoe_v2_swap_exact_proof",
            "run_uniswap_v3_lp_open_exact_proof",
            "run_uniswap_v3_lp_close_exact_proof",
            "execute_aave_lending_target",
        )
    }
    source = (REPO / "qa_lab" / "run_mainnet_intent.py").read_text(encoding="utf-8")
    seen: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id not in helpers:
            continue
        seen.add(node.func.id)
        assert not node.args, f"{node.func.id} must be called keyword-only at line {node.lineno}"
        keywords = {kw.arg for kw in node.keywords if kw.arg is not None}
        required = {
            parameter.name
            for parameter in inspect.signature(helpers[node.func.id]).parameters.values()
            if parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is inspect.Parameter.empty
        }
        missing = sorted(required - keywords)
        assert not missing, f"{node.func.id} call at line {node.lineno} omits required arguments: {missing}"
    assert seen == set(helpers), f"enumeration lost call sites: {sorted(set(helpers) - seen)}"


def test_hygiene_unwind_attempts_every_item_and_never_raises(tmp_path: Path, monkeypatch) -> None:
    """One revoke failure must not block the next pair, an unclosable position is
    recorded as stuck, and the pass reports zeros only from re-derived chain
    observations. This is the inverted-semantics duty lane the 2026-08-30
    incident lacked: the verdict path aborted on its first exception and every
    later risk-reducing step was skipped.
    """
    import asyncio

    runner = _runner_module()
    recipe = SimpleNamespace(
        protocol="uniswap_v3",
        intent="LP_OPEN",
        chain="arbitrum",
        resource_address="0x" + "22" * 20,
        factory_address="0x" + "33" * 20,
        terminal=("UNISWAP_V3_NFT_BALANCE_ZERO", runner.NO_RESIDUAL_ALLOWANCES, "POOL_WALLET_RELEASED"),
    )
    pairs = [("0x" + "aa" * 20, "0x" + "bb" * 20), ("0x" + "cc" * 20, "0x" + "dd" * 20)]
    attempted: list[tuple[str, str]] = []

    def send(web3, *, account, token, spender, chain_id):
        attempted.append((token, spender))
        if token == pairs[0][0]:
            raise RuntimeError("revoke reverted")
        return SimpleNamespace(), "0x" + "e" * 64

    monkeypatch.setattr(runner, "_approval_pairs", lambda rows, *, wallet: list(pairs))
    monkeypatch.setattr(runner, "_allowance", lambda web3, *, token, owner, spender, block="latest": 5)
    monkeypatch.setattr(runner, "_send_allowance_revoke", send)
    monkeypatch.setattr(runner, "_write_receipt_artifact", lambda **kwargs: tmp_path / "receipt.json")
    monkeypatch.setattr(runner, "_owned_position_ids", lambda web3, *, collection, wallet: [123])
    monkeypatch.setattr(runner, "_capture_terminal_paths", lambda **kwargs: ({}, False))
    monkeypatch.setattr(runner, "_observe_allowances", lambda **kwargs: (tmp_path / "allowances.json", False))
    web3 = SimpleNamespace(
        eth=SimpleNamespace(account=SimpleNamespace(from_key=lambda key: SimpleNamespace(address="0x" + "11" * 20)))
    )

    paths, terminal_zero = asyncio.run(
        runner._hygiene_unwind(
            web3=web3,
            output=tmp_path,
            recipe=recipe,
            wallet="0x" + "11" * 20,
            private_key="0x" + "ab" * 32,
            rows=[],
            orchestrator=None,
            context=None,
            rpc_url="",
            gateway_client=None,
            price_oracle={},
        )
    )

    assert attempted == pairs, "one failed revoke must not block the next pair"
    assert terminal_zero is False
    assert paths[runner.NO_RESIDUAL_ALLOWANCES] == tmp_path / "allowances.json"
    report = json.loads((tmp_path / "hygiene.json").read_text())
    stuck_kinds = [item["kind"] for item in report["stuck"]]
    assert "standing_allowance" in stuck_kinds, "the reverted revoke is recorded, not raised"
    assert "open_position" in stuck_kinds, "an unclosable position is recorded as stuck"
    assert [item["kind"] for item in report["actions"]] == ["revoke"], "the second pair still revoked"


def test_anchor_target_prices_copies_only_symbols_the_anchor_priced() -> None:
    """A swap's output token is never funded, so it has no anchor row; demanding
    one crashed envelope building (KeyError 'WETH') on the first SWAP run whose
    cleanup passed. The envelope copies what the anchor priced, nothing more.
    """
    runner = _runner_module()
    rows = {"ETH": {"price": "2500", "native": True}, "USDC": {"price": "1"}}
    assert runner._anchor_target_prices(rows, UNISWAP_V3_ARBITRUM_SWAP_EOA) == {"USDC": "1"}
    assert runner._anchor_target_prices({**rows, "WETH": {"price": "2500.1"}}, UNISWAP_V3_ARBITRUM_SWAP_EOA) == {
        "USDC": "1",
        "WETH": "2500.1",
    }


def test_hygiene_unwind_recovers_pairs_from_the_chain_when_rows_were_lost(tmp_path: Path, monkeypatch) -> None:
    """The 2026-09-01 LP_CLOSE crash unwound before its setup rows were assigned:
    _approval_pairs saw nothing and a wallet holding two live approvals was
    RELEASED on a vacuously-zero observation. Hygiene must re-derive approvals
    from the chain's own Approval logs since funding.
    """
    import asyncio

    runner = _runner_module()
    recipe = SimpleNamespace(
        protocol="aave_v3",
        intent="REPAY",
        chain="arbitrum",
        resource_address="0x" + "22" * 20,
        terminal=(runner.NO_RESIDUAL_ALLOWANCES,),
    )
    pair = ("0x" + "aa" * 20, "0x" + "bb" * 20)
    sent: list[tuple[str, str]] = []
    observed: dict = {}

    monkeypatch.setattr(runner, "_approval_pairs", lambda rows, *, wallet: [])
    monkeypatch.setattr(runner, "_funding_start_block", lambda web3, output: 500)
    monkeypatch.setattr(runner, "_chain_approval_pairs", lambda web3, *, wallet, from_block: [pair])
    monkeypatch.setattr(runner, "_allowance", lambda web3, **kwargs: 7)
    monkeypatch.setattr(
        runner,
        "_send_allowance_revoke",
        lambda web3, *, account, token, spender, chain_id: (
            sent.append((token, spender)),
            (SimpleNamespace(), "0x" + "e" * 64),
        )[1],
    )
    monkeypatch.setattr(runner, "_write_receipt_artifact", lambda **kwargs: tmp_path / "receipt.json")
    monkeypatch.setattr(
        runner,
        "_observe_allowances",
        lambda **kwargs: (observed.setdefault("pairs", list(kwargs["pairs"])), (tmp_path / "a.json", True))[1],
    )
    monkeypatch.setattr(runner, "_capture_terminal_paths", lambda **kwargs: ({}, True))
    web3 = SimpleNamespace(
        eth=SimpleNamespace(account=SimpleNamespace(from_key=lambda key: SimpleNamespace(address="0x" + "11" * 20)))
    )

    _, terminal_zero = asyncio.run(
        runner._hygiene_unwind(
            web3=web3,
            output=tmp_path,
            recipe=recipe,
            wallet="0x" + "11" * 20,
            private_key="0x" + "ab" * 32,
            rows=[],
            orchestrator=None,
            context=None,
            rpc_url="",
            gateway_client=None,
            price_oracle={},
        )
    )

    assert sent == [pair], "the chain-census pair must be revoked despite empty rows"
    assert observed["pairs"] == [pair], "the observation must cover the census pairs"
    assert terminal_zero is True
    report = json.loads((tmp_path / "hygiene.json").read_text())
    assert report["approval_census_ok"] is True


def test_hygiene_unwind_treats_an_unproven_empty_pair_set_as_not_zero(tmp_path: Path, monkeypatch) -> None:
    """Zero pairs proven by nothing (rows lost AND census unavailable) must
    quarantine the wallet, never release it on a vacuous observation.
    """
    import asyncio

    runner = _runner_module()
    recipe = SimpleNamespace(
        protocol="aave_v3",
        intent="REPAY",
        chain="arbitrum",
        resource_address="0x" + "22" * 20,
        terminal=(runner.NO_RESIDUAL_ALLOWANCES,),
    )
    monkeypatch.setattr(runner, "_approval_pairs", lambda rows, *, wallet: [])
    monkeypatch.setattr(runner, "_funding_start_block", lambda web3, output: None)
    monkeypatch.setattr(runner, "_observe_allowances", lambda **kwargs: (tmp_path / "a.json", True))
    monkeypatch.setattr(runner, "_capture_terminal_paths", lambda **kwargs: ({}, True))
    web3 = SimpleNamespace(
        eth=SimpleNamespace(account=SimpleNamespace(from_key=lambda key: SimpleNamespace(address="0x" + "11" * 20)))
    )

    _, terminal_zero = asyncio.run(
        runner._hygiene_unwind(
            web3=web3,
            output=tmp_path,
            recipe=recipe,
            wallet="0x" + "11" * 20,
            private_key="0x" + "ab" * 32,
            rows=[],
            orchestrator=None,
            context=None,
            rpc_url="",
            gateway_client=None,
            price_oracle={},
        )
    )

    assert terminal_zero is False, "vacuous zero must not release the wallet"
    report = json.loads((tmp_path / "hygiene.json").read_text())
    assert any(item["kind"] == "approval_census" for item in report["stuck"])
    assert report["approval_census_ok"] is False


def test_chain_approval_census_excludes_erc721_approvals(monkeypatch) -> None:
    """ERC-721 Approval shares topic0 with ERC-20 but has no allowance() to read.

    get_logs matches topic0 only, so a position-NFT approval (4 topics, empty
    data) lands in the same result set; admitting it sends `allowance(address,
    address)` to a contract that reverts, which marks a clean run FAIL in the
    verdict path and wedges the hygiene pass.
    """
    from types import SimpleNamespace

    runner = _runner_module()
    wallet = "0x" + "11" * 20
    owner_word = "0x" + wallet.removeprefix("0x").rjust(64, "0")
    spender_word = "0x" + ("bb" * 20).rjust(64, "0")
    erc20 = {
        "address": "0x" + "aa" * 20,
        "topics": [runner.APPROVAL_TOPIC, owner_word, spender_word],
        "data": "0x" + "01".rjust(64, "0"),
    }
    erc721 = {
        "address": "0x" + "cc" * 20,
        "topics": [runner.APPROVAL_TOPIC, owner_word, spender_word, "0x" + "07".rjust(64, "0")],
        "data": "0x",
    }
    web3 = SimpleNamespace(eth=SimpleNamespace(get_logs=lambda params: [erc20, erc721]))

    pairs = runner._chain_approval_pairs(web3, wallet=wallet, from_block=1)

    assert pairs == [("0x" + "aa" * 20, "0x" + "bb" * 20)]
