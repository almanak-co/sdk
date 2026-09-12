"""Runner-side contract for the Morpho Blue mainnet lane.

These are the pieces that decide what a live wallet actually does: how an
obligation string turns into an Intent, and which sizes the scenario spends.
"""

from __future__ import annotations

import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from qa_lab.mainnet_intent_recipe import (
    MORPHO_ROBINHOOD_BORROW_EOA,
    MORPHO_ROBINHOOD_REPAY_EOA,
    MORPHO_ROBINHOOD_SUPPLY_EOA,
    MORPHO_ROBINHOOD_WITHDRAW_EOA,
)

MORPHO_RECIPES = (
    MORPHO_ROBINHOOD_SUPPLY_EOA,
    MORPHO_ROBINHOOD_WITHDRAW_EOA,
    MORPHO_ROBINHOOD_BORROW_EOA,
    MORPHO_ROBINHOOD_REPAY_EOA,
)


def _runner() -> Any:
    root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(root / "qa_lab"))
    spec = importlib.util.spec_from_file_location(
        "qa_lab.run_mainnet_intent", root / "qa_lab" / "run_mainnet_intent.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("qa_lab.run_mainnet_intent", module)
    spec.loader.exec_module(module)
    return module


RUNNER = _runner()


def test_obligation_parser_requires_a_market_id() -> None:
    """Morpho is a singleton, so an action/symbol pair names no resource."""
    market = MORPHO_ROBINHOOD_BORROW_EOA.market_id
    action, symbol, amount, parsed_market = RUNNER._parse_morpho_action(f"BORROW:USDG:1:{market}")
    assert (action, symbol, amount, parsed_market) == ("BORROW", "USDG", Decimal("1"), market.lower())
    for bad in (
        "BORROW:USDG:1",
        "BORROW:USDG",
        f"BORROW:USDG:1:{market[:-2]}",
        # A 20-byte address is not a market id; accepting one would let a pool
        # address stand in for a market.
        "BORROW:USDG:1:0x" + "11" * 20,
    ):
        with pytest.raises(ValueError):
            RUNNER._parse_morpho_action(bad)


def test_obligation_parser_pins_the_amount_shape_to_the_verb() -> None:
    market = MORPHO_ROBINHOOD_BORROW_EOA.market_id
    with pytest.raises(ValueError, match="amount shape"):
        RUNNER._parse_morpho_action(f"REPAY_ALL:USDG:1:{market}")
    with pytest.raises(ValueError, match="amount shape"):
        RUNNER._parse_morpho_action(f"REPAY:USDG:{market}")
    for bad in (f"REPAY:USDG:0:{market}", f"REPAY:USDG:-1:{market}"):
        with pytest.raises(ValueError, match="must be positive"):
            RUNNER._parse_morpho_action(bad)


def test_unknown_verbs_are_refused_rather_than_routed_somewhere() -> None:
    market = MORPHO_ROBINHOOD_BORROW_EOA.market_id
    for bad in ("LIQUIDATE", "FLASH_LOAN", "SUPPLY_ALL"):
        with pytest.raises(ValueError, match="Unsupported Mainnet Morpho obligation"):
            RUNNER._parse_morpho_action(f"{bad}:USDG:1:{market}")


def test_an_obligation_for_another_market_is_refused_before_it_executes() -> None:
    other = "0x" + "ab" * 32
    with pytest.raises(RuntimeError, match="names a market the recipe did not approve"):
        RUNNER._morpho_action_intent(recipe=MORPHO_ROBINHOOD_BORROW_EOA, obligation=f"BORROW:USDG:1:{other}")


@pytest.mark.parametrize(
    ("obligation_template", "expected_type", "expected_flags"),
    [
        ("SUPPLY:USDG:1:{market}", "SupplyIntent", {"use_as_collateral": False}),
        ("SUPPLY_COLLATERAL:USDe:4:{market}", "SupplyIntent", {"use_as_collateral": True}),
        # The loan-side withdraw must say is_collateral=False or Morpho routes it
        # to withdrawCollateral() and underflows the collateral position.
        ("WITHDRAW:USDG:1:{market}", "WithdrawIntent", {"is_collateral": False, "withdraw_all": False}),
        ("WITHDRAW_ALL:USDG:{market}", "WithdrawIntent", {"is_collateral": False, "withdraw_all": True}),
        ("WITHDRAW_COLLATERAL_ALL:USDe:{market}", "WithdrawIntent", {"is_collateral": True, "withdraw_all": True}),
        ("REPAY:USDG:1:{market}", "RepayIntent", {"repay_full": False}),
        ("REPAY_ALL:USDG:{market}", "RepayIntent", {"repay_full": True}),
    ],
)
def test_each_verb_routes_to_the_leg_it_names(
    obligation_template: str, expected_type: str, expected_flags: dict[str, Any]
) -> None:
    recipe = MORPHO_ROBINHOOD_BORROW_EOA
    intent = RUNNER._morpho_action_intent(recipe=recipe, obligation=obligation_template.format(market=recipe.market_id))
    assert type(intent).__name__ == expected_type
    for field, value in expected_flags.items():
        assert getattr(intent, field) == value, field
    assert intent.market_id == recipe.market_id


@pytest.mark.parametrize("recipe", MORPHO_RECIPES)
def test_only_the_sizes_the_scenario_spends_are_non_zero(recipe: Any) -> None:
    """Each field must say what this cell spends, not what some other leg spends.

    SUPPLY appears both as this cell's target and as WITHDRAW's prerequisite, so
    a merged scan of setup and target populates withdraw_setup on the SUPPLY
    cell with a number that leg never spends.
    """
    amounts = RUNNER._morpho_amounts(recipe)
    declared: dict[str, Decimal] = {}
    for obligation in (*recipe.setup, *recipe.target):
        action, _, amount, _ = RUNNER._parse_morpho_action(obligation)
        if amount is not None:
            declared[action] = amount
    expected = {
        "collateral": declared.get("SUPPLY_COLLATERAL", Decimal(0)),
        "borrow": declared.get("BORROW", Decimal(0)),
        "supply": Decimal(recipe.target_amount) if recipe.intent == "SUPPLY" else Decimal(0),
        "withdraw_setup": (
            Decimal(recipe.setup[0].split(":")[2])
            if recipe.setup and recipe.setup[0].startswith("SUPPLY:")
            else Decimal(0)
        ),
        "withdraw": Decimal(recipe.target_amount) if recipe.intent == "WITHDRAW" else Decimal(0),
        "repay": Decimal(recipe.target_amount) if recipe.intent == "REPAY" else Decimal(0),
    }
    for field, value in expected.items():
        assert getattr(amounts, field) == value, f"{recipe.intent}.{field}"


@pytest.mark.parametrize("recipe", MORPHO_RECIPES)
def test_the_scenarios_sizes_never_fall_back_to_the_anvil_defaults(recipe: Any) -> None:
    """Liveness: the Anvil defaults are an order of magnitude above these caps."""
    from tests.intents._morpho_blue_exact_proofs import MorphoAmounts

    defaults = MorphoAmounts()
    amounts = RUNNER._morpho_amounts(recipe)
    for field in ("collateral", "borrow", "supply", "withdraw_setup", "withdraw", "repay"):
        value = getattr(amounts, field)
        assert value == 0 or value < getattr(defaults, field), f"{recipe.intent}.{field} kept the Anvil size"


def test_every_terminal_field_is_read_off_the_position_triple() -> None:
    """All three zeros must map to a real field, or a terminal silently passes."""
    assert set(RUNNER.MORPHO_TERMINAL_FIELDS.values()) == {"supply_shares", "borrow_shares", "collateral"}
    for recipe in MORPHO_RECIPES:
        named = {item.split(":", 1)[0] for item in recipe.terminal} & set(RUNNER.MORPHO_TERMINAL_FIELDS)
        assert named == set(RUNNER.MORPHO_TERMINAL_FIELDS)


def _morpho_stuck(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Only the Morpho legs; the allowance census is a separate concern."""
    return [item for item in report["stuck"] if item["kind"] == "morpho_position"]


class _Recorder:
    """Stands in for the compile/execute lane, recording what hygiene attempted."""

    def __init__(self, fail_on: set[str] | None = None) -> None:
        self.attempted: list[str] = []
        self.fail_on = fail_on or set()

    async def __call__(self, *, obligations: tuple[str, ...], **kwargs: Any) -> list[dict[str, Any]]:
        obligation = obligations[0]
        verb = obligation.split(":", 1)[0]
        self.attempted.append(verb)
        if verb in self.fail_on:
            raise RuntimeError(f"{verb} reverted")
        return [{"tx_hash": f"0x{verb.lower()}", "action": obligation, "role": "primary"}]


async def _run_hygiene(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, position: dict[str, int], recorder: _Recorder):
    recipe = MORPHO_ROBINHOOD_BORROW_EOA
    monkeypatch.setattr(
        RUNNER,
        "_morpho_position_call",
        lambda web3, *, recipe, wallet, block="latest": {"wallet": wallet, **position},
    )
    monkeypatch.setattr(RUNNER, "_execute_morpho_obligation_phase", recorder)
    monkeypatch.setattr(RUNNER, "_approval_pairs", lambda rows, *, wallet: [])
    monkeypatch.setattr(RUNNER, "_funding_start_block", lambda web3, output: None)
    monkeypatch.setattr(RUNNER, "_capture_terminal_paths", lambda **kwargs: ({}, True))
    monkeypatch.setattr(RUNNER, "_observe_allowances", lambda **kwargs: (tmp_path / "a.json", True))
    monkeypatch.setattr(RUNNER, "write_json", lambda path, payload: report.update(payload))
    report: dict[str, Any] = {}

    class _Account:
        address = "0x" + "11" * 20

    class _Eth:
        account = type("A", (), {"from_key": staticmethod(lambda key: _Account())})()

    await RUNNER._hygiene_unwind(
        web3=type("W", (), {"eth": _Eth()})(),
        output=tmp_path,
        recipe=recipe,
        wallet="0x" + "11" * 20,
        private_key="0x" + "22" * 32,
        rows=[],
        orchestrator=object(),
        context=object(),
        rpc_url="http://localhost:8545",
        gateway_client=object(),
        price_oracle={"USDG": 1},
    )
    return report


@pytest.mark.asyncio
async def test_hygiene_unwinds_a_stranded_morpho_position_debt_first(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failure after setup leaves collateral and debt inside the protocol.

    The sweep cannot reach them -- the value is not in the wallet -- and the
    allowance pass alone would release a durable pool wallet that still owns a
    live collateralised borrow. Debt must be retired before collateral, because
    withdrawing collateral under an open borrow is refused by the protocol.
    """
    recorder = _Recorder()
    report = await _run_hygiene(
        monkeypatch, tmp_path, {"supply_shares": 0, "borrow_shares": 5, "collateral": 9}, recorder
    )
    assert recorder.attempted == ["REPAY_ALL", "WITHDRAW_COLLATERAL_ALL"]
    assert [action["field"] for action in report["actions"]] == ["borrow_shares", "collateral"]
    assert _morpho_stuck(report) == []


@pytest.mark.asyncio
async def test_hygiene_skips_the_legs_that_are_already_flat(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Liveness: a flat position must not send transactions to prove it is flat."""
    recorder = _Recorder()
    report = await _run_hygiene(
        monkeypatch, tmp_path, {"supply_shares": 0, "borrow_shares": 0, "collateral": 0}, recorder
    )
    assert recorder.attempted == []
    assert report["actions"] == []


@pytest.mark.asyncio
async def test_one_stuck_morpho_leg_never_blocks_the_next(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Hygiene runs in the finally lane with inverted semantics.

    A repay that reverts must still be followed by the collateral withdrawal
    attempt and recorded, never raised -- the 2026-08-30 incident was one
    exception skipping every later risk-reducing step.
    """
    recorder = _Recorder(fail_on={"REPAY_ALL"})
    report = await _run_hygiene(
        monkeypatch, tmp_path, {"supply_shares": 3, "borrow_shares": 5, "collateral": 9}, recorder
    )
    assert recorder.attempted == ["REPAY_ALL", "WITHDRAW_ALL", "WITHDRAW_COLLATERAL_ALL"]
    assert [item["field"] for item in _morpho_stuck(report)] == ["borrow_shares"]
    assert [action["field"] for action in report["actions"]] == ["supply_shares", "collateral"]


@pytest.mark.asyncio
async def test_a_position_that_cannot_be_unwound_is_recorded_stuck_not_silently_dropped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When no compiler context survived, the standing position must still be named."""
    recipe = MORPHO_ROBINHOOD_BORROW_EOA
    monkeypatch.setattr(
        RUNNER,
        "_morpho_position_call",
        lambda web3, *, recipe, wallet, block="latest": {
            "wallet": wallet,
            "supply_shares": 0,
            "borrow_shares": 5,
            "collateral": 9,
        },
    )
    monkeypatch.setattr(RUNNER, "_approval_pairs", lambda rows, *, wallet: [])
    monkeypatch.setattr(RUNNER, "_funding_start_block", lambda web3, output: None)
    monkeypatch.setattr(RUNNER, "_capture_terminal_paths", lambda **kwargs: ({}, True))
    monkeypatch.setattr(RUNNER, "_observe_allowances", lambda **kwargs: (tmp_path / "a.json", True))
    report: dict[str, Any] = {}
    monkeypatch.setattr(RUNNER, "write_json", lambda path, payload: report.update(payload))

    class _Account:
        address = "0x" + "11" * 20

    class _Eth:
        account = type("A", (), {"from_key": staticmethod(lambda key: _Account())})()

    await RUNNER._hygiene_unwind(
        web3=type("W", (), {"eth": _Eth()})(),
        output=tmp_path,
        recipe=recipe,
        wallet="0x" + "11" * 20,
        private_key="0x" + "22" * 32,
        rows=[],
        orchestrator=None,
        context=None,
        rpc_url="http://localhost:8545",
        gateway_client=None,
        price_oracle={},
    )
    assert [item["field"] for item in _morpho_stuck(report)] == ["borrow_shares", "collateral"]
