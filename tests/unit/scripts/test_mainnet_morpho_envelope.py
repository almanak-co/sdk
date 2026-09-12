"""Seal-time contract for the Morpho Blue lending mainnet cells.

Morpho is a singleton: every market on the chain settles through one contract,
so an emitter address discriminates nothing and the market id does all the
work. These cells are also the first mainnet lane that can leave value behind
in two different places -- shares and collateral -- so the terminal set is
pinned as three separate zeros rather than one.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from qa_lab.mainnet_intent_envelope import (
    MainnetEnvelopeError,
    _required_guard_ids,
    _validate_morpho_guard,
    _validate_morpho_phase_action,
)
from qa_lab.mainnet_intent_recipe import (
    MORPHO_ROBINHOOD_BORROW_EOA,
    MORPHO_ROBINHOOD_REPAY_EOA,
    MORPHO_ROBINHOOD_SUPPLY_EOA,
    MORPHO_ROBINHOOD_WITHDRAW_EOA,
    RECIPES,
)

CHAIN_ID = 4663
WALLET = "0x" + "11" * 20
MORPHO_RECIPES = (
    MORPHO_ROBINHOOD_SUPPLY_EOA,
    MORPHO_ROBINHOOD_WITHDRAW_EOA,
    MORPHO_ROBINHOOD_BORROW_EOA,
    MORPHO_ROBINHOOD_REPAY_EOA,
)
RECIPE = MORPHO_ROBINHOOD_BORROW_EOA


def _identity(**overrides: Any) -> dict[str, Any]:
    observation = {
        "chain_id": CHAIN_ID,
        "block_number": 7,
        "block_hash": "0x" + "ab" * 32,
        "morpho": RECIPE.resource_address,
        "market_id": RECIPE.market_id,
        "loan_token": RECIPE.asset_address,
        "collateral_token": RECIPE.collateral_address,
        "oracle": RECIPE.oracle_address,
        "irm": RECIPE.irm_address,
        "lltv": int(RECIPE.lltv),
        "morpho_code_sha256": "cd" * 32,
    }
    observation.update(overrides)
    return observation


def _liquidity(**overrides: Any) -> dict[str, Any]:
    observation = {
        "managed_fork": False,
        "chain_id": CHAIN_ID,
        "block_number": 8,
        "block_hash": "0x" + "ba" * 32,
        "market_id": RECIPE.market_id,
        "raw_result": "0x" + "01" * 192,
        "requested_amount_raw": 1_000_000,
        "position_before": 0,
        "position_after": 999_999,
    }
    observation.update(overrides)
    return observation


@pytest.mark.parametrize("recipe", MORPHO_RECIPES)
def test_every_morpho_cell_is_registered_and_market_bound(recipe: Any) -> None:
    assert RECIPES[recipe.cell_id] is recipe
    assert recipe.protocol == "morpho_blue"
    assert recipe.exec_path == "eoa"
    assert recipe.semantic_profile == "lending.v1"
    assert recipe.market_id.startswith("0x") and len(recipe.market_id) == 66
    assert recipe.nodeid.endswith(f"::test_{recipe.intent.lower()}_exact_eoa")
    # Every obligation names the approved market; a bare action/symbol pair
    # would be satisfied by a receipt from any market on the chain.
    for obligation in (*recipe.setup, *recipe.target, *recipe.cleanup):
        if obligation == "SWEEP_TO_MASTER":
            continue
        assert obligation.rsplit(":", 1)[1].lower() == recipe.market_id.lower()
    assert recipe.terminal == (
        f"MORPHO_SUPPLY_SHARES_ZERO:{recipe.market_id}",
        f"MORPHO_BORROW_SHARES_ZERO:{recipe.market_id}",
        f"MORPHO_COLLATERAL_ZERO:{recipe.market_id}",
        "NO_RESIDUAL_ALLOWANCES",
        "POOL_WALLET_RELEASED",
    )
    assert _required_guard_ids(recipe) == {
        "morpho_blue_exact_market_identity",
        "morpho_blue_production_market_liquidity",
    }


@pytest.mark.parametrize("recipe", MORPHO_RECIPES)
def test_every_debt_cell_unwinds_both_legs(recipe: Any) -> None:
    """A cleanup that repays but strands collateral leaves a live position."""
    cleanup = [item for item in recipe.cleanup if item != "SWEEP_TO_MASTER"]
    if recipe.intent in {"BORROW", "REPAY"}:
        assert [item.split(":", 1)[0] for item in cleanup] == ["REPAY_ALL", "WITHDRAW_COLLATERAL_ALL"]
    else:
        assert [item.split(":", 1)[0] for item in cleanup] == ["WITHDRAW_ALL"]


@pytest.mark.parametrize("recipe", (MORPHO_ROBINHOOD_BORROW_EOA, MORPHO_ROBINHOOD_REPAY_EOA))
def test_debt_cells_stay_far_below_the_market_liquidation_threshold(recipe: Any) -> None:
    """Live oracle drift must not be able to liquidate the setup mid-run."""
    collateral = next(Decimal(item.split(":")[2]) for item in recipe.setup if item.startswith("SUPPLY_COLLATERAL:"))
    borrowed = Decimal(
        next(item.split(":")[2] for item in (*recipe.setup, *recipe.target) if item.startswith("BORROW:"))
    )
    lltv = Decimal(recipe.lltv) / Decimal(10**18)
    assert borrowed / collateral <= Decimal("0.30") < lltv


@pytest.mark.parametrize("recipe", (MORPHO_ROBINHOOD_BORROW_EOA, MORPHO_ROBINHOOD_REPAY_EOA))
def test_debt_cells_fund_an_interest_buffer(recipe: Any) -> None:
    """REPAY_ALL owes principal plus accrued interest, which the borrow did not provide."""
    funded = {item.split(":")[0]: Decimal(item.split(":")[1]) for item in recipe.funding_tokens}
    assert funded.get(recipe.asset_symbol, Decimal(0)) > 0


def test_market_identity_guard_accepts_a_healthy_observation() -> None:
    _validate_morpho_guard(
        guard_id="morpho_blue_exact_market_identity",
        observation=_identity(),
        recipe=RECIPE,
        chain_id=CHAIN_ID,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"chain_id": 1},
        {"morpho": "0x" + "22" * 20},
        {"market_id": "0x" + "33" * 32},
        {"loan_token": "0x" + "44" * 20},
        {"collateral_token": "0x" + "55" * 20},
        # The id is the hash of all five parameters; a market that kept the pair
        # but changed its oracle or IRM is a different market with different risk.
        {"oracle": "0x" + "66" * 20},
        {"irm": "0x" + "77" * 20},
        {"lltv": 860000000000000000},
        {"morpho_code_sha256": "00" * 32},
    ],
)
def test_market_identity_guard_refuses_each_drift(override: dict[str, Any]) -> None:
    with pytest.raises(MainnetEnvelopeError):
        _validate_morpho_guard(
            guard_id="morpho_blue_exact_market_identity",
            observation=_identity(**override),
            recipe=RECIPE,
            chain_id=CHAIN_ID,
        )


def test_liquidity_guard_accepts_a_target_that_moved_the_position() -> None:
    _validate_morpho_guard(
        guard_id="morpho_blue_production_market_liquidity",
        observation=_liquidity(),
        recipe=RECIPE,
        chain_id=CHAIN_ID,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"managed_fork": True},
        {"chain_id": 1},
        {"market_id": "0x" + "88" * 32},
        {"requested_amount_raw": 0},
        {"raw_result": "0x" + "00" * 192},
        # A target that left the position where it found it moved nothing,
        # whatever its receipt said.
        {"position_before": 500, "position_after": 500},
    ],
)
def test_liquidity_guard_refuses_an_unmoved_or_unbound_target(override: dict[str, Any]) -> None:
    with pytest.raises(MainnetEnvelopeError):
        _validate_morpho_guard(
            guard_id="morpho_blue_production_market_liquidity",
            observation=_liquidity(**override),
            recipe=RECIPE,
            chain_id=CHAIN_ID,
        )


def test_unknown_morpho_guard_id_is_refused_rather_than_ignored() -> None:
    with pytest.raises(MainnetEnvelopeError, match="No validator for mandatory production guard"):
        _validate_morpho_guard(guard_id="morpho_blue_something_new", observation={}, recipe=RECIPE, chain_id=CHAIN_ID)


class _Event:
    def __init__(self, event_type: Any, market_id: str, assets: int, on_behalf_of: str) -> None:
        self.event_type = event_type
        self.data = {"market_id": market_id, "assets": assets, "on_behalf_of": on_behalf_of}


class _Parsed:
    def __init__(self, events: list[_Event]) -> None:
        self.success = True
        self.events = events


@pytest.fixture
def stub_parser(monkeypatch: pytest.MonkeyPatch):
    """Let each case declare what the parser decodes, independently of the logs.

    The receipt parser has its own dedicated tests; what is under test here is
    that the envelope binds the decoded event to the approved market, wallet,
    and amount.
    """
    import qa_lab.mainnet_intent_envelope as envelope

    holder: dict[str, list[_Event]] = {"events": []}

    class _Parser:
        def parse_receipt(self, receipt: dict[str, Any]) -> _Parsed:
            return _Parsed(holder["events"])

    monkeypatch.setattr(envelope, "MorphoBlueReceiptParser", lambda *a, **k: _Parser())
    return holder


def _logs(event_name: str, market_id: str | None = None) -> list[dict[str, Any]]:
    from almanak.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

    return [
        {
            "address": RECIPE.resource_address,
            "topics": [EVENT_TOPICS[event_name], market_id or RECIPE.market_id, "0x" + "00" * 32],
            "data": "0x",
        }
    ]


def _payload(event_name: str, market_id: str | None = None) -> dict[str, Any]:
    return {
        "raw_receipt": {
            "transactionHash": "0x" + "ee" * 32,
            "blockNumber": 9,
            "logs": _logs(event_name, market_id),
        }
    }


def test_collateral_setup_receipt_is_bound_to_market_wallet_and_amount(stub_parser) -> None:
    from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueEventType

    obligation = RECIPE.setup[0]
    expected = int(Decimal(obligation.split(":")[2]) * Decimal(10**RECIPE.collateral_decimals))
    stub_parser["events"] = [_Event(MorphoBlueEventType.SUPPLY_COLLATERAL, RECIPE.market_id, expected, WALLET)]
    _validate_morpho_phase_action(
        payload=_payload("SupplyCollateral"), action_spec=obligation, recipe=RECIPE, wallet=WALLET
    )


def test_a_setup_event_from_another_market_is_refused(stub_parser) -> None:
    from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueEventType

    obligation = RECIPE.setup[0]
    expected = int(Decimal(obligation.split(":")[2]) * Decimal(10**RECIPE.collateral_decimals))
    other = "0x" + "99" * 32
    stub_parser["events"] = [_Event(MorphoBlueEventType.SUPPLY_COLLATERAL, other, expected, WALLET)]
    with pytest.raises(MainnetEnvelopeError, match="authoritative Morpho event in the approved market"):
        _validate_morpho_phase_action(
            payload=_payload("SupplyCollateral", market_id=other),
            action_spec=obligation,
            recipe=RECIPE,
            wallet=WALLET,
        )


def test_a_setup_event_credited_to_another_account_is_refused(stub_parser) -> None:
    from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueEventType

    obligation = RECIPE.setup[0]
    expected = int(Decimal(obligation.split(":")[2]) * Decimal(10**RECIPE.collateral_decimals))
    stub_parser["events"] = [
        _Event(MorphoBlueEventType.SUPPLY_COLLATERAL, RECIPE.market_id, expected, "0x" + "77" * 20)
    ]
    with pytest.raises(MainnetEnvelopeError, match="not bound to the approved wallet"):
        _validate_morpho_phase_action(
            payload=_payload("SupplyCollateral"), action_spec=obligation, recipe=RECIPE, wallet=WALLET
        )


def test_a_setup_amount_that_differs_from_the_obligation_is_refused(stub_parser) -> None:
    from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueEventType

    obligation = RECIPE.setup[0]
    stub_parser["events"] = [_Event(MorphoBlueEventType.SUPPLY_COLLATERAL, RECIPE.market_id, 1, WALLET)]
    with pytest.raises(MainnetEnvelopeError, match="does not prove its amount obligation"):
        _validate_morpho_phase_action(
            payload=_payload("SupplyCollateral"), action_spec=obligation, recipe=RECIPE, wallet=WALLET
        )


def test_an_all_obligation_only_requires_a_positive_amount(stub_parser) -> None:
    from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueEventType

    obligation = next(item for item in RECIPE.cleanup if item.startswith("REPAY_ALL:"))
    stub_parser["events"] = [_Event(MorphoBlueEventType.REPAY, RECIPE.market_id, 7, WALLET)]
    _validate_morpho_phase_action(payload=_payload("Repay"), action_spec=obligation, recipe=RECIPE, wallet=WALLET)


def test_an_obligation_naming_the_wrong_leg_is_refused(stub_parser) -> None:
    """A collateral verb carrying the loan symbol must not validate."""
    spec = f"SUPPLY_COLLATERAL:{RECIPE.asset_symbol}:4:{RECIPE.market_id}"
    with pytest.raises(MainnetEnvelopeError, match="differs from the approved market leg"):
        _validate_morpho_phase_action(
            payload=_payload("SupplyCollateral"), action_spec=spec, recipe=RECIPE, wallet=WALLET
        )


def test_an_unknown_morpho_verb_is_refused(stub_parser) -> None:
    spec = f"LIQUIDATE:{RECIPE.asset_symbol}:1:{RECIPE.market_id}"
    with pytest.raises(MainnetEnvelopeError, match="Unsupported Morpho phase obligation"):
        _validate_morpho_phase_action(payload=_payload("Repay"), action_spec=spec, recipe=RECIPE, wallet=WALLET)


def _position(**overrides: Any) -> dict[str, Any]:
    observation = {
        "chain_id": CHAIN_ID,
        "block_number": 11,
        "block_hash": "0x" + "dd" * 32,
        "wallet": WALLET,
        "morpho": RECIPE.resource_address,
        "market_id": RECIPE.market_id,
        "supply_shares": 0,
        "borrow_shares": 0,
        "collateral": 0,
    }
    observation.update(overrides)
    return observation


@pytest.mark.parametrize("obligation", [item for item in RECIPE.terminal if item.startswith("MORPHO_")])
def test_terminal_accepts_a_position_read_flat(obligation: str) -> None:
    from qa_lab.mainnet_intent_envelope import _validate_morpho_terminal

    _validate_morpho_terminal(
        obligation=obligation,
        observation=_position(),
        recipe=RECIPE,
        wallet=WALLET,
        chain_id=CHAIN_ID,
    )


@pytest.mark.parametrize(
    ("obligation_prefix", "field"),
    [
        ("MORPHO_SUPPLY_SHARES_ZERO", "supply_shares"),
        ("MORPHO_BORROW_SHARES_ZERO", "borrow_shares"),
        ("MORPHO_COLLATERAL_ZERO", "collateral"),
    ],
)
def test_each_terminal_reads_its_own_field_and_no_other(obligation_prefix: str, field: str) -> None:
    """The failure this catches is a validator wired to the wrong field.

    Three zeros that all read `supply_shares` would release a wallet still
    holding collateral. Each obligation must refuse when ITS field is non-zero
    and stay silent when only the others are.
    """
    from qa_lab.mainnet_intent_envelope import _validate_morpho_terminal

    obligation = f"{obligation_prefix}:{RECIPE.market_id}"
    with pytest.raises(MainnetEnvelopeError, match="is not independently zero"):
        _validate_morpho_terminal(
            obligation=obligation,
            observation=_position(**{field: 1}),
            recipe=RECIPE,
            wallet=WALLET,
            chain_id=CHAIN_ID,
        )
    others = {name: 1 for name in ("supply_shares", "borrow_shares", "collateral") if name != field}
    _validate_morpho_terminal(
        obligation=obligation,
        observation=_position(**others),
        recipe=RECIPE,
        wallet=WALLET,
        chain_id=CHAIN_ID,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"chain_id": 1},
        {"wallet": "0x" + "99" * 20},
        {"morpho": "0x" + "88" * 20},
        {"market_id": "0x" + "77" * 32},
        {"block_hash": ""},
    ],
)
def test_terminal_refuses_an_observation_bound_to_something_else(override: dict[str, Any]) -> None:
    from qa_lab.mainnet_intent_envelope import _validate_morpho_terminal

    with pytest.raises(MainnetEnvelopeError):
        _validate_morpho_terminal(
            obligation=f"MORPHO_COLLATERAL_ZERO:{RECIPE.market_id}",
            observation=_position(**override),
            recipe=RECIPE,
            wallet=WALLET,
            chain_id=CHAIN_ID,
        )


def test_a_terminal_obligation_naming_another_market_is_refused() -> None:
    """A flat position in the wrong market proves nothing about this one."""
    from qa_lab.mainnet_intent_envelope import _validate_morpho_terminal

    with pytest.raises(MainnetEnvelopeError, match="is not independently zero"):
        _validate_morpho_terminal(
            obligation="MORPHO_COLLATERAL_ZERO:0x" + "66" * 32,
            observation=_position(),
            recipe=RECIPE,
            wallet=WALLET,
            chain_id=CHAIN_ID,
        )


@pytest.mark.parametrize("recipe", MORPHO_RECIPES)
def test_morpho_funding_fits_inside_its_own_wallet_cap(recipe: Any) -> None:
    """Native funding is a coin quantity; the cap is dollars.

    On a chain whose gas asset is ETH the float is worth more than the trade, so
    a native amount chosen in coin units silently blows the dollar ceiling and
    `preflight_pool_wallet.build_plan` refuses before the cell can run. All four
    Morpho plans did exactly that. Headroom is demanded, not just a pass, because
    the float is denominated in an asset whose price moves.
    """
    from decimal import Decimal

    native_price = Decimal("2512")  # Robinhood settles gas in ETH
    funded = Decimal(recipe.native_funding) * native_price
    for spec in recipe.funding_tokens:
        symbol, _, amount = spec.partition(":")
        assert symbol in {recipe.asset_symbol, recipe.collateral_symbol}
        funded += Decimal(amount)  # both legs are dollar stables
    cap = Decimal(recipe.total_wallet_cap_usd)
    assert funded <= cap, f"{recipe.intent}: funds ${funded} against a ${cap} cap"
    assert cap / funded >= Decimal("1.4"), f"{recipe.intent}: only {cap / funded:.2f}x headroom on a moving gas asset"
    assert Decimal(recipe.trading_cap_usd) + Decimal(recipe.gas_budget_usd) <= cap


def test_a_terminal_reading_from_before_cleanup_cannot_certify_a_flat_wallet() -> None:
    """Every terminal obligation answers "is the wallet flat NOW".

    A zero read before cleanup -- or before setup -- is a true statement about
    the wrong block, and it certifies a released wallet exactly as convincingly
    as a real one. Only the allowance validator enforced the pin; the position
    and balance validators asked whether a block was pinned at all, which a
    pre-phase observation satisfies.
    """
    from qa_lab.mainnet_intent_envelope import _assert_terminal_pinned

    for stale in (1, 99):
        with pytest.raises(MainnetEnvelopeError, match="not pinned at or after the last phase receipt"):
            _assert_terminal_pinned(
                observation={"block_number": stale},
                last_phase_block=100,
                label="Terminal observation",
            )
    with pytest.raises(MainnetEnvelopeError, match="not pinned at or after the last phase receipt"):
        _assert_terminal_pinned(observation={"block_number": 0}, last_phase_block=0, label="Terminal observation")
    # Liveness: the gate is not simply refusing everything.
    for fresh in (100, 101):
        _assert_terminal_pinned(
            observation={"block_number": fresh}, last_phase_block=100, label="Terminal observation"
        )


def test_the_terminal_floor_is_the_latest_block_any_phase_reached() -> None:
    """A floor taken from the wrong phase lets a mid-run reading through.

    Cleanup is the last thing that moves value, so a floor computed from setup
    or target alone would admit an observation taken before the unwind.
    """
    from qa_lab.mainnet_intent_envelope import _last_phase_block

    def _payload(block: int) -> tuple[dict[str, Any], dict[str, Any]]:
        return ({}, {"raw_receipt": {"blockNumber": block}})

    assert _last_phase_block({"setup": [_payload(10)], "target": [_payload(20)], "cleanup": [_payload(30)]}) == 30
    assert _last_phase_block({"setup": [_payload(30)], "cleanup": [_payload(10)]}) == 30
    assert _last_phase_block({}) == 0
