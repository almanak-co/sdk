"""Truth table for LP position-event token-pair evidence precedence."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors._strategy_base.primitive_money_leg import (
    PrimitiveMoneyLeg,
    PrimitiveMoneyLegs,
)
from almanak.framework.accounting.measured import MeasuredMoney
from almanak.framework.observability import position_events
from almanak.framework.observability.position_events import IntentEventContext, PositionEvent

LABEL_PAIR = ("LABEL0", "LABEL1")
PARSER_PAIR = ("PARSER0", "PARSER1")
DECLARED_PAIR = ("DECLARED0", "DECLARED1")
ADDRESS_PAIR = ("ADDRESS0", "ADDRESS1")

_PARSER_OUTCOMES = {
    "resolved": PARSER_PAIR,
    "unresolved": ("", ""),
    "absent": None,
    "partial": None,
}

_PRECEDENCE_CASES = (
    ("resolved", "valid", PARSER_PAIR, False),
    ("resolved", "unusable", PARSER_PAIR, False),
    ("resolved", "absent", PARSER_PAIR, False),
    ("unresolved", "valid", DECLARED_PAIR, False),
    ("unresolved", "unusable", LABEL_PAIR, False),
    ("unresolved", "absent", LABEL_PAIR, False),
    ("absent", "valid", DECLARED_PAIR, False),
    ("absent", "unusable", LABEL_PAIR, False),
    ("absent", "absent", ADDRESS_PAIR, True),
    ("partial", "valid", DECLARED_PAIR, False),
    ("partial", "unusable", LABEL_PAIR, False),
    ("partial", "absent", ADDRESS_PAIR, True),
)


def _event(**overrides: Any) -> PositionEvent:
    values = {
        "id": "event-1",
        "deployment_id": "deployment-1",
        "position_id": "position-1",
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": datetime(2026, 9, 3, tzinfo=UTC),
        "protocol": "test_lp",
        "chain": "event-chain",
        "token0": LABEL_PAIR[0],
        "token1": LABEL_PAIR[1],
        # A measured zero remains evidence and must not short-circuit the ladder.
        "amount0": "0",
        "amount1": "1",
        "liquidity": "123",
        "ledger_entry_id": "ledger-1",
    }
    return PositionEvent(**(values | overrides))


def _context(extracted: Any, *, chain: Any = "context-chain") -> IntentEventContext:
    return IntentEventContext(
        intent=SimpleNamespace(),
        result=None,
        extracted=extracted,
        deployment_id="deployment-1",
        chain=chain,
        ledger_entry_id="ledger-1",
    )


def _lp_data_key(opening: bool) -> str:
    return "lp_open_data" if opening else "lp_close_data"


@pytest.mark.parametrize("opening", [True, False], ids=["open", "close"])
@pytest.mark.parametrize(
    ("parser_state", "legs_state", "expected_pair", "address_called"),
    _PRECEDENCE_CASES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_pair_precedence_truth_table(
    monkeypatch: pytest.MonkeyPatch,
    opening: bool,
    parser_state: str,
    legs_state: str,
    expected_pair: tuple[str, str],
    address_called: bool,
) -> None:
    calls: list[tuple[str, Any]] = []

    def parser_pair(lp_data: dict[str, str], chain: str) -> tuple[str, str] | None:
        calls.append(("parser", (lp_data["parser_state"], chain)))
        return _PARSER_OUTCOMES[lp_data["parser_state"]]

    def declared_pair(extracted: dict[str, Any], *, opening: bool = False) -> tuple[str | None, str | None]:
        calls.append(("declared", opening))
        if extracted["legs_state"] == "valid":
            return DECLARED_PAIR
        return (DECLARED_PAIR[0], None)

    def address_pair(token0: str, token1: str, chain: str) -> tuple[str, str]:
        calls.append(("address", (token0, token1, chain)))
        return ADDRESS_PAIR

    monkeypatch.setattr(position_events, "_pair_tokens_from_parser_currencies", parser_pair)
    monkeypatch.setattr(position_events, "_pair_tokens_from_declared_legs", declared_pair)
    monkeypatch.setattr(
        "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
        address_pair,
    )

    extracted: dict[str, Any] = {
        _lp_data_key(opening): {"parser_state": parser_state},
        "legs_state": legs_state,
    }
    if legs_state != "absent":
        extracted["primitive_money_legs"] = object()

    event = _event(event_type="OPEN" if opening else "CLOSE")
    before = event.to_dict()
    result = position_events._realign_event_lp_pair_if_needed(event, _context(extracted), opening=opening)

    assert result is None
    assert event.to_dict() == before | {"token0": expected_pair[0], "token1": expected_pair[1]}
    assert any(name == "address" for name, _ in calls) is address_called
    assert [(name, value) for name, value in calls if name == "declared"] == (
        [("declared", opening)] if parser_state != "resolved" and legs_state != "absent" else []
    )


@pytest.mark.parametrize("opening", [True, False], ids=["open", "close"])
def test_lifecycle_role_selects_its_own_lp_payload(monkeypatch: pytest.MonkeyPatch, opening: bool) -> None:
    open_pair = ("OPEN0", "OPEN1")
    close_pair = ("CLOSE0", "CLOSE1")
    open_data = {"pair": open_pair}
    close_data = {"pair": close_pair}

    monkeypatch.setattr(
        position_events,
        "_pair_tokens_from_parser_currencies",
        lambda lp_data, _chain: lp_data["pair"],
    )

    event = _event(event_type="OPEN" if opening else "CLOSE")
    position_events._realign_event_lp_pair_if_needed(
        event,
        _context({"lp_open_data": open_data, "lp_close_data": close_data}),
        opening=opening,
    )

    assert (event.token0, event.token1) == (open_pair if opening else close_pair)


@pytest.mark.parametrize("opening", [True, False], ids=["open", "close"])
@pytest.mark.parametrize("guard", ["event_coin_symbols", "payload_coin_symbols", "additional_amounts"])
def test_ncoin_guards_outrank_all_pair_sources(
    monkeypatch: pytest.MonkeyPatch,
    opening: bool,
    guard: str,
) -> None:
    monkeypatch.setattr(
        position_events,
        "_pair_tokens_from_parser_currencies",
        lambda *_args, **_kwargs: pytest.fail("N-coin guard must run before parser identity"),
    )

    event = _event(coin_symbols=["LABEL0", "LABEL1", "LABEL2"] if guard == "event_coin_symbols" else None)
    lp_data: dict[str, Any] = {}
    if guard == "payload_coin_symbols":
        lp_data["coin_symbols"] = ["LABEL0", "LABEL1", "LABEL2"]
    elif guard == "additional_amounts":
        lp_data["additional_amounts"] = {2: "0"}
    extracted = {
        _lp_data_key(opening): lp_data,
        "primitive_money_legs": object(),
    }

    assert position_events._realign_event_lp_pair_if_needed(event, _context(extracted), opening=opening) is None
    assert (event.token0, event.token1) == LABEL_PAIR


@pytest.mark.parametrize("opening", [True, False], ids=["open", "close"])
def test_partial_parser_identity_keeps_legacy_full_pair_inference_alm3510(
    monkeypatch: pytest.MonkeyPatch,
    opening: bool,
) -> None:
    monkeypatch.setattr(
        "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
        lambda *_args: ADDRESS_PAIR,
    )
    lp_data = SimpleNamespace(currency0="observed-slot-0", currency1=None)
    event = _event(event_type="OPEN" if opening else "CLOSE")

    position_events._realign_event_lp_pair_if_needed(
        event,
        _context({_lp_data_key(opening): lp_data}),
        opening=opening,
    )

    assert (event.token0, event.token1) == ADDRESS_PAIR


@pytest.mark.parametrize("bad_value", [None, "", "   ", 0, 1, Decimal("0"), object()])
@pytest.mark.parametrize("field", ["token0", "token1", "amount0", "amount1"])
def test_malformed_or_empty_event_pair_fields_fail_open_without_mutation(field: str, bad_value: Any) -> None:
    event = _event(**{field: bad_value})
    original_pair = (event.token0, event.token1)

    assert position_events._realign_event_lp_pair_if_needed(event, _context({}), opening=True) is None
    assert (event.token0, event.token1) == original_pair


@pytest.mark.parametrize("malformed_extracted", [None, "bad", [], object()])
def test_malformed_extracted_payload_uses_final_address_fallback(
    monkeypatch: pytest.MonkeyPatch,
    malformed_extracted: Any,
) -> None:
    monkeypatch.setattr(
        "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
        lambda *_args: ADDRESS_PAIR,
    )
    event = _event()

    position_events._realign_event_lp_pair_if_needed(event, _context(malformed_extracted), opening=True)

    assert (event.token0, event.token1) == ADDRESS_PAIR


@pytest.mark.parametrize("malformed_lp_data", ["bad", [], object(), {"currency0": object(), "currency1": object()}])
def test_malformed_lp_payload_does_not_raise_or_fabricate_identity(
    monkeypatch: pytest.MonkeyPatch,
    malformed_lp_data: Any,
) -> None:
    monkeypatch.setattr(
        "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
        lambda *_args: ADDRESS_PAIR,
    )
    event = _event()

    position_events._realign_event_lp_pair_if_needed(
        event,
        _context({"lp_open_data": malformed_lp_data}),
        opening=True,
    )

    expected = LABEL_PAIR if isinstance(malformed_lp_data, dict) else ADDRESS_PAIR
    assert (event.token0, event.token1) == expected


@pytest.mark.parametrize("malformed_legs", [False, 0, "", {}, [], object()])
def test_malformed_declared_legs_block_weaker_address_inference(
    monkeypatch: pytest.MonkeyPatch,
    malformed_legs: Any,
) -> None:
    monkeypatch.setattr(
        "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
        lambda *_args: pytest.fail("A present declared-leg carrier must be terminal"),
    )
    event = _event()

    position_events._realign_event_lp_pair_if_needed(
        event,
        _context({"lp_open_data": {}, "primitive_money_legs": malformed_legs}),
        opening=True,
    )

    assert (event.token0, event.token1) == LABEL_PAIR


@pytest.mark.parametrize(
    ("context_chain", "event_chain", "expected_chain"),
    [
        ("context-chain", "event-chain", "context-chain"),
        ("", "event-chain", "event-chain"),
        (1, "event-chain", "event-chain"),
    ],
)
def test_chain_precedence_is_context_then_event_then_empty(
    monkeypatch: pytest.MonkeyPatch,
    context_chain: Any,
    event_chain: str,
    expected_chain: str,
) -> None:
    seen: list[str] = []

    def address_pair(token0: str, token1: str, chain: str) -> tuple[str, str]:
        seen.append(chain)
        return (token0, token1)

    monkeypatch.setattr(
        "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
        address_pair,
    )

    position_events._realign_event_lp_pair_if_needed(
        _event(chain=event_chain),
        _context({}, chain=context_chain),
        opening=True,
    )

    assert seen == [expected_chain]


@pytest.mark.parametrize("opening", [True, False], ids=["open", "close"])
def test_real_declared_pair_uses_the_role_matching_the_lifecycle(opening: bool) -> None:
    input_legs = (
        PrimitiveMoneyLeg.input("INPUT0", MeasuredMoney.measured(Decimal("1"))),
        PrimitiveMoneyLeg.input("INPUT1", MeasuredMoney.measured(Decimal("2"))),
    )
    output_legs = (
        PrimitiveMoneyLeg.output("OUTPUT0", MeasuredMoney.measured(Decimal("3"))),
        PrimitiveMoneyLeg.output("OUTPUT1", MeasuredMoney.measured(Decimal("4"))),
    )
    event = _event(event_type="OPEN" if opening else "CLOSE")

    position_events._realign_event_lp_pair_if_needed(
        event,
        _context(
            {
                _lp_data_key(opening): {},
                "primitive_money_legs": PrimitiveMoneyLegs.of(*input_legs, *output_legs),
            }
        ),
        opening=opening,
    )

    expected_prefix = "INPUT" if opening else "OUTPUT"
    assert (event.token0, event.token1) == (f"{expected_prefix}0", f"{expected_prefix}1")
