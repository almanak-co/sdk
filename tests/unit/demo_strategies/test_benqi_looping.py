"""Unit tests for the benqi_looping demo (archetype #9, leveraged-long-AVAX loop).

Drives the cross-asset state machine in-memory (no chain) with mocked prices and
simulated swap outputs: fail-fast config validation, the
SUPPLY->BORROW->SWAP->UNWRAP->SUPPLY build, the price-driven HF, the
WITHDRAW->WRAP->SWAP->REPAY unwind staircase, and the standalone-supply contract.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.demo_strategies.benqi_looping import BenqiLoopingStrategy

_WALLET = "0x" + "1" * 40
_BASE_CFG = {
    "collateral_token": "AVAX",
    "borrow_token": "USDC",
    "wrapped_native": "WAVAX",
    "initial_collateral": "0.1",
    "target_loops": 2,
    "target_ltv": "0.3",
    "collateral_factor": "0.5",
    "hf_danger": "1.30",
    "hf_unwind_floor": "1.10",
    "swap_slippage": "0.01",
}


def _make(**overrides) -> BenqiLoopingStrategy:
    return BenqiLoopingStrategy(chain="avalanche", wallet_address=_WALLET, config={**_BASE_CFG, **overrides})


def _market(avax_price: float) -> MagicMock:
    m = MagicMock()
    m.price.side_effect = lambda t: Decimal(str(avax_price)) if t in ("AVAX", "WAVAX") else Decimal("1")
    return m


def _fake_result(intent, avax_price: float):
    """Simulate a swap fill: output = input converted at price, minus 1% slippage."""
    if intent.intent_type.value != "SWAP":
        return None
    amount = Decimal(str(intent.amount))
    price = Decimal(str(avax_price))
    if intent.to_token in ("WAVAX", "AVAX"):  # USDC -> WAVAX (build)
        out = amount / price * Decimal("0.99")
    else:  # WAVAX -> USDC (unwind)
        out = amount * price * Decimal("0.99")
    return SimpleNamespace(swap_amounts=SimpleNamespace(amount_out_decimal=out))


def _run(strategy, avax_price: float, max_steps: int = 80):
    market = _market(avax_price)
    for _ in range(max_steps):
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, True, _fake_result(intent, avax_price))
        yield intent.intent_type.value, strategy._state


# --------------------------------------------------------------- fail-fast config


@pytest.mark.parametrize(
    "override,needle",
    [
        ({"target_ltv": "0.6"}, "target_ltv"),  # >= collateral_factor (0.5)
        ({"target_loops": 0}, "target_loops"),
        ({"initial_collateral": "0"}, "initial_collateral"),
        ({"hf_danger": "0.9"}, "hf_danger"),
        ({"hf_unwind_floor": "1.0"}, "hf_unwind_floor"),
        ({"collateral_factor": "1.5"}, "collateral_factor"),
        ({"collateral_token": "USDC"}, "must differ"),  # same as borrow_token
    ],
)
def test_rejects_nonsensical_config(override, needle):
    with pytest.raises(ValueError, match=needle):
        _make(**override)


# ------------------------------------------------------------------- build phase


def test_build_sequence_and_leverage():
    s = _make()
    seq = []
    for intent_type, state in _run(s, avax_price=20.0):
        seq.append(intent_type)
        if state == "levered":
            break
    # Two full loops: each = SUPPLY,BORROW,SWAP,UNWRAP + final SUPPLY closes the loop.
    assert seq[:5] == ["SUPPLY", "BORROW", "SWAP", "UNWRAP_NATIVE", "SUPPLY"]
    assert "WRAP_NATIVE" not in seq  # no wrap in the build phase
    assert s._loops_done == 2
    assert s._debt_usdc > Decimal("0")
    # HF = collateral_usd * cf / debt_usd, healthy buffer above danger.
    hf = s._health_factor(Decimal("20"), Decimal("1"))
    assert hf > Decimal("1.5"), hf


def test_borrow_does_not_bundle_collateral():
    """Standalone SUPPLY before BORROW (VIB-3586): borrow leg carries no collateral."""
    s = _make()
    market = _market(20.0)
    supply = s.decide(market)
    assert supply.intent_type.value == "SUPPLY"
    s.on_intent_executed(supply, True, None)
    borrow = s.decide(market)
    assert borrow.intent_type.value == "BORROW"
    assert borrow.collateral_amount == Decimal("0")
    # 0.1 AVAX * $20 * 0.3 LTV = $6 ... wait: 0.1*20*0.3 = 0.6 USDC
    assert borrow.borrow_amount == Decimal("0.60")


def test_price_unavailable_holds():
    s = _make()
    s.on_intent_executed(s.decide(_market(20.0)), True, None)  # supply -> supplied
    bad = MagicMock()
    bad.price.side_effect = lambda t: Decimal("0")  # non-positive -> unavailable
    intent = s.decide(bad)
    assert intent.intent_type.value == "HOLD"


# ------------------------------------------------------------------ unwind phase


def test_levered_holds_above_danger():
    s = _make()
    for _, state in _run(s, avax_price=20.0):
        if state == "levered":
            break
    intent = s.decide(_market(20.0))  # HF ~1.78 > danger 1.30
    assert intent.intent_type.value == "HOLD"
    assert s._state == "levered"


def test_avax_drop_triggers_unwind_to_flat():
    s = _make()
    for _, state in _run(s, avax_price=20.0):
        if state == "levered":
            break
    assert s._state == "levered"
    # AVAX drops ~30% -> HF (~1.25) crosses hf_danger (1.30) but stays above the
    # unwind floor -> price-driven staircase deleverage proceeds and converges.
    for _it, state in _run(s, avax_price=14.0):
        if state == "complete":
            break
    assert s._state == "complete"
    assert s._debt_usdc <= Decimal("0.05")
    assert s._collateral_avax <= Decimal("0.0001")


# ---------------------------------------------------------------------- teardown


def test_teardown_unwinds_with_wrap_swap_legs():
    s = _make()
    for _, state in _run(s, avax_price=20.0):
        if state == "levered":
            break
    intents = s.generate_teardown_intents(mode=None, market=_market(20.0))
    types = [i.intent_type.value for i in intents]
    # Each staircase round is WITHDRAW -> WRAP -> SWAP -> REPAY; ends with an
    # explicit-amount WITHDRAW (BENQI rejects withdraw_all without a redeem_amount).
    assert types[:4] == ["WITHDRAW", "WRAP_NATIVE", "SWAP", "REPAY"]
    assert types[-1] == "WITHDRAW"
    assert intents[-1].withdraw_all is False
    assert Decimal(str(intents[-1].amount)) > Decimal("0")


def test_teardown_empty_when_flat():
    s = _make()
    assert s.generate_teardown_intents(mode=None, market=_market(20.0)) == []
