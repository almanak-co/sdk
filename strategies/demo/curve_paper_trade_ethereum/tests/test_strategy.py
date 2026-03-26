"""Unit tests for Curve 3pool paper trade strategy (VIB-1856)."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from strategies.demo.curve_paper_trade_ethereum import CurvePaperTradeStrategy


def _make_strategy(**overrides) -> CurvePaperTradeStrategy:
    with patch.object(CurvePaperTradeStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = CurvePaperTradeStrategy.__new__(CurvePaperTradeStrategy)

    strategy._strategy_id = "test-curve-paper"
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x" + "ab" * 20
    strategy.pool = "3pool"
    strategy.deposit_token = "USDC"
    strategy.deposit_amount = Decimal("100")
    strategy.hold_ticks = 3
    strategy._has_position = False
    strategy._lp_token_balance = Decimal("0")
    strategy._ticks_held = 0
    strategy._cycles_completed = 0
    strategy._lp_token_address = None
    strategy._current_intent = None

    for k, v in overrides.items():
        setattr(strategy, k, v)

    return strategy


def _mock_market(balance: Decimal = Decimal("1000")) -> MagicMock:
    market = MagicMock()
    market.balance.return_value = balance
    return market


def test_opens_lp_when_no_position() -> None:
    """First tick should open LP position."""
    strategy = _make_strategy()
    market = _mock_market()

    intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "LP_OPEN"
    assert intent.protocol == "curve"


def test_holds_during_hold_period() -> None:
    """Should hold for hold_ticks after opening."""
    strategy = _make_strategy(_has_position=True, _ticks_held=0)
    market = _mock_market()

    intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "HOLD"
    assert strategy._ticks_held == 1


def test_closes_after_hold_period() -> None:
    """Should close LP after hold_ticks."""
    strategy = _make_strategy(
        _has_position=True,
        _ticks_held=2,  # Will be incremented to 3 == hold_ticks
        _lp_token_balance=Decimal("96.15"),
    )
    market = _mock_market()

    intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "LP_CLOSE"
    assert intent.protocol == "curve"


def test_usdc_goes_to_amount1() -> None:
    """USDC is index 1 in 3pool, so it should go in amount1."""
    strategy = _make_strategy(deposit_token="USDC", deposit_amount=Decimal("100"))

    intent = strategy._create_open_intent()

    assert intent.amount0 == Decimal("0")
    assert intent.amount1 == Decimal("100")


def test_dai_goes_to_amount0() -> None:
    """DAI is index 0 in 3pool, so it should go in amount0."""
    strategy = _make_strategy(deposit_token="DAI", deposit_amount=Decimal("100"))

    intent = strategy._create_open_intent()

    assert intent.amount0 == Decimal("100")
    assert intent.amount1 == Decimal("0")


def test_on_intent_executed_open_success_via_liquidity() -> None:
    """LP amount comes from extracted_data['liquidity'] (Curve enrichment path)."""
    strategy = _make_strategy()
    intent = MagicMock()
    intent.intent_type.value = "LP_OPEN"
    result = MagicMock()
    # Curve position_id is LP token ADDRESS, not amount
    result.position_id = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
    result.extracted_data = {"liquidity": "96.15"}

    strategy.on_intent_executed(intent, True, result)

    assert strategy._has_position is True
    assert strategy._lp_token_balance == Decimal("96.15")
    assert strategy._lp_token_address == "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
    assert strategy._ticks_held == 0


def test_on_intent_executed_open_success_via_lp_tokens() -> None:
    """Falls back to extracted_data['lp_tokens'] when 'liquidity' is not set."""
    strategy = _make_strategy()
    intent = MagicMock()
    intent.intent_type.value = "LP_OPEN"
    result = MagicMock()
    result.extracted_data = {"lp_tokens": "98.50"}

    strategy.on_intent_executed(intent, True, result)

    assert strategy._has_position is True
    assert strategy._lp_token_balance == Decimal("98.50")


def test_on_intent_executed_open_no_enrichment_stays_zero() -> None:
    """When enrichment fails, LP balance stays zero (fail closed, no fabrication)."""
    strategy = _make_strategy()
    intent = MagicMock()
    intent.intent_type.value = "LP_OPEN"
    result = MagicMock()
    result.extracted_data = {}

    strategy.on_intent_executed(intent, True, result)

    assert strategy._has_position is True
    assert strategy._lp_token_balance == Decimal("0")


def test_on_intent_executed_close_success() -> None:
    """Successful LP_CLOSE should reset state and increment cycle."""
    strategy = _make_strategy(
        _has_position=True,
        _lp_token_balance=Decimal("96.15"),
        _ticks_held=3,
    )
    intent = MagicMock()
    intent.intent_type.value = "LP_CLOSE"

    strategy.on_intent_executed(intent, True, MagicMock())

    assert strategy._has_position is False
    assert strategy._lp_token_balance == Decimal("0")
    assert strategy._cycles_completed == 1


def test_persistent_state_roundtrip() -> None:
    """State should survive save/load cycle."""
    strategy = _make_strategy(
        _has_position=True,
        _lp_token_balance=Decimal("96.15"),
        _lp_token_address="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
        _ticks_held=2,
        _cycles_completed=1,
    )

    state = strategy.get_persistent_state()

    strategy2 = _make_strategy()
    strategy2.load_persistent_state(state)

    assert strategy2._has_position is True
    assert strategy2._lp_token_balance == Decimal("96.15")
    assert strategy2._lp_token_address == "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
    assert strategy2._ticks_held == 2
    assert strategy2._cycles_completed == 1


def test_teardown_generates_close_when_has_position() -> None:
    """Teardown should generate LP_CLOSE when position exists."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy(
        _has_position=True,
        _lp_token_balance=Decimal("96.15"),
    )

    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

    assert len(intents) == 1
    assert intents[0].intent_type.value == "LP_CLOSE"


def test_teardown_empty_when_no_position() -> None:
    """Teardown should return empty list when no position."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy()
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert intents == []


def test_hold_when_insufficient_balance() -> None:
    """Should hold when balance is insufficient."""
    strategy = _make_strategy(deposit_amount=Decimal("1000"))
    market = _mock_market(balance=Decimal("50"))

    intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "HOLD"
    assert "Insufficient" in intent.reason


def test_usdt_deposit_rejected() -> None:
    """USDT (index 2) is not supported because LPOpenIntent only has amount0/amount1."""
    import pytest

    strategy = _make_strategy(deposit_token="USDT")

    with pytest.raises(ValueError, match="Unsupported deposit_token"):
        strategy._create_open_intent()


def test_hold_when_balance_check_fails() -> None:
    """Should hold (fail closed) when balance check throws an exception."""
    strategy = _make_strategy()
    market = MagicMock()
    market.balance.side_effect = ValueError("gateway unavailable")

    intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "HOLD"
    assert "balance" in intent.reason.lower()


def test_close_uses_address_when_balance_zero_but_address_known() -> None:
    """Falls back to LP token address when balance is zero.

    The Curve compiler accepts 0x-prefixed LP token addresses and queries
    the on-chain balance automatically, so the strategy can still exit.
    """
    strategy = _make_strategy(
        _has_position=True,
        _lp_token_balance=Decimal("0"),
        _lp_token_address="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
    )

    intent = strategy._create_close_intent()

    assert intent.intent_type.value == "LP_CLOSE"
    assert intent.position_id == "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"


def test_close_holds_when_no_balance_and_no_address() -> None:
    """Should hold when neither LP balance nor LP token address is available."""
    strategy = _make_strategy(
        _has_position=True,
        _lp_token_balance=Decimal("0"),
        _lp_token_address=None,
    )

    intent = strategy._create_close_intent()

    assert intent.intent_type.value == "HOLD"
    assert "available" in intent.reason.lower()
