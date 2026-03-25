"""Teardown position discovery tests for AaveBorrowStrategy."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from strategies.demo.aave_borrow import AaveBorrowStrategy


def _make_strategy(*, with_gateway: bool = False) -> AaveBorrowStrategy:
    with patch.object(AaveBorrowStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = AaveBorrowStrategy.__new__(AaveBorrowStrategy)

    strategy._strategy_id = "test-aave-borrow"
    strategy._chain = "arbitrum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy.collateral_token = "WETH"
    strategy.borrow_token = "USDC"
    strategy.interest_rate_mode = "variable"
    strategy._supplied_amount = Decimal("0.5")
    strategy._borrowed_amount = Decimal("500")
    strategy._compiler = None

    if with_gateway:
        mock_client = MagicMock()
        mock_compiler = MagicMock()
        mock_compiler._gateway_client = mock_client
        strategy._compiler = mock_compiler

    return strategy


def test_get_open_positions_falls_back_to_internal_state_without_gateway() -> None:
    """When no gateway client is available, uses internal state tracking."""
    strategy = _make_strategy(with_gateway=False)
    summary = strategy.get_open_positions()

    assert summary.strategy_id == strategy.STRATEGY_NAME
    assert len(summary.positions) == 2
    assert {p.position_type.value for p in summary.positions} == {"SUPPLY", "BORROW"}


def test_get_open_positions_prefers_onchain_results_when_gateway_available() -> None:
    """When gateway is available and on-chain query succeeds, uses on-chain data."""
    strategy = _make_strategy(with_gateway=True)

    fake_positions = [
        MagicMock(
            position_type=MagicMock(value="SUPPLY"),
            position_id=f"aave-supply-WETH-{strategy.chain}",
        ),
        MagicMock(
            position_type=MagicMock(value="BORROW"),
            position_id=f"aave-borrow-USDC-{strategy.chain}",
            value_usd=Decimal("300"),
        ),
    ]

    with patch.object(strategy, "_query_aave_positions_via_gateway", return_value=fake_positions):
        summary = strategy.get_open_positions()

    assert len(summary.positions) == 2
    ids = {p.position_id for p in summary.positions}
    assert f"aave-supply-WETH-{strategy.chain}" in ids
    assert f"aave-borrow-USDC-{strategy.chain}" in ids


def test_get_open_positions_falls_back_on_gateway_exception() -> None:
    """When gateway query raises, falls back to internal state gracefully."""
    strategy = _make_strategy(with_gateway=True)

    with patch.object(strategy, "_query_aave_positions_via_gateway", side_effect=ConnectionError("RPC timeout")):
        summary = strategy.get_open_positions()

    # Should use internal state fallback
    assert len(summary.positions) == 2
    assert {p.position_type.value for p in summary.positions} == {"SUPPLY", "BORROW"}


def test_get_open_positions_returns_empty_when_onchain_shows_no_positions() -> None:
    """On-chain query succeeds but finds no positions -- returns empty (authoritative)."""
    strategy = _make_strategy(with_gateway=True)

    with patch.object(strategy, "_query_aave_positions_via_gateway", return_value=[]):
        summary = strategy.get_open_positions()

    # On-chain is authoritative -- should NOT fall back to stale internal state
    assert len(summary.positions) == 0
    assert summary.total_value_usd == Decimal("0")


def test_get_open_positions_returns_none_falls_back() -> None:
    """When gateway query returns None (unsupported chain), falls back to internal state."""
    strategy = _make_strategy(with_gateway=True)

    with patch.object(strategy, "_query_aave_positions_via_gateway", return_value=None):
        summary = strategy.get_open_positions()

    # Should fall back to internal state
    assert len(summary.positions) == 2
    assert {p.position_type.value for p in summary.positions} == {"SUPPLY", "BORROW"}


def test_teardown_intents_withdraw_uses_concrete_amount() -> None:
    """Withdraw intent must use a concrete amount, not 'all'.

    The TeardownManager now resolves amount='all' via from_token with token
    fallback. Using a concrete amount with withdraw_all=True remains a robust
    guard against resolver/path regressions (VIB-1851).
    """
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy()

    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

    # Should have: REPAY, WITHDRAW, SWAP
    assert len(intents) == 3

    # The withdraw intent (index 1) must NOT use amount="all"
    withdraw_intent = intents[1]
    assert withdraw_intent.intent_type.value == "WITHDRAW"
    assert withdraw_intent.amount != "all"
    assert withdraw_intent.amount == Decimal("0.5")
    assert withdraw_intent.withdraw_all is True


def test_teardown_intents_supply_only_no_repay() -> None:
    """When only supply exists (no borrow), teardown skips repay."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy()
    strategy._borrowed_amount = Decimal("0")

    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

    # Should have: WITHDRAW, SWAP (no REPAY)
    assert len(intents) == 2
    assert intents[0].intent_type.value == "WITHDRAW"
    assert intents[0].amount == Decimal("0.5")
    assert intents[1].intent_type.value == "SWAP"
    assert intents[1].from_token == "WETH"
