"""ALM-2807 Layer 2: a successful teardown LP_CLOSE must reset the demo state.

The TraderJoe crisis/pnl LP demos drive their LP state machine from
``on_intent_executed``. The decide() loop transitions ``active -> closing``
before a rebalance close, but **teardown** emits its LP_CLOSE while the state is
still ``"active"`` (it bypasses decide()). Previously the success handler keyed
the ``-> idle`` reset on ``_state == "closing"``, so a teardown close left the
position phantom-open (``get_open_positions`` reports for ``_state in
("active","opening")``), which failed post-teardown verification.
"""

from __future__ import annotations

import pytest

from almanak.demo_strategies.traderjoe_crisis_lp.strategy import TraderJoeCrisisLPStrategy
from almanak.demo_strategies.traderjoe_pnl_lp.strategy import TraderJoePnLLPStrategy

_WALLET = "0x1234567890123456789012345678901234567890"


class _FakeIntent:
    """Minimal stand-in: on_intent_executed only reads intent.intent_type.value."""

    def __init__(self, intent_type: str) -> None:
        self.intent_type = type("_T", (), {"value": intent_type})()


def _make(strategy_class):
    return strategy_class(config={}, chain="avalanche", wallet_address=_WALLET)


STRATEGIES = [TraderJoeCrisisLPStrategy, TraderJoePnLLPStrategy]


@pytest.mark.parametrize("strategy_class", STRATEGIES)
def test_teardown_close_while_active_resets_to_idle(strategy_class):
    strat = _make(strategy_class)
    # Teardown fires the close while the position is "active".
    strat._state = "active"
    strat._position_bin_ids = [1, 2, 3]
    rebalances_before = strat._rebalance_count

    strat.on_intent_executed(_FakeIntent("LP_CLOSE"), success=True, result=None)

    assert strat._state == "idle"
    assert strat._position_bin_ids == []
    # Teardown is not a rebalance — the rebalance counter must not move.
    assert strat._rebalance_count == rebalances_before
    # And the position is no longer reported for teardown.
    assert strat.get_open_positions().positions == []


@pytest.mark.parametrize("strategy_class", STRATEGIES)
def test_rebalance_close_still_resets_and_counts(strategy_class):
    strat = _make(strategy_class)
    # decide()-driven rebalance close path.
    strat._state = "closing"
    strat._position_bin_ids = [4, 5]
    rebalances_before = strat._rebalance_count

    strat.on_intent_executed(_FakeIntent("LP_CLOSE"), success=True, result=None)

    assert strat._state == "idle"
    assert strat._position_bin_ids == []
    assert strat._rebalance_count == rebalances_before + 1


@pytest.mark.parametrize("strategy_class", STRATEGIES)
def test_failed_teardown_close_preserves_open_position(strategy_class):
    strat = _make(strategy_class)
    strat._state = "active"
    strat._position_bin_ids = [1, 2, 3]

    strat.on_intent_executed(_FakeIntent("LP_CLOSE"), success=False, result=None)

    # A failed close must leave the position open so teardown retries / reports it.
    assert strat._state == "active"
    assert strat.get_open_positions().positions != []


@pytest.mark.parametrize("strategy_class", STRATEGIES)
def test_open_sets_active(strategy_class):
    strat = _make(strategy_class)
    strat._state = "opening"

    strat.on_intent_executed(_FakeIntent("LP_OPEN"), success=True, result=None)

    assert strat._state == "active"
