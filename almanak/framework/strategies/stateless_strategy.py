"""Stateless strategy base class for strategies that never hold positions.

Use StatelessStrategy instead of IntentStrategy when your strategy is a pure
signal generator, monitor, or alert-only strategy with no open positions to
track. Provides default empty implementations of get_open_positions() and
generate_teardown_intents() so you only need to implement decide().
"""

from .intent_strategy import IntentStrategy


class StatelessStrategy(IntentStrategy):
    """Base class for strategies that never hold positions.

    Use this instead of IntentStrategy when your strategy is a pure signal
    generator, monitor, or alert-only strategy with no open positions to track.
    Provides default empty implementations of get_open_positions() and
    generate_teardown_intents() so you only need to implement decide().
    """

    def get_open_positions(self):
        """Return empty position summary (no positions held)."""
        from ..teardown.models import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.strategy_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        """Return empty list (no positions to close)."""
        return []
