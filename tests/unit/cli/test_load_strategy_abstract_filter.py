"""Test that load_strategy_from_file skips abstract base classes.

VIB-2846: Strategies importing StatelessStrategy caused the loader to pick up
the abstract base class (alphabetically first) instead of the concrete subclass.

Tests cover both loader implementations:
- almanak.framework.cli.intent_debug.load_strategy_from_file
- almanak.framework.cli.teardown.load_strategy_from_file
"""

import textwrap
from pathlib import Path

import pytest

from almanak.framework.cli.intent_debug import (
    load_strategy_from_file as intent_debug_loader,
)
from almanak.framework.cli.teardown import (
    load_strategy_from_file as teardown_loader,
)

STRATEGY_WITH_ABSTRACT_IMPORT = """\
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import StatelessStrategy
from almanak.framework.intents import Intent

class ZebraStrategy(StatelessStrategy):
    \"\"\"Concrete strategy whose name sorts AFTER StatelessStrategy.\"\"\"

    def decide(self, market: MarketSnapshot) -> Intent:
        return Intent.hold(reason="test")
"""

STRATEGY_WITH_MULTIPLE_ABSTRACT_IMPORTS = """\
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, StatelessStrategy
from almanak.framework.intents import Intent

class ConcreteStrategy(StatelessStrategy):
    def decide(self, market: MarketSnapshot) -> Intent:
        return Intent.hold(reason="test")
"""


@pytest.mark.parametrize(
    "loader",
    [intent_debug_loader, teardown_loader],
    ids=["intent_debug", "teardown"],
)
def test_load_skips_abstract_base_class(tmp_path: Path, loader):
    """Loader should return the concrete subclass, not imported abstract bases."""
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(textwrap.dedent(STRATEGY_WITH_ABSTRACT_IMPORT))

    cls, err = loader(strategy_file)
    assert err is None, f"Unexpected error: {err}"
    assert cls is not None
    assert cls.__name__ == "ZebraStrategy", (
        f"Expected ZebraStrategy but got {cls.__name__} — "
        "loader picked up abstract base class instead of concrete subclass"
    )


@pytest.mark.parametrize(
    "loader",
    [intent_debug_loader, teardown_loader],
    ids=["intent_debug", "teardown"],
)
def test_load_skips_all_abstract_classes(tmp_path: Path, loader):
    """Loader should skip any class with unresolved abstract methods."""
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(textwrap.dedent(STRATEGY_WITH_MULTIPLE_ABSTRACT_IMPORTS))

    cls, err = loader(strategy_file)
    assert err is None
    assert cls is not None
    assert cls.__name__ == "ConcreteStrategy"
    # Verify abstract classes were filtered out
    assert not getattr(cls, "__abstractmethods__", frozenset())
