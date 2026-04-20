"""CLI tools for Almanak Strategy Framework."""

from .backtest import backtest
from .demo import demo
from .intent_debug import inspect, intent_group, trace
from .new_protocol import new_protocol
from .new_strategy import new_strategy
from .qa_data import qa_data
from .replay import replay
from .status import list_strategies, strategy_logs, strategy_status
from .teardown import teardown

__all__ = [
    "backtest",
    "demo",
    "list_strategies",
    "new_strategy",
    "new_protocol",
    "qa_data",
    "replay",
    "strategy_logs",
    "strategy_status",
    "teardown",
    "intent_group",
    "inspect",
    "trace",
]
