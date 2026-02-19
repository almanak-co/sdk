"""CLI tools for Almanak Strategy Framework."""

from .backtest import backtest
from .intent_debug import inspect, intent_group, trace
from .new_protocol import new_protocol
from .new_strategy import new_strategy
from .qa_data import qa_data
from .replay import replay
from .teardown import teardown

__all__ = [
    "backtest",
    "new_strategy",
    "new_protocol",
    "qa_data",
    "replay",
    "teardown",
    "intent_group",
    "inspect",
    "trace",
]
