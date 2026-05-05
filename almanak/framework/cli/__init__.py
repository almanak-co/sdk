"""CLI tools for Almanak Strategy Framework.

Click commands are resolved lazily via :pep:`562` ``__getattr__`` so that
importing a sibling module (notably ``framework.cli.chain_resolution`` from
the sweep-worker subprocess hot path — see #1703) does not transitively
load every CLI command + its deps. Each command pulls in heavy framework
state (gateway proto stubs, price aggregators, RSI providers, etc.); the
top-level ``almanak`` CLI dispatcher in ``almanak/cli/cli.py`` imports each
specific command by name, which still works through the lazy dispatch.
"""

from typing import TYPE_CHECKING

from almanak._lazy import LazySpec, build_lazy_module_dispatch

if TYPE_CHECKING:
    from .backtest import backtest
    from .check import check
    from .demo import demo
    from .intent_debug import inspect, intent_group, trace
    from .new_protocol import new_protocol
    from .new_strategy import new_strategy
    from .qa_data import qa_data
    from .replay import replay
    from .status import list_strategies, strategy_logs, strategy_status
    from .teardown import teardown


# Maps each public name to (relative submodule, attribute name on that submodule).
_LAZY_IMPORTS: dict[str, LazySpec] = {
    "backtest": (".backtest", "backtest"),
    "check": (".check", "check"),
    "demo": (".demo", "demo"),
    "inspect": (".intent_debug", "inspect"),
    "intent_group": (".intent_debug", "intent_group"),
    "trace": (".intent_debug", "trace"),
    "new_protocol": (".new_protocol", "new_protocol"),
    "new_strategy": (".new_strategy", "new_strategy"),
    "qa_data": (".qa_data", "qa_data"),
    "replay": (".replay", "replay"),
    "list_strategies": (".status", "list_strategies"),
    "strategy_logs": (".status", "strategy_logs"),
    "strategy_status": (".status", "strategy_status"),
    "teardown": (".teardown", "teardown"),
}

__all__ = [*sorted(_LAZY_IMPORTS)]

__getattr__, __dir__ = build_lazy_module_dispatch(_LAZY_IMPORTS, package=__name__, namespace=globals())
