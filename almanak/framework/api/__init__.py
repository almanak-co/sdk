"""Almanak Strategy Framework v2.0 - API Endpoints.

Public names are resolved lazily via :pep:`562` ``__getattr__``. Eager loading
of ``api.actions`` re-enters ``api.strategies.base -> ..intents`` and the
gateway-driven import path triggers that cycle before strategies has a chance
to finish loading. Lazy resolution at this layer means submodules like
``api.timeline`` (heavily used by other framework modules) load without
dragging in ``api.actions``.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .actions import (
        ActionResponse,
        BumpGasRequest,
        CancelTxRequest,
        ConfigUpdateRequest,
        ConfigUpdateResponse,
    )
    from .actions import router as actions_router
    from .health import (
        HealthResponse,
        get_running_strategies,
        register_running_strategy,
        unregister_running_strategy,
        update_strategy_status,
    )
    from .health import router as health_router
    from .teardown import (
        CancelResponse,
        ClosePreviewResponse,
        CloseStartedResponse,
        CloseStatusResponse,
    )
    from .teardown import router as teardown_router
    from .timeline import (
        TimelineEvent,
        TimelineEventType,
        TimelineResponse,
    )
    from .timeline import router as timeline_router


# Maps each public name to (relative module path, attribute name on that module).
# The ``*_router`` aliases all source from ``router`` on their respective
# submodule, hence the explicit attr names.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "ActionResponse": (".actions", "ActionResponse"),
    "BumpGasRequest": (".actions", "BumpGasRequest"),
    "CancelTxRequest": (".actions", "CancelTxRequest"),
    "ConfigUpdateRequest": (".actions", "ConfigUpdateRequest"),
    "ConfigUpdateResponse": (".actions", "ConfigUpdateResponse"),
    "actions_router": (".actions", "router"),
    "HealthResponse": (".health", "HealthResponse"),
    "get_running_strategies": (".health", "get_running_strategies"),
    "register_running_strategy": (".health", "register_running_strategy"),
    "unregister_running_strategy": (".health", "unregister_running_strategy"),
    "update_strategy_status": (".health", "update_strategy_status"),
    "health_router": (".health", "router"),
    "CancelResponse": (".teardown", "CancelResponse"),
    "ClosePreviewResponse": (".teardown", "ClosePreviewResponse"),
    "CloseStartedResponse": (".teardown", "CloseStartedResponse"),
    "CloseStatusResponse": (".teardown", "CloseStatusResponse"),
    "teardown_router": (".teardown", "router"),
    "TimelineEvent": (".timeline", "TimelineEvent"),
    "TimelineEventType": (".timeline", "TimelineEventType"),
    "TimelineResponse": (".timeline", "TimelineResponse"),
    "timeline_router": (".timeline", "router"),
}

__all__ = [*sorted(_LAZY_IMPORTS)]


def __getattr__(name: str) -> object:
    import importlib

    if name in _LAZY_IMPORTS:
        rel_module, attr_name = _LAZY_IMPORTS[name]
        module = importlib.import_module(rel_module, package=__name__)
        attr = getattr(module, attr_name)
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
