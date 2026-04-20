"""Cycle-scoped context for correlation ID propagation.

The cycle_id is a UUID4 generated once per StrategyRunner.run_iteration() call.
It propagates through decide -> compile -> execute -> enrich so all forensic
events in a single cycle share one correlation ID.

Strategy authors never see the cycle_id — it is framework-internal.
"""

from contextvars import ContextVar
from uuid import uuid4

_cycle_id_var: ContextVar[str | None] = ContextVar("cycle_id", default=None)


def new_cycle_id() -> str:
    """Generate and set a new cycle_id for the current context."""
    cid = str(uuid4())
    _cycle_id_var.set(cid)
    return cid


def set_cycle_id(cycle_id: str) -> None:
    """Set the cycle_id for the current context."""
    _cycle_id_var.set(cycle_id)


def get_cycle_id() -> str | None:
    """Get the current cycle_id, or None if not in a cycle."""
    return _cycle_id_var.get()


def clear_cycle_id() -> None:
    """Clear the cycle_id at the end of an iteration."""
    _cycle_id_var.set(None)
