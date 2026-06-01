"""Category-handler registry for AccountingProcessor (VIB-4163, T3).

Replaces the if-ladder at ``processor.py:_dispatch`` with an explicit dict keyed
by ``AccountingCategory``. Each handler module registers itself at module-load
time via ``@register(category)``.

The registry is populated **eagerly** when this package is imported: every
handler module is imported below, which runs the ``@register`` decorator and
appends to ``HANDLERS``. This avoids the lazy-import-inside-_dispatch pattern
while remaining free of import cycles (the cycle-avoidance proof is in
``tests/unit/accounting/test_category_handler_registry.py::test_registry_imports_in_clean_subprocess``).

Handlers are free to defer expensive imports inside their function bodies if
needed; **registration** runs at module load, but handler **execution** can
still import as needed.

Public API:

* ``HANDLERS`` — ``dict[AccountingCategory, HandlerFn]`` populated at package
  load. The ``AccountingProcessor._dispatch`` method does
  ``HANDLERS.get(category)`` to find the right handler.
* ``HandlerContext`` — frozen dataclass passed to every handler. Carries the
  outbox/ledger rows and the helpers the legacy ladder used (basis store,
  prior-LP-open lookup).
* ``register(category)`` — decorator a handler module uses to bind itself to
  an ``AccountingCategory``. Raises ``ValueError`` if the category is already
  registered (no silent shadowing).

The legacy public functions (``handle_lp``, ``handle_lending``, …) remain
unchanged in their respective handler modules so existing tests that import
them directly continue to work. The registry adapter is a thin wrapper
co-located with each legacy handler.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from almanak.framework.primitives.types import AccountingCategory

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore


@dataclass(frozen=True)
class HandlerContext:
    """Inputs threaded into every category handler.

    ``frozen=True`` so handlers cannot reassign attributes (the rows themselves
    are dicts and remain mutable; the freeze guards the bindings, not deep
    state — this is documented for the cycle-test reviewer in the UAT card's
    D3.F4 step).
    """

    outbox_row: dict[str, Any]
    ledger_row: dict[str, Any]
    basis_store: FIFOBasisStore
    # VIB-4275 — ``(position_key, discriminator)`` → prior LP_OPEN payload.
    # ``discriminator`` is the closing leg's per-position id (NFT token id) used
    # to disambiguate co-pool opens; ``None`` resolves only the single-open
    # legacy case. The resolver NEVER falls back to "latest open" under
    # ambiguity (it returns ``None``).
    prior_open_lookup: Callable[[str, str | None], dict[str, Any] | None]


HandlerFn = Callable[[HandlerContext], Any]


HANDLERS: dict[AccountingCategory, HandlerFn] = {}


def register(category: AccountingCategory) -> Callable[[HandlerFn], HandlerFn]:
    """Bind a handler function to ``category`` in :data:`HANDLERS`.

    Raises ``ValueError`` (loud, at import time) when the category already has
    a registered handler. Two modules cannot silently shadow each other —
    the package will fail to load and name both registrants. This is the D3.F2
    "no silent shadow" guard.
    """

    def _decorator(fn: HandlerFn) -> HandlerFn:
        if category in HANDLERS:
            existing = HANDLERS[category]
            existing_qualname = f"{existing.__module__}.{getattr(existing, '__qualname__', existing.__name__)}"
            new_qualname = f"{fn.__module__}.{getattr(fn, '__qualname__', fn.__name__)}"
            msg = (
                f"AccountingCategory.{category.name} already registered to {existing_qualname}; "
                f"refusing to shadow with {new_qualname} (VIB-4163 D3.F2 guard)."
            )
            raise ValueError(msg)
        HANDLERS[category] = fn
        return fn

    return _decorator


# Eager imports populate HANDLERS via @register at module load.
# Order is alphabetical for diff-friendliness; cycle-safety is enforced by
# tests/unit/accounting/test_category_handler_registry.py::test_registry_imports_in_clean_subprocess.
# (The handlers are imported AFTER HANDLERS / HandlerContext / register are
# defined above so the decorator has the registry ready by the time it runs.)
#
# DO NOT remove the unused-looking imports below — they are side-effect-only
# (the @register decorator runs at module load). The startup assertion below
# fails the import if a future linter "tidies" the list and silently drops a
# handler.
from almanak.framework.accounting.category_handlers import (  # noqa: E402, F401
    lending_handler,
    lp_handler,
    pendle_handler,
    perp_handler,
    prediction_handler,
    swap_handler,
    transfer_handler,
    vault_handler,
)

# Startup assertion — fail loud at import time rather than degrade silently
# at runtime. Every ``AccountingCategory`` except ``NO_ACCOUNTING`` must have
# a registered handler after eager imports. A missing entry typically means
# (a) someone removed an unused-looking import above, or (b) a handler module
# raised silently during init (the duplicate-registration ValueError would
# already have surfaced loudly at the @register call).
_required = set(AccountingCategory) - {AccountingCategory.NO_ACCOUNTING}
_missing = _required - HANDLERS.keys()
if _missing:  # pragma: no cover — covered by D3.F3 exhaustiveness test
    raise RuntimeError(
        f"category_handlers registry under-populated at import time. "
        f"Missing: {sorted(c.value for c in _missing)}. "
        f"Did a `# noqa: F401` import get tidied away in __init__.py?"
    )

__all__ = ["HANDLERS", "HandlerContext", "HandlerFn", "register"]
