"""Pendle LP / PT category-handler adapters.

The accounting bodies were relocated to the connector
(``almanak.connectors.pendle.accounting_spec``) under VIB-4931; the framework no
longer carries Pendle-specific logic. These thin ``@register`` adapters keep the
legacy ``PENDLE_LP`` / ``PENDLE_PT`` dispatch live by routing each event through the
strategy-side :class:`AccountingTreatmentRegistry` to the connector's published
treatment — the import here is the *generic* ``_strategy_base`` registry, so no
connector is named in the framework. This whole module is removed once the generic
dispatcher consumes the registry directly (VIB-4931 PR-B), along with the
``PENDLE_LP`` / ``PENDLE_PT`` enum members it registers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.accounting_treatment_registry import (
    AccountingTreatmentRegistry,
)
from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory

if TYPE_CHECKING:
    from almanak.framework.accounting.models import PendleAccountingEvent


@register(AccountingCategory.PENDLE_LP)
def _dispatch_pendle_lp(ctx: HandlerContext) -> PendleAccountingEvent | None:
    treat = AccountingTreatmentRegistry.treatment_for("pendle_lp")
    return treat(ctx) if treat is not None else None


@register(AccountingCategory.PENDLE_PT)
def _dispatch_pendle_pt(ctx: HandlerContext) -> PendleAccountingEvent | None:
    treat = AccountingTreatmentRegistry.treatment_for("pendle_pt")
    return treat(ctx) if treat is not None else None
