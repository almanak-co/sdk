"""AccountingProcessor intent classifier — re-export of canonical taxonomy.

VIB-4162 (T2): the legacy local frozensets (``_LP_TYPES`` / ``_LENDING_TYPES`` /
``_PERP_TYPES`` / ``_VAULT_TYPES`` / ``_PREDICTION_TYPES`` / ``_NO_ACCOUNTING_TYPES``)
are gone. ``classify`` is re-exported from
:mod:`almanak.framework.primitives.taxonomy` so a static import-identity check
(``classifier.classify is taxonomy.classify``) is the delegation lock.

The ``__all__`` shape is preserved so existing consumers
(``from almanak.framework.accounting.classifier import AccountingCategory, classify``)
keep working without code changes.
"""

from __future__ import annotations

from almanak.framework.primitives.taxonomy import classify  # noqa: F401
from almanak.framework.primitives.types import AccountingCategory

__all__ = ["AccountingCategory", "classify"]
