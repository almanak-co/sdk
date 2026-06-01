"""Offline accounting-repair engines (operator tools).

VIB-4896 — engines here mutate an existing SQLite state DB to backfill rows
broken by historical bugs. They are NOT part of the runner's write path:
they run offline, against a stopped strategy's DB, invoked by an operator
CLI. ``AccountingWriter`` remains the only write path for the *live* runner
(CLAUDE.md §Accounting); these engines exist precisely because the live path
already ran and produced incomplete rows that cannot be re-derived on-chain.
"""

from almanak.framework.accounting.repair.lp_close_repair import (
    LpCloseRepairResult,
    RepairedRow,
    repair_teardown_lp_close,
)

__all__ = [
    "LpCloseRepairResult",
    "RepairedRow",
    "repair_teardown_lp_close",
]
