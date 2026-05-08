"""AccountingProcessor intent classifier — maps (intent_type, protocol, token_out) to AccountingCategory.

VIB-4161 (T1): :class:`AccountingCategory` now lives in
``almanak.framework.primitives.types`` and is re-exported here for
back-compat. The :func:`classify` function is kept as the local routing
authority for one release; T2 (VIB-4163 et al.) re-points consumers at
``primitives.taxonomy.classify`` and deletes this local copy.
"""

from __future__ import annotations

from almanak.framework.primitives.types import AccountingCategory

__all__ = ["AccountingCategory", "classify"]


_LENDING_TYPES: frozenset[str] = frozenset({"SUPPLY", "BORROW", "REPAY", "DELEVERAGE", "WITHDRAW"})
_LP_TYPES: frozenset[str] = frozenset({"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"})
_PERP_TYPES: frozenset[str] = frozenset({"PERP_OPEN", "PERP_CLOSE", "PERP_INCREASE", "PERP_DECREASE", "PERP_LIQUIDATE"})
_VAULT_TYPES: frozenset[str] = frozenset(
    {"VAULT_DEPOSIT", "VAULT_WITHDRAW", "VAULT_REDEEM", "VAULT_HARVEST", "VAULT_REALLOCATE"}
)
_PREDICTION_TYPES: frozenset[str] = frozenset({"PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"})
_NO_ACCOUNTING_TYPES: frozenset[str] = frozenset(
    {
        "BRIDGE",
        "HOLD",
        "WRAP_NATIVE",
        "UNWRAP_NATIVE",
        "ENSURE_BALANCE",
        "FLASH_LOAN",
    }
)


def classify(intent_type: str, protocol: str = "", token_out: str = "") -> AccountingCategory:
    """Map (intent_type, protocol, token_out) to AccountingCategory.

    Routing rules (in priority order):
    - no_accounting: BRIDGE, HOLD, WRAP_NATIVE, UNWRAP_NATIVE, ENSURE_BALANCE, FLASH_LOAN
    - lending:  SUPPLY, BORROW, REPAY, DELEVERAGE, WITHDRAW  (any protocol)
    - pendle_lp: LP_OPEN, LP_CLOSE, LP_COLLECT_FEES  with "pendle" in protocol
    - lp:       LP_OPEN, LP_CLOSE, LP_COLLECT_FEES  (non-Pendle protocols)
    - perp:     PERP_OPEN/CLOSE/INCREASE/DECREASE/LIQUIDATE  (any protocol)
    - vault:    VAULT_DEPOSIT/WITHDRAW/REDEEM/HARVEST/REALLOCATE  (any protocol)
    - prediction: PREDICTION_BUY/PREDICTION_SELL/PREDICTION_REDEEM  (any protocol)
    - pendle_pt: SWAP with "pendle" in protocol AND token_out starts with "PT-"
    - swap:     SWAP  (all other cases)
    - no_accounting: unrecognised intent types

    Pendle PT classification uses the "PT-" symbol prefix as a fast-path
    heuristic (correct for all current Pendle markets). A token-registry-based
    check is deferred until VIB-3476.

    Prediction-market routing covers Polymarket BUY/SELL/REDEEM (VIB-3707).
    Prior to VIB-3707 these intents fell through to NO_ACCOUNTING and were
    silently dropped by processor._dispatch — leaving the framework with no
    cost-basis or realized-PnL record on prediction-market trades. The
    handler lives at category_handlers/prediction_handler.py.
    """
    t = intent_type.upper()
    p = protocol.lower()

    if t in _NO_ACCOUNTING_TYPES:
        return AccountingCategory.NO_ACCOUNTING
    if t in _LENDING_TYPES:
        return AccountingCategory.LENDING
    if t in _LP_TYPES:
        return AccountingCategory.PENDLE_LP if "pendle" in p else AccountingCategory.LP
    if t in _PERP_TYPES:
        return AccountingCategory.PERP
    if t in _VAULT_TYPES:
        return AccountingCategory.VAULT
    if t in _PREDICTION_TYPES:
        return AccountingCategory.PREDICTION
    if t == "SWAP":
        if "pendle" in p and token_out.upper().startswith("PT-"):
            return AccountingCategory.PENDLE_PT
        return AccountingCategory.SWAP
    return AccountingCategory.NO_ACCOUNTING
