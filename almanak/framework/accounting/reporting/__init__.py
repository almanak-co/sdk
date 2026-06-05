"""Accounting reporting module — strategy-class-aware PnL reports."""

from importlib import import_module

from .accountant_query import (
    AccountingReportFilter,
    TaxPeriod,
    accountant_report_from_db,
)
from .data_quality import DataQualitySection, build_data_quality
from .lending_report import LendingSection, build_lending_report
from .loader import AccountingData, StrategyClass, load_accounting_data
from .lp_report import LPSection, build_lp_report

_PENDLE_COMPAT_EXPORTS = frozenset({"PendleSection", "build_pendle_report"})

__all__ = [
    "AccountantReport",  # re-exported below
    "AccountingData",
    "AccountingReportFilter",
    "StrategyClass",
    "TaxPeriod",
    "accountant_report_from_db",
    "load_accounting_data",
    "LPSection",
    "build_lp_report",
    "LendingSection",
    "build_lending_report",
    "PendleSection",
    "build_pendle_report",
    "DataQualitySection",
    "build_data_quality",
]


def __getattr__(name: str) -> object:
    """Lazily preserve legacy Pendle report exports without eager central imports."""
    if name in _PENDLE_COMPAT_EXPORTS:
        module = import_module("almanak.framework.accounting.reporting.pendle_report")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Re-export AccountantReport so callers don't have to reach across into
# `accountant_test` for the type alone — the reporting API returns it.
from almanak.framework.accounting.accountant_test import AccountantReport  # noqa: E402
