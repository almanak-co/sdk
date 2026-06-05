"""Accounting reporting module — strategy-class-aware PnL reports."""

from .accountant_query import (
    AccountingReportFilter,
    TaxPeriod,
    accountant_report_from_db,
)
from .data_quality import DataQualitySection, build_data_quality
from .lending_report import LendingSection, build_lending_report
from .loader import AccountingData, StrategyClass, load_accounting_data
from .lp_report import LPSection, build_lp_report

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
    "DataQualitySection",
    "build_data_quality",
]


# Re-export AccountantReport so callers don't have to reach across into
# `accountant_test` for the type alone — the reporting API returns it.
from almanak.framework.accounting.accountant_test import AccountantReport  # noqa: E402
