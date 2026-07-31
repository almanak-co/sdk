"""Tests for the signing-topology and strategy-run mode boundary."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.config.runtime import ExecutionMode as LegacySigningMode
from almanak.config.runtime import SigningMode
from almanak.framework.accounting.models import AccountingIdentity
from almanak.framework.models.run_mode import RunMode
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.observability.position_events import PositionEvent
from almanak.framework.portfolio.models import PortfolioMetrics, PortfolioSnapshot
from almanak.framework.runner.strategy_runner import ExecutionMode as LegacyRunMode


def test_signing_and_run_modes_are_distinct_domains() -> None:
    assert SigningMode.EOA.value == "eoa"
    assert RunMode.LIVE.value == "live"
    assert SigningMode is not RunMode
    assert set(SigningMode) == {
        SigningMode.EOA,
        SigningMode.SAFE_DIRECT,
        SigningMode.SAFE_ZODIAC,
    }
    assert set(RunMode) == {RunMode.DRY_RUN, RunMode.PAPER, RunMode.LIVE}


def test_ambiguous_legacy_imports_remain_compatible() -> None:
    assert LegacySigningMode is SigningMode
    assert LegacyRunMode is RunMode


def test_boundary_parsers_normalize_and_reject_cross_domain_values() -> None:
    assert SigningMode.from_string("SAFE_ZODIAC") is SigningMode.SAFE_ZODIAC
    assert RunMode.parse(" PAPER ") is RunMode.PAPER
    assert RunMode.parse_optional("") == ""

    with pytest.raises(ValueError, match="invalid run mode"):
        RunMode.parse("backtest")
    with pytest.raises(ValueError, match="invalid run mode"):
        RunMode.parse(SigningMode.EOA)


def test_financial_models_keep_typed_modes_in_memory_and_strings_on_wire() -> None:
    now = datetime.now(UTC)
    snapshot = PortfolioSnapshot(
        timestamp=now,
        deployment_id="deployment:test",
        total_value_usd=Decimal("10"),
        available_cash_usd=Decimal("2"),
        execution_mode=RunMode.PAPER,
    )
    metrics = PortfolioMetrics(
        deployment_id="deployment:test",
        timestamp=now,
        total_value_usd=Decimal("10"),
        initial_value_usd=Decimal("8"),
        execution_mode=RunMode.DRY_RUN,
    )
    ledger = LedgerEntry(deployment_id="deployment:test", execution_mode=RunMode.LIVE)
    position_event = PositionEvent(deployment_id="deployment:test", execution_mode=RunMode.PAPER)
    identity = AccountingIdentity(
        id="event-1",
        deployment_id="deployment:test",
        cycle_id="cycle-1",
        execution_mode=RunMode.LIVE,
        timestamp=now,
        chain="arbitrum",
        protocol="uniswap_v3",
        wallet_address="0xabc",
        tx_hash="0xtx",
        ledger_entry_id="ledger-1",
    )

    assert snapshot.execution_mode is RunMode.PAPER
    assert metrics.execution_mode is RunMode.DRY_RUN
    assert ledger.execution_mode is RunMode.LIVE
    assert position_event.execution_mode is RunMode.PAPER
    assert identity.execution_mode is RunMode.LIVE
    assert snapshot.to_dict()["execution_mode"] == "paper"
    assert metrics.to_dict()["execution_mode"] == "dry_run"
    assert ledger.to_dict()["execution_mode"] == "live"
    assert position_event.to_dict()["execution_mode"] == "paper"
    assert identity.to_dict()["execution_mode"] == "live"


def test_legacy_empty_persistence_value_round_trips_as_empty_string() -> None:
    now = datetime.now(UTC)
    snapshot = PortfolioSnapshot(
        timestamp=now,
        deployment_id="deployment:test",
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
    )
    metrics = PortfolioMetrics(
        deployment_id="deployment:test",
        timestamp=now,
        total_value_usd=Decimal("0"),
        initial_value_usd=Decimal("0"),
    )
    ledger = LedgerEntry(deployment_id="deployment:test")
    position_event = PositionEvent(deployment_id="deployment:test")
    identity = AccountingIdentity(
        id="event-legacy",
        deployment_id="deployment:test",
        cycle_id="cycle-legacy",
        execution_mode="",
        timestamp=now,
        chain="arbitrum",
        protocol="uniswap_v3",
        wallet_address="0xabc",
        tx_hash="0xtx",
        ledger_entry_id="ledger-legacy",
    )
    restored = PortfolioSnapshot.from_dict(snapshot.to_dict())

    assert restored.execution_mode == ""
    assert restored.to_dict()["execution_mode"] == ""
    assert metrics.execution_mode == ""
    assert ledger.execution_mode == ""
    assert position_event.execution_mode == ""
    assert identity.execution_mode == ""
    assert metrics.to_dict()["execution_mode"] == ""
    assert ledger.to_dict()["execution_mode"] == ""
    assert position_event.to_dict()["execution_mode"] == ""
    assert identity.to_dict()["execution_mode"] == ""
