"""VIB-3917 / VIB-4085 — same-iteration cost-basis fallback for lending positions.

When a snapshot fires in the same iteration as a SUPPLY/BORROW, the
Layer 5 (accounting_events) row hasn't flushed through the outbox yet,
but Layer 3 (position_events) has — and the runner's
``_recent_open_events`` cache mirrors it synchronously. This helper
reads cost basis off the cache when ``_enrich_lending_pnl`` returns no
events.

Tests pin:
* SUPPLY (asset side) — unrealized_pnl = value - cost_basis
* BORROW (liability side) — unrealized_pnl = value + cost_basis (value is negative)
* Wallet field tolerance (wallet / wallet_address / owner)
* Missing wallet/asset/chain → no-op
* Cache miss → no-op
* Zero/non-positive cost basis → no-op
* Non-string timestamp ignored
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


def _supply_position(details: dict | None = None) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="lending:arbitrum:aave_v3:0xabc:usdc",
        chain="arbitrum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details=details if details is not None else {"wallet": "0xABC", "asset": "USDC"},
    )


def _borrow_position(details: dict | None = None) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.BORROW,
        position_id="lending:arbitrum:aave_v3:0xabc:usdt",
        chain="arbitrum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details=details if details is not None else {"wallet": "0xABC", "asset": "USDT"},
    )


def _position_value(value_usd: Decimal) -> SimpleNamespace:
    return SimpleNamespace(
        cost_basis_usd=Decimal("0"),
        unrealized_pnl_usd=Decimal("0"),
        entry_timestamp="",
        ledger_entry_id="",
        value_usd=value_usd,
    )


@pytest.fixture
def valuer():
    v = PortfolioValuer.__new__(PortfolioValuer)
    v._accounting_store = MagicMock(spec=[])
    v._deployment_id = "AccountingQuantLoopingStrategy:test"
    v._snapshot_event_cache = None
    v._recent_open_events = {}
    return v


def test_supply_populates_cost_basis_and_unrealized_pnl(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "100.0",
            "ledger_entry_id": "led-supply",
            "timestamp": "2026-05-06T00:00:00Z",
        }
    }
    pv = _position_value(Decimal("105"))
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("100.0")
    # SUPPLY signage: value - cost_basis
    assert pv.unrealized_pnl_usd == Decimal("5")
    assert pv.entry_timestamp == "2026-05-06T00:00:00Z"
    assert pv.ledger_entry_id == "led-supply"


def test_borrow_uses_inverted_signage(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdt", "LENDING_DEBT"): {
            "value_usd": "50.0",
            "ledger_entry_id": "led-borrow",
            "timestamp": "2026-05-06T00:00:01Z",
        }
    }
    # Borrow positions surface as negative value_usd in the snapshot.
    pv = _position_value(Decimal("-52"))
    pi = _borrow_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("50.0")
    # BORROW signage: value + cost_basis (so a -52 value with 50 basis = -2 PnL)
    assert pv.unrealized_pnl_usd == Decimal("-2")


def test_wallet_address_field_alias(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "10",
            "ledger_entry_id": "led-1",
            "timestamp": "ts",
        }
    }
    pv = _position_value(Decimal("11"))
    pi = _supply_position(details={"wallet_address": "0xABC", "asset": "USDC"})

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("10")


def test_owner_field_alias(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "10",
            "ledger_entry_id": "led-1",
            "timestamp": "ts",
        }
    }
    pv = _position_value(Decimal("11"))
    pi = _supply_position(details={"owner": "0xABC", "asset": "USDC"})

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("10")


def test_missing_wallet_is_noop(valuer):
    pv = _position_value(Decimal("100"))
    pi = _supply_position(details={"asset": "USDC"})

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("0")


def test_missing_asset_is_noop(valuer):
    pv = _position_value(Decimal("100"))
    pi = _supply_position(details={"wallet": "0xABC"})

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("0")


def test_missing_chain_is_noop(valuer):
    pv = _position_value(Decimal("100"))
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "")

    assert pv.cost_basis_usd == Decimal("0")


def test_cache_miss_is_noop(valuer):
    valuer._recent_open_events = {}
    pv = _position_value(Decimal("100"))
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("0")


def test_zero_value_usd_in_cache_is_noop(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "0",
            "ledger_entry_id": "led-1",
            "timestamp": "ts",
        }
    }
    pv = _position_value(Decimal("100"))
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    # Zero cost basis is treated as no measurement, consistent with disk path semantics.
    assert pv.cost_basis_usd == Decimal("0")


def test_unparseable_value_is_noop(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "not-a-number",
            "ledger_entry_id": "led-1",
            "timestamp": "ts",
        }
    }
    pv = _position_value(Decimal("100"))
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("0")


def test_non_string_timestamp_does_not_overwrite(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "100",
            "ledger_entry_id": "led-1",
            "timestamp": 123,  # int, not str
        }
    }
    pv = _position_value(Decimal("105"))
    pv.entry_timestamp = "preserved"
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    # cost_basis still applied, but entry_timestamp not overwritten by non-str
    assert pv.cost_basis_usd == Decimal("100")
    assert pv.entry_timestamp == "preserved"


def test_empty_ledger_entry_id_in_cache_does_not_overwrite(valuer):
    valuer._recent_open_events = {
        ("lending:arbitrum:aave_v3:0xabc:usdc", "LENDING_COLLATERAL"): {
            "value_usd": "100",
            "ledger_entry_id": "",
            "timestamp": "ts",
        }
    }
    pv = _position_value(Decimal("105"))
    pv.ledger_entry_id = "preserved"
    pi = _supply_position()

    valuer._enrich_lending_pnl_from_open_event(pv, pi, "arbitrum")

    assert pv.cost_basis_usd == Decimal("100")
    assert pv.ledger_entry_id == "preserved"
