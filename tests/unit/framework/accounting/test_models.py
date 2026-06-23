"""Tests for schema_version enforcement in accounting event models (VIB-3498).

Covers:
- LendingAccountingEvent round-trip at v1
- PendleAccountingEvent round-trip at v1
- ValueError raised for unknown schema_version in both models
- ValueError raised for non-int schema_version (bool/float/"1" edge cases)
- Missing schema_version key treated as v1 (backward compat for old records)
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    PendleAccountingEvent,
    PendleEventType,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _identity() -> AccountingIdentity:
    return AccountingIdentity(
        id="test-id",
        deployment_id="strat-1",
        cycle_id="cycle-1",
        execution_mode="live",
        timestamp=datetime(2026, 4, 27, 12, 0, 0, tzinfo=UTC),
        chain="arbitrum",
        protocol="aave_v3",
        wallet_address="0xdeadbeef",
        tx_hash="0xabcd1234",
        ledger_entry_id="ledger-42",
    )


def _lending_event(identity: AccountingIdentity) -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=identity,
        event_type=LendingEventType.SUPPLY,
        position_key="lending:arb:aave_v3:0xdeadbeef:USDC",
        market_id="aave-v3-arb-usdc",
        asset="USDC",
        collateral_value_before_usd=Decimal("5000"),
        collateral_value_after_usd=Decimal("6000"),
        debt_value_before_usd=None,
        debt_value_after_usd=None,
        net_equity_before_usd=Decimal("5000"),
        net_equity_after_usd=Decimal("6000"),
        health_factor_before=Decimal("1.8"),
        health_factor_after=Decimal("1.9"),
        liquidation_threshold=Decimal("0.85"),
        lltv=Decimal("0.80"),
        supply_apr_bps=320,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("1000"),
        interest_delta_usd=None,
        gas_usd=Decimal("1.23"),
        amount_token=Decimal("1000.5"),
        confidence=AccountingConfidence.HIGH,
        unavailable_reason="",
        schema_version=1,
    )


def _pendle_event(identity: AccountingIdentity) -> PendleAccountingEvent:
    return PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_BUY,
        position_key="pendle:arb:0xmarket:0xdeadbeef",
        market_id="pendle-arb-market",
        pt_token="0xpttoken",
        maturity_timestamp=datetime(2026, 12, 31, tzinfo=UTC),
        pt_amount=Decimal("500000000000000000000"),
        sy_amount=Decimal("490000000000000000000"),
        pt_price=Decimal("0.95"),
        implied_apr_bps=750,
        days_to_maturity=247,
        realized_yield_usd=Decimal("1.25"),
        realized_yield_sy=Decimal("0.0005"),
        basis_lot_id="lot-1",
        confidence=AccountingConfidence.HIGH,
        unavailable_reason="",
        schema_version=1,
    )


# ---------------------------------------------------------------------------
# Round-trip tests
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_lending_event_roundtrip_v1(self):
        identity = _identity()
        original = _lending_event(identity)
        payload = original.to_payload_json()
        restored = LendingAccountingEvent.from_payload_json(identity, payload)

        assert restored.event_type == original.event_type
        assert restored.position_key == original.position_key
        assert restored.market_id == original.market_id
        assert restored.asset == original.asset
        assert restored.collateral_value_before_usd == original.collateral_value_before_usd
        assert restored.collateral_value_after_usd == original.collateral_value_after_usd
        assert restored.debt_value_before_usd == original.debt_value_before_usd
        assert restored.debt_value_after_usd == original.debt_value_after_usd
        assert restored.net_equity_before_usd == original.net_equity_before_usd
        assert restored.net_equity_after_usd == original.net_equity_after_usd
        assert restored.health_factor_before == original.health_factor_before
        assert restored.health_factor_after == original.health_factor_after
        assert restored.liquidation_threshold == original.liquidation_threshold
        assert restored.lltv == original.lltv
        assert restored.supply_apr_bps == original.supply_apr_bps
        assert restored.borrow_apr_bps == original.borrow_apr_bps
        assert restored.principal_delta_usd == original.principal_delta_usd
        assert restored.interest_delta_usd == original.interest_delta_usd
        assert restored.gas_usd == original.gas_usd
        assert restored.amount_token == original.amount_token
        assert restored.confidence == original.confidence
        assert restored.unavailable_reason == original.unavailable_reason
        assert restored.schema_version == 1

    def test_pendle_event_roundtrip_v1(self):
        identity = _identity()
        original = _pendle_event(identity)
        payload = original.to_payload_json()
        restored = PendleAccountingEvent.from_payload_json(identity, payload)

        assert restored.event_type == original.event_type
        assert restored.position_key == original.position_key
        assert restored.market_id == original.market_id
        assert restored.pt_token == original.pt_token
        assert restored.maturity_timestamp == original.maturity_timestamp
        assert restored.pt_amount == original.pt_amount
        assert restored.sy_amount == original.sy_amount
        assert restored.pt_price == original.pt_price
        assert restored.implied_apr_bps == original.implied_apr_bps
        assert restored.days_to_maturity == original.days_to_maturity
        assert restored.realized_yield_usd == original.realized_yield_usd
        assert restored.realized_yield_sy == original.realized_yield_sy
        assert restored.basis_lot_id == original.basis_lot_id
        assert restored.confidence == original.confidence
        assert restored.unavailable_reason == original.unavailable_reason
        assert restored.schema_version == 1

    def test_pendle_event_realized_yield_sy_empty_not_zero(self):
        """VIB-5314 Empty≠Zero: realized_yield_sy None stays None, 0 stays 0
        across the payload round-trip; realized_yield_usd stays strictly USD-or-None."""
        identity = _identity()
        base = _pendle_event(identity)

        # Unmeasured USD price case: usd None, sy measured.
        none_usd = replace(base, realized_yield_usd=None, realized_yield_sy=Decimal("0.0005"))
        r = PendleAccountingEvent.from_payload_json(identity, none_usd.to_payload_json())
        assert r.realized_yield_usd is None
        assert r.realized_yield_sy == Decimal("0.0005")

        # Measured break-even: both Decimal("0"), never coerced to None.
        zero = replace(base, realized_yield_usd=Decimal("0"), realized_yield_sy=Decimal("0"))
        r0 = PendleAccountingEvent.from_payload_json(identity, zero.to_payload_json())
        assert r0.realized_yield_usd == Decimal("0")
        assert r0.realized_yield_usd is not None
        assert r0.realized_yield_sy == Decimal("0")

        # Fully unmeasured (no lot match): both None.
        both_none = replace(base, realized_yield_usd=None, realized_yield_sy=None)
        rn = PendleAccountingEvent.from_payload_json(identity, both_none.to_payload_json())
        assert rn.realized_yield_usd is None
        assert rn.realized_yield_sy is None


# ---------------------------------------------------------------------------
# Unknown schema_version raises ValueError
# ---------------------------------------------------------------------------


class TestUnknownSchemaVersion:
    def test_lending_event_raises_on_unknown_schema_version(self):
        identity = _identity()
        original = _lending_event(identity)
        d = json.loads(original.to_payload_json())
        d["schema_version"] = 99
        with pytest.raises(ValueError, match="99"):
            LendingAccountingEvent.from_payload_json(identity, json.dumps(d))

    def test_pendle_event_raises_on_unknown_schema_version(self):
        identity = _identity()
        original = _pendle_event(identity)
        d = json.loads(original.to_payload_json())
        d["schema_version"] = 99
        with pytest.raises(ValueError, match="99"):
            PendleAccountingEvent.from_payload_json(identity, json.dumps(d))

    @pytest.mark.parametrize("bad_version", [True, 1.0, "1"])
    def test_lending_event_raises_on_non_int_schema_version(self, bad_version: object) -> None:
        # Python equality: True == 1 and 1.0 == 1, so a naive `in frozenset({1})`
        # check would accept these.  The type-gate must fire first.
        identity = _identity()
        d = json.loads(_lending_event(identity).to_payload_json())
        d["schema_version"] = bad_version
        with pytest.raises(ValueError, match="schema_version"):
            LendingAccountingEvent.from_payload_json(identity, json.dumps(d))

    @pytest.mark.parametrize("bad_version", [True, 1.0, "1"])
    def test_pendle_event_raises_on_non_int_schema_version(self, bad_version: object) -> None:
        identity = _identity()
        d = json.loads(_pendle_event(identity).to_payload_json())
        d["schema_version"] = bad_version
        with pytest.raises(ValueError, match="schema_version"):
            PendleAccountingEvent.from_payload_json(identity, json.dumps(d))


# ---------------------------------------------------------------------------
# Missing schema_version defaults to v1 (backward compat for old records)
# ---------------------------------------------------------------------------


class TestMissingSchemaVersionDefaultsToV1:
    def test_lending_event_accepts_missing_schema_version_as_v1(self):
        identity = _identity()
        original = _lending_event(identity)
        d = json.loads(original.to_payload_json())
        d.pop("schema_version")
        restored = LendingAccountingEvent.from_payload_json(identity, json.dumps(d))
        assert restored.schema_version == 1
        assert restored.event_type == original.event_type

    def test_pendle_event_accepts_missing_schema_version_as_v1(self):
        identity = _identity()
        original = _pendle_event(identity)
        d = json.loads(original.to_payload_json())
        d.pop("schema_version")
        restored = PendleAccountingEvent.from_payload_json(identity, json.dumps(d))
        assert restored.schema_version == 1
        assert restored.event_type == original.event_type
