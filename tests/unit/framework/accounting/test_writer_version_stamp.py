"""Tests for the G13 version stamp + L1/L4 lending alias projection on
``accounting_events`` (Accounting-AttemptNo17 §A4).

Augmentation lives in :func:`augment_accounting_payload`. Both state
backends (SQLiteStore and GatewayStateManager) call it before serialising
the payload. The writer itself is now a pure delegator (VIB-3862) and
does not mutate event instances.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    MATCHING_POLICY_VERSION,
    SCHEMA_VERSION,
)
from almanak.framework.accounting.writer import (
    AccountingWriter,
    _project_lending_aliases,
    augment_accounting_payload,
)
from almanak.framework.state.exceptions import AccountingPersistenceError


class _FakeIdentity:
    def __init__(self, *, execution_mode: str = "live") -> None:
        self.execution_mode = execution_mode
        self.deployment_id = "dep-1"
        self.deployment_id = "strat-1"
        self.cycle_id = "cycle-1"


class _FakeEvent:
    """Minimal stand-in for a typed accounting event."""

    def __init__(self, payload: dict[str, Any], *, execution_mode: str = "live") -> None:
        self.identity = _FakeIdentity(execution_mode=execution_mode)
        self._payload = payload
        self.schema_version = 1
        self.confidence = "HIGH"

    def to_payload_json(self) -> str:
        return json.dumps(self._payload)


# ─── augment_accounting_payload happy path ──────────────────────────────


def test_augment_stamps_versions_on_dict_payload():
    payload = json.dumps({"event_type": "REPAY", "asset": "USDC"})
    out = json.loads(augment_accounting_payload(payload, is_live=True))

    assert out["schema_version"] == SCHEMA_VERSION
    assert out["formula_version"] == FORMULA_VERSION
    assert out["matching_policy_version"] == MATCHING_POLICY_VERSION
    # Original keys preserved.
    assert out["event_type"] == "REPAY"
    assert out["asset"] == "USDC"


def test_augment_overrides_pre_existing_versions():
    """payload_schemas constants are AUTHORITATIVE — even a per-event
    ``to_payload_json()`` that emits a stale value gets overwritten."""
    payload = json.dumps(
        {
            "event_type": "BORROW",
            "schema_version": 999,
            "formula_version": 999,
            "matching_policy_version": 999,
        }
    )
    out = json.loads(augment_accounting_payload(payload, is_live=True))

    assert out["schema_version"] == SCHEMA_VERSION
    assert out["formula_version"] == FORMULA_VERSION
    assert out["matching_policy_version"] == MATCHING_POLICY_VERSION


def test_augment_idempotent():
    payload = json.dumps({"event_type": "SWAP", "x": 1})
    once = augment_accounting_payload(payload, is_live=True)
    twice = augment_accounting_payload(once, is_live=True)
    assert json.loads(once) == json.loads(twice)


# ─── augment_accounting_payload mode-aware failure contract (VIB-3863) ─


def test_augment_raises_on_invalid_json_when_live():
    with pytest.raises(AccountingPersistenceError) as excinfo:
        augment_accounting_payload("not-json{", is_live=True)
    assert "not valid JSON" in str(excinfo.value)
    assert excinfo.value.write_kind == "accounting"


def test_augment_returns_original_on_invalid_json_when_not_live(caplog):
    out = augment_accounting_payload("not-json{", is_live=False)
    assert out == "not-json{"
    assert any("not valid JSON" in rec.message for rec in caplog.records)


def test_augment_raises_on_non_dict_payload_when_live():
    payload = json.dumps([1, 2, 3])
    with pytest.raises(AccountingPersistenceError) as excinfo:
        augment_accounting_payload(payload, is_live=True)
    assert "must decode to dict" in str(excinfo.value)


def test_augment_returns_original_on_non_dict_when_not_live(caplog):
    payload = json.dumps([1, 2, 3])
    out = augment_accounting_payload(payload, is_live=False)
    assert out == payload
    assert any("must decode to dict" in rec.message for rec in caplog.records)


# ─── L1 / L4 lending alias projection ───────────────────────────────────


def test_lending_alias_projection_repay():
    d = {
        "event_type": "REPAY",
        "principal_delta_usd": "100.0",
        "interest_delta_usd": "1.5",
    }
    _project_lending_aliases(d)
    assert d["principal_repaid_usd"] == "100.0"
    assert d["interest_paid_usd"] == "1.5"


def test_lending_alias_projection_deleverage_same_as_repay():
    d = {
        "event_type": "DELEVERAGE",
        "principal_delta_usd": "200.0",
        "interest_delta_usd": "0.5",
    }
    _project_lending_aliases(d)
    assert d["principal_repaid_usd"] == "200.0"
    assert d["interest_paid_usd"] == "0.5"


def test_lending_alias_projection_withdraw_emits_accrued():
    d = {
        "event_type": "WITHDRAW",
        "principal_delta_usd": "50.0",
        "interest_delta_usd": "0.25",
    }
    _project_lending_aliases(d)
    assert d["interest_accrued_usd"] == "0.25"
    # WITHDRAW has no principal-repaid concept on the supply side.
    assert "principal_repaid_usd" not in d


def test_lending_alias_projection_does_not_overwrite_existing():
    d = {
        "event_type": "REPAY",
        "principal_delta_usd": "100.0",
        "principal_repaid_usd": "999.0",  # native — wins
    }
    _project_lending_aliases(d)
    assert d["principal_repaid_usd"] == "999.0"


def test_lending_alias_projection_skips_missing_event_type():
    d = {"principal_delta_usd": "100.0"}
    _project_lending_aliases(d)
    assert "principal_repaid_usd" not in d


def test_lending_alias_projection_handles_none_amounts():
    d = {"event_type": "REPAY", "principal_delta_usd": None, "interest_delta_usd": None}
    _project_lending_aliases(d)
    assert "principal_repaid_usd" not in d
    assert "interest_paid_usd" not in d


# ─── AccountingWriter.write delegation contract ─────────────────────────


@pytest.mark.asyncio
async def test_writer_delegates_to_store_save():
    """The writer hands the event off untouched. Augmentation happens at
    the backend chokepoint (covered by integration tests)."""
    event = _FakeEvent({"event_type": "REPAY"})
    handed_off: list[Any] = []

    async def _fake_save(ev: Any) -> bool:
        handed_off.append(ev)
        return True

    store = AsyncMock()
    store.save_accounting_event = _fake_save

    writer = AccountingWriter(store)
    ok = await writer.write(event)

    assert ok is True
    assert handed_off == [event]


@pytest.mark.asyncio
async def test_writer_does_not_mutate_event_to_payload_json():
    """VIB-3862: the writer must NOT monkey-patch event.to_payload_json.
    A subsequent call on the same instance should return the unaugmented
    payload — the event class's method is preserved.

    Bound-method *identity* is unreliable in Python (each ``obj.method``
    access creates a fresh bound-method wrapper), so this test verifies
    the underlying function has not been swapped via ``__func__`` AND
    that calling the method post-write still yields the original payload.
    """
    event = _FakeEvent({"event_type": "REPAY"})
    underlying_func_before = type(event).to_payload_json

    async def _fake_save(ev: Any) -> bool:
        return True

    store = AsyncMock()
    store.save_accounting_event = _fake_save

    writer = AccountingWriter(store)
    await writer.write(event)

    # The class-level function must be unchanged (no rebinding).
    assert type(event).to_payload_json is underlying_func_before
    # And the instance must not carry an instance-level override.
    assert "to_payload_json" not in event.__dict__
    # Calling it should still yield the unaugmented payload.
    out = json.loads(event.to_payload_json())
    assert "schema_version" not in out
    assert out == {"event_type": "REPAY"}


@pytest.mark.asyncio
async def test_writer_raises_in_live_when_store_lacks_save():
    """A misconfigured store in live mode must halt — silently dropping a
    REPAY would leave the books out of sync with the chain."""
    event = _FakeEvent({"event_type": "REPAY"}, execution_mode="live")

    class _StoreWithoutSave:
        pass

    writer = AccountingWriter(_StoreWithoutSave())
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await writer.write(event)
    assert "save_accounting_event" in str(excinfo.value)
    assert excinfo.value.write_kind == "accounting"


@pytest.mark.asyncio
async def test_writer_logs_and_returns_false_in_paper_when_store_lacks_save(caplog):
    event = _FakeEvent({"event_type": "REPAY"}, execution_mode="paper")

    class _StoreWithoutSave:
        pass

    writer = AccountingWriter(_StoreWithoutSave())
    ok = await writer.write(event)
    assert ok is False
    assert any("save_accounting_event" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_writer_propagates_typed_persistence_error_in_live():
    event = _FakeEvent({"event_type": "REPAY"}, execution_mode="live")

    async def _fake_save(_ev: Any) -> bool:
        raise AccountingPersistenceError("accounting", deployment_id="strat-1")

    store = AsyncMock()
    store.save_accounting_event = _fake_save

    writer = AccountingWriter(store)
    with pytest.raises(AccountingPersistenceError):
        await writer.write(event)


@pytest.mark.asyncio
async def test_writer_swallows_typed_persistence_error_in_paper(caplog):
    event = _FakeEvent({"event_type": "REPAY"}, execution_mode="paper")

    async def _fake_save(_ev: Any) -> bool:
        raise AccountingPersistenceError("accounting", deployment_id="strat-1")

    store = AsyncMock()
    store.save_accounting_event = _fake_save

    writer = AccountingWriter(store)
    ok = await writer.write(event)
    assert ok is False
    assert any("AccountingWriter.write failed" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_writer_wraps_unexpected_error_in_live():
    """An unexpected (non-typed) exception in live mode must surface as
    AccountingPersistenceError so the runner pipeline can trap it
    consistently."""
    event = _FakeEvent({"event_type": "REPAY"}, execution_mode="live")

    async def _fake_save(_ev: Any) -> bool:
        raise RuntimeError("backend exploded")

    store = AsyncMock()
    store.save_accounting_event = _fake_save

    writer = AccountingWriter(store)
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await writer.write(event)
    assert isinstance(excinfo.value.cause, RuntimeError)
