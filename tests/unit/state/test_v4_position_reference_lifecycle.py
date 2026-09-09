"""Real V4 NFT registry and accounting identities remain separate and joined."""

import inspect
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.framework.accounting.accountant_test import _cell22_registry_coherence
from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, LPEventType
from almanak.framework.accounting.writer import AccountingWriter, augment_accounting_payload
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.ledger_registry_mode import LedgerRegistrySaveMode


def _fixture():
    return json.loads(
        (Path(__file__).parents[2] / "fixtures/accounting/v4_registry_reference_lifecycle.json").read_text()
    )


def _event(row):
    payload = json.loads(row["payload_json"])
    identity_fields = inspect.signature(AccountingIdentity).parameters
    identity = {k: row[k] for k in identity_fields}
    identity["timestamp"] = datetime.fromisoformat(identity["timestamp"])
    kwargs = {k: payload[k] for k in inspect.signature(LPAccountingEvent).parameters if k in payload}
    kwargs.update(
        identity=AccountingIdentity(**identity),
        event_type=LPEventType(row["event_type"]),
        confidence=AccountingConfidence(row["confidence"]),
    )
    return LPAccountingEvent(**kwargs)


def _registry(raw):
    fields = inspect.signature(RegistryRow).parameters
    kwargs = {k: raw[k] for k in fields if k in raw}
    kwargs["payload"] = json.loads(raw["payload"])
    return RegistryRow(**kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_first", [True, False])
async def test_real_v4_writer_registry_join_in_both_write_orders(tmp_path, registry_first):
    fixture = _fixture()
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    registry = _registry(fixture["registry"])

    async def save_registry():
        await store.save_ledger_and_registry_atomic(
            SimpleNamespace(id="replay"), registry, None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
        )

    try:
        if registry_first:
            await save_registry()
        for row in fixture["events"]:
            assert await AccountingWriter(store).write(_event(row))
        if not registry_first:
            await save_registry()
        events = [dict(r) for r in store._conn.execute("SELECT * FROM accounting_events")]
        rows = [dict(r) for r in store._conn.execute("SELECT * FROM position_registry")]
        for event in events:
            payload = json.loads(event["payload_json"])
            reference = json.loads(event["position_reference"])
            assert reference == payload["position_reference"]
            assert reference["primitive"] == "lp_v4"
            assert reference["physical_identity_hash"] == registry.physical_identity_hash
            original = next(r for r in fixture["events"] if r["id"] == event["id"])
            assert payload["position_hash"] == json.loads(original["payload_json"])["position_hash"]
            assert payload["position_hash"] != reference["physical_identity_hash"]
        cell = _cell22_registry_coherence(
            events,
            rows,
            position_reference_column_present=True,
            position_registry_table_present=True,
            malformed_position_reference_row_ids=[],
        )
        assert cell.status == "PASS", cell.diagnostic
    finally:
        await store.close()


@pytest.mark.parametrize("protocol,expected", [("uniswap_v4", "lp_v4"), ("uniswap_v3", "lp"), ("unknown", "lp")])
def test_protocol_identity_refinement_is_canonical_without_registry(protocol, expected):
    payload = json.loads(_fixture()["events"][0]["payload_json"])
    payload["protocol"] = protocol
    result = json.loads(augment_accounting_payload(json.dumps(payload), is_live=True))
    assert result["position_reference"]["primitive"] == expected
    assert result["position_reference"]["physical_identity_hash"] is None
    assert result["position_hash"] == payload["position_hash"]


@pytest.mark.asyncio
async def test_v4_same_transaction_multiple_nfts_does_not_guess_reference(tmp_path):
    from dataclasses import replace

    fixture = _fixture()
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "ambiguous.db")))
    await store.initialize()
    try:
        registry = _registry(fixture["registry"])
        other = replace(
            registry, physical_identity_hash="0x" + "ab" * 32, payload={**registry.payload, "token_id": "3026952"}
        )
        for entry in (registry, other):
            await store.save_ledger_and_registry_atomic(
                SimpleNamespace(id="replay"), entry, None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
            )
        for row in fixture["events"]:
            assert await AccountingWriter(store).write(_event(row))
        for row in store._conn.execute("SELECT position_reference FROM accounting_events"):
            reference = json.loads(row[0])
            assert reference["primitive"] == "lp_v4"
            assert reference["physical_identity_hash"] is None
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("chain", "arbitrum"), ("deployment_id", "deployment:other")])
async def test_v4_deferred_reference_cannot_cross_scope(tmp_path, field, value):
    from dataclasses import replace

    fixture = _fixture()
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "scope.db")))
    await store.initialize()
    try:
        for row in fixture["events"]:
            assert await AccountingWriter(store).write(_event(row))
        registry = replace(_registry(fixture["registry"]), **{field: value})
        await store.save_ledger_and_registry_atomic(
            SimpleNamespace(id="replay"), registry, None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
        )
        for row in store._conn.execute("SELECT position_reference FROM accounting_events"):
            assert json.loads(row[0])["physical_identity_hash"] is None
    finally:
        await store.close()
