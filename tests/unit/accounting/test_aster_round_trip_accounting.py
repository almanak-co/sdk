"""Replay observed Aster execution economics through the production persistence codecs."""

from __future__ import annotations

import copy
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.venue_account_read_base import SettledVenueTransfer
from almanak.connectors.aster_perps.gateway.payout import payout_log
from almanak.connectors.aster_perps.runner_hooks import AsterPerpsRunnerHookConnector
from almanak.framework.accounting.accountant_test import _cell_g1_money_trail, _cell_g6_reconciliation, _cells_perp
from almanak.framework.accounting.category_handlers.perp_handler import handle_perp
from almanak.framework.accounting.receipt_set import evaluate_landed_receipt_sets, validated_landed_receipt_hashes
from almanak.framework.execution.extracted_data import PerpData, ProtocolFees
from almanak.framework.observability.ledger import deserialize_extracted_data, serialize_extracted_data
from almanak.framework.observability.pnl_attributor import attribute_perp
from almanak.framework.observability.position_events import (
    IntentEventContext,
    PositionEvent,
    PositionEventType,
    _apply_perp,
)
from almanak.framework.runner.capital_flow_state import PendingUnclassified

FIXTURE = Path(__file__).parents[2] / "fixtures/accounting/aster_pro_round_trip.json"
WALLET = "0x1111111111111111111111111111111111111111"


def replay():
    data = json.loads(FIXTURE.read_text())
    ledger = data["transaction_ledger"]
    events = []
    positions = []
    for row in ledger:
        extracted = deserialize_extracted_data(row["extracted_data_json"])
        if row["intent_type"] == "PERP_DEPOSIT":
            continue
        extracted.pop("perp_data", None)
        extracted.pop("protocol_fees", None)
        if row["intent_type"] == "PERP_WITHDRAW":
            # This field is now carried from the submitted request by the handler.
            extracted["aster_withdraw"]["client_request_id"] = row["id"]
        result = SimpleNamespace(extracted_data=extracted, protocol_fees=None)
        AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc", wallet_address=WALLET)
        row["extracted_data_json"] = serialize_extracted_data(extracted)
        outbox = next(o for o in data["accounting_outbox"] if o["ledger_entry_id"] == row["id"])
        event = handle_perp(outbox, row)
        if event is not None:
            payload = json.loads(event.to_payload_json())
            events.append(
                {
                    "id": event.identity.id,
                    "event_type": event.event_type,
                    "ledger_entry_id": row["id"],
                    "payload_json": json.dumps(payload),
                }
            )
            position = PositionEvent(
                event_type=PositionEventType.CLOSE if row["intent_type"] == "PERP_CLOSE" else PositionEventType.OPEN
            )
            position.protocol_fees_usd = str(result.protocol_fees.total_usd)
            position.gas_usd = "0"
            position.is_long = True
            _apply_perp(position, IntentEventContext(None, result, extracted, row["deployment_id"], "bsc", row["id"]))
            positions.append(vars(position))
    return data, events, positions


def test_real_fill_round_trip_survives_persistence_and_recomputation():
    data, events, positions = replay()
    opened, closed = [json.loads(e["payload_json"]) for e in events]
    assert Decimal(opened["size"]) == Decimal("5.09332")
    assert Decimal(opened["open_fee_usd"]) == Decimal("0.00203732")
    assert Decimal(closed["close_fee_usd"]) == Decimal("0.00204074")
    assert Decimal(closed["exit_price"]) == Decimal("2550.93")
    assert Decimal(closed["realized_pnl_usd"]) == Decimal("0.00854")
    result = attribute_perp(*positions)
    assert Decimal(result["trade_pnl_usd"]) == Decimal("0.00446194")
    assert result["net_pnl_usd"] is None and result["funding_pnl_usd"] is None
    assert result["is_long"] is True
    positions[1]["attribution_json"] = json.dumps(result)
    assert attribute_perp(*positions) == result
    payloads = {e["id"]: json.loads(e["payload_json"]) for e in events}
    cells = {c.cell_id: c for c in _cells_perp(events, [], payloads, {})}
    assert cells["P3"].status == "PASS" and cells["P5"].status == "PASS"
    assert _cell_g1_money_trail(data["transaction_ledger"], events, payloads, "perp").status == "PASS"
    assert evaluate_landed_receipt_sets(data["transaction_ledger"]).passed
    hashes = validated_landed_receipt_hashes(data["transaction_ledger"])
    assert all(h.startswith("0x") for h in hashes)
    assert not any("18517" in h for h in hashes)


@pytest.mark.parametrize("top_level,venue,json_sidecar", [("0", "7", "{}"), (None, "0", "[]"), (None, None, "{}")])
def test_perp_close_preserves_funding_measurement_and_top_level_precedence(top_level, venue, json_sidecar):
    event = PositionEvent(event_type=PositionEventType.CLOSE, attribution_json=json_sidecar)
    perp = PerpData(funding_fee_usd=None if venue is None else Decimal(venue), realized_pnl=Decimal(0))
    extracted = {"perp_data": perp, "funding_fee_usd": top_level}
    _apply_perp(event, IntentEventContext(None, None, extracted, "deployment:test", "bsc", "ledger-close"))
    sidecar = json.loads(event.attribution_json)
    assert sidecar["realized_pnl"] == "0"
    if top_level is None and venue is None:
        assert "funding_fee_usd" not in sidecar
    else:
        assert sidecar["funding_fee_usd"] == "0"


def test_reconciliation_counts_execution_and_withdrawal_costs_without_scaling_notional_twice():
    data, events, _ = replay()
    payloads = {e["id"]: json.loads(e["payload_json"]) for e in events}
    cell, decomposition = _cell_g6_reconciliation(
        data["portfolio_snapshots"], data["transaction_ledger"], data["position_events"], events, "perp", payloads, {}
    )
    assert Decimal(decomposition["Σ_perp_trading_fee_usd"]) == Decimal("0.00407806")
    assert Decimal(decomposition["Σ_venue_withdrawal_fee_usd"]) == Decimal("0.11")
    assert Decimal(decomposition["ε_scaling_base_usd"]) == Decimal("5.10186")
    assert Decimal(decomposition["gap_usd"]) < Decimal("0.00000001")
    assert cell.status == "FAIL"  # Funding is still unmeasured; no fabricated PASS.


def test_measured_venue_fill_still_requires_its_inline_execution_fee():
    data, events, _ = replay()
    payloads = {e["id"]: json.loads(e["payload_json"]) for e in events}
    for payload in payloads.values():
        payload["open_fee_usd"] = None
        payload["close_fee_usd"] = None
    cell, decomposition = _cell_g6_reconciliation(
        data["portfolio_snapshots"], data["transaction_ledger"], data["position_events"], events, "perp", payloads, {}
    )
    assert cell.status == "FAIL"
    assert decomposition["Σ_perp_execution_fee_null_count"] == "2"


@pytest.mark.parametrize(
    "field,value", [("schema_version", 999), ("execution_id", "bogus"), ("request_id", ""), ("wallet_address", "")]
)
def test_venue_receipt_rejects_inconsistent_identity(field, value):
    data, _, _ = replay()
    row = data["transaction_ledger"][1]
    extracted = json.loads(row["extracted_data_json"])
    extracted["venue_receipt"][field] = value
    row["extracted_data_json"] = json.dumps(extracted)
    assert not evaluate_landed_receipt_sets([row]).passed


def test_duplicate_venue_execution_cannot_substantiate_two_rows():
    data, _, _ = replay()
    row = data["transaction_ledger"][1]
    duplicate = {**row, "id": "duplicate"}
    assert not evaluate_landed_receipt_sets([row, duplicate]).passed
    assert _cell_g1_money_trail([row, duplicate], [], {}, "perp").status == "FAIL"


@pytest.mark.parametrize("raw", ["[1]", "{", '{"realized_pnl":"NaN"}', '{"realized_pnl":"Infinity"}'])
def test_corrupt_close_economics_remain_unknown(raw):
    result = attribute_perp({"protocol_fees_usd": "0"}, {"protocol_fees_usd": "0", "attribution_json": raw})
    assert result["price_pnl_usd"] is None and result["net_pnl_usd"] is None


def test_measured_zero_and_unknown_round_trip_separately():
    for value in (None, Decimal(0)):
        data = deserialize_extracted_data(
            serialize_extracted_data(
                {
                    "perp_data": PerpData(realized_pnl=value),
                    "protocol_fees": ProtocolFees(
                        total_usd=value, perp_fee_usd=value, unavailable_reason="unmeasured" if value is None else None
                    ),
                }
            )
        )
        assert data["perp_data"].realized_pnl == value
        assert data["protocol_fees"].perp_fee_usd == value
    result = attribute_perp(
        {"protocol_fees_usd": "0", "gas_usd": "0"},
        {"protocol_fees_usd": "0", "gas_usd": "0", "attribution_json": '{"realized_pnl":"0","funding_fee_usd":"0"}'},
    )
    assert result["net_pnl_usd"] == "0"


def receipt():
    return {
        "status": 1,
        "blockNumber": 12,
        "logs": [
            {
                "address": WALLET,
                "logIndex": 4,
                "topics": [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "0x" + "2" * 64,
                    "0x" + WALLET[2:].zfill(64),
                ],
                "data": hex(2894461940000000000),
            }
        ],
    }


def test_payout_matches_exact_log_and_survives_pending_record_restart():
    proof = payout_log(receipt(), token=WALLET, receiver=WALLET, net=Decimal("2.89446194"), decimals=18)
    assert proof == {"log_index": 4, "block_number": 12, "raw_amount": "2894461940000000000"}
    evidence = SettledVenueTransfer("withdrawal", "bsc", "0x123", 4, WALLET, WALLET, int(proof["raw_amount"]), 12)
    pending = PendingUnclassified("0x123", "bsc", WALLET, "IN", 12, Decimal("2.89446194"), 4, int(proof["raw_amount"]))
    assert evidence.matches(PendingUnclassified.from_record(pending.to_record()))
    assert not evidence.matches(PendingUnclassified("0x123", "bsc", WALLET, "IN", 12, Decimal(1), 5, 10**18))
    assert not evidence.matches(PendingUnclassified("0x123", "bsc", WALLET, "IN", 12, Decimal(1)))


@pytest.mark.parametrize("change", ["status", "token", "receiver", "amount", "duplicate"])
def test_payout_rejects_wrong_or_ambiguous_receipt(change):
    raw = receipt()
    if change == "status":
        raw["status"] = 0
    if change == "token":
        raw["logs"][0]["address"] = "0x" + "3" * 40
    if change == "receiver":
        raw["logs"][0]["topics"][2] = "0x" + "3" * 64
    if change == "amount":
        raw["logs"][0]["data"] = "0x01"
    if change == "duplicate":
        raw["logs"].append(copy.deepcopy(raw["logs"][0]))
    with pytest.raises(ValueError):
        payout_log(raw, token=WALLET, receiver=WALLET, net=Decimal("2.89446194"), decimals=18)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch",
    [
        None,
        "amount",
        "receiver",
        "chain",
        "wallet",
        "FAILED",
        "CANCELED",
        "CANCELLED",
        "REJECTED",
        "reverted",
        "status_missing",
        "status_string_zero",
        "status_string_one",
        "status_two",
        "unknown_state",
    ],
)
async def test_gateway_confirms_authenticated_withdrawal_and_exact_payout(monkeypatch, mismatch):
    from almanak.connectors.aster_perps.gateway.service import AsterPerpsServiceServicer
    from almanak.connectors.aster_perps.proto import aster_perps_pb2

    class Client:
        user_address = WALLET

        async def transfer_history(self):
            record = {
                "id": "withdrawal",
                "type": "WITHDRAW",
                "asset": "USDT",
                "amount": "3.00446194",
                "state": "SUCCESS",
                "chainId": 56,
                "address": WALLET,
                "txHash": "0x" + "1" * 64,
            }
            if mismatch == "amount":
                record["amount"] = "4"
            if mismatch == "receiver":
                record["address"] = "0x" + "2" * 40
            if mismatch == "chain":
                record["chainId"] = 1
            if mismatch in {"FAILED", "CANCELED", "CANCELLED", "REJECTED"}:
                record["state"] = mismatch
            if mismatch == "unknown_state":
                record["state"] = "UNKNOWN"
            return [record]

    servicer = AsterPerpsServiceServicer(SimpleNamespace(private_key=None, safe_mode=None))
    servicer._client = Client()
    payout_receipt = receipt()
    if mismatch == "reverted":
        payout_receipt["status"] = 0
    if mismatch == "status_missing":
        payout_receipt.pop("status")
    elif mismatch == "status_string_zero":
        payout_receipt["status"] = "0"
    elif mismatch == "status_string_one":
        payout_receipt["status"] = "1"
    elif mismatch == "status_two":
        payout_receipt["status"] = 2
    payout_receipt["logs"][0]["address"] = "0x55d398326f99059fF775485246999027B3197955"
    monkeypatch.setattr(
        "almanak.connectors.aster_perps.gateway.service.get_cached_web3",
        lambda chain: SimpleNamespace(eth=SimpleNamespace(get_transaction_receipt=lambda tx: payout_receipt)),
    )
    request = aster_perps_pb2.AsterWithdrawalPayoutRequest(
        wallet_address=WALLET if mismatch != "wallet" else "0x" + "2" * 40,
        withdrawal_id="withdrawal",
        asset="USDT",
        gross_amount="3.00446194",
        fee_amount="0.11",
    )
    response = await servicer.GetWithdrawalPayout(request, None)
    if mismatch in {None, "status_string_one"}:
        assert response.success and response.settled
        assert response.log_index == 4 and response.raw_amount == "2894461940000000000"
    elif mismatch in {
        "FAILED",
        "CANCELED",
        "CANCELLED",
        "REJECTED",
        "reverted",
        "status_missing",
        "status_string_zero",
        "status_two",
    }:
        assert response.success and not response.settled
        assert response.withdrawal_id == "withdrawal" and not response.tx_hash
    else:
        assert not response.success and not response.settled


@pytest.mark.asyncio
async def test_runner_resolves_only_exact_venue_payout_and_defers_failed_reads(monkeypatch):
    from almanak.framework.accounting.capital_flows import (
        ChainScanResult,
        CounterpartyKind,
        FlowClassification,
        TransferDirection,
        TransferObservation,
    )
    from almanak.framework.runner import runner_state
    from almanak.framework.runner.capital_flow_state import STATUS_MEASURED, CapitalFlowRecord

    data, _, _ = replay()
    proof = SettledVenueTransfer("withdrawal", "bsc", "0x123", 4, WALLET, WALLET, 2894461940000000000, 12)
    runner = SimpleNamespace(_primary_chain_lower="bsc", _get_gateway_client=lambda: object())
    snapshot = SimpleNamespace(
        chain="bsc", snapshot_metadata={}, total_value_usd=Decimal(15), available_cash_usd=Decimal(0)
    )
    record = CapitalFlowRecord(status=STATUS_MEASURED, cursors={"bsc": 10}, era_start={"bsc": 10})
    monkeypatch.setattr(runner_state, "_capital_flow_wallet", lambda *args: WALLET)
    monkeypatch.setattr(runner_state, "_capital_flow_token_universe", lambda *args, **kwargs: {})
    monkeypatch.setattr(runner_state, "_capital_flow_price_lookup", lambda *args: lambda *args: Decimal(1))
    monkeypatch.setattr(
        "almanak.connectors._strategy_base.venue_account_read_registry.VenueAccountReadRegistry.settled_transfers",
        lambda *args, **kwargs: (proof,),
    )
    own = TransferObservation(
        "bsc",
        WALLET,
        "USDT",
        Decimal("2.89446194"),
        proof.raw_amount,
        TransferDirection.IN,
        "0x" + "2" * 40,
        CounterpartyKind.CONTRACT,
        "0x123",
        12,
        4,
        FlowClassification.UNCLASSIFIED_IN,
        True,
    )
    other = TransferObservation(
        "bsc",
        WALLET,
        "USDT",
        Decimal(1),
        10**18,
        TransferDirection.IN,
        "0x" + "2" * 40,
        CounterpartyKind.CONTRACT,
        "0x123",
        12,
        5,
        FlowClassification.UNCLASSIFIED_IN,
        True,
    )
    monkeypatch.setattr(
        runner_state, "scan_chain_transfers", lambda *args, **kwargs: ChainScanResult("bsc", 10, 12, (own, other))
    )
    folded, detail = await runner_state._scan_capital_flow_interval(
        runner,
        snapshot,
        record=record,
        chains=["bsc"],
        handles={"bsc": object()},
        heads={"bsc": 12},
        ledger_rows=data["transaction_ledger"],
    )
    assert detail is None and len(folded.pending_unclassified) == 1
    assert folded.pending_unclassified[0].log_index == 5
    assert snapshot.snapshot_metadata["settled_venue_transfers"][0]["log_index"] == 4

    monkeypatch.setattr(
        "almanak.connectors._strategy_base.venue_account_read_registry.VenueAccountReadRegistry.settled_transfers",
        lambda *args, **kwargs: (),
    )
    terminal, detail = await runner_state._scan_capital_flow_interval(
        runner,
        snapshot,
        record=record,
        chains=["bsc"],
        handles={"bsc": object()},
        heads={"bsc": 12},
        ledger_rows=data["transaction_ledger"],
    )
    assert detail is None and terminal.cursors["bsc"] == 12
    assert len(terminal.pending_unclassified) == 2
    assert snapshot.snapshot_metadata["settled_venue_transfers"] == []

    def failed(*args, **kwargs):
        raise RuntimeError("RPC unavailable")

    monkeypatch.setattr(
        "almanak.connectors._strategy_base.venue_account_read_registry.VenueAccountReadRegistry.settled_transfers",
        failed,
    )
    deferred, detail = await runner_state._scan_capital_flow_interval(
        runner,
        snapshot,
        record=record,
        chains=["bsc"],
        handles={"bsc": object()},
        heads={"bsc": 12},
        ledger_rows=data["transaction_ledger"],
    )
    assert deferred == record and detail == "scan_deferred"


@pytest.mark.asyncio
async def test_real_sqlite_writer_advances_matrix_cells(tmp_path):
    import sqlite3

    from almanak.framework.accounting.accountant_test import run_against_sqlite
    from almanak.framework.accounting.writer import AccountingWriter
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    data, events, _ = replay()
    db = tmp_path / "aster-replay.sqlite"
    store = SQLiteStore(SQLiteConfig(db_path=str(db)))
    await store.initialize()
    try:
        with sqlite3.connect(db) as connection:
            for table in ("transaction_ledger", "accounting_outbox", "position_events", "portfolio_snapshots"):
                columns = {r[1] for r in connection.execute(f"PRAGMA table_info({table})")}
                for row in data[table]:
                    row = {key: value for key, value in row.items() if key in columns}
                    names = ",".join(row)
                    placeholders = ",".join("?" for _ in row)
                    connection.execute(f"INSERT INTO {table} ({names}) VALUES ({placeholders})", tuple(row.values()))
        writer = AccountingWriter(store)
        for row in data["transaction_ledger"]:
            outbox = next(o for o in data["accounting_outbox"] if o["ledger_entry_id"] == row["id"])
            event = handle_perp(outbox, row)
            if event is not None:
                assert await writer.write(event)
        report = run_against_sqlite(db, primitive="perp")
        cells = {cell.cell_id: cell.status for cell in report.cells}
        assert cells["G1"] == "PASS" and cells["G17"] == "PASS"
        assert cells["P3"] == "PASS" and cells["P5"] == "PASS"
        assert Decimal(report.g6_decomposition["gap_usd"]) < Decimal("0.00000001")
        assert cells["G6"] == "FAIL"  # The fixture has no measured funding receipt.
        with sqlite3.connect(db) as connection:
            payloads = [json.loads(r[0]) for r in connection.execute("SELECT payload_json FROM accounting_events")]
        assert all(p["primitive_version"] == 3 for p in payloads)
    finally:
        await store.close()


def test_observed_bsc_payout_receipt_decodes_exact_net_amount():
    raw = json.loads((FIXTURE.parent / "aster_pro_payout_receipt.json").read_text())
    proof = payout_log(
        raw,
        token="0x55d398326f99059fF775485246999027B3197955",
        receiver="0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF",
        net=Decimal("2.89446194"),
        decimals=18,
    )
    assert proof["block_number"] == 126295971
    assert proof["raw_amount"] == "2894461940000000000"


def test_legacy_measured_unrealized_pnl_survives_repeated_attribution():
    opened = {"protocol_fees_usd": "0", "gas_usd": "0"}
    closed = {
        "protocol_fees_usd": "0",
        "gas_usd": "0",
        "unrealized_pnl": "10",
        "attribution_json": '{"funding_fee_usd":"0"}',
    }
    first = attribute_perp(opened, closed)
    assert first["net_pnl_usd"] == "10"
    closed["attribution_json"] = json.dumps(first)
    assert attribute_perp(opened, closed) == first


@pytest.mark.parametrize(
    "event_type,fee_side",
    [
        ("PERP_OPEN", "open_fee_usd"),
        ("PERP_INCREASE", "open_fee_usd"),
        ("PERP_CLOSE", "close_fee_usd"),
        ("PERP_DECREASE", "close_fee_usd"),
        ("PERP_LIQUIDATE", "close_fee_usd"),
    ],
)
@pytest.mark.parametrize("fee", [Decimal(0), Decimal("0.002"), None])
def test_perp_lifecycle_fees_survive_typed_ledger_projection(event_type, fee_side, fee):
    data, _, _ = replay()
    row = next(r for r in data["transaction_ledger"] if r["intent_type"] == "PERP_OPEN")
    outbox = next(o for o in data["accounting_outbox"] if o["ledger_entry_id"] == row["id"])
    row["intent_type"] = outbox["intent_type"] = event_type
    extracted = deserialize_extracted_data(row["extracted_data_json"])
    if fee is None:
        extracted.pop("protocol_fees", None)
    else:
        extracted["protocol_fees"] = ProtocolFees(total_usd=fee, perp_fee_usd=fee)
    row["extracted_data_json"] = serialize_extracted_data(extracted)
    payload = json.loads(handle_perp(outbox, row).to_payload_json())
    assert payload[fee_side] == (None if fee is None else str(fee))
    other_side = "close_fee_usd" if fee_side == "open_fee_usd" else "open_fee_usd"
    assert payload[other_side] is None
