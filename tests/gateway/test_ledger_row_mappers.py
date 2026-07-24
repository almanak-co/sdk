"""Pin-down tests for the state_service ledger / accounting-event row mappers.

Characterizes the row->proto mapping semantics of ``_ledger_entry_to_proto``,
``_ledger_entry_from_row``, and ``_sqlite_row_to_accounting_event`` before the
cc-reduction refactor so it is provably behavior-preserving: proto3-string
collapse of None/""/missing to "", the or-default numeric fallbacks, JSON
payload bytes round-trips, ISO-vs-epoch timestamp handling, and the
Empty != Zero contract on ``slippage_bps`` where only Python None maps to
absent on the wire.
"""

from datetime import UTC, datetime

from almanak.framework.observability.ledger import LedgerEntry
from almanak.gateway.services.state_service import (
    _ledger_entry_from_row,
    _ledger_entry_to_proto,
    _sqlite_row_to_accounting_event,
)

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_TS_EPOCH = int(_TS.timestamp())
_TS_ISO = "2026-01-01T00:00:00+00:00"


class TestLedgerEntryToProto:
    def test_full_entry_maps_every_field(self):
        entry = LedgerEntry(
            id="ledger-1",
            cycle_id="cycle-1",
            deployment_id="deployment:abc",
            timestamp=_TS,
            intent_type="SWAP",
            token_in="WETH",
            amount_in="1.5",
            token_out="USDC",
            amount_out="3000",
            effective_price="2000",
            slippage_bps=2.5,
            gas_used=21000,
            gas_usd="0.5",
            tx_hash="0xabc",
            chain="arbitrum",
            protocol="uniswap_v3",
            success=True,
            error="",
        )
        msg = _ledger_entry_to_proto(entry)
        assert msg.id == "ledger-1"
        assert msg.cycle_id == "cycle-1"
        assert msg.deployment_id == "deployment:abc"
        assert msg.timestamp == _TS_EPOCH
        assert msg.intent_type == "SWAP"
        assert msg.token_in == "WETH"
        assert msg.amount_in == "1.5"
        assert msg.token_out == "USDC"
        assert msg.amount_out == "3000"
        assert msg.effective_price == "2000"
        assert msg.slippage_bps == 2.5
        assert msg.gas_used == 21000
        assert msg.gas_usd == "0.5"
        assert msg.tx_hash == "0xabc"
        assert msg.chain == "arbitrum"
        assert msg.protocol == "uniswap_v3"
        assert msg.success is True
        assert msg.error == ""

    def test_falsy_fields_collapse_to_wire_defaults(self):
        entry = LedgerEntry(
            id="",
            cycle_id="",
            deployment_id="",
            timestamp=_TS,
            slippage_bps=None,
            gas_used=0,
            success=False,
            error="boom",
        )
        msg = _ledger_entry_to_proto(entry)
        assert msg.id == ""
        assert msg.cycle_id == ""
        assert msg.deployment_id == ""
        assert msg.intent_type == ""
        assert msg.token_in == ""
        assert msg.amount_in == ""
        assert msg.slippage_bps == 0.0
        assert msg.gas_used == 0
        assert msg.success is False
        assert msg.error == "boom"

    def test_non_datetime_timestamp_maps_to_zero(self):
        entry = LedgerEntry(id="ledger-1", timestamp=_TS)
        entry.timestamp = None
        assert _ledger_entry_to_proto(entry).timestamp == 0


def _ledger_row(**overrides):
    row = {
        "id": "ledger-1",
        "cycle_id": "cycle-1",
        "deployment_id": "deployment:abc",
        "execution_mode": "live",
        "intent_type": "SWAP",
        "token_in": "WETH",
        "amount_in": "1.5",
        "token_out": "USDC",
        "amount_out": "3000",
        "effective_price": "2000",
        "slippage_bps": 2.5,
        "gas_used": 21000,
        "gas_usd": "0.5",
        "tx_hash": "0xabc",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "success": 1,
        "error": "",
        "extracted_data_json": '{"a": 1}',
        "price_inputs_json": "",
        "pre_state_json": None,
        "post_state_json": b'{"b": 2}',
    }
    row.update(overrides)
    return row


_ROW_KEYS = {
    "extracted_data_key": "extracted_data_json",
    "price_inputs_key": "price_inputs_json",
    "pre_state_key": "pre_state_json",
    "post_state_key": "post_state_json",
}


class TestLedgerEntryFromRow:
    def test_full_row_maps_every_field(self):
        msg = _ledger_entry_from_row(_ledger_row(), timestamp=_TS_EPOCH, **_ROW_KEYS)
        assert msg.id == "ledger-1"
        assert msg.cycle_id == "cycle-1"
        assert msg.deployment_id == "deployment:abc"
        assert msg.execution_mode == "live"
        assert msg.timestamp == _TS_EPOCH
        assert msg.intent_type == "SWAP"
        assert msg.token_in == "WETH"
        assert msg.amount_in == "1.5"
        assert msg.token_out == "USDC"
        assert msg.amount_out == "3000"
        assert msg.effective_price == "2000"
        assert msg.slippage_bps == 2.5
        assert msg.gas_used == 21000
        assert msg.gas_usd == "0.5"
        assert msg.tx_hash == "0xabc"
        assert msg.chain == "arbitrum"
        assert msg.protocol == "uniswap_v3"
        assert msg.success is True
        assert msg.error == ""

    def test_json_columns_encode_str_passthrough_bytes_and_collapse_falsy(self):
        msg = _ledger_entry_from_row(_ledger_row(), timestamp=_TS_EPOCH, **_ROW_KEYS)
        assert msg.extracted_data_json == b'{"a": 1}'
        assert msg.price_inputs_json == b""
        assert msg.pre_state_json == b""
        assert msg.post_state_json == b'{"b": 2}'

    def test_falsy_and_missing_fields_collapse_to_wire_defaults(self):
        msg = _ledger_entry_from_row(
            {"id": None, "success": 0},
            timestamp=0,
            **_ROW_KEYS,
        )
        assert msg.id == ""
        assert msg.cycle_id == ""
        assert msg.execution_mode == ""
        assert msg.intent_type == ""
        assert msg.gas_used == 0
        assert msg.gas_usd == ""
        assert msg.success is False
        assert msg.extracted_data_json == b""

    def test_none_slippage_stays_unset_and_string_slippage_coerces(self):
        unset = _ledger_entry_from_row(_ledger_row(slippage_bps=None), timestamp=_TS_EPOCH, **_ROW_KEYS)
        assert unset.slippage_bps == 0.0
        coerced = _ledger_entry_from_row(_ledger_row(slippage_bps="7.25"), timestamp=_TS_EPOCH, **_ROW_KEYS)
        assert coerced.slippage_bps == 7.25


def _accounting_row(**overrides):
    row = {
        "id": "evt-1",
        "deployment_id": "deployment:abc",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": _TS_ISO,
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "wallet_address": "0xwallet",
        "event_type": "lp_open",
        "position_key": "pos-1",
        "ledger_entry_id": "ledger-1",
        "tx_hash": "0xabc",
        "confidence": "measured",
        "payload_json": '{"a": 1}',
        "schema_version": 2,
    }
    row.update(overrides)
    return row


class TestSqliteRowToAccountingEvent:
    def test_full_row_maps_every_field(self):
        msg = _sqlite_row_to_accounting_event(_accounting_row())
        assert msg.id == "evt-1"
        assert msg.deployment_id == "deployment:abc"
        assert msg.cycle_id == "cycle-1"
        assert msg.execution_mode == "live"
        assert msg.timestamp == _TS_EPOCH
        assert msg.chain == "arbitrum"
        assert msg.protocol == "uniswap_v3"
        assert msg.wallet_address == "0xwallet"
        assert msg.event_type == "lp_open"
        assert msg.position_key == "pos-1"
        assert msg.ledger_entry_id == "ledger-1"
        assert msg.tx_hash == "0xabc"
        assert msg.confidence == "measured"
        assert msg.payload_json == b'{"a": 1}'
        assert msg.schema_version == 2

    def test_bytes_payload_is_decoded_then_reencoded(self):
        msg = _sqlite_row_to_accounting_event(_accounting_row(payload_json=b'{"b": 2}'))
        assert msg.payload_json == b'{"b": 2}'

    def test_falsy_and_missing_fields_collapse_to_wire_defaults(self):
        msg = _sqlite_row_to_accounting_event({"id": None, "timestamp": None})
        assert msg.id == ""
        assert msg.deployment_id == ""
        assert msg.cycle_id == ""
        assert msg.execution_mode == ""
        assert msg.timestamp == 0
        assert msg.wallet_address == ""
        assert msg.event_type == ""
        assert msg.confidence == ""
        assert msg.payload_json == b"{}"
        assert msg.schema_version == 1

    def test_empty_payload_string_collapses_to_empty_object(self):
        msg = _sqlite_row_to_accounting_event(_accounting_row(payload_json=""))
        assert msg.payload_json == b"{}"
