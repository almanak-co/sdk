"""Pin-down tests for state_service._position_event_row_to_proto.

Characterizes the row->proto mapping semantics before the cc-reduction
refactor so it is provably behavior-preserving: proto3-string collapse of
None/""/missing to "", the attribution defaults, ISO-vs-epoch timestamp
handling, and the Empty != Zero presence contract on the four optional
fields (tick_lower/tick_upper/in_range/is_long) where 0/False must still
be set on the wire and only Python None maps to absent.
"""

from almanak.gateway.services.state_service import _position_event_row_to_proto


def _full_row(**overrides):
    row = {
        "id": "evt-1",
        "deployment_id": "deployment:abc",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "position_id": "pos-1",
        "position_type": "lp",
        "event_type": "open",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "WETH",
        "token1": "USDC",
        "amount0": "1.5",
        "amount1": "3000",
        "value_usd": "6000",
        "liquidity": "12345",
        "fees_token0": "0.01",
        "fees_token1": "20",
        "leverage": "1",
        "entry_price": "2000",
        "mark_price": "2100",
        "unrealized_pnl": "150",
        "tx_hash": "0xabc",
        "gas_usd": "0.5",
        "ledger_entry_id": "ledger-1",
        "protocol_fees_usd": "0.1",
        "attribution_json": '{"a": 1}',
        "attribution_version": 2,
        "tick_lower": -100,
        "tick_upper": 100,
        "in_range": 1,
        "is_long": 0,
    }
    row.update(overrides)
    return row


class TestFullRow:
    def test_all_fields_mapped(self):
        msg = _position_event_row_to_proto(_full_row())
        assert msg.id == "evt-1"
        assert msg.deployment_id == "deployment:abc"
        assert msg.cycle_id == "cycle-1"
        assert msg.execution_mode == "live"
        assert msg.position_id == "pos-1"
        assert msg.position_type == "lp"
        assert msg.event_type == "open"
        assert msg.timestamp == 1767225600
        assert msg.protocol == "uniswap_v3"
        assert msg.chain == "arbitrum"
        assert msg.token0 == "WETH"
        assert msg.token1 == "USDC"
        assert msg.amount0 == "1.5"
        assert msg.amount1 == "3000"
        assert msg.value_usd == "6000"
        assert msg.liquidity == "12345"
        assert msg.fees_token0 == "0.01"
        assert msg.fees_token1 == "20"
        assert msg.leverage == "1"
        assert msg.entry_price == "2000"
        assert msg.mark_price == "2100"
        assert msg.unrealized_pnl == "150"
        assert msg.tx_hash == "0xabc"
        assert msg.gas_usd == "0.5"
        assert msg.ledger_entry_id == "ledger-1"
        assert msg.protocol_fees_usd == "0.1"
        assert msg.attribution_json == '{"a": 1}'
        assert msg.attribution_version == 2


class TestSparseRow:
    def test_defaults(self):
        msg = _position_event_row_to_proto({"id": "x"})
        assert msg.id == "x"
        assert msg.deployment_id == ""
        assert msg.token0 == ""
        assert msg.value_usd == ""
        assert msg.attribution_json == "{}"
        assert msg.attribution_version == 0
        assert msg.timestamp == 0
        for field in ("tick_lower", "tick_upper", "in_range", "is_long"):
            assert not msg.HasField(field)


class TestEmptyNeZero:
    def test_zero_but_present_optionals_are_set(self):
        # SQLite int-bools: 0 is a measured value, not absence.
        msg = _position_event_row_to_proto(
            _full_row(tick_lower=0, tick_upper=0, in_range=0, is_long=0)
        )
        for field in ("tick_lower", "tick_upper", "in_range", "is_long"):
            assert msg.HasField(field)
        assert msg.tick_lower == 0
        assert msg.tick_upper == 0
        assert msg.in_range is False
        assert msg.is_long is False

    def test_none_optionals_are_absent(self):
        msg = _position_event_row_to_proto(
            _full_row(tick_lower=None, tick_upper=None, in_range=None, is_long=None)
        )
        for field in ("tick_lower", "tick_upper", "in_range", "is_long"):
            assert not msg.HasField(field)


class TestStringCollapse:
    def test_none_and_empty_both_collapse_to_empty(self):
        # Intentional proto3-string contract: no presence tracking.
        assert _position_event_row_to_proto(_full_row(token0=None)).token0 == ""
        assert _position_event_row_to_proto(_full_row(token0="")).token0 == ""


class TestAttributionDefaults:
    def test_none_and_empty_json_default_to_empty_object(self):
        assert _position_event_row_to_proto(_full_row(attribution_json=None)).attribution_json == "{}"
        assert _position_event_row_to_proto(_full_row(attribution_json="")).attribution_json == "{}"

    def test_none_version_defaults_to_zero(self):
        assert _position_event_row_to_proto(_full_row(attribution_version=None)).attribution_version == 0


class TestTimestamp:
    def test_pg_epoch_int_passthrough(self):
        assert _position_event_row_to_proto(_full_row(timestamp=1767225600)).timestamp == 1767225600

    def test_garbage_timestamp_maps_to_zero(self):
        assert _position_event_row_to_proto(_full_row(timestamp="not-a-date")).timestamp == 0
