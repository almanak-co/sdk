"""Unit tests for ``GatewayStateManager.save_position_event``.

The method maps a ``PositionEvent`` dataclass onto the
``SavePositionEventRequest`` proto and issues the RPC through the gateway
client seam (``self._client.state.SavePositionEvent``). It is a
non-blocking observability write: every failure mode (gateway
``success=False``, transport error, request-build error) logs a warning
and returns ``False`` instead of raising, so the strategy loop is never
halted by a position-event write.

The gateway client is mocked at the same seam the accounting-persistence
tests use (``tests/unit/runner/test_accounting_persistence.py``): a
``MagicMock`` client whose ``state.SavePositionEvent`` returns a stub
response. Requests are real ``gateway_pb2`` messages, so proto
``optional`` field presence (``HasField``) is asserted for the four
``None``-able fields (tick_lower / tick_upper / in_range / is_long) —
"absent on wire" must mean ``None`` at the source, per the
Empty != Zero contract.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from almanak.framework.observability.position_events import PositionEvent
from almanak.framework.state.gateway_state_manager import GatewayStateManager

FIXED_TS = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)


def _make_gsm(
    *,
    response_success: bool = True,
    error: str = "",
    timeout: float = 30.0,
) -> tuple[GatewayStateManager, MagicMock]:
    response = MagicMock()
    response.success = response_success
    response.error = error

    client = MagicMock()
    client.state.SavePositionEvent = MagicMock(return_value=response)
    return GatewayStateManager(client=client, timeout=timeout), client


def _lp_event(**overrides) -> PositionEvent:
    """A fully-populated LP OPEN event (all optional LP fields set)."""
    defaults = dict(
        id="evt-1",
        deployment_id="deployment:abc123def456",
        cycle_id="cycle-7",
        execution_mode="live",
        position_id="12345",
        position_type="LP",
        event_type="OPEN",
        timestamp=FIXED_TS,
        protocol="uniswap_v3",
        chain="ethereum",
        token0="WETH",
        token1="USDC",
        amount0="1.5",
        amount1="3000.0",
        value_usd="6000.0",
        tick_lower=-887220,
        tick_upper=887220,
        liquidity="123456789",
        in_range=True,
        fees_token0="0.001",
        fees_token1="2.5",
        tx_hash="0xabc",
        gas_usd="1.23",
        ledger_entry_id="ledger-1",
        protocol_fees_usd="0.42",
        attribution_json='{"fees": "2.5"}',
        attribution_version=3,
    )
    defaults.update(overrides)
    return PositionEvent(**defaults)


@pytest.mark.asyncio
async def test_success_maps_all_request_fields() -> None:
    """Happy path: every scalar field lands on the request, RPC gets the timeout."""
    gsm, client = _make_gsm(timeout=7.5)

    ok = await gsm.save_position_event(_lp_event())

    assert ok is True
    client.state.SavePositionEvent.assert_called_once()
    req = client.state.SavePositionEvent.call_args.args[0]
    assert client.state.SavePositionEvent.call_args.kwargs["timeout"] == 7.5

    assert req.id == "evt-1"
    assert req.deployment_id == "deployment:abc123def456"
    assert req.cycle_id == "cycle-7"
    assert req.execution_mode == "live"
    assert req.position_id == "12345"
    assert req.position_type == "LP"
    assert req.event_type == "OPEN"
    assert req.timestamp == int(FIXED_TS.timestamp())
    assert req.protocol == "uniswap_v3"
    assert req.chain == "ethereum"
    assert req.token0 == "WETH"
    assert req.token1 == "USDC"
    assert req.amount0 == "1.5"
    assert req.amount1 == "3000.0"
    assert req.value_usd == "6000.0"
    assert req.liquidity == "123456789"
    assert req.fees_token0 == "0.001"
    assert req.fees_token1 == "2.5"
    assert req.tx_hash == "0xabc"
    assert req.gas_usd == "1.23"
    assert req.ledger_entry_id == "ledger-1"
    assert req.protocol_fees_usd == "0.42"
    assert req.attribution_json == '{"fees": "2.5"}'
    assert req.attribution_version == 3


@pytest.mark.asyncio
async def test_lp_optional_fields_present_perp_fields_absent() -> None:
    """LP event: tick/in_range set on wire; is_long absent (None at source)."""
    gsm, client = _make_gsm()

    ok = await gsm.save_position_event(_lp_event())

    assert ok is True
    req = client.state.SavePositionEvent.call_args.args[0]
    assert req.HasField("tick_lower")
    assert req.tick_lower == -887220
    assert req.HasField("tick_upper")
    assert req.tick_upper == 887220
    assert req.HasField("in_range")
    assert req.in_range is True
    assert not req.HasField("is_long")


@pytest.mark.asyncio
async def test_perp_event_sets_is_long_only() -> None:
    """PERP event: is_long present; LP-only optionals absent."""
    gsm, client = _make_gsm()
    event = _lp_event(
        position_type="PERP",
        tick_lower=None,
        tick_upper=None,
        in_range=None,
        is_long=False,
        liquidity="",
        leverage="5",
        entry_price="3000",
        mark_price="3100",
        unrealized_pnl="150",
    )

    ok = await gsm.save_position_event(event)

    assert ok is True
    req = client.state.SavePositionEvent.call_args.args[0]
    assert not req.HasField("tick_lower")
    assert not req.HasField("tick_upper")
    assert not req.HasField("in_range")
    assert req.HasField("is_long")
    assert req.is_long is False
    assert req.leverage == "5"
    assert req.entry_price == "3000"
    assert req.mark_price == "3100"
    assert req.unrealized_pnl == "150"


@pytest.mark.asyncio
async def test_default_event_all_optionals_absent_and_fallbacks_applied() -> None:
    """Bare PositionEvent: no optional proto field set, empty-string fallbacks hold."""
    gsm, client = _make_gsm()
    event = PositionEvent(id="evt-min", timestamp=FIXED_TS)

    ok = await gsm.save_position_event(event)

    assert ok is True
    req = client.state.SavePositionEvent.call_args.args[0]
    assert not req.HasField("tick_lower")
    assert not req.HasField("tick_upper")
    assert not req.HasField("in_range")
    assert not req.HasField("is_long")
    assert req.cycle_id == ""
    assert req.execution_mode == ""
    assert req.protocol_fees_usd == ""
    # Dataclass default is already "{}" — stays "{}" on the wire.
    assert req.attribution_json == "{}"
    assert req.attribution_version == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attribution_json", "expected"),
    [
        ("", "{}"),  # falsy → "{}" fallback
        (None, "{}"),  # falsy → "{}" fallback
        ('{"il": "0"}', '{"il": "0"}'),  # truthy → passthrough
    ],
)
async def test_attribution_json_fallback(attribution_json, expected) -> None:
    gsm, client = _make_gsm()
    event = _lp_event()
    event.attribution_json = attribution_json

    ok = await gsm.save_position_event(event)

    assert ok is True
    req = client.state.SavePositionEvent.call_args.args[0]
    assert req.attribution_json == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("protocol_fees_usd", "expected"),
    [
        (None, ""),  # unmeasured (attribute None) → empty on wire
        ("", ""),  # parser didn't emit → stays empty (Empty != Zero)
        ("0", "0"),  # measured zero survives as "0"
        ("1.23", "1.23"),
    ],
)
async def test_protocol_fees_usd_none_maps_to_empty(protocol_fees_usd, expected) -> None:
    gsm, client = _make_gsm()
    event = _lp_event()
    event.protocol_fees_usd = protocol_fees_usd

    ok = await gsm.save_position_event(event)

    assert ok is True
    req = client.state.SavePositionEvent.call_args.args[0]
    assert req.protocol_fees_usd == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["cycle_id", "execution_mode"])
async def test_none_cycle_id_and_execution_mode_map_to_empty(field) -> None:
    """``None`` on the source object never reaches the proto string field."""
    gsm, client = _make_gsm()
    event = _lp_event(**{field: None})

    ok = await gsm.save_position_event(event)

    assert ok is True
    req = client.state.SavePositionEvent.call_args.args[0]
    assert getattr(req, field) == ""


@pytest.mark.asyncio
async def test_gateway_failure_returns_false_without_raising() -> None:
    """success=False from the gateway → False, no exception (non-blocking write)."""
    gsm, client = _make_gsm(response_success=False, error="db down")

    ok = await gsm.save_position_event(_lp_event())

    assert ok is False
    client.state.SavePositionEvent.assert_called_once()


@pytest.mark.asyncio
async def test_rpc_exception_returns_false() -> None:
    """Transport-level error → False, no exception (non-blocking write)."""
    gsm, client = _make_gsm()
    client.state.SavePositionEvent.side_effect = RuntimeError("rpc boom")

    ok = await gsm.save_position_event(_lp_event())

    assert ok is False


@pytest.mark.asyncio
async def test_request_build_error_returns_false_before_rpc() -> None:
    """A malformed event (non-datetime timestamp) fails in request building.

    The except clause covers the build path too: the RPC is never issued
    and the method still returns False instead of raising.
    """
    gsm, client = _make_gsm()
    event = _lp_event()
    event.timestamp = "not-a-datetime"

    ok = await gsm.save_position_event(event)

    assert ok is False
    client.state.SavePositionEvent.assert_not_called()
