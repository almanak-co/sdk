"""Synthetic parameter changes test continuity independently of archive availability."""

import pytest
from eth_abi import encode

from almanak.backtest.history_logs import HistoricalLog
from almanak.connectors.curve.economic_history import (
    FEE,
    RAMP,
    STOP,
    parameter_event,
    validate_parameters,
)
from tests.test_curve_economic_model import component
from tests.test_curve_settlement import target as target


def event(pool, signature, values, seconds=12):
    return HistoricalLog(
        block=pool.block.model_copy(
            update={"number": pool.block.number + 1, "timestamp": pool.block.timestamp + seconds}
        ),
        transaction_hash="0x" + "a" * 64,
        transaction_index=0,
        log_index=0,
        address=pool.source.pool,
        topics=(signature,),
        data="0x" + encode(("uint256",) * len(values), values).hex(),
    )


def test_ramp_stop_and_fee_changes_preserve_amplification_at_event_time(target):
    model, state = component(target)
    pool = model.book.at(target, state.time).payload.pool
    at = pool.block.timestamp + 12
    amount = pool.amplification.at(at)
    ramp = event(pool, RAMP, (amount, amount * 2, at, at + 86400))
    amp = parameter_event(ramp)
    current = pool.model_copy(update={"amplification": amp})
    validate_parameters(pool, current, (ramp,))
    stopped_at = at + 43200
    stop = event(pool, STOP, (amp.at(stopped_at), stopped_at), 43212)
    stopped = current.model_copy(
        update={"amplification": parameter_event(stop), "fee": 100, "offpeg_multiplier": 10**10}
    )
    fee = event(pool, FEE, (100, 10**10), 43212)
    validate_parameters(pool, stopped, (ramp, stop, fee))
    with pytest.raises(ValueError, match="complete retained event"):
        validate_parameters(pool, stopped, (ramp, stop))


@pytest.mark.parametrize("signature", (RAMP, STOP))
def test_amplification_cannot_jump_when_an_event_starts(target, signature):
    model, state = component(target)
    pool = model.book.at(target, state.time).payload.pool
    at = pool.block.timestamp + 12
    amount = pool.amplification.at(at) + 1
    values = (amount, amount * 2, at, at + 86400) if signature == RAMP else (amount, at)
    changed = event(pool, signature, values)
    with pytest.raises(ValueError, match="breaks the retained"):
        validate_parameters(
            pool, pool.model_copy(update={"amplification": parameter_event(changed)}), (changed,)
        )


@pytest.mark.parametrize("values", ((5 * 10**9 + 1, 1), (5 * 10**9, 10**10 + 1)))
def test_fee_event_requires_contract_bounds(target, values):
    model, state = component(target)
    pool = model.book.at(target, state.time).payload.pool
    with pytest.raises(ValueError, match="contract bounds"):
        parameter_event(event(pool, FEE, values))


def test_parameter_event_requires_canonical_time_and_layout(target):
    model, state = component(target)
    pool = model.book.at(target, state.time).payload.pool
    bad_time = event(pool, STOP, (100, pool.block.timestamp))
    with pytest.raises(ValueError, match="effective time"):
        parameter_event(bad_time)
    bad_ramp = event(pool, RAMP, (100, 200, pool.block.timestamp, pool.block.timestamp + 86400))
    with pytest.raises(ValueError, match="canonical event time"):
        parameter_event(bad_ramp)
    with pytest.raises(ValueError, match="indexed layout"):
        parameter_event(bad_time.model_copy(update={"topics": (STOP, FEE)}))
    with pytest.raises(ValueError, match="unsupported event"):
        parameter_event(bad_time.model_copy(update={"topics": ("0x" + "b" * 64,)}))


def test_parameter_events_extend_grid_without_relaxing_limits():
    from types import SimpleNamespace

    from almanak.connectors.curve.economic_collection import sample_times
    from almanak.connectors.curve.economic_history import scan

    settings = SimpleNamespace(
        schedule=SimpleNamespace(start=100, deadline=160),
        data=SimpleNamespace(sample_seconds=60, max_samples=3),
    )
    path = SimpleNamespace(events=(SimpleNamespace(block=SimpleNamespace(timestamp=125)),))
    assert sample_times(settings, path) == (100, 125, 160)
    settings.data.max_samples = 2
    with pytest.raises(ValueError, match="sample limit"):
        sample_times(settings, path)
    with pytest.raises(ValueError, match="log request limit"):
        scan(
            None,
            None,
            SimpleNamespace(number=10),
            SimpleNamespace(number=2011),
            SimpleNamespace(max_parameter_log_requests=1),
        )
