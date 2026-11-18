"""Preserve every supported amplification and fee change independently of sampling."""

from almanak.backtest.history_logs import MAX_LOG_BLOCKS, HistoricalLog, LogScope, read_logs
from almanak.connectors.curve import crypto_history
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.domain import Amplification
from almanak.connectors.curve.economic_data import ParameterPath
from almanak.connectors.curve.receipts import topic
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.v3_prepared_acquisition import evidence
from almanak.gateway.evm_logs import data

RAMP = topic("RampA(uint256,uint256,uint256,uint256)")
STOP = topic("StopRampA(uint256,uint256)")
FEE = topic("ApplyNewFee(uint256,uint256)")
TOPICS = RAMP, STOP, FEE


def log_scope(source, first, last):
    return LogScope(
        address=source.pool,
        topics=TOPICS if source.family == "stableswap_ng" else crypto_history.pool_topics(source),
        first_block=first.number,
        last_block=last.number,
    )


def log_scopes(source, first, last):
    pool = log_scope(source, first, last)
    if source.family == "stableswap_ng":
        return (pool,)
    return pool, LogScope(
        address=source.factory,
        topics=crypto_history.FACTORY_TOPICS,
        first_block=first.number,
        last_block=last.number,
    )


def scan(history, source, first, last, policy):
    count = (last.number - first.number + MAX_LOG_BLOCKS - 1) // MAX_LOG_BLOCKS
    if count > policy.max_parameter_log_requests:
        raise ValueError("Curve parameter history exceeds its explicit log request limit")
    scopes = log_scopes(source, first, last)
    if count * len(scopes) > policy.max_parameter_log_requests:
        raise ValueError("Curve parameter history exceeds its explicit log request limit")
    events: list[HistoricalLog] = []
    for left in range(first.number + 1, last.number + 1, MAX_LOG_BLOCKS):
        right = min(last.number, left + MAX_LOG_BLOCKS - 1)
        history.pin(history.header(left))
        history.pin(history.header(right))
        for scope in scopes:
            events.extend(read_logs(history, scope, left, right))
        if len(events) > policy.max_parameter_events:
            raise ValueError("Curve parameter history exceeds its explicit event limit")
    for event in events:
        history.pin(event.block)
        if source.family == "stableswap_ng":
            parameter_event(event)
        else:
            crypto_history.parameter_event(event, source)
    return tuple(sorted(events, key=lambda event: event.order))


def parameter_event(event):
    if len(event.topics) != 1:
        raise ValueError("Curve parameter event has an unexpected indexed layout")
    raw = {"data": event.data}
    signature = event.topics[0]
    if signature == RAMP:
        initial, future, start, end = data(raw, ("uint256",) * 4)
        if start != event.block.timestamp:
            raise ValueError("Curve ramp does not start at its canonical event time")
        return Amplification(initial=initial, future=future, initial_time=start, future_time=end)
    if signature == STOP:
        amount, time = data(raw, ("uint256", "uint256"))
        if time != event.block.timestamp:
            raise ValueError("Curve stop-ramp event has another effective time")
        return Amplification(initial=amount, future=amount, initial_time=time, future_time=time)
    if signature == FEE:
        fee, multiplier = data(raw, ("uint256", "uint256"))
        if fee > 5 * 10**9 or fee * multiplier > 5 * 10**19:
            raise ValueError("Curve fee event exceeds the verified contract bounds")
        return fee, multiplier
    raise ValueError("Curve parameter path contains an unsupported event")


def preflight(history, target, schedule, policy):
    history.reader.used.clear()
    if history.reader.pin_block("finalized") != history.upper:
        raise ValueError("Curve finalized historical anchor changed during preparation")
    first, last = history.floor(schedule.start), history.floor(schedule.deadline)
    if last.number == history.upper.number:
        raise ValueError("Curve preparation requires a finalized successor to its end boundary")
    first_next, last_next = history.header(first.number + 1), history.header(last.number + 1)
    for block in (first, first_next, last, last_next):
        history.pin(block)
    events = scan(history, target.pool, first, last, policy)
    return ParameterPath(
        source=target.pool,
        first=first,
        first_next=first_next,
        last=last,
        last_next=last_next,
        events=events,
        inputs=tuple(record.digest for record in evidence(history.reader)),
    )


def validate_parameters(previous, current, events):
    if isinstance(previous, (CryptoObservation, TriCryptoObservation)):
        return crypto_history.validate_parameters(previous, current, events)
    amplification = previous.amplification
    fees = previous.fee, previous.offpeg_multiplier
    for event in events:
        result = parameter_event(event)
        if isinstance(result, Amplification):
            if result.initial != amplification.at(event.block.timestamp):
                raise ValueError("Curve amplification event breaks the retained parameter path")
            amplification = result
        else:
            fees = result
    if amplification != current.amplification or fees != (current.fee, current.offpeg_multiplier):
        raise ValueError("Curve parameters changed without complete retained event evidence")
