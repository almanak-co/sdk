"""Prepared Curve pools refuse samples, paths and bindings outside their frozen scope.

The captured discovery pool stands in for a prepared dataset, so these checks run
without the retained evidence tree.
"""

from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors.curve.economic_book import PoolBook, validate_continuity
from almanak.connectors.curve.economic_collection import contract_bindings
from almanak.connectors.curve.economic_data import ParameterPath, key
from almanak.connectors.curve.economic_history import RAMP, log_scopes, scan
from almanak.connectors.curve.economic_reporting import position_report
from almanak.gateway.permission_models import ContractIdentity
from tests.test_curve_economic_model import component
from tests.test_curve_parameter_history import event
from tests.test_curve_settlement import target as target


@pytest.fixture
def prepared(target):
    model, state = component(target)
    return model, state, model.book.at(target, state.time)


def later(block, blocks, seconds=12):
    return block.model_copy(
        update={"number": block.number + blocks, "timestamp": block.timestamp + seconds * blocks}
    )


def path(target, block, events=(), span=10):
    return ParameterPath(
        source=target.pool,
        first=block,
        first_next=later(block, 1),
        last=later(block, span),
        last_next=later(block, span + 1),
        events=events,
        inputs=(),
    )


def test_a_parameter_path_keeps_ordered_boundaries_and_in_window_events(target, prepared):
    pool = prepared[2].payload.pool
    ramp = event(pool, RAMP, (100, 200, pool.block.timestamp + 12, pool.block.timestamp + 86400))
    assert path(target, pool.block, (ramp,)).events == (ramp,)
    with pytest.raises(ValueError, match="ordered chain scope"):
        path(target, pool.block, span=-2)
    with pytest.raises(ValueError, match="time-floor boundary"):
        ParameterPath(
            source=target.pool,
            first=pool.block,
            first_next=later(pool.block, 2),
            last=later(pool.block, 10),
            last_next=later(pool.block, 11),
            events=(),
            inputs=(),
        )


def test_a_parameter_path_refuses_repeated_foreign_or_early_events(target, prepared):
    pool = prepared[2].payload.pool
    ramp = event(pool, RAMP, (100, 200, pool.block.timestamp + 12, pool.block.timestamp + 86400))
    with pytest.raises(ValueError, match="unique canonical ordering"):
        path(target, pool.block, (ramp, ramp))
    foreign = ramp.model_copy(update={"address": "0x" + "99" * 20})
    with pytest.raises(ValueError, match="outside its pool or time window"):
        path(target, pool.block, (foreign,))
    early = ramp.model_copy(update={"block": pool.block})
    with pytest.raises(ValueError, match="outside its pool or time window"):
        path(target, pool.block, (early,))


def test_consecutive_samples_keep_identities_and_canonical_block_order(target, prepared):
    record = prepared[2]
    retained = path(target, record.reference)
    validate_continuity(record, record, retained)
    moved = record.payload.model_copy(
        update={"contracts": (ContractIdentity.model_construct(address="0x" + "77" * 20),)}
    )
    with pytest.raises(ValueError, match="executable identities changed"):
        validate_continuity(record, record.model_copy(update={"payload": moved}), retained)
    earlier = record.model_copy(update={"reference": later(record.reference, -1)})
    with pytest.raises(ValueError, match="increasing canonical blocks"):
        validate_continuity(record, earlier, retained)
    forked = record.model_copy(
        update={"reference": record.reference.model_copy(update={"hash": "0x" + "ee" * 32})}
    )
    with pytest.raises(ValueError, match="increasing canonical blocks"):
        validate_continuity(record, forked, retained)


def book(max_age=60, evidence=None, start=0, deadline=0):
    result = object.__new__(PoolBook)
    result.settings = SimpleNamespace(
        data=SimpleNamespace(max_age_seconds=max_age),
        schedule=SimpleNamespace(start=start, deadline=deadline),
    )
    result.reader = SimpleNamespace(evidence=evidence or (lambda digest: None))
    return result


def test_a_sample_must_name_its_pool_block_time_floor_age_and_path(target, prepared):
    record = prepared[2]
    moved = record.model_copy(update={"reference": later(record.reference, 1)})
    with pytest.raises(ValueError, match="another pool or block identity"):
        book().validate_row(target, moved)
    late = record.model_copy(update={"available_at": record.payload.successor.timestamp})
    with pytest.raises(ValueError, match="canonical time floor"):
        book().validate_row(target, late)
    aged = record.model_copy(update={"available_at": record.source_time + 5})
    with pytest.raises(ValueError, match="source age limit"):
        book(max_age=1).validate_row(target, aged)
    unproven = record.model_copy(update={"evidence": ("b" * 64,)})
    with pytest.raises(ValueError, match="omits its parameter path evidence"):
        book().validate_row(target, unproven)


def test_a_sample_without_retained_headers_cannot_be_verified(target, prepared):
    with pytest.raises(ValueError, match="omits required archival RPC evidence"):
        book().validate_row(target, prepared[2])


def test_samples_with_two_parameter_paths_are_refused(target, prepared):
    record = prepared[2]
    other = record.model_copy(
        update={"payload": record.payload.model_copy(update={"parameter_path": "b" * 64})}
    )
    loaded = book()
    loaded.manifest = SimpleNamespace(
        preparation=SimpleNamespace(requirement=lambda data_key: data_key)
    )
    requested = []
    loaded.reader.timeline = lambda data_key, kind: (
        requested.append(data_key) or SimpleNamespace(rows=(record, other))
    )
    with pytest.raises(ValueError, match="share one complete parameter path"):
        loaded.load_target(target)
    assert requested == [key(target)]


def test_a_parameter_path_must_name_the_pool_and_cover_the_window(target, prepared):
    block = prepared[2].reference
    other = target.pool.model_copy(update={"pool": "0x" + "98" * 20})
    with pytest.raises(ValueError, match="identifies another pool"):
        book().validate_path(target, path(target, block).model_copy(update={"source": other}))
    outside = book(start=block.timestamp - 1, deadline=block.timestamp)
    with pytest.raises(ValueError, match="frozen historical window"):
        outside.validate_path(target, path(target, block))


def test_history_bindings_cover_exactly_the_pool_chains(target, prepared):
    model, _, record = prepared
    chain = target.fund.chain
    settings = SimpleNamespace(history=(SimpleNamespace(chain=chain),))
    (binding,) = contract_bindings(settings, model.config)
    expected = {target.pool.factory, target.pool.pool, *(a.reference for a in target.assets)}
    assert binding.contracts == tuple(sorted(expected)) and binding.logs == ()
    bounds = record.reference, later(record.reference, 10)
    (scoped,) = contract_bindings(settings, model.config, {chain: bounds})
    assert scoped.logs == log_scopes(target.pool, *bounds)
    with pytest.raises(ValueError, match="exactly the configured pool chains"):
        contract_bindings(SimpleNamespace(history=()), model.config)


def history(block, upper):
    pinned = []
    return SimpleNamespace(
        upper=upper,
        header=lambda number: later(block, number - block.number),
        pin=pinned.append,
        pinned=pinned,
        reader=SimpleNamespace(_rpc=lambda method, params: []),
    )


def test_a_parameter_scan_reads_bounded_windows_and_refuses_excess(target, prepared):
    first = prepared[2].reference
    last = later(first, 3)
    observed = history(first, last)
    policy = SimpleNamespace(max_parameter_log_requests=1, max_parameter_events=0)
    assert scan(observed, target.pool, first, last, policy) == ()
    assert observed.pinned == [later(first, 1), last]
    with pytest.raises(ValueError, match="explicit log request limit"):
        scan(observed, target.pool, first, last, SimpleNamespace(max_parameter_log_requests=0))


def test_an_lp_report_is_modeled_or_names_its_missing_input(target, prepared):
    model, state, _ = prepared
    payload = state.portfolio.positions[0].payload
    modeled = position_report(state, model, payload)
    assert modeled["measurement_status"] == "modeled" and modeled["missing_input"] is None
    assert [claim["amount"] for claim in modeled["proportional_lp_claims"]] == ["0", "0"]

    def unavailable(target, time):
        raise InputUnavailable(key(target), time, "missing sample")

    model.book = SimpleNamespace(at=unavailable)
    missing = position_report(state, model, payload)
    assert missing["measurement_status"] == "unavailable" and missing["observation"] is None
    assert missing["missing_input"]["reason"] == "missing sample"
