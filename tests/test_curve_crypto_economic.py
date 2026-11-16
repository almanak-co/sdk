"""Crypto prepared semantics preserve native math, canonical time and explicit LP scaling."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.backtest.types import UnixTime
from almanak.connectors.curve import crypto_history as history
from almanak.connectors.curve import economic_pool
from almanak.connectors.curve.crypto_observation import CryptoRamp
from almanak.connectors.curve.economic_data import (
    CRYPTO_MODEL,
    CRYPTO_REVISION,
    MODEL,
    REVISION,
    model_identity,
)
from almanak.connectors.curve.economic_history import log_scopes, validate_parameters
from almanak.connectors.curve.operations import Addition, Swap
from almanak.connectors.curve.positions import Position
from almanak.models import AssetAmount
from tests.test_curve_crypto_execution import CASES, inputs


@pytest.mark.parametrize("case", CASES)
def test_prepared_dispatch_matches_captured_native_transition_at_eligibility(case):
    pool, operation, *_ = inputs(case)
    owned = SimpleNamespace(position=None)
    state = economic_pool.virtual_pool(pool, owned, UnixTime(case["timestamp"]))
    assert state.pool is pool and state.timestamp == case["timestamp"]
    if isinstance(operation, Swap):
        first = operation.target.assets.index(operation.amount.asset)
        second = operation.target.assets.index(operation.minimum.asset)
        assert (
            economic_pool.swap(state, first, second, operation.amount.amount)
            == case["predicted"]["amounts"][second]
        )
    elif isinstance(operation, Addition):
        assert (
            economic_pool.minted(state, tuple(v.amount for v in operation.amounts.root))
            == case["actual_lp_change"]
        )
    else:
        index = (
            None
            if operation.output_asset is None
            else operation.target.assets.index(operation.output_asset)
        )
        assert economic_pool.removed(state, operation.shares.amount, index) == tuple(
            case["predicted"]["amounts"]
        )


def test_virtual_lp_overlay_keeps_extensive_ratios_and_original_evidence():
    pool, operation, *_ = inputs(CASES[0])
    saved = pool.model_dump_json()
    shares = pool.total_supply // 100
    owned = SimpleNamespace(
        position=Position(shares=AssetAmount(asset=pool.source.shares_asset, amount=shares))
    )
    state = economic_pool.virtual_pool(pool, owned, pool.block.timestamp + 60)
    assert state.pool.block == pool.block
    assert state.pool.total_supply == pool.total_supply + shares
    for old, new in zip(
        (*pool.balances, pool.invariant, pool.accounting.virtual_shares),
        (*state.pool.balances, state.pool.invariant, state.pool.accounting.virtual_shares),
        strict=True,
    ):
        assert new == old + old * shares // pool.total_supply
    assert state.pool.ramp == pool.ramp and state.pool.price_scale == pool.price_scale
    assert state.pool.accounting.profit == pool.accounting.profit
    assert economic_pool.virtual_pool(pool, owned, state.timestamp) == state
    assert pool.model_dump_json() == saved
    with pytest.raises(ValueError, match="future observation"):
        economic_pool.virtual_pool(pool, owned, pool.block.timestamp - 1)


def event(pool, signature, values, types=None, *, address=None, timestamp=None):
    return SimpleNamespace(
        topics=(signature,),
        data="0x" + encode(types or ("uint256",) * len(values), values).hex(),
        block=SimpleNamespace(timestamp=timestamp or pool.block.timestamp),
        address=address or pool.source.pool,
    )


def test_ramp_fee_and_factory_receiver_changes_require_complete_ordered_evidence():
    pool, *_ = inputs(CASES[0])
    start = pool.block.timestamp
    a, g = pool.ramp.at(start)
    ramp = event(pool, history.RAMP, (a, a + 100, g, g + 100, start, start + 600))
    fees = event(pool, history.PARAMETERS, (1000000, 30000000, 10**14, 10**12, 10**12, 100))
    receiver = event(
        pool,
        history.RECEIVER,
        (pool.accounting.receiver, "0x" + "12" * 20),
        ("address", "address"),
        address=pool.source.factory,
    )
    parsed = history.parameter_event(fees, pool.source)
    changed = pool.model_copy(
        update={
            "ramp": history.parameter_event(ramp, pool.source),
            "packed_fees": parsed.packed_fees,
            "packed_rebalancing": parsed.packed_rebalancing,
            "accounting": pool.accounting.model_copy(update={"receiver": "0x" + "12" * 20}),
        }
    )
    validate_parameters(pool, changed, (ramp, fees, receiver))
    for removed in range(3):
        with pytest.raises(ValueError, match="complete retained"):
            validate_parameters(
                pool,
                changed,
                tuple(v for i, v in enumerate((ramp, fees, receiver)) if i != removed),
            )
    broken = event(pool, history.RAMP, (a + 1, a + 100, g, g + 100, start, start + 600))
    with pytest.raises(ValueError, match="ramp breaks"):
        validate_parameters(pool, changed, (broken, fees, receiver))
    bad_receiver = event(
        pool,
        history.RECEIVER,
        ("0x" + "ab" * 20, "0x" + "12" * 20),
        ("address", "address"),
        address=pool.source.factory,
    )
    with pytest.raises(ValueError, match="receiver breaks"):
        validate_parameters(pool, changed, (ramp, fees, bad_receiver))
    stop_at = start + 60
    aa, gg = changed.ramp.at(stop_at)
    stop = event(pool, history.STOP, (aa, gg, stop_at), timestamp=stop_at)
    stopped = changed.model_copy(update={"ramp": history.parameter_event(stop, pool.source)})
    validate_parameters(changed, stopped, (stop,))
    assert isinstance(stopped.ramp, CryptoRamp) and stopped.ramp.future_time == stop_at


@pytest.mark.parametrize(
    "attack", ["indexed", "time", "contract", "views", "unknown", "fee", "rebalance"]
)
def test_crypto_parameter_events_fail_closed(attack):
    pool, *_ = inputs(CASES[0])
    a, g = pool.ramp.at(pool.block.timestamp)
    row = event(pool, history.STOP, (a, g, pool.block.timestamp))
    if attack == "indexed":
        row.topics += ("0x" + "00" * 32,)
    elif attack == "time":
        row.block.timestamp += 1
    elif attack == "contract":
        row.address = pool.source.factory
    elif attack == "views":
        row = event(
            pool,
            history.VIEWS,
            (pool.views, pool.views),
            ("address", "address"),
            address=pool.source.factory,
        )
    elif attack == "unknown":
        row.topics = ("0x" + "00" * 32,)
    elif attack == "fee":
        row = event(pool, history.PARAMETERS, (10**10 + 1, 10**10, 10**14, 0, 0, 100))
    else:
        row = event(pool, history.PARAMETERS, (1000000, 30000000, 10**14, 0, 0, 86))
    with pytest.raises(ValueError):
        history.parameter_event(row, pool.source)


def test_crypto_log_scope_and_model_identity_do_not_relabel_stable_datasets():
    pool, *_ = inputs(CASES[0])
    source = pool.source
    scopes = log_scopes(source, pool.block, pool.block)
    assert [(s.address, s.topics) for s in scopes] == [
        (source.pool, history.POOL_TOPICS),
        (source.factory, history.FACTORY_TOPICS),
    ]
    config = SimpleNamespace(allocations=(SimpleNamespace(pool=source),))
    assert model_identity(config) == (CRYPTO_MODEL, CRYPTO_REVISION)
    config.allocations = (
        SimpleNamespace(pool=source.model_copy(update={"family": "stableswap_ng"})),
    )
    assert model_identity(config) == (MODEL, REVISION)
