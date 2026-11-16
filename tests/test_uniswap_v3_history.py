"""Deployed V3 event layouts constrain continuity checks independently of sampling."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.history_logs import HistoricalLog
from almanak.connectors.uniswap_v3.history import (
    continuous_gross,
    liquidity_event,
    liquidity_logs,
)
from almanak.connectors.uniswap_v3.models import Parameters
from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.receipts import POOL_BURN, POOL_MINT
from tests.exchange_scope import without_exchange_budgets

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-operations.json.gz").read_bytes()
    )
)


def target_events(row):
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    target = config.target(config.allocations[0].name)
    events = []
    for step in row["steps"]:
        block = Snapshot.model_validate_json(json.dumps(step["after"])).block
        for event in step["receipt"]["logs"]:
            if event["address"].lower() != target.pool.pool or event["topics"][0] not in (
                POOL_BURN,
                POOL_MINT,
            ):
                continue
            events.append(
                HistoricalLog(
                    block=block,
                    transaction_hash=event["transactionHash"],
                    transaction_index=int(event["transactionIndex"], 16),
                    log_index=int(event["logIndex"], 16),
                    address=event["address"].lower(),
                    topics=tuple(event["topics"]),
                    data=event["data"],
                )
            )
    return target, tuple(events)


@pytest.mark.parametrize("row", CAPTURE, ids=lambda r: r["origin"]["chain"]["chain_id"])
def test_deployed_liquidity_changes_reconcile_both_range_boundaries(row):
    target, events = target_events(row)
    assert continuous_gross(target, (1000, 2000), events) == (1000, 2000)
    assert len(events) == 6
    assert all(
        liquidity_event(target, e)[:2] == (target.range.lower, target.range.upper) for e in events
    )


def test_boundary_reset_and_forged_source_refuse_fixed_historical_growth():
    target, events = target_events(CAPTURE[0])
    burn = next(e for e in events if e.topics[0] == POOL_BURN and liquidity_event(target, e)[2] < 0)
    amount = -liquidity_event(target, burn)[2]
    with pytest.raises(ValueError, match="cleared"):
        continuous_gross(target, (amount, amount), (burn,))
    with pytest.raises(ValueError, match="initialized"):
        continuous_gross(target, (0, 1), ())
    forged = burn.model_copy(update={"address": target.contracts.position_manager})
    with pytest.raises(ValueError, match="configured V3 pool"):
        continuous_gross(target, (amount * 2, amount * 2), (forged,))


def test_log_limits_reject_before_expanding_collection(monkeypatch):
    target, events = target_events(CAPTURE[0])
    first, last = SimpleNamespace(number=1), SimpleNamespace(number=4001)
    policy = SimpleNamespace(max_liquidity_log_requests=1, max_liquidity_events=1)
    with pytest.raises(ValueError, match="needs 2 liquidity log requests"):
        liquidity_logs(None, target, first, last, policy)
    policy.max_liquidity_log_requests = 2
    monkeypatch.setattr(
        "almanak.connectors.v3_pool_history.read_log_windows", lambda *args: iter([events, events])
    )
    with pytest.raises(ValueError, match="max_liquidity_events limit"):
        liquidity_logs(None, target, first, last, policy)


def test_trading_evidence_retains_pool_events_without_boundary_measurements():
    from almanak.connectors.uniswap_v3.history import BoundaryEvidence

    target, events = target_events(CAPTURE[0])
    trading = target.model_copy(update={"range": None})
    first = events[0].block.model_copy(update={"number": events[0].block.number - 1})
    evidence = dict(target=trading, first=first, last=events[-1].block, contracts=(), inputs=())
    retained = BoundaryEvidence(**evidence, initial_gross=None, final_gross=None, events=events)
    assert retained.events == events
    # A capture made before trading targets retained events still validates.
    assert (
        BoundaryEvidence(**evidence, initial_gross=None, final_gross=None, events=()).events == ()
    )
    with pytest.raises(ValueError, match="only a fixed range can claim boundary measurements"):
        BoundaryEvidence(**evidence, initial_gross=(1, 1), final_gross=(1, 1), events=events)
    forged = events[0].model_copy(update={"address": target.contracts.position_manager})
    with pytest.raises(ValueError, match="configured V3 pool"):
        BoundaryEvidence(**evidence, initial_gross=None, final_gross=None, events=(forged,))
