"""Synthetic timestamp groups verify observer units without changing retained inputs."""

import importlib
import json
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.backtest.progress import ProgressJob
from almanak.connectors.morpho_blue.domain import MarketObservation
from tests.test_aave_v3_risk import position
from tests.test_compound_v3_economic_data import market
from tests.test_morpho_blue_reader import recorded


def observation(connector):
    if connector == "aave_v3":
        return position(0).reserve
    if connector == "compound_v3":
        return market()
    return MarketObservation.model_validate_json(json.dumps(recorded(1, False)["market"]))


def retime(connector, observed, block):
    if connector == "aave_v3":
        fields = {"oracle": observed.oracle.model_copy(update={"block": block})}
    elif connector == "compound_v3":
        fields = {
            "base_price": observed.base_price.model_copy(update={"block": block}),
            "collaterals": tuple(
                item.model_copy(update={"price": item.price.model_copy(update={"block": block})})
                for item in observed.collaterals
            ),
        }
    else:
        fields = {"price": observed.price.model_copy(update={"block": block})}
    return observed.model_copy(update={"block": block, **fields})


def inputs(connector):
    data = importlib.import_module(f"almanak.connectors.{connector}.economic_data")
    original = observation(connector)
    start = original.block.timestamp
    settings = SimpleNamespace(
        schedule=SimpleNamespace(start=start, deadline=start + 20),
        data=SimpleNamespace(sample_seconds=10, max_age_seconds=30),
    )
    headers = tuple(
        original.block.model_copy(
            update={"number": original.block.number + i, "timestamp": start + t}
        )
        for i, t in enumerate((0, 10, 10, 10, 20, 20, 30))
    )
    risk = data.requirement(original.source, settings, "risk")
    accounting = data.requirement(original.source, settings, "accounting")
    raw = RawEvidence(body='{"fixture":"synthetic repeated timestamps; no fidelity claim"}')
    rows = []
    for left, right in ((0, 1), (1, 4), (4, 6)):
        group, following = headers[left:right], headers[right]
        block = group[-1]
        observed = retime(connector, original, block)
        payload = (
            {"reserve": observed, "liquidation_grace_until": 0}
            if connector == "aave_v3"
            else {"market": observed}
        )
        sample = data.RiskSample(block_group=group, following_block=following, **payload)
        fields = dict(
            source_time=block.timestamp,
            available_at=block.timestamp,
            retrieved_at=float(start + 30),
            reference=block,
            evidence=(raw.digest,),
        )
        rows.append(
            (
                data.RiskRecord(key=risk.key, unit=risk.unit, payload=sample, **fields),
                data.RateRecord(
                    key=accounting.key, unit=accounting.unit, payload=observed, **fields
                ),
            )
        )
    return data, original, settings, headers, risk, accounting, raw, tuple(rows)


def retain(writer, record, deadline, following, raw):
    span = AcquiredRange(
        key=record.key,
        start=record.available_at,
        end=min(deadline, following.timestamp - 1),
        evidence=record.evidence,
    )
    writer.commit_range(span, (record,), (raw,))


@pytest.mark.parametrize("connector", ("aave_v3", "compound_v3", "morpho_blue"))
@pytest.mark.parametrize("cached_groups", (0, 2))
def test_risk_progress_counts_raw_headers_and_excludes_cached_work_from_throughput(
    connector, cached_groups, tmp_path, monkeypatch
):
    data, original, settings, headers, risk, accounting, raw, rows = inputs(connector)
    acquisition = importlib.import_module(f"almanak.connectors.{connector}.economic_acquisition")
    prepare = importlib.import_module(f"almanak.connectors.{connector}.economic_prepare")
    writer = DatasetWriter(
        tmp_path / "dataset",
        Preparation(
            model=data.MODEL,
            acquisition_revision=data.REVISION,
            policy_digest="a" * 64,
            requirements=(risk, accounting),
        ),
    )
    expected = [json.loads(row.model_dump_json()) for row, _ in rows]
    for risk_row, rate_row in rows:
        retain(writer, rate_row, settings.schedule.deadline, risk_row.payload.following_block, raw)
    for risk_row, _ in rows[:cached_groups]:
        retain(writer, risk_row, settings.schedule.deadline, risk_row.payload.following_block, raw)
    by_time = {row.available_at: row for row, _ in rows}
    calls = []

    def sample(history, config, source, at, options):
        calls.append(at)
        return by_time[at], (raw,)

    monkeypatch.setattr(acquisition, "risk_sample", sample)
    source = (
        SimpleNamespace(name="collateral", reserve=original.source)
        if connector == "aave_v3"
        else original.source
    )
    try:
        with ProgressJob(tmp_path / "progress.json", "synthetic-risk-groups") as job:
            counts = []
            original_advance = job.advance

            def advance(units=1.0):
                original_advance(units)
                counts.append(job.phases[job.active].completed)

            monkeypatch.setattr(job, "advance", advance)
            prepare.collect(writer, None, None, source, settings, (headers[0], headers[-2]))
            phase = next(iter(job.phases.values()))
            assert phase.unit == "blocks"
            assert phase.total == phase.completed == 6
            assert phase.baseline == (4 if cached_groups else 0)
            assert counts == ([6] if cached_groups else [1, 4, 6])
        assert calls == [row.available_at for row, _ in rows[cached_groups:]]
        retained = [
            json.loads(record.model_dump_json())
            for span in writer.ranges(risk.key)
            for record in writer.range_records(span)
        ]
        assert retained == expected
        writer.publish()
    finally:
        writer.close()
