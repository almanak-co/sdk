"""Synthetic datasets check Morpho evidence boundaries, policy freezing and reports."""

from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable, RawEvidence
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.connectors.morpho_blue.authority import declared
from almanak.connectors.morpho_blue.economic_book import Book
from almanak.connectors.morpho_blue.economic_config import EconomicConfig
from almanak.connectors.morpho_blue.economic_data import (
    MODEL,
    REVISION,
    RateRecord,
    RiskRecord,
    RiskSample,
)
from almanak.connectors.morpho_blue.economic_events import EconomicDriver
from almanak.connectors.morpho_blue.economic_parameters import identity, initial, requirements
from almanak.connectors.morpho_blue.economic_prepare import provision
from almanak.connectors.morpho_blue.economic_reporting import report
from almanak.connectors.morpho_blue.economic_state import State
from almanak.worker_session import preloaded_workers
from tests.test_morpho_blue_config import parameters as parameters
from tests.test_morpho_blue_economic_runtime import runtime as runtime


@pytest.fixture
def dataset(runtime, tmp_path):
    config, old, strategy, interval, market = runtime
    settings = EconomicConfig(
        market_model="economic",
        schedule=old.schedule,
        funds=old.funds,
        costs={"kind": "excluded"},
        liquidation_model="immediate_morpho_liquidation",
        history=(
            {
                "chain": market.source.chain,
                "provider": {
                    "kind": "alchemy",
                    "network": "eth-mainnet",
                    "api_key_env": "SYNTHETIC_UNUSED_KEY",
                },
            },
        ),
        data={
            "source": "morpho_blue_archive",
            "risk_model": "morpho_oracle_each_block",
            "sample_seconds": 10,
            "max_age_seconds": 100,
            "max_accounting_samples": 10,
            "max_risk_blocks": 10,
            "max_risk_samples": 10,
            "max_liquidations_per_event": 10,
        },
    )
    directory = tmp_path / "inputs"
    preparation = Preparation(
        model=MODEL,
        acquisition_revision=REVISION,
        policy_digest=identity(config, settings),
        requirements=requirements(config, settings),
    )
    writer = DatasetWriter(directory, preparation)
    raw = RawEvidence(body='{"fixture":"synthetic timestamps; captured market payload"}')
    try:
        for needed in preparation.requirements:
            for index, at in enumerate(range(needed.start, needed.end + 1, 10)):
                block = market.block.model_copy(
                    update={"number": market.block.number + index, "timestamp": at}
                )
                following = block.model_copy(
                    update={"number": block.number + 1, "timestamp": at + 10}
                )
                observed = market.model_copy(
                    update={
                        "block": block,
                        "price": market.price.model_copy(update={"block": block}),
                    }
                )
                fields = dict(
                    key=needed.key,
                    unit=needed.unit,
                    source_time=at,
                    available_at=at,
                    retrieved_at=float(at),
                    evidence=(raw.digest,),
                    reference=block,
                )
                record = (
                    RateRecord(payload=observed, **fields)
                    if needed.key.product == "accounting"
                    else RiskRecord(
                        payload=RiskSample(
                            market=observed, block_group=(block,), following_block=following
                        ),
                        **fields,
                    )
                )
                writer.commit_range(
                    AcquiredRange(
                        key=needed.key,
                        start=at,
                        end=min(at + 9, needed.end),
                        evidence=(raw.digest,),
                    ),
                    (record,),
                    (raw,),
                )
        writer.publish()
    finally:
        writer.close()
    return SimpleNamespace(
        config=config,
        settings=settings,
        strategy=strategy,
        interval=interval,
        market=market,
        directory=directory,
    )


def test_dataset_reopens_and_drives_reference_with_explicit_report(dataset, tmp_path):
    d = dataset
    book = Book(d.directory, d.config, d.settings)
    try:
        source, start = d.market.source, d.settings.schedule.start
        assert book.at(source, start).source == source
        assert book.evidence(source, start)
        assert book.next_boundary(start, start + 20) == start + 10
        with pytest.raises(InputUnavailable):
            book.at(source, start - 1)
        driver = EconomicDriver(
            d.config, d.settings, d.strategy, book, authority=declared(d.config)
        )
        journal = Journal(tmp_path / "state.sqlite", State)
        journal.create(initial(d.settings, d.config, d.interval))
        with preloaded_workers():
            state = Simulator(journal, driver).run()
        result = report(state, driver)
        assert result["closed"] and result["completed_intents"] == 9
        assert {p["ownership_unit"] for p in result["positions"]} == {
            "morpho_loan_shares",
            "collateral_base_units",
        }
        assert all(
            p["measurement_status"] == "excluded" and p["cost"] is None
            for p in result["gas_payers"]
        )
    finally:
        book.close()
    changed = d.settings.model_copy(
        update={"data": d.settings.data.model_copy(update={"sample_seconds": 20})}
    )
    with pytest.raises(ValueError, match="frozen model or data policy"):
        Book(d.directory, d.config, changed)


def test_acquisition_directory_retains_policy_and_private_token(dataset, tmp_path):
    d = dataset
    path = tmp_path / "acquisition"
    path.mkdir()
    provision(path, d.config, d.settings)
    token = (path / "gateway-auth.token").read_bytes()
    provision(path, d.config, d.settings)
    assert (path / "gateway-auth.token").read_bytes() == token
    assert (path / "gateway-auth.token").stat().st_mode & 0o777 == 0o600
    changed = d.settings.model_copy(
        update={"data": d.settings.data.model_copy(update={"sample_seconds": 20})}
    )
    with pytest.raises(ValueError, match="cannot change"):
        provision(path, d.config, changed)


def test_acquisition_bounds_refuse_unfinalized_and_excessive_windows(dataset):
    from almanak.connectors.morpho_blue.economic_acquisition import bounds

    d = dataset
    first = d.market.block
    last = first.model_copy(
        update={"number": first.number + 4, "timestamp": d.settings.schedule.deadline}
    )
    upper = last.model_copy(update={"number": last.number + 1})
    history = SimpleNamespace(
        floor=lambda at: first if at == d.settings.schedule.start else last, upper=upper
    )
    histories = {first.chain: history}
    assert bounds(histories, d.config, d.settings) == {first.chain: (first, last)}
    changed = d.settings.model_copy(
        update={"data": d.settings.data.model_copy(update={"max_risk_blocks": 4})}
    )
    with pytest.raises(ValueError, match="block or sample limit"):
        bounds(histories, d.config, changed)
    history.upper = last
    with pytest.raises(ValueError, match="finalized following"):
        bounds(histories, d.config, d.settings)


def test_risk_timestamp_groups_preserve_all_blocks_and_enforce_bound(dataset):
    from almanak.connectors.morpho_blue.economic_acquisition import group_at

    block = dataset.market.block
    headers = {
        block.number - 2: block.model_copy(
            update={"number": block.number - 2, "timestamp": block.timestamp - 1}
        ),
        block.number - 1: block.model_copy(update={"number": block.number - 1}),
        block.number: block,
        block.number + 1: block.model_copy(
            update={"number": block.number + 1, "timestamp": block.timestamp + 1}
        ),
    }
    history = SimpleNamespace(floor=lambda at: block, header=headers.__getitem__)
    group, following = group_at(history, block.timestamp, block.timestamp - 10, 2)
    assert group == (headers[block.number - 1], block)
    assert following == headers[block.number + 1]
    with pytest.raises(ValueError, match="explicit block limit"):
        group_at(history, block.timestamp, block.timestamp - 10, 1)
    headers[block.number + 1] = following.model_copy(update={"timestamp": block.timestamp})
    with pytest.raises(ValueError, match="latest eligible block"):
        group_at(history, block.timestamp, block.timestamp, 2)


def test_preparation_checks_exact_provider_scope_and_sample_budget(dataset):
    from almanak.connectors.morpho_blue.economic_prepare import validate_sources

    d = dataset
    args = SimpleNamespace(strategy=None)
    config = validate_sources(args, d.config, d.settings)
    assert config.target("loan").fund == d.config.target("loan").fund
    missing = d.settings.model_copy(update={"history": ()})
    with pytest.raises(ValueError, match="exactly the configured chains"):
        validate_sources(args, d.config, missing)
    bounded = d.settings.model_copy(
        update={"data": d.settings.data.model_copy(update={"max_accounting_samples": 1})}
    )
    with pytest.raises(ValueError, match="explicit sample limit"):
        validate_sources(args, d.config, bounded)
