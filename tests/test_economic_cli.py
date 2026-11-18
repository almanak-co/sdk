"""CLI lifecycle against explicitly synthetic prepared inputs, with HTTP forbidden."""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.backtest import cli, economic_cli
from almanak.backtest.config import financial_policy
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.economic_data import (
    MODEL,
    AccountingSource,
    PriceRecord,
    RateRecord,
    Rates,
    RiskRecord,
    rate_key,
    reserve_key,
)
from almanak.backtest.economic_prepare import requirement
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from tests.test_economic_math import reserve as reserve
from tests.test_economic_model import simulation as simulation


def synthetic_dataset(directory, model):
    start, end = model.settings.schedule.start, model.settings.schedule.deadline
    products = {}
    for source, market in model.book.markets.items():
        reserve = market.risk.reserve
        accounting = AccountingSource(
            chain=source.chain,
            hub=source.hub,
            asset=source.asset,
            hub_asset_id=reserve.hub_asset_id,
        )
        products[rate_key(accounting)] = (
            RateRecord,
            Rates(
                source=accounting,
                convention="annual_fraction_ray_365_days",
                supply_apr_ray=0,
                borrow_apr_ray=0,
                supply_index_ray=market.supply_index,
                borrow_index_ray=market.borrow_index,
                added_assets=0,
                added_shares=0,
                liquidity_fee_bps=0,
            ),
            reserve.block,
            "annual_fraction_ray",
            300,
        )
        products[reserve_key(source, "risk")] = (
            RiskRecord,
            market.risk,
            reserve.block,
            "aave_v4_risk_configuration",
            None,
        )
        products[reserve_key(source, "price")] = (
            PriceRecord,
            market.price,
            reserve.block,
            "USD_per_asset_e8",
            None,
        )
    requirements = tuple(
        requirement(key, start, end, values[3], values[4]) for key, values in products.items()
    )
    from almanak.backtest.pricing import (
        AssetPrice,
        CoinGeckoPrice,
        VendorPriceRecord,
        VendorReference,
    )
    from almanak.backtest.vendor import requirement_for

    # Bindings with different purposes may share one vendor key; like
    # production preparation, the harness keeps the first binding per key.
    vendor_by_key = {}
    for binding in model.settings.data.prices:
        if isinstance(binding.source, CoinGeckoPrice):
            vendor_by_key.setdefault(binding.key, binding)
    vendor_bindings = tuple(vendor_by_key.values())
    requirements += tuple(requirement_for(binding, start, end) for binding in vendor_bindings)
    writer = DatasetWriter(
        directory,
        Preparation(
            model=MODEL,
            policy_digest=financial_policy(model.settings.data),
            acquisition_revision="synthetic-cli-test",
            requirements=requirements,
        ),
    )
    raw = RawEvidence(body="explicit synthetic CLI test, no historical or protocol proof")
    try:
        for key, (record_type, payload, block, unit, _) in products.items():
            record = record_type(
                key=key,
                payload=payload,
                reference=block,
                unit=unit,
                source_time=start,
                available_at=start,
                retrieved_at=end,
                evidence=(raw.digest,),
            )
            writer.commit_range(
                AcquiredRange(key=key, start=start, end=end, evidence=(raw.digest,)),
                (record,),
                (raw,),
            )
        for binding in vendor_bindings:
            records = tuple(
                VendorPriceRecord(
                    key=binding.key,
                    payload=AssetPrice(
                        asset=binding.asset,
                        source=binding.source,
                        quote="USD",
                        value=Decimal(3000),
                    ),
                    reference=VendorReference(
                        coin_id=binding.source.coin_id,
                        endpoint="https://synthetic.invalid/range",
                        timestamp_ms=at * 1000,
                    ),
                    unit="USD_per_asset",
                    source_time=at,
                    available_at=at,
                    retrieved_at=end,
                    evidence=(raw.digest,),
                )
                for at in range(start, end + 1, binding.source.seconds)
            )
            writer.commit_range(
                AcquiredRange(key=binding.key, start=start, end=end, evidence=(raw.digest,)),
                records,
                (raw,),
            )
        return writer.publish()
    finally:
        writer.close()


def test_offline_cli_run_resume_reproduce_and_content_binding(simulation, tmp_path, monkeypatch):
    import httpx

    def forbidden(*args, **kwargs):
        raise AssertionError("economic CLI cannot call HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    journal, model = simulation
    dataset = tmp_path / "data"
    synthetic_dataset(dataset, model)
    author = model.config
    strategy = tmp_path / "strategy.py"
    strategy.write_text(model.strategy.source)
    args = SimpleNamespace(dataset=dataset, strategy=strategy, output=tmp_path / "run")
    report = economic_cli.create(args, author, model.settings)
    assert report["closed"] and report["completed_steps"] == 4
    assert report["provider_requests_during_execution"] == 0
    assert all(int(p["gross_supply_yield"]) == 0 for p in report["positions"])
    retained = SimpleNamespace(run=args.output)
    assert cli.resume(retained)["decision_evaluations"] == report["decision_evaluations"]
    assert cli.reproduce(retained)["reproduced"]
    manifest = json.loads((dataset / "dataset.json").read_text())
    manifest["records"] += 1
    (dataset / "dataset.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="manifest differs"):
        cli.resume(retained)


def test_automatic_preparation_reuses_data_and_freezes_policy(simulation, tmp_path, monkeypatch):
    from almanak.backtest import economic_prepare
    from tests.backtest_support import ROOT

    journal, model = simulation
    author = model.config
    args = SimpleNamespace(
        strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
        config=tmp_path / "author.json",
        backtest_config=tmp_path / "backtest.json",
        output=tmp_path / "run",
        env_file=None,
    )
    preparations = []

    def prepare(request):
        preparations.append(request.output)
        if not (request.output / "dataset.json").exists():
            synthetic_dataset(request.output, model)

    monkeypatch.setattr(economic_prepare, "prepare", prepare)
    first = economic_cli.create(args, author, model.settings)
    args.output = tmp_path / "second-run"
    second = economic_cli.create(args, author, model.settings)
    assert preparations[0] == preparations[1]
    assert first["wallets"] == second["wallets"]
    assert second["provider_requests_during_execution"] == 0
    assert second["end_to_end_seconds"] >= second["elapsed_seconds"]


@pytest.fixture
def public_dataset(tmp_path, reserve):
    """A public root resolved once over the synthetic archive, with a synthetic dataset."""
    import yaml

    from almanak.backtest.economic_model import parameters
    from almanak.backtest.economic_prepare import provision
    from almanak.backtest.inputs import selected
    from almanak.triggers import IntervalSelection
    from tests.backtest_support import ROOT, Archive, archive_server, converted_config, iso
    from tests.test_economic_model import FixedBook

    data = Archive()
    server, thread = archive_server(data)
    try:
        config, operator, env_file = converted_config(
            tmp_path, data, server, "backtest-economic.yaml"
        )
        # The synthetic dataset holds one rate sample at the fixture block, like
        # the simulation fixture, so the window starts at that block's time.
        settings_path = config.with_name("backtest-economic.yaml")
        raw = yaml.safe_load(settings_path.read_text())
        raw["window"].update(start=iso(data.start), end=iso(data.start + 21), cleanup="60s")
        raw["execution"].pop("decision_interval")
        settings_path.write_text(yaml.safe_dump(raw, sort_keys=False))
        dataset = tmp_path / "dataset"
        args = SimpleNamespace(
            config=config,
            backtest_config=config.with_name("backtest-economic.yaml"),
            gateway_config=operator,
            strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
            dataset=None,
            output=dataset,
            env_file=env_file,
        )
        selection = selected(args)
        author, settings = selection.resolve(dataset, env_file)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    reserves = tuple(dict.fromkeys(a.reserve for a in author.allocations))
    provision(dataset, settings, reserves, selection.resolution_facts)
    config = parameters(settings, author, IntervalSelection(configured=author.decision_trigger))
    synthetic_dataset(dataset, SimpleNamespace(settings=settings, book=FixedBook(config, reserve)))
    return args, dataset, selection.resolution_facts


def test_public_run_reuses_the_dataset_resolution_without_credentials(
    public_dataset, tmp_path, monkeypatch
):
    import httpx

    from almanak.configuration.bootstrap import read_progress

    def forbidden(*args, **kwargs):
        raise AssertionError("a run over a prepared dataset cannot call HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    args, dataset, facts = public_dataset
    run = SimpleNamespace(**{**vars(args), "dataset": dataset, "output": tmp_path / "run"})
    run.env_file = None
    report = cli.create_run(run)
    assert report["closed"] and report["provider_requests_during_execution"] == 0
    assert read_progress(run.output / "resolution.json").result.facts_digest == facts
    manifest = json.loads((run.output / "manifest.json").read_text())
    assert manifest["resolution_facts"] == facts
    progress = read_progress(dataset / "resolution.json")
    assert manifest["resolution_identity"] == progress.result.digest
    assert manifest["config"]["accounts"] == read_progress(
        dataset / "resolution.json"
    ).result.binding.accounts.model_dump(mode="json")
    retained = SimpleNamespace(run=run.output)
    assert cli.resume(retained)["decision_evaluations"] == report["decision_evaluations"]
    assert cli.reproduce(retained)["reproduced"]
    resolution = run.output / "resolution.json"
    original = resolution.read_text()
    raw = json.loads(original)
    raw["context"]["identity"] = "f" * 32
    from almanak.configuration.resolution import ReadContext
    from almanak.gateway.bootstrap import ResolutionResult

    context = ReadContext.model_validate_json(json.dumps(raw["context"]))
    for binding in (raw["binding"], raw["result"]["binding"]):
        binding["context_digest"] = context.digest
    raw["result"]["anchors"]["context_digest"] = context.digest
    result = ResolutionResult.model_validate_json(json.dumps(raw["result"]))
    raw["result_digest"] = result.digest
    resolution.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="resolution identity differs"):
        cli.resume(retained)
    with pytest.raises(ValueError, match="resolution identity differs"):
        cli.reproduce(retained)
    resolution.write_text(original)
    (run.output / "resolution.json").unlink()
    with pytest.raises(ValueError, match="requires its retained resolution"):
        cli.resume(retained)


def test_dataset_reuse_requires_matching_resolution_facts(public_dataset, tmp_path, monkeypatch):
    import httpx

    monkeypatch.setattr(httpx.Client, "send", lambda *a, **k: pytest.fail("HTTP is forbidden"))
    args, dataset, facts = public_dataset
    acquisition = dataset / "acquisition.json"
    raw = json.loads(acquisition.read_text())
    assert raw["resolution_facts"] == facts
    raw["resolution_facts"] = "0" * 64
    acquisition.write_text(json.dumps(raw))
    run = SimpleNamespace(**{**vars(args), "dataset": dataset, "output": tmp_path / "run"})
    run.env_file = None
    with pytest.raises(ValueError, match="different resolved market facts"):
        cli.create_run(run)
    assert not (tmp_path / "run" / "manifest.json").exists()
    # A harness dataset without retained facts cannot serve a public root either.
    acquisition.unlink()
    with pytest.raises(ValueError, match="different resolved market facts"):
        cli.create_run(SimpleNamespace(**{**vars(run), "output": tmp_path / "second"}))


def test_synthetic_dataset_keeps_the_first_binding_per_shared_vendor_key(simulation, tmp_path):
    from almanak.backtest.economic_book import EconomicBook
    from almanak.backtest.pricing import CoinGeckoPrice, PriceBinding

    journal, model = simulation
    reserve = next(iter(model.book.markets))
    source = CoinGeckoPrice(
        kind="coingecko",
        provider="synthetic",
        coin_id="weth",
        quote="USD",
        interval="5m",
        max_age_seconds=360,
        availability="retrospective_timestamp",
        availability_lag_seconds=0,
    )
    bindings = tuple(
        PriceBinding(purpose=purpose, chain=reserve.chain, asset=reserve.asset, source=source)
        for purpose in ("valuation", "strategy")
    )
    assert bindings[0].key == bindings[1].key
    model.settings = model.settings.model_copy(
        update={"data": model.settings.data.model_copy(update={"prices": bindings})}
    )
    path = tmp_path / "shared-vendor-key"
    manifest = synthetic_dataset(path, model)
    assert [r.key for r in manifest.preparation.requirements].count(bindings[0].key) == 1
    start, end = model.settings.schedule.start, model.settings.schedule.deadline
    book = EconomicBook(path, tuple(model.book.markets), start, end, data=model.settings.data)
    try:
        for purpose in ("valuation", "strategy"):
            rows = book.observations(purpose, start)
            assert [row.binding.purpose for row in rows] == [purpose]
    finally:
        book.close()


def test_explicit_oracle_observations_reuse_prepared_timeline(simulation, tmp_path, monkeypatch):
    from almanak.backtest.economic_book import EconomicBook
    from almanak.backtest.pricing import PriceBinding, ProtocolPrice

    journal, model = simulation
    bindings = tuple(
        PriceBinding(
            purpose="valuation",
            chain=r.chain,
            asset=r.asset,
            market=reserve_key(r, "price"),
            source=ProtocolPrice(kind="protocol_oracle", key=reserve_key(r, "price")),
        )
        for r in model.book.markets
    )
    model.settings = model.settings.model_copy(
        update={"data": model.settings.data.model_copy(update={"prices": bindings})}
    )
    path = tmp_path / "oracle-observations"
    synthetic_dataset(path, model)
    start, end = model.settings.schedule.start, model.settings.schedule.deadline
    book = EconomicBook(path, tuple(model.book.markets), start, end, data=model.settings.data)

    def forbidden(*args):
        raise AssertionError("an observation must not reread the entire prepared timeline")

    monkeypatch.setattr(book.reader, "timeline", forbidden)
    try:
        for time in (start, start + 1, end):
            rows = book.observations("valuation", time)
            assert all(row.value == 3000 and row.binding in bindings for row in rows)
            assert len(rows) == len(bindings)
    finally:
        book.close()
