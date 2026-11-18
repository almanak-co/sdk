"""Report-level valuation layer over a real EconomicBook and frozen synthetic data."""

import json
from decimal import Decimal

from almanak.backtest.config import EconomicBacktestConfig, financial_policy
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.economic_book import EconomicBook
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
from almanak.backtest.economic_events import EconomicDriver
from almanak.backtest.economic_model import initial_state, parameters
from almanak.backtest.economic_prepare import requirement
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.backtest.pricing import (
    AssetPrice,
    CoinGeckoPrice,
    PriceBinding,
    ProtocolPrice,
    VendorPriceRecord,
    VendorReference,
)
from almanak.backtest.vendor import requirement_for
from almanak.caip import AssetType
from almanak.connectors.aave_v4.economic_binding import result_report
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.models import Asset
from almanak.triggers import IntervalSelection
from tests.fork.eoa import configuration
from tests.test_economic_math import market
from tests.test_economic_math import reserve as reserve
from tests.test_economic_model import simulation as simulation
from tests.test_economic_prepare import acquisition as acquisition

WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
ETH = AssetType("eip155:1/slip44:60")


def vendor_source(coin_id):
    return CoinGeckoPrice(
        kind="coingecko",
        provider="synthetic",
        coin_id=coin_id,
        quote="USD",
        interval="5m",
        max_age_seconds=360,
        availability="retrospective_timestamp",
        availability_lag_seconds=0,
    )


def valuation_run(
    tmp_path, reserve, reference_strategy, bindings, rows, metadata=None, span=21, override=None
):
    raw = configuration()
    from tests.backtest_support import ROOT

    raw["backtest"] = json.loads(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/backtest.json").read_text()
    )
    raw["backtest"]["funds"] = raw["backtest"]["funds"][:1]
    raw["backtest"].update(
        market_model="economic",
        data={
            "source": "aave_v4_archive",
            "rate_sample_seconds": 300,
            "risk_prices": "spoke_oracle_events",
            "prices": [b.model_dump(mode="json") for b in bindings],
        },
    )
    if metadata:
        raw["asset_metadata"] = metadata
    start = reserve.block.timestamp
    raw["backtest"]["schedule"].update(start=start, end=start + span, cleanup_seconds=60)
    if override is not None:
        raw["backtest"]["schedule"]["decision_interval_override_seconds"] = override
    settings = EconomicBacktestConfig.model_validate_json(json.dumps(raw.pop("backtest")))
    author = ForkConfig.model_validate_json(json.dumps(raw))
    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(settings, author, interval)
    state = initial_state(settings, config, interval)
    markets = {}
    for allocation in config.positions:
        source = allocation.reserve
        markets[source] = market(
            reserve.model_copy(
                update={
                    "source": source,
                    "asset": Asset(chain=source.chain, identifier=source.asset, decimals=18),
                }
            ),
            price=3000 * 10**8,
        )
    dataset = tmp_path / "dataset"
    vendor_rows = {b: rows[b.asset] for b in bindings if isinstance(b.source, CoinGeckoPrice)}
    write_dataset(dataset, settings, markets, vendor_rows)
    reserves = tuple(markets)
    book = EconomicBook(dataset, reserves, start, settings.schedule.deadline, data=settings.data)
    journal = Journal(tmp_path / "state.sqlite", EconomicState)
    journal.create(state)
    driver = EconomicDriver(config, settings, reference_strategy, book)
    try:
        result = Simulator(journal, driver).run()
        return result_report(result, driver), result
    finally:
        book.close()


def write_dataset(directory, settings, markets, vendor_rows):
    start, end = settings.schedule.start, settings.schedule.deadline
    products = {}
    for source, view in markets.items():
        observed = view.risk.reserve
        accounting = AccountingSource(
            chain=source.chain,
            hub=source.hub,
            asset=source.asset,
            hub_asset_id=observed.hub_asset_id,
        )
        products[rate_key(accounting)] = (
            RateRecord,
            Rates(
                source=accounting,
                convention="annual_fraction_ray_365_days",
                supply_apr_ray=0,
                borrow_apr_ray=0,
                supply_index_ray=view.supply_index,
                borrow_index_ray=view.borrow_index,
                added_assets=0,
                added_shares=0,
                liquidity_fee_bps=0,
            ),
            observed.block,
            "annual_fraction_ray",
            300,
        )
        products[reserve_key(source, "risk")] = (
            RiskRecord,
            view.risk,
            observed.block,
            "aave_v4_risk_configuration",
            None,
        )
        products[reserve_key(source, "price")] = (
            PriceRecord,
            view.price,
            observed.block,
            "USD_per_asset_e8",
            None,
        )
    requirements = [
        requirement(key, start, end, values[3], values[4]) for key, values in products.items()
    ]
    requirements += [requirement_for(b, start, end) for b in vendor_rows]
    writer = DatasetWriter(
        directory,
        Preparation(
            model=MODEL,
            policy_digest=financial_policy(settings.data),
            acquisition_revision="synthetic-valuation-test",
            requirements=tuple(requirements),
        ),
    )
    raw = RawEvidence(body="explicit synthetic valuation test, no historical or protocol proof")
    try:
        for key, (record_type, payload, block, unit, sample) in products.items():
            times = (start,) if sample is None else tuple(range(start, end + 1, sample))
            records = tuple(
                record_type(
                    key=key,
                    payload=payload,
                    reference=block.model_copy(update={"timestamp": at}),
                    unit=unit,
                    source_time=at,
                    available_at=at,
                    retrieved_at=end,
                    evidence=(raw.digest,),
                )
                for at in times
            )
            writer.commit_range(
                AcquiredRange(key=key, start=start, end=end, evidence=(raw.digest,)),
                records,
                (raw,),
            )
        for binding, points in vendor_rows.items():
            records = tuple(
                VendorPriceRecord(
                    key=binding.key,
                    payload=AssetPrice(
                        asset=binding.asset, source=binding.source, quote="USD", value=value
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
                for at, value in points
            )
            writer.commit_range(
                AcquiredRange(key=binding.key, start=start, end=end, evidence=(raw.digest,)),
                records,
                (raw,),
            )
        return writer.publish()
    finally:
        writer.close()


def test_vendor_valuation_layer_values_the_whole_run(tmp_path, reserve, reference_strategy):
    start = reserve.block.timestamp
    binding = PriceBinding(
        purpose="valuation",
        chain=reserve.source.chain,
        asset=WETH,
        source=vendor_source("weth"),
    )
    rows = {WETH: ((start, Decimal(3000)), (start + 300, Decimal(1500)))}
    report, state = valuation_run(
        tmp_path, reserve, reference_strategy, (binding,), rows, span=300, override=60
    )
    json.dumps(report)
    assert report["valuation"] == "explicit_sources"
    assert report["valuation_quote"] == "USD"
    curve = report["equity_curve"]
    assert curve[0] == {
        "time": start,
        "value": "120",
        "complete": False,
        "unvalued_assets": [str(ETH)],
    }
    assert curve[-1]["time"] == int(state.time)
    assert curve[-1]["value"] == "60"
    eligible = {int(c.pending.eligible_at) for c in state.completed}
    assert eligible <= {point["time"] for point in curve}
    assert {start, start + 300} <= {point["time"] for point in curve}
    series = report["price_series"]
    assert [point["time"] for point in series] == [point["time"] for point in curve]
    assert series[0]["prices"] == {str(WETH): "3000"}
    assert series[-1]["prices"] == {str(WETH): "1500"}
    assert report["metrics"] == {
        "total_return_pct": -50.0,
        "max_drawdown_pct": 50.0,
        "sharpe_ratio": None,
    }
    assert report["net_quote_return"] == {"quote": "USD", "value": "-60", "return_pct": -50.0}
    assert report["valuation_prices"][0]["value"] == "1500"


def test_valuation_is_complete_when_every_held_asset_is_priced(
    tmp_path, reserve, reference_strategy
):
    start = reserve.block.timestamp
    bindings = (
        PriceBinding(
            purpose="valuation",
            chain=reserve.source.chain,
            asset=WETH,
            source=vendor_source("weth"),
        ),
        PriceBinding(
            purpose="valuation",
            chain=reserve.source.chain,
            asset=ETH,
            source=vendor_source("ethereum"),
        ),
    )
    rows = {WETH: ((start, Decimal(3000)),), ETH: ((start, Decimal(2000)),)}
    metadata = {str(ETH): {"decimals": 18}}
    report, _ = valuation_run(tmp_path, reserve, reference_strategy, bindings, rows, metadata)
    assert all(point["complete"] for point in report["equity_curve"])
    # 5 native ETH plus 0.04 WETH, with constant prices.
    assert report["equity_curve"][0]["value"] == "10120"
    assert report["net_quote_return"] == {"quote": "USD", "value": "0", "return_pct": 0.0}
    assert report["metrics"]["max_drawdown_pct"] == 0.0


def test_protocol_oracle_binding_values_positions_from_reserve_oracle(
    tmp_path, reserve, reference_strategy
):
    from tests.backtest_support import ROOT

    main = reserve.source
    binding = PriceBinding(
        purpose="valuation",
        chain=main.chain,
        asset=WETH,
        market=reserve_key(main, "price"),
        source=ProtocolPrice(kind="protocol_oracle", key=reserve_key(main, "price")),
    )
    report, _ = valuation_run(tmp_path, reserve, reference_strategy, (binding,), {})
    assert report["price_series"][0]["prices"] == {str(WETH): "3000"}
    assert report["equity_curve"][0]["value"] == "120"
    assert report["equity_curve"][0]["complete"] is False
    assert report["net_quote_return"] == {"quote": "USD", "value": "0", "return_pct": 0.0}
    settings_path = ROOT / "strategies/aave_v4_weth_supply/backtest-economic.yaml"
    assert settings_path.read_text().count("purpose: valuation") == 3


def test_report_without_valuation_bindings_keeps_asset_units_only(simulation):
    journal, model = simulation
    result = Simulator(journal, model).run()
    report = result_report(result, model)
    assert report["valuation"] == "asset_units_only"
    assert report["valuation_prices"] == []
    assert report["net_quote_return"] is None
    for field in ("equity_curve", "price_series", "metrics", "valuation_quote"):
        assert field not in report


def test_valuation_layer_is_absent_without_any_usable_observation(simulation):
    from types import SimpleNamespace

    from almanak.connectors.aave_v4.economic_valuation import valuation_layer

    journal, model = simulation
    binding = PriceBinding(
        purpose="valuation",
        chain=next(iter(model.book.markets)).chain,
        asset=WETH,
        source=vendor_source("weth"),
    )
    model.settings = model.settings.model_copy(
        update={"data": model.settings.data.model_copy(update={"prices": (binding,)})}
    )
    model.book = SimpleNamespace(
        risks={},
        binding_timeline=lambda *args: (_ for _ in ()).throw(ValueError("no requirement")),
    )
    assert valuation_layer(journal.load(), model) == {}


def test_valuation_bindings_freeze_vendor_prices_at_preparation(acquisition, tmp_path):
    import sqlite3
    from types import SimpleNamespace

    from almanak.backtest.economic_prepare import prepare, provision
    from almanak.backtest.history import Capture
    from almanak.backtest.inputs import HarnessSelection
    from almanak.backtest.vendor import VendorCollector, VendorEvidence, requests_for

    args, original, reserves = acquisition
    weth = next(r for r in reserves if r.reserve_id == 0)
    raw = original.model_dump(mode="json")
    raw["data"]["vendors"] = [{"name": "prices", "provider": {"api": "demo", "api_key_env": "CG"}}]
    raw["data"]["prices"] = [
        {
            "purpose": "valuation",
            "chain": weth.chain.model_dump(mode="json"),
            "asset": str(weth.asset),
            "source": {
                "kind": "coingecko",
                "provider": "prices",
                "coin_id": "weth",
                "quote": "USD",
                "interval": "5m",
                "max_age_seconds": 360,
                "availability": "retrospective_timestamp",
                "availability_lag_seconds": 0,
            },
        }
    ]
    settings = EconomicBacktestConfig.model_validate_json(json.dumps(raw))
    directory = tmp_path / "valuation"
    provision(directory, settings, reserves)
    with sqlite3.connect(args.output / "observations.sqlite") as source:
        with sqlite3.connect(directory / "observations.sqlite") as target:
            source.backup(target)
    capture = Capture(directory / "observations.sqlite")
    start, end = settings.schedule.start, settings.schedule.deadline
    binding = settings.data.prices[0]
    requests = tuple(dict.fromkeys(requests_for(binding, start, end)))
    collector = VendorCollector(capture, requests, settings.data.vendors, None)
    for request in requests:
        prices = [[at * 1000, 3000] for at in range(start - 300, end + 1, 300)]
        row = VendorEvidence(
            request=request,
            endpoint="https://api.coingecko.com/api/v3/coins/weth/market_chart/range",
            response=json.dumps({"prices": prices}),
            retrieved_at=end + 1,
        )
        with capture.connect() as db:
            db.execute(
                "INSERT INTO vendor_observations VALUES (?, ?)",
                (request.key, row.model_dump_json()),
            )
    collector.close()
    capture.close()
    path = tmp_path / "valuation-backtest.json"
    path.write_text(settings.model_dump_json())
    result = prepare(
        SimpleNamespace(
            config=args.config,
            backtest_config=path,
            output=directory,
            env_file=None,
            _backtest_inputs=HarnessSelection(
                args._backtest_inputs.author, settings, args._backtest_inputs.strategy
            ),
        )
    )
    assert result["provider_activity"]["attempts"] == 0
    book = EconomicBook(directory, reserves, start, end, data=settings.data)
    try:
        assert len(book.vendor_prices) == 1
        observations = book.observations("valuation", start + 60)
        assert [str(o.binding.asset) for o in observations] == [str(weth.asset)]
        assert observations[0].value == Decimal(3000)
    finally:
        book.close()
