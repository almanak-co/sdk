"""Aave consumes vendor risk inputs without inventing oracle or block evidence."""

import json
import sqlite3
from types import SimpleNamespace

import pytest

from almanak.backtest.config import EconomicBacktestConfig
from almanak.backtest.economic_book import EconomicBook
from almanak.backtest.economic_prepare import prepare, provision
from almanak.backtest.history import Capture
from almanak.backtest.inputs import HarnessSelection
from almanak.backtest.pricing import CoinGeckoPrice
from almanak.backtest.progress import ProgressJob
from almanak.backtest.vendor import VendorCollector, VendorEvidence, requests_for
from tests.test_economic_math import reserve as reserve
from tests.test_economic_prepare import acquisition as acquisition


def vendor_settings(settings, reserves):
    raw = settings.model_dump(mode="json")
    raw["data"]["risk_prices"] = "configured"
    raw["data"]["vendors"] = [{"name": "prices", "provider": {"api": "demo", "api_key_env": "CG"}}]
    raw["data"]["prices"] = [
        {
            "purpose": "risk",
            "chain": r.chain.model_dump(mode="json"),
            "asset": str(r.asset),
            "source": {
                "kind": "coingecko",
                "provider": "prices",
                "coin_id": "weth" if r.reserve_id == 0 else "usd-coin",
                "quote": "USD",
                "interval": "5m",
                "max_age_seconds": 360,
                "availability": "retrospective_timestamp",
                "availability_lag_seconds": 0,
            },
        }
        for r in dict((r.asset, r) for r in reserves).values()
    ]
    return EconomicBacktestConfig.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("discover", [False, True])
def test_archive_accounting_with_synthetic_vendor_risk_and_policy_binding(
    acquisition, tmp_path, discover
):
    args, original, reserves = acquisition
    settings = vendor_settings(original, reserves)
    if discover:
        raw = settings.model_dump(mode="json")
        for binding in raw["data"]["prices"]:
            binding["source"].pop("coin_id")
        settings = EconomicBacktestConfig.model_validate_json(json.dumps(raw))
    directory = tmp_path / "vendor"
    provision(directory, settings, reserves)
    with sqlite3.connect(args.output / "observations.sqlite") as source:
        with sqlite3.connect(directory / "observations.sqlite") as target:
            source.backup(target)
    capture = Capture(directory / "observations.sqlite")
    start, end = settings.schedule.start, settings.schedule.deadline
    requests = tuple(
        dict.fromkeys(r for b in settings.data.prices for r in requests_for(b, start, end))
    )
    collector = VendorCollector(capture, requests, settings.data.vendors, None)
    for request in requests:
        discovery = None
        coin_id = request.coin_id
        if discover:
            from almanak.backtest.vendor_discovery import Discovery

            asset = request.asset
            weth = next(r.asset for r in reserves if r.reserve_id == 0)
            coin_id = "weth" if asset == weth else "usd-coin"
            discovery = Discovery(
                asset=asset,
                platform="ethereum",
                coin_id=coin_id,
                platforms_response=json.dumps([{"id": "ethereum", "chain_identifier": 1}]),
                coin_response=json.dumps(
                    {"id": coin_id, "platforms": {"ethereum": asset.reference}}
                ),
                retrieved_at=float(end + 1),
            )
        base = 3000 if coin_id == "weth" else 1
        prices = [
            [at * 1000, base if at < start + 300 else base / 2]
            for at in range(start - 300, end + 1, 300)
        ]
        row = VendorEvidence(
            request=request,
            endpoint=f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range",
            discovery=discovery,
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
    path = tmp_path / "vendor-backtest.json"
    path.write_text(settings.model_dump_json())
    seen = []
    with ProgressJob(tmp_path / "vendor.progress.json", "vendor", callback=seen.append):
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
    assert not any("oracle_prices" in p.phase_id for p in seen[-1].phases)
    vendor = next(p for p in seen[-1].phases if p.phase_id == "preparation.vendor_prices")
    assert vendor.finished and vendor.completed_units == len(requests)
    assert result["provider_activity"]["attempts"] == 0
    book = EconomicBook(directory, reserves, start, end, data=settings.data)
    try:
        for reserve in reserves:
            initial = book.at(reserve, start).price
            assert isinstance(initial.vendor, CoinGeckoPrice)
            assert not hasattr(initial, "oracle")
            assert book.at(reserve, start + 300).price.price == initial.price // 2
        assert len(book.vendor_prices) == 2
        changed = settings.data.model_copy(update={"rate_sample_seconds": 900})
        with pytest.raises(ValueError, match="financial data policy"):
            EconomicBook(directory, reserves, start, end, data=changed)
    finally:
        book.close()


def test_vendor_risk_conversion_never_uses_decimal_context_rounding():
    from decimal import Decimal, localcontext

    from almanak.backtest.economic_book import usd_e8

    with localcontext() as context:
        context.prec = 6
        assert usd_e8(Decimal("12345678901234567890.123456789")) == 1234567890123456789012345678


def test_vendor_and_oracle_units_produce_same_liquidation_for_same_prices(reserve):
    from almanak.backtest.economic_data import VendorRiskPrice
    from almanak.backtest.economic_liquidation import settle_liquidations
    from almanak.backtest.economic_math import MarketView
    from tests.test_economic_liquidation import pair

    request = pair(reserve, collateral=10, debt=850)
    positions = (request.collateral, request.debt)
    oracle_markets = (request.collateral_market, request.debt_market)
    vendor_markets = []
    for index, market in enumerate(oracle_markets):
        source = CoinGeckoPrice(
            kind="coingecko",
            provider="synthetic",
            coin_id=f"synthetic-asset-{index}",
            quote="USD",
            interval="hourly",
            max_age_seconds=3700,
            availability="retrospective_timestamp",
            availability_lag_seconds=0,
        )
        vendor_markets.append(
            MarketView(
                supply_index=market.supply_index,
                borrow_index=market.borrow_index,
                risk=market.risk,
                price=VendorRiskPrice(
                    source=market.risk.reserve.source, vendor=source, price=market.price.price
                ),
            )
        )
    expected = settle_liquidations(positions, oracle_markets)
    actual = settle_liquidations(positions, tuple(vendor_markets))
    assert actual == expected
    assert actual[1] and actual[0][1].drawn_shares == 0


def test_oracle_binding_cannot_relabel_another_asset(acquisition):
    from almanak.backtest.economic_data import reserve_key
    from almanak.backtest.economic_prepare import validate_sources
    from almanak.backtest.pricing import PriceBinding, ProtocolPrice

    args, settings, reserves = acquisition
    weth = next(r for r in reserves if r.reserve_id == 0)
    usdc = next(r for r in reserves if r.asset != weth.asset)
    binding = PriceBinding(
        purpose="valuation",
        chain=weth.chain,
        asset=usdc.asset,
        source=ProtocolPrice(kind="protocol_oracle", key=reserve_key(weth, "price")),
    )
    settings = settings.model_copy(
        update={"data": settings.data.model_copy(update={"prices": (binding,)})}
    )
    with pytest.raises(ValueError, match="declared asset"):
        validate_sources(settings, reserves)
