"""Contract discovery preserves exact identity across capture and offline use."""

import json
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from almanak.backtest.economic_book import validate_vendor_row
from almanak.backtest.history import Capture
from almanak.backtest.pricing import CoinGeckoProvider, PriceBinding, VendorBinding
from almanak.backtest.vendor import VendorCollector, price_records, requests_for, requirement_for
from almanak.backtest.vendor_discovery import Discovery, coin_for, platform_for
from almanak.caip import AssetType
from tests.test_vendor_prices import binding as binding

WBTC = AssetType("eip155:1/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
PLATFORMS = [{"id": "ethereum", "chain_identifier": 1}]


def auto_binding(binding, asset=WBTC):
    raw = binding.model_dump(mode="json")
    raw["chain"] = {"chain_id": str(asset.chain), "environment": "mainnet"}
    raw["asset"] = str(asset)
    raw["source"].pop("coin_id")
    return PriceBinding.model_validate_json(json.dumps(raw))


def metadata(asset, coin_id):
    return {"id": coin_id, "platforms": {"ethereum": asset.reference.upper()}}


def discovered(asset=WBTC, coin_id="wrapped-bitcoin"):
    return Discovery(
        asset=asset,
        platform="ethereum",
        coin_id=coin_id,
        platforms_response=json.dumps(PLATFORMS),
        coin_response=json.dumps(metadata(asset, coin_id)),
        retrieved_at=100.0,
    )


def collector_for(tmp_path, bindings):
    requests = tuple(r for b in bindings for r in requests_for(b, 7200, 10810))
    env = tmp_path / "vendor.env"
    env.write_text("CG=fixture-secret\n")
    capture = Capture(tmp_path / "capture.sqlite")
    vendors = (
        VendorBinding(name="market_data", provider=CoinGeckoProvider(api="demo", api_key_env="CG")),
    )
    return VendorCollector(capture, requests, vendors, env), requests


def test_discovery_two_assets_and_offline_price_identity(binding, tmp_path):
    bindings = (auto_binding(binding), auto_binding(binding, USDC))
    collector, requests = collector_for(tmp_path, bindings)
    calls = []

    def respond(request):
        calls.append(request.url.path)
        if request.url.path.endswith("asset_platforms"):
            return httpx.Response(200, json=PLATFORMS)
        for asset, coin_id in [(WBTC, "wrapped-bitcoin"), (USDC, "usd-coin")]:
            if request.url.path.endswith(asset.reference):
                return httpx.Response(200, json=metadata(asset, coin_id))
        value = 60000 if "wrapped-bitcoin" in request.url.path else 0.98
        return httpx.Response(
            200, json={"prices": [[3600000, value], [7200000, value], [10800000, value]]}
        )

    collector.client.close()
    collector.client = httpx.Client(transport=httpx.MockTransport(respond))
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(collector.resolve, requests * 4))
    evidence = [collector.read(r) for r in requests]
    assert len(calls) == 6
    with collector.capture.connect() as db:
        assert (
            db.execute("SELECT COUNT(DISTINCT request) FROM provider_activity").fetchone()[0] == 6
        )
    assert [e.discovery.coin_id for e in evidence] == ["wrapped-bitcoin", "usd-coin"]
    assert all("fixture-secret" not in e.model_dump_json() for e in evidence)
    collector.close()
    vendors = (
        VendorBinding(name="market_data", provider=CoinGeckoProvider(api="demo", api_key_env="CG")),
    )
    restored = VendorCollector(collector.capture, requests, vendors, None)
    assert [restored.read(r) for r in requests] == evidence
    restored.close()
    for selected, item in zip(bindings, evidence, strict=True):
        rows = price_records(selected, requirement_for(selected, 7200, 10810), (item,))
        for row in rows:
            validate_vendor_row(row, selected)
        assert rows[0].reference.discovery.coin_id == item.discovery.coin_id
        assert rows[0].payload.source.coin_id is None
        wrong = rows[0].model_copy(
            update={"reference": rows[0].reference.model_copy(update={"coin_id": "bitcoin"})}
        )
        with pytest.raises(ValueError, match="discovered coin"):
            validate_vendor_row(wrong, selected)
    assert evidence[1].response.find("0.98") >= 0
    collector.capture.close()


def test_discovery_freezes_before_history_failure_and_parallel_chunks(binding, tmp_path):
    selected = auto_binding(binding)
    collector, requests = collector_for(tmp_path, (selected,))
    request = requests[0]
    calls = []

    def respond(req):
        calls.append(req.url.path)
        if req.url.path.endswith("asset_platforms"):
            return httpx.Response(200, json=PLATFORMS)
        if "/contract/" in req.url.path:
            return httpx.Response(200, json=metadata(WBTC, "wrapped-bitcoin"))
        return httpx.Response(403)

    collector.client.close()
    collector.client = httpx.Client(transport=httpx.MockTransport(respond))
    from almanak.gateway.ethereum import DataError

    with pytest.raises(DataError):
        collector.read(request)
    assert len(calls) == 3
    collector.close()
    vendors = (
        VendorBinding(name="market_data", provider=CoinGeckoProvider(api="demo", api_key_env="CG")),
    )
    restored = VendorCollector(collector.capture, requests, vendors, None)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(restored.resolve, [request] * 8))
    assert len(set(r.model_dump_json() for r in results)) == 1
    assert results[0].coin_id == "wrapped-bitcoin"
    restored.close()
    collector.capture.close()


@pytest.mark.parametrize(
    "rows",
    [
        [],
        PLATFORMS * 2,
        [{"id": "ethereum", "chain_identifier": True}],
        [{"id": "../evil", "chain_identifier": 1}],
    ],
)
def test_discovery_rejects_unknown_ambiguous_or_invalid_platform(rows):
    with pytest.raises(ValueError):
        platform_for(WBTC, json.dumps(rows))


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"id": "bitcoin", "platforms": {"ethereum": USDC.reference}},
        {"id": "../bitcoin", "platforms": {"ethereum": WBTC.reference}},
    ],
)
def test_discovery_rejects_wrong_contract_and_invalid_coin(row):
    with pytest.raises(ValueError):
        coin_for(WBTC, "ethereum", json.dumps(row))


def test_discovery_requires_supported_asset_and_verified_evidence(binding):
    with pytest.raises(ValueError, match="EVM ERC-20"):
        auto_binding(binding, AssetType("eip155:1/slip44:60"))
    raw = discovered().model_dump(mode="json")
    raw["coin_id"] = "bitcoin"
    with pytest.raises(ValueError, match="lookup evidence"):
        Discovery.model_validate_json(json.dumps(raw))


def test_auto_history_refuses_missing_wrong_or_changed_discovery(binding, tmp_path):
    from almanak.backtest.vendor import VendorEvidence

    selected = auto_binding(binding)
    request = requests_for(selected, 7200, 10810)[0]
    item = VendorEvidence(
        request=request,
        endpoint="https://pro-api.coingecko.com/api/v3/coins/wrapped-bitcoin/market_chart/range",
        response=json.dumps({"prices": [[3600000, 60000], [7200000, 60000], [10800000, 60000]]}),
        retrieved_at=20000.0,
        discovery=discovered(),
    )
    requirement = requirement_for(selected, 7200, 10810)
    for changed in [
        item.model_copy(update={"discovery": None}),
        item.model_copy(update={"discovery": discovered(USDC, "usd-coin")}),
        item.model_copy(
            update={
                "endpoint": "https://pro-api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
            }
        ),
    ]:
        with pytest.raises(ValueError):
            price_records(selected, requirement, (changed,))
    with pytest.raises(ValueError, match="conflicting discovery"):
        price_records(
            selected,
            requirement,
            (item, item.model_copy(update={"discovery": discovered(WBTC, "bitcoin")})),
        )


def test_discovery_lookup_failure_never_falls_back(binding, tmp_path):
    from almanak.gateway.ethereum import DataError

    selected = auto_binding(binding)
    collector, requests = collector_for(tmp_path, (selected,))
    calls = []

    def respond(request):
        calls.append(request.url.path)
        return httpx.Response(404)

    collector.client.close()
    collector.client = httpx.Client(transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(DataError, match="HTTP 404"):
            collector.read(requests[0])
        assert calls == ["/api/v3/asset_platforms"]
        with collector.capture.connect() as db:
            assert db.execute("SELECT COUNT(*) FROM vendor_discovery").fetchone()[0] == 0
    finally:
        collector.close()
        collector.capture.close()
