"""Vendor evidence, source identity, visibility and non-EVM price contracts."""

import json
from decimal import Decimal

import httpx
import pytest

from almanak.backtest.dataset import DataKey, Timeline
from almanak.backtest.dependencies import DataDependencies, validate_dependencies
from almanak.backtest.history import Capture
from almanak.backtest.pricing import (
    CoinGeckoPrice,
    CoinGeckoProvider,
    PriceBinding,
    ProtocolPrice,
    VendorBinding,
    price_binding,
)
from almanak.backtest.types import ConnectorId, UnixTime
from almanak.backtest.vendor import (
    VendorCollector,
    VendorEvidence,
    price_records,
    requests_for,
    requirement_for,
    validate_response,
)
from almanak.caip import AssetType, ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Chain


@pytest.fixture
def binding():
    chain = Chain(
        chain_id=ChainId("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"), environment="mainnet"
    )
    return PriceBinding(
        purpose="strategy",
        chain=chain,
        asset=AssetType(str(chain.chain_id) + "/slip44:501"),
        source=CoinGeckoPrice(
            kind="coingecko",
            provider="market_data",
            coin_id="solana",
            quote="USD",
            interval="hourly",
            max_age_seconds=3700,
            availability="retrospective_timestamp",
            availability_lag_seconds=10,
        ),
    )


def evidence(binding, points):
    request = requests_for(binding, 7200, 10810)[0]
    return VendorEvidence(
        request=request,
        endpoint="https://api.coingecko.com/test",
        response=json.dumps({"prices": points}),
        retrieved_at=20000,
    )


def test_vendor_visibility_initialization_and_source_preservation(binding):
    requirement = requirement_for(binding, UnixTime(7200), UnixTime(10810))
    raw = evidence(binding, [[3600000, 10], [7200000, 11], [10800000, 12]])
    rows = price_records(binding, requirement, (raw,))
    timeline = Timeline(rows, requirement)
    assert timeline.at(7200).payload.value == 10
    assert timeline.at(7209).payload.value == 10
    assert timeline.at(7210).payload.value == 11
    assert timeline.at(10810).payload.value == 12
    assert rows[0].source_time == 3600
    assert rows[0].reference.timestamp_ms == 3600000
    with pytest.raises(ValueError, match="initialization"):
        price_records(binding, requirement, (evidence(binding, [[7200000, 11]]),))
    with pytest.raises(ValueError, match="missing data"):
        price_records(binding, requirement, (evidence(binding, [[3600000, 10]]),))
    with pytest.raises(ValueError, match="conflicting"):
        price_records(binding, requirement, (raw, evidence(binding, [[3600000, 20]])))


def test_dex_dependency_has_no_lending_or_evm_fields(binding):
    market = DataKey(
        connector=ConnectorId("example_dex"),
        chain=binding.chain,
        source="pool-explicit",
        product="pool_state",
    )
    deps = DataDependencies(assets=(binding.asset,), markets=(market,), prices=(binding,))
    validate_dependencies(deps, (binding,), {market})
    assert DataDependencies.model_validate_json(deps.model_dump_json()) == deps
    oracle = binding.model_copy(
        update={"source": ProtocolPrice(kind="protocol_oracle", key=market)}
    )
    with pytest.raises(ValueError, match="sources differ"):
        validate_dependencies(deps, (oracle,), {market})
    with pytest.raises(ValueError, match="no supported connector"):
        validate_dependencies(deps, (binding,), set())
    assert price_binding((binding,), "strategy", binding.chain, binding.asset) == binding
    with pytest.raises(ValueError, match="explicit source"):
        price_binding((), "risk", binding.chain, binding.asset)


def test_vendor_gateway_authorization_capture_and_offline_reuse(binding, tmp_path):
    requests = requests_for(binding, 7200, 10810)
    provider = VendorBinding(
        name="market_data", provider=CoinGeckoProvider(api="demo", api_key_env="CG")
    )
    env = tmp_path / "vendor.env"
    env.write_text("CG=fixture-secret\n")
    capture = Capture(tmp_path / "data.sqlite")
    collector = VendorCollector(capture, requests, (provider,), env)
    calls = []

    def respond(request):
        calls.append(request)
        assert request.headers["x-cg-demo-api-key"] == "fixture-secret"
        return httpx.Response(200, json={"prices": [[3600000, 10], [7200000, 11], [10800000, 12]]})

    collector.client.close()
    collector.client = httpx.Client(transport=httpx.MockTransport(respond))
    first = collector.read(requests[0])
    assert collector.read(requests[0]) == first
    assert len(calls) == 1 and "fixture-secret" not in first.model_dump_json()
    with pytest.raises(ValueError, match="outside declared"):
        collector.read(requests[0].model_copy(update={"coin_id": "ethereum"}))
    restored = VendorCollector(capture, requests, (provider,), None)
    assert restored.read(requests[0]) == first
    collector.close()
    restored.close()
    capture.close()


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(403),
        httpx.Response(429),
        httpx.Response(500),
        httpx.Response(200, json={}),
        httpx.Response(200, json={"prices": {}}),
    ],
)
def test_vendor_errors_are_explicit(response):
    with pytest.raises(DataError):
        validate_response(response)


def test_price_precision_and_invalid_mapping(binding):
    requirement = requirement_for(binding, UnixTime(7200), UnixTime(10800))
    raw = evidence(binding, [[3600000, "0.1234567890123456789"], [7200000, "0.12"]])
    assert price_records(binding, requirement, (raw,))[0].payload.value == Decimal(
        "0.1234567890123456789"
    )
    with pytest.raises(ValueError, match="another chain"):
        PriceBinding(
            purpose="strategy",
            chain=binding.chain,
            asset=AssetType("eip155:1/slip44:60"),
            source=binding.source,
        )


def test_vendor_refuses_silent_resolution_change_and_missing_credentials(binding, tmp_path):
    requirement = requirement_for(binding, UnixTime(7200), UnixTime(10810))
    with pytest.raises(ValueError, match="resolution"):
        price_records(
            binding,
            requirement,
            (evidence(binding, [[3600000, 10], [7200000, 11], [7500000, 12], [10800000, 13]]),),
        )
    requests = requests_for(binding, 7200, 10810)
    provider = VendorBinding(
        name="market_data", provider=CoinGeckoProvider(api="demo", api_key_env="CG")
    )
    capture = Capture(tmp_path / "data.sqlite")
    collector = VendorCollector(capture, requests, (provider,), None)
    try:
        with pytest.raises(DataError, match="missing vendor credential CG"):
            collector.read(requests[0])
    finally:
        collector.close()
        capture.close()


def test_vendor_transport_keeps_single_requests_when_overriding_concurrency():
    provider = CoinGeckoProvider.model_validate_json(
        json.dumps({"api": "pro", "api_key_env": "CG", "collection": {"max_in_flight": 8}})
    )
    assert provider.collection.batch_size == 1
    assert provider.collection.max_in_flight == 8
    with pytest.raises(ValueError):
        CoinGeckoProvider.model_validate_json(
            json.dumps({"api": "pro", "api_key_env": "CG", "collection": {"batch_size": 8}})
        )
