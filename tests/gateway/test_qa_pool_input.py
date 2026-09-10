"""Exclusive fork-input routing and end-to-end price provenance controls."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
from eth_abi import encode

from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.market.models import PriceData
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.price import qa_pool
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer

WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"


def test_pool_input_client_accepts_literal_loopback_anvil(monkeypatch):
    monkeypatch.setattr(qa_pool, "get_rpc_url", lambda *args, **kwargs: "http://127.0.0.1:8545")
    client = qa_pool.PoolInputRoute._client(object.__new__(qa_pool.PoolInputRoute))
    assert client.provider.endpoint_uri == "http://127.0.0.1:8545"


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1:8545",
        "http://192.0.2.1:8545",
        "http://127.0.0.1",
        "http://user@127.0.0.1:8545",
        "http://127.0.0.1:8545?query=1",
        "http://127.0.0.1:8545/#fragment",
        "http://127.0.0.1:8545/rpc",
    ],
)
def test_pool_input_client_rejects_non_literal_loopback(monkeypatch, endpoint):
    monkeypatch.setattr(qa_pool, "get_rpc_url", lambda *args, **kwargs: endpoint)
    with pytest.raises(ValueError, match="literal loopback"):
        qa_pool.PoolInputRoute._client(object.__new__(qa_pool.PoolInputRoute))


@pytest.fixture
def experiment(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text('{"chain":"arbitrum"}')
    database = tmp_path / "almanak_state.db"
    manifest = tmp_path / "pool-input.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": "lp-dual-test-001",
                "preparation_sha256": "a" * 64,
                "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
                "fork_block": 100,
                "fork_hash": "0x" + "61" * 32,
                "database_path": str(database),
            }
        )
    )
    monkeypatch.setattr(qa_pool, "local_db_path", lambda: database)
    monkeypatch.setattr(qa_pool, "is_local", lambda: True)
    settings = GatewaySettings(network="anvil", chains=["arbitrum"], qa_pool_price_manifest=manifest)
    instance = {"instanceId": "owned-anvil", "forkedNetwork": {"forkBlockNumber": 100}}
    slot0 = encode(["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"], [2**96, 0, 0, 1, 1, 0, True])
    responses = {
        "0x0dfe1681": encode(["address"], [WETH]),
        "0xd21220a7": encode(["address"], [USDC]),
        "0xddca3f43": encode(["uint24"], [500]),
        "0x3850c7bd": slot0,
    }
    calls = []

    def call(tx, block_identifier=None, **kwargs):
        calls.append(block_identifier)
        if tx["data"].startswith("0x1698ee82"):
            return responses.get("factory_pool", encode(["address"], [POOL]))
        if tx["data"].startswith("0x70a08231"):
            return encode(["uint256"], [10**12])
        if tx["data"] == "0x313ce567":
            return encode(["uint8"], [18 if tx["to"].lower() == WETH else 6])
        return responses[tx["data"]]

    client = SimpleNamespace(
        provider=SimpleNamespace(endpoint_uri="http://127.0.0.1:8545", make_request=lambda *args: {"result": instance}),
        eth=SimpleNamespace(
            chain_id=42161,
            call=call,
            get_balance=lambda *args, **kwargs: 10**18,
            get_transaction_count=lambda *args, **kwargs: 5,
            get_code=lambda *args, **kwargs: b"",
            get_block=lambda tag: {
                "number": 101 if tag == "latest" else tag,
                "hash": b"a" * 32,
                "timestamp": 1780000000,
            },
        ),
    )
    monkeypatch.setattr(qa_pool.PoolInputRoute, "_client", lambda self: client)
    monkeypatch.setattr(qa_pool, "get_rpc_url", lambda *args, **kwargs: "http://127.0.0.1:8545")
    return SimpleNamespace(
        settings=settings,
        manifest=manifest,
        config=config,
        client=client,
        instance=instance,
        responses=responses,
        calls=calls,
        root=tmp_path,
    )


def test_price_is_bound_to_durable_raw_witness(experiment):
    route = qa_pool.PoolInputRoute(experiment.settings)
    result = route.get_price(WETH, "USD", "arbitrum")
    assert result.price == Decimal(10**12)
    assert result.source == "qa_fork_pool"
    observation_id = result.source_details["observation_id"]
    raw = (experiment.root / "price-observations" / f"{observation_id}.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == observation_id
    observation = json.loads(raw)
    assert observation["price"] == str(result.price)
    assert observation["measurement_policy"] == {
        "confidence": str(result.confidence),
        "confidence_basis": "fixed_experiment_setting_not_calibrated",
        "stale": result.stale,
        "stale_basis": "uncached_pinned_fork_read",
        "freshness_basis": "gateway_observation_to_consumption_time",
    }
    assert len(observation["raw_reads"]) == 7
    assert observation["fork_identity"]["instance_id"] == "owned-anvil"
    assert experiment.calls == [101] * 7
    assert route.get_price(USDC, "USD", "arbitrum").price == Decimal(1)


@pytest.mark.parametrize(
    "field,value",
    [
        ("network", "mainnet"),
        ("chains", ["base"]),
        ("chains", ["arbitrum", "base"]),
        ("enable_manual_price_overrides", True),
    ],
)
def test_other_gateway_modes_refuse_before_rpc(experiment, field, value):
    setattr(experiment.settings, field, value)
    with pytest.raises(ValueError):
        qa_pool.PoolInputRoute(experiment.settings)
    assert experiment.calls == []


def test_hosted_gateway_cannot_enable_pool_input(experiment, monkeypatch):
    monkeypatch.setattr(qa_pool, "is_local", lambda: False)
    with pytest.raises(ValueError, match="Local SDK"):
        qa_pool.PoolInputRoute(experiment.settings)


@pytest.mark.parametrize(
    "token,quote,chain", [("WBTC", "USD", "arbitrum"), (WETH, "ETH", "arbitrum"), (WETH, "USD", "base")]
)
def test_unsupported_request_has_no_oracle_fallback(experiment, token, quote, chain):
    route = qa_pool.PoolInputRoute(experiment.settings)
    with pytest.raises(ValueError):
        route.get_price(token, quote, chain)
    assert experiment.calls == []


@pytest.mark.parametrize("mutation", ["instance", "config", "manifest", "pair", "missing", "evidence"])
def test_loss_of_bound_evidence_refuses_price(experiment, monkeypatch, mutation):
    route = qa_pool.PoolInputRoute(experiment.settings)
    route.get_price(WETH, "USD", "arbitrum")
    if mutation == "instance":
        experiment.instance["instanceId"] = "replacement-anvil"
    elif mutation == "config":
        experiment.config.write_text("{}")
    elif mutation == "manifest":
        experiment.manifest.write_text("{}")
    elif mutation == "pair":
        experiment.responses["0xddca3f43"] = encode(["uint24"], [3000])
    elif mutation == "missing":
        experiment.responses["0x3850c7bd"] = b""
    else:

        def disk_failed(*args):
            raise OSError("Evidence disk unavailable")

        monkeypatch.setattr(qa_pool, "_retain", disk_failed)
    with pytest.raises((ValueError, OSError)):
        route.get_price(WETH, "USD", "arbitrum")


@pytest.mark.asyncio
async def test_grpc_price_preserves_observation_through_strategy_price_data(experiment):
    service = MarketServiceServicer(experiment.settings)
    service._get_live_price = AsyncMock(side_effect=AssertionError("Live price must not run"))
    response = await service.GetPrice(gateway_pb2.PriceRequest(token=WETH, chain="arbitrum"), MagicMock())
    assert response.observation_id
    client = SimpleNamespace(is_connected=True, market=SimpleNamespace(GetPrice=MagicMock(return_value=response)))
    oracle = GatewayPriceOracle(client)
    result = await oracle.get_aggregated_price(WETH, chain="arbitrum")
    data = PriceData.from_price_result(result)
    assert data.price == Decimal(response.price)
    assert data.observation_id == response.observation_id
    assert data.to_oracle_entry()["observation_id"] == response.observation_id
    client.market.GetPrice.reset_mock()
    snapshot = MarketSnapshotBuilder.for_strategy_runner(
        strategy=SimpleNamespace(price_oracle=oracle), chain="arbitrum", wallet_address=WETH
    )
    assert snapshot.price(WETH) == data.price
    assert snapshot.price_data(WETH).observation_id == response.observation_id
    assert client.market.GetPrice.call_count == 1
    service._get_live_price.assert_not_called()


@pytest.mark.parametrize("raw", [b"", encode(["address"], ["0x" + "00" * 20])])
def test_missing_factory_pool_cannot_produce_a_price(experiment, raw):
    experiment.responses["factory_pool"] = raw
    route = qa_pool.PoolInputRoute(experiment.settings)
    with pytest.raises(ValueError, match="missing|absent pool"):
        route.get_price(WETH, "USD", "arbitrum")
    assert not list(route.evidence.glob("[0-9a-f]" * 64 + ".json"))


def test_missing_address_capability_fails_before_rpc(experiment, monkeypatch):
    monkeypatch.setattr(qa_pool.GATEWAY_REGISTRY, "get", lambda protocol: None)
    monkeypatch.setattr(qa_pool.GATEWAY_REGISTRY, "all", lambda: ())
    route = qa_pool.PoolInputRoute(experiment.settings)
    with pytest.raises(ValueError, match="registered factory"):
        route.get_price(WETH, "USD", "arbitrum")
    assert experiment.calls == []


@pytest.mark.asyncio
async def test_grpc_failure_does_not_fall_through_to_live_prices(experiment):
    service = MarketServiceServicer(experiment.settings)
    service._get_live_price = AsyncMock(side_effect=AssertionError("Live fallback must not run"))
    experiment.config.write_text("{}")
    context = MagicMock()
    result = await service.GetPrice(gateway_pb2.PriceRequest(token=WETH, chain="arbitrum"), context)
    assert result.price == "" and result.observation_id == ""
    context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)
    service._get_live_price.assert_not_called()


@pytest.mark.asyncio
async def test_normal_gateway_retains_existing_price_path():
    service = MarketServiceServicer(GatewaySettings())
    expected = gateway_pb2.PriceResponse(price="2500", source="real-provider")
    service._get_live_price = AsyncMock(return_value=expected)
    assert await service.GetPrice(gateway_pb2.PriceRequest(token="ETH"), MagicMock()) == expected


def _stimulus_route(experiment):
    from almanak.framework.anvil.accounts import anvil_default_address

    payload = json.loads(experiment.manifest.read_text())
    payload.update(stimulus_wallet=anvil_default_address(1), stimulus_usdc_raw=10**12)
    experiment.manifest.write_text(json.dumps(payload))
    return qa_pool.PoolInputRoute(experiment.settings)


@pytest.mark.asyncio
async def test_stimulus_provisioning_retains_measured_balances(experiment):
    route = _stimulus_route(experiment)
    manager = SimpleNamespace(
        anvil_port=8545, fund_wallet=AsyncMock(return_value=True), fund_tokens_report=AsyncMock(return_value=[])
    )
    await route.provision_stimulus(manager, WETH)
    proof = json.loads((route.evidence / "stimulus-provisioning.json").read_text())
    assert proof["usdc_raw"] == str(10**12)
    assert proof["native_wei"] == str(10**18)
    assert proof["block_number"] == 101
    assert proof["nonce"] == 5
    startup = json.loads((route.root / "gateway-startup.json").read_text())
    assert startup["rpc_url"] == "http://127.0.0.1:8545"
    assert startup["subject_wallet"].lower() == WETH.lower()
    assert startup["fork_identity"]["instance_id"] == "owned-anvil"
    assert startup["execution_status"] == "UNMEASURED"
    manager.fund_wallet.assert_awaited_once_with(route.manifest.stimulus_wallet, Decimal(1))
    assert manager.fund_tokens_report.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["subject_wallet", "wrong_rpc", "underfunded", "pending_submission"])
async def test_stimulus_provisioning_refuses_unsafe_or_unmeasured_setup(experiment, fault):
    route = _stimulus_route(experiment)
    manager = SimpleNamespace(
        anvil_port=8545, fund_wallet=AsyncMock(return_value=True), fund_tokens_report=AsyncMock(return_value=[])
    )
    subject_wallet = WETH
    if fault == "subject_wallet":
        subject_wallet = route.manifest.stimulus_wallet
    elif fault == "wrong_rpc":
        manager.anvil_port = 8546
    elif fault == "pending_submission":
        experiment.client.eth.get_transaction_count = lambda wallet, block: 6 if block == "pending" else 5
    else:
        experiment.client.eth.get_balance = lambda *args: 0
    with pytest.raises(ValueError):
        await route.provision_stimulus(manager, subject_wallet)
    assert not (route.evidence / "stimulus-provisioning.json").exists()
    if fault in {"subject_wallet", "wrong_rpc"}:
        manager.fund_wallet.assert_not_awaited()
