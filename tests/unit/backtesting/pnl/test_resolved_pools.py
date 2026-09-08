"""Dynamic explicit-pool resolution, pinning, and offline replay contracts."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors._strategy_base.v3_pool_abi import encode_get_pool
from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_manifest import RunDataManifest
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability, MarketState
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import HistoricalPoolStatePoint
from almanak.framework.backtesting.pnl.resolved_pools import (
    ConfiguredPoolReference,
    PoolResolutionError,
    _authenticate_factory,
    _deployment_block,
    _resolve_pair_address,
    _resolve_reference_blocking,
    extract_configured_pool_references,
    resolve_configured_pool_descriptors,
)
from almanak.framework.data.pools.descriptor import ResolvedPoolDescriptor
from tests.backtesting_funding import pnl_token_funding

CHAIN = "base"
POOL = "0xaff8" + "0" * 36
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
VNXAU = "0xac3fe22294beaed9d1fd752323a6d06d12ff3098"
FACTORY = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
START = datetime(2026, 7, 1, tzinfo=UTC)


def _descriptor(**overrides: Any) -> ResolvedPoolDescriptor:
    values = {
        "chain": CHAIN,
        "protocol": "uniswap_v3",
        "address": POOL,
        "token0": USDC,
        "token1": VNXAU,
        "token0_decimals": 6,
        "token1_decimals": 18,
        "fee_tier_units": 500,
        "provenance": "historical:test",
        "factory": FACTORY,
        "discriminator_kind": "fee_tier",
        "discriminator": 500,
        "deployment_block": 37,
    }
    values.update(overrides)
    return ResolvedPoolDescriptor(**values)


def _config(*, descriptors: tuple[ResolvedPoolDescriptor, ...] = ()) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=1),
        interval_seconds=3600,
        chain=CHAIN,
        tokens=[(CHAIN, USDC), (CHAIN, VNXAU)],
        token_funding=pnl_token_funding(Decimal("100"), chain=CHAIN),
        include_gas_costs=False,
        preflight_validation=False,
        resolved_pool_descriptors=descriptors,
    )


def test_extracts_direct_address_and_offline_pair_references() -> None:
    address_strategy = SimpleNamespace(protocol="uniswap_v3", pool_address=POOL)
    address_refs = extract_configured_pool_references(
        address_strategy,
        {"chain": CHAIN, "protocol": "uniswap_v3", "pool_address": POOL},
        default_chain=CHAIN,
    )
    assert len(address_refs) == 1
    assert address_refs[0].address == POOL

    pair_refs = extract_configured_pool_references(
        SimpleNamespace(protocol="uniswap_v3"),
        {
            "chain": CHAIN,
            "protocol": "uniswap_v3",
            "pool": "USDC/VNXAU/500",
            "base_token": {"symbol": "USDC", "address": USDC},
            "quote_token": {"symbol": "VNXAU", "address": VNXAU},
        },
        default_chain=CHAIN,
    )
    assert len(pair_refs) == 1
    assert (pair_refs[0].token_a, pair_refs[0].token_b, pair_refs[0].discriminator) == (USDC, VNXAU, 500)


@pytest.mark.parametrize(
    ("token_key", "address_key"),
    (
        ("base_token", "base_token_address"),
        ("quote_token", "quote_token_address"),
        ("token0_token", "token0_token_address"),
        ("token1_token", "token1_token_address"),
    ),
)
def test_pair_resolution_uses_paired_address_for_mapping_without_usable_nested_address(
    token_key: str,
    address_key: str,
) -> None:
    refs = extract_configured_pool_references(
        SimpleNamespace(protocol="uniswap_v3"),
        {
            "chain": CHAIN,
            "protocol": "uniswap_v3",
            "pool": "USDC/VNXAU/500",
            token_key: {"symbol": "USDC", "address": "not-an-address"},
            address_key: USDC,
            "token_funding": ({"symbol": "VNXAU", "address": VNXAU},),
        },
        default_chain=CHAIN,
    )

    assert len(refs) == 1
    assert (refs[0].token_a, refs[0].token_b) == (USDC, VNXAU)


@pytest.mark.asyncio
async def test_pinned_descriptor_replay_performs_no_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = _descriptor()
    strategy = SimpleNamespace(protocol="uniswap_v3", pool_address=POOL)
    config = _config(descriptors=(descriptor,))
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: pytest.fail("pinned replay attempted RPC discovery"),
    )

    resolved = await resolve_configured_pool_descriptors(
        strategy,
        {"chain": CHAIN, "protocol": "uniswap_v3", "pool_address": POOL},
        config,
    )

    assert resolved == (descriptor,)


def test_pair_resolution_uses_factory_metadata_without_liquidity(monkeypatch: pytest.MonkeyPatch) -> None:
    reference = ConfiguredPoolReference(
        chain=CHAIN,
        protocol="uniswap_v3",
        source_key="config.pool",
        token_a=USDC,
        token_b=VNXAU,
    )
    spec = POOL_READER_REGISTRY.require("uniswap_v3")
    token0, token1 = sorted((USDC, VNXAU))
    calls: list[str] = []

    def eth_call(_client: Any, _chain: str, _to: str, data: str, _block: int) -> str:
        calls.append(data)
        matching = encode_get_pool(spec.get_pool_selector, token0, token1, 500)
        address = POOL[2:] if data == matching else "0" * 40
        return "0x" + "0" * 24 + address

    monkeypatch.setattr("almanak.framework.backtesting.pnl.resolved_pools._eth_call", eth_call)

    assert _resolve_pair_address(object(), reference, 123) == POOL
    assert len(calls) == len(spec.factories_for(CHAIN)) * len(spec.candidate_pool_keys)


def test_deployment_block_is_binary_searched_from_code_history() -> None:
    class Rpc:
        def __init__(self) -> None:
            self.blocks: list[int] = []

        def Call(self, request: Any, timeout: float) -> Any:  # noqa: N802 - protobuf service shape
            assert timeout == 1
            _, raw_block = json.loads(request.params)
            block = int(raw_block, 16)
            self.blocks.append(block)
            return SimpleNamespace(success=True, result=json.dumps("0x6000" if block >= 37 else "0x"), error="")

    rpc = Rpc()
    client = SimpleNamespace(rpc=rpc, config=SimpleNamespace(timeout=1))
    gateway_pb2 = SimpleNamespace(RpcRequest=lambda **kwargs: SimpleNamespace(**kwargs))

    assert _deployment_block(client, gateway_pb2, CHAIN, POOL, 100) == 37
    assert rpc.blocks[0] == 100
    assert len(rpc.blocks) <= 9


def test_blocking_resolution_pins_boundary_identity_factory_and_deployment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = ConfiguredPoolReference(
        chain=CHAIN,
        protocol="uniswap_v3",
        source_key="config.pool_address",
        address=POOL,
    )
    samples: list[int] = []

    def boundary(_protocol: str, _chain: str, _address: str, sample: int, _interval: int) -> HistoricalPoolStatePoint:
        samples.append(sample)
        return HistoricalPoolStatePoint(
            timestamp=sample,
            block_number=100 if len(samples) == 1 else 200,
            sqrt_price_x96=1,
            tick=0,
            liquidity=1,
            token0=USDC,
            token1=VNXAU,
            token0_decimals=6,
            token1_decimals=18,
            fee_tier=500,
            reserve0_raw=1,
            reserve1_raw=1,
            source="archive:test",
        )

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools.get_connected_gateway_client",
        lambda: (object(), object()),
    )
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools.require_historical_pool_state",
        lambda _protocol: None,
    )
    monkeypatch.setattr("almanak.framework.backtesting.pnl.resolved_pools._boundary_point", boundary)
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._authenticate_factory",
        lambda *_args: ("fee_tier", 500, FACTORY),
    )
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._deployment_block",
        lambda *_args: 37,
    )

    descriptor = _resolve_reference_blocking(reference, start_ts=10, end_ts=20, interval_seconds=3600)

    assert descriptor == _descriptor(provenance="historical:archive:test")
    assert samples == [10, 20]


def test_factory_authentication_refuses_missing_discriminator_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = ConfiguredPoolReference(
        chain=CHAIN,
        protocol="uniswap_v3",
        source_key="config.pool_address",
        address=POOL,
    )
    point = HistoricalPoolStatePoint(
        timestamp=10,
        block_number=100,
        sqrt_price_x96=1,
        tick=0,
        liquidity=1,
        token0=USDC,
        token1=VNXAU,
        token0_decimals=6,
        token1_decimals=18,
        fee_tier=500,
        reserve0_raw=1,
        reserve1_raw=1,
        source="archive:test",
    )
    spec = SimpleNamespace(
        discriminator_kind=SimpleNamespace(value="none"),
        factories_for=lambda _chain: (FACTORY,),
    )
    monkeypatch.setattr(POOL_READER_REGISTRY, "require", lambda _protocol: spec)

    with pytest.raises(PoolResolutionError, match="declares factories but no 'none' discriminator"):
        _authenticate_factory(object(), reference, POOL, point)


def test_config_hash_and_manifest_include_pinned_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = _descriptor()
    without = _config()
    with_descriptor = _config(descriptors=(descriptor,))

    real_dumps = json.dumps
    hash_payload: dict[str, Any] = {}

    def capture_hash_payload(value: Any, *args: Any, **kwargs: Any) -> str:
        if isinstance(value, dict) and kwargs.get("sort_keys") is True:
            hash_payload.clear()
            hash_payload.update(value)
        return real_dumps(value, *args, **kwargs)

    monkeypatch.setattr("almanak.framework.backtesting.pnl.config.json.dumps", capture_hash_payload)
    without_hash = without.calculate_config_hash()
    assert "resolved_pool_descriptors" not in hash_payload

    with_descriptor_hash = with_descriptor.calculate_config_hash()
    assert hash_payload["resolved_pool_descriptors"] == [descriptor.to_dict()]
    assert with_descriptor_hash != without_hash
    restored = PnLBacktestConfig.from_dict(with_descriptor.to_dict())
    assert restored.resolved_pool_descriptors == (descriptor,)
    assert restored.calculate_config_hash() == with_descriptor.calculate_config_hash()

    manifest = RunDataManifest()
    manifest.pin_pool_descriptors((descriptor,))
    payload = manifest.to_dict()
    assert payload["schema_version"] == 3
    assert payload["resolved_pool_descriptors"] == [descriptor.to_dict()]
    payload["resolved_pool_descriptors"][0]["address"] = "mutated"
    assert manifest.to_dict()["resolved_pool_descriptors"] == [descriptor.to_dict()]


@pytest.mark.asyncio
async def test_incomplete_pinned_descriptor_refuses_without_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    strategy = SimpleNamespace(protocol="uniswap_v3", pool_address=POOL)
    config = _config(descriptors=(_descriptor(deployment_block=None),))
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: pytest.fail("invalid pin attempted RPC discovery"),
    )

    with pytest.raises(PoolResolutionError, match="has no deployment_block"):
        await resolve_configured_pool_descriptors(
            strategy,
            {"chain": CHAIN, "protocol": "uniswap_v3", "pool_address": POOL},
            config,
        )


@pytest.mark.asyncio
async def test_wrong_factory_pin_refuses_without_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    strategy = SimpleNamespace(protocol="uniswap_v3", pool_address=POOL)
    config = _config(descriptors=(_descriptor(factory="0x" + "11" * 20),))
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: pytest.fail("wrong-factory pin attempted RPC discovery"),
    )

    with pytest.raises(PoolResolutionError, match="connector declares"):
        await resolve_configured_pool_descriptors(
            strategy,
            {"chain": CHAIN, "protocol": "uniswap_v3", "pool_address": POOL},
            config,
        )


@pytest.mark.asyncio
async def test_conflicting_exact_pin_refuses_instead_of_rediscovery(monkeypatch: pytest.MonkeyPatch) -> None:
    strategy = SimpleNamespace(protocol="uniswap_v3", pool_address=POOL)
    config = _config(descriptors=(_descriptor(),))
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: pytest.fail("conflicting pin attempted RPC discovery"),
    )

    with pytest.raises(PoolResolutionError, match="conflicts with the configured discriminator"):
        await resolve_configured_pool_descriptors(
            strategy,
            {"chain": CHAIN, "protocol": "uniswap_v3", "pool_address": POOL, "fee_tier": 3000},
            config,
        )


class _ReplayProvider:
    provider_name = "pinned_replay"
    historical_capability = HistoricalDataCapability.FULL

    def __init__(self) -> None:
        self.iterate_calls = 0

    async def iterate(self, config: PnLBacktestConfig):
        self.iterate_calls += 1
        for timestamp in (config.start_time, config.end_time):
            yield (
                timestamp,
                MarketState(
                    timestamp=timestamp,
                    prices={(CHAIN, USDC): Decimal("1"), (CHAIN, VNXAU): Decimal("2500")},
                    chain=CHAIN,
                ),
            )


class _PoolReadingStrategy:
    deployment_id = "dynamic-pool-replay"
    protocol = "uniswap_v3"
    pool_address = POOL
    config = {"chain": CHAIN, "protocol": protocol, "pool_address": POOL}

    def __init__(self) -> None:
        self.decide_calls = 0

    def decide(self, market: Any) -> None:
        self.decide_calls += 1
        market.pool_price(POOL)
        return None


@pytest.mark.asyncio
async def test_public_preflight_does_not_mutate_caller_config(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config()
    original = config.to_dict()
    descriptor = _descriptor()
    strategy = _PoolReadingStrategy()
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: descriptor,
    )
    backtester = PnLBacktester(data_provider=_ReplayProvider(), fee_models={}, slippage_models={})

    await backtester.run_preflight_validation(config, strategy=strategy)

    assert config.to_dict() == original
    assert config.resolved_pool_descriptors == ()


@pytest.mark.asyncio
async def test_backtest_full_preflight_persists_run_local_pool_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config()
    config.preflight_validation = True
    config.fail_on_preflight_error = False
    descriptor = _descriptor()
    strategy = _PoolReadingStrategy()
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: descriptor,
    )
    backtester = PnLBacktester(data_provider=_ReplayProvider(), fee_models={}, slippage_models={})

    result = await backtester.backtest(strategy, config)

    assert result.error is None
    assert result.config is not None
    assert result.config["resolved_pool_descriptors"] == [descriptor.to_dict()]
    assert config.resolved_pool_descriptors == ()


@pytest.mark.asyncio
async def test_pinned_run_is_offline_and_persists_descriptor_in_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = _descriptor()
    provider = _ReplayProvider()
    strategy = _PoolReadingStrategy()
    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools._resolve_reference_blocking",
        lambda *_args, **_kwargs: pytest.fail("pinned run attempted RPC discovery"),
    )
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={},
        slippage_models={},
        resolved_pool_descriptors={descriptor.manifest_key: descriptor},
    )

    result = await backtester.backtest(strategy, _config())

    assert result.error is None
    assert provider.iterate_calls == 1
    assert strategy.decide_calls == 2
    assert result.config is not None
    assert result.config["resolved_pool_descriptors"] == [descriptor.to_dict()]
    assert result.data_manifest is not None
    assert result.data_manifest["resolved_pool_descriptors"] == [descriptor.to_dict()]


@pytest.mark.asyncio
async def test_unresolved_explicit_pool_fails_before_ticks_even_when_general_preflight_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ReplayProvider()
    strategy = _PoolReadingStrategy()

    async def fail_resolution(*_args: Any, **_kwargs: Any) -> tuple[ResolvedPoolDescriptor, ...]:
        raise PoolResolutionError("factory authentication failed")

    monkeypatch.setattr(
        "almanak.framework.backtesting.pnl.resolved_pools.resolve_configured_pool_descriptors",
        fail_resolution,
    )
    backtester = PnLBacktester(data_provider=provider, fee_models={}, slippage_models={})

    with pytest.raises(PreflightValidationError) as raised:
        await backtester.backtest(strategy, _config())

    assert raised.value.code == "POOL_RESOLUTION_FAILED"
    assert provider.iterate_calls == 0
    assert strategy.decide_calls == 0


@pytest.mark.asyncio
async def test_constructor_and_config_pin_conflict_is_a_preflight_failure_before_ticks() -> None:
    provider = _ReplayProvider()
    strategy = _PoolReadingStrategy()
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={},
        slippage_models={},
        resolved_pool_descriptors=(_descriptor(),),
    )

    with pytest.raises(PreflightValidationError, match="Conflicting pinned pool identities") as raised:
        await backtester.backtest(strategy, _config(descriptors=(_descriptor(deployment_block=38),)))

    assert raised.value.code == "POOL_RESOLUTION_FAILED"
    assert provider.iterate_calls == 0
    assert strategy.decide_calls == 0
