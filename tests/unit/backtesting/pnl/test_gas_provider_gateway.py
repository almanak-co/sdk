"""Gateway-backed gas provider tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.providers import gas as gas_mod
from almanak.framework.backtesting.pnl.providers.gas import (
    DEFAULT_GAS_PRICES,
    EtherscanGasPriceProvider,
    GasPrice,
    GasPriceDataCache,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class _FakeRateHistory:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.requests: list[Any] = []

    def GetGasPriceAt(self, request: Any) -> Any:  # noqa: N802 - gRPC stub name
        self.requests.append(request)
        return self.response


class _FakeClient:
    is_connected = True

    def __init__(self, response: Any) -> None:
        self.rate_history = _FakeRateHistory(response)


class _FakePb2:
    @staticmethod
    def GetGasPriceAtRequest(**kwargs: Any) -> SimpleNamespace:  # noqa: N802 - proto ctor name
        return SimpleNamespace(**kwargs)


def _response(
    *,
    success: bool = True,
    source: str = "archive_rpc",
    error: str = "",
    chain: str = "ethereum",
    timestamp: int = 1_700_000_000,
    base_fee_gwei: str = "20",
    priority_fee_gwei: str = "",
    gas_price_gwei: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        success=success,
        source=source,
        error=error,
        chain=chain,
        point=SimpleNamespace(
            timestamp=timestamp,
            base_fee_gwei=base_fee_gwei,
            priority_fee_gwei=priority_fee_gwei,
            gas_price_gwei=gas_price_gwei,
        ),
    )


@pytest.mark.asyncio
async def test_get_gas_price_calls_gateway_and_decodes_empty_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(_response())
    monkeypatch.setattr(gas_mod, "_get_connected_gateway_client", lambda: (fake_client, _FakePb2()))

    provider = EtherscanGasPriceProvider()
    target = datetime.fromtimestamp(1_700_000_000, tz=UTC)

    gas_price = await provider.get_gas_price(target, chain="ethereum")

    assert fake_client.rate_history.requests == [SimpleNamespace(chain="ethereum", timestamp=1_700_000_000)]
    assert gas_price.chain == "ethereum"
    assert gas_price.base_fee_gwei == Decimal("20")
    assert gas_price.priority_fee_gwei is None
    assert gas_price.gas_price_gwei is None
    assert gas_price.source == "archive_rpc"
    assert gas_price.confidence is DataConfidence.HIGH


@pytest.mark.asyncio
async def test_get_current_gas_price_sends_zero_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(
        _response(
            source="etherscan",
            timestamp=1_700_000_123,
            base_fee_gwei="10",
            priority_fee_gwei="1",
            gas_price_gwei="11",
        )
    )
    monkeypatch.setattr(gas_mod, "_get_connected_gateway_client", lambda: (fake_client, _FakePb2()))

    provider = EtherscanGasPriceProvider()
    gas_price = await provider.get_gas_price(chain="ethereum")

    assert fake_client.rate_history.requests[0].timestamp == 0
    assert gas_price.effective_gas_price_gwei == Decimal("11")
    assert gas_price.source == "etherscan"


@pytest.mark.asyncio
async def test_gateway_unavailable_falls_back_to_config_only(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(_response(success=False, source="archive_rpc", error="no data"))
    monkeypatch.setattr(gas_mod, "_get_connected_gateway_client", lambda: (fake_client, _FakePb2()))

    provider = EtherscanGasPriceProvider(
        data_config=SimpleNamespace(gas_fallback_gwei=Decimal("7.5")),  # type: ignore[arg-type]
    )
    target = datetime.fromtimestamp(1_700_000_000, tz=UTC)

    gas_price = await provider.get_gas_price(target, chain="ethereum")

    assert gas_price.gas_price_gwei == Decimal("7.5")
    assert gas_price.source == "config_fallback"
    assert gas_price.confidence is DataConfidence.LOW


@pytest.mark.asyncio
async def test_unset_gas_fallback_falls_through_to_chain_aware_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """A data_config that leaves gas_fallback_gwei unset must not pin a flat
    Ethereum-shaped fallback on other chains (the VIB-5088 defect class)."""
    fake_client = _FakeClient(_response(success=False, chain="arbitrum", error="no data"))
    monkeypatch.setattr(gas_mod, "_get_connected_gateway_client", lambda: (fake_client, _FakePb2()))

    provider = EtherscanGasPriceProvider(data_config=BacktestDataConfig())
    target = datetime.fromtimestamp(1_700_000_000, tz=UTC)

    gas_price = await provider.get_gas_price(target, chain="arbitrum")

    defaults = DEFAULT_GAS_PRICES["arbitrum"]
    assert gas_price.gas_price_gwei == defaults["base_fee"] + defaults["priority_fee"]
    assert gas_price.source == "config_fallback"
    assert gas_price.confidence is DataConfidence.LOW


def test_file_backed_cache_uses_on_demand_connections(tmp_path: Any) -> None:
    cache = GasPriceDataCache(str(tmp_path / "gas.db"))
    target = datetime.fromtimestamp(1_700_000_000, tz=UTC)

    assert cache._conn is None
    cache.set(GasPrice(timestamp=target, chain="ethereum", gas_price_gwei=Decimal("9"), source="test"))

    cached = cache.get("ethereum", target)

    assert cached is not None
    assert cached.gas_price_gwei == Decimal("9")
    cache.close()


def test_persistent_cache_does_not_synthesize_interpolated_values() -> None:
    cache = GasPriceDataCache(":memory:")
    before = datetime.fromtimestamp(1_700_000_000, tz=UTC)
    middle = datetime.fromtimestamp(1_700_000_300, tz=UTC)
    after = datetime.fromtimestamp(1_700_000_600, tz=UTC)
    cache.set_batch(
        [
            GasPrice(timestamp=before, chain="ethereum", gas_price_gwei=Decimal("10"), source="before"),
            GasPrice(timestamp=after, chain="ethereum", gas_price_gwei=Decimal("20"), source="after"),
        ]
    )

    assert cache.get_interpolated("ethereum", middle) is None
    assert cache.get_interpolated("ethereum", before) is not None
    cache.close()


def test_persistent_cache_range_omits_expired_rows() -> None:
    cache = GasPriceDataCache(":memory:", ttl_seconds=1)
    target = datetime.fromtimestamp(1_700_000_000, tz=UTC)
    cache.set(GasPrice(timestamp=target, chain="ethereum", gas_price_gwei=Decimal("9"), source="test"))
    with cache._connection() as conn:
        conn.execute(
            "UPDATE gas_prices SET created_at = ?",
            ((datetime.now(UTC) - timedelta(seconds=5)).isoformat(),),
        )
        conn.commit()

    assert cache.get_range("ethereum", target - timedelta(minutes=1), target + timedelta(minutes=1)) == []
    cache.close()


@pytest.mark.asyncio
async def test_gateway_unavailable_cooldown_skips_repeated_gateway_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = EtherscanGasPriceProvider(
        data_config=SimpleNamespace(gas_fallback_gwei=Decimal("7.5")),  # type: ignore[arg-type]
    )
    calls = 0

    def _raise_unavailable(*, chain: str, timestamp: datetime, is_current: bool) -> GasPrice:
        nonlocal calls
        calls += 1
        raise gas_mod.DataSourceUnavailable(source="gateway", reason="down")

    monkeypatch.setattr(provider, "_get_gateway_gas_price", _raise_unavailable)

    first = await provider.get_gas_price(datetime.fromtimestamp(1_700_000_000, tz=UTC), chain="ethereum")
    second = await provider.get_gas_price(datetime.fromtimestamp(1_700_000_120, tz=UTC), chain="ethereum")

    assert calls == 1
    assert first.source == "config_fallback"
    assert second.source == "config_fallback"
