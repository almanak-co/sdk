"""Tests for gateway-owned gas egress helpers."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.core.chains import ChainRegistry
from almanak.gateway.data.gas import etherscan
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable


@pytest.fixture(autouse=True)
def _clear_gas_cache() -> None:
    etherscan._GAS_CACHE.clear()
    etherscan._GAS_IN_FLIGHT.clear()
    yield
    etherscan._GAS_CACHE.clear()
    etherscan._GAS_IN_FLIGHT.clear()


class _HexLike:
    def hex(self) -> str:
        return "0x0100"


def test_int_from_rpc_quantity_accepts_bytes_and_hex_like_values() -> None:
    assert etherscan._int_from_rpc_quantity(b"\x01\x00") == 256
    assert etherscan._int_from_rpc_quantity(bytearray(b"\x01\x00")) == 256
    assert etherscan._int_from_rpc_quantity(_HexLike()) == 256


class _GasOracleResponse:
    status = 200

    def __init__(self, *, status: int = 200, payload: dict[str, Any] | None = None) -> None:
        self.status = status
        self.payload = payload or {
            "status": "1",
            "result": {
                "suggestBaseFee": "10",
                "ProposeGasPrice": "12",
            },
        }

    async def __aenter__(self) -> _GasOracleResponse:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    async def json(self) -> dict[str, Any]:
        return self.payload

    async def text(self) -> str:
        return "upstream body with token=secret"


class _GasOracleSession:
    def __init__(self, response: _GasOracleResponse) -> None:
        self.response = response
        self.calls = 0

    def get(self, *_args: Any, **_kwargs: Any) -> _GasOracleResponse:
        self.calls += 1
        return self.response


class _HttpServicer:
    def __init__(self, session: _GasOracleSession) -> None:
        self.session = session

    async def _get_http_session(self) -> _GasOracleSession:
        return self.session


def test_fetch_current_gas_oracle_sanitizes_http_body(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = ChainRegistry.resolve("ethereum")
    session = _GasOracleSession(_GasOracleResponse(status=500))
    monkeypatch.setattr(etherscan, "_wait_for_egress_slot", _noop)

    with pytest.raises(RateHistoryUnavailable) as exc_info:
        asyncio.run(
            etherscan._fetch_current_gas_oracle(
                _HttpServicer(session),  # type: ignore[arg-type]
                chain="ethereum",
                descriptor=descriptor,
            )
        )

    assert "HTTP 500" in str(exc_info.value)
    assert "secret" not in str(exc_info.value)


def test_fetch_gas_price_at_uses_gateway_side_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = ChainRegistry.resolve("ethereum")
    session = _GasOracleSession(_GasOracleResponse())
    etherscan._GAS_CACHE.clear()
    monkeypatch.setattr(etherscan, "_wait_for_egress_slot", _noop)

    first = asyncio.run(
        etherscan.fetch_gas_price_at(
            _HttpServicer(session),  # type: ignore[arg-type]
            chain="ethereum",
            timestamp=0,
            descriptor=descriptor,
        )
    )
    second = asyncio.run(
        etherscan.fetch_gas_price_at(
            _HttpServicer(session),  # type: ignore[arg-type]
            chain="ethereum",
            timestamp=0,
            descriptor=descriptor,
        )
    )

    assert session.calls == 1
    assert first[0].gas_price_gwei == Decimal("12")
    assert second[0].gas_price_gwei == Decimal("12")


@pytest.mark.asyncio
async def test_fetch_gas_price_at_deduplicates_concurrent_cache_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = ChainRegistry.resolve("ethereum")
    calls = 0

    async def _fetch_once(
        _servicer: object,
        *,
        chain: str,
        descriptor: object,
    ) -> tuple[SimpleNamespace, str]:
        nonlocal calls
        assert chain == "ethereum"
        calls += 1
        await asyncio.sleep(0)
        return SimpleNamespace(timestamp=0, gas_price_gwei=Decimal("12")), "etherscan"

    monkeypatch.setattr(etherscan, "_fetch_current_gas_oracle", _fetch_once)

    results = await asyncio.gather(
        *(
            etherscan.fetch_gas_price_at(
                SimpleNamespace(),  # type: ignore[arg-type]
                chain="ethereum",
                timestamp=0,
                descriptor=descriptor,
            )
            for _ in range(5)
        )
    )

    assert calls == 1
    assert [point.gas_price_gwei for point, _source in results] == [Decimal("12")] * 5
    assert etherscan._GAS_IN_FLIGHT == {}


def test_gas_cache_evicts_oldest_entries_when_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(etherscan, "_MAX_GAS_CACHE_ENTRIES", 2)

    asyncio.run(etherscan._gas_cache_set(("ethereum", 1), (SimpleNamespace(timestamp=1), "archive_rpc"), current=False))
    asyncio.run(etherscan._gas_cache_set(("ethereum", 2), (SimpleNamespace(timestamp=2), "archive_rpc"), current=False))
    asyncio.run(etherscan._gas_cache_set(("ethereum", 3), (SimpleNamespace(timestamp=3), "archive_rpc"), current=False))

    assert list(etherscan._GAS_CACHE) == [("ethereum", 2), ("ethereum", 3)]


class _ArchiveEth:
    async def get_block(self, block: str | int) -> dict[str, Any]:
        if block == "latest":
            return {"number": 100, "timestamp": 1_700_000_120}
        return {
            "number": block,
            "timestamp": 1_699_999_997,
            "baseFeePerGas": "0x77359400",
        }


class _ArchiveServicer:
    async def _get_web3(self, chain: str) -> SimpleNamespace:
        assert chain == "ethereum"
        return SimpleNamespace(eth=_ArchiveEth())


def test_historical_archive_gas_uses_actual_block_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = ChainRegistry.resolve("ethereum")
    monkeypatch.setattr(etherscan, "_wait_for_egress_slot", _noop)

    point, source = asyncio.run(
        etherscan._fetch_historical_archive_gas(
            _ArchiveServicer(),  # type: ignore[arg-type]
            chain="ethereum",
            timestamp=1_700_000_000,
            descriptor=descriptor,
        )
    )

    assert source == "archive_rpc"
    assert point.timestamp == 1_699_999_997
    assert point.base_fee_gwei == Decimal("2")


async def _noop() -> None:
    return None
