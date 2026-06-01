"""Framework-side INVALID_ARGUMENT mapping for PoolHistoryReader (VIB-4755).

Covers UAT card ``docs/internal/uat-cards/VIB-4755.md`` row D3.F8.
The validator-level rejections live on the gateway (POOL-3 / VIB-4751);
this file asserts the framework boundary maps them faithfully —
``DataSourceUnavailable`` with ``__cause__`` reaching
``grpc.RpcError(StatusCode.INVALID_ARGUMENT)``, classify_failure walks
to DATA_UNAVAILABLE.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.pools.history import PoolHistoryReader
from almanak.framework.runner.failure_kind import FailureKind, classify_failure


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details
        super().__init__(details)

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def _reader_with_invalid_argument_stub() -> PoolHistoryReader:
    stub = MagicMock()
    stub.GetPoolHistory.side_effect = _FakeRpcError(
        grpc.StatusCode.INVALID_ARGUMENT,
        "validator rejected request",
    )
    gateway = MagicMock()
    gateway.pool_history = stub
    return PoolHistoryReader(gateway_client=gateway)


# Parametrised over inputs the gateway validator rejects with INVALID_ARGUMENT.
# Each row exercises the framework -> grpc.RpcError -> DataSourceUnavailable
# mapping. The framework reader does NOT pre-validate (the gateway is the
# canonical validator — pre-validating would diverge from gateway normalisation).
_INVALID_INPUTS = [
    pytest.param(
        {"pool_address": "", "chain": "base", "protocol": "uniswap_v3"},
        id="empty_pool_address",
    ),
    pytest.param(
        {"pool_address": "0xdead", "chain": "base", "protocol": "uniswap_v999"},
        id="unknown_protocol_slug",
    ),
    pytest.param(
        {"pool_address": "0xdead", "chain": "solana", "protocol": "uniswap_v3"},
        id="solana_out_of_scope",
    ),
    pytest.param(
        {"pool_address": "0xdead", "chain": "ethereum", "protocol": "aerodrome"},
        id="aerodrome_not_supported_on_ethereum",
    ),
    pytest.param(
        {"pool_address": "../etc/passwd", "chain": "base", "protocol": "uniswap_v3"},
        id="malformed_pool_address",
    ),
]


@pytest.mark.parametrize("inputs", _INVALID_INPUTS)
def test_invalid_argument_maps_to_datasource_unavailable(inputs: dict[str, str]) -> None:
    """D3.F8: gateway-side INVALID_ARGUMENT -> DataSourceUnavailable
    with __cause__ reaching grpc.RpcError(INVALID_ARGUMENT)."""
    reader = _reader_with_invalid_argument_stub()

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_history(
            pool_address=inputs["pool_address"],
            chain=inputs["chain"],
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
            resolution="1h",
            protocol=inputs["protocol"],
        )
    assert isinstance(excinfo.value.__cause__, grpc.RpcError)
    assert excinfo.value.__cause__.code() == grpc.StatusCode.INVALID_ARGUMENT
    # classify_failure walks __cause__ to DATA_UNAVAILABLE.
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


def test_start_after_end_maps_to_datasource_unavailable() -> None:
    """D3.F8: start_date > end_date triggers INVALID_ARGUMENT
    (gateway rejects start_ts > end_ts)."""
    reader = _reader_with_invalid_argument_stub()

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_history(
            pool_address="0xdead",
            chain="base",
            start_date=datetime(2024, 3, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )
    assert isinstance(excinfo.value.__cause__, grpc.RpcError)
    assert excinfo.value.__cause__.code() == grpc.StatusCode.INVALID_ARGUMENT
