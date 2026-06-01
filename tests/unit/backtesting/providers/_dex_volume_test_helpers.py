"""Shared mock plumbing for the gateway-backed per-DEX volume provider tests.

**VIB-4870 / W7**: every per-DEX volume provider is now a thin gRPC
client of ``RateHistoryService.GetDexVolumeHistory``. The per-DEX test
files share the same gateway-client mock shape; this module centralises
it so each test file only declares its DEX-specific
``(provider_class, supported_chain, data_source, dex_routing_key)``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

_GW_MODULE = "almanak.framework.backtesting.pnl.providers.dex._gateway_volume"


def make_point(timestamp: int, volume_usd: str) -> MagicMock:
    """Build a mock ``DexVolumePoint`` (timestamp unix-sec, volume_usd str)."""
    point = MagicMock()
    point.timestamp = timestamp
    point.volume_usd = volume_usd
    return point


def make_response(
    points: list[MagicMock],
    *,
    success: bool = True,
    source: str = "the_graph",
    error: str = "",
) -> MagicMock:
    """Build a mock ``DexVolumeHistoryResponse``."""
    resp = MagicMock()
    resp.success = success
    resp.source = source
    resp.error = error
    resp.points = points
    return resp


def patch_gateway(response: MagicMock):
    """Patch the shared gateway-client helper to return ``response``.

    Returns ``(patcher, captured)`` — ``captured["request"]`` holds the
    ``GetDexVolumeHistory`` request the provider built so tests can assert
    dex / chain / pool / window.
    """
    captured: dict[str, object] = {}

    client = MagicMock()
    client.is_connected = True

    def _get_volume_history(request):
        captured["request"] = request
        return response

    client.rate_history.GetDexVolumeHistory = _get_volume_history

    import almanak.gateway.proto.gateway_pb2 as gateway_pb2

    patcher = patch(
        f"{_GW_MODULE}._get_connected_gateway_client",
        return_value=(client, gateway_pb2),
    )
    return patcher, captured


def patch_gateway_rpc_error(exc: Exception):
    """Patch the gateway client so ``GetDexVolumeHistory`` raises ``exc``."""
    client = MagicMock()
    client.is_connected = True
    client.rate_history.GetDexVolumeHistory = MagicMock(side_effect=exc)

    import almanak.gateway.proto.gateway_pb2 as gateway_pb2

    return patch(
        f"{_GW_MODULE}._get_connected_gateway_client",
        return_value=(client, gateway_pb2),
    )
