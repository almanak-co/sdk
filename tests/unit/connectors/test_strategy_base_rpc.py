from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.rpc import eth_call, eth_estimate_gas


def test_eth_call_rejects_non_http_direct_rpc_scheme() -> None:
    with pytest.raises(ValueError, match="Unsupported RPC URL scheme 'file'"):
        eth_call(
            chain="arbitrum",
            to="0x1111111111111111111111111111111111111111",
            data="0x12345678",
            rpc_url="file:///etc/hosts",
        )


def test_eth_call_surfaces_rpc_error_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"error": {"code": -32000, "message": "execution reverted"}}

    def post(*_args: object, **_kwargs: object) -> Response:
        return Response()

    monkeypatch.setattr("requests.post", post)

    with pytest.raises(ValueError, match="execution reverted"):
        eth_call(
            chain="arbitrum",
            to="0x1111111111111111111111111111111111111111",
            data="0x12345678",
            rpc_url="https://arb.example.invalid",
        )


def test_eth_call_uses_direct_rpc_when_gateway_is_disconnected(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = MagicMock()
    gateway.is_connected = False

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"result": "0x" + f"{1:064x}"}

    calls: list[dict[str, object]] = []

    def post(url: str, **kwargs: object) -> Response:
        calls.append({"url": url, **kwargs})
        return Response()

    monkeypatch.setattr("requests.post", post)

    result = eth_call(
        chain="arbitrum",
        to="0x1111111111111111111111111111111111111111",
        data="0x12345678",
        rpc_url="https://arb.example.invalid",
        gateway_client=gateway,
    )

    assert result == (1).to_bytes(32, "big")
    assert calls[0]["url"] == "https://arb.example.invalid"
    gateway.eth_call.assert_not_called()


# ---------------------------------------------------------------------------
# eth_estimate_gas (VIB-5440)
# ---------------------------------------------------------------------------


def test_eth_estimate_gas_uses_connected_gateway() -> None:
    """A connected gateway serves the estimate over the gateway channel (no egress)."""
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.estimate_gas.return_value = 450_000

    result = eth_estimate_gas(
        chain="ethereum",
        to="0x2222222222222222222222222222222222222222",
        data="0xabcdef",
        from_address="0x3333333333333333333333333333333333333333",
        value=123,
        gateway_client=gateway,
    )

    assert result == 450_000
    gateway.estimate_gas.assert_called_once_with(
        "ethereum",
        "0x2222222222222222222222222222222222222222",
        "0xabcdef",
        from_address="0x3333333333333333333333333333333333333333",
        value=123,
    )


def test_eth_estimate_gas_returns_none_when_gateway_raises() -> None:
    """A gateway failure returns None (unmeasured) -- never 0 (Empty≠Zero)."""
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.estimate_gas.side_effect = RuntimeError("boom")

    result = eth_estimate_gas(
        chain="ethereum",
        to="0x2222222222222222222222222222222222222222",
        data="0xabcdef",
        gateway_client=gateway,
    )

    assert result is None


def test_eth_estimate_gas_returns_none_without_transport() -> None:
    """No gateway and no rpc_url -> None (caller falls back to static floor)."""
    assert (
        eth_estimate_gas(
            chain="ethereum",
            to="0x2222222222222222222222222222222222222222",
            data="0xabcdef",
        )
        is None
    )


def test_eth_estimate_gas_disconnected_gateway_returns_none_no_egress(monkeypatch: pytest.MonkeyPatch) -> None:
    """A DISCONNECTED gateway yields None with NO direct-RPC egress.

    ``eth_estimate_gas`` is gateway-only (unlike ``eth_call``, the estimate is
    optional — the caller falls back to a static floor). A disconnected gateway
    must NOT fall through to ``requests.post`` (that would be a new
    ``vib-2986-exempt`` bypass), so no HTTP call may be made.
    """
    gateway = MagicMock()
    gateway.is_connected = False

    def _boom(*_a: object, **_k: object) -> None:
        raise AssertionError("eth_estimate_gas must not make a direct-RPC call")

    monkeypatch.setattr("requests.post", _boom)

    result = eth_estimate_gas(
        chain="arbitrum",
        to="0x1111111111111111111111111111111111111111",
        data="0x12345678",
        from_address="0x4444444444444444444444444444444444444444",
        value=7,
        gateway_client=gateway,
    )

    assert result is None
    gateway.estimate_gas.assert_not_called()
