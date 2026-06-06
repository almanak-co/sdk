from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.rpc import eth_call


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
