"""Caller-aware static-call probe seam (VIB-5716).

``eth_call_static_probe`` simulates a state-changing call from a wallet and
classifies the outcome three ways — success / revert(+reason) / transport —
so probe consumers (the Curve LP deployability probe) can tell "the pool
rejected this caller" apart from "the network dropped the question". These
tests drive both transports with fakes and pin the revert-reason extraction
(Error(string) data decode, panic, custom-error selector, inline message).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.rpc import (
    _decode_revert_data,
    _extract_revert_reason,
    _looks_like_revert,
    eth_call_static_probe,
)

TO = "0x313698667d7FDD6789a9BC70821309ff891E729A"
WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _error_string_blob(reason: str) -> str:
    """ABI-encode ``Error(string)`` revert data for ``reason``."""
    payload = reason.encode()
    head = format(32, "064x") + format(len(payload), "064x")
    body = payload.hex().ljust(((len(payload) + 31) // 32) * 64, "0")
    return "0x08c379a0" + head + body


# =============================================================================
# Reason extraction / decode
# =============================================================================


class TestRevertDecoding:
    def test_error_string_blob_decodes(self) -> None:
        assert _decode_revert_data(_error_string_blob("!wl")) == "!wl"

    def test_panic_blob_decodes(self) -> None:
        blob = "0x4e487b71" + format(0x11, "064x")
        assert _decode_revert_data(blob) == "panic 0x11"

    def test_custom_error_selector_surfaces(self) -> None:
        blob = "0xfb8f41b2" + format(0, "064x")
        assert _decode_revert_data(blob) == "custom error 0xfb8f41b2"

    def test_extract_prefers_data_field_over_message(self) -> None:
        text = "{'code': 3, 'message': 'execution reverted', 'data': '" + _error_string_blob("!wl") + "'}"
        assert _extract_revert_reason(text) == "!wl"

    def test_extract_falls_back_to_inline_message(self) -> None:
        text = "{'code': 3, 'message': 'execution reverted: Slippage'}"
        assert _extract_revert_reason(text) == "Slippage"

    def test_reasonless_revert_extracts_none(self) -> None:
        text = "{'code': 3, 'message': 'execution reverted'}"
        assert _extract_revert_reason(text) is None

    def test_address_hex_in_message_is_not_misread_as_revert_data(self) -> None:
        # A 40-hex address elsewhere in the error text must not be decoded as
        # revert data (only the error object's ``data`` field is).
        text = f"RPC eth_call failed for {TO}: connection reset"
        assert _extract_revert_reason(text) is None
        assert _looks_like_revert(text) is False

    def test_json_rpc_code_3_is_a_revert_even_without_the_word(self) -> None:
        assert _looks_like_revert('{"code": 3, "message": "out of gas?"}') is True

    def test_transport_error_is_not_a_revert(self) -> None:
        assert _looks_like_revert("Gateway eth_call transport error: UNAVAILABLE") is False


# =============================================================================
# Gateway transport
# =============================================================================


class _GatewayRaises:
    is_connected = True

    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.kwargs: dict[str, object] = {}

    def eth_call(self, **kwargs: object) -> str:
        self.kwargs = kwargs
        raise self._exc


class TestGatewayPath:
    def test_success_passes_probe_kwargs(self) -> None:
        gateway = MagicMock()
        gateway.is_connected = True
        gateway.eth_call.return_value = "0x" + format(1, "064x")
        probe = eth_call_static_probe(
            chain="ethereum", to=TO, data="0x12345678", from_address=WALLET, value=7, gateway_client=gateway
        )
        assert probe.outcome == "success"
        assert probe.data == bytes.fromhex(format(1, "064x"))
        kwargs = gateway.eth_call.call_args.kwargs
        assert kwargs["from_address"] == WALLET
        assert kwargs["value"] == 7
        assert kwargs["raise_on_error"] is True

    def test_revert_error_classifies_with_reason(self) -> None:
        payload = json.dumps({"code": 3, "message": "execution reverted: !wl"})
        gateway = _GatewayRaises(ValueError(f"Gateway eth_call error for {TO} on ethereum: {payload}"))
        probe = eth_call_static_probe(
            chain="ethereum", to=TO, data="0x12345678", from_address=WALLET, gateway_client=gateway
        )
        assert probe.outcome == "revert"
        assert probe.revert_reason == "!wl"

    def test_grpc_failure_classifies_transport(self) -> None:
        gateway = _GatewayRaises(ValueError("Gateway eth_call transport error for ... : UNAVAILABLE"))
        probe = eth_call_static_probe(
            chain="ethereum", to=TO, data="0x12345678", from_address=WALLET, gateway_client=gateway
        )
        assert probe.outcome == "transport"


# =============================================================================
# Direct-RPC transport
# =============================================================================


class TestDirectPath:
    def test_revert_with_data_blob_decodes_reason(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {
                    "error": {
                        "code": 3,
                        "message": "execution reverted",
                        "data": _error_string_blob("!wl"),
                    }
                }

        monkeypatch.setattr("requests.post", lambda *a, **k: Response())
        probe = eth_call_static_probe(
            chain="ethereum", to=TO, data="0x12345678", from_address=WALLET, rpc_url="https://eth.example.invalid"
        )
        assert probe.outcome == "revert"
        assert probe.revert_reason == "!wl"

    def test_direct_call_includes_from_and_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, object] = {}

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {"result": "0x"}

        def post(url: str, **kwargs: object) -> Response:
            seen.update(kwargs)
            return Response()

        monkeypatch.setattr("requests.post", post)
        probe = eth_call_static_probe(
            chain="ethereum",
            to=TO,
            data="0x12345678",
            from_address=WALLET,
            value=5,
            rpc_url="https://eth.example.invalid",
        )
        assert probe.outcome == "success"
        call_obj = seen["json"]["params"][0]  # type: ignore[index]
        assert call_obj["from"] == WALLET
        assert call_obj["value"] == hex(5)

    def test_network_error_classifies_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def post(*_a: object, **_k: object) -> object:
            raise ConnectionError("connection reset by peer")

        monkeypatch.setattr("requests.post", post)
        probe = eth_call_static_probe(
            chain="ethereum", to=TO, data="0x12345678", from_address=WALLET, rpc_url="https://eth.example.invalid"
        )
        assert probe.outcome == "transport"

    def test_no_transport_at_all_is_transport(self) -> None:
        probe = eth_call_static_probe(chain="ethereum", to=TO, data="0x12345678", from_address=WALLET)
        assert probe.outcome == "transport"
        assert probe.error == "no read transport"
