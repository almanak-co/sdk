"""Unit tests for yield_poke_base._send_tx receipt-status verification.

Anvil mines reverting transactions (receipt status 0x0) instead of rejecting
them, so a returned tx hash alone does not prove execution: the Aave V3
``supply(0)`` poke (reverting with InvalidAmount()) and the old Compound V3
``scale()`` poke both reported ``PokeResult(success=True)`` while doing
nothing (VIB-2630 spike). These tests pin that ``_send_tx``:

  (a) returns the tx hash only when the receipt status is 0x1;
  (b) raises when the receipt reports a revert (status 0x0) or is malformed;
  (c) polls until the receipt appears and raises when it never does;
  (d) propagates JSON-RPC errors from eth_sendTransaction.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from almanak.connectors._strategy_base import yield_poke_base
from almanak.connectors._strategy_base.yield_poke_base import _rpc, _send_tx

RPC_URL = "http://localhost:8545"
TX_HASH = "0x" + "ab" * 32


def _rpc_mock(receipt_responses: list) -> AsyncMock:
    """Mock for yield_poke_base._rpc: send returns TX_HASH, receipts pop in order."""
    responses = list(receipt_responses)

    async def rpc(session, rpc_url, method, params):
        if method == "eth_sendTransaction":
            return TX_HASH
        if method == "eth_getTransactionReceipt":
            assert params == [TX_HASH]
            return responses.pop(0) if responses else None
        raise AssertionError(f"unexpected RPC method {method}")

    return AsyncMock(side_effect=rpc)


class TestSendTxReceiptVerification:
    @pytest.mark.asyncio
    async def test_returns_hash_when_receipt_status_is_success(self) -> None:
        with patch.object(yield_poke_base, "_rpc", _rpc_mock([{"status": "0x1"}])):
            tx_hash = await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

        assert tx_hash == TX_HASH

    @pytest.mark.asyncio
    async def test_raises_when_receipt_status_is_reverted(self) -> None:
        """A mined-but-reverted poke (e.g. Aave InvalidAmount()) must not look successful."""
        with (
            patch.object(yield_poke_base, "_rpc", _rpc_mock([{"status": "0x0"}])),
            pytest.raises(RuntimeError, match="reverted"),
        ):
            await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

    @pytest.mark.asyncio
    async def test_raises_when_receipt_status_is_malformed(self) -> None:
        with (
            patch.object(yield_poke_base, "_rpc", _rpc_mock([{"status": None}])),
            pytest.raises(RuntimeError, match="reverted"),
        ):
            await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

    @pytest.mark.asyncio
    async def test_accepts_integer_receipt_status(self) -> None:
        """Some clients / mocks return status as a bare int rather than hex."""
        with patch.object(yield_poke_base, "_rpc", _rpc_mock([{"status": 1}])):
            tx_hash = await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

        assert tx_hash == TX_HASH

    @pytest.mark.asyncio
    async def test_raises_when_integer_receipt_status_is_zero(self) -> None:
        with (
            patch.object(yield_poke_base, "_rpc", _rpc_mock([{"status": 0}])),
            pytest.raises(RuntimeError, match="reverted"),
        ):
            await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

    @pytest.mark.asyncio
    async def test_polls_until_receipt_appears(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Under interval mining the receipt lands after the send returns."""
        monkeypatch.setattr(yield_poke_base, "_RECEIPT_POLL_INTERVAL_SECONDS", 0)
        with patch.object(yield_poke_base, "_rpc", _rpc_mock([None, None, {"status": "0x1"}])):
            tx_hash = await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

        assert tx_hash == TX_HASH

    @pytest.mark.asyncio
    async def test_raises_when_receipt_never_appears(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(yield_poke_base, "_RECEIPT_POLL_ATTEMPTS", 3)
        monkeypatch.setattr(yield_poke_base, "_RECEIPT_POLL_INTERVAL_SECONDS", 0)
        with (
            patch.object(yield_poke_base, "_rpc", _rpc_mock([])),
            pytest.raises(RuntimeError, match="no receipt"),
        ):
            await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

    @pytest.mark.asyncio
    async def test_propagates_send_error(self) -> None:
        with (
            patch.object(
                yield_poke_base,
                "_rpc",
                AsyncMock(side_effect=RuntimeError("execution reverted")),
            ),
            pytest.raises(RuntimeError, match="execution reverted"),
        ):
            await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

    @pytest.mark.asyncio
    async def test_returns_none_when_send_yields_no_result(self) -> None:
        """A null send result (no hash) is returned as-is without receipt polling."""
        rpc = AsyncMock(return_value=None)
        with patch.object(yield_poke_base, "_rpc", rpc):
            tx_hash = await _send_tx(RPC_URL, "0xwallet", "0xpool", "0xdata")

        assert tx_hash is None
        assert rpc.await_count == 1


class _FakeResponse:
    """Stand-in for an aiohttp response context manager."""

    def __init__(self, json_data, http_error: Exception | None = None) -> None:
        self._json_data = json_data
        self._http_error = http_error

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return False

    def raise_for_status(self) -> None:
        if self._http_error is not None:
            raise self._http_error

    async def json(self):
        return self._json_data


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def post(self, url, json):
        return self._response


class TestRpcResponseValidation:
    @pytest.mark.asyncio
    async def test_raises_on_http_error_status(self) -> None:
        """An HTTP-level error surfaces before any JSON parsing."""
        session = _FakeSession(_FakeResponse(None, http_error=ConnectionError("502 Bad Gateway")))
        with pytest.raises(ConnectionError, match="502"):
            await _rpc(session, RPC_URL, "eth_sendTransaction", [])

    @pytest.mark.asyncio
    async def test_raises_on_non_dict_response(self) -> None:
        session = _FakeSession(_FakeResponse(["not", "a", "dict"]))
        with pytest.raises(RuntimeError, match="invalid JSON-RPC response"):
            await _rpc(session, RPC_URL, "eth_sendTransaction", [])

    @pytest.mark.asyncio
    async def test_raises_with_rpc_error_message(self) -> None:
        session = _FakeSession(_FakeResponse({"error": {"message": "execution reverted"}}))
        with pytest.raises(RuntimeError, match="execution reverted"):
            await _rpc(session, RPC_URL, "eth_sendTransaction", [])

    @pytest.mark.asyncio
    async def test_returns_result_field(self) -> None:
        session = _FakeSession(_FakeResponse({"result": TX_HASH}))
        assert await _rpc(session, RPC_URL, "eth_sendTransaction", []) == TX_HASH
