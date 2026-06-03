"""Tests for RpcService upstream retry/backoff (Phase 6 of VIB-2986).

These prove that transient upstream RPC failures get retried at the
gateway layer — the architectural correction from the original VIB-2984
branch (which retried inside every connector's Web3 provider).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.rpc_service import RpcServiceServicer


def _make_service() -> RpcServiceServicer:
    settings = MagicMock(spec=GatewaySettings)
    settings.network = "mainnet"
    return RpcServiceServicer(settings)


class _FakeResponse:
    """Minimal async context-manager mock for aiohttp session.post."""

    headers: dict[str, str] = {}

    def __init__(self, status: int = 200, text_body: str = "", json_body=None):
        self.status = status
        self._text_body = text_body
        self._json_body = json_body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text_body

    async def json(self):
        if isinstance(self._json_body, Exception):
            raise self._json_body
        return self._json_body


class TestRpcServiceRetry:
    """Retry behavior on transient errors (VIB-2984 fix at gateway layer)."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(return_value=_FakeResponse(200, json_body={"result": "0x42", "id": "1"}))

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert error is None
        assert result == "0x42"
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_on_500_then_success(self):
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            side_effect=[
                _FakeResponse(500, text_body="upstream hiccup"),
                _FakeResponse(200, json_body={"result": "0x1", "id": "1"}),
            ]
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert error is None
        assert result == "0x1"
        assert session.post.call_count == 2
        # Exactly one backoff between attempt 1 (fail) and attempt 2 (success)
        assert mock_sleep.await_count == 1

    @pytest.mark.asyncio
    async def test_retry_on_429_then_success(self):
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            side_effect=[
                _FakeResponse(429, text_body="rate limit"),
                _FakeResponse(200, json_body={"result": "0x2", "id": "1"}),
            ]
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()),
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert error is None
        assert result == "0x2"
        assert session.post.call_count == 2

    @pytest.mark.asyncio
    async def test_retry_exhausted_surfaces_last_error(self):
        svc = _make_service()
        session = MagicMock()
        # All 3 attempts get 503s
        session.post = MagicMock(return_value=_FakeResponse(503, text_body="unavailable"))

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()),
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert result is None
        assert error is not None
        assert error["code"] == -32603
        assert "503" in error["message"]
        # Max 3 attempts
        assert session.post.call_count == svc._RETRY_MAX_ATTEMPTS

    @pytest.mark.asyncio
    async def test_retry_on_network_error(self):
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            side_effect=[
                aiohttp.ClientError("connection reset"),
                _FakeResponse(200, json_body={"result": "0x3", "id": "1"}),
            ]
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()),
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert error is None
        assert result == "0x3"
        assert session.post.call_count == 2

    @pytest.mark.asyncio
    async def test_no_retry_on_localhost_connection_error(self):
        """Local Anvil down is a user-actionable error — don't waste time retrying."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(side_effect=aiohttp.ClientError("Cannot connect"))

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("http://127.0.0.1:8545", "eth_blockNumber", [], "1")

        assert result is None
        assert error is not None
        assert "Cannot connect to local RPC" in error["message"]
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_retry_on_jsonrpc_error_response(self):
        """JSON-RPC-level errors (e.g. execution reverted) reached upstream — don't retry."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            return_value=_FakeResponse(
                200,
                json_body={"error": {"code": -32000, "message": "execution reverted"}, "id": "1"},
            )
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_call", [], "1")

        assert result is None
        assert error == {"code": -32000, "message": "execution reverted"}
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_exponential_backoff_delays(self):
        """Backoff delays grow exponentially, bounded by 50%-150% jitter bands."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(return_value=_FakeResponse(503, text_body="unavailable"))

        sleep_mock = AsyncMock()
        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", sleep_mock),
        ):
            await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        # 2 backoffs between 3 attempts
        assert sleep_mock.await_count == svc._RETRY_MAX_ATTEMPTS - 1
        delays = [c.args[0] for c in sleep_mock.await_args_list]
        # Jitter range: base * 2^(attempt-1) * [0.5, 1.5]
        base = svc._RETRY_BASE_DELAY
        assert base * 0.5 <= delays[0] <= base * 1.5
        assert base * 2 * 0.5 <= delays[1] <= base * 2 * 1.5
        # And the second delay must be strictly greater than the first on average —
        # the exponential step outweighs jitter (lower bound of step 2 >= upper bound of step 1 / 1.5 / 2)
        assert delays[1] >= base  # second delay is at least as large as unjittered first step

    @pytest.mark.asyncio
    async def test_no_retry_on_eth_send_raw_transaction(self):
        """eth_sendRawTransaction is not idempotent — one 5xx must NOT retry."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(return_value=_FakeResponse(503, text_body="unavailable"))

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call(
                "https://rpc.example",
                "eth_sendRawTransaction",
                ["0xdeadbeef"],
                "1",
            )

        assert result is None
        assert error is not None
        assert "503" in error["message"]
        # CRITICAL: exactly one attempt — retry could double-broadcast a signed tx.
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_retry_on_solana_send_transaction(self):
        """Solana sendTransaction — same signed blob is valid in the blockhash window; never retry."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(return_value=_FakeResponse(502, text_body="bad gateway"))

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call(
                "https://solana-rpc.example",
                "sendTransaction",
                ["<base58-signed-tx>"],
                "1",
            )

        assert result is None
        assert error is not None
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_after_header_is_honored(self):
        """Upstream Retry-After header takes precedence over exponential backoff."""
        svc = _make_service()
        session = MagicMock()

        class _RetryAfterResponse(_FakeResponse):
            headers = {"Retry-After": "2"}

        session.post = MagicMock(
            side_effect=[
                _RetryAfterResponse(429, text_body="throttled"),
                _FakeResponse(200, json_body={"result": "0x5", "id": "1"}),
            ]
        )

        sleep_mock = AsyncMock()
        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", sleep_mock),
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert error is None
        assert result == "0x5"
        # Honored Retry-After: clamped to _RETRY_MAX_AFTER but 2.0 is under the cap.
        assert sleep_mock.await_args_list[0].args[0] == 2.0

    @pytest.mark.asyncio
    async def test_retry_after_header_clamped(self):
        """Retry-After over _RETRY_MAX_AFTER is clamped to avoid stalling the decide loop."""
        svc = _make_service()
        session = MagicMock()

        class _LargeRetryAfter(_FakeResponse):
            headers = {"Retry-After": "999"}

        session.post = MagicMock(
            side_effect=[
                _LargeRetryAfter(429, text_body="throttled"),
                _FakeResponse(200, json_body={"result": "0x6", "id": "1"}),
            ]
        )

        sleep_mock = AsyncMock()
        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", sleep_mock),
        ):
            await svc._make_rpc_call("https://rpc.example", "eth_blockNumber", [], "1")

        assert sleep_mock.await_args_list[0].args[0] == svc._RETRY_MAX_AFTER


class TestRpcServiceIndexerLagRetry:
    """Receipt-indexer-lag retry (VIB-4985 / ALM-2777).

    A pinned post-execution read (block=receipt.block_number) can race the
    upstream RPC's receipt indexer: the block is confirmed but the node serving
    the eth_call has not ingested it yet → "Unknown block". Without a retry the
    lending row drops to confidence=ESTIMATED. These prove the narrow lag-marker
    set is retried while every other error still fails fast.
    """

    @pytest.mark.parametrize(
        "marker",
        [
            "Unknown block",
            "header not found",
            "missing trie node",
            "block not found",
            "no state available for block 0x1234",
            "UNKNOWN BLOCK",  # case-insensitive
        ],
    )
    @pytest.mark.asyncio
    async def test_retry_on_jsonrpc_lag_then_success(self, marker):
        """JSON-RPC-level lag error is retried and recovers to a real result."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            side_effect=[
                _FakeResponse(200, json_body={"error": {"code": -32000, "message": marker}, "id": "1"}),
                _FakeResponse(200, json_body={"result": "0xcafe", "id": "1"}),
            ]
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_call", [], "1", chain="base")

        assert error is None
        assert result == "0xcafe"
        assert session.post.call_count == 2
        assert mock_sleep.await_count == 1
        assert svc._metrics.indexer_lag_retries == 1

    @pytest.mark.asyncio
    async def test_retry_on_http400_unknown_block_then_success(self):
        """Some providers wrap lag in a non-2xx (HTTP 400) — retry that too."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            side_effect=[
                _FakeResponse(400, text_body='{"error":"Unknown block"}'),
                _FakeResponse(200, json_body={"result": "0xbeef", "id": "1"}),
            ]
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_call", [], "1", chain="base")

        assert error is None
        assert result == "0xbeef"
        assert session.post.call_count == 2
        assert mock_sleep.await_count == 1
        assert svc._metrics.indexer_lag_retries == 1

    @pytest.mark.asyncio
    async def test_lag_retry_exhausted_fails_closed(self):
        """Persistent lag exhausts attempts and surfaces the error (Empty ≠ Zero).

        The read returns None → the lending handler keeps confidence=ESTIMATED
        rather than fabricating after-state.
        """
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            return_value=_FakeResponse(
                200, json_body={"error": {"code": -32000, "message": "Unknown block"}, "id": "1"}
            )
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_call", [], "1", chain="base")

        assert result is None
        assert error == {"code": -32000, "message": "Unknown block"}
        assert session.post.call_count == svc._RETRY_MAX_ATTEMPTS
        # 2 backoffs between 3 attempts.
        assert mock_sleep.await_count == svc._RETRY_MAX_ATTEMPTS - 1
        assert svc._metrics.indexer_lag_retries == svc._RETRY_MAX_ATTEMPTS - 1

    @pytest.mark.asyncio
    async def test_no_lag_retry_on_revert(self):
        """A revert is not lag — must fail fast even though it shares code -32000."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            return_value=_FakeResponse(
                200, json_body={"error": {"code": -32000, "message": "execution reverted"}, "id": "1"}
            )
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call("https://rpc.example", "eth_call", [], "1", chain="base")

        assert result is None
        assert error == {"code": -32000, "message": "execution reverted"}
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert svc._metrics.indexer_lag_retries == 0

    @pytest.mark.asyncio
    async def test_no_lag_retry_on_write_method(self):
        """A write method capped at 1 attempt never lag-retries (no double-broadcast)."""
        svc = _make_service()
        session = MagicMock()
        session.post = MagicMock(
            return_value=_FakeResponse(400, text_body='{"error":"Unknown block"}'),
        )

        with (
            patch.object(svc, "_get_session", AsyncMock(return_value=session)),
            patch("asyncio.sleep", AsyncMock()) as mock_sleep,
        ):
            result, error = await svc._make_rpc_call(
                "https://rpc.example", "eth_sendRawTransaction", ["0xdeadbeef"], "1", chain="base"
            )

        assert result is None
        assert error is not None
        assert session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert svc._metrics.indexer_lag_retries == 0

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("Unknown block", True),
            ("missing trie node 0xabc", True),
            ("no state available for block", True),
            ("execution reverted", False),
            ("invalid api key", False),
            ("invalid argument 0: hex string too short", False),
            ("", False),
            (None, False),
            # Non-compliant providers may return a non-string message field —
            # the classifier must not crash, it returns False (fail fast).
            (123, False),
            ({"error": "Unknown block"}, False),
            (["Unknown block"], False),
        ],
    )
    def test_is_indexer_lag_error_classifier(self, message, expected):
        assert RpcServiceServicer._is_indexer_lag_error(message) is expected
