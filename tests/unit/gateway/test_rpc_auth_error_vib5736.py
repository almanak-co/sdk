"""VIB-5736: RPC auth/entitlement error classifier.

The regression gate for the Robinhood (4663) crash-loop: a keyed provider that
returns HTTP 403 ("chain not enabled on the Alchemy app") must be recognised as
a deterministic auth/entitlement failure so the balance-read path fails FAST with
an actionable message, instead of retrying the same 403 every iteration.

The classifier must be narrow: 401/403 are entitlement (fail fast); 429/5xx are
transient (keep retrying); everything else (timeouts, reverts, indexer lag) is
not entitlement.
"""

from __future__ import annotations

import pytest
from aiohttp import ClientResponseError
from aiohttp.client_reqrep import RequestInfo
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from almanak.gateway.data.balance.web3_provider import RPCError
from almanak.gateway.utils.rpc_auth_error import (
    entitlement_error_message,
    is_rpc_entitlement_error,
    rpc_http_status,
)


class _AiohttpLikeError(Exception):
    """Mimics aiohttp.ClientResponseError (web3 async HTTPProvider): carries .status."""

    def __init__(self, status: int, message: str = ""):
        self.status = status
        super().__init__(message or f"{status}, message='{message or 'error'}'")


class _Response:
    def __init__(self, status_code: int):
        self.status_code = status_code


class _RequestsLikeError(Exception):
    """Mimics requests.HTTPError (sync HTTPProvider): carries .response.status_code."""

    def __init__(self, status_code: int, message: str = ""):
        self.response = _Response(status_code)
        super().__init__(message or f"{status_code} Client Error")


# --------------------------------------------------------------------------- #
# rpc_http_status
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "exc,expected",
    [
        (_AiohttpLikeError(403), 403),
        (_AiohttpLikeError(401), 401),
        (_RequestsLikeError(404), 404),
        (_AiohttpLikeError(429), 429),
        (TimeoutError("timed out"), None),
        (Exception("no status here"), None),
        (None, None),
    ],
)
def test_rpc_http_status(exc, expected):
    assert rpc_http_status(exc) == expected


def test_rpc_http_status_ignores_jsonrpc_code():
    """A web3 JSON-RPC error code (negative, on .code) is NOT an HTTP status."""

    class _JsonRpc(Exception):
        code = -32000  # revert code — must be ignored

    assert rpc_http_status(_JsonRpc()) is None


def test_real_aiohttp_client_response_error_shape_is_classified():
    """Exercise the concrete exception type used by web3's async transport."""
    url = URL("https://rpc.example.invalid")
    request_info = RequestInfo(
        url=url,
        method="POST",
        headers=CIMultiDictProxy(CIMultiDict()),
        real_url=url,
    )
    exc = ClientResponseError(request_info, (), status=403, message="Forbidden")

    assert rpc_http_status(exc) == 403
    assert is_rpc_entitlement_error(exc) is True


# --------------------------------------------------------------------------- #
# is_rpc_entitlement_error — status-driven
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("status", [401, 403])
def test_entitlement_status_true(status):
    assert is_rpc_entitlement_error(_AiohttpLikeError(status)) is True
    assert is_rpc_entitlement_error(_RequestsLikeError(status)) is True


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_transient_status_false(status):
    """429 rate-limit and 5xx outages are transient — NOT entitlement."""
    assert is_rpc_entitlement_error(_AiohttpLikeError(status)) is False


# --------------------------------------------------------------------------- #
# is_rpc_entitlement_error — message-driven fallback
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "message",
    [
        "ROBINHOOD_MAINNET is not enabled for this app",  # the real Alchemy 403 body
        "403 Client Error: Forbidden for url",
        "401 Unauthorized",
        "invalid api key",
    ],
)
def test_entitlement_message_true(message):
    assert is_rpc_entitlement_error(Exception(message)) is True


@pytest.mark.parametrize(
    "message",
    [
        "Unknown block",  # indexer lag — must stay retryable
        "execution reverted",
        "429 Too Many Requests",
        "connection reset by peer",
        "",
    ],
)
def test_non_entitlement_message_false(message):
    assert is_rpc_entitlement_error(Exception(message)) is False


@pytest.mark.parametrize(
    "message",
    [
        "execution reverted: Forbidden",
        "execution reverted: Unauthorized",
        "eth_call failed: method is not enabled",
    ],
)
def test_application_level_auth_words_are_not_transport_entitlement(message):
    assert is_rpc_entitlement_error(Exception(message)) is False


def test_structured_non_auth_status_wins_over_auth_like_body():
    assert is_rpc_entitlement_error(_AiohttpLikeError(429, "403 Forbidden")) is False
    assert is_rpc_entitlement_error(_AiohttpLikeError(500, "invalid api key")) is False


def test_rate_limit_marker_vetoes_entitlement_marker():
    """A 429 body that also contains 'forbidden' is transient, not entitlement."""
    assert is_rpc_entitlement_error(Exception("429 forbidden: rate limit")) is False


def test_none_is_not_entitlement():
    assert is_rpc_entitlement_error(None) is False


# --------------------------------------------------------------------------- #
# is_rpc_entitlement_error — walks the exception chain
# --------------------------------------------------------------------------- #


def test_walks_original_error_wrapper():
    """A 403 wrapped in this gateway's RPCError (via original_error) is caught."""
    wrapped = RPCError("balanceOf failed", method="balanceOf", original_error=_AiohttpLikeError(403))
    assert is_rpc_entitlement_error(wrapped) is True


def test_walks_cause_chain():
    """A 403 linked through `raise ... from` (__cause__) is caught."""
    try:
        try:
            raise _AiohttpLikeError(403)
        except _AiohttpLikeError as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        assert is_rpc_entitlement_error(outer) is True


def test_cause_cycle_does_not_hang():
    """A pathological cause cycle is bounded, not infinite."""
    a = Exception("a")
    b = Exception("b")
    a.__cause__ = b
    b.__cause__ = a
    assert is_rpc_entitlement_error(a) is False  # returns, does not hang


# --------------------------------------------------------------------------- #
# entitlement_error_message
# --------------------------------------------------------------------------- #


def test_local_message_names_chain_method_and_override(monkeypatch):
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    msg = entitlement_error_message("robinhood", "eth_getBalance")
    assert "robinhood" in msg
    assert "eth_getBalance" in msg
    assert "ALMANAK_ROBINHOOD_RPC_URL" in msg  # the exact override the operator needs
    assert "will not resolve by retrying" in msg.lower() or "not resolve by retrying" in msg.lower()


def test_local_message_includes_public_rpc_hint_when_registered(monkeypatch):
    """robinhood declares a public_rpc — the message should paste it as the fix value."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    msg = entitlement_error_message("robinhood", "eth_getBalance")
    assert "rpc.mainnet.chain.robinhood.com" in msg


def test_hosted_message_requires_platform_operator_and_omits_public_rpc(monkeypatch):
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")

    msg = entitlement_error_message("robinhood", "eth_getBalance")

    assert "Platform operator action required" in msg
    assert "unapproved public endpoint" in msg
    assert "ALMANAK_ROBINHOOD_RPC_URL" not in msg
    assert "rpc.mainnet.chain.robinhood.com" not in msg
