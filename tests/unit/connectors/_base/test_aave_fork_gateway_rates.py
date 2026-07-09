"""Unit tests for the fork-shared Aave-V3 gateway lending-rate pipeline.

Direct coverage of the failure branches in
:mod:`almanak.connectors._base.aave_fork_gateway_rates` — the shared
``getReserveData`` pipeline both Aave V3 and Spark route through. The
gateway dispatcher tests (``tests/gateway/services/test_rate_history_*``)
only exercise a couple of happy paths + the all-zero case indirectly; this
module drives each helper's error path so a malformed / hostile RPC reply
surfaces as ``RateHistoryUnavailable`` (never an unhandled ``TypeError`` /
``AttributeError`` / ``ValueError`` crossing the gateway boundary).

The shared module drives real money data for two protocols and needs its
branches pinned directly, not just through the dispatcher.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors._base import aave_fork_gateway_rates as mod
from almanak.gateway.services.rate_history_service import LendingRatePoint, RateHistoryUnavailable

_PROTOCOL = "aave_v3"
_DISPLAY = "Aave"
_CONTRACTS = {"ethereum": {"pool_data_provider": "0xDATA"}}
_TOKENS = {"ethereum": {"USDC": "0x" + "a" * 40}}


def _reserve_hex(
    supply_ray: int,
    borrow_ray: int,
    *,
    total_atoken: int = 10**12,
    total_variable_debt: int = 5 * 10**11,
) -> str:
    """Build a valid 12-word ``getReserveData`` return blob (see the ABI order)."""
    words = [
        0,  # 0 unbacked
        0,  # 1 accruedToTreasuryScaled
        total_atoken,  # 2 totalAToken
        0,  # 3 totalStableDebt
        total_variable_debt,  # 4 totalVariableDebt
        supply_ray,  # 5 liquidityRate
        borrow_ray,  # 6 variableBorrowRate
        0,  # 7 stableBorrowRate
        0,  # 8 averageStableBorrowRate
        10**27,  # 9 liquidityIndex
        10**27,  # 10 variableBorrowIndex
        1_700_000_000,  # 11 lastUpdateTimestamp
    ]
    return "0x" + "".join(w.to_bytes(32, "big").hex() for w in words)


# =============================================================================
# _resolve_data_provider
# =============================================================================


class TestResolveDataProvider:
    def test_resolves_configured_address(self) -> None:
        assert mod._resolve_data_provider(_PROTOCOL, _CONTRACTS, "ethereum") == "0xDATA"

    def test_missing_chain_raises(self) -> None:
        with pytest.raises(RateHistoryUnavailable) as exc:
            mod._resolve_data_provider(_PROTOCOL, _CONTRACTS, "arbitrum")
        assert "No PoolDataProvider configured" in exc.value.reason

    def test_empty_provider_string_raises(self) -> None:
        with pytest.raises(RateHistoryUnavailable):
            mod._resolve_data_provider(_PROTOCOL, {"ethereum": {"pool_data_provider": ""}}, "ethereum")


# =============================================================================
# _resolve_token_address
# =============================================================================


class TestResolveTokenAddress:
    def test_curated_table_hit_skips_resolver(self) -> None:
        addr = mod._resolve_token_address(_PROTOCOL, _DISPLAY, _TOKENS, "ethereum", "USDC")
        assert addr == _TOKENS["ethereum"]["USDC"]

    def test_resolver_fallback_hit(self) -> None:
        resolver = MagicMock()
        resolver.resolve.return_value = MagicMock(address="0xRESOLVED")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            addr = mod._resolve_token_address(_PROTOCOL, _DISPLAY, {}, "ethereum", "USDC")
        assert addr == "0xRESOLVED"
        resolver.resolve.assert_called_once_with("USDC", "ethereum")

    def test_resolver_token_not_found_raises_unavailable(self) -> None:
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        resolver = MagicMock()
        resolver.resolve.side_effect = TokenNotFoundError(token="ZZZ", chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            with pytest.raises(RateHistoryUnavailable) as exc:
                mod._resolve_token_address(_PROTOCOL, _DISPLAY, {}, "ethereum", "ZZZ")
        assert "not in Aave catalogue" in exc.value.reason


# =============================================================================
# _resolve_rpc_url
# =============================================================================


class TestResolveRpcUrl:
    def test_returns_url(self) -> None:
        servicer = MagicMock()
        servicer.settings.network = "mainnet"
        with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
            assert mod._resolve_rpc_url(_PROTOCOL, servicer, "ethereum") == "http://rpc.test"

    def test_value_error_raises_unavailable(self) -> None:
        servicer = MagicMock()
        servicer.settings.network = "mainnet"
        with patch("almanak.gateway.utils.get_rpc_url", side_effect=ValueError("no url")):
            with pytest.raises(RateHistoryUnavailable) as exc:
                mod._resolve_rpc_url(_PROTOCOL, servicer, "ethereum")
        assert "No RPC URL configured" in exc.value.reason


# =============================================================================
# _post_get_reserve_data
# =============================================================================


def _session_returning(json_value: Any) -> MagicMock:
    """aiohttp-shaped mock session whose POST yields ``json_value`` from .json()."""
    response = AsyncMock()
    response.raise_for_status = MagicMock()
    response.json = AsyncMock(return_value=json_value)

    def _post(url: str, *, json: dict[str, Any]) -> Any:
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=response)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    session = MagicMock()
    session.post = _post
    return session


def _session_raising(exc: Exception) -> MagicMock:
    def _post(url: str, *, json: dict[str, Any]) -> Any:
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(side_effect=exc)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    session = MagicMock()
    session.post = _post
    return session


def _post_reserve(session: MagicMock) -> str:
    return asyncio.run(
        mod._post_get_reserve_data(
            _PROTOCOL,
            session,
            rpc_url="http://rpc.test",
            data_provider="0xDATA",
            calldata="0xdeadbeef",
            chain="ethereum",
        )
    )


class TestPostGetReserveData:
    def test_happy_path_returns_result(self) -> None:
        session = _session_returning({"jsonrpc": "2.0", "id": 1, "result": "0xabc"})
        assert _post_reserve(session) == "0xabc"

    def test_missing_result_returns_empty_string(self) -> None:
        session = _session_returning({"jsonrpc": "2.0", "id": 1})
        assert _post_reserve(session) == ""

    def test_transport_exception_raises_unavailable(self) -> None:
        session = _session_raising(RuntimeError("connection reset"))
        with pytest.raises(RateHistoryUnavailable) as exc:
            _post_reserve(session)
        assert "RPC request / decode failed" in exc.value.reason

    def test_rpc_error_object_raises_unavailable(self) -> None:
        session = _session_returning({"error": {"code": -32000, "message": "execution reverted"}})
        with pytest.raises(RateHistoryUnavailable) as exc:
            _post_reserve(session)
        assert "execution reverted" in exc.value.reason

    def test_rpc_error_bare_string_raises_unavailable(self) -> None:
        """The JSON-RPC ``error`` member should be an object, but a bare string
        must not crash on ``.get`` — it is coerced into the message."""
        session = _session_returning({"error": "boom"})
        with pytest.raises(RateHistoryUnavailable) as exc:
            _post_reserve(session)
        assert "boom" in exc.value.reason

    def test_non_dict_response_raises_unavailable(self) -> None:
        """A list / string / null JSON body must fail closed, not AttributeError."""
        for bad in ([], "0xabc", None, 42):
            session = _session_returning(bad)
            with pytest.raises(RateHistoryUnavailable) as exc:
                _post_reserve(session)
            assert "unexpected RPC response" in exc.value.reason


# =============================================================================
# _split_hex_words / _words_all_zero
# =============================================================================


class TestSplitAndZero:
    def test_split_into_32_byte_words(self) -> None:
        words = mod._split_hex_words("0x" + "11" * 32 + "22" * 32)
        assert len(words) == 2
        assert words[0] == b"\x11" * 32
        assert words[1] == b"\x22" * 32

    def test_words_all_zero_true(self) -> None:
        assert mod._words_all_zero([b"\x00" * 32, b"\x00" * 32]) is True

    def test_words_all_zero_false(self) -> None:
        assert mod._words_all_zero([b"\x00" * 32, b"\x01" + b"\x00" * 31]) is False


# =============================================================================
# _decode_reserve_words
# =============================================================================


def _decode(hex_data: str) -> list[bytes]:
    return mod._decode_reserve_words(_PROTOCOL, _DISPLAY, hex_data, chain="ethereum", asset_symbol="USDC")


class TestDecodeReserveWords:
    def test_valid_blob_decodes(self) -> None:
        words = _decode(_reserve_hex(5 * 10**25, 7 * 10**25))
        assert len(words) == 12

    def test_empty_raises(self) -> None:
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("")
        assert "not a registered Aave reserve" in exc.value.reason

    def test_bare_0x_raises(self) -> None:
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("0x")
        assert "not a registered Aave reserve" in exc.value.reason

    def test_missing_0x_prefix_raises(self) -> None:
        # A hex body without the 0x prefix would slice wrong in _split_hex_words.
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("ab" * 32)
        assert "not a registered Aave reserve" in exc.value.reason

    def test_non_word_aligned_length_raises(self) -> None:
        # 0x + 100 hex chars: (100 % 64) != 0 → malformed, rejected before fromhex.
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("0x" + "ab" * 50)
        assert "not word-aligned" in exc.value.reason

    def test_non_hex_chars_raise_malformed(self) -> None:
        # 64 chars, word-aligned length, but not valid hex → ValueError → unavailable.
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("0x" + "zz" * 32)
        assert "malformed getReserveData hex" in exc.value.reason

    def test_short_response_raises(self) -> None:
        # Six full 32-byte words (< _MIN_RESPONSE_WORDS = 7), word-aligned, non-zero.
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("0x" + "11" * 32 * 6)
        assert "unexpected getReserveData response" in exc.value.reason

    def test_all_zero_struct_raises(self) -> None:
        with pytest.raises(RateHistoryUnavailable) as exc:
            _decode("0x" + "00" * (12 * 32))
        assert "not a listed Aave reserve" in exc.value.reason


# =============================================================================
# _compute_apy_and_utilization
# =============================================================================


class TestComputeApyAndUtilization:
    def test_supply_side(self) -> None:
        words = _decode(_reserve_hex(5 * 10**25, 7 * 10**25, total_atoken=10**12, total_variable_debt=72 * 10**10))
        apy, util = mod._compute_apy_and_utilization(words, side="supply")
        assert apy == Decimal("5")
        assert util == Decimal("72")

    def test_borrow_side(self) -> None:
        words = _decode(_reserve_hex(5 * 10**25, 7 * 10**25))
        apy, _util = mod._compute_apy_and_utilization(words, side="borrow")
        assert apy == Decimal("7")

    def test_zero_atoken_yields_none_utilization(self) -> None:
        words = _decode(_reserve_hex(5 * 10**25, 7 * 10**25, total_atoken=0, total_variable_debt=0))
        _apy, util = mod._compute_apy_and_utilization(words, side="supply")
        assert util is None


# =============================================================================
# fetch_aave_fork_lending_current (integration of the helpers)
# =============================================================================


def _run_fetch(session: MagicMock, *, side: str = "supply") -> LendingRatePoint:
    servicer = MagicMock()
    servicer.settings.network = "mainnet"
    servicer._get_http_session = AsyncMock(return_value=session)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        return asyncio.run(
            mod.fetch_aave_fork_lending_current(
                servicer,
                protocol=_PROTOCOL,
                display_name=_DISPLAY,
                contracts_by_chain=_CONTRACTS,
                tokens_by_chain=_TOKENS,
                chain="ethereum",
                asset_symbol="USDC",
                side=side,
            )
        )


class TestFetchLendingCurrent:
    def test_supply_point_only_supply_populated(self) -> None:
        session = _session_returning(
            {"result": _reserve_hex(5 * 10**25, 7 * 10**25, total_atoken=10**12, total_variable_debt=72 * 10**10)}
        )
        point = _run_fetch(session, side="supply")
        assert point.supply_apy_pct == Decimal("5")
        assert point.borrow_apy_pct is None  # Empty != Zero: unmeasured this call.
        assert point.utilization_pct == Decimal("72")

    def test_malformed_response_propagates_unavailable(self) -> None:
        session = _session_returning({"result": "0x" + "ab" * 50})
        with pytest.raises(RateHistoryUnavailable):
            _run_fetch(session)
