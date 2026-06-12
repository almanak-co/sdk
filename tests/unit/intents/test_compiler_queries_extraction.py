"""Seam-preservation tests for the compiler_queries.py extraction (plan 016).

Two seams could silently break when moving query helpers out of IntentCompiler:

1. Instance-patch seam: tests patch helpers as instance attributes on the compiler
   (``patch.object(compiler, "_resolve_token", ...)``) and expect composite helpers
   like ``_parse_pool_info`` to call through to the patched version. This works only
   if composite helpers route cross-calls back through the host wrappers
   (``self._host._resolve_token``), not collaborator-internal calls.

2. Live-state seam: tests reassign compiler state (``compiler.price_oracle = {...}``,
   ``compiler._using_placeholders = False``) after construction and expect helpers to
   see the new values immediately. This works only if the collaborator reads state live
   through the host reference, not from values captured at init.

These tests are the regression contract for those two seams. They must pass before
and after any future refactoring of compiler_queries.py. Do NOT edit these tests to
make a refactor pass — a test failure here means the seam was broken.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import TokenInfo
from almanak.framework.intents.compiler_queries import CompilerQueries


def _make_compiler() -> IntentCompiler:
    """Build a minimal compiler with placeholder prices for unit testing."""
    return IntentCompiler(
        chain="arbitrum",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestInstancePatchSeam:
    """Composite helpers must route cross-calls through host wrappers so that
    instance-level patches on the compiler propagate into composite helpers."""

    def test_resolve_token_patch_propagates_into_parse_pool_info(self) -> None:
        """_parse_pool_info calls _resolve_token through host wrapper.

        test_compiler_curve.py:950 patches ``compiler._resolve_token = MagicMock(return_value=None)``
        and expects pool parsing to return None. This verifies that seam still works.
        """
        compiler = _make_compiler()
        with patch.object(compiler, "_resolve_token", return_value=None):
            result = compiler._parse_pool_info("WETH/USDC/3000")
        assert result is None, (
            "_parse_pool_info should return None when _resolve_token returns None "
            "(verifies cross-call routes through host wrapper, not collaborator-internal)"
        )

    def test_require_token_price_patch_propagates_into_usd_to_token_amount(self) -> None:
        """_usd_to_token_amount calls _require_token_price through host wrapper."""
        compiler = _make_compiler()
        token = TokenInfo(symbol="X", address="0x" + "11" * 20, decimals=6)
        with patch.object(compiler, "_require_token_price", return_value=Decimal("2")):
            result = compiler._usd_to_token_amount(Decimal("4"), token)
        # $4 / $2_per_token * 10^6 = 2_000_000
        assert result == 2_000_000, (
            f"Expected 2_000_000, got {result}. "
            "_usd_to_token_amount must route _require_token_price through host wrapper."
        )

    def test_wrapper_delegation_to_query_erc20_balance(self) -> None:
        """Patching the collaborator method is reflected through the compiler wrapper."""
        compiler = _make_compiler()
        with patch.object(type(compiler._queries), "query_erc20_balance", return_value=7):
            result = compiler._query_erc20_balance("0x" + "22" * 20, compiler.wallet_address)
        assert result == 7, (
            f"Expected 7 from patched CompilerQueries.query_erc20_balance, got {result}"
        )


class TestLiveStateSeam:
    """Helpers must see state reassigned after construction, not values captured at init."""

    def test_price_oracle_reassignment_seen_by_require_token_price(self) -> None:
        """_require_token_price reads price_oracle live, not from init-time snapshot."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("100")},
        )
        # Reassign after construction — the collaborator must see the new value
        compiler.price_oracle = {"ETH": Decimal("200")}
        compiler._using_placeholders = False

        price = compiler._require_token_price("ETH")
        assert price == Decimal("200"), (
            f"Expected Decimal('200'), got {price}. "
            "_require_token_price must read price_oracle live (no capture at init)."
        )

    def test_using_placeholders_reassignment_seen_by_require_token_price(self) -> None:
        """Toggling _using_placeholders after construction affects behavior immediately."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("100")},
        )
        # Force into placeholder mode — must return Decimal("1") for unknown token
        compiler._using_placeholders = True
        compiler.price_oracle = None

        price = compiler._require_token_price("UNKNOWN_TOKEN")
        assert price == Decimal("1"), (
            f"Expected Decimal('1') in placeholder mode, got {price}. "
            "_require_token_price must read _using_placeholders live."
        )


class TestNewBypassSeam:
    """Tests that bypass __init__ via IntentCompiler.__new__ must still work.

    Several test files (e.g. test_lp_open_token_sorting.py) create a bare
    IntentCompiler via __new__ then set a minimal subset of attributes before
    calling methods. The _queries property must lazy-init without AttributeError.
    """

    def test_parse_pool_info_works_on_new_bypassed_compiler(self) -> None:
        """_parse_pool_info works on a compiler created via __new__ with a mocked resolver."""
        c = IntentCompiler.__new__(IntentCompiler)
        c.chain = "arbitrum"
        mock_token = MagicMock()
        mock_token.symbol = "WETH"
        mock_token.address = "0xfff9976782d46CC05630D1f6eBAb18b2324d6B14"
        mock_token.decimals = 18
        mock_token.is_native = False
        # _queries must lazy-init here; if it raises AttributeError the seam is broken
        with patch.object(c, "_resolve_token", return_value=mock_token):
            result = c._parse_pool_info("WETH/WETH/3000")
        # Both tokens resolve to same address -> same token, tokens_swapped=False
        assert result is not None


# ---------------------------------------------------------------------------
# Helper: build a minimal stub host for CompilerQueries
# ---------------------------------------------------------------------------


def _make_stub_host(
    *,
    chain: str = "arbitrum",
    rpc_url: str | None = None,
    gateway_client: object | None = None,
    web3: object | None = None,
) -> SimpleNamespace:
    """Build a minimal stub host that satisfies CompilerQueryHost.

    All attributes are set to safe defaults; tests override what they need.
    """
    host = SimpleNamespace()
    host.chain = chain
    host.rpc_url = rpc_url
    host._gateway_client = gateway_client
    host._web3 = web3
    host.rpc_timeout = 10.0
    # These aren't used by the balance/position methods, but keep the namespace complete
    host.price_oracle = None
    host._using_placeholders = False
    host._token_resolver = MagicMock()
    host._stablecoin_fallback_logged = set()
    # Stub out host wrapper methods used by _for_chain variants
    host._query_erc20_balance = MagicMock(return_value=9999)
    host._query_native_balance = MagicMock(return_value=8888)
    host._get_rpc_url_for_chain = MagicMock(return_value=None)
    return host


# ---------------------------------------------------------------------------
# Tests for query_native_balance
# ---------------------------------------------------------------------------


class TestQueryNativeBalance:
    """Coverage for CompilerQueries.query_native_balance (cc=8, tripping CRAP gate).

    Verbatim extraction from compiler.py under plan 016. Tests cover the
    primary decision branches: gateway happy path, gateway error, no-rpc
    early-return, and existing cached web3.
    """

    def test_gateway_happy_path_returns_balance(self) -> None:
        """When gateway_client is set, delegates to it and returns the balance."""
        gw = MagicMock()
        gw.query_native_balance.return_value = 1_000_000
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_native_balance("0xWallet")

        assert result == 1_000_000
        gw.query_native_balance.assert_called_once_with(chain="arbitrum", wallet_address="0xWallet")

    def test_gateway_error_returns_none(self) -> None:
        """When gateway call raises, returns None (fail-safe)."""
        gw = MagicMock()
        gw.query_native_balance.side_effect = RuntimeError("rpc timeout")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_native_balance("0xWallet")

        assert result is None

    def test_no_gateway_no_rpc_returns_none(self) -> None:
        """Without gateway or rpc_url, returns None immediately."""
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=None)
        cq = CompilerQueries(host)

        result = cq.query_native_balance("0xWallet")

        assert result is None

    def test_existing_web3_returns_balance(self) -> None:
        """When _web3 is already cached, uses it to get balance without HTTP construction."""
        mock_web3 = MagicMock()
        mock_web3.eth.get_balance.return_value = 5_000_000
        mock_web3.to_checksum_address.return_value = "0xChecksumWallet"
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_native_balance("0xWallet")

        assert result == 5_000_000
        mock_web3.eth.get_balance.assert_called_once_with("0xChecksumWallet")

    def test_web3_exception_returns_none(self) -> None:
        """When web3.eth.get_balance raises, returns None."""
        mock_web3 = MagicMock()
        mock_web3.eth.get_balance.side_effect = Exception("connection error")
        mock_web3.to_checksum_address.return_value = "0xChecksumWallet"
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_native_balance("0xWallet")

        assert result is None


# ---------------------------------------------------------------------------
# Tests for query_erc20_balance
# ---------------------------------------------------------------------------


class TestQueryErc20Balance:
    """Coverage for CompilerQueries.query_erc20_balance (cc=8, tripping CRAP gate)."""

    def test_gateway_happy_path_returns_balance(self) -> None:
        """Gateway call returns the token balance."""
        gw = MagicMock()
        gw.query_erc20_balance.return_value = 500_000
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_erc20_balance("0xToken", "0xWallet")

        assert result == 500_000
        gw.query_erc20_balance.assert_called_once_with(
            chain="arbitrum", token_address="0xToken", wallet_address="0xWallet"
        )

    def test_gateway_error_returns_none(self) -> None:
        """Gateway error is swallowed; returns None."""
        gw = MagicMock()
        gw.query_erc20_balance.side_effect = RuntimeError("bad")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        assert cq.query_erc20_balance("0xToken", "0xWallet") is None

    def test_no_gateway_no_rpc_returns_none(self) -> None:
        """No gateway, no rpc_url, no cached web3 - returns None."""
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=None)
        cq = CompilerQueries(host)

        assert cq.query_erc20_balance("0xToken", "0xWallet") is None

    def test_existing_web3_returns_balance(self) -> None:
        """Uses cached web3 instance to call balanceOf."""
        mock_web3 = MagicMock()
        # balanceOf returns a 32-byte big-endian integer
        mock_web3.eth.call.return_value = (12345).to_bytes(32, byteorder="big")
        mock_web3.to_checksum_address.side_effect = lambda x: x.upper()
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_erc20_balance("0xtoken", "0xwallet")

        assert result == 12345

    def test_web3_call_exception_returns_none(self) -> None:
        """web3.eth.call raising returns None."""
        mock_web3 = MagicMock()
        mock_web3.eth.call.side_effect = Exception("revert")
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        assert cq.query_erc20_balance("0xToken", "0xWallet") is None


# ---------------------------------------------------------------------------
# Tests for query_erc20_balance_for_chain
# ---------------------------------------------------------------------------


class TestQueryErc20BalanceForChain:
    """Coverage for CompilerQueries.query_erc20_balance_for_chain (cc=7, tripping CRAP gate)."""

    def test_same_chain_delegates_to_host_wrapper(self) -> None:
        """When chain matches host.chain, calls host._query_erc20_balance."""
        host = _make_stub_host(chain="arbitrum")
        host._query_erc20_balance.return_value = 7777
        cq = CompilerQueries(host)

        result = cq.query_erc20_balance_for_chain("0xToken", "0xWallet", "arbitrum")

        assert result == 7777
        host._query_erc20_balance.assert_called_once_with("0xToken", "0xWallet")

    def test_cross_chain_gateway_happy_path(self) -> None:
        """Cross-chain query: uses gateway when available."""
        gw = MagicMock()
        gw.query_erc20_balance.return_value = 3333
        host = _make_stub_host(chain="arbitrum", gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_erc20_balance_for_chain("0xToken", "0xWallet", "base")

        assert result == 3333
        gw.query_erc20_balance.assert_called_once_with(
            chain="base", token_address="0xToken", wallet_address="0xWallet"
        )

    def test_cross_chain_gateway_error_returns_none(self) -> None:
        """Cross-chain gateway error returns None (fail-closed)."""
        gw = MagicMock()
        gw.query_erc20_balance.side_effect = RuntimeError("gateway down")
        host = _make_stub_host(chain="arbitrum", gateway_client=gw)
        cq = CompilerQueries(host)

        assert cq.query_erc20_balance_for_chain("0xToken", "0xWallet", "base") is None

    def test_no_gateway_no_rpc_returns_none(self) -> None:
        """No gateway, no rpc_url for target chain - returns None."""
        host = _make_stub_host(chain="arbitrum", gateway_client=None)
        host._get_rpc_url_for_chain.return_value = None
        cq = CompilerQueries(host)

        assert cq.query_erc20_balance_for_chain("0xToken", "0xWallet", "base") is None


# ---------------------------------------------------------------------------
# Tests for query_native_balance_for_chain
# ---------------------------------------------------------------------------


class TestQueryNativeBalanceForChain:
    """Coverage for CompilerQueries.query_native_balance_for_chain (cc=7, tripping CRAP gate)."""

    def test_same_chain_delegates_to_host_wrapper(self) -> None:
        """When chain matches host.chain, calls host._query_native_balance."""
        host = _make_stub_host(chain="arbitrum")
        host._query_native_balance.return_value = 6666
        cq = CompilerQueries(host)

        result = cq.query_native_balance_for_chain("0xWallet", "arbitrum")

        assert result == 6666
        host._query_native_balance.assert_called_once_with("0xWallet")

    def test_cross_chain_gateway_happy_path(self) -> None:
        """Cross-chain query: uses gateway when available."""
        gw = MagicMock()
        gw.query_native_balance.return_value = 4444
        host = _make_stub_host(chain="arbitrum", gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_native_balance_for_chain("0xWallet", "base")

        assert result == 4444
        gw.query_native_balance.assert_called_once_with(chain="base", wallet_address="0xWallet")

    def test_cross_chain_gateway_error_returns_none(self) -> None:
        """Cross-chain gateway error returns None (fail-closed)."""
        gw = MagicMock()
        gw.query_native_balance.side_effect = RuntimeError("timeout")
        host = _make_stub_host(chain="arbitrum", gateway_client=gw)
        cq = CompilerQueries(host)

        assert cq.query_native_balance_for_chain("0xWallet", "base") is None

    def test_no_gateway_no_rpc_returns_none(self) -> None:
        """No gateway, no rpc_url for target chain - returns None."""
        host = _make_stub_host(chain="arbitrum", gateway_client=None)
        host._get_rpc_url_for_chain.return_value = None
        cq = CompilerQueries(host)

        assert cq.query_native_balance_for_chain("0xWallet", "base") is None


# ---------------------------------------------------------------------------
# Tests for query_position_liquidity
# ---------------------------------------------------------------------------


class TestQueryPositionLiquidity:
    """Coverage for CompilerQueries.query_position_liquidity (cc=10, tripping CRAP gate)."""

    def test_gateway_happy_path_returns_liquidity(self) -> None:
        """Gateway returns liquidity value."""
        gw = MagicMock()
        gw.query_position_liquidity.return_value = 987654
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_liquidity("0xPosManager", 42)

        assert result == 987654
        gw.query_position_liquidity.assert_called_once_with(
            chain="arbitrum", position_manager="0xPosManager", token_id=42
        )

    def test_gateway_invalid_token_id_returns_zero(self) -> None:
        """Gateway raises with 'invalid token id' message - position is closed, returns 0."""
        gw = MagicMock()
        gw.query_position_liquidity.side_effect = Exception("Invalid token id for position")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_liquidity("0xPosManager", 99)

        assert result == 0

    def test_gateway_other_error_returns_none(self) -> None:
        """Generic gateway error returns None."""
        gw = MagicMock()
        gw.query_position_liquidity.side_effect = RuntimeError("gateway unavailable")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_liquidity("0xPosManager", 42)

        assert result is None

    def test_no_gateway_no_rpc_returns_none(self) -> None:
        """No gateway, no rpc_url, no cached web3 - returns None early."""
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=None)
        cq = CompilerQueries(host)

        result = cq.query_position_liquidity("0xPosManager", 42)

        assert result is None

    def test_existing_web3_short_result_returns_none(self) -> None:
        """When web3 is cached but result is too short, returns None."""
        mock_web3 = MagicMock()
        # Return fewer than 256 bytes so the length check fails
        mock_web3.eth.call.return_value = bytes(100)
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_position_liquidity("0xPosManager", 42)

        assert result is None

    def test_existing_web3_valid_result_returns_liquidity(self) -> None:
        """Cached web3 with a 256-byte result decodes liquidity at offset 7*32."""
        mock_web3 = MagicMock()
        # Build a 256-byte response with liquidity=12345 at offset 7*32
        data = bytearray(256)
        liquidity_offset = 7 * 32
        data[liquidity_offset : liquidity_offset + 32] = (12345).to_bytes(32, byteorder="big")
        mock_web3.eth.call.return_value = bytes(data)
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_position_liquidity("0xPosManager", 42)

        assert result == 12345

    def test_existing_web3_exception_returns_none(self) -> None:
        """Exception from web3 call returns None."""
        mock_web3 = MagicMock()
        mock_web3.eth.call.side_effect = Exception("call reverted")
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        assert cq.query_position_liquidity("0xPosManager", 42) is None


# ---------------------------------------------------------------------------
# Tests for query_position_tokens_owed
# ---------------------------------------------------------------------------


class TestQueryPositionTokensOwed:
    """Coverage for CompilerQueries.query_position_tokens_owed (cc=17, tripping CRAP gate)."""

    def _make_success_response(self, tokens_owed0: str = "100", tokens_owed1: str = "200") -> MagicMock:
        resp = MagicMock()
        resp.success = True
        resp.tokens_owed0 = tokens_owed0
        resp.tokens_owed1 = tokens_owed1
        return resp

    def _make_failed_response(self, error: str = "") -> MagicMock:
        resp = MagicMock()
        resp.success = False
        resp.error = error
        return resp

    def test_gateway_happy_path_returns_tokens_owed(self) -> None:
        """Gateway returns a success response; tokens are parsed and returned."""
        gw = MagicMock()
        resp = MagicMock()
        resp.success = True
        resp.tokens_owed0 = "1500"
        resp.tokens_owed1 = "2500"
        gw.rpc.QueryPositionTokensOwed.return_value = resp
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (1500, 2500)

    def test_gateway_position_not_found_returns_zero_tuple(self) -> None:
        """'position not found' in error message returns (0, 0)."""
        gw = MagicMock()
        resp = self._make_failed_response("position not found")
        gw.rpc.QueryPositionTokensOwed.return_value = resp
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (0, 0)

    def test_gateway_invalid_token_id_in_error_returns_zero_tuple(self) -> None:
        """'invalid token id' in error message returns (0, 0)."""
        gw = MagicMock()
        resp = self._make_failed_response("invalid token id")
        gw.rpc.QueryPositionTokensOwed.return_value = resp
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (0, 0)

    def test_gateway_other_failure_returns_none_tuple(self) -> None:
        """Other gateway failure returns (None, None)."""
        gw = MagicMock()
        gw.rpc.QueryPositionTokensOwed.return_value = self._make_failed_response("unknown error")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (None, None)

    def test_gateway_exception_invalid_token_id_returns_zero_tuple(self) -> None:
        """Exception with 'invalid token id' in message returns (0, 0)."""
        gw = MagicMock()
        gw.rpc.QueryPositionTokensOwed.side_effect = Exception("Invalid token id")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (0, 0)

    def test_gateway_exception_other_returns_none_tuple(self) -> None:
        """Generic exception from gateway returns (None, None)."""
        gw = MagicMock()
        gw.rpc.QueryPositionTokensOwed.side_effect = RuntimeError("timeout")
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (None, None)

    def test_no_gateway_no_rpc_returns_none_tuple(self) -> None:
        """No gateway, no rpc_url, no cached web3 - returns (None, None) early."""
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=None)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (None, None)

    def test_existing_web3_valid_result_returns_tokens_owed(self) -> None:
        """Cached web3 with >= 384-byte result decodes tokens at offsets 10*32 and 11*32."""
        mock_web3 = MagicMock()
        data = bytearray(384)
        offset0 = 10 * 32
        offset1 = 11 * 32
        data[offset0 : offset0 + 32] = (777).to_bytes(32, byteorder="big")
        data[offset1 : offset1 + 32] = (888).to_bytes(32, byteorder="big")
        mock_web3.eth.call.return_value = bytes(data)
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (777, 888)

    def test_existing_web3_short_result_returns_none_tuple(self) -> None:
        """Short result (< 384 bytes) returns (None, None)."""
        mock_web3 = MagicMock()
        mock_web3.eth.call.return_value = bytes(200)
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (None, None)

    def test_existing_web3_exception_returns_none_tuple(self) -> None:
        """Exception from web3.eth.call returns (None, None)."""
        mock_web3 = MagicMock()
        mock_web3.eth.call.side_effect = Exception("reverted")
        mock_web3.to_checksum_address.side_effect = lambda x: x
        host = _make_stub_host(gateway_client=None, rpc_url=None, web3=mock_web3)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (None, None)

    def test_gateway_success_parse_error_returns_none_tuple(self) -> None:
        """Unparseable tokens_owed values in success response returns (None, None)."""
        gw = MagicMock()
        resp = MagicMock()
        resp.success = True
        resp.tokens_owed0 = "not_an_int"
        resp.tokens_owed1 = "200"
        gw.rpc.QueryPositionTokensOwed.return_value = resp
        host = _make_stub_host(gateway_client=gw)
        cq = CompilerQueries(host)

        result = cq.query_position_tokens_owed("0xPosManager", 7)

        assert result == (None, None)
