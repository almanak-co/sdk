"""Tests for Curve single-sided LP_CLOSE (VIB-5437, audit P0-4 + P2-3).

Before this change ``LPCloseIntent`` had no way to express a single-sided exit:
the Curve compiler only ever emitted the proportional ``remove_liquidity``, so
``remove_liquidity_one_coin`` (and its ``calc_withdraw_one_coin`` min-out) was
unreachable dead code. This adds an optional ``coin_index`` to the intent
vocabulary, wires the compiler to emit ``remove_liquidity_one_coin`` when it is
set, and replaces the old flat 1% penalty estimate with the pool's real on-chain
``calc_withdraw_one_coin`` quote — derived FAIL-CLOSED exactly like the VIB-5441
LP-open ``min_lp`` floor (never ship ``min_amount=0``, a 100% sandwich vector).

Selector families verified on real mainnet 2026-06-29:

* StableSwap (3pool): ``calc_withdraw_one_coin(uint256,int128)`` = 0xcc2b27d7,
  ``remove_liquidity_one_coin(uint256,int128,uint256)`` = 0x1a4d01d2.
* CryptoSwap (tricrypto2): ``calc_withdraw_one_coin(uint256,uint256)`` = 0x4fb08c5e,
  ``remove_liquidity_one_coin(uint256,uint256,uint256)`` = 0xf1dc3cc9.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from almanak.connectors.curve.adapter import (
    CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR,
    CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR,
    REMOVE_LIQUIDITY_ONE_CRYPTO_SELECTOR,
    REMOVE_LIQUIDITY_ONE_SELECTOR,
    CurveAdapter,
    CurveConfig,
    PoolInfo,
    PoolType,
)
from almanak.connectors.curve.compiler import CurveCompiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent

WALLET = "0x1234567890123456789012345678901234567890"
_EC = "almanak.connectors.curve.adapter.eth_call_uint256"


def _rpc_adapter(chain: str = "ethereum") -> CurveAdapter:
    """Adapter wired with an rpc_url so the on-chain quote path (mocked at
    eth_call_uint256) is reached rather than the no-transport fail-closed guard."""
    return CurveAdapter(
        CurveConfig(
            chain=chain,
            wallet_address=WALLET,
            default_slippage_bps=50,
            rpc_url="http://localhost:8545",
        )
    )


def _stable_pool() -> PoolInfo:
    return PoolInfo(
        address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        lp_token="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
        coins=["DAI", "USDC", "USDT"],
        coin_addresses=["0x6b17", "0xa0b8", "0xdac1"],
        pool_type=PoolType.STABLESWAP,
        n_coins=3,
        name="3pool",
    )


def _crypto_pool() -> PoolInfo:
    return PoolInfo(
        address="0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
        lp_token="0xc4AD29ba4B3c580e6D59105FFf484999997675Ff",
        coins=["USDT", "WBTC", "WETH"],
        coin_addresses=["0xdac1", "0x2260", "0xc02a"],
        pool_type=PoolType.TRICRYPTO,
        n_coins=3,
        name="tricrypto2",
    )


# =============================================================================
# Adapter: on-chain calc_withdraw_one_coin min-out + selector dispatch
# =============================================================================


class TestCalcWithdrawOneCoinMinOut:
    def test_min_out_derived_from_onchain_quote_with_slippage(self) -> None:
        adapter = _rpc_adapter()
        quote = 1_000 * 10**6  # USDC gross expected out (6 decimals)
        with patch(_EC, return_value=quote):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address,
                lp_amount=Decimal("1000"),
                coin_index=1,
                slippage_bps=50,
            )
        assert result.success is True
        # 50 bps applied: floor = quote * 9950 // 10000, strictly below gross, non-zero.
        assert result.amounts[1] == quote * (10000 - 50) // 10000
        assert 0 < result.amounts[1] < quote

    def test_explicit_zero_slippage_yields_exact_quote(self) -> None:
        """slippage_bps=0 must mean min_out == expected_out (no default haircut)."""
        adapter = _rpc_adapter()
        quote = 1_000 * 10**6
        with patch(_EC, return_value=quote):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address,
                lp_amount=Decimal("1000"),
                coin_index=1,
                slippage_bps=0,
            )
        assert result.success is True
        assert result.amounts[1] == quote  # exact, NOT quote * 9950 // 10000

    def test_stableswap_uses_int128_selectors(self) -> None:
        adapter = _rpc_adapter()
        seen: list[str] = []

        def _capture(*, data: str, **_: Any) -> int:
            seen.append(data[:10])
            return 1_000 * 10**6

        with patch(_EC, side_effect=_capture):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1
            )
        assert result.success is True
        # The FIRST calc_withdraw_one_coin selector tried is the stable int128 form
        # (filter out any unrelated pool-registry refresh reads that precede it).
        calc_calls = [
            s for s in seen if s in (CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR, CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR)
        ]
        assert calc_calls[0] == CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        assert remove_tx.data.startswith(REMOVE_LIQUIDITY_ONE_SELECTOR)

    def test_cryptoswap_uses_uint256_selectors(self) -> None:
        adapter = _rpc_adapter()
        seen: list[str] = []

        def _capture(*, data: str, **_: Any) -> int:
            seen.append(data[:10])
            return 1_500 * 10**6

        with patch(_EC, side_effect=_capture):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_crypto_pool().address, lp_amount=Decimal("1"), coin_index=0
            )
        assert result.success is True
        # The FIRST calc_withdraw_one_coin selector tried is the crypto uint256 form
        # (filter out any unrelated pool-registry refresh reads that precede it).
        calc_calls = [
            s for s in seen if s in (CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR, CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR)
        ]
        assert calc_calls[0] == CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        assert remove_tx.data.startswith(REMOVE_LIQUIDITY_ONE_CRYPTO_SELECTOR)

    def test_falls_back_to_other_selector_when_primary_reverts(self) -> None:
        """A pool mislabelled in config still gets a quote via the fallback
        selector rather than failing closed — AND the emitted remove tx uses the
        selector family that actually answered (not the static label), so it does
        not revert on execution (CodeRabbit Major, VIB-5437)."""
        adapter = _rpc_adapter()

        def _fake(*, data: str, **_: Any) -> int | None:
            # Stable-labelled pool: primary (stable) calc selector "reverts" (None);
            # the CryptoSwap selector answers instead.
            if data.startswith(CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR):
                return None
            return 1_000 * 10**6

        with patch(_EC, side_effect=_fake):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1
            )
        assert result.success is True
        assert result.amounts[1] > 0
        # The crypto calc selector answered → the remove tx MUST use the crypto
        # remove selector, not the stale stable label.
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        assert remove_tx.data.startswith(REMOVE_LIQUIDITY_ONE_CRYPTO_SELECTOR)


class TestSingleSidedFailClosed:
    def test_no_transport_fails_closed(self) -> None:
        """No gateway and no rpc_url → refuse (never min_amount=0)."""
        adapter = CurveAdapter(CurveConfig(chain="ethereum", wallet_address=WALLET))
        result = adapter.remove_liquidity_one_coin(
            pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1
        )
        assert result.success is False
        assert "min_amount=0" in (result.error or "")

    def test_quote_unavailable_fails_closed(self) -> None:
        """On-chain read returns nothing for every selector → refuse."""
        adapter = _rpc_adapter()
        with patch(_EC, return_value=None):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1
            )
        assert result.success is False
        assert "no quote" in (result.error or "")

    def test_zero_quote_fails_closed(self) -> None:
        adapter = _rpc_adapter()
        with patch(_EC, return_value=0):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1
            )
        assert result.success is False

    def test_floor_rounds_to_zero_fails_closed(self) -> None:
        """A tiny quote that the slippage scaling floors to 0 must be rejected."""
        adapter = _rpc_adapter()
        # quote=1, slippage 50 bps → 1 * 9950 // 10000 == 0.
        with patch(_EC, return_value=1):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1, slippage_bps=50
            )
        assert result.success is False
        assert "min_amount is 0" in (result.error or "")

    def test_out_of_range_slippage_fails_closed(self) -> None:
        """A direct adapter caller passing slippage_bps>10000 (negative floor) must
        be rejected, not reach _pad_uint256 with a negative int (malformed calldata)."""
        adapter = _rpc_adapter()
        with patch(_EC, return_value=1_000 * 10**6):
            result = adapter.remove_liquidity_one_coin(
                pool_address=_stable_pool().address, lp_amount=Decimal("1000"), coin_index=1, slippage_bps=10001
            )
        assert result.success is False
        assert "must be > 0" in (result.error or "")

    def test_query_never_returns_zero(self) -> None:
        """The on-chain query helper raises rather than returning 0/None."""
        adapter = _rpc_adapter()
        with patch(_EC, return_value=None), pytest.raises(ValueError, match="no quote"):
            adapter._query_calc_withdraw_one_coin_onchain(_stable_pool(), 10**18, 0)


class TestDisconnectedGateway:
    def _gw_adapter(self, *, rpc: bool) -> tuple[CurveAdapter, Any]:
        from unittest.mock import MagicMock

        gw = MagicMock()
        gw.is_connected = False
        return (
            CurveAdapter(
                CurveConfig(
                    chain="ethereum",
                    wallet_address=WALLET,
                    gateway_client=gw,
                    rpc_url="http://localhost:8545" if rpc else None,
                )
            ),
            gw,
        )

    def test_disconnected_no_rpc_fails_closed(self) -> None:
        adapter, _ = self._gw_adapter(rpc=False)
        with pytest.raises(ValueError, match="disconnected"):
            adapter._query_calc_withdraw_one_coin_onchain(_stable_pool(), 10**18, 0)

    def test_disconnected_with_rpc_drops_gateway(self) -> None:
        adapter, _ = self._gw_adapter(rpc=True)
        captured: dict[str, Any] = {}

        def _fake(**kwargs: Any) -> int:
            captured.update(kwargs)
            return 1_000 * 10**6

        with patch(_EC, side_effect=_fake):
            out, used_crypto = adapter._query_calc_withdraw_one_coin_onchain(_stable_pool(), 10**18, 0)
        assert out == 1_000 * 10**6
        assert used_crypto is False  # stable pool answered via the stable selector
        assert captured["gateway_client"] is None  # disconnected gateway dropped...
        assert captured["rpc_url"] == "http://localhost:8545"  # ...rpc_url forwarded


# =============================================================================
# Compiler: coin_index branches to remove_liquidity_one_coin (else proportional)
# =============================================================================


@dataclass
class _StubContext:
    chain: str
    wallet_address: str = WALLET
    rpc_url: str | None = "http://localhost:8545"
    gateway_client: Any = None
    services: Any = None


def _close_intent(coin_index: int | None, max_slippage: Decimal | None = None) -> LPCloseIntent:
    return LPCloseIntent(
        position_id="100",  # decimal LP amount → no on-chain balance query
        pool="3pool",
        protocol="curve",
        chain="ethereum",
        coin_index=coin_index,
        max_slippage=max_slippage,
    )


class TestCompilerSingleSided:
    def test_coin_index_emits_remove_liquidity_one_coin(self) -> None:
        quote = 1_000 * 10**6
        with patch(_EC, return_value=quote):
            result = CurveCompiler().compile_lp_close(_StubContext(chain="ethereum"), _close_intent(coin_index=1))
        assert result.status == CompilationStatus.SUCCESS, result.error
        meta = result.action_bundle.metadata
        assert meta["operation"] == "remove_liquidity_one_coin"
        assert meta["coin_index"] == 1
        remove_tx = next(tx for tx in result.action_bundle.transactions if tx["tx_type"] == "remove_liquidity")
        # StableSwap single-sided selector, and a non-zero min-out word.
        assert remove_tx["data"].startswith(REMOVE_LIQUIDITY_ONE_SELECTOR)
        body = remove_tx["data"][10:]
        min_word = int(body[128:192], 16)  # 3rd arg: _min_amount
        assert min_word == quote * (10000 - 50) // 10000 > 0

    def test_no_coin_index_keeps_proportional(self) -> None:
        """Backward compat: coin_index unset → proportional remove_liquidity.

        The proportional estimator reads balances/totalSupply over its own RPC
        method (not eth_call_uint256), so we stub it directly to keep this test
        focused on the compiler branch rather than the proportional math.
        """
        # Stub the proportional estimate, and also stub eth_call_uint256 (the
        # allowance read inside _build_approve_txs) so this stays a hermetic unit
        # test with no real network call.
        with (
            patch.object(CurveAdapter, "_estimate_remove_liquidity", return_value=[10**18, 10**18, 10**18]),
            patch(_EC, return_value=0),
        ):
            result = CurveCompiler().compile_lp_close(_StubContext(chain="ethereum"), _close_intent(coin_index=None))
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["operation"] == "remove_liquidity"
        assert result.action_bundle.metadata["coin_index"] is None

    def test_compile_fails_closed_when_quote_unavailable(self) -> None:
        """No transport → adapter fails closed → compiler FAILED, never min_out=0."""
        result = CurveCompiler().compile_lp_close(
            _StubContext(chain="ethereum", rpc_url=None), _close_intent(coin_index=1)
        )
        assert result.status == CompilationStatus.FAILED
        assert result.action_bundle is None

    def test_compiler_honors_intent_slippage(self) -> None:
        quote = 1_000 * 10**6
        with patch(_EC, return_value=quote):
            tight = CurveCompiler().compile_lp_close(
                _StubContext(chain="ethereum"), _close_intent(coin_index=1, max_slippage=Decimal("0.001"))
            )
            wide = CurveCompiler().compile_lp_close(
                _StubContext(chain="ethereum"), _close_intent(coin_index=1, max_slippage=Decimal("0.02"))
            )

        def _min_word(res: Any) -> int:
            tx = next(t for t in res.action_bundle.transactions if t["tx_type"] == "remove_liquidity")
            return int(tx["data"][10:][128:192], 16)

        assert _min_word(tight) == quote * (10000 - 10) // 10000
        assert _min_word(wide) == quote * (10000 - 200) // 10000
        assert _min_word(tight) > _min_word(wide)  # tighter slippage → higher floor


# =============================================================================
# Vocabulary: coin_index field, factory threading, validation, round-trip
# =============================================================================


class TestLpCloseCoinIndexField:
    def test_default_is_none(self) -> None:
        assert Intent.lp_close(position_id="123").coin_index is None

    def test_factory_threads_coin_index(self) -> None:
        intent = Intent.lp_close(position_id="100", pool="3pool", protocol="curve", coin_index=2)
        assert intent.coin_index == 2

    @pytest.mark.parametrize("bad", [-1, -5])
    def test_negative_rejected(self, bad: int) -> None:
        with pytest.raises(ValueError, match="coin_index must be a non-negative integer"):
            LPCloseIntent(position_id="100", pool="3pool", protocol="curve", coin_index=bad)

    def test_bool_rejected(self) -> None:
        # bool is an int subclass in Python; True/False are never a valid index.
        # Pydantic's strict int field rejects bool at field validation (before the
        # model_validator guard), so either message is acceptable — what matters is
        # that the construction fails rather than silently coercing True -> 1.
        with pytest.raises(ValueError):  # noqa: PT011 — bool rejected at field or model layer
            LPCloseIntent(position_id="100", pool="3pool", protocol="curve", coin_index=True)

    def test_non_curve_protocol_rejected(self) -> None:
        """coin_index is Curve-only; a non-Curve protocol (default uniswap_v3)
        would silently ignore it — fail fast (CodeRabbit, VIB-5438 consistency)."""
        with pytest.raises(ValueError, match="only supported by the Curve connector"):
            LPCloseIntent(position_id="100", pool="3pool", protocol="uniswap_v3", coin_index=1)

    def test_serialize_round_trip_preserves_coin_index(self) -> None:
        intent = LPCloseIntent(position_id="100", pool="3pool", protocol="curve", coin_index=1)
        restored = LPCloseIntent.deserialize(intent.serialize())
        assert restored.coin_index == 1
