"""CryptoSwap/Tricrypto LP-open min-LP protection (VIB-5441 / audit P1-7).

Volatile LP deposits must never ship ``min_lp=0`` (an MEV theft vector). These
tests prove the adapter now (a) derives min-LP from an on-chain ``calc_token_amount``
quote, probing the bool-carrying selector then the deposit-only one, and (b) FAILS
CLOSED — rejecting the deposit — when no real quote can be obtained, rather than
falling back to ``min_lp=0``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.curve.adapter import (
    CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS,
    CURVE_POOLS,
    CurveAdapter,
    CurveConfig,
)

TRICRYPTO2 = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
ARB_TRICRYPTO = CURVE_POOLS["arbitrum"]["tricrypto"]["address"]
WALLET = "0x1234567890123456789012345678901234567890"


def _adapter(*, rpc: bool, chain: str = "ethereum") -> CurveAdapter:
    return CurveAdapter(
        CurveConfig(
            chain=chain,
            wallet_address=WALLET,
            rpc_url="http://localhost:8545" if rpc else None,
            default_slippage_bps=50,
        )
    )


class TestFailClosed:
    def test_no_rpc_no_gateway_rejects_instead_of_min_lp_zero(self) -> None:
        """A volatile deposit with no on-chain access is rejected, not min_lp=0."""
        result = _adapter(rpc=False).add_liquidity(
            pool_address=TRICRYPTO2,
            amounts=[Decimal("0"), Decimal("0"), Decimal("0.001")],  # USDT/WBTC/WETH
        )
        assert result.success is False
        assert "min_lp=0" in (result.error or "")

    def test_quote_unavailable_rejects(self) -> None:
        """If both selectors return no quote, add_liquidity fails closed."""
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=None):
            result = _adapter(rpc=True).add_liquidity(
                pool_address=TRICRYPTO2,
                amounts=[Decimal("0"), Decimal("0"), Decimal("0.001")],
            )
        assert result.success is False
        assert "no quote" in (result.error or "").lower() or "min_lp" in (result.error or "")


class TestOnChainQuote:
    def test_derives_min_lp_from_quote_with_slippage(self) -> None:
        """min_lp = on-chain quote × (1 − slippage); never 0."""
        quote = 1_084_907_444_686_091
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=quote):
            result = _adapter(rpc=True).add_liquidity(
                pool_address=TRICRYPTO2,
                amounts=[Decimal("0"), Decimal("0"), Decimal("0.001")],
                slippage_bps=50,
            )
        assert result.success is True
        assert result.lp_amount == quote * (10000 - 50) // 10000
        assert result.lp_amount > 0

    def test_probes_bool_selector_first_then_no_bool(self) -> None:
        """Bool selector tried first; falls through to the deposit-only selector."""
        sel_bool, sel_nobool = CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS[3]
        seen: list[str] = []

        def fake_call(**kwargs):
            data = kwargs["data"]
            seen.append(data[:10])
            # bool variant returns nothing (wrong for this pool); no-bool answers.
            return 999 if data.startswith(sel_nobool) else None

        adapter = _adapter(rpc=True)
        pool_info = adapter.get_pool_info(TRICRYPTO2)
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", side_effect=fake_call):
            got = adapter._query_calc_token_amount_crypto_onchain(pool_info, [0, 0, 10**15])
        assert got == 999
        assert seen[0].startswith(sel_bool)  # bool selector probed first
        assert any(s.startswith(sel_nobool) for s in seen)

    def test_unsupported_coin_count_raises(self) -> None:
        adapter = _adapter(rpc=True)
        pool_info = adapter.get_pool_info(TRICRYPTO2)
        object.__setattr__(pool_info, "n_coins", 5)  # no selector for 5-coin
        with pytest.raises(ValueError, match="No CryptoSwap calc_token_amount selector"):
            adapter._query_calc_token_amount_crypto_onchain(pool_info, [0, 0, 0, 0, 0])


class TestSlippageRoundsToZero:
    """A positive on-chain quote that rounds to <=0 after integer slippage math
    must fail closed, not re-introduce the unprotected ``min_lp=0`` path."""

    def test_tiny_quote_rounds_to_zero_fails_closed(self) -> None:
        # quote=1, 50bps slippage → 1 * 9950 // 10000 == 0 → must reject.
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=1):
            result = _adapter(rpc=True).add_liquidity(
                pool_address=TRICRYPTO2,
                amounts=[Decimal("0"), Decimal("0"), Decimal("0.001")],
                slippage_bps=50,
            )
        assert result.success is False
        assert "min_lp=0" in (result.error or "")

    def test_wide_slippage_rounds_to_zero_fails_closed(self) -> None:
        # quote=50, slippage 9999bps → 50 * 1 // 10000 == 0 → must reject.
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=50):
            result = _adapter(rpc=True).add_liquidity(
                pool_address=TRICRYPTO2,
                amounts=[Decimal("0"), Decimal("0"), Decimal("0.001")],
                slippage_bps=9999,
            )
        assert result.success is False
        assert "min_lp=0" in (result.error or "")


class TestDisconnectedGateway:
    """A present-but-disconnected GatewayClient must fail fast (clear min_lp message)
    or drop to rpc_url — never reach eth_call and surface a low-level RPC error."""

    def _gw_adapter(self, *, rpc: bool) -> tuple[CurveAdapter, MagicMock]:
        gw = MagicMock()
        gw.is_connected = False
        adapter = CurveAdapter(
            CurveConfig(
                chain="ethereum",
                wallet_address=WALLET,
                gateway_client=gw,
                rpc_url="http://localhost:8545" if rpc else None,
                default_slippage_bps=50,
            )
        )
        return adapter, gw

    def test_disconnected_no_rpc_fails_closed(self) -> None:
        adapter, _ = self._gw_adapter(rpc=False)
        pool_info = adapter.get_pool_info(TRICRYPTO2)
        with pytest.raises(ValueError, match="disconnected"):
            adapter._query_calc_token_amount_crypto_onchain(pool_info, [0, 0, 10**15])

    def test_disconnected_with_rpc_drops_gateway(self) -> None:
        adapter, _ = self._gw_adapter(rpc=True)
        pool_info = adapter.get_pool_info(TRICRYPTO2)
        captured: dict = {}

        def fake_call(**kwargs):
            captured["gateway_client"] = kwargs.get("gateway_client")
            captured["rpc_url"] = kwargs.get("rpc_url")
            return 999

        with patch("almanak.connectors.curve.adapter.eth_call_uint256", side_effect=fake_call):
            got = adapter._query_calc_token_amount_crypto_onchain(pool_info, [0, 0, 10**15])
        assert got == 999
        assert captured["gateway_client"] is None  # disconnected gateway dropped...
        assert captured["rpc_url"] == "http://localhost:8545"  # ...and rpc_url IS forwarded (real fallback)


class TestArbitrumTricrypto:
    """Arbitrum tricrypto LP_OPEN exercises the SAME 3-coin bool-deposit selector
    path as Ethereum tricrypto2. The ethereum fork test covers compile→execute→
    receipt→delta end-to-end; these assert the arbitrum pool resolves and derives a
    protected min_lp via the identical selector path, deterministically (no fork)."""

    def test_arb_tricrypto_resolves_3coin_bool_selector_first(self) -> None:
        adapter = _adapter(rpc=True, chain="arbitrum")
        pool_info = adapter.get_pool_info(ARB_TRICRYPTO)
        assert pool_info.n_coins == 3
        sel_bool, _ = CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS[3]
        seen: list[str] = []

        def fake_call(**kwargs):
            seen.append(kwargs["data"][:10])
            return 777  # the first (bool-deposit) selector answers

        with patch("almanak.connectors.curve.adapter.eth_call_uint256", side_effect=fake_call):
            got = adapter._query_calc_token_amount_crypto_onchain(pool_info, [0, 0, 10**15])
        assert got == 777
        assert seen[0].startswith(sel_bool)  # arb tricrypto probes the bool selector first

    def test_arb_tricrypto_fails_closed_without_onchain(self) -> None:
        result = _adapter(rpc=False, chain="arbitrum").add_liquidity(
            pool_address=ARB_TRICRYPTO,
            amounts=[Decimal("0"), Decimal("0"), Decimal("0.001")],
        )
        assert result.success is False
        assert "min_lp=0" in (result.error or "")
