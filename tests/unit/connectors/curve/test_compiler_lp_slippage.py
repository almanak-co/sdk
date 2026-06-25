"""Tests for Curve LP_OPEN/LP_CLOSE honoring ``intent.max_slippage`` (audit P0-7).

Before this change the Curve compiler hardcoded ``slippage_bps = 50`` (0.5%) for
both ``compile_lp_open`` and ``compile_lp_close`` and ignored the intent's
requested ``max_slippage`` — so a strategy author could not tune the slippage
floor on a large LP position, even though the SWAP path already honored
``intent.max_slippage``. The fix adds an optional ``max_slippage`` field to
``LPOpenIntent`` / ``LPCloseIntent`` (same field/units as ``SwapIntent``) and
threads it through the compiler, falling back to the historical 50 bps only when
the field is ``None``.

These tests drive the *real* ``CurveCompiler`` against the *real* ``CurveAdapter``
(offline LP-output estimate from the static ``virtual_price``) and assert on the
slippage-adjusted min-out word(s) encoded directly into the compiled
``add_liquidity`` / ``remove_liquidity`` calldata — the bytes that actually reach
the chain. No mocks of the path under test.

Both pools used here are legacy (non-NG) StableSwap pools, so the calldata layout
is fixed:

* ``add_liquidity(uint256[N] amounts, uint256 min_mint)`` — ``min_mint`` is the
  trailing 32-byte word.
* ``remove_liquidity(uint256 _amount, uint256[N] min_amounts)`` — ``min_amounts``
  are the ``N`` trailing words after the leading ``_amount`` word.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.curve.compiler import (
    _DEFAULT_LP_SLIPPAGE_BPS,
    CurveCompiler,
    _resolve_lp_slippage_bps,
)
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent, LPOpenIntent

WALLET = "0x1234567890123456789012345678901234567890"
_WORD = 64  # 32-byte word as hex chars

# eth_call selectors the adapter's proportional-remove estimator issues.
_TOTAL_SUPPLY_SELECTOR = "18160ddd"
_BALANCES_UINT256_SELECTOR = "4903b0d1"
_BALANCES_INT128_SELECTOR = "065a80d8"
# Canned on-chain reads so LP_CLOSE min_amounts are deterministic & non-zero.
# Per-coin balances are large + distinct so the proportional share is non-trivial.
_FAKE_TOTAL_SUPPLY = 1_000_000 * 10**18
_FAKE_BALANCE = 500_000 * 10**18


class _FakeRpcResponse:
    def __init__(self, result_word: int) -> None:
        self.success = True
        self.error = ""
        # The adapter does json.loads(response.result) then decodes the first word.
        self.result = json.dumps("0x" + hex(result_word)[2:].zfill(64))


class _FakeRpc:
    """Returns canned totalSupply / balances so the proportional estimate is fixed."""

    def Call(self, rpc_request: Any, timeout: float = 10.0) -> _FakeRpcResponse:  # noqa: N802
        params = json.loads(rpc_request.params)
        data = params[0]["data"]
        selector = data[2:10] if data.startswith("0x") else data[:8]
        if selector == _TOTAL_SUPPLY_SELECTOR:
            return _FakeRpcResponse(_FAKE_TOTAL_SUPPLY)
        if selector in (_BALANCES_UINT256_SELECTOR, _BALANCES_INT128_SELECTOR):
            return _FakeRpcResponse(_FAKE_BALANCE)
        raise AssertionError(f"unexpected eth_call selector: {selector}")


@dataclass
class _FakeGatewayClient:
    rpc: _FakeRpc = field(default_factory=_FakeRpc)


@dataclass
class _StubContext:
    """Minimal context satisfying the fields the LP compile paths read.

    Named/decimal-position LP intents on legacy pools never invoke ``services``,
    so a ``None`` placeholder is fine here. LP_CLOSE requires a ``gateway_client``
    for the proportional-remove on-chain estimate (otherwise the adapter fails
    closed with all-zero min_amounts); a canned fake supplies it.
    """

    chain: str
    wallet_address: str = WALLET
    rpc_url: str | None = None
    gateway_client: Any = None
    services: Any = None


def _compile_open(intent: LPOpenIntent, chain: str) -> Any:
    return CurveCompiler().compile_lp_open(_StubContext(chain=chain), intent)


def _compile_close(intent: LPCloseIntent, chain: str) -> Any:
    ctx = _StubContext(chain=chain, gateway_client=_FakeGatewayClient())
    return CurveCompiler().compile_lp_close(ctx, intent)


def _tx_of_type(result: Any, tx_type: str) -> dict[str, Any]:
    assert result.status == CompilationStatus.SUCCESS, result.error
    txs = [tx for tx in result.action_bundle.transactions if tx["tx_type"] == tx_type]
    assert len(txs) == 1, f"expected exactly one {tx_type} tx, got {len(txs)}"
    return txs[0]


def _words(calldata: str) -> list[int]:
    """Split calldata (minus the 4-byte selector) into integer 32-byte words."""
    body = calldata[10:] if calldata.startswith("0x") else calldata[8:]
    return [int(body[i : i + _WORD], 16) for i in range(0, len(body), _WORD)]


def _open_min_mint(result: Any) -> int:
    """Decode the trailing ``min_mint`` word from a legacy add_liquidity tx."""
    return _words(_tx_of_type(result, "add_liquidity")["data"])[-1]


def _close_min_amounts(result: Any, n_coins: int) -> list[int]:
    """Decode the trailing ``min_amounts`` words from a legacy remove_liquidity tx."""
    words = _words(_tx_of_type(result, "remove_liquidity")["data"])
    # Layout: [lp_amount, *min_amounts]
    return words[1 : 1 + n_coins]


def _open_intent(chain_pool: str, max_slippage: Decimal | None) -> LPOpenIntent:
    return LPOpenIntent(
        pool=chain_pool,
        amount0=Decimal("100"),
        amount1=Decimal("100"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="curve",
        max_slippage=max_slippage,
    )


def _close_intent(chain_pool: str, max_slippage: Decimal | None) -> LPCloseIntent:
    return LPCloseIntent(
        position_id="100",  # decimal LP amount → no on-chain balance query needed
        pool=chain_pool,
        protocol="curve",
        max_slippage=max_slippage,
    )


# =============================================================================
# _resolve_lp_slippage_bps: the conversion helper (mirrors SWAP)
# =============================================================================


class TestResolveLpSlippageBps:
    def test_none_falls_back_to_default(self) -> None:
        assert _resolve_lp_slippage_bps(None) == _DEFAULT_LP_SLIPPAGE_BPS == 50

    @pytest.mark.parametrize(
        ("max_slippage", "expected_bps"),
        [
            (Decimal("0.005"), 50),  # 0.5% → same as the default, but now explicit
            (Decimal("0.01"), 100),  # 1%
            (Decimal("0.02"), 200),  # 2%
            (Decimal("0.001"), 10),  # 0.1% — tighter than default
            (Decimal("0"), 0),  # 0% (caller's choice; adapter fails closed on all-zero)
        ],
    )
    def test_explicit_slippage_converted_like_swap(self, max_slippage: Decimal, expected_bps: int) -> None:
        # Mirrors the SWAP path: int(max_slippage * Decimal("10000")).
        assert _resolve_lp_slippage_bps(max_slippage) == expected_bps


# =============================================================================
# LP_OPEN: min_mint honors the requested slippage
# =============================================================================


class TestLpOpenSlippage:
    def test_default_matches_50bps_explicit(self) -> None:
        """No max_slippage reproduces the historical 50 bps min_mint byte-for-byte."""
        default = _open_min_mint(_compile_open(_open_intent("2pool", None), "arbitrum"))
        explicit_50 = _open_min_mint(_compile_open(_open_intent("2pool", Decimal("0.005")), "arbitrum"))
        assert default == explicit_50

    def test_wider_slippage_lowers_min_mint(self) -> None:
        """A 2% tolerance must produce a strictly smaller min_mint floor than 0.5%."""
        default = _open_min_mint(_compile_open(_open_intent("2pool", None), "arbitrum"))
        wide = _open_min_mint(_compile_open(_open_intent("2pool", Decimal("0.02")), "arbitrum"))
        assert wide < default
        # The min-out scales with (10000 - bps): the gross LP estimate is shared,
        # so the wide floor must equal default rescaled from 50 bps to 200 bps
        # (allow ±1 wei for floor-division rounding across the two scalings).
        rescaled = default * (10000 - 200) // (10000 - 50)
        assert abs(wide - rescaled) <= 1

    def test_tighter_slippage_raises_min_mint(self) -> None:
        """A 0.1% tolerance must produce a strictly larger min_mint floor than 0.5%."""
        default = _open_min_mint(_compile_open(_open_intent("2pool", None), "arbitrum"))
        tight = _open_min_mint(_compile_open(_open_intent("2pool", Decimal("0.001")), "arbitrum"))
        assert tight > default

    def test_three_coin_pool_honors_slippage(self) -> None:
        default = _open_min_mint(_compile_open(_open_intent("3pool", None), "polygon"))
        wide = _open_min_mint(_compile_open(_open_intent("3pool", Decimal("0.01")), "polygon"))
        assert wide < default


# =============================================================================
# LP_CLOSE: min_amounts honor the requested slippage
# =============================================================================


class TestLpCloseSlippage:
    def test_default_matches_50bps_explicit(self) -> None:
        default = _close_min_amounts(_compile_close(_close_intent("2pool", None), "arbitrum"), 2)
        explicit_50 = _close_min_amounts(_compile_close(_close_intent("2pool", Decimal("0.005")), "arbitrum"), 2)
        assert default == explicit_50

    def test_wider_slippage_lowers_min_amounts(self) -> None:
        default = _close_min_amounts(_compile_close(_close_intent("2pool", None), "arbitrum"), 2)
        wide = _close_min_amounts(_compile_close(_close_intent("2pool", Decimal("0.02")), "arbitrum"), 2)
        assert all(w < d for w, d in zip(wide, default, strict=True))
        assert any(d > 0 for d in default)  # guard: estimates are non-trivial

    def test_tighter_slippage_raises_min_amounts(self) -> None:
        default = _close_min_amounts(_compile_close(_close_intent("2pool", None), "arbitrum"), 2)
        tight = _close_min_amounts(_compile_close(_close_intent("2pool", Decimal("0.001")), "arbitrum"), 2)
        assert all(t >= d for t, d in zip(tight, default, strict=True))
        assert any(t > d for t, d in zip(tight, default, strict=True))

    def test_three_coin_pool_honors_slippage(self) -> None:
        default = _close_min_amounts(_compile_close(_close_intent("3pool", None), "polygon"), 3)
        wide = _close_min_amounts(_compile_close(_close_intent("3pool", Decimal("0.01")), "polygon"), 3)
        assert all(w < d for w, d in zip(wide, default, strict=True))


# =============================================================================
# Intent vocabulary: optional field, factory threading, validation
# =============================================================================


class TestLpIntentMaxSlippageField:
    def test_lp_open_default_is_none(self) -> None:
        intent = Intent.lp_open(
            pool="0xpool",
            amount0=Decimal("1"),
            amount1=Decimal("2"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
        )
        assert intent.max_slippage is None

    def test_lp_close_default_is_none(self) -> None:
        intent = Intent.lp_close(position_id="123")
        assert intent.max_slippage is None

    def test_lp_open_factory_threads_max_slippage(self) -> None:
        intent = Intent.lp_open(
            pool="2pool",
            coin_amounts=[Decimal("100"), Decimal("100")],
            protocol="curve",
            chain="arbitrum",
            max_slippage=Decimal("0.02"),
        )
        assert intent.max_slippage == Decimal("0.02")

    def test_lp_close_factory_threads_max_slippage(self) -> None:
        intent = Intent.lp_close(
            position_id="100",
            pool="2pool",
            protocol="curve",
            chain="arbitrum",
            max_slippage=Decimal("0.02"),
        )
        assert intent.max_slippage == Decimal("0.02")

    @pytest.mark.parametrize("bad", [Decimal("-0.01"), Decimal("1.5")])
    def test_lp_open_slippage_out_of_range_rejected(self, bad: Decimal) -> None:
        with pytest.raises(ValueError, match="max_slippage must be between 0 and 1"):
            LPOpenIntent(
                pool="2pool",
                amount0=Decimal("1"),
                amount1=Decimal("1"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
                max_slippage=bad,
            )

    @pytest.mark.parametrize("bad", [Decimal("-0.01"), Decimal("1.5")])
    def test_lp_close_slippage_out_of_range_rejected(self, bad: Decimal) -> None:
        with pytest.raises(ValueError, match="max_slippage must be between 0 and 1"):
            LPCloseIntent(
                position_id="100",
                pool="2pool",
                protocol="curve",
                max_slippage=bad,
            )

    def test_lp_open_serialize_round_trip_preserves_max_slippage(self) -> None:
        intent = LPOpenIntent(
            pool="2pool",
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            max_slippage=Decimal("0.02"),
        )
        restored = LPOpenIntent.deserialize(intent.serialize())
        assert restored.max_slippage == Decimal("0.02")

    def test_lp_close_serialize_round_trip_preserves_max_slippage(self) -> None:
        intent = LPCloseIntent(
            position_id="100",
            pool="2pool",
            protocol="curve",
            max_slippage=Decimal("0.02"),
        )
        restored = LPCloseIntent.deserialize(intent.serialize())
        assert restored.max_slippage == Decimal("0.02")
