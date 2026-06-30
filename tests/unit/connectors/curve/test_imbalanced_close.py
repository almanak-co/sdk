"""Tests for Curve imbalanced LP_CLOSE (VIB-5438, audit P0-4).

Before this change ``LPCloseIntent`` had no way to express an imbalanced exit:
the Curve compiler only ever emitted the proportional ``remove_liquidity`` or
(VIB-5437) the single-sided ``remove_liquidity_one_coin``, so the pool's
``remove_liquidity_imbalance(uint256[N] amounts, uint256 max_burn_amount)`` —
which lets you name the EXACT per-coin amounts OUT — was unreachable. This adds
an optional ``imbalanced_amounts`` vector to the intent vocabulary, wires the
compiler to emit ``remove_liquidity_imbalance`` when it is set, and sizes the
``max_burn_amount`` ceiling FAIL-CLOSED from the pool's on-chain
``calc_token_amount(amounts, is_deposit=False)`` LP-burn quote.

INVERTED SAFETY SEMANTICS vs single-sided: the floor here is a MAX-BURN CEILING
(the most LP we will spend), padded UP by slippage — NOT a min-out padded down.
An unbounded ``max_burn`` (e.g. MAX_UINT256) would let the pool burn the entire
LP balance for a tiny withdrawal (a theft/sandwich vector), so it is NEVER
emitted; an unavailable quote fails the compile loudly.

Selectors verified on real mainnet 2026-06-29 against 3pool (0xbEbc44…):
``calc_token_amount(uint256[3],bool)`` = 0x3883e119 (is_deposit=False → LP burned),
``remove_liquidity_imbalance(uint256[3],uint256)`` = 0x9fdaea0c (a too-tight
max_burn reverts "Slippage screwed you"; an adequate one succeeds).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from almanak.connectors.curve.adapter import (
    BALANCES_INT128_SELECTOR,
    BALANCES_UINT256_SELECTOR,
    NG_CALC_TOKEN_AMOUNT_SELECTOR,
    REMOVE_LIQUIDITY_IMBALANCE_DYN_SELECTOR,
    REMOVE_LIQUIDITY_IMBALANCE_SELECTORS,
    STABLE_CALC_TOKEN_AMOUNT_SELECTORS,
    CurveAdapter,
    CurveConfig,
    PoolInfo,
    PoolType,
)
from almanak.connectors.curve.compiler import CurveCompiler
from almanak.connectors.curve.receipt_parser import EVENT_TOPICS, CurveReceiptParser
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent

WALLET = "0x1234567890123456789012345678901234567890"
_EC = "almanak.connectors.curve.adapter.eth_call_uint256"

# 3pool reserves are huge; use a balance well above any test withdrawal so the
# pool-balance bound never trips unless a test intends it to.
_BIG_BALANCE = 10**30


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
        coin_decimals=[18, 6, 6],
    )


def _stable_ng_pool() -> PoolInfo:
    return PoolInfo(
        address="0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E",
        lp_token="0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E",
        coins=["crvUSD", "USDC"],
        coin_addresses=["0xf939", "0xa0b8"],
        pool_type=PoolType.STABLESWAP,
        n_coins=2,
        name="crvusd_usdc_ng",
        coin_decimals=[18, 6],
        is_ng=True,
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
        coin_decimals=[6, 8, 18],
    )


def _calc_selectors() -> set[str]:
    return set(STABLE_CALC_TOKEN_AMOUNT_SELECTORS.values()) | {NG_CALC_TOKEN_AMOUNT_SELECTOR}


def _balance_selectors() -> set[str]:
    return {BALANCES_UINT256_SELECTOR, BALANCES_INT128_SELECTOR}


def _fake_reads(lp_burn_quote: int, *, balance: int = _BIG_BALANCE):
    """eth_call_uint256 side_effect: answer balances() with ``balance`` and the
    calc_token_amount LP-burn quote with ``lp_burn_quote``. Any other (registry
    refresh) read returns a benign positive default."""

    def _fake(*, data: str, **_: Any) -> int:
        sel = data[:10]
        if sel in _balance_selectors():
            return balance
        if sel in _calc_selectors():
            return lp_burn_quote
        return 1  # unrelated refresh read

    return _fake


# =============================================================================
# Adapter: on-chain calc_token_amount max-burn ceiling + selector dispatch
# =============================================================================


class TestImbalancedMaxBurn:
    def test_max_burn_derived_from_onchain_quote_padded_up(self) -> None:
        adapter = _rpc_adapter()
        quote = 96 * 10**18  # LP that the requested withdrawal would burn
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake_reads(quote)):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                    slippage_bps=50,
                )
        assert result.success is True
        # 50 bps applied as a CEILING (padded UP, strictly above the quote).
        expected = quote * (10000 + 50) // 10000
        assert result.lp_amount == expected
        assert result.lp_amount > quote  # ceiling, not a min-out
        # The amounts vector is exact per-coin (100 DAI in 18 decimals, others 0).
        assert result.amounts == [100 * 10**18, 0, 0]
        assert result.operation == "remove_liquidity_imbalance"

    def test_explicit_zero_slippage_yields_exact_quote(self) -> None:
        """slippage_bps=0 → max_burn == the on-chain LP-burn quote (no padding)."""
        adapter = _rpc_adapter()
        quote = 96 * 10**18
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake_reads(quote)):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                    slippage_bps=0,
                )
        assert result.success is True
        assert result.lp_amount == quote  # exact, not padded

    def test_fixed_array_selector_and_calldata_layout(self) -> None:
        """Legacy StableSwap → fixed-array remove_liquidity_imbalance(uint256[3],uint256)
        with amounts inline then max_burn last."""
        adapter = _rpc_adapter()
        quote = 96 * 10**18
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake_reads(quote)):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                    slippage_bps=50,
                )
        assert result.success is True
        tx = next(t for t in result.transactions if t.tx_type == "remove_liquidity_imbalance")
        assert tx.data.startswith(REMOVE_LIQUIDITY_IMBALANCE_SELECTORS[3])
        body = tx.data[10:]
        words = [body[i : i + 64] for i in range(0, len(body), 64)]
        assert len(words) == 4  # 3 amounts + max_burn
        assert int(words[0], 16) == 100 * 10**18
        assert int(words[1], 16) == 0
        assert int(words[2], 16) == 0
        assert int(words[3], 16) == result.lp_amount  # max_burn last

    def test_ng_pool_uses_dynamic_array_selectors(self) -> None:
        """StableSwap-NG → dynamic-array calc + remove selectors, offset/length head."""
        adapter = _rpc_adapter()
        quote = 50 * 10**18
        seen: list[str] = []

        def _capture(*, data: str, **_: Any) -> int:
            seen.append(data[:10])
            if data[:10] in _balance_selectors():
                return _BIG_BALANCE
            if data[:10] in _calc_selectors():
                return quote
            return 1

        with patch.object(adapter, "get_pool_info", return_value=_stable_ng_pool()):
            with patch(_EC, side_effect=_capture):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_ng_pool().address,
                    amounts=[Decimal("50"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                    slippage_bps=50,
                )
        assert result.success is True
        # NG calc uses the dynamic-array bool selector.
        assert NG_CALC_TOKEN_AMOUNT_SELECTOR in seen
        tx = next(t for t in result.transactions if t.tx_type == "remove_liquidity_imbalance")
        assert tx.data.startswith(REMOVE_LIQUIDITY_IMBALANCE_DYN_SELECTOR)
        body = tx.data[10:]
        words = [body[i : i + 64] for i in range(0, len(body), 64)]
        # head: offset(0x40), max_burn ; tail: length(2), amount0, amount1
        assert int(words[0], 16) == 0x40
        assert int(words[1], 16) == result.lp_amount
        assert int(words[2], 16) == 2  # array length
        assert int(words[3], 16) == 50 * 10**18
        assert int(words[4], 16) == 0


class TestImbalancedFailClosed:
    def test_never_emits_unbounded_max_burn(self) -> None:
        """The emitted max_burn is always a bounded, sane multiple of the quote —
        NEVER MAX_UINT256 / unbounded (the core theft-vector invariant)."""
        adapter = _rpc_adapter()
        quote = 96 * 10**18
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake_reads(quote)):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                    slippage_bps=50,
                )
        assert result.success is True
        tx = next(t for t in result.transactions if t.tx_type == "remove_liquidity_imbalance")
        max_burn = int(tx.data[10:][-64:], 16)
        assert max_burn == result.lp_amount
        assert max_burn < 2**256 - 1  # never the unbounded cap
        # bounded just above the quote by exactly the slippage pad
        assert quote < max_burn <= quote * 2

    def test_no_transport_fails_closed(self) -> None:
        """No gateway and no rpc_url → refuse (never an unbounded max_burn)."""
        adapter = CurveAdapter(CurveConfig(chain="ethereum", wallet_address=WALLET))
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            result = adapter.remove_liquidity_imbalance(
                pool_address=_stable_pool().address,
                amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                lp_amount=Decimal("1000"),
            )
        assert result.success is False
        assert "max_burn" in (result.error or "")

    def test_calc_quote_unavailable_fails_closed(self) -> None:
        """calc_token_amount returns None → refuse (no max_burn ceiling)."""
        adapter = _rpc_adapter()

        def _fake(*, data: str, **_: Any) -> int | None:
            if data[:10] in _balance_selectors():
                return _BIG_BALANCE
            if data[:10] in _calc_selectors():
                return None  # quote unavailable
            return 1

        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                )
        assert result.success is False
        assert "no quote" in (result.error or "").lower() or "max_burn" in (result.error or "")

    def test_cryptoswap_pool_rejected(self) -> None:
        """remove_liquidity_imbalance is StableSwap-only — a CryptoSwap pool fails loud."""
        adapter = _rpc_adapter()
        with patch.object(adapter, "get_pool_info", return_value=_crypto_pool()):
            result = adapter.remove_liquidity_imbalance(
                pool_address=_crypto_pool().address,
                amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                lp_amount=Decimal("1"),
            )
        assert result.success is False
        assert "not supported" in (result.error or "").lower()

    def test_amounts_length_mismatch_rejected(self) -> None:
        adapter = _rpc_adapter()
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            result = adapter.remove_liquidity_imbalance(
                pool_address=_stable_pool().address,
                amounts=[Decimal("100"), Decimal("0")],  # 2 != 3 coins
                lp_amount=Decimal("1000"),
            )
        assert result.success is False
        assert "length" in (result.error or "").lower()

    def test_request_exceeds_pool_balance_rejected(self) -> None:
        """A requested amount above the pool's on-chain reserve fails closed."""
        adapter = _rpc_adapter()
        quote = 96 * 10**18
        # pool holds only 10 DAI worth (tiny balance) but we ask for 100.
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake_reads(quote, balance=10 * 10**18)):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),
                )
        assert result.success is False
        assert "exceeds pool balance" in (result.error or "")

    @pytest.mark.parametrize("bad_bps", [-1, 10_001, 50_000])
    def test_out_of_range_slippage_rejected(self, bad_bps: int) -> None:
        """CodeRabbit: a direct caller passing slippage_bps outside [0,10000] is
        refused before any max_burn is formed (defense-in-depth at the adapter)."""
        adapter = _rpc_adapter()
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            result = adapter.remove_liquidity_imbalance(
                pool_address=_stable_pool().address,
                amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                lp_amount=Decimal("1000"),
                slippage_bps=bad_bps,
            )
        assert result.success is False
        assert "slippage_bps must be in [0, 10000]" in (result.error or "")

    def test_request_needs_more_lp_than_held_rejected(self) -> None:
        """If the LP-burn quote exceeds the position's LP, fail loud (under-funded)."""
        adapter = _rpc_adapter()
        quote = 2000 * 10**18  # needs 2000 LP
        with patch.object(adapter, "get_pool_info", return_value=_stable_pool()):
            with patch(_EC, side_effect=_fake_reads(quote)):
                result = adapter.remove_liquidity_imbalance(
                    pool_address=_stable_pool().address,
                    amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
                    lp_amount=Decimal("1000"),  # only holds 1000 LP
                )
        assert result.success is False
        assert "holds only" in (result.error or "")


# =============================================================================
# Vocabulary: imbalanced_amounts validation
# =============================================================================


class TestImbalancedVocabulary:
    def test_mutually_exclusive_with_coin_index(self) -> None:
        with pytest.raises(ValueError, match="mutually exclusive"):
            LPCloseIntent(
                position_id="x",
                protocol="curve",
                coin_index=0,
                imbalanced_amounts=[Decimal("1"), Decimal("0")],
            )

    def test_rejects_all_zero(self) -> None:
        with pytest.raises(ValueError, match="at least one positive"):
            LPCloseIntent(position_id="x", protocol="curve", imbalanced_amounts=[Decimal("0"), Decimal("0")])

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            LPCloseIntent(position_id="x", protocol="curve", imbalanced_amounts=[Decimal("1"), Decimal("-1")])

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            LPCloseIntent(position_id="x", protocol="curve", imbalanced_amounts=[])

    def test_factory_threads_field(self) -> None:
        intent = Intent.lp_close(
            position_id="x",
            protocol="curve",
            imbalanced_amounts=[Decimal("1"), Decimal("0"), Decimal("0")],
        )
        assert intent.imbalanced_amounts == [Decimal("1"), Decimal("0"), Decimal("0")]

    def test_default_none_is_backward_compatible(self) -> None:
        intent = Intent.lp_close(position_id="x", protocol="curve")
        assert intent.imbalanced_amounts is None
        assert intent.coin_index is None

    def test_rejects_non_curve_protocol(self) -> None:
        """CodeRabbit: imbalanced_amounts is Curve-only; a non-Curve protocol
        (e.g. the default uniswap_v3) would silently ignore it — fail fast."""
        with pytest.raises(ValueError, match="only supported by the Curve connector"):
            LPCloseIntent(
                position_id="x",
                protocol="uniswap_v3",
                imbalanced_amounts=[Decimal("1"), Decimal("0")],
            )

    def test_mutually_exclusive_with_amount_close_all(self) -> None:
        """audit P3: imbalanced (exact amounts) + amount='all' (close-all) is contradictory."""
        with pytest.raises(ValueError, match="amount"):
            LPCloseIntent(
                position_id="x",
                protocol="curve",
                amount="all",
                imbalanced_amounts=[Decimal("1"), Decimal("0")],
            )

    @pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
    def test_rejects_non_finite(self, bad: Decimal) -> None:
        """gemini: SafeDecimal field rejects NaN/Infinity at construction."""
        with pytest.raises(ValueError):
            LPCloseIntent(position_id="x", protocol="curve", imbalanced_amounts=[Decimal("1"), bad])

    def test_rejects_float_with_clear_message(self) -> None:
        """SafeDecimal rejects float entries (precision-loss guard), like coin_amounts."""
        with pytest.raises(ValueError):
            LPCloseIntent(position_id="x", protocol="curve", imbalanced_amounts=[1.5])  # type: ignore[list-item]


# =============================================================================
# Compiler: dispatch to remove_liquidity_imbalance (else single-sided / proportional)
# =============================================================================


@dataclass
class _StubContext:
    chain: str
    wallet_address: str = WALLET
    rpc_url: str | None = "http://localhost:8545"
    gateway_client: Any = None
    services: Any = None


def _close_intent(
    imbalanced_amounts: list[Decimal] | None = None,
    coin_index: int | None = None,
    max_slippage: Decimal | None = None,
) -> LPCloseIntent:
    return LPCloseIntent(
        position_id="1000",  # decimal LP amount → no on-chain balance query
        pool="3pool",
        protocol="curve",
        chain="ethereum",
        coin_index=coin_index,
        imbalanced_amounts=imbalanced_amounts,
        max_slippage=max_slippage,
    )


class TestImbalancedCompiler:
    def test_imbalanced_amounts_emits_remove_liquidity_imbalance(self) -> None:
        quote = 96 * 10**18
        with (
            patch.object(CurveAdapter, "get_pool_info", return_value=_stable_pool()),
            patch(_EC, side_effect=_fake_reads(quote)),
        ):
            result = CurveCompiler().compile_lp_close(
                _StubContext(chain="ethereum"),
                _close_intent(imbalanced_amounts=[Decimal("100"), Decimal("0"), Decimal("0")]),
            )
        assert result.status == CompilationStatus.SUCCESS, result.error
        meta = result.action_bundle.metadata
        assert meta["operation"] == "remove_liquidity_imbalance"
        assert meta["imbalanced_amounts"] == ["100", "0", "0"]
        assert meta["coin_index"] is None
        tx = next(t for t in result.action_bundle.transactions if t["tx_type"] == "remove_liquidity_imbalance")
        assert tx["data"].startswith(REMOVE_LIQUIDITY_IMBALANCE_SELECTORS[3])
        # max_burn (last word) is bounded above the quote, never unbounded.
        max_burn = int(tx["data"][10:][-64:], 16)
        assert quote < max_burn < 2**256 - 1

    @pytest.mark.parametrize("bad_pos", ["0", "-1", "NaN", "Infinity"])
    def test_non_positive_or_non_finite_lp_amount_string_rejected(self, bad_pos: str) -> None:
        """CodeRabbit: a decimal-string position_id that parses to 0/negative/NaN/Inf
        must FAIL the compile clearly, not reach wei conversion / calldata padding."""
        intent = LPCloseIntent(
            position_id=bad_pos,
            pool="3pool",
            protocol="curve",
            chain="ethereum",
            imbalanced_amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
        )
        with patch.object(CurveAdapter, "get_pool_info", return_value=_stable_pool()):
            result = CurveCompiler().compile_lp_close(_StubContext(chain="ethereum"), intent)
        assert result.status == CompilationStatus.FAILED
        assert "positive finite decimal" in (result.error or "")

    def test_no_imbalanced_amounts_keeps_proportional(self) -> None:
        """Backward compat: neither imbalanced_amounts nor coin_index set →
        proportional remove_liquidity (unchanged)."""
        with (
            patch.object(CurveAdapter, "_estimate_remove_liquidity", return_value=[10**18, 10**18, 10**18]),
            patch(_EC, return_value=0),
        ):
            result = CurveCompiler().compile_lp_close(_StubContext(chain="ethereum"), _close_intent())
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["operation"] == "remove_liquidity"
        assert result.action_bundle.metadata["imbalanced_amounts"] is None

    def test_compile_fails_closed_when_quote_unavailable(self) -> None:
        """No transport → adapter fails closed → compiler FAILED, never unbounded max_burn."""
        with patch.object(CurveAdapter, "get_pool_info", return_value=_stable_pool()):
            result = CurveCompiler().compile_lp_close(
                _StubContext(chain="ethereum", rpc_url=None),
                _close_intent(imbalanced_amounts=[Decimal("100"), Decimal("0"), Decimal("0")]),
            )
        assert result.status == CompilationStatus.FAILED
        assert result.action_bundle is None

    def test_compiler_honors_intent_slippage(self) -> None:
        quote = 96 * 10**18
        with (
            patch.object(CurveAdapter, "get_pool_info", return_value=_stable_pool()),
            patch(_EC, side_effect=_fake_reads(quote)),
        ):
            tight = CurveCompiler().compile_lp_close(
                _StubContext(chain="ethereum"),
                _close_intent(
                    imbalanced_amounts=[Decimal("100"), Decimal("0"), Decimal("0")], max_slippage=Decimal("0.001")
                ),
            )
            wide = CurveCompiler().compile_lp_close(
                _StubContext(chain="ethereum"),
                _close_intent(
                    imbalanced_amounts=[Decimal("100"), Decimal("0"), Decimal("0")], max_slippage=Decimal("0.02")
                ),
            )
        assert tight.status == CompilationStatus.SUCCESS, tight.error
        assert wide.status == CompilationStatus.SUCCESS, wide.error

        def _max_burn(res: Any) -> int:
            tx = next(t for t in res.action_bundle.transactions if t["tx_type"] == "remove_liquidity_imbalance")
            return int(tx["data"][10:][-64:], 16)

        # A wider slippage tolerance → a higher max-burn ceiling (more LP allowed).
        assert _max_burn(wide) > _max_burn(tight) > quote


# =============================================================================
# Parser: the existing #3093 RemoveLiquidityImbalance decode books a proceeds
# leg for EVERY non-zero coin of a COMPILER-EMITTED imbalance close (no ghost)
# =============================================================================


def _pad(v: int) -> str:
    return format(v, "064x")


def _imbalance_receipt(pool: str, amounts: list[int]) -> dict:
    """Synthetic RemoveLiquidityImbalance{,3,4} receipt matching the shape a
    compiler-emitted remove_liquidity_imbalance produces on-chain: fixed-array
    ``amounts[N], fees[N], invariant, token_supply`` (fees all zero here)."""
    n = len(amounts)
    name = {2: "RemoveLiquidityImbalance", 3: "RemoveLiquidityImbalance3", 4: "RemoveLiquidityImbalance4"}[n]
    topic = EVENT_TOPICS[name]
    provider_topic = "0x" + "00" * 12 + WALLET[2:]
    words = list(amounts) + [0] * n + [10**20, 10**21]  # fees, invariant, supply
    data = "0x" + "".join(_pad(w) for w in words)
    return {
        "status": 1,
        "from": WALLET,
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 19_000_005,
        "gasUsed": 200_000,
        "logs": [{"address": pool, "topics": [topic, provider_topic], "data": data, "logIndex": 0}],
    }


class TestImbalancedParserNoGhost:
    """The existing VIB-5433 decode must capture ALL proceeds of a compiler-shaped
    imbalanced close — no zero-proceeds ghost that would understate realized PnL."""

    def test_single_nonzero_coin_books_proceeds_not_ghost(self) -> None:
        # Mirrors the compiler-emitted [100 DAI, 0, 0] vector on 3pool.
        amounts = [100 * 10**18, 0, 0]
        receipt = _imbalance_receipt(_stable_pool().address, amounts)
        result = CurveReceiptParser(chain="ethereum").extract_lp_close_data(receipt)
        assert result is not None  # NOT a ghost (would be None)
        assert result.amount0_collected == amounts[0] > 0
        assert result.amount1_collected == 0  # measured zero, not None
        assert result.additional_amounts == {2: 0}
        # Every non-zero coin is booked; total proceeds > 0.
        assert (result.amount0_collected or 0) + (result.amount1_collected or 0) > 0

    def test_multi_nonzero_coins_each_booked(self) -> None:
        # A genuinely imbalanced vector: every non-zero coin must land on a leg.
        amounts = [100 * 10**18, 40 * 10**6, 25 * 10**6]  # DAI, USDC, USDT
        receipt = _imbalance_receipt(_stable_pool().address, amounts)
        result = CurveReceiptParser(chain="ethereum").extract_lp_close_data(receipt)
        assert result is not None
        assert result.amount0_collected == amounts[0]
        assert result.amount1_collected == amounts[1]
        assert result.additional_amounts == {2: amounts[2]}
        booked = [result.amount0_collected, result.amount1_collected, result.additional_amounts[2]]
        assert booked == amounts  # no leg dropped

    def test_2coin_imbalance_books_both(self) -> None:
        amounts = [12 * 10**18, 8 * 10**6]
        receipt = _imbalance_receipt(_stable_ng_pool().address, amounts)
        result = CurveReceiptParser(chain="ethereum").extract_lp_close_data(receipt)
        assert result is not None
        assert result.amount0_collected == amounts[0]
        assert result.amount1_collected == amounts[1]
