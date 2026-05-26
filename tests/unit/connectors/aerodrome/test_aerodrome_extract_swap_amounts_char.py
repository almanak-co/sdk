"""Characterization tests for ``AerodromeReceiptParser.extract_swap_amounts``.

Phase 7.3 hardening: pins current behavior of the CC-39 ``extract_swap_amounts``
method before its planned per-phase extraction. Each test here represents a
documented behavior that MUST remain byte-identical after refactor.

Coverage targets (per plan):
- Happy path: V1/V2 stable + volatile pools
- Happy path: Slipstream (CL) pool swap
- Multi-hop swap — "first swap event wins"
- Missing Swap event (no log)
- Multiple Swap events — first-wins semantics
- Zero-value output
- Reverted tx (status = 0)
- amount0 / amount1 sign conventions (CL)
- Token decimal handling edge cases (no metadata, unresolved decimals)
- Router-path vs direct swap (pool-fallback token resolution)
- VIB-3203 ``expected_out`` slippage override
"""

from decimal import Decimal

import pytest

from almanak.connectors.aerodrome.receipt_parser import (
    EVENT_TOPICS,
    AerodromeReceiptParser,
)

# ---------------------------------------------------------------------------
# Helpers to build mock receipts (mirrors existing test harness)
# ---------------------------------------------------------------------------


def _pad32(val: int, signed: bool = False) -> str:
    """Encode an integer as a 32-byte hex word (no 0x prefix)."""
    if signed and val < 0:
        val = val + (1 << 256)
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    """Pad an address to a 32-byte topic."""
    return "0x" + addr.lower().replace("0x", "").zfill(64)


# Known real addresses on Base — token resolver can look up decimals for these.
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"
CBETH_BASE = "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22"

POOL_ADDR = "0x" + "cc" * 20
ROUTER_ADDR = "0x" + "dd" * 20
INTERMEDIATE_POOL = "0x" + "ee" * 20


def _v1_swap_log(
    amount0_in: int,
    amount1_in: int,
    amount0_out: int,
    amount1_out: int,
    sender: str = "0x" + "aa" * 20,
    to: str = "0x" + "bb" * 20,
    pool: str = POOL_ADDR,
    log_index: int = 0,
) -> dict:
    data = "0x" + _pad32(amount0_in) + _pad32(amount1_in) + _pad32(amount0_out) + _pad32(amount1_out)
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["Swap"], _addr_topic(sender), _addr_topic(to)],
        "data": data,
        "logIndex": log_index,
    }


def _cl_swap_log(
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**18,
    tick: int = 0,
    sender: str = "0x" + "aa" * 20,
    recipient: str = "0x" + "bb" * 20,
    pool: str = POOL_ADDR,
    log_index: int = 0,
) -> dict:
    data = (
        "0x"
        + _pad32(amount0, signed=True)
        + _pad32(amount1, signed=True)
        + _pad32(sqrt_price_x96)
        + _pad32(liquidity)
        + _pad32(tick, signed=True)
    )
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["SwapCL"], _addr_topic(sender), _addr_topic(recipient)],
        "data": data,
        "logIndex": log_index,
    }


def _transfer_log(
    token: str,
    frm: str,
    to: str,
    amount: int,
    log_index: int = 0,
) -> dict:
    return {
        "address": token,
        "topics": [EVENT_TOPICS["Transfer"], _addr_topic(frm), _addr_topic(to)],
        "data": "0x" + _pad32(amount),
        "logIndex": log_index,
    }


def _receipt(logs: list[dict], status: int = 1, wallet: str | None = None) -> dict:
    r: dict = {
        "transactionHash": "0x" + "11" * 32,
        "blockNumber": 100,
        "status": status,
        "gasUsed": 150_000,
        "logs": logs,
    }
    if wallet is not None:
        r["from"] = wallet
    return r


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestHappyPaths:
    """Pin the happy-path behaviors: stable, volatile (V1/V2), and CL."""

    def _stable_or_volatile_parser(self) -> AerodromeReceiptParser:
        # Stable vs volatile pools share the V1 Swap ABI — the receipt is the
        # same shape. Parser has no concept of "stable"; it's a pool-factory
        # concern. The test pins the invariant that both cases parse identically.
        return AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )

    def test_happy_path_v1_stable_pool_swap(self):
        """V1 stable pool swap: wallet pays USDC, receives WETH."""
        parser = self._stable_or_volatile_parser()
        wallet = "0x" + "aa" * 20
        receipt = _receipt(
            [
                _transfer_log(USDC_BASE, wallet, POOL_ADDR, 3_000_000, log_index=0),
                _v1_swap_log(3_000_000, 0, 0, 10**15, log_index=1),
                _transfer_log(WETH_BASE, POOL_ADDR, wallet, 10**15, log_index=2),
            ],
            wallet=wallet,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 3_000_000
        assert sa.amount_out == 10**15
        assert sa.amount_in_decimal == Decimal("3")
        assert sa.amount_out_decimal == Decimal("0.001")
        assert sa.effective_price == Decimal("0.001") / Decimal("3")
        assert sa.token_in == "USDC"
        assert sa.token_out == "WETH"

    def test_happy_path_v1_volatile_pool_swap(self):
        """V1 volatile pool swap: wallet pays WETH, receives USDC."""
        parser = self._stable_or_volatile_parser()
        wallet = "0x" + "aa" * 20
        receipt = _receipt(
            [
                _transfer_log(WETH_BASE, wallet, POOL_ADDR, 10**18, log_index=0),
                _v1_swap_log(0, 10**18, 2500_000_000, 0, log_index=1),
                _transfer_log(USDC_BASE, POOL_ADDR, wallet, 2500_000_000, log_index=2),
            ],
            wallet=wallet,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 10**18  # 1 WETH
        assert sa.amount_out == 2500_000_000  # 2500 USDC
        assert sa.amount_in_decimal == Decimal("1")
        assert sa.amount_out_decimal == Decimal("2500")
        assert sa.token_in == "WETH"
        assert sa.token_out == "USDC"

    def test_happy_path_slipstream_cl_swap(self):
        """Slipstream CL pool swap with signed amount conventions."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        wallet = "0x" + "aa" * 20
        # amount0 = +10_000_000 (USDC into pool), amount1 = -4e15 (WETH out of pool)
        receipt = _receipt(
            [
                _transfer_log(USDC_BASE, wallet, POOL_ADDR, 10_000_000, log_index=0),
                _cl_swap_log(amount0=10_000_000, amount1=-(4 * 10**15), log_index=1),
                _transfer_log(WETH_BASE, POOL_ADDR, wallet, 4 * 10**15, log_index=2),
            ],
            wallet=wallet,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 10_000_000
        assert sa.amount_out == 4 * 10**15
        assert sa.token_in == "USDC"
        assert sa.token_out == "WETH"
        assert sa.effective_price > 0


# ---------------------------------------------------------------------------
# Multi-hop + "first swap event wins" semantics
# ---------------------------------------------------------------------------


class TestMultiHopSemantics:
    """Multi-hop swaps emit >1 Swap event. Parser keeps first-wins semantics."""

    def test_multihop_returns_first_swap_event_amounts(self):
        """Two Swap events in a multi-hop: parser pins to the first one."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        wallet = "0x" + "aa" * 20
        # Hop 1: USDC -> WETH (first swap, must win)
        # Hop 2: WETH -> some-other-token on a separate pool
        receipt = _receipt(
            [
                _transfer_log(USDC_BASE, wallet, POOL_ADDR, 5_000_000, log_index=0),
                _v1_swap_log(5_000_000, 0, 0, 10**15, pool=POOL_ADDR, log_index=1),
                _v1_swap_log(
                    0,
                    10**15,
                    999_999_999,
                    0,
                    pool=INTERMEDIATE_POOL,
                    log_index=2,
                ),
                _transfer_log(CBETH_BASE, INTERMEDIATE_POOL, wallet, 999_999_999, log_index=3),
            ],
            wallet=wallet,
        )
        result = parser.parse_receipt(receipt)
        # parse_receipt collects all swap events...
        assert len(result.swap_events) == 2
        # ...but extract_swap_amounts pins amounts to the first swap event.
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 5_000_000  # hop 1 input
        assert sa.amount_out == 10**15  # hop 1 output (NOT the 999_999_999 final output)

    def test_multiple_swap_events_first_wins(self):
        """Explicit pin for first-wins when multiple Swap events share a pool shape."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt(
            [
                _v1_swap_log(1_000_000, 0, 0, 10**14, log_index=0),
                _v1_swap_log(9_000_000, 0, 0, 9 * 10**14, log_index=1),
            ]
        )
        result = parser.parse_receipt(receipt)
        assert result.swap_result is not None
        # swap_result is built from swap_events[0].
        assert result.swap_result.amount_in == 1_000_000
        assert result.swap_result.amount_out == 10**14


# ---------------------------------------------------------------------------
# Missing / edge-case receipts
# ---------------------------------------------------------------------------


class TestMissingAndEdgeCases:
    """Inputs that should produce ``None``."""

    def test_missing_swap_event_returns_none(self):
        parser = AerodromeReceiptParser(chain="base", token0_decimals=6, token1_decimals=18)
        receipt = _receipt([])
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_reverted_tx_returns_none(self):
        parser = AerodromeReceiptParser(chain="base", token0_decimals=6, token1_decimals=18)
        receipt = _receipt(
            [_v1_swap_log(3_000_000, 0, 0, 10**15)],
            status=0,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_zero_value_output_returns_swap_amounts_with_zero(self):
        """Parser does not special-case amount_out == 0; caller must decide.

        This is a characterization pin: zero-output swaps should NOT silently
        be promoted to ``None``. They are their own signal.
        """
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        # amount_in > 0, amount_out == 0 (contrived but legal per the ABI)
        receipt = _receipt([_v1_swap_log(3_000_000, 0, 0, 0)])
        sa = parser.extract_swap_amounts(receipt)
        # With decimals resolved the parser returns a SwapAmounts with zero out.
        assert sa is not None
        assert sa.amount_in == 3_000_000
        assert sa.amount_out == 0
        assert sa.amount_out_decimal == Decimal("0")
        # effective_price = amount_out / amount_in = 0 / 3 = 0 (amount_in > 0).
        assert sa.effective_price == Decimal("0")

    def test_unresolved_decimals_returns_none(self):
        """Parser built without metadata AND tokens aren't resolvable -> None.

        Uses synthetic addresses that token resolver cannot resolve. No
        constructor-supplied decimals means ``_resolve_decimals`` returns None
        for both sides and ``extract_swap_amounts`` must return None.
        """
        parser = AerodromeReceiptParser(chain="base")
        # Synthetic, un-resolvable token addresses via pool-fallback.
        syn_token_in = "0x" + "01" * 20
        syn_token_out = "0x" + "02" * 20
        pool = POOL_ADDR
        wallet = "0x" + "aa" * 20
        receipt = _receipt(
            [
                _transfer_log(syn_token_in, wallet, pool, 3_000_000, log_index=0),
                _v1_swap_log(3_000_000, 0, 0, 10**15, pool=pool, log_index=1),
                _transfer_log(syn_token_out, pool, wallet, 10**15, log_index=2),
            ],
            wallet=wallet,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None


# ---------------------------------------------------------------------------
# Sign conventions (CL)
# ---------------------------------------------------------------------------


class TestSignConventions:
    """CL swap amounts use signed int256 with pool-perspective sign rules."""

    @pytest.mark.parametrize(
        ("amount0", "amount1", "expect_in_is_token0"),
        [
            # token0 positive = token0 into pool => token0 is input
            (3_000_000, -(10**15), True),
            # token1 positive = token1 into pool => token1 is input
            (-(5_000_000), 2 * 10**15, False),
        ],
    )
    def test_cl_swap_sign_conventions(self, amount0: int, amount1: int, expect_in_is_token0: bool):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt([_cl_swap_log(amount0=amount0, amount1=amount1)])
        result = parser.parse_receipt(receipt)
        assert result.swap_result is not None
        if expect_in_is_token0:
            assert result.swap_result.token_in_symbol == "USDC"
            assert result.swap_result.token_out_symbol == "WETH"
        else:
            assert result.swap_result.token_in_symbol == "WETH"
            assert result.swap_result.token_out_symbol == "USDC"

    def test_v1_swap_amount0_in_sign_convention(self):
        """V1 sign convention: amount0_in > 0 means token0 is input."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt([_v1_swap_log(3_000_000, 0, 0, 10**15)])
        result = parser.parse_receipt(receipt)
        assert result.swap_result is not None
        assert result.swap_result.token_in_symbol == "USDC"
        assert result.swap_result.token_out_symbol == "WETH"

    def test_v1_swap_amount1_in_sign_convention(self):
        """V1 sign convention: amount1_in > 0 means token1 is input."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt([_v1_swap_log(0, 10**18, 2500_000_000, 0)])
        result = parser.parse_receipt(receipt)
        assert result.swap_result is not None
        assert result.swap_result.token_in_symbol == "WETH"
        assert result.swap_result.token_out_symbol == "USDC"


# ---------------------------------------------------------------------------
# Router-path vs direct swap
# ---------------------------------------------------------------------------


class TestRouterVsDirectSwap:
    """Router-path swaps have Transfers between router and pool (not wallet)."""

    def test_router_path_swap_uses_pool_fallback(self):
        """Transfers go router<->pool: extract_swap_amounts resolves via pool fallback."""
        parser = AerodromeReceiptParser(chain="base")
        wallet = "0x" + "ff" * 20  # wallet is NOT party to any Transfer
        receipt = _receipt(
            [
                # Transfers are router<->pool (aggregator-router style), NOT wallet<->pool
                _transfer_log(USDC_BASE, ROUTER_ADDR, POOL_ADDR, 10_000_000, log_index=0),
                _v1_swap_log(10_000_000, 0, 0, 4 * 10**15, sender=ROUTER_ADDR, to=ROUTER_ADDR, log_index=1),
                _transfer_log(WETH_BASE, POOL_ADDR, ROUTER_ADDR, 4 * 10**15, log_index=2),
            ],
            wallet=wallet,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 10_000_000
        assert sa.amount_out == 4 * 10**15

    def test_direct_swap_uses_wallet_transfers(self):
        """Direct wallet<->pool swap: extract_swap_amounts identifies via wallet Transfers."""
        parser = AerodromeReceiptParser(chain="base")
        wallet = "0x" + "aa" * 20
        receipt = _receipt(
            [
                _transfer_log(USDC_BASE, wallet, POOL_ADDR, 5_000_000, log_index=0),
                _v1_swap_log(5_000_000, 0, 0, 2 * 10**15, log_index=1),
                _transfer_log(WETH_BASE, POOL_ADDR, wallet, 2 * 10**15, log_index=2),
            ],
            wallet=wallet,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 5_000_000
        assert sa.amount_out == 2 * 10**15


# ---------------------------------------------------------------------------
# VIB-3203 expected_out slippage override
# ---------------------------------------------------------------------------


class TestExpectedOutSlippage:
    """``expected_out`` kwarg supplies pre-slippage quote for realized-slippage."""

    def test_expected_out_computes_positive_slippage(self):
        """Realized out < expected out => positive slippage_bps."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        # Swap nets 0.001 WETH; compiler quoted 0.00101 (1% higher).
        receipt = _receipt([_v1_swap_log(3_000_000, 0, 0, 10**15)])
        sa = parser.extract_swap_amounts(receipt, expected_out=Decimal("0.00101"))
        assert sa is not None
        # realized = (0.00101 - 0.001) / 0.00101 ≈ 0.0099 => 99 bps
        assert sa.slippage_bps is not None
        assert 95 <= sa.slippage_bps <= 100
        assert sa.expected_out_decimal == Decimal("0.00101")

    def test_expected_out_zero_does_not_override(self):
        """expected_out==0 must NOT divide; parser keeps original slippage."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt([_v1_swap_log(3_000_000, 0, 0, 10**15)])
        sa = parser.extract_swap_amounts(receipt, expected_out=Decimal("0"))
        assert sa is not None
        # expected_out=0 is a guard; parser leaves slippage_bps at underlying value.
        # Underlying swap_result has slippage_bps=0 (no quoted_price set).
        # The SwapAmounts branch maps 0 -> None via `sr.slippage_bps if sr.slippage_bps else None`.
        assert sa.slippage_bps is None
        assert sa.expected_out_decimal == Decimal("0")

    def test_expected_out_none_leaves_slippage_alone(self):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt([_v1_swap_log(3_000_000, 0, 0, 10**15)])
        sa = parser.extract_swap_amounts(receipt, expected_out=None)
        assert sa is not None
        assert sa.expected_out_decimal is None


# ---------------------------------------------------------------------------
# Token decimal edge cases
# ---------------------------------------------------------------------------


class TestTokenDecimalHandling:
    """Decimal resolution corner cases."""

    def test_different_decimals_six_vs_eighteen(self):
        """USDC (6) x WETH (18) produces human-scaled amounts."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_BASE,
            token1_address=WETH_BASE,
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt([_v1_swap_log(1_234_567, 0, 0, 555_555_555_555_555_555)])
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in_decimal == Decimal("1.234567")
        assert sa.amount_out_decimal == Decimal("0.555555555555555555")

    def test_same_decimals_eighteen_vs_eighteen(self):
        """Both tokens at 18 decimals."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=WETH_BASE,
            token1_address=CBETH_BASE,
            token0_decimals=18,
            token1_decimals=18,
            token0_symbol="WETH",
            token1_symbol="cbETH",
        )
        receipt = _receipt([_v1_swap_log(10**18, 0, 0, 9 * 10**17)])
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in_decimal == Decimal("1")
        assert sa.amount_out_decimal == Decimal("0.9")
        assert sa.effective_price == Decimal("0.9")
