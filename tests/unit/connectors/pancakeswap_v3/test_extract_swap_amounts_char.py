"""Characterization tests for ``PancakeSwapV3ReceiptParser.extract_swap_amounts``.

Phase 8.4 hardening: pins current behavior of the CC-30 ``extract_swap_amounts``
method BEFORE per-phase extraction. Each test here represents a documented
behavior that MUST remain byte-identical after refactor.

Mirrors the Phase 7.3 Aerodrome characterization suite and covers:

- Happy path — PCS V3 swap (exact-input / exact-output-style receipts)
- Multi-hop — first-Swap-event-wins semantics (via Transfer ordering)
- Missing Swap event (no pool Swap log)
- Multiple Swap events — multi-hop disambiguation via wallet Transfers
- Reverted tx (status = 0)
- amount0 / amount1 sign conventions (PCS V3 Swap has 9 params, signed int256)
- Token decimal handling (resolved via token registry vs unresolved)
- Zero-value output (amount_out == 0 is its own signal — parser returns None)
- Fee-on-transfer tokens — realized out < amount1 (pool-perspective)
- VIB-3203 expected_out slippage override
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    PancakeSwapV3ReceiptParser,
)

# ---------------------------------------------------------------------------
# BSC addresses (real, so the token resolver resolves decimals)
# ---------------------------------------------------------------------------

USDC_BSC = "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"  # 18 decimals on BSC
WBNB_BSC = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"  # 18 decimals
CAKE_BSC = "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"  # 18 decimals
USDT_BSC = "0x55d398326f99059ff775485246999027b3197955"  # 18 decimals

POOL_ADDR = "0x" + "cc" * 20
INTERMEDIATE_POOL = "0x" + "ee" * 20
ROUTER_ADDR = "0x" + "dd" * 20
WALLET = "0x" + "aa" * 20


# ---------------------------------------------------------------------------
# Log-building helpers
# ---------------------------------------------------------------------------


def _pad32(val: int, signed: bool = False) -> str:
    """Encode an integer as a 32-byte hex word (no 0x prefix)."""
    if signed and val < 0:
        val = val + (1 << 256)
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    """Pad an address to a 32-byte topic."""
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _pcs_swap_log(
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**18,
    tick: int = 0,
    protocol_fees0: int = 0,
    protocol_fees1: int = 0,
    sender: str = ROUTER_ADDR,
    recipient: str = WALLET,
    pool: str = POOL_ADDR,
    log_index: int = 1,
) -> dict:
    """Build a PancakeSwap V3 Swap event log.

    NOTE: PCS V3 Swap has 9 params vs UniV3's 7 (two extra uint128 protocol-fees).
    amount0/amount1 are signed int256: positive = into pool, negative = out of pool.
    """
    data = (
        "0x"
        + _pad32(amount0, signed=True)
        + _pad32(amount1, signed=True)
        + _pad32(sqrt_price_x96)
        + _pad32(liquidity)
        + _pad32(tick, signed=True)
        + _pad32(protocol_fees0)
        + _pad32(protocol_fees1)
    )
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["Swap"], _addr_topic(sender), _addr_topic(recipient)],
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


def _receipt(logs: list[dict], status: int = 1, wallet: str | None = WALLET) -> dict:
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
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    """Pin the happy-path behavior: a direct PCS V3 swap through the router."""

    def test_happy_path_pcs_v3_swap(self):
        """Wallet pays USDC (token0), receives WBNB (token1); amounts resolve."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        amount_in_raw = 3 * 10**18  # 3 USDC (BSC USDC is 18 decimals)
        amount_out_raw = 10**16  # 0.01 WBNB
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, amount_in_raw, log_index=0),
                _pcs_swap_log(
                    amount0=amount_in_raw,
                    amount1=-amount_out_raw,
                    log_index=1,
                ),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, amount_out_raw, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == amount_in_raw
        assert sa.amount_out == amount_out_raw
        assert sa.amount_in_decimal == Decimal("3")
        assert sa.amount_out_decimal == Decimal("0.01")
        # Effective price = out/in
        assert sa.effective_price == Decimal("0.01") / Decimal("3")
        # token addresses flow through
        # VIB-4978: ledger token identity is the canonical symbol, not the raw address.
        assert sa.token_in == "USDC"
        assert sa.token_out == "WBNB"


# ---------------------------------------------------------------------------
# Exact-input vs exact-output receipt shapes
# ---------------------------------------------------------------------------


class TestExactInputExactOutput:
    """Both exact-in and exact-out swaps emit the same receipt shape.

    The ``exactInputSingle`` vs ``exactOutputSingle`` distinction is a
    router-call concern; the pool-level Swap event and the Transfer events
    are identical. Parser pins both cases to the same behavior.
    """

    def test_exact_input_swap(self):
        """exactInputSingle-style: quoted in, variable out."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 5 * 10**18, log_index=0),
                _pcs_swap_log(amount0=5 * 10**18, amount1=-(2 * 10**16), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 2 * 10**16, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 5 * 10**18
        assert sa.amount_out == 2 * 10**16

    def test_exact_output_swap(self):
        """exactOutputSingle-style: quoted out, variable in."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        # Target: exactly 0.01 WBNB out; actual in: 3.14 USDC
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3_140_000_000_000_000_000, log_index=0),
                _pcs_swap_log(amount0=3_140_000_000_000_000_000, amount1=-(10**16), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**16, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 3_140_000_000_000_000_000
        assert sa.amount_out == 10**16


# ---------------------------------------------------------------------------
# Multi-hop + "first/last Transfer wins" semantics
# ---------------------------------------------------------------------------


class TestMultiHopSemantics:
    """Multi-hop swaps emit >1 Swap event.

    The extract_swap_amounts path is wallet-transfer based (NOT swap-event
    based), so it sees:
        transfers_from_wallet[0]  -> the token the wallet paid first
        transfers_to_wallet[-1]   -> the final token the wallet received last

    This is the only way to correctly disambiguate multi-hop receipts where
    wallet pays token A, router swaps A->B->C, wallet receives C.
    """

    def test_multihop_uses_first_transfer_out_and_last_transfer_in(self):
        """Multi-hop USDC -> WBNB -> CAKE. Wallet pays USDC, receives CAKE.

        Pin: parser MUST report USDC->CAKE (wallet-level), NOT USDC->WBNB
        (first hop only). This is the wallet-transfer-based behavior.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                # Hop 1: USDC wallet -> POOL_ADDR
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 5 * 10**18, log_index=0),
                _pcs_swap_log(
                    amount0=5 * 10**18,
                    amount1=-(10**15),
                    pool=POOL_ADDR,
                    log_index=1,
                ),
                # Hop 1 intermediate: WBNB pool -> router
                _transfer_log(WBNB_BSC, POOL_ADDR, ROUTER_ADDR, 10**15, log_index=2),
                # Hop 2: WBNB router -> intermediate pool
                _transfer_log(WBNB_BSC, ROUTER_ADDR, INTERMEDIATE_POOL, 10**15, log_index=3),
                _pcs_swap_log(
                    amount0=10**15,
                    amount1=-(9 * 10**18),
                    pool=INTERMEDIATE_POOL,
                    log_index=4,
                ),
                # Hop 2 final: CAKE intermediate pool -> wallet
                _transfer_log(CAKE_BSC, INTERMEDIATE_POOL, WALLET, 9 * 10**18, log_index=5),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        # Pin wallet-level amounts (NOT first-hop amounts)
        assert sa.amount_in == 5 * 10**18  # USDC the wallet paid
        assert sa.amount_out == 9 * 10**18  # CAKE the wallet received
        # And wallet-level tokens (VIB-4978: canonical symbols)
        assert sa.token_in == "USDC"
        assert sa.token_out == "CAKE"

    def test_multiple_swap_events_do_not_affect_wallet_level_amounts(self):
        """Even with N Swap events, wallet-level in/out Transfers drive amounts.

        This is the PCS V3 extract_swap_amounts contract — it does NOT look
        at Swap events at all; it relies on wallet-level Transfer events.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 7 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _pcs_swap_log(amount0=4 * 10**18, amount1=-(2 * 10**15), log_index=2),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 3 * 10**15, log_index=3),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        # Wallet-level: paid 7 USDC, got 3e15 WBNB.
        assert sa.amount_in == 7 * 10**18
        assert sa.amount_out == 3 * 10**15


# ---------------------------------------------------------------------------
# Missing / reverted / zero-output receipts
# ---------------------------------------------------------------------------


class TestMissingAndReverted:
    """Inputs that should produce ``None``."""

    def test_empty_logs_returns_none(self):
        """No logs at all => None."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt([])
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_missing_swap_event_returns_none(self):
        """Transfer-only receipt (no PCS Swap log) => None.

        CodeRabbit PR #1798: guards against misclassifying transfer-only
        receipts (e.g. a plain ERC-20 Transfer, or an LP-add receipt that
        still produces wallet in/out Transfer logs) as swaps. The
        ``_has_pcs_swap_log`` gate short-circuits before any wallet-transfer
        collection. Pins the method contract: "returns None if no swap
        event found".
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        amount_in_raw = 3 * 10**18
        amount_out_raw = 10**16
        receipt = _receipt(
            [
                # Wallet-out + wallet-in Transfers that *look* like a swap,
                # but NO PancakeSwap Swap log is present.
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, amount_in_raw, log_index=0),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, amount_out_raw, log_index=1),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_receipt_with_no_wallet_transfers_returns_none(self):
        """Only a Swap log but no wallet Transfer => None."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt([_pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15))])
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_missing_from_wallet_leg_returns_none(self):
        """PCS Swap log + only a wallet-IN Transfer (no wallet-OUT) => None.

        Fail-closed guarantee (CR #1798): if the wallet paid nothing we can
        identify, the parser MUST refuse to fabricate an input leg. Previously
        the helper silently defaulted ``token_in=""`` / ``amount_in=0`` and
        emitted a bogus SwapAmounts with ``effective_price=0``, which corrupts
        ledger / PnL downstream.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                # Wallet-IN transfer only (no wallet-OUT — cannot identify input leg)
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=0),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=1),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_reverted_tx_returns_none(self):
        """status=0 => None, regardless of logs."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ],
            status=0,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_reverted_tx_hex_status_returns_none(self):
        """status='0x0' (hex) must also be treated as revert."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [_transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=0)],
        )
        receipt["status"] = "0x0"
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_zero_output_returns_none(self):
        """Pin current behavior: amount_out == 0 => None.

        This is the current PCS V3 implementation (receipt_parser.py:376).
        Differs from Aerodrome which returns a zero-amount SwapAmounts.
        We pin it to catch any regression during refactor.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=0, log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 0, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_missing_from_field_returns_none(self):
        """No 'from' on receipt => cannot identify wallet => None."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ],
            wallet=None,
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None


# ---------------------------------------------------------------------------
# Sign conventions (PCS V3 Swap has 9 params, signed amount0/amount1)
# ---------------------------------------------------------------------------


class TestSignConventions:
    """PCS V3 Swap emits signed int256 amount0/amount1 (pool-perspective).

    + into pool, - out of pool. extract_swap_amounts itself does NOT look at
    the Swap-event sign — it relies on wallet Transfers. These tests pin
    the invariant that both sign conventions route to the same wallet-level
    in/out amounts.
    """

    def test_token0_in_convention(self):
        """amount0 > 0, amount1 < 0: token0 into pool, token1 out of pool."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(
                    amount0=3 * 10**18,
                    amount1=-(10**15),
                    log_index=1,
                ),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 3 * 10**18
        assert sa.amount_out == 10**15

    def test_token1_in_convention(self):
        """amount1 > 0, amount0 < 0: token1 into pool, token0 out of pool."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(WBNB_BSC, WALLET, POOL_ADDR, 10**18, log_index=0),
                _pcs_swap_log(
                    amount0=-(2500 * 10**18),
                    amount1=10**18,
                    log_index=1,
                ),
                _transfer_log(USDC_BSC, POOL_ADDR, WALLET, 2500 * 10**18, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 10**18
        assert sa.amount_out == 2500 * 10**18


# ---------------------------------------------------------------------------
# Token decimal handling
# ---------------------------------------------------------------------------


class TestTokenDecimals:
    """Decimal resolution behaviors (via token resolver)."""

    def test_resolved_decimals_produce_correct_human_amounts(self):
        """USDC_BSC (18) + WBNB (18) => amount/1e18."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 1_234_567_890_000_000_000, log_index=0),
                _pcs_swap_log(
                    amount0=1_234_567_890_000_000_000,
                    amount1=-(555_555_555_555_555_555),
                    log_index=1,
                ),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 555_555_555_555_555_555, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in_decimal == Decimal("1.23456789")
        assert sa.amount_out_decimal == Decimal("0.555555555555555555")

    def test_unresolved_output_decimals_returns_none(self):
        """If output token decimals cannot be resolved, parser returns None.

        Pinning the fail-closed behavior: we'd rather return None than emit
        silent zero-decimal garbage into PnL. Uses synthetic address.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        unresolvable_out = "0x" + "01" * 20
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(unresolvable_out, POOL_ADDR, WALLET, 10**15, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is None

    def test_unresolved_input_decimals_returns_unresolved_amounts(self):
        """Unresolved input decimals => SwapAmounts with None for unmeasurable fields.

        The parser fails-closed only on output decimals (returns None and
        skips the row). Unresolved INPUT decimals fall through gracefully
        so the row is still emitted, but ``amount_in_decimal`` and
        ``effective_price`` are ``None`` -- NOT ``Decimal(0)``.

        Per the "Empty != zero" invariant in
        ``docs/internal/blueprints/27-accounting.md``: ``Decimal(0)`` is a measured
        zero and a literal sentinel here would silently reconcile a
        real swap as 0% slippage in the Accountant Test. The raw integer
        ``amount_in`` is preserved so the row carries the wallet-level
        truth, and ``amount_in_decimal_resolved=False`` flags the row
        for downstream consumers.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        unresolvable_in = "0x" + "02" * 20
        receipt = _receipt(
            [
                _transfer_log(unresolvable_in, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        # Raw integer wallet amount is preserved on both sides.
        assert sa.amount_in == 3 * 10**18
        assert sa.amount_out == 10**15
        # Output decimals resolved -> human amount populated.
        assert sa.amount_out_decimal == Decimal("0.001")
        assert sa.amount_out_decimal_resolved is True
        # Input decimals NOT resolved -> human amount + price are None,
        # NEVER Decimal(0). Flag marks the row for the ledger writer.
        assert sa.amount_in_decimal is None
        assert sa.amount_in_decimal_resolved is False
        assert sa.effective_price is None


# ---------------------------------------------------------------------------
# Fee-on-transfer tokens
# ---------------------------------------------------------------------------


class TestFeeOnTransferTokens:
    """FoT tokens burn a fee on transfer — Swap.amount1 != Transfer amount.

    The parser MUST pin to the wallet-received amount (Transfer), NOT the
    pool-level amount1 (Swap event). Otherwise PnL over-reports output.
    """

    def test_fee_on_transfer_uses_actual_wallet_transfer_amount(self):
        """Pool says it sent 10**15 WBNB, but wallet only received 95% of it.

        Pin: parser reports the 95% wallet-transfer amount.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        pool_out_amount = 10**15
        fee = pool_out_amount // 20  # 5%
        wallet_received = pool_out_amount - fee  # 95%
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                # Pool-level amount1 = -pool_out_amount (pool-perspective)
                _pcs_swap_log(amount0=3 * 10**18, amount1=-pool_out_amount, log_index=1),
                # Fee to a burn sink (not wallet)
                _transfer_log(WBNB_BSC, POOL_ADDR, "0x" + "de" * 20, fee, log_index=2),
                # Final wallet-received amount (after FoT fee)
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, wallet_received, log_index=3),
            ]
        )
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        # Must be wallet_received, NOT pool_out_amount
        assert sa.amount_out == wallet_received
        assert sa.amount_out != pool_out_amount


# ---------------------------------------------------------------------------
# VIB-3203 expected_out slippage override
# ---------------------------------------------------------------------------


class TestExpectedOutSlippage:
    """``expected_out`` kwarg supplies pre-slippage-discount quote."""

    def test_expected_out_computes_positive_slippage(self):
        """realized < expected => positive slippage_bps."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        # Realized: 0.001 WBNB out. Expected: 0.00101 (1% higher).
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt, expected_out=Decimal("0.00101"))
        assert sa is not None
        assert sa.slippage_bps is not None
        # (0.00101 - 0.001) / 0.00101 = 0.00990099..., 99 bps after int-truncation
        assert 95 <= sa.slippage_bps <= 100
        assert sa.expected_out_decimal == Decimal("0.00101")

    def test_expected_out_none_leaves_slippage_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt, expected_out=None)
        assert sa is not None
        assert sa.slippage_bps is None
        assert sa.expected_out_decimal is None

    def test_expected_out_zero_does_not_override(self):
        """expected_out == 0 is a guard — no realized slippage is computed."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=0),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=1),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=2),
            ]
        )
        sa = parser.extract_swap_amounts(receipt, expected_out=Decimal("0"))
        assert sa is not None
        assert sa.slippage_bps is None
        # expected_out_decimal still persists the call-site value
        assert sa.expected_out_decimal == Decimal("0")


# ---------------------------------------------------------------------------
# Exception-safety
# ---------------------------------------------------------------------------


class TestExceptionSafety:
    """extract_swap_amounts wraps all its work in try/except and returns None."""

    def test_malformed_transfer_data_does_not_raise(self):
        """A Transfer log with un-decodable data => skip that log, keep going."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt(
            [
                # First transfer has malformed data — current code wraps
                # decode_uint256 in try/except and skips.
                {
                    "address": USDC_BSC,
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        _addr_topic(WALLET),
                        _addr_topic(POOL_ADDR),
                    ],
                    "data": "0xNOTHEX",
                    "logIndex": 0,
                },
                _transfer_log(USDC_BSC, WALLET, POOL_ADDR, 3 * 10**18, log_index=1),
                _pcs_swap_log(amount0=3 * 10**18, amount1=-(10**15), log_index=2),
                _transfer_log(WBNB_BSC, POOL_ADDR, WALLET, 10**15, log_index=3),
            ]
        )
        # Must not raise.
        sa = parser.extract_swap_amounts(receipt)
        assert sa is not None
        assert sa.amount_in == 3 * 10**18

    @pytest.mark.parametrize("bad_receipt", [{}, {"status": 1}, {"logs": []}])
    def test_empty_or_partial_receipt_returns_none(self, bad_receipt: dict):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_swap_amounts(bad_receipt) is None
