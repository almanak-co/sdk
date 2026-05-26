"""Tests for ``UniswapV3ReceiptParser.extract_lp_close_data`` fee split.

Closes the Accountant Test LP3 gap. A Uniswap V3 LP close in a single TX
emits both ``Burn`` (carries principal amounts removed from liquidity) and
``Collect`` (carries principal + accrued fees being transferred to the
recipient). The accrued fees are the difference between the two — without
this split the position_events row's ``fees_token0`` / ``fees_token1``
columns stay zero and the cell can never flip GREEN.

The parser previously returned ``fees0=0, fees1=0`` unconditionally with
the comment "Uniswap V3 doesn't separate fees in events". That comment
was wrong: the protocol DOES separate them — you just need both Burn
*and* Collect from the same TX.

lp-close-may20.md follow-up: when the V3 compiler emits LP_CLOSE as three
separate transactions (decreaseLiquidity → collect → burn-NFT), each
receipt carries only ONE of Burn/Collect. The parser CANNOT distinguish
the split-tx collect leg from a legitimate fee-only LP_COLLECT_FEES
harvest from a single receipt — both look identical at the receipt
level. The parser therefore returns its best single-receipt
understanding (``fees = collect_amount`` for collect-only, treating the
whole transfer as fees because no Burn was observed) and tags
``source="collect"``. The ``ResultEnricher`` aggregate layer overrides
to ``fees = max(collect.amount - decrease.amount, 0)`` when a
``decrease_liquidity`` sibling is present (split-tx LP_CLOSE). See
``tests/unit/execution/test_result_enricher_aggregate_close.py`` for
the aggregator tests.
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.uniswap_v3.receipt_parser import (
    EVENT_TOPICS,
    UniswapV3ReceiptParser,
)
from almanak.framework.execution.extracted_data import LPCloseData

POOL_ADDR = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
PRINCIPAL0 = 1_000_000_000  # 1000 USDC raw (6 dec)
PRINCIPAL1 = 500_000_000_000_000_000  # 0.5 WETH raw (18 dec)
FEES0 = 12_345  # accrued USDC fee
FEES1 = 6_789_000_000_000  # accrued WETH fee
LIQUIDITY = 9_876_543_210


def _enc_int24_topic(value: int) -> str:
    return f"0x{value & ((1 << 256) - 1):064x}"


def _make_burn_log(
    *,
    amount: int,
    amount0: int,
    amount1: int,
    pool_address: str = POOL_ADDR,
    log_index: int = 1,
) -> dict[str, Any]:
    """Build an ABI-faithful Uniswap V3 Pool ``Burn`` log.

    Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper,
         uint128 amount, uint256 amount0, uint256 amount1)
    Data layout: amount (uint128 padded to 32B) ‖ amount0 (uint256) ‖ amount1 (uint256).
    """
    data = f"{amount:064x}{amount0:064x}{amount1:064x}"
    return {
        "address": pool_address,
        "topics": [
            EVENT_TOPICS["Burn"],
            "0x" + "0" * 24 + "1" * 40,  # owner (indexed)
            _enc_int24_topic(-100),  # tickLower (indexed)
            _enc_int24_topic(100),  # tickUpper (indexed)
        ],
        "data": f"0x{data}",
        "logIndex": log_index,
    }


RECIPIENT_ADDR = "0x" + "0" * 24 + "2" * 40


def _make_collect_log(
    *,
    amount0: int,
    amount1: int,
    pool_address: str = POOL_ADDR,
    recipient: str = RECIPIENT_ADDR,
    log_index: int = 2,
) -> dict[str, Any]:
    """Build an ABI-faithful Uniswap V3 Pool ``Collect`` log.

    Collect(address indexed owner, address recipient, int24 indexed tickLower,
            int24 indexed tickUpper, uint128 amount0, uint128 amount1)

    ``owner``/``tickLower``/``tickUpper`` are indexed → topic1..topic3.
    ``recipient`` is **non-indexed** → first 32-byte data slot.
    Data layout: recipient (address padded to 32B) ‖ amount0 (uint128 padded
    to 32B) ‖ amount1 (uint128 padded to 32B).
    """
    # Strip "0x" and right-pad-to-32B (address occupies the low 20 bytes).
    recipient_padded = recipient.removeprefix("0x").rjust(64, "0")
    data = f"{recipient_padded}{amount0:064x}{amount1:064x}"
    return {
        "address": pool_address,
        "topics": [
            EVENT_TOPICS["Collect"],
            "0x" + "0" * 24 + "1" * 40,
            _enc_int24_topic(-100),
            _enc_int24_topic(100),
        ],
        "data": f"0x{data}",
        "logIndex": log_index,
    }


@pytest.fixture
def parser() -> UniswapV3ReceiptParser:
    return UniswapV3ReceiptParser(chain="arbitrum")


def test_burn_plus_collect_yields_principal_and_fees(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Single-tx multicall close: ``decreaseLiquidity`` (Burn) +
    ``collect`` (Collect) in the same TX. fees = collect - burn,
    ``source="collect"`` because the receipt carries the Collect event."""
    receipt = {
        "logs": [
            _make_burn_log(amount=LIQUIDITY, amount0=PRINCIPAL0, amount1=PRINCIPAL1),
            _make_collect_log(amount0=PRINCIPAL0 + FEES0, amount1=PRINCIPAL1 + FEES1),
        ],
        "status": 1,
    }
    out = parser.extract_lp_close_data(receipt)
    assert isinstance(out, LPCloseData)
    assert out.amount0_collected == PRINCIPAL0 + FEES0
    assert out.amount1_collected == PRINCIPAL1 + FEES1
    assert out.fees0 == FEES0
    assert out.fees1 == FEES1
    assert out.liquidity_removed == LIQUIDITY
    assert out.source == "collect"


def test_collect_only_treats_full_amount_as_fees(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Collect-only receipt (either: in-range fee harvest, OR the
    collect-half of the V3 compiler's 3-tx LP_CLOSE bundle, OR
    LP_CLOSE on a position whose ``liquidity == 0`` already so the
    compiler skipped the decrease step).

    The parser CANNOT tell those cases apart from a single receipt —
    intent context lives at the caller. The parser's best single-
    receipt answer is ``fees = collect_amount`` (treating the whole
    transfer as fees because no Burn was observed), which is correct
    for LP_COLLECT_FEES and no-liquidity-but-owed-tokens scenarios.

    For split-tx LP_CLOSE, the aggregate layer
    (``ResultEnricher._select_preferred_aggregate``) overrides this
    attribution to ``fees = max(collect.amount - decrease.amount, 0)``
    when a ``decrease_liquidity`` sibling is present. See
    ``tests/unit/execution/test_result_enricher_aggregate_close.py``
    for the aggregator coverage.
    """
    receipt = {
        "logs": [_make_collect_log(amount0=FEES0, amount1=FEES1)],
        "status": 1,
    }
    out = parser.extract_lp_close_data(receipt)
    assert out is not None
    # amount*_collected reflects the actual transfer (truth on receipt).
    assert out.amount0_collected == FEES0
    assert out.amount1_collected == FEES1
    # Parser's best single-receipt attribution: whole transfer as fees.
    # Aggregator overrides this when a decrease_liquidity sibling exists.
    assert out.fees0 == FEES0
    assert out.fees1 == FEES1
    assert out.liquidity_removed is None
    # Source tag enables the aggregator's preferred-source picker.
    assert out.source == "collect"


def test_burn_only_yields_principal_with_unmeasured_fees(
    parser: UniswapV3ReceiptParser,
) -> None:
    """The decrease-half of a 3-tx LP_CLOSE bundle: only the Burn event
    is in this receipt. Principal is measured (from Burn), fees are
    unmeasured (no Collect to diff against). VIB-4470 / blueprint 27
    §Empty ≠ Zero. Source tag is ``"decrease_liquidity"``."""
    receipt = {
        "logs": [
            _make_burn_log(amount=LIQUIDITY, amount0=PRINCIPAL0, amount1=PRINCIPAL1),
        ],
        "status": 1,
    }
    out = parser.extract_lp_close_data(receipt)
    assert out is not None
    assert out.amount0_collected == PRINCIPAL0
    assert out.amount1_collected == PRINCIPAL1
    assert out.fees0 is None
    assert out.fees1 is None
    assert out.liquidity_removed == LIQUIDITY
    assert out.source == "decrease_liquidity"


def test_no_burn_no_collect_returns_none(parser: UniswapV3ReceiptParser) -> None:
    """No relevant events → None (matches existing contract: never raise,
    let the caller treat absence as 'no LP close happened')."""
    receipt = {"logs": [], "status": 1}
    assert parser.extract_lp_close_data(receipt) is None


def test_collect_below_burn_clamps_fees_to_zero(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Pathological case: a Collect amount below the Burn amount (would
    only happen with multi-position aggregation or a mid-TX state we
    don't fully support). Must clamp at zero — never report negative fees."""
    receipt = {
        "logs": [
            _make_burn_log(amount=LIQUIDITY, amount0=PRINCIPAL0, amount1=PRINCIPAL1),
            _make_collect_log(amount0=PRINCIPAL0 - 1, amount1=PRINCIPAL1 - 1),
        ],
        "status": 1,
    }
    out = parser.extract_lp_close_data(receipt)
    assert out is not None
    assert out.fees0 == 0
    assert out.fees1 == 0
