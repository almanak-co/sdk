"""Tests for ``UniswapV3ReceiptParser.extract_lp_open_data``.

Closes the April 30 audit gap (item #4): the runner was logging
``Parser UniswapV3ReceiptParser does not declare support for 'lp_open_data'``
because ``lp_open_data`` was missing from ``SUPPORTED_EXTRACTIONS`` and no
extractor was wired up. The accounting handler downstream relies on
``LPOpenData`` carrying the raw on-chain ``amount0`` / ``amount1`` /
``liquidity`` / ``position_id`` so it can scale with the token resolver and
emit a populated ``position_events`` row.

These tests pin the parser contract:
  * ``IncreaseLiquidity`` from the chain's NonfungiblePositionManager →
    ``LPOpenData`` populated with raw ints (no decimal scaling at this layer).
  * Missing ``IncreaseLiquidity`` (e.g. an LP_OPEN that failed mid-bundle) →
    ``None`` — never a raise.
  * Wrong-contract ``IncreaseLiquidity`` (i.e. the topic appears on a log
    not emitted by the registered position manager) is filtered out — we
    refuse to attribute liquidity to an LP_OPEN that did not pass through
    the protocol's NPM.
  * The tagged ``_result`` variant returns the right ``ExtractResult`` for
    each branch (Ok / Missing / Error) so the framework's three-variant
    contract is honored (VIB-3159).

Real-shaped fixtures are constructed inline rather than loaded from a
captured receipt file because the layout is fully specified by the ABI:
``IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity,
uint256 amount0, uint256 amount1)``. Inline fixtures keep the test
self-documenting and avoid a hidden dependency on a JSON blob.
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.uniswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    UniswapV3ReceiptParser,
)
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
)
from almanak.framework.execution.extracted_data import LPOpenData

# A real Uniswap V3 LP_OPEN on Arbitrum produces these (illustrative but
# real-shaped) values for a USDC/WETH 0.05% range deposit. amount0 is in
# 6-decimal USDC (100 USDC), amount1 is in 18-decimal WETH (~0.05 WETH).
TOKEN_ID = 1234567
LIQUIDITY = 12_345_678_901_234
AMOUNT0 = 100_000_000  # 100 USDC raw (6 decimals)
AMOUNT1 = 50_000_000_000_000_000  # 0.05 WETH raw (18 decimals)

ARBITRUM_NPM = POSITION_MANAGER_ADDRESSES["arbitrum"].lower()


def _make_increase_liquidity_log(
    *,
    token_id: int,
    liquidity: int,
    amount0: int,
    amount1: int,
    address: str = ARBITRUM_NPM,
    log_index: int = 3,
) -> dict[str, Any]:
    """Build an ABI-faithful ``IncreaseLiquidity`` log.

    Layout:
        topics[0] = keccak("IncreaseLiquidity(uint256,uint128,uint256,uint256)")
        topics[1] = tokenId (indexed uint256, left-padded)
        data      = liquidity (uint128, left-padded to 32 bytes)
                  ‖ amount0 (uint256)
                  ‖ amount1 (uint256)
    """
    data = (
        f"{liquidity:064x}"  # uint128 widened to 32 bytes
        f"{amount0:064x}"
        f"{amount1:064x}"
    )
    return {
        "address": address,
        "topics": [
            EVENT_TOPICS["IncreaseLiquidity"],
            f"0x{token_id:064x}",
        ],
        "data": f"0x{data}",
        "logIndex": log_index,
    }


def _make_unrelated_transfer_log() -> dict[str, Any]:
    """An ERC-20 Transfer that should not affect lp_open_data extraction."""
    return {
        "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x0000000000000000000000001111111111111111111111111111111111111111",
            "0x0000000000000000000000002222222222222222222222222222222222222222",
        ],
        "data": f"0x{AMOUNT0:064x}",
        "logIndex": 1,
    }


@pytest.fixture
def parser() -> UniswapV3ReceiptParser:
    return UniswapV3ReceiptParser(chain="arbitrum")


# ---------------------------------------------------------------------------
# SUPPORTED_EXTRACTIONS contract
# ---------------------------------------------------------------------------


def test_lp_open_data_is_declared_supported(parser: UniswapV3ReceiptParser) -> None:
    """The accounting writer relies on the ``SUPPORTED_EXTRACTIONS`` flag —
    if it's missing the enricher silently skips the field with a warning,
    which is exactly the bug the April 30 audit caught."""
    assert "lp_open_data" in parser.SUPPORTED_EXTRACTIONS


# ---------------------------------------------------------------------------
# Happy path: real-shaped IncreaseLiquidity log
# ---------------------------------------------------------------------------


def test_extract_lp_open_data_populates_all_fields(parser: UniswapV3ReceiptParser) -> None:
    receipt = {
        "logs": [
            _make_unrelated_transfer_log(),
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
            ),
        ],
        "status": 1,
    }

    out = parser.extract_lp_open_data(receipt)

    assert isinstance(out, LPOpenData)
    assert out.position_id == TOKEN_ID
    assert out.liquidity == LIQUIDITY
    assert out.amount0 == AMOUNT0
    assert out.amount1 == AMOUNT1


def test_extract_lp_open_data_handles_zero_amount_branches(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Single-sided LP_OPENs (one token at the edge of the range) emit
    IncreaseLiquidity with amount0=0 OR amount1=0, but never both. The
    parser must surface the zero — accounting needs to know which side
    was deposited."""
    receipt = {
        "logs": [
            _make_increase_liquidity_log(
                token_id=42,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=0,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data(receipt)
    assert out is not None
    assert out.amount0 == AMOUNT0
    assert out.amount1 == 0


def test_extract_lp_open_data_uses_correct_chain_npm(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Logs from a different chain's NPM (e.g. Base address on Arbitrum
    parser) must be ignored — otherwise an unrelated NPM event in a
    multi-protocol bundle could be attributed to a Uniswap V3 LP_OPEN."""
    base_npm = POSITION_MANAGER_ADDRESSES["base"].lower()
    assert base_npm != ARBITRUM_NPM, "fixture invariant"
    receipt = {
        "logs": [
            _make_increase_liquidity_log(
                token_id=999,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
                address=base_npm,
            ),
        ],
        "status": 1,
    }
    assert parser.extract_lp_open_data(receipt) is None


# ---------------------------------------------------------------------------
# Missing event paths — must return None, never raise
# ---------------------------------------------------------------------------


def test_extract_lp_open_data_missing_event_returns_none(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Receipt with logs but no IncreaseLiquidity (e.g. an LP_OPEN that
    failed mid-bundle on the swap step) — must be ``None``, not a raise."""
    receipt = {"logs": [_make_unrelated_transfer_log()], "status": 1}
    assert parser.extract_lp_open_data(receipt) is None


def test_extract_lp_open_data_empty_logs_returns_none(
    parser: UniswapV3ReceiptParser,
) -> None:
    assert parser.extract_lp_open_data({"logs": []}) is None


def test_extract_lp_open_data_no_logs_key_returns_none(
    parser: UniswapV3ReceiptParser,
) -> None:
    assert parser.extract_lp_open_data({}) is None


def test_extract_lp_open_data_malformed_topic_returns_none(
    parser: UniswapV3ReceiptParser,
) -> None:
    """A garbage tokenId topic must not raise — accounting must not be
    crashed by a malformed log."""
    bad_log = {
        "address": ARBITRUM_NPM,
        "topics": [
            EVENT_TOPICS["IncreaseLiquidity"],
            "not-a-hex-value",
        ],
        "data": "0x" + "00" * 96,
    }
    assert parser.extract_lp_open_data({"logs": [bad_log]}) is None


# ---------------------------------------------------------------------------
# Tagged-variant ``_result`` contract (VIB-3159)
# ---------------------------------------------------------------------------


def test_extract_lp_open_data_result_ok_on_real_event(
    parser: UniswapV3ReceiptParser,
) -> None:
    receipt = {
        "logs": [
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data_result(receipt)
    assert isinstance(out, ExtractOk)
    assert isinstance(out.value, LPOpenData)
    assert out.value.position_id == TOKEN_ID


def test_extract_lp_open_data_result_empty_logs_is_missing(
    parser: UniswapV3ReceiptParser,
) -> None:
    out = parser.extract_lp_open_data_result({"logs": []})
    assert isinstance(out, ExtractMissing)


def test_extract_lp_open_data_result_unrelated_logs_is_missing(
    parser: UniswapV3ReceiptParser,
) -> None:
    out = parser.extract_lp_open_data_result(
        {"logs": [_make_unrelated_transfer_log()], "status": 1}
    )
    assert isinstance(out, ExtractMissing)


def test_extract_lp_open_data_result_crash_is_error(
    parser: UniswapV3ReceiptParser,
) -> None:
    """A crash in ``extract_lp_open_data`` must surface as ``ExtractError``,
    not ``ExtractMissing`` — silently treating a parse error as 'no event'
    is the ghost-position failure mode VIB-3159 closes."""

    def boom(_receipt: dict[str, Any]) -> LPOpenData | None:
        raise RuntimeError("induced lp_open_data crash")

    parser.extract_lp_open_data = boom  # type: ignore[method-assign]
    out = parser.extract_lp_open_data_result({"logs": [{"topics": []}]})
    assert isinstance(out, ExtractError)
    assert "induced lp_open_data crash" in out.error


# ---------------------------------------------------------------------------
# Tick range extraction (LP1 acceptance: tick_lower / tick_upper on every
# LP_OPEN row so range exposure can be reconstructed downstream)
# ---------------------------------------------------------------------------


def _npm_owner_topic(npm_address: str = ARBITRUM_NPM) -> str:
    """Encode the NPM address as a 32-byte indexed topic (right-aligned)."""
    return "0x" + npm_address.removeprefix("0x").rjust(64, "0")


def _make_pool_mint_log(
    *,
    tick_lower: int,
    tick_upper: int,
    pool_address: str = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443",
    owner: str = ARBITRUM_NPM,
    log_index: int = 5,
) -> dict[str, Any]:
    """Build an ABI-faithful Uniswap V3 Pool ``Mint`` log.

    Pool Mint signature::

        Mint(
            address sender,
            address indexed owner,
            int24 indexed tickLower,
            int24 indexed tickUpper,
            uint128 amount,
            uint256 amount0,
            uint256 amount1,
        )

    For NPM-mediated LP_OPENs (the only path the parser claims to support),
    ``owner`` is the NonfungiblePositionManager — the parser uses that to
    pair Mints with their corresponding IncreaseLiquidity in multi-position
    receipts.

    int24 in topics is sign-extended to 32 bytes — we encode the tick as
    its two's-complement int256 representation so HexDecoder.decode_int24
    recovers the signed value.
    """

    def _encode_int24_topic(value: int) -> str:
        # Two's-complement to 256 bits, formatted as 64-char hex
        return f"0x{value & ((1 << 256) - 1):064x}"

    owner_topic = "0x" + owner.removeprefix("0x").rjust(64, "0")
    return {
        "address": pool_address,
        "topics": [
            EVENT_TOPICS["Mint"],
            owner_topic,  # owner = NPM (indexed)
            _encode_int24_topic(tick_lower),
            _encode_int24_topic(tick_upper),
        ],
        "data": "0x" + "0" * 192,  # amount + amount0 + amount1, irrelevant here
        "logIndex": log_index,
    }


def test_extract_lp_open_data_populates_ticks_from_pool_mint(
    parser: UniswapV3ReceiptParser,
) -> None:
    """A real LP_OPEN bundles BOTH the pool's ``Mint`` (carries ticks) AND
    the NPM's ``IncreaseLiquidity`` (carries tokenId / liquidity / amounts).
    The parser must surface ticks via ``LPOpenData.tick_lower`` /
    ``tick_upper`` so downstream position_events rows carry range exposure
    (Accountant Test cell LP1)."""
    receipt = {
        "logs": [
            _make_pool_mint_log(tick_lower=-199960, tick_upper=-197950),
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data(receipt)
    assert isinstance(out, LPOpenData)
    assert out.tick_lower == -199960
    assert out.tick_upper == -197950


def test_extract_lp_open_data_negative_ticks_round_trip(
    parser: UniswapV3ReceiptParser,
) -> None:
    """int24 sign extension: a tick like -887272 (Uniswap V3 absolute min)
    must come back negative, not a huge positive uint256."""
    receipt = {
        "logs": [
            _make_pool_mint_log(tick_lower=-887272, tick_upper=887272),
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data(receipt)
    assert out is not None
    assert out.tick_lower == -887272
    assert out.tick_upper == 887272


def test_extract_lp_open_data_pairs_ticks_with_immediate_prior_mint(
    parser: UniswapV3ReceiptParser,
) -> None:
    """Multi-position bundle: two Mints from two different pools followed by
    one IncreaseLiquidity. The parser must pair the IncreaseLiquidity with
    the **most recent** prior NPM-owned Mint, not the first one in the list.

    Without this pairing, an LP_OPEN inside a multicall that closes one
    position and opens another reports ticks from the unrelated old position
    — exactly the failure mode behind the accounting attempt's outsized-
    liquidity values caught during PR #1997 review."""
    receipt = {
        "logs": [
            # Earlier Mint (different pool): ticks belong to a position the
            # caller is closing/refreshing. Must NOT be attributed.
            _make_pool_mint_log(
                tick_lower=-100_000,
                tick_upper=-90_000,
                pool_address="0x1111111111111111111111111111111111111111",
                log_index=2,
            ),
            # Later Mint (the position being opened by THIS LP_OPEN): ticks
            # belong here. Must be attributed.
            _make_pool_mint_log(
                tick_lower=-199960,
                tick_upper=-197950,
                log_index=4,
            ),
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
                log_index=5,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data(receipt)
    assert isinstance(out, LPOpenData)
    assert out.tick_lower == -199960
    assert out.tick_upper == -197950


def test_extract_lp_open_data_ignores_pool_mint_with_non_npm_owner(
    parser: UniswapV3ReceiptParser,
) -> None:
    """A Pool Mint whose ``owner`` is NOT the NPM (e.g. a custom router or
    a different protocol's mint that happens to share the topic0) must not
    contribute ticks — otherwise an unrelated mint can corrupt range data
    on the LP_OPEN we're actually parsing."""
    receipt = {
        "logs": [
            _make_pool_mint_log(
                tick_lower=-12345,
                tick_upper=12345,
                owner="0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead",
            ),
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data(receipt)
    assert out is not None
    assert out.tick_lower is None
    assert out.tick_upper is None


def test_extract_lp_open_data_no_pool_mint_keeps_ticks_none(
    parser: UniswapV3ReceiptParser,
) -> None:
    """If the receipt has IncreaseLiquidity but no Mint (e.g. an
    increase-on-existing-position TX, which doesn't re-emit Mint), the
    parser must still return LPOpenData with ticks=None — not crash, not
    invent ticks."""
    receipt = {
        "logs": [
            _make_increase_liquidity_log(
                token_id=TOKEN_ID,
                liquidity=LIQUIDITY,
                amount0=AMOUNT0,
                amount1=AMOUNT1,
            ),
        ],
        "status": 1,
    }
    out = parser.extract_lp_open_data(receipt)
    assert out is not None
    assert out.position_id == TOKEN_ID
    assert out.tick_lower is None
    assert out.tick_upper is None
