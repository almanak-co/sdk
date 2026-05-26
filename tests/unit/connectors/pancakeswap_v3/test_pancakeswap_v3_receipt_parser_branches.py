"""Branch coverage tests for ``PancakeSwapV3ReceiptParser.extract_lp_open_data``.

Modelled after ``tests/unit/connectors/aerodrome/test_aerodrome_receipt_parser_branches.py``
(the Slipstream sibling that shipped with PR #2241). PancakeSwap V3 is a
direct Uniswap V3 fork, so the test surface is intentionally parallel:

  * Happy path (Pool Mint + NPM IncreaseLiquidity + Pool Swap)
  * NPM address filter (logs from a non-NPM contract are ignored)
  * Missing Pool Mint (LPOpenData still emitted, ticks=None, pool=='')
  * Missing Swap (current_tick=None — pure NPM.mint LP_OPEN)
  * Negative tick sign extension (indexed int24 two's-complement)
  * Unknown chain fail-loud (warn + None — never silent default)
  * Malformed IncreaseLiquidity payload → ExtractError via the
    ``extract_lp_open_data_result`` wrapper
  * ``_strict_parse`` happy/missing/crash paths for the result wrapper

Reference: blueprints/19-receipt-parser-base-infrastructure.md (VIB-3159
fail-closed semantics), VIB-3887 (current_tick on LP_OPEN), VIB-3893
(pool_address on LP_OPEN).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from almanak.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    PancakeSwapV3ReceiptParser,
)
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
)

# =============================================================================
# Constants and helpers
# =============================================================================

# Canonical PCS V3 NPM (same address across all 5 supported chains today).
NPM = POSITION_MANAGER_ADDRESSES["arbitrum"]
POOL = "0x" + "aa" * 20
WALLET = "0x" + "cc" * 20
MINT_TOPIC = EVENT_TOPICS["Mint"]
INCREASE_TOPIC = EVENT_TOPICS["IncreaseLiquidity"]
SWAP_TOPIC = EVENT_TOPICS["Swap"]


def _pad32(val: int) -> str:
    """Unsigned uint256 padded to 32 bytes (no 0x prefix)."""
    return f"{val:064x}"


def _signed_pad32(val: int) -> str:
    """Signed int256 padded to 32 bytes via two's complement (no 0x prefix)."""
    if val < 0:
        val += 1 << 256
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    """Encode a 20-byte address as a 32-byte indexed topic (with 0x prefix)."""
    return "0x" + addr.replace("0x", "").lower().zfill(64)


def _int24_topic(val: int) -> str:
    """Encode a signed int24 as a 32-byte indexed topic (two's complement)."""
    if val < 0:
        val += 1 << 256
    return "0x" + f"{val:064x}"


def _receipt(logs: list[dict]) -> dict:
    """Wrap a list of logs into a minimal successful receipt."""
    return {"status": 1, "logs": logs}


def _pool_mint_log(
    *,
    tick_lower: int,
    tick_upper: int,
    pool: str = POOL,
    owner: str = NPM,
    amount: int = 0,
    amount0: int = 0,
    amount1: int = 0,
    log_index: int = 1,
) -> dict:
    """Uniswap-V3-style pool Mint event emitted by a PancakeSwap V3 pool.

    Layout:
        topics = [topic0, owner(indexed), tickLower(indexed int24), tickUpper(indexed int24)]
        data   = sender(32B) + amount(32B) + amount0(32B) + amount1(32B)
    """
    return {
        "address": pool,
        "topics": [
            MINT_TOPIC,
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x"
        + _addr_topic(WALLET).removeprefix("0x")  # sender
        + _pad32(amount)
        + _pad32(amount0)
        + _pad32(amount1),
        "logIndex": log_index,
    }


def _npm_increase_liquidity_log(
    *,
    token_id: int,
    liquidity: int,
    amount0: int,
    amount1: int,
    npm: str = NPM,
    log_index: int = 2,
) -> dict:
    """IncreaseLiquidity event emitted by the PCS V3 NPM (address-filtered)."""
    return {
        "address": npm,
        "topics": [
            INCREASE_TOPIC,
            _addr_topic("0x" + format(token_id, "040x")),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _pcs_swap_log(
    *,
    tick: int,
    pool: str = POOL,
    amount0: int = 1,
    amount1: int = -1,
    sqrt_price_x96: int = 0,
    liquidity: int = 0,
    protocol_fees_token0: int = 0,
    protocol_fees_token1: int = 0,
    log_index: int = 0,
) -> dict:
    """PancakeSwap V3 Swap event (9 params — extra protocolFees vs UV3's 7).

    Layout: amount0 (int256) | amount1 (int256) | sqrtPriceX96 (uint160 padded)
        | liquidity (uint128 padded) | tick (int24 sign-extended)
        | protocolFeesToken0 (uint128 padded) | protocolFeesToken1 (uint128 padded)
    """
    return {
        "address": pool,
        "topics": [SWAP_TOPIC, _addr_topic(WALLET), _addr_topic(WALLET)],
        "data": "0x"
        + _signed_pad32(amount0)
        + _signed_pad32(amount1)
        + _pad32(sqrt_price_x96)
        + _pad32(liquidity)
        + _signed_pad32(tick)
        + _pad32(protocol_fees_token0)
        + _pad32(protocol_fees_token1),
        "logIndex": log_index,
    }


# =============================================================================
# extract_lp_open_data — branch coverage
# =============================================================================


class TestPancakeSwapV3ExtractLpOpenData:
    """Coverage for ``PancakeSwapV3ReceiptParser.extract_lp_open_data``."""

    def test_full_path_with_pool_mint_and_swap(self) -> None:
        """Happy path: Pool Mint + IncreaseLiquidity + Swap → full LPOpenData."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pcs_swap_log(tick=12345),
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(token_id=42, liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 42
        assert out.liquidity == 10**18
        assert out.amount0 == 1_000_000
        assert out.amount1 == 5 * 10**14
        assert out.tick_lower == -100
        assert out.tick_upper == 100
        assert out.current_tick == 12345
        assert out.pool_address == POOL.lower()

    def test_negative_tick_sign_extension(self) -> None:
        """Pool Mint with negative ticks decodes to signed int24."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-887220, tick_upper=-100),
            _npm_increase_liquidity_log(token_id=1, liquidity=1, amount0=1, amount1=1),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.tick_lower == -887220
        assert out.tick_upper == -100

    def test_no_increase_liquidity_returns_none(self) -> None:
        """Empty receipt → None (no event found, not a crash)."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_lp_open_data(_receipt([])) is None

    def test_ignores_increase_from_non_npm_address(self) -> None:
        """IncreaseLiquidity from an unrelated contract is filtered out."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _npm_increase_liquidity_log(
                token_id=1,
                liquidity=1,
                amount0=1,
                amount1=1,
                npm="0x" + "ee" * 20,  # NOT the canonical NPM
            ),
        ]
        assert parser.extract_lp_open_data(_receipt(logs)) is None

    def test_ignores_pool_mint_with_non_npm_owner(self) -> None:
        """Pool Mint with owner != NPM still yields LPOpenData, but ticks=None.

        The Pool Mint is skipped (not NPM-owned), so ``last_npm_mint`` stays
        None and we cannot recover tick bounds or pool address. The
        IncreaseLiquidity itself still decodes — partial result is the
        documented behaviour for this case.
        """
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(
                tick_lower=-100,
                tick_upper=100,
                owner=WALLET,
            ),
            _npm_increase_liquidity_log(token_id=99, liquidity=1, amount0=1, amount1=1),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 99
        assert out.tick_lower is None
        assert out.tick_upper is None
        assert out.pool_address == ""

    def test_no_swap_event_leaves_current_tick_none(self) -> None:
        """No Swap log in receipt → current_tick=None (framework slot0 fallback)."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-50, tick_upper=50),
            _npm_increase_liquidity_log(token_id=7, liquidity=1, amount0=1, amount1=1),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.current_tick is None
        assert out.tick_lower == -50
        assert out.tick_upper == 50

    def test_log_with_object_shape_uses_getattr(self) -> None:
        """Logs presented as web3 AttributeDict-like objects decode via getattr."""

        class _LogObj:
            def __init__(self, d: dict) -> None:
                self.topics = d["topics"]
                self.address = d["address"]
                self.data = d["data"]

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "status": 1,
            "logs": [
                _LogObj(_pool_mint_log(tick_lower=-200, tick_upper=200)),
                _LogObj(_npm_increase_liquidity_log(token_id=11, liquidity=10, amount0=1, amount1=2)),
            ],
        }
        out = parser.extract_lp_open_data(receipt)
        assert out is not None
        assert out.position_id == 11
        assert out.tick_lower == -200
        assert out.tick_upper == 200

    def test_log_with_bytes_address_decoded(self) -> None:
        """Logs whose ``address`` field is raw bytes still match the NPM filter."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        npm_bytes = bytes.fromhex(NPM.removeprefix("0x"))
        increase = _npm_increase_liquidity_log(token_id=22, liquidity=1, amount0=1, amount1=1)
        increase["address"] = npm_bytes
        logs = [
            _pool_mint_log(tick_lower=-50, tick_upper=50),
            increase,
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 22

    def test_empty_topics_log_skipped(self) -> None:
        """Logs with empty topics lists are skipped (defensive)."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        empty_log: dict[str, Any] = {
            "address": NPM,
            "topics": [],
            "data": "0x",
        }
        logs = [
            empty_log,
            _pool_mint_log(tick_lower=-10, tick_upper=10),
            _npm_increase_liquidity_log(token_id=33, liquidity=1, amount0=1, amount1=1),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 33

    def test_malformed_tokenid_topic_skipped(self) -> None:
        """A non-hex tokenId topic on an IncreaseLiquidity is skipped."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        bad_log = {
            "address": NPM,
            "topics": [INCREASE_TOPIC, "0xZZZZNOTHEX"],
            "data": "0x" + _pad32(1) + _pad32(1) + _pad32(1),
        }
        # The bad log is skipped; no good IL follows → return None
        assert parser.extract_lp_open_data(_receipt([bad_log])) is None

    def test_pool_mint_with_fewer_than_4_topics_skipped(self) -> None:
        """Pool Mint logs with < 4 topics (malformed) do not crash the parser."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        short_mint = {
            "address": POOL,
            "topics": [MINT_TOPIC, _addr_topic(NPM)],
            "data": "0x",
        }
        logs = [
            short_mint,
            _npm_increase_liquidity_log(token_id=44, liquidity=1, amount0=1, amount1=1),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 44
        assert out.tick_lower is None
        assert out.tick_upper is None

    def test_unknown_chain_fails_loud(self, caplog: pytest.LogCaptureFixture) -> None:
        """Unknown chain returns None AND emits the fail-loud warning.

        Asserts BOTH the return value (None) AND the warning content, so a
        regression that silently swallows the warning still trips this test.
        """
        parser = PancakeSwapV3ReceiptParser(chain="optimism")  # not in PCS V3 chain set
        logs = [
            _pool_mint_log(tick_lower=-10, tick_upper=10),
            _npm_increase_liquidity_log(token_id=55, liquidity=1, amount0=1, amount1=1),
        ]
        with caplog.at_level(
            logging.WARNING,
            logger="almanak.connectors.pancakeswap_v3.receipt_parser",
        ):
            result = parser.extract_lp_open_data(_receipt(logs))
        # Behavioural contract: no silent default to a known-chain NPM.
        assert result is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("optimism" in r.getMessage() for r in warnings), (
            f"Expected a WARNING naming 'optimism' but got: {[r.getMessage() for r in warnings]!r}"
        )
        assert any("PancakeSwap V3 NPM not registered" in r.getMessage() for r in warnings), (
            "Expected the 'PancakeSwap V3 NPM not registered' phrasing"
        )

    def test_empty_data_skipped(self) -> None:
        """IncreaseLiquidity with empty data is skipped (not a crash)."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        empty_data_il = {
            "address": NPM,
            "topics": [INCREASE_TOPIC, _addr_topic("0x" + format(1, "040x"))],
            "data": "0x",
        }
        assert parser.extract_lp_open_data(_receipt([empty_data_il])) is None

    def test_multiple_swap_events_pick_latest_tick(self) -> None:
        """For multi-hop receipts, the LATEST Swap on the pool wins.

        Receipts arrive from the RPC in logIndex order, so the final
        matching Swap carries the live post-swap tick the LP_OPEN sees.
        """
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pcs_swap_log(tick=1111, log_index=0),
            _pool_mint_log(tick_lower=-100, tick_upper=100, log_index=1),
            _npm_increase_liquidity_log(token_id=8, liquidity=1, amount0=1, amount1=1, log_index=2),
            _pcs_swap_log(tick=2222, log_index=3),  # later Swap wins
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.current_tick == 2222


# =============================================================================
# extract_lp_open_data_result — fail-closed wrapper coverage (VIB-3159)
# =============================================================================


class TestPancakeSwapV3ExtractLpOpenDataResult:
    """Coverage for the ``_result`` fail-closed variant."""

    def test_malformed_increase_liquidity_propagates(self) -> None:
        """Malformed IncreaseLiquidity payload → ExtractError (NOT ExtractMissing).

        VIB-3159 / Blueprint 19 invariant: parser crash ≠ event missing.
        """
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        bad = {
            "address": NPM,
            "topics": [
                INCREASE_TOPIC,
                _addr_topic("0x" + format(1, "040x")),
            ],
            # Non-hex inside ``data`` — passes the not-empty check but fails
            # the uint128/uint256 decode.
            "data": "0xZZ",
        }
        result = parser.extract_lp_open_data_result(_receipt([bad]))
        assert isinstance(result, ExtractError)

    def test_truncated_increase_liquidity_payload_propagates(self) -> None:
        """Truncated IncreaseLiquidity payload → ExtractError, NOT silent
        ``amount0=0`` / ``amount1=0`` (Codex P2 on PR #2248).

        ``HexDecoder.decode_uint256`` returns ``0`` when reading past the
        end of a normalized hex string, so without the length guard a
        truncated payload would record measured-zero amounts and corrupt
        the audit trail (CLAUDE.md §Accounting "Empty ≠ Zero").
        """
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        # Valid hex but only 64 chars after 0x → enough for liquidity, not
        # for amount0 / amount1. Must raise via the new length guard.
        truncated = {
            "address": NPM,
            "topics": [
                INCREASE_TOPIC,
                _addr_topic("0x" + format(1, "040x")),
            ],
            "data": "0x" + ("00" * 16) + ("00" * 16),  # 64 hex chars = 32 bytes only
        }
        result = parser.extract_lp_open_data_result(_receipt([truncated]))
        assert isinstance(result, ExtractError)
        assert "Truncated IncreaseLiquidity" in str(result.error)

    def test_result_wrapper_ok(self) -> None:
        """Successful extraction → ExtractOk(value=LPOpenData)."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(token_id=1, liquidity=1, amount0=1, amount1=1),
        ]
        result = parser.extract_lp_open_data_result(_receipt(logs))
        assert isinstance(result, ExtractOk)

    def test_result_wrapper_missing(self) -> None:
        """Empty receipt → ExtractMissing (benign, no event present)."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_lp_open_data_result(_receipt([]))
        assert isinstance(result, ExtractMissing)

    def test_result_wrapper_crash(self) -> None:
        """Synthetic crash inside extract_lp_open_data → ExtractError."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")

        def boom(_r: dict) -> None:
            raise RuntimeError("synthetic")

        parser.extract_lp_open_data = boom  # type: ignore[method-assign]
        result = parser.extract_lp_open_data_result(_receipt([]))
        assert isinstance(result, ExtractError)


# =============================================================================
# POSITION_MANAGER_ADDRESSES — sourced from core/contracts.py
# =============================================================================


class TestPositionManagerAddressesSource:
    """Verify the NPM map is derived from the canonical contracts registry."""

    def test_addresses_match_core_contracts(self) -> None:
        """Each chain's NPM in ``POSITION_MANAGER_ADDRESSES`` must equal
        ``PANCAKESWAP_V3[chain]['nft']`` (case-insensitive).

        Catches drift between the parser's address dict and the canonical
        registry — the regression that motivated VIB-3893 / the Aerodrome
        refactor in PR #2241.
        """
        from almanak.core.contracts import PANCAKESWAP_V3

        for chain, entry in PANCAKESWAP_V3.items():
            expected = entry["nft"].lower()
            actual = POSITION_MANAGER_ADDRESSES[chain.lower()]
            assert actual == expected, (
                f"Drift: {chain} NPM = {actual!r}, expected {expected!r}. "
                f"Update PANCAKESWAP_V3[{chain!r}]['nft'] in core/contracts.py "
                f"if the address legitimately changed."
            )

    def test_bnb_alias_resolves_to_bsc(self) -> None:
        """``bnb`` is preserved as an alias of ``bsc`` (historical callers)."""
        assert POSITION_MANAGER_ADDRESSES["bnb"] == POSITION_MANAGER_ADDRESSES["bsc"]
