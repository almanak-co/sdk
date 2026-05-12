"""Unit tests for SushiSwap V3 ``extract_lp_open_data`` and fail-closed wrappers.

Covers the LP_OPEN extraction logic introduced as the SushiSwap V3 sibling of
the Aerodrome Slipstream PR #2241 work:

- Happy path: ``IncreaseLiquidity`` + Pool ``Mint`` + Pool ``Swap`` in one
  receipt yields a fully populated ``LPOpenData``.
- Negative-tick sign extension (e.g. WETH/USDC range crossing zero).
- NPM address filter rejects ``IncreaseLiquidity`` from unrelated contracts.
- Pool Mint with ``owner != NPM`` doesn't claim its ticks for the next IL.
- Missing Pool Mint → partial result (ticks=None, pool_address="").
- Missing Pool Swap → ``current_tick=None``.
- Empty / failed / object-shaped logs handled defensively.
- Malformed IncreaseLiquidity payload → propagates exception (becomes
  ``ExtractError`` via the fail-closed wrapper).
- Unknown chain → fail-loud (None + WARNING).
- ``extract_lp_open_data_result`` returns:
    - ``ExtractOk`` on success
    - ``ExtractMissing`` on no logs / no IncreaseLiquidity
    - ``ExtractError`` on parser crash
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    SushiSwapV3ReceiptParser,
)
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
)

# Pre-resolved fixtures
_NPM_ARB = POSITION_MANAGER_ADDRESSES["arbitrum"]
_POOL = "0xC6962004F452BE9203591991D15F6B388e09E8D0"
_WALLET = "0x1234567890123456789012345678901234567890"
_OTHER_CONTRACT = "0x" + "ee" * 20


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _addr_topic(addr: str) -> str:
    """Pad an address to 32-byte topic."""
    return "0x" + addr.lower().removeprefix("0x").zfill(64)


def _pad32(value: int) -> str:
    """Pack an unsigned 256-bit value into 32 hex bytes (no 0x prefix)."""
    return f"{value:064x}"


def _pad32_signed(value: int) -> str:
    """Pack a signed 256-bit value into 32 hex bytes (two's complement)."""
    if value < 0:
        value = value + (1 << 256)
    return f"{value:064x}"


def _int24_topic(value: int) -> str:
    """Pack a signed int24 into an indexed topic (right-aligned 32 bytes)."""
    if value < 0:
        value = value + (1 << 256)
    return "0x" + f"{value:064x}"


def _receipt(logs: list[Any]) -> dict[str, Any]:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": "0x1000000",
        "status": 1,
        "logs": logs,
    }


def _pool_mint_log(
    *,
    tick_lower: int,
    tick_upper: int,
    owner: str = _NPM_ARB,
    pool: str = _POOL,
    amount: int = 10**18,
    amount0: int = 0,
    amount1: int = 0,
    log_index: int = 1,
) -> dict[str, Any]:
    """SushiSwap V3 Pool ``Mint`` event (Uniswap V3-shaped).

    Layout:
        topics = [topic0, owner(indexed), tickLower(indexed int24),
                  tickUpper(indexed int24)]
        data   = sender(32B) + amount(uint128, 32B) + amount0(32B) + amount1(32B)
    """
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Mint"],
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x"
        + _addr_topic(_WALLET).removeprefix("0x")  # sender
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
    npm: str = _NPM_ARB,
    log_index: int = 2,
) -> dict[str, Any]:
    """``IncreaseLiquidity`` event emitted by the SushiSwap V3 NPM.

    Layout:
        topics = [topic0, tokenId(indexed uint256)]
        data   = liquidity(uint128, left-padded to 32B) + amount0(uint256)
                 + amount1(uint256)
    """
    return {
        "address": npm,
        "topics": [
            EVENT_TOPICS["IncreaseLiquidity"],
            "0x" + _pad32(token_id),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _pool_swap_log(
    *,
    tick: int,
    pool: str = _POOL,
    amount0: int = 1,
    amount1: int = -1,
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**12,
    log_index: int = 0,
) -> dict[str, Any]:
    """SushiSwap V3 Pool ``Swap`` event.

    Layout:
        topics = [topic0, sender, recipient]
        data   = amount0(int256, 32B) + amount1(int256, 32B)
                 + sqrtPriceX96(uint160 padded 32B) + liquidity(uint128 padded 32B)
                 + tick(int24 sign-extended into 32B).
    """
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Swap"],
            _addr_topic(_WALLET),
            _addr_topic(_WALLET),
        ],
        "data": "0x"
        + _pad32_signed(amount0)
        + _pad32_signed(amount1)
        + _pad32(sqrt_price_x96)
        + _pad32(liquidity)
        + _pad32_signed(tick),
        "logIndex": log_index,
    }


# -----------------------------------------------------------------------------
# Happy path
# -----------------------------------------------------------------------------


class TestExtractLpOpenData:
    def test_full_path_with_pool_mint_and_swap(self) -> None:
        """Receipt with Swap + Pool Mint + IncreaseLiquidity → full LPOpenData."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_swap_log(tick=12345),
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=42, liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14
            ),
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
        assert out.pool_address == _POOL.lower()

    def test_negative_tick_sign_extension(self) -> None:
        """Pool Mint with negative ticks decodes to signed int24."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-887220, tick_upper=-100),
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.tick_lower == -887220
        assert out.tick_upper == -100

    def test_no_logs_returns_none(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_lp_open_data(_receipt([])) is None

    def test_no_increase_liquidity_returns_none(self) -> None:
        """Pool Mint without a matching IncreaseLiquidity → None."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [_pool_mint_log(tick_lower=-100, tick_upper=100)]
        assert parser.extract_lp_open_data(_receipt(logs)) is None

    def test_ignores_increase_from_non_npm_address(self) -> None:
        """IncreaseLiquidity from a contract that is NOT the registered NPM
        is filtered out by the address gate."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1, npm=_OTHER_CONTRACT
            ),
        ]
        assert parser.extract_lp_open_data(_receipt(logs)) is None

    def test_ignores_pool_mint_with_non_npm_owner(self) -> None:
        """Pool Mint whose ``owner`` != NPM still yields LPOpenData
        (the IncreaseLiquidity is the source of truth for amounts), but
        the parser doesn't claim the unrelated Mint's ticks."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100, owner=_WALLET),
            _npm_increase_liquidity_log(
                token_id=99, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 99
        assert out.tick_lower is None
        assert out.tick_upper is None
        assert out.pool_address == ""

    def test_no_swap_event_leaves_current_tick_none(self) -> None:
        """No matching Pool Swap → ``current_tick=None`` (framework slot0 fallback)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-50, tick_upper=50),
            _npm_increase_liquidity_log(
                token_id=7, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.current_tick is None
        assert out.tick_lower == -50
        assert out.tick_upper == 50

    def test_log_with_object_shape_uses_getattr(self) -> None:
        """Logs presented as objects (web3 AttributeDict-like) decode via getattr."""

        class _LogObj:
            def __init__(self, d: dict[str, Any]) -> None:
                self.topics = d["topics"]
                self.address = d["address"]
                self.data = d["data"]

        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "logs": [
                _LogObj(_pool_mint_log(tick_lower=-200, tick_upper=200)),
                _LogObj(
                    _npm_increase_liquidity_log(
                        token_id=11, liquidity=10, amount0=1, amount1=2
                    )
                ),
            ]
        }
        out = parser.extract_lp_open_data(receipt)
        assert out is not None
        assert out.position_id == 11
        assert out.tick_lower == -200
        assert out.tick_upper == 200

    def test_log_with_bytes_address_decoded(self) -> None:
        """Logs whose ``address`` is raw bytes still match the NPM filter."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        npm_bytes = bytes.fromhex(_NPM_ARB.removeprefix("0x"))
        increase = _npm_increase_liquidity_log(
            token_id=22, liquidity=1, amount0=1, amount1=1
        )
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
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        empty_log = {"address": _NPM_ARB, "topics": [], "data": "0x"}
        logs = [
            empty_log,
            _pool_mint_log(tick_lower=-10, tick_upper=10),
            _npm_increase_liquidity_log(
                token_id=33, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 33

    def test_malformed_tokenid_topic_skipped(self) -> None:
        """A non-hex tokenId topic on IncreaseLiquidity is skipped (not crashed).

        ``int(topic, 16)`` raises ValueError on a non-hex string; the parser
        wraps that single decode in try/except and continues scanning. No
        good IL follows, so the call returns None.
        """
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        bad_log = {
            "address": _NPM_ARB,
            "topics": [EVENT_TOPICS["IncreaseLiquidity"], "0xZZZZNOTHEX"],
            "data": "0x" + _pad32(1) + _pad32(1) + _pad32(1),
        }
        assert parser.extract_lp_open_data(_receipt([bad_log])) is None

    def test_pool_mint_with_fewer_than_4_topics_skipped(self) -> None:
        """Pool Mint logs with < 4 topics do not crash the parser."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        short_mint = {
            "address": _POOL,
            "topics": [EVENT_TOPICS["Mint"], _addr_topic(_NPM_ARB)],
            "data": "0x",
        }
        logs = [
            short_mint,
            _npm_increase_liquidity_log(
                token_id=44, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 44
        assert out.tick_lower is None
        assert out.tick_upper is None

    def test_unknown_chain_fails_loud(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unknown chain returns None AND emits the fail-loud warning.

        Asserts both the return value AND the warning content, so a regression
        that silently swallows the warning still trips this test. This is the
        ``# Fail-loud on unknown chains`` rule from AGENTS.md / task spec.
        """
        # A chain key the SushiSwap V3 registry has never seen.
        parser = SushiSwapV3ReceiptParser(chain="zerog")
        logs = [
            _pool_mint_log(tick_lower=-10, tick_upper=10),
            _npm_increase_liquidity_log(
                token_id=55, liquidity=1, amount0=1, amount1=1
            ),
        ]
        with caplog.at_level(
            logging.WARNING,
            logger="almanak.framework.connectors.sushiswap_v3.receipt_parser",
        ):
            result = parser.extract_lp_open_data(_receipt(logs))
        # Behavioural contract: no silent default to any specific chain.
        assert result is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("zerog" in r.getMessage() for r in warnings), (
            f"Expected a WARNING naming 'zerog' but got: "
            f"{[r.getMessage() for r in warnings]!r}"
        )
        assert any(
            "SushiSwap V3 NPM not registered" in r.getMessage() for r in warnings
        ), (
            "Expected the 'SushiSwap V3 NPM not registered' fail-loud phrasing"
        )

    def test_npm_address_filter_lowercase_match(self) -> None:
        """The NPM address comparison is case-insensitive (registry stores lower-case)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        increase = _npm_increase_liquidity_log(
            token_id=66, liquidity=1, amount0=1, amount1=1
        )
        # Mixed-case address in the receipt should still match.
        increase["address"] = _NPM_ARB.upper()
        logs = [_pool_mint_log(tick_lower=-10, tick_upper=10), increase]
        out = parser.extract_lp_open_data(_receipt(logs))
        assert out is not None
        assert out.position_id == 66


# -----------------------------------------------------------------------------
# Fail-closed wrapper (VIB-3159 / Blueprint 19)
# -----------------------------------------------------------------------------


class TestExtractLpOpenDataResult:
    def test_returns_ok_on_happy_path(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=77, liquidity=10**18, amount0=1, amount1=1
            ),
        ]
        result = parser.extract_lp_open_data_result(_receipt(logs))
        assert isinstance(result, ExtractOk)
        assert result.value.position_id == 77

    def test_returns_missing_on_no_logs(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_lp_open_data_result(_receipt([]))
        assert isinstance(result, ExtractMissing)
        assert "no logs" in result.reason

    def test_returns_missing_on_no_increase_liquidity(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # Pool Mint but no NPM IncreaseLiquidity in the receipt.
        logs = [_pool_mint_log(tick_lower=-10, tick_upper=10)]
        result = parser.extract_lp_open_data_result(_receipt(logs))
        assert isinstance(result, ExtractMissing)
        assert "no IncreaseLiquidity" in result.reason

    def test_returns_error_on_parser_crash(self) -> None:
        """A genuine parser-side exception is surfaced as ``ExtractError``,
        NOT swallowed into ``ExtractMissing``. The wrapper monkey-patches the
        legacy method to raise — exactly the path that VIB-3159 was filed for.
        """
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        def boom(*_args: Any, **_kwargs: Any) -> Any:  # pragma: no cover (sentinel)
            raise RuntimeError("synthetic parser crash")

        parser.extract_lp_open_data = boom  # type: ignore[method-assign]
        # ``extract_lp_open_data_result`` short-circuits to ExtractMissing if
        # logs is empty before reaching the legacy method, so feed in a
        # non-empty receipt to actually exercise the crash path.
        logs = [
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            ),
        ]
        result = parser.extract_lp_open_data_result(_receipt(logs))
        assert isinstance(result, ExtractError)
        assert "synthetic parser crash" in result.error
        assert isinstance(result.exception, RuntimeError)
