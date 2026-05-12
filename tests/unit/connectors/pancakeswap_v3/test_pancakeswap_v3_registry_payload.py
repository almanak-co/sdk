"""Unit tests for PancakeSwap V3 registry-payload extractors (VIB-4305).

Mirrors the Uniswap V3 coverage at
``tests/unit/connectors/uniswap_v3/test_extract_registry_payload_close_helpers.py``,
adapted for the PancakeSwap V3 parser. Since PancakeSwap V3 is a direct UV3 fork
at the NPM contract level, its ``extract_registry_payload_close`` reuses the
Uniswap V3 helpers (``_open_payload_disagrees`` / ``_build_close_receipt_payload`` /
``_merge_open_payload_fields``); we exercise the orchestrator end-to-end here
and lean on the UV3 helpers' own coverage for the lower-level contracts.

Test surface:

- ``extract_registry_payload_open`` happy path + every fail-closed branch
  (token_id missing / pool missing / ticks missing / liquidity missing /
  fee_tier omission / token labels).
- ``_decreaseliquidity_token_id`` happy path + filter / parse branches.
- ``extract_registry_payload_close`` happy path + every fail-closed branch
  (lp_close None / token_id missing / pool empty / open_payload disagreement)
  + OPEN-side field merge + fee_tier handling.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    PancakeSwapV3ReceiptParser,
)


# =============================================================================
# Constants and helpers
# =============================================================================

NPM = POSITION_MANAGER_ADDRESSES["arbitrum"]
POOL = "0x" + "aa" * 20
WALLET = "0x" + "cc" * 20

MINT_TOPIC = EVENT_TOPICS["Mint"].lower()
INCREASE_TOPIC = EVENT_TOPICS["IncreaseLiquidity"].lower()
DECREASE_TOPIC = EVENT_TOPICS["DecreaseLiquidity"].lower()
BURN_TOPIC = EVENT_TOPICS["Burn"].lower()
COLLECT_TOPIC = EVENT_TOPICS["Collect"].lower()


def _pad32(val: int) -> str:
    """Unsigned uint256 padded to 32 bytes (no 0x prefix)."""
    return f"{val:064x}"


def _signed_pad32(val: int) -> str:
    """Signed int256 padded to 32 bytes via two's complement (no 0x prefix)."""
    if val < 0:
        val += 1 << 256
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.replace("0x", "").lower().zfill(64)


def _int24_topic(val: int) -> str:
    if val < 0:
        val += 1 << 256
    return "0x" + f"{val:064x}"


def _pool_mint_log(
    *,
    tick_lower: int = -100,
    tick_upper: int = 100,
    pool: str = POOL,
    owner: str = NPM,
    amount: int = 0,
    amount0: int = 0,
    amount1: int = 0,
    log_index: int = 1,
) -> dict:
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


def _npm_increase_log(
    *,
    token_id: int = 42,
    liquidity: int = 10**18,
    amount0: int = 1_000_000,
    amount1: int = 5 * 10**14,
    npm: str = NPM,
    log_index: int = 2,
) -> dict:
    return {
        "address": npm,
        "topics": [
            INCREASE_TOPIC,
            _addr_topic("0x" + format(token_id, "040x")),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _npm_decrease_log(
    *,
    token_id: int = 42,
    liquidity: int = 10**18,
    amount0: int = 1_000_000,
    amount1: int = 5 * 10**14,
    npm: str = NPM,
    log_index: int = 10,
) -> dict:
    return {
        "address": npm,
        "topics": [
            DECREASE_TOPIC,
            _addr_topic("0x" + format(token_id, "040x")),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _pool_burn_log(
    *,
    liquidity: int = 10**18,
    amount0: int = 1_000_000,
    amount1: int = 5 * 10**14,
    tick_lower: int = -100,
    tick_upper: int = 100,
    pool: str = POOL,
    owner: str = NPM,
    log_index: int = 11,
) -> dict:
    return {
        "address": pool,
        "topics": [
            BURN_TOPIC,
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _pool_collect_log(
    *,
    amount0: int = 1_000_000,
    amount1: int = 5 * 10**14,
    tick_lower: int = -100,
    tick_upper: int = 100,
    pool: str = POOL,
    owner: str = NPM,
    log_index: int = 12,
) -> dict:
    # Collect data: recipient (32B padded) | amount0 (uint128 padded) | amount1 (uint128 padded).
    return {
        "address": pool,
        "topics": [
            COLLECT_TOPIC,
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x" + _addr_topic(WALLET).removeprefix("0x") + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _receipt(logs: list[dict]) -> dict:
    return {"status": 1, "logs": logs}


# =============================================================================
# _nft_manager_address
# =============================================================================


class TestNftManagerAddress:
    def test_returns_canonical_npm_for_supported_chain(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        assert parser._nft_manager_address() == NPM.lower()

    def test_returns_empty_string_for_unsupported_chain(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="unknown-chain")
        assert parser._nft_manager_address() == ""


# =============================================================================
# _decreaseliquidity_token_id
# =============================================================================


class TestDecreaseLiquidityTokenId:
    def test_happy_path(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [_npm_decrease_log(token_id=42)]
        assert parser._decreaseliquidity_token_id(_receipt(logs)) == 42

    def test_returns_none_when_no_logs(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        assert parser._decreaseliquidity_token_id({"logs": []}) is None

    def test_returns_none_when_unsupported_chain(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="unknown-chain")
        logs = [_npm_decrease_log(token_id=42)]
        assert parser._decreaseliquidity_token_id(_receipt(logs)) is None

    def test_filters_non_npm_emitter(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [_npm_decrease_log(token_id=42, npm="0x" + "ee" * 20)]
        assert parser._decreaseliquidity_token_id(_receipt(logs)) is None

    def test_filters_non_decrease_topic(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        # Same shape, but with IncreaseLiquidity topic — must be ignored.
        log = _npm_decrease_log(token_id=42)
        log["topics"][0] = INCREASE_TOPIC
        assert parser._decreaseliquidity_token_id(_receipt([log])) is None

    def test_skips_log_with_one_topic_only(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        log = {"address": NPM, "topics": [DECREASE_TOPIC], "data": "0x"}
        assert parser._decreaseliquidity_token_id(_receipt([log])) is None

    def test_handles_bytes_topic_and_address(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": bytes.fromhex(NPM.replace("0x", "")),
            "topics": [
                bytes.fromhex(DECREASE_TOPIC.replace("0x", "")),
                bytes.fromhex(_addr_topic("0x" + format(99, "040x")).replace("0x", "")),
            ],
            "data": "0x",
        }
        assert parser._decreaseliquidity_token_id(_receipt([log])) == 99


# =============================================================================
# extract_registry_payload_open
# =============================================================================


class TestExtractRegistryPayloadOpen:
    """Happy + fail-closed branches for the open-side registry payload builder."""

    def test_happy_path_full_payload(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_log(
                token_id=42, liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14
            ),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs), fee_tier=500)
        assert out is not None
        assert out["token_id"] == "42"
        assert out["pool_address"] == POOL.lower()
        assert out["tick_lower"] == -100
        assert out["tick_upper"] == 100
        assert out["liquidity"] == str(10**18)
        assert out["amount0"] == "1000000"
        assert out["amount1"] == str(5 * 10**14)
        assert out["nft_manager_addr"] == NPM.lower()
        assert out["fee_tier"] == 500

    def test_happy_path_without_fee_tier(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_log(token_id=42),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs))
        assert out is not None
        # Empty != Zero — fee_tier key absent rather than substituting 0.
        assert "fee_tier" not in out

    def test_fee_tier_zero_or_negative_omitted(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_log(token_id=42),
        ]
        out_zero = parser.extract_registry_payload_open(_receipt(logs), fee_tier=0)
        out_neg = parser.extract_registry_payload_open(_receipt(logs), fee_tier=-1)
        assert out_zero is not None
        assert out_neg is not None
        assert "fee_tier" not in out_zero
        assert "fee_tier" not in out_neg

    def test_returns_none_when_extract_lp_open_data_returns_none(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        # Empty receipt → no IncreaseLiquidity → extract_lp_open_data returns None.
        assert parser.extract_registry_payload_open(_receipt([])) is None

    def test_returns_none_when_token_id_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=0,
                tick_lower=-100,
                tick_upper=100,
                liquidity=1,
                amount0=1,
                amount1=1,
                pool_address=POOL,
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_returns_none_when_token_id_negative(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=-5,
                tick_lower=-100,
                tick_upper=100,
                liquidity=1,
                amount0=1,
                amount1=1,
                pool_address=POOL,
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_returns_none_when_pool_address_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=42,
                tick_lower=-100,
                tick_upper=100,
                liquidity=1,
                amount0=1,
                amount1=1,
                pool_address="",
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_returns_none_when_tick_lower_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=42,
                tick_lower=None,
                tick_upper=100,
                liquidity=1,
                amount0=1,
                amount1=1,
                pool_address=POOL,
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_returns_none_when_tick_upper_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=42,
                tick_lower=-100,
                tick_upper=None,
                liquidity=1,
                amount0=1,
                amount1=1,
                pool_address=POOL,
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_returns_none_when_liquidity_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=42,
                tick_lower=-100,
                tick_upper=100,
                liquidity=None,
                amount0=1,
                amount1=1,
                pool_address=POOL,
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_returns_none_on_unsupported_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty NPM (unsupported chain) refuses to stamp a collision-prone hash."""
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="unknown-chain")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=42,
                tick_lower=-100,
                tick_upper=100,
                liquidity=1,
                amount0=1,
                amount1=1,
                pool_address=POOL,
            ),
        )
        assert parser.extract_registry_payload_open({"logs": []}) is None

    def test_amount0_amount1_none_emit_none_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty != Zero — None amounts stay as None in payload."""
        from almanak.framework.execution.extracted_data import LPOpenData

        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_open_data",
            lambda _r: LPOpenData(
                position_id=42,
                tick_lower=-100,
                tick_upper=100,
                liquidity=1,
                amount0=None,
                amount1=None,
                pool_address=POOL,
            ),
        )
        out = parser.extract_registry_payload_open({"logs": []})
        assert out is not None
        assert out["amount0"] is None
        assert out["amount1"] is None

    def test_token_labels_merged_when_symbols_set(self) -> None:
        """When the parser carries token symbol attrs, the payload picks them up."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        parser.token0_symbol = "WETH"  # type: ignore[attr-defined]
        parser.token1_symbol = "USDC"  # type: ignore[attr-defined]
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_log(token_id=42),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs))
        assert out is not None
        assert out["_token0_label"] == "WETH"
        assert out["_token1_label"] == "USDC"

    def test_token_labels_absent_when_symbols_unset(self) -> None:
        """Default Pancake parser has no symbol attrs — labels stay absent."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_log(token_id=42),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs))
        assert out is not None
        assert "_token0_label" not in out
        assert "_token1_label" not in out


# =============================================================================
# extract_registry_payload_close
# =============================================================================


def _make_lp_close(
    *,
    amount0_collected: int = 2_295_340,
    amount1_collected: int = 979_486_010_818_981,
    fees0: int = 14_368,
    fees1: int = 0,
    liquidity_removed: int | None = 1_042_017_676_194,
    pool_address: str = POOL,
    current_tick: int | None = None,
) -> SimpleNamespace:
    """Stand-in for ``LPCloseData`` — only the attributes the helpers read."""
    return SimpleNamespace(
        amount0_collected=amount0_collected,
        amount1_collected=amount1_collected,
        fees0=fees0,
        fees1=fees1,
        liquidity_removed=liquidity_removed,
        pool_address=pool_address,
        current_tick=current_tick,
    )


class TestExtractRegistryPayloadClose:
    def test_happy_path_receipt_only(self) -> None:
        """Burn + Collect + DecreaseLiquidity → full close payload, no open_payload."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _npm_decrease_log(token_id=42, liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14),
            _pool_burn_log(
                liquidity=10**18,
                amount0=1_000_000,
                amount1=5 * 10**14,
            ),
            _pool_collect_log(amount0=1_000_500, amount1=5 * 10**14 + 1_000),
        ]
        out = parser.extract_registry_payload_close(_receipt(logs))
        assert out is not None
        assert out["token_id"] == "42"
        assert out["pool_address"] == POOL.lower()
        # amount{0,1}_close = Collect totals as-emitted by parser.
        assert out["amount0_close"] == "1000500"
        assert out["amount1_close"] == str(5 * 10**14 + 1_000)
        # fees = collect - burn (clamped at zero).
        assert out["fee_owed_0"] == "500"
        assert out["fee_owed_1"] == "1000"
        assert out["nft_manager_addr"] == NPM.lower()
        # liquidity = burn-side liquidity_removed (no open_payload merge).
        assert out["liquidity"] == str(10**18)

    def test_returns_none_when_no_lp_close_data(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: None)
        assert parser.extract_registry_payload_close({"logs": []}) is None

    def test_returns_none_when_token_id_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: None)
        assert parser.extract_registry_payload_close({"logs": []}) is None

    def test_returns_none_when_token_id_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 0)
        assert parser.extract_registry_payload_close({"logs": []}) is None

    def test_returns_none_when_pool_address_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser,
            "extract_lp_close_data",
            lambda _r: _make_lp_close(pool_address=""),
        )
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        assert parser.extract_registry_payload_close({"logs": []}) is None

    def test_returns_none_on_unsupported_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="unknown-chain")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        assert parser.extract_registry_payload_close({"logs": []}) is None

    def test_returns_none_on_open_payload_token_id_disagreement(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Audit M1 — wrong open_payload token_id refuses the close."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        op = {"token_id": "9999999", "pool_address": POOL.lower()}
        assert parser.extract_registry_payload_close({"logs": []}, open_payload=op) is None

    def test_returns_none_on_open_payload_pool_disagreement(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Audit M1 — wrong open_payload pool_address refuses the close."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        op = {"token_id": "42", "pool_address": "0x" + "ff" * 20}
        assert parser.extract_registry_payload_close({"logs": []}, open_payload=op) is None

    def test_merges_open_side_ticks_and_amounts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        op = {
            "token_id": "42",
            "pool_address": POOL.lower(),
            "tick_lower": -199_740,
            "tick_upper": -197_740,
            "amount0": "3000000",
            "amount1": "1000000000000000",
            "liquidity": "9999999999999",
            "fee_tier": 500,
            "_token0_label": "USDC",
            "_token1_label": "WETH",
        }
        out = parser.extract_registry_payload_close({"logs": []}, open_payload=op)
        assert out is not None
        assert out["tick_lower"] == -199_740
        assert out["tick_upper"] == -197_740
        assert out["amount0_open"] == "3000000"
        assert out["amount1_open"] == "1000000000000000"
        # OPEN-time liquidity wins per the merge contract.
        assert out["liquidity"] == "9999999999999"
        assert out["fee_tier"] == 500
        assert out["_token0_label"] == "USDC"
        assert out["_token1_label"] == "WETH"

    def test_fee_tier_arg_used_when_open_payload_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        out = parser.extract_registry_payload_close({"logs": []}, fee_tier=500)
        assert out is not None
        assert out["fee_tier"] == 500

    def test_open_payload_fee_tier_wins_over_arg(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """setdefault — fee_tier arg does not override open_payload's value."""
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: _make_lp_close())
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 42)
        op = {"token_id": "42", "pool_address": POOL.lower(), "fee_tier": 100}
        out = parser.extract_registry_payload_close(
            {"logs": []}, open_payload=op, fee_tier=500
        )
        assert out is not None
        assert out["fee_tier"] == 100  # OPEN-side wins


# =============================================================================
# End-to-end smoke: real receipt-shaped logs → open + close payload
# =============================================================================


class TestEndToEnd:
    """Receipts built from log primitives flow through both extractors."""

    def test_open_then_close_roundtrip(self) -> None:
        parser = PancakeSwapV3ReceiptParser(chain="arbitrum")

        open_logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_log(
                token_id=42,
                liquidity=10**18,
                amount0=1_000_000,
                amount1=5 * 10**14,
            ),
        ]
        open_payload = parser.extract_registry_payload_open(_receipt(open_logs), fee_tier=500)
        assert open_payload is not None
        assert open_payload["token_id"] == "42"
        assert open_payload["pool_address"] == POOL.lower()

        close_logs = [
            _npm_decrease_log(
                token_id=42, liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14
            ),
            _pool_burn_log(liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14),
            _pool_collect_log(amount0=1_000_500, amount1=5 * 10**14 + 1_000),
        ]
        close_payload = parser.extract_registry_payload_close(
            _receipt(close_logs), open_payload=open_payload
        )
        assert close_payload is not None
        assert close_payload["token_id"] == "42"
        assert close_payload["pool_address"] == POOL.lower()
        # OPEN-side ticks merged in.
        assert close_payload["tick_lower"] == -100
        assert close_payload["tick_upper"] == 100
        # OPEN-time amounts merged as `_open` suffixed keys.
        assert close_payload["amount0_open"] == "1000000"
        assert close_payload["amount1_open"] == str(5 * 10**14)
        # OPEN-time liquidity wins over burn-side liquidity_removed.
        assert close_payload["liquidity"] == str(10**18)
        # fee_tier from OPEN-side.
        assert close_payload["fee_tier"] == 500


# =============================================================================
# Helper-import contract: the V3-fork-shared helpers are reused from Uniswap V3
# =============================================================================


class TestHelperImportContract:
    """Audit hook: prove the shared Uniswap V3 helpers are stable for the
    PancakeSwap V3 close-path reuse. If any of these helpers gets renamed /
    moved / signature-changed, this test points at the breakage immediately
    rather than waiting for an integration-level surprise.
    """

    def test_uniswap_v3_helpers_importable(self) -> None:
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        assert callable(UniswapV3ReceiptParser._open_payload_disagrees)
        assert callable(UniswapV3ReceiptParser._build_close_receipt_payload)
        assert callable(UniswapV3ReceiptParser._merge_open_payload_fields)

    def test_helper_signatures_accept_pancake_payload_shape(self) -> None:
        """Smoke test that the helpers accept the dict/object shapes we pass."""
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        # _open_payload_disagrees with None open_payload → never disagrees.
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=None,
                token_id=42,
                pool_address=POOL.lower(),
            )
            is False
        )

        # _build_close_receipt_payload accepts SimpleNamespace stand-in.
        out = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=42,
            pool_address=POOL.lower(),
            lp_close=_make_lp_close(),
            nft_manager_addr=NPM.lower(),
        )
        assert out["token_id"] == "42"

        # _merge_open_payload_fields is in-place; None open_payload is no-op.
        payload: dict[str, Any] = {"x": 1}
        UniswapV3ReceiptParser._merge_open_payload_fields(payload, None)
        assert payload == {"x": 1}


# ---------------------------------------------------------------------------
# Runner-side dispatch — the strategy_runner._registry_resolve_receipt_and_parser
# branch added for pancakeswap_v3 must route to PancakeSwapV3ReceiptParser (NOT
# the Uni V3 parser, whose NPM filter would drop every Pancake IncreaseLiquidity
# / DecreaseLiquidity log). CRAP-gate coverage for the dispatch block.
# ---------------------------------------------------------------------------


class TestRunnerPancakeDispatch:
    """Direct tests for `StrategyRunner._registry_resolve_receipt_and_parser`
    pancakeswap_v3 branch. Bypasses runner construction with a minimal stub.
    """

    def test_pancakeswap_v3_routes_to_pancake_parser(self) -> None:
        """protocol='pancakeswap_v3' returns PancakeSwapV3ReceiptParser,
        NOT UniswapV3ReceiptParser. Regression guard for the dispatch added
        on PR #2248.
        """
        from types import SimpleNamespace

        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
            PancakeSwapV3ReceiptParser,
        )
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )
        from almanak.framework.runner.strategy_runner import StrategyRunner

        receipt = {"logs": [{"topics": ["0xdeadbeef"], "address": "0x0", "data": "0x"}]}
        result = SimpleNamespace(receipts=[receipt])

        out = StrategyRunner._registry_resolve_receipt_and_parser(
            StrategyRunner.__new__(StrategyRunner),  # type: ignore[call-arg]
            result=result,
            chain="arbitrum",
            intent_type_str="LP_OPEN",
            protocol="pancakeswap_v3",
        )
        assert out is not None
        _receipt_out, parser = out
        assert isinstance(parser, PancakeSwapV3ReceiptParser)
        assert not isinstance(parser, UniswapV3ReceiptParser)

    def test_pancakeswap_v3_branch_returns_correct_chain(self) -> None:
        """The Pancake parser is constructed with the right chain so
        POSITION_MANAGER_ADDRESSES[chain] resolves to Pancake's NPM (not
        UV3's)."""
        from types import SimpleNamespace

        from almanak.framework.runner.strategy_runner import StrategyRunner

        receipt = {"logs": [{"topics": ["0xdeadbeef"], "address": "0x0", "data": "0x"}]}
        result = SimpleNamespace(receipts=[receipt])

        out = StrategyRunner._registry_resolve_receipt_and_parser(
            StrategyRunner.__new__(StrategyRunner),  # type: ignore[call-arg]
            result=result,
            chain="arbitrum",
            intent_type_str="LP_CLOSE",
            protocol="pancakeswap_v3",
        )
        assert out is not None
        _receipt_out, parser = out
        # Pancake NPM on Arbitrum (NOT UV3's 0xC36442b4...).
        assert parser._nft_manager_address().lower() == NPM.lower()


# ---------------------------------------------------------------------------
# backfill._nft_manager_for_protocol_chain — Pancake branch coverage.
# Mirrors the runner-dispatch test above; the backfill function and the
# runner function are the read-twin / write-twin for the (protocol, chain)
# → NPM map. CRAP gate would otherwise flag the new pancakeswap_v3 branch
# here too (cc=7, cov=10%).
# ---------------------------------------------------------------------------


class TestBackfillPancakeBranch:
    """`_nft_manager_for_protocol_chain('pancakeswap_v3', <chain>)` must
    return the Pancake NPM from `PANCAKESWAP_V3[chain]['nft']`, NOT UV3's
    NPM (which is what the legacy `_nft_manager_for_chain` returns for the
    same chain). Without this branch, every Pancake LP_OPEN's
    `physical_identity_hash` would mismatch on-chain truth and registry
    lookups would silently miss.
    """

    def test_returns_pancake_npm_for_arbitrum(self) -> None:
        from almanak.framework.migration.backfill import _nft_manager_for_protocol_chain

        out = _nft_manager_for_protocol_chain("pancakeswap_v3", "arbitrum")
        assert out is not None
        # Pancake NPM on Arbitrum (matches `POSITION_MANAGER_ADDRESSES["arbitrum"]`).
        assert out.lower() == NPM.lower()

    def test_pancake_npm_differs_from_uv3(self) -> None:
        """Regression guard: legacy `_nft_manager_for_chain` would return
        UV3's NPM on the same chain. The protocol-keyed path must return
        Pancake's instead."""
        from almanak.framework.migration.backfill import (
            _nft_manager_for_chain,
            _nft_manager_for_protocol_chain,
        )

        pancake_npm = _nft_manager_for_protocol_chain("pancakeswap_v3", "arbitrum")
        uv3_npm = _nft_manager_for_chain("arbitrum")
        assert pancake_npm is not None
        # If these are equal on a given chain it would be an upstream
        # coincidence (same registry address) — flagging visibly so the
        # follow-up audit catches it.
        assert pancake_npm.lower() != (uv3_npm or "").lower(), (
            f"Pancake NPM ({pancake_npm}) matched UV3 NPM ({uv3_npm}) on Arbitrum — "
            "either upstream contracts collided or the protocol-keyed branch is dead."
        )

    def test_returns_none_for_unknown_chain(self) -> None:
        from almanak.framework.migration.backfill import _nft_manager_for_protocol_chain

        # Unknown chain → None (NOT empty string, NOT a UV3 fallback).
        assert _nft_manager_for_protocol_chain("pancakeswap_v3", "not-a-real-chain") is None

    def test_returns_none_for_empty_inputs(self) -> None:
        from almanak.framework.migration.backfill import _nft_manager_for_protocol_chain

        # Empty protocol routes to the legacy _nft_manager_for_chain path
        # which itself returns None for unknown chains.
        assert _nft_manager_for_protocol_chain("", "not-a-real-chain") is None
        # Empty chain — same fallback.
        assert _nft_manager_for_protocol_chain("pancakeswap_v3", "") is None
