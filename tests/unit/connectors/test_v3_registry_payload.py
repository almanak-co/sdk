"""Unit tests for ``almanak.connectors._strategy_base.v3_registry_payload``.

Tests the canonical implementations of the four LP_CLOSE registry-payload
helpers that were promoted from ``UniswapV3ReceiptParser`` to the shared
``_strategy_base`` module (plan 014).

The per-parser tests (uniswap_v3/test_extract_registry_payload_close_helpers.py,
etc.) now exercise the delegate methods; this file pins the canonical behaviour
of the module-level functions directly so regressions localise here rather than
surfacing as downstream connector failures.

Cover at minimum (per plan 014 Step 6):
- token-id coercion (None / "" / "12" / "abc" / 12)
- disagrees (open_payload=None -> False; matching anchors -> False;
  token-id mismatch -> True; pool mismatch -> True)
- close payload (fees None -> JSON null not "None"; liquidity_removed=None ->
  no liquidity key)
- merge (None no-op; ticks only when absent; amount{0,1}_open setdefault;
  OPEN-time liquidity overwrites; fee_tier setdefault; label setdefault)
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors._strategy_base.v3_registry_payload import (
    build_close_receipt_payload,
    merge_open_payload_fields,
    open_payload_disagrees,
    open_payload_token_id_int,
)


# ---------------------------------------------------------------------------
# open_payload_token_id_int
# ---------------------------------------------------------------------------


class TestOpenPayloadTokenIdInt:
    """Coerce ``open_payload['token_id']`` to int or None."""

    def test_string_int_coerces(self) -> None:
        assert open_payload_token_id_int({"token_id": "12"}) == 12

    def test_int_passes_through(self) -> None:
        assert open_payload_token_id_int({"token_id": 12}) == 12

    def test_missing_key_is_none(self) -> None:
        assert open_payload_token_id_int({}) is None

    def test_none_value_is_none(self) -> None:
        assert open_payload_token_id_int({"token_id": None}) is None

    def test_empty_string_is_none(self) -> None:
        assert open_payload_token_id_int({"token_id": ""}) is None

    def test_non_numeric_string_is_none(self) -> None:
        assert open_payload_token_id_int({"token_id": "abc"}) is None

    def test_float_string_is_none(self) -> None:
        # Python int() refuses float-like strings.
        assert open_payload_token_id_int({"token_id": "42.5"}) is None

    def test_zero_is_zero(self) -> None:
        assert open_payload_token_id_int({"token_id": "0"}) == 0

    def test_large_token_id(self) -> None:
        assert open_payload_token_id_int({"token_id": "999999999"}) == 999999999


# ---------------------------------------------------------------------------
# open_payload_disagrees - audit M1 cross-check
# ---------------------------------------------------------------------------


class TestOpenPayloadDisagrees:
    """Audit M1: refuse close when open_payload's identity anchors disagree."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    TOKEN_ID = 5467895

    def test_none_open_payload_does_not_disagree(self) -> None:
        assert (
            open_payload_disagrees(
                open_payload=None, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_matching_anchors_does_not_disagree(self) -> None:
        op = {"token_id": str(self.TOKEN_ID), "pool_address": self.POOL}
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_token_id_mismatch_disagrees(self) -> None:
        op = {"token_id": "9999999", "pool_address": self.POOL}
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is True
        )

    def test_pool_mismatch_disagrees(self) -> None:
        op = {"token_id": str(self.TOKEN_ID), "pool_address": "0x" + "00" * 20}
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is True
        )

    def test_empty_open_pool_does_not_disagree(self) -> None:
        # When open_payload has no pool, only token_id is checked.
        op = {"token_id": str(self.TOKEN_ID), "pool_address": ""}
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_missing_open_token_id_skips_token_check(self) -> None:
        op = {"pool_address": self.POOL}
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_garbage_open_token_id_skips_token_check(self) -> None:
        op = {"token_id": "not-a-number", "pool_address": self.POOL}
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_pool_case_normalized(self) -> None:
        # The function lowercases the OPEN-side pool for comparison.
        op = {
            "token_id": str(self.TOKEN_ID),
            "pool_address": self.POOL.upper(),
        }
        assert (
            open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )


# ---------------------------------------------------------------------------
# build_close_receipt_payload
# ---------------------------------------------------------------------------


def _make_lp_close(
    *,
    amount0_collected: int | None = 2295340,
    amount1_collected: int | None = 979486010818981,
    fees0: int | None = 14368,
    fees1: int | None = 0,
    liquidity_removed: int | None = 1042017676194,
) -> SimpleNamespace:
    """Stand-in for ``LPCloseData`` - only the attributes the helper reads."""
    return SimpleNamespace(
        amount0_collected=amount0_collected,
        amount1_collected=amount1_collected,
        fees0=fees0,
        fees1=fees1,
        liquidity_removed=liquidity_removed,
    )


class TestBuildCloseReceiptPayload:
    """Receipt-only portion of the close payload (T08 golden contract)."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    NPM = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
    TOKEN_ID = 5467895

    def test_keys_and_amount_decimals_match_t08_contract(self) -> None:
        lp_close = _make_lp_close()
        out = build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["token_id"] == str(self.TOKEN_ID)
        assert out["pool_address"] == self.POOL
        assert out["nft_manager_addr"] == self.NPM
        # Audit m8 - amount{0,1}_close = collected as-emitted by parser.
        assert out["amount0_close"] == "2295340"
        assert out["amount1_close"] == "979486010818981"
        # Fees stay parallel, NOT subtracted.
        assert out["fee_owed_0"] == "14368"
        assert out["fee_owed_1"] == "0"
        assert out["liquidity"] == "1042017676194"

    def test_liquidity_removed_none_omits_liquidity_key(self) -> None:
        lp_close = _make_lp_close(liquidity_removed=None)
        out = build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert "liquidity" not in out

    def test_fees_none_emits_json_null_not_string_none(self) -> None:
        # VIB-4470: None fees must be JSON null (Python None), NOT "None".
        lp_close = _make_lp_close(fees0=None, fees1=None)
        out = build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["fee_owed_0"] is None
        assert out["fee_owed_1"] is None
        # Confirm it is NOT the string "None" (the pre-VIB-4470 bug).
        assert out["fee_owed_0"] != "None"
        assert out["fee_owed_1"] != "None"

    def test_zero_fees_emit_zero_strings(self) -> None:
        # Empty != zero: measured-zero emits "0", not None.
        lp_close = _make_lp_close(fees0=0, fees1=0)
        out = build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["fee_owed_0"] == "0"
        assert out["fee_owed_1"] == "0"

    def test_zero_amounts_emit_zero_strings(self) -> None:
        lp_close = _make_lp_close(amount0_collected=0, amount1_collected=0)
        out = build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["amount0_close"] == "0"
        assert out["amount1_close"] == "0"

    def test_none_amount_emits_json_null_not_string_none(self) -> None:
        # VIB-5117: an unmeasured native principal leg (None) must serialise as
        # JSON null (Python None), NOT the literal string "None" — symmetric with
        # the fee_owed guard. Distinct from a measured-zero leg → "0".
        lp_close = _make_lp_close(amount0_collected=None, amount1_collected=0)
        out = build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["amount0_close"] is None
        assert out["amount0_close"] != "None"
        assert out["amount1_close"] == "0"  # measured zero stays distinguishable


# ---------------------------------------------------------------------------
# merge_open_payload_fields
# ---------------------------------------------------------------------------


class TestMergeOpenPayloadFields:
    """OPEN-time field merge onto a close payload (in-place)."""

    @pytest.fixture
    def base_close_payload(self) -> dict[str, Any]:
        return {
            "token_id": "5467895",
            "pool_address": "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443",
            "amount0_close": "2295340",
            "amount1_close": "979486010818981",
            "fee_owed_0": "14368",
            "fee_owed_1": "0",
            "nft_manager_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
            "liquidity": "1042017676194",
        }

    def test_none_open_payload_is_noop(self, base_close_payload: dict[str, Any]) -> None:
        before = dict(base_close_payload)
        merge_open_payload_fields(base_close_payload, None)
        assert base_close_payload == before

    def test_ticks_merged_when_absent(self, base_close_payload: dict[str, Any]) -> None:
        op = {"tick_lower": -199740, "tick_upper": -197740}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["tick_lower"] == -199740
        assert base_close_payload["tick_upper"] == -197740

    def test_ticks_not_overwritten_when_already_present(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        base_close_payload["tick_lower"] = 0
        base_close_payload["tick_upper"] = 100
        op = {"tick_lower": -199740, "tick_upper": -197740}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["tick_lower"] == 0
        assert base_close_payload["tick_upper"] == 100

    def test_amount_open_fields_added(self, base_close_payload: dict[str, Any]) -> None:
        op = {"amount0": "3000000", "amount1": "1000000000000000"}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["amount0_open"] == "3000000"
        assert base_close_payload["amount1_open"] == "1000000000000000"

    def test_amount_open_setdefault_does_not_overwrite(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        # setdefault semantics: if already set, keep existing value.
        base_close_payload["amount0_open"] = "existing_value"
        op = {"amount0": "new_value", "amount1": "1000000000000000"}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["amount0_open"] == "existing_value"

    def test_open_liquidity_overwrites_close_liquidity(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        # Per docstring: OPEN-time liquidity wins.
        assert base_close_payload["liquidity"] == "1042017676194"
        op = {"liquidity": "9999999999999"}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["liquidity"] == "9999999999999"

    def test_fee_tier_setdefault(self, base_close_payload: dict[str, Any]) -> None:
        op = {"fee_tier": 500}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["fee_tier"] == 500

    def test_fee_tier_does_not_overwrite_existing(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        base_close_payload["fee_tier"] = 100
        op = {"fee_tier": 500}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["fee_tier"] == 100

    def test_token_labels_merged(self, base_close_payload: dict[str, Any]) -> None:
        op = {"_token0_label": "USDC", "_token1_label": "WETH"}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["_token0_label"] == "USDC"
        assert base_close_payload["_token1_label"] == "WETH"

    def test_label_setdefault_does_not_overwrite_existing(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        base_close_payload["_token0_label"] = "existing"
        op = {"_token0_label": "new"}
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["_token0_label"] == "existing"

    def test_falsy_label_skipped(self, base_close_payload: dict[str, Any]) -> None:
        # Empty-string / None labels must NOT be merged (not useful annotations).
        op = {"_token0_label": "", "_token1_label": None}
        merge_open_payload_fields(base_close_payload, op)
        assert "_token0_label" not in base_close_payload
        assert "_token1_label" not in base_close_payload

    def test_none_amount_skipped(self, base_close_payload: dict[str, Any]) -> None:
        # None amount in open_payload (Empty != zero) must not be carried.
        op = {"amount0": None, "amount1": None}
        merge_open_payload_fields(base_close_payload, op)
        assert "amount0_open" not in base_close_payload
        assert "amount1_open" not in base_close_payload

    def test_none_tick_skipped(self, base_close_payload: dict[str, Any]) -> None:
        # None tick values must not be merged.
        op = {"tick_lower": None, "tick_upper": None}
        merge_open_payload_fields(base_close_payload, op)
        assert "tick_lower" not in base_close_payload
        assert "tick_upper" not in base_close_payload

    def test_full_open_payload_merge(self, base_close_payload: dict[str, Any]) -> None:
        # T08-shaped open_payload - exercises every merge path.
        op = {
            "tick_lower": -199740,
            "tick_upper": -197740,
            "amount0": "3000000",
            "amount1": "1000000000000000",
            "liquidity": "1042017676194",
            "fee_tier": 500,
            "_token0_label": "USDC",
            "_token1_label": "WETH",
        }
        merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["tick_lower"] == -199740
        assert base_close_payload["tick_upper"] == -197740
        assert base_close_payload["amount0_open"] == "3000000"
        assert base_close_payload["amount1_open"] == "1000000000000000"
        assert base_close_payload["liquidity"] == "1042017676194"
        assert base_close_payload["fee_tier"] == 500
        assert base_close_payload["_token0_label"] == "USDC"
        assert base_close_payload["_token1_label"] == "WETH"
