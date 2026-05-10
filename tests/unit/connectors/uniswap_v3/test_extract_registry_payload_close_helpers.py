"""Focused unit tests for the helpers extracted from
``UniswapV3ReceiptParser.extract_registry_payload_close`` (VIB-4198 / T12 + CRAP refactor).

The L2 contract test
(``tests/accounting/L2/test_univ3_ledger_registry_atomicity.py``) exercises the
full close path against the T08 goldens. This file pins the contract of each
extracted helper at unit grain so:

1. Coverage on ``extract_registry_payload_close`` (the orchestrator) +
   each helper sits at ~100%, which lands the function under the CRAP gate
   threshold (CRAP = cc² × (1 − cov) + cc — coverage is the multiplier).
2. A regression in any single helper localises to a focused failure
   rather than a downstream symptom on the L2 contract test.
3. The audit M1 contract — close MUST prove itself with DecreaseLiquidity
   on the receipt, NEVER from ``open_payload`` — has its cross-check
   guard tested in isolation.

Helper map:

- ``_open_payload_token_id_int`` — coerces ``open_payload['token_id']`` to int.
- ``_open_payload_disagrees`` — audit M1 cross-check.
- ``_build_close_receipt_payload`` — receipt-only base payload.
- ``_merge_open_payload_fields`` — OPEN-time field merge (in-place).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.connectors.uniswap_v3.receipt_parser import (
    UniswapV3ReceiptParser,
)


# ---------------------------------------------------------------------------
# _open_payload_token_id_int
# ---------------------------------------------------------------------------


class TestOpenPayloadTokenIdInt:
    """Coerce ``open_payload['token_id']`` to int or None."""

    def test_string_int_coerces(self) -> None:
        assert UniswapV3ReceiptParser._open_payload_token_id_int({"token_id": "42"}) == 42

    def test_int_passes_through(self) -> None:
        assert UniswapV3ReceiptParser._open_payload_token_id_int({"token_id": 42}) == 42

    def test_missing_key_is_none(self) -> None:
        assert UniswapV3ReceiptParser._open_payload_token_id_int({}) is None

    def test_none_value_is_none(self) -> None:
        assert UniswapV3ReceiptParser._open_payload_token_id_int({"token_id": None}) is None

    def test_empty_string_is_none(self) -> None:
        assert UniswapV3ReceiptParser._open_payload_token_id_int({"token_id": ""}) is None

    def test_non_numeric_string_is_none(self) -> None:
        # Garbage value — must not raise; coerce to None.
        assert UniswapV3ReceiptParser._open_payload_token_id_int({"token_id": "abc"}) is None

    def test_float_string_truncates_via_int(self) -> None:
        # Python int() refuses float-like strings — must return None, not raise.
        assert UniswapV3ReceiptParser._open_payload_token_id_int({"token_id": "42.5"}) is None


# ---------------------------------------------------------------------------
# _open_payload_disagrees — audit M1 cross-check
# ---------------------------------------------------------------------------


class TestOpenPayloadDisagrees:
    """Audit M1 (CodeRabbit): refuse close when open_payload's identity
    anchors disagree with the receipt-derived ones."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    TOKEN_ID = 5467895

    def test_none_open_payload_does_not_disagree(self) -> None:
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=None, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_matching_anchors_does_not_disagree(self) -> None:
        op = {"token_id": str(self.TOKEN_ID), "pool_address": self.POOL}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_token_id_mismatch_disagrees(self) -> None:
        op = {"token_id": "9999999", "pool_address": self.POOL}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is True
        )

    def test_pool_mismatch_disagrees(self) -> None:
        op = {"token_id": str(self.TOKEN_ID), "pool_address": "0x" + "00" * 20}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is True
        )

    def test_empty_open_pool_does_not_disagree(self) -> None:
        # When open_payload has no pool, only token_id is checked.
        op = {"token_id": str(self.TOKEN_ID), "pool_address": ""}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_missing_open_token_id_skips_token_check(self) -> None:
        # Missing token_id → only pool is checked. With matching pool → no
        # disagreement.
        op = {"pool_address": self.POOL}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_garbage_open_token_id_skips_token_check(self) -> None:
        # Non-coercible token_id → token check is skipped (treated as
        # "open_payload didn't carry a useful token_id"), pool check still
        # applies.
        op = {"token_id": "not-a-number", "pool_address": self.POOL}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_pool_case_normalized(self) -> None:
        # The receipt's pool_address is already lowercased by the caller
        # (lp_close.pool_address.lower()). The cross-check lowercases the
        # OPEN-side pool too — case difference doesn't trigger disagreement.
        op = {
            "token_id": str(self.TOKEN_ID),
            "pool_address": self.POOL.upper(),
        }
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )


# ---------------------------------------------------------------------------
# _build_close_receipt_payload
# ---------------------------------------------------------------------------


def _make_lp_close(
    *,
    amount0_collected: int = 2295340,
    amount1_collected: int = 979486010818981,
    fees0: int = 14368,
    fees1: int = 0,
    liquidity_removed: int | None = 1042017676194,
) -> SimpleNamespace:
    """Stand-in for ``LPCloseData`` — only the attributes the helpers read."""
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
        out = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["token_id"] == str(self.TOKEN_ID)
        assert out["pool_address"] == self.POOL
        assert out["nft_manager_addr"] == self.NPM
        # Audit m8 — amount{0,1}_close = collected as-emitted by parser.
        assert out["amount0_close"] == "2295340"
        assert out["amount1_close"] == "979486010818981"
        # Fees stay parallel, NOT subtracted.
        assert out["fee_owed_0"] == "14368"
        assert out["fee_owed_1"] == "0"
        assert out["liquidity"] == "1042017676194"

    def test_liquidity_removed_none_omits_liquidity_key(self) -> None:
        lp_close = _make_lp_close(liquidity_removed=None)
        out = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert "liquidity" not in out

    def test_zero_amounts_emit_zero_strings(self) -> None:
        # Empty != zero contract: the receipt-emitted zero is preserved as
        # the string "0" — never collapsed to None or absent-key.
        lp_close = _make_lp_close(
            amount0_collected=0, amount1_collected=0, fees0=0, fees1=0
        )
        out = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=self.NPM,
        )
        assert out["amount0_close"] == "0"
        assert out["amount1_close"] == "0"
        assert out["fee_owed_0"] == "0"
        assert out["fee_owed_1"] == "0"


# ---------------------------------------------------------------------------
# _merge_open_payload_fields
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
            "liquidity": "1042017676194",  # close-side liquidity_removed
        }

    def test_none_open_payload_is_noop(self, base_close_payload: dict[str, Any]) -> None:
        before = dict(base_close_payload)
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, None)
        assert base_close_payload == before

    def test_ticks_merged(self, base_close_payload: dict[str, Any]) -> None:
        op = {"tick_lower": -199740, "tick_upper": -197740}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["tick_lower"] == -199740
        assert base_close_payload["tick_upper"] == -197740

    def test_ticks_dont_overwrite_when_already_present(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        # Defensive: if the close payload somehow already has ticks (future
        # parser change), the merge should NOT overwrite. Setdefault-style
        # merge.
        base_close_payload["tick_lower"] = 0
        base_close_payload["tick_upper"] = 100
        op = {"tick_lower": -199740, "tick_upper": -197740}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["tick_lower"] == 0
        assert base_close_payload["tick_upper"] == 100

    def test_amount_open_fields_added(self, base_close_payload: dict[str, Any]) -> None:
        op = {"amount0": "3000000", "amount1": "1000000000000000"}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["amount0_open"] == "3000000"
        assert base_close_payload["amount1_open"] == "1000000000000000"

    def test_open_liquidity_overrides_close_liquidity(
        self, base_close_payload: dict[str, Any]
    ) -> None:
        # Per docstring: OPEN-time liquidity wins. The base close payload's
        # close-side liquidity_removed is overwritten by the OPEN-time
        # mint amount.
        assert base_close_payload["liquidity"] == "1042017676194"
        op = {"liquidity": "9999999999999"}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["liquidity"] == "9999999999999"

    def test_fee_tier_merged(self, base_close_payload: dict[str, Any]) -> None:
        op = {"fee_tier": 500}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["fee_tier"] == 500

    def test_token_labels_merged(self, base_close_payload: dict[str, Any]) -> None:
        op = {"_token0_label": "USDC", "_token1_label": "WETH"}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert base_close_payload["_token0_label"] == "USDC"
        assert base_close_payload["_token1_label"] == "WETH"

    def test_falsy_label_skipped(self, base_close_payload: dict[str, Any]) -> None:
        # An empty-string / None label MUST NOT be merged in (Empty != zero
        # — but empty label is also not a useful annotation).
        op = {"_token0_label": "", "_token1_label": None}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert "_token0_label" not in base_close_payload
        assert "_token1_label" not in base_close_payload

    def test_none_amount_skipped(self, base_close_payload: dict[str, Any]) -> None:
        # ``None`` amount0 / amount1 in open_payload (Empty != zero) must
        # not get carried as an OPEN-side field.
        op = {"amount0": None, "amount1": None}
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        assert "amount0_open" not in base_close_payload
        assert "amount1_open" not in base_close_payload

    def test_full_open_payload_merge(self, base_close_payload: dict[str, Any]) -> None:
        # T08-shaped open_payload → exercises every merge path.
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
        UniswapV3ReceiptParser._merge_open_payload_fields(base_close_payload, op)
        for k, v in op.items():
            target_key = (
                "amount0_open"
                if k == "amount0"
                else "amount1_open"
                if k == "amount1"
                else k
            )
            assert base_close_payload[target_key] == v


# ---------------------------------------------------------------------------
# Orchestrator integration — close path with refactored helpers
# ---------------------------------------------------------------------------


class TestExtractRegistryPayloadCloseOrchestrator:
    """High-level checks that the orchestrator wires the helpers correctly."""

    def test_returns_none_when_no_lp_close_data(self, monkeypatch) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        # Monkey-patch ``extract_lp_close_data`` to return None — the
        # orchestrator must short-circuit before touching any helper.
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: None)
        out = parser.extract_registry_payload_close({"logs": []})
        assert out is None

    def test_returns_none_when_token_id_missing(self, monkeypatch) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        # lp_close present, DecreaseLiquidity log absent → token_id None.
        monkeypatch.setattr(
            parser,
            "extract_lp_close_data",
            lambda _r: _make_lp_close(),
        )
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: None)
        out = parser.extract_registry_payload_close({"logs": []})
        assert out is None

    def test_returns_none_when_token_id_zero(self, monkeypatch) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        monkeypatch.setattr(
            parser, "extract_lp_close_data", lambda _r: _make_lp_close()
        )
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 0)
        out = parser.extract_registry_payload_close({"logs": []})
        assert out is None

    def test_returns_none_when_pool_address_empty(self, monkeypatch) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        # pool_address coming from extract_lp_close_data is empty.
        lp_close = _make_lp_close()
        # Replace the SimpleNamespace's pool_address with empty.
        lp_close.pool_address = ""  # type: ignore[attr-defined]
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: lp_close)
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 5467895)
        out = parser.extract_registry_payload_close({"logs": []})
        assert out is None

    def test_returns_none_on_open_payload_disagreement(self, monkeypatch) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        lp_close = _make_lp_close()
        lp_close.pool_address = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"  # type: ignore[attr-defined]
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: lp_close)
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 5467895)
        # open_payload supplies the wrong token_id.
        op = {"token_id": "9999999", "pool_address": lp_close.pool_address}
        out = parser.extract_registry_payload_close({"logs": []}, open_payload=op)
        assert out is None

    def test_fee_tier_argument_used_when_open_payload_absent(self, monkeypatch) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        lp_close = _make_lp_close()
        lp_close.pool_address = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"  # type: ignore[attr-defined]
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: lp_close)
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 5467895)
        out = parser.extract_registry_payload_close({"logs": []}, fee_tier=500)
        assert out is not None
        assert out["fee_tier"] == 500

    def test_fee_tier_argument_does_not_override_open_payload_fee_tier(
        self, monkeypatch
    ) -> None:
        # When open_payload carries fee_tier, the merge runs FIRST. The
        # orchestrator's fee_tier argument is ``setdefault`` — it does not
        # override.
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        lp_close = _make_lp_close()
        lp_close.pool_address = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"  # type: ignore[attr-defined]
        monkeypatch.setattr(parser, "extract_lp_close_data", lambda _r: lp_close)
        monkeypatch.setattr(parser, "_decreaseliquidity_token_id", lambda _r: 5467895)
        op = {
            "token_id": "5467895",
            "pool_address": lp_close.pool_address,
            "fee_tier": 100,
        }
        out = parser.extract_registry_payload_close(
            {"logs": []}, open_payload=op, fee_tier=500
        )
        assert out is not None
        assert out["fee_tier"] == 100  # OPEN-side wins
