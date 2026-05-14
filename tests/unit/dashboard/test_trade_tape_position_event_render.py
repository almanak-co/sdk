"""PositionValueFixes — trade-tape kv-block rendering for the Linked
position event panel.

These tests pin five operator-visible behaviours surfaced during the
2026-05-13 live ``lp_dual`` audit:

* **Bug 2** — ``_filter_position_event_fields`` drops PERP-only columns
  on LP rows and vice-versa (the unified ``position_events`` table has
  one half NULL per row; rendering them as blanks confused operators).
* **Bug 4** — ``_format_scalar_kv_value`` hides empty
  ``protocol_fees_usd`` for OPEN/ADJUST events (no fees by definition)
  and renders "unmeasured" for other events; **never** substitutes 0
  (AGENTS §Accounting "Empty ≠ Zero").
* **Bug 5** — ``_format_scalar_kv_value`` scales raw integer LP amounts
  via the token resolver, returning the human value plus the token
  symbol so the same logical amount no longer renders raw on one panel
  and scaled on another panel of the same card.
* **Bug 6** — ``_registry_handle_from_payload`` extracts the strategy-
  stamped handle (``leg_narrow`` / ``leg_wide``) from the typed payload
  so the headline can distinguish multi-position LP legs.

Bug 7 (st.json widget for nested values) is a rendering-only behaviour
that's exercised by the integration path; it's covered by the
``test_kv_block_splits_nested_into_st_json`` assertion that nested
values go through the ``st`` widget rather than the inline HTML block.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard.pages.trade_tape import (
    KVContext,
    _filter_position_event_fields,
    _format_lp_ledger_amount,
    _format_scalar_kv_value,
    _registry_handle_from_payload,
)


class TestFilterPositionEventFields:
    """Bug 2 — LP / PERP fields rendered per row's ``position_type``."""

    def test_lp_row_keeps_lp_fields_and_drops_perp(self) -> None:
        row = {
            "position_type": "LP",
            "position_id": "5487862",
            "token0": "WETH",
            "amount0": "871720086157647",
            # PERP-only columns must be dropped from an LP row.
            "leverage": None,
            "entry_price": None,
            "mark_price": None,
            "is_long": None,
            "unrealized_pnl": None,
        }
        out = _filter_position_event_fields(row)
        assert "position_type" in out
        assert "amount0" in out
        assert "token0" in out
        for perp_only in ("leverage", "entry_price", "mark_price", "is_long", "unrealized_pnl"):
            assert perp_only not in out, f"{perp_only} must not survive on an LP row"

    def test_perp_row_keeps_perp_fields_and_drops_lp(self) -> None:
        row = {
            "position_type": "PERP",
            "position_id": "perp-1",
            "leverage": "5.0",
            "entry_price": "2000",
            "amount0": None,
            "tick_lower": None,
            "in_range": None,
        }
        out = _filter_position_event_fields(row)
        assert out["leverage"] == "5.0"
        assert out["entry_price"] == "2000"
        for lp_only in ("amount0", "tick_lower", "in_range"):
            assert lp_only not in out

    def test_unknown_position_type_passes_through(self) -> None:
        # Future / unrecognised position_type values should NOT silently
        # drop fields — better to render the noise than hide unfamiliar
        # data from the operator until the schema evolves.
        row = {"position_type": "STAKE", "amount0": "1", "leverage": "x"}
        out = _filter_position_event_fields(row)
        assert out == row


class TestProtocolFeesUsdRender:
    """Bug 4 — Empty ≠ Zero, with event-type-aware hiding for OPEN/ADJUST."""

    @pytest.mark.parametrize("event_type", ["OPEN", "ADJUST", "LP_OPEN", "PERP_OPEN"])
    def test_empty_hidden_for_open_adjust_events(self, event_type: str) -> None:
        ctx = KVContext(event_type=event_type)
        assert _format_scalar_kv_value("protocol_fees_usd", "", ctx) is None
        assert _format_scalar_kv_value("protocol_fees_usd", None, ctx) is None

    def test_empty_labelled_unmeasured_for_other_events(self) -> None:
        ctx = KVContext(event_type="LP_COLLECT_FEES")
        out = _format_scalar_kv_value("protocol_fees_usd", "", ctx)
        assert out is not None
        # Hard invariant: the OPERATOR-VISIBLE text must equal exactly
        # ``"unmeasured"`` — never a digit. The previous assertion
        # (``"0" not in out or "unmeasured" in out``) was vacuously
        # true because the right operand always passed; a regression
        # rendering ``"0 unmeasured"`` would have slipped through.
        # Strip all HTML attributes/tags (incl. ``title='Empty ≠ Zero'``
        # which contains the literal "Zero" word but no digit) and
        # check the visible text is exactly the unmeasured chip.
        visible = re.sub(r"<[^>]+>", "", out).strip()
        assert visible == "unmeasured", (
            f"Empty protocol_fees_usd rendered visible text {visible!r}; "
            "AGENTS Empty ≠ Zero requires exactly 'unmeasured'."
        )

    def test_measured_zero_passes_through(self) -> None:
        ctx = KVContext(event_type="LP_OPEN")
        # Decimal("0") / "0" / "0.00" are MEASURED zero — these must
        # render the value, not be hidden.
        for v in ("0", "0.00", "0.0000"):
            out = _format_scalar_kv_value("protocol_fees_usd", v, ctx)
            assert out is not None
            assert "unmeasured" not in out


class TestLPAmountScaling:
    """Bug 5 — raw on-chain integer amounts scaled via the token resolver."""

    @pytest.fixture
    def resolver_mock(self) -> Any:
        def _resolve(symbol: str, chain: str = "") -> Any:
            decimals_map = {"WETH": 18, "USDC": 6}
            if symbol in decimals_map:
                info = MagicMock()
                info.decimals = decimals_map[symbol]
                return info
            return None

        mock = MagicMock(resolve=MagicMock(side_effect=_resolve))
        return mock

    def test_amount0_scaled_with_token0_decimals(self, resolver_mock: Any) -> None:
        ctx = KVContext(event_type="LP_OPEN", chain="arbitrum", token0="WETH", token1="USDC")
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=resolver_mock,
        ):
            out = _format_scalar_kv_value("amount0", "871720086157647", ctx)
        # 871720086157647 / 10**18 ≈ 0.0008717 — the headline-chip value
        # the operator already sees on the same card.
        assert "0.0008717" in out
        assert "WETH" in out

    def test_amount1_scaled_with_token1_decimals(self, resolver_mock: Any) -> None:
        ctx = KVContext(event_type="LP_OPEN", chain="arbitrum", token0="WETH", token1="USDC")
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=resolver_mock,
        ):
            out = _format_scalar_kv_value("amount1", "2402098", ctx)
        # 2402098 / 10**6 = 2.402098 → "2.40" (≥1 path)
        assert "2.40" in out
        assert "USDC" in out

    def test_falls_back_to_raw_when_decimals_unknown(self) -> None:
        # No resolver → raw integer passes through via _format_value.
        ctx = KVContext(event_type="LP_OPEN", chain="arbitrum", token0="ZZZ", token1="QQQ")

        empty_resolver = MagicMock(resolve=MagicMock(return_value=None))
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=empty_resolver,
        ):
            out = _format_scalar_kv_value("amount0", "871720086157647", ctx)
        assert "871720086157647" in out

    def test_non_amount_keys_unaffected(self, resolver_mock: Any) -> None:
        ctx = KVContext(event_type="LP_OPEN", chain="arbitrum", token0="WETH", token1="USDC")
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=resolver_mock,
        ):
            # ``position_id`` is also a large integer string but should
            # NOT be scaled — only the configured LP_AMOUNT_FIELDS_TOKEN*
            # keys go through the resolver.
            out = _format_scalar_kv_value("position_id", "5487862", ctx)
        assert "5487862" in out
        assert "WETH" not in out


class TestRegistryHandleFromPayload:
    """Bug 6 — surface the strategy-stamped handle on the row headline."""

    def test_extracts_registry_handle_when_present(self) -> None:
        payload = (
            '{"position_reference": {"registry_handle": "leg_wide", '
            '"semantic_grouping_key": "arbitrum:0xc69..."}}'
        )
        assert _registry_handle_from_payload(payload) == "leg_wide"

    def test_returns_empty_when_no_payload(self) -> None:
        assert _registry_handle_from_payload("") == ""
        assert _registry_handle_from_payload("null") == ""

    def test_returns_empty_when_no_position_reference(self) -> None:
        assert _registry_handle_from_payload('{"event_type": "LP_OPEN"}') == ""

    def test_returns_empty_when_no_registry_handle(self) -> None:
        assert _registry_handle_from_payload('{"position_reference": {"semantic_grouping_key": "x"}}') == ""

    def test_returns_empty_on_malformed_payload(self) -> None:
        assert _registry_handle_from_payload("{not json") == ""


class TestLPLedgerAmountFallback:
    """Regression: ledger ``amount_in/out`` carries EITHER raw ints (from
    ``LPOpenData``) OR human ``Decimal`` (intent fallback) with no
    discriminator. ``_format_lp_ledger_amount`` must not 10**decimals-scale
    a human Decimal — that turned a 1 WETH intent fallback into
    ``"1e-18 WETH"`` on operator panels (audit Blocker A pre-merge).
    """

    @pytest.fixture
    def resolver_mock(self) -> Any:
        def _resolve(symbol: str, chain: str = "") -> Any:
            decimals_map = {"WETH": 18, "USDC": 6, "WBTC": 8}
            if symbol in decimals_map:
                info = MagicMock()
                info.decimals = decimals_map[symbol]
                return info
            return None

        return MagicMock(resolve=MagicMock(side_effect=_resolve))

    def test_human_decimal_fallback_does_not_get_rescaled(self, resolver_mock: Any) -> None:
        # Hard invariant: ``Decimal("1")`` (1 WETH human, intent fallback
        # path when ``LPOpenData`` is missing) MUST NOT render as
        # ``"1e-18 WETH"``. Any rendered value visibly < 1 here would
        # mean the operator sees a position size off by 10**18.
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=resolver_mock,
        ):
            out = _format_lp_ledger_amount("1", "WETH", "arbitrum")
        assert "1e-18" not in out
        assert "0." not in out  # nothing should look sub-1
        assert "1" in out  # the value 1 must still appear

    def test_raw_integer_above_threshold_still_scales(self, resolver_mock: Any) -> None:
        # 871720086157647 / 10**18 ≈ 0.0008717 — must still scale, the
        # whole reason this helper exists.
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=resolver_mock,
        ):
            out = _format_lp_ledger_amount("871720086157647", "WETH", "arbitrum")
        assert "0.0008717" in out

    def test_human_decimal_string_passes_through(self, resolver_mock: Any) -> None:
        # ``"0.5"`` is a non-integral Decimal — even if a token's decimals
        # are known, it must NOT be scaled.
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=resolver_mock,
        ):
            out = _format_lp_ledger_amount("0.5", "WETH", "arbitrum")
        assert "0.5" in out
        assert "5e-19" not in out
