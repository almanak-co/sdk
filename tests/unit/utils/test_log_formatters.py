"""Unit tests for almanak.framework.utils.log_formatters.

These tests exercise every formatter branch: happy paths, edge cases
(None / zero / very small / very large / negative), precision boundaries,
emoji-vs-plain output paths, and wei/human conversions.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.utils import log_formatters as lf
from almanak.framework.utils.log_formatters import (
    _emojis_enabled,
    format_address,
    format_balance_delta,
    format_balance_summary,
    format_error,
    format_execution_status,
    format_gas_cost,
    format_health_factor,
    format_info,
    format_intent_type_emoji,
    format_leverage,
    format_percentage,
    format_price,
    format_slippage,
    format_slippage_bps,
    format_token_amount,
    format_token_amount_human,
    format_token_with_usd,
    format_token_with_usd_human,
    format_tx_hash,
    format_usd,
    format_warning,
    human_to_wei,
    wei_to_human,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def emojis_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force emoji mode on."""
    monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "true")


@pytest.fixture
def emojis_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force emoji mode off."""
    monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "false")


# ---------------------------------------------------------------------------
# format_usd
# ---------------------------------------------------------------------------


class TestFormatUsd:
    def test_none_returns_na(self):
        assert format_usd(None) == "N/A"

    def test_standard_float(self):
        assert format_usd(1234.56) == "$1,234.56"

    def test_decimal(self):
        assert format_usd(Decimal("0.01")) == "$0.01"

    def test_large_int(self):
        assert format_usd(1_000_000) == "$1,000,000.00"

    def test_zero(self):
        # Zero is exactly 0 and not < 0.01 (equal), so it hits zero branch and standard path
        assert format_usd(0) == "$0.00"

    def test_very_small_positive(self):
        # Uses 6-decimal micro-format
        assert format_usd(Decimal("0.000123")) == "$0.000123"

    def test_very_small_negative(self):
        # abs() < 0.01 and non-zero -> micro format
        assert format_usd(Decimal("-0.000050")) == "$-0.000050"

    def test_negative_standard(self):
        assert format_usd(Decimal("-1234.56")) == "$-1,234.56"

    def test_rounds_to_two_decimals(self):
        # Decimal format-spec uses banker's rounding (ROUND_HALF_EVEN): 1.235 -> 1.24.
        assert format_usd(Decimal("1.235")) == "$1.24"


# ---------------------------------------------------------------------------
# format_token_amount (wei -> human)
# ---------------------------------------------------------------------------


class TestFormatTokenAmount:
    def test_none_returns_na_with_symbol(self):
        assert format_token_amount(None, "ETH", 18) == "N/A ETH"

    def test_standard_eth(self):
        assert format_token_amount(1_500_000_000_000_000_000, "ETH", 18) == "1.5000 ETH"

    def test_usdc_6_decimals(self):
        assert format_token_amount(100_000_000, "USDC", 6) == "100.0000 USDC"

    def test_zero_amount_returns_zero_no_decimals(self):
        assert format_token_amount(0, "ETH", 18) == "0 ETH"

    def test_very_small_amount_uses_8_decimals(self):
        # 1e10 wei / 1e18 = 1e-8 -> < 0.0001 threshold
        assert format_token_amount(10_000_000_000, "ETH", 18) == "0.00000001 ETH"

    def test_custom_max_decimals(self):
        assert format_token_amount(1_500_000_000_000_000_000, "ETH", 18, max_decimals=2) == "1.50 ETH"

    def test_negative_amount_standard(self):
        assert format_token_amount(-1_500_000_000_000_000_000, "ETH", 18) == "-1.5000 ETH"

    def test_negative_small_amount_uses_8_decimals(self):
        # abs() hits small threshold
        assert format_token_amount(-10_000_000_000, "ETH", 18) == "-0.00000001 ETH"

    def test_decimal_amount_input(self):
        assert format_token_amount(Decimal("2000000000000000000"), "ETH", 18) == "2.0000 ETH"


# ---------------------------------------------------------------------------
# format_token_amount_human (already-human units)
# ---------------------------------------------------------------------------


class TestFormatTokenAmountHuman:
    def test_none(self):
        assert format_token_amount_human(None, "ETH") == "N/A ETH"

    def test_float(self):
        assert format_token_amount_human(1.5, "ETH") == "1.5000 ETH"

    def test_decimal(self):
        assert format_token_amount_human(Decimal("100.50"), "USDC") == "100.5000 USDC"

    def test_zero(self):
        assert format_token_amount_human(0, "DAI") == "0 DAI"

    def test_very_small(self):
        assert format_token_amount_human(Decimal("0.00000005"), "ETH") == "0.00000005 ETH"

    def test_negative_small(self):
        assert format_token_amount_human(Decimal("-0.00000005"), "ETH") == "-0.00000005 ETH"

    def test_custom_max_decimals(self):
        assert format_token_amount_human(1.5, "ETH", max_decimals=2) == "1.50 ETH"


# ---------------------------------------------------------------------------
# format_token_with_usd (wei variant)
# ---------------------------------------------------------------------------


class TestFormatTokenWithUsd:
    def test_full_formatting(self):
        assert (
            format_token_with_usd(1_500_000_000_000_000_000, "ETH", 18, Decimal("3400"))
            == "1.5000 ETH ($5,100.00)"
        )

    def test_usdc_with_price(self):
        assert format_token_with_usd(100_000_000, "USDC", 6, Decimal("1")) == "100.0000 USDC ($100.00)"

    def test_none_amount_skips_usd(self):
        assert format_token_with_usd(None, "ETH", 18, Decimal("3400")) == "N/A ETH"

    def test_none_price_skips_usd(self):
        assert format_token_with_usd(1_500_000_000_000_000_000, "ETH", 18, None) == "1.5000 ETH"

    def test_float_price(self):
        result = format_token_with_usd(1_000_000_000_000_000_000, "ETH", 18, 1000.0)
        assert result == "1.0000 ETH ($1,000.00)"


# ---------------------------------------------------------------------------
# format_token_with_usd_human
# ---------------------------------------------------------------------------


class TestFormatTokenWithUsdHuman:
    def test_full_formatting(self):
        assert format_token_with_usd_human(1.5, "ETH", Decimal("3400")) == "1.5000 ETH ($5,100.00)"

    def test_none_amount(self):
        assert format_token_with_usd_human(None, "ETH", Decimal("3400")) == "N/A ETH"

    def test_none_price(self):
        assert format_token_with_usd_human(Decimal("1.5"), "ETH", None) == "1.5000 ETH"

    def test_zero_amount(self):
        # Zero amount short-circuits in format_token_amount_human, but USD block still
        # runs because amount is not None and price is provided.
        assert format_token_with_usd_human(Decimal("0"), "ETH", Decimal("3400")) == "0 ETH ($0.00)"


# ---------------------------------------------------------------------------
# format_gas_cost
# ---------------------------------------------------------------------------


class TestFormatGasCost:
    def test_gas_only(self):
        assert format_gas_cost(131_114) == "131,114 gas"

    def test_gas_only_when_price_missing(self):
        assert format_gas_cost(131_114, None, Decimal("3400")) == "131,114 gas"

    def test_gas_only_when_eth_price_missing(self):
        assert format_gas_cost(131_114, 20, None) == "131,114 gas"

    def test_gas_with_usd_cost(self):
        # 131114 * 20 * 1e-9 = 0.00262228 ETH; * 3400 = 8.91575... -> 8.92 (ROUND_HALF_EVEN).
        assert format_gas_cost(131_114, 20, Decimal("3400")) == "131,114 gas (~$8.92)"

    def test_gas_with_float_price(self):
        result = format_gas_cost(100_000, 10.0, 2000.0)
        # 100000 * 10 * 1e-9 = 0.001 ETH; * 2000 = 2.00
        assert result == "100,000 gas (~$2.00)"

    def test_zero_gas(self):
        assert format_gas_cost(0) == "0 gas"


# ---------------------------------------------------------------------------
# format_slippage
# ---------------------------------------------------------------------------


class TestFormatSlippage:
    def test_better(self):
        assert format_slippage(100, 102) == "+2.00% (better)"

    def test_worse(self):
        assert format_slippage(100, 98) == "-2.00% (worse)"

    def test_exact(self):
        assert format_slippage(100, 100) == "0.00% (exact)"

    def test_expected_zero(self):
        assert format_slippage(0, 10) == "N/A (expected was 0)"

    def test_expected_zero_even_if_actual_zero(self):
        assert format_slippage(0, 0) == "N/A (expected was 0)"

    def test_decimal_inputs(self):
        assert format_slippage(Decimal("100"), Decimal("98")) == "-2.00% (worse)"

    def test_float_inputs(self):
        assert format_slippage(100.0, 101.5) == "+1.50% (better)"

    def test_large_positive_slippage(self):
        assert format_slippage(100, 200) == "+100.00% (better)"


# ---------------------------------------------------------------------------
# format_slippage_bps
# ---------------------------------------------------------------------------


class TestFormatSlippageBps:
    def test_standard(self):
        assert format_slippage_bps(50) == "50bp (0.50%)"

    def test_zero(self):
        assert format_slippage_bps(0) == "0bp (0.00%)"

    def test_large(self):
        assert format_slippage_bps(10_000) == "10000bp (100.00%)"

    def test_float_rounds_half_even(self):
        # round(12.5) uses banker's rounding -> 12; pct 0.125 -> 0.12 under .2f
        assert format_slippage_bps(12.5) == "12bp (0.12%)"

    def test_float_rounds_up(self):
        # round(12.7) -> 13; pct 0.127 -> 0.13 under .2f
        assert format_slippage_bps(12.7) == "13bp (0.13%)"

    def test_negative(self):
        assert format_slippage_bps(-25) == "-25bp (-0.25%)"


# ---------------------------------------------------------------------------
# format_balance_delta
# ---------------------------------------------------------------------------


class TestFormatBalanceDelta:
    def test_basic_delta_with_prices(self):
        result = format_balance_delta(
            {"USDC": Decimal("1000"), "WETH": Decimal("0.5")},
            {"USDC": Decimal("900"), "WETH": Decimal("0.534")},
            {"USDC": Decimal("1"), "WETH": Decimal("3400")},
        )
        assert "USDC: -100.0000 (-$100.00)" in result
        assert "WETH: +0.0340 (+$115.60)" in result
        assert " | " in result

    def test_no_prices(self):
        result = format_balance_delta(
            {"USDC": Decimal("1000")},
            {"USDC": Decimal("1100")},
        )
        assert result == "USDC: +100.0000"

    def test_no_changes(self):
        result = format_balance_delta(
            {"USDC": Decimal("1000")},
            {"USDC": Decimal("1000")},
        )
        assert result == "No changes"

    def test_empty_inputs(self):
        assert format_balance_delta({}, {}) == "No changes"

    def test_token_only_in_after(self):
        result = format_balance_delta(
            {},
            {"USDC": Decimal("100")},
        )
        assert result == "USDC: +100.0000"

    def test_token_only_in_before(self):
        result = format_balance_delta(
            {"USDC": Decimal("100")},
            {},
        )
        assert result == "USDC: -100.0000"

    def test_price_missing_for_one_token(self):
        # Second token has no price -> no USD suffix for it
        result = format_balance_delta(
            {"USDC": Decimal("0"), "FOO": Decimal("0")},
            {"USDC": Decimal("50"), "FOO": Decimal("25")},
            {"USDC": Decimal("1")},
        )
        assert "USDC: +50.0000 (+$50.00)" in result
        assert "FOO: +25.0000" in result
        # FOO must NOT have USD-suffix
        foo_part = [p for p in result.split(" | ") if p.startswith("FOO")][0]
        assert "$" not in foo_part

    def test_sorted_alphabetically(self):
        result = format_balance_delta(
            {"ZRX": Decimal("0"), "AAVE": Decimal("0")},
            {"ZRX": Decimal("1"), "AAVE": Decimal("1")},
        )
        # AAVE should appear before ZRX
        assert result.index("AAVE") < result.index("ZRX")

    def test_negative_usd_sign_for_loss(self):
        result = format_balance_delta(
            {"USDC": Decimal("1000")},
            {"USDC": Decimal("500")},
            {"USDC": Decimal("1")},
        )
        assert result == "USDC: -500.0000 (-$500.00)"

    def test_zero_price_does_not_render_as_loss(self):
        # Non-zero delta with price=0 -> delta_usd is 0, must render unsigned.
        result = format_balance_delta(
            {"FOO": Decimal("0")},
            {"FOO": Decimal("100")},
            {"FOO": Decimal("0")},
        )
        assert result == "FOO: +100.0000 ($0.00)"


# ---------------------------------------------------------------------------
# format_balance_summary
# ---------------------------------------------------------------------------


class TestFormatBalanceSummary:
    def test_empty_wallet(self):
        assert format_balance_summary({}) == "Empty wallet"

    def test_all_zero_balances(self):
        # zero balances are skipped, yielding empty parts
        assert format_balance_summary({"USDC": Decimal("0"), "ETH": Decimal("0")}) == "Empty wallet"

    def test_no_prices_alphabetical_sort(self):
        result = format_balance_summary(
            {"ZRX": Decimal("10"), "AAVE": Decimal("5")},
        )
        assert result.index("AAVE") < result.index("ZRX")
        assert "AAVE: 5.0000" in result
        assert "ZRX: 10.0000" in result

    def test_with_prices_value_sort(self):
        # WETH value (0.5 * 3400 = 1700) > USDC value (1000 * 1 = 1000)
        result = format_balance_summary(
            {"USDC": Decimal("1000"), "WETH": Decimal("0.5")},
            {"USDC": Decimal("1"), "WETH": Decimal("3400")},
        )
        assert result.index("WETH") < result.index("USDC")
        assert "WETH: 0.5000 ($1,700.00)" in result
        assert "USDC: 1000.0000 ($1,000.00)" in result

    def test_max_tokens_truncates(self):
        balances = {f"T{i}": Decimal(str(i + 1)) for i in range(8)}
        result = format_balance_summary(balances, max_tokens=3)
        assert "... +5 more" in result

    def test_price_missing_for_one_token(self):
        # Token without price still printed but without USD suffix
        result = format_balance_summary(
            {"USDC": Decimal("100"), "FOO": Decimal("42")},
            {"USDC": Decimal("1")},
        )
        assert "USDC: 100.0000 ($100.00)" in result
        foo_parts = [p for p in result.split(" | ") if p.startswith("FOO")]
        assert foo_parts, f"Expected FOO in output, got: {result}"
        assert "$" not in foo_parts[0]

    def test_skips_zero_balance_token(self):
        result = format_balance_summary(
            {"USDC": Decimal("100"), "ZERO": Decimal("0")},
            {"USDC": Decimal("1"), "ZERO": Decimal("1")},
        )
        assert "USDC" in result
        assert "ZERO" not in result


# ---------------------------------------------------------------------------
# format_price
# ---------------------------------------------------------------------------


class TestFormatPrice:
    def test_none(self):
        assert format_price(None, "ETH") == "N/A USD/ETH"

    def test_none_custom_quote(self):
        assert format_price(None, "ETH", quote="BTC") == "N/A BTC/ETH"

    def test_standard(self):
        assert format_price(Decimal("3456.78"), "ETH") == "3,456.78 USD/ETH"

    def test_float_input(self):
        assert format_price(1000.5, "BTC", "USD") == "1,000.50 USD/BTC"

    def test_custom_quote(self):
        assert format_price(Decimal("20"), "ETH", quote="BTC") == "20.00 BTC/ETH"


# ---------------------------------------------------------------------------
# format_health_factor
# ---------------------------------------------------------------------------


class TestFormatHealthFactor:
    def test_none(self):
        assert format_health_factor(None) == "N/A"

    def test_liquidatable(self):
        assert format_health_factor(Decimal("0.95")) == "0.95 (LIQUIDATABLE)"

    def test_exact_one_is_low(self):
        # hf == 1.0 -> not < 1.0, but < 1.25 -> low
        assert format_health_factor(Decimal("1.0")) == "1.00 (low)"

    def test_low(self):
        assert format_health_factor(Decimal("1.15")) == "1.15 (low)"

    def test_moderate(self):
        assert format_health_factor(Decimal("1.30")) == "1.30 (moderate)"

    def test_safe(self):
        assert format_health_factor(Decimal("1.85")) == "1.85"

    def test_boundary_1_25_is_moderate(self):
        # 1.25 is not < 1.25 -> moderate branch
        assert format_health_factor(Decimal("1.25")) == "1.25 (moderate)"

    def test_boundary_1_5_is_safe(self):
        assert format_health_factor(Decimal("1.5")) == "1.50"

    def test_float_input(self):
        assert format_health_factor(2.0) == "2.00"


# ---------------------------------------------------------------------------
# format_leverage
# ---------------------------------------------------------------------------


class TestFormatLeverage:
    def test_none(self):
        assert format_leverage(None) == "N/A"

    def test_decimal(self):
        assert format_leverage(Decimal("2.0")) == "2.0x"

    def test_float(self):
        assert format_leverage(3.5) == "3.5x"

    def test_int(self):
        assert format_leverage(5) == "5.0x"


# ---------------------------------------------------------------------------
# format_percentage
# ---------------------------------------------------------------------------


class TestFormatPercentage:
    def test_none(self):
        assert format_percentage(None) == "N/A"

    def test_basic(self):
        assert format_percentage(0.05) == "5.00%"

    def test_decimal(self):
        assert format_percentage(Decimal("0.1234")) == "12.34%"

    def test_custom_decimals(self):
        assert format_percentage(Decimal("0.12345"), decimals=4) == "12.3450%"

    def test_zero(self):
        assert format_percentage(0) == "0.00%"

    def test_negative(self):
        assert format_percentage(Decimal("-0.05")) == "-5.00%"


# ---------------------------------------------------------------------------
# format_tx_hash / format_address
# ---------------------------------------------------------------------------


class TestFormatTxHash:
    def test_empty_string(self):
        assert format_tx_hash("") == "N/A"

    def test_none_treated_as_empty(self):
        # falsy -> N/A
        assert format_tx_hash(None) == "N/A"  # type: ignore[arg-type]

    def test_short_hash_not_truncated(self):
        # len 10 <= 16, so returned as-is
        assert format_tx_hash("0xabcdef12") == "0xabcdef12"

    def test_long_hash_truncated(self):
        full = "0x" + "a" * 62 + "1234"
        result = format_tx_hash(full)
        assert result == f"{full[:6]}...{full[-4:]}"

    def test_long_hash_no_truncate(self):
        full = "0x" + "a" * 62 + "1234"
        assert format_tx_hash(full, truncate=False) == full

    def test_exactly_16_chars_not_truncated(self):
        # len 16 is NOT > 16, so not truncated
        val = "0x" + "a" * 14
        assert len(val) == 16
        assert format_tx_hash(val) == val

    def test_seventeen_chars_truncated(self):
        val = "0x" + "a" * 15
        assert len(val) == 17
        assert format_tx_hash(val) == f"{val[:6]}...{val[-4:]}"


class TestFormatAddress:
    def test_delegates_to_tx_hash(self):
        addr = "0x" + "b" * 38 + "cdef"
        assert format_address(addr) == f"{addr[:6]}...{addr[-4:]}"

    def test_no_truncate(self):
        addr = "0x" + "b" * 38 + "cdef"
        assert format_address(addr, truncate=False) == addr

    def test_empty(self):
        assert format_address("") == "N/A"


# ---------------------------------------------------------------------------
# Emoji helpers
# ---------------------------------------------------------------------------


class TestEmojisEnabled:
    def test_default_is_enabled(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("ALMANAK_LOG_EMOJIS", raising=False)
        assert _emojis_enabled() is True

    def test_explicit_true(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "true")
        assert _emojis_enabled() is True

    def test_false_disables(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "false")
        assert _emojis_enabled() is False

    def test_zero_disables(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "0")
        assert _emojis_enabled() is False

    def test_no_disables(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "no")
        assert _emojis_enabled() is False

    def test_mixed_case_false(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "FALSE")
        assert _emojis_enabled() is False

    def test_whitespace_stripped(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "  false  ")
        assert _emojis_enabled() is False

    def test_arbitrary_value_treated_as_enabled(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "yes")
        assert _emojis_enabled() is True


class TestFormatIntentTypeEmoji:
    def test_known_type_with_emojis(self, emojis_enabled):
        result = format_intent_type_emoji("SWAP")
        assert "SWAP" in result
        # emoji prefix present
        assert result.startswith("\U0001f504")  # swap emoji

    def test_unknown_type_with_emojis_uses_default(self, emojis_enabled):
        result = format_intent_type_emoji("NOT_A_REAL_TYPE")
        assert "NOT_A_REAL_TYPE" in result
        assert result.startswith("\U0001f4cb")  # clipboard

    def test_lowercase_input_normalized(self, emojis_enabled):
        result = format_intent_type_emoji("swap")
        assert "swap" in result
        # emoji lookup uses upper() so SWAP mapping hit
        assert result.startswith("\U0001f504")

    def test_plain_mode_uses_tag(self, emojis_disabled):
        assert format_intent_type_emoji("SWAP") == "[SWAP]"

    def test_plain_mode_uppercases(self, emojis_disabled):
        assert format_intent_type_emoji("swap") == "[SWAP]"

    def test_all_mappings_produce_non_default(self, emojis_enabled):
        # Each known key should map to a specific emoji (not the clipboard default)
        default = "\U0001f4cb"
        for key in [
            "SWAP",
            "SUPPLY",
            "BORROW",
            "REPAY",
            "WITHDRAW",
            "LP_OPEN",
            "LP_CLOSE",
            "LP_REBALANCE",
            "PERP_OPEN",
            "PERP_CLOSE",
            "PERP_MODIFY",
            "BRIDGE",
            "HOLD",
            "APPROVE",
        ]:
            assert not format_intent_type_emoji(key).startswith(default), key


class TestFormatExecutionStatus:
    def test_success_with_emojis(self, emojis_enabled):
        assert format_execution_status(True) == "\u2705 SUCCESS"

    def test_failure_with_emojis(self, emojis_enabled):
        assert format_execution_status(False) == "\u274c FAILED"

    def test_success_plain(self, emojis_disabled):
        assert format_execution_status(True) == "[SUCCESS]"

    def test_failure_plain(self, emojis_disabled):
        assert format_execution_status(False) == "[FAILED]"


class TestFormatWarning:
    def test_with_emojis(self, emojis_enabled):
        result = format_warning("hello")
        assert result.endswith(" hello")
        assert result.startswith("\u26a0")

    def test_plain(self, emojis_disabled):
        assert format_warning("hello") == "[WARN] hello"

    def test_empty_message(self, emojis_disabled):
        assert format_warning("") == "[WARN] "


class TestFormatError:
    def test_with_emojis(self, emojis_enabled):
        result = format_error("boom")
        assert result.endswith(" boom")
        assert result.startswith("\u274c")

    def test_plain(self, emojis_disabled):
        assert format_error("boom") == "[ERROR] boom"


class TestFormatInfo:
    def test_with_emojis(self, emojis_enabled):
        result = format_info("fyi")
        assert result.endswith(" fyi")
        # Info prefix is an emoji; just assert non-bracket prefix
        assert not result.startswith("[")

    def test_plain(self, emojis_disabled):
        assert format_info("fyi") == "[INFO] fyi"


# ---------------------------------------------------------------------------
# wei_to_human / human_to_wei
# ---------------------------------------------------------------------------


class TestWeiHuman:
    def test_wei_to_human_eth(self):
        assert wei_to_human(1_500_000_000_000_000_000) == Decimal("1.5")

    def test_wei_to_human_usdc(self):
        assert wei_to_human(100_000_000, decimals=6) == Decimal("100")

    def test_wei_to_human_zero(self):
        assert wei_to_human(0) == Decimal("0")

    def test_human_to_wei_eth(self):
        assert human_to_wei(Decimal("1.5"), 18) == 1_500_000_000_000_000_000

    def test_human_to_wei_float(self):
        assert human_to_wei(1.5, 18) == 1_500_000_000_000_000_000

    def test_human_to_wei_usdc(self):
        assert human_to_wei(Decimal("100"), 6) == 100_000_000

    def test_human_to_wei_zero(self):
        assert human_to_wei(0, 18) == 0

    def test_round_trip(self):
        wei = 1_234_567_890_000_000_000
        assert human_to_wei(wei_to_human(wei, 18), 18) == wei


# ---------------------------------------------------------------------------
# Module-level dunder sanity
# ---------------------------------------------------------------------------


def test_module_exports_core_functions():
    # Guardrail: if somebody renames/deletes an exported symbol the tests above
    # catch it, but this is an explicit enumeration of what downstream code relies on.
    for name in [
        "format_usd",
        "format_token_amount",
        "format_token_amount_human",
        "format_token_with_usd",
        "format_token_with_usd_human",
        "format_gas_cost",
        "format_slippage",
        "format_slippage_bps",
        "format_balance_delta",
        "format_balance_summary",
        "format_price",
        "format_health_factor",
        "format_leverage",
        "format_percentage",
        "format_tx_hash",
        "format_address",
        "format_intent_type_emoji",
        "format_execution_status",
        "format_warning",
        "format_error",
        "format_info",
        "wei_to_human",
        "human_to_wei",
    ]:
        assert hasattr(lf, name), f"missing export: {name}"
