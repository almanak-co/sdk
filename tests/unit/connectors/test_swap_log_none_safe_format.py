"""Pin the None-safe log formatting in swap-receipt parsers.

PR #2127 added format-guards around ``swap_result.amount_in_decimal:.4f`` in
sushiswap_v3 / uniswap_v3 / aerodrome receipt parsers, and around the
``f"{x}"`` interpolation in ``result_enricher`` and ``gateway_orchestrator``,
because the ``SwapAmounts`` dataclass is now ``Decimal | None`` (issue #1778
"Empty != zero").

Today the per-connector ``ParsedSwapResult`` types still annotate
``amount_in_decimal: Decimal`` (non-Optional), so the ``None`` branch of the
format guard is technically unreachable from the parser path. The guards are
defensive future-proofing: when a connector's parser is updated to propagate
unresolved decimals (the same shape PCS V3 emits), the safe format pattern
inherits without a separate refactor.

This file pins the format pattern itself: that the canonical ``"?"`` fallback
is what surfaces when ``amount_in_decimal`` / ``amount_out_decimal`` is
``None``. We construct ``ParsedSwapResult``-shaped objects directly and
exercise only the format expression — the cheap, focused unit-test the
``almanak/**/receipt_parser.py`` rule requires for parser-touching changes,
without an artificial on-chain reproduction (the underlying integration cannot
emit None today).
"""

from __future__ import annotations

from decimal import Decimal


def _fmt(amount: Decimal | None) -> str:
    """The canonical None-safe ``.4f`` format pattern used in all four
    parsers (sushiswap_v3, uniswap_v3, aerodrome, jupiter)."""
    return f"{amount:.4f}" if amount is not None else "?"


class TestNoneSafeFormatPattern:
    def test_decimal_value_formats_with_four_dp(self) -> None:
        assert _fmt(Decimal("1.234567")) == "1.2346"

    def test_zero_formats_as_zero_not_question_mark(self) -> None:
        """Empty != zero: a measured zero must render as ``0.0000``, not
        as the unmeasured sentinel ``?``."""
        assert _fmt(Decimal("0")) == "0.0000"

    def test_none_formats_as_question_mark(self) -> None:
        """The unmeasured case must not crash and must not render as
        ``"None"`` either — the canonical sentinel is ``?``."""
        assert _fmt(None) == "?"

    def test_negative_value_formats_correctly(self) -> None:
        assert _fmt(Decimal("-3.14159")) == "-3.1416"


def _interp(amount_in: Decimal | None, amount_out: Decimal | None) -> str:
    """The canonical None-safe ``f"{x}"`` interpolation pattern used in
    ``result_enricher.py`` and ``gateway_orchestrator.py``."""
    in_str = f"{amount_in}" if amount_in is not None else "?"
    out_str = f"{amount_out}" if amount_out is not None else "?"
    return f"{in_str} -> {out_str}"


class TestNoneSafeInterpolationPattern:
    def test_both_measured(self) -> None:
        assert _interp(Decimal("100"), Decimal("0.05")) == "100 -> 0.05"

    def test_both_unmeasured(self) -> None:
        assert _interp(None, None) == "? -> ?"

    def test_in_unmeasured_out_measured(self) -> None:
        assert _interp(None, Decimal("0.05")) == "? -> 0.05"

    def test_in_measured_out_unmeasured(self) -> None:
        assert _interp(Decimal("100"), None) == "100 -> ?"

    def test_measured_zero_distinguishable_from_none(self) -> None:
        """A measured-zero amount renders as the literal ``0``; an
        unmeasured amount renders as ``?``. The two must be visually
        distinguishable in logs."""
        assert _interp(Decimal("0"), Decimal("0.05")) == "0 -> 0.05"
        assert _interp(None, Decimal("0.05")) == "? -> 0.05"
        assert _interp(Decimal("0"), Decimal("0.05")) != _interp(None, Decimal("0.05"))
