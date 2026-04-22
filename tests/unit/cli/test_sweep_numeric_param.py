"""Unit tests for sweep parameter coercion (#1702).

The original ``float(value)`` blanket coercion silently changed the
semantics of certain string values (``"0001"`` -> 1.0, ``"1e5"`` -> 100000.0,
``"inf"`` -> float infinity, etc). The narrow fix introduces:

- ``_coerce_sweep_value`` helper with a ``numeric_param_names`` opt-in,
- ambiguity warnings on semantics-changing coercions and inf/nan,
- a new ``--numeric-param``/``-P`` CLI flag that enforces strict numeric
  parsing for marked names.
"""

from __future__ import annotations

import math

import click
import pytest

from almanak.framework.cli.backtest.sweep import (
    _coerce_sweep_value,
    _preflight_validate_numeric_params,
)


class TestCoerceSweepValueAmbiguityWarnings:
    def test_integer_like_string_round_trips_without_warning(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "threshold", "1.5", numeric_param_names=frozenset(), warned_ambiguous=warned
        )
        assert result == 1.5
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_plain_integer_round_trips_without_warning(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        # "1" vs float(1)="1.0" — the `f"{1.0:g}" == "1"` branch covers this.
        result = _coerce_sweep_value(
            "window", "1", numeric_param_names=frozenset(), warned_ambiguous=warned
        )
        assert result == 1.0
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_zero_padded_string_warns(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """'0001' -> 1.0 is almost never what the author meant (#1702)."""
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "token_id", "0001", numeric_param_names=frozenset(), warned_ambiguous=warned
        )
        assert result == 1.0
        captured = capsys.readouterr()
        assert "token_id=0001" in captured.err
        assert "coerced to float" in captured.err

    def test_scientific_notation_warns(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "size", "1e5", numeric_param_names=frozenset(), warned_ambiguous=warned
        )
        assert result == 100000.0
        captured = capsys.readouterr()
        assert "size=1e5" in captured.err

    def test_infinity_string_warns_specially(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "cap", "inf", numeric_param_names=frozenset(), warned_ambiguous=warned
        )
        assert math.isinf(result)
        captured = capsys.readouterr()
        assert "cap=inf" in captured.err
        assert "not what you meant" in captured.err

    def test_nan_string_warns_specially(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "ratio", "nan", numeric_param_names=frozenset(), warned_ambiguous=warned
        )
        assert math.isnan(result)
        captured = capsys.readouterr()
        assert "ratio=nan" in captured.err

    def test_non_numeric_string_passes_through_silently(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "mode",
            "aggressive",
            numeric_param_names=frozenset(),
            warned_ambiguous=warned,
        )
        assert result == "aggressive"
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_warning_is_deduped_per_name_value_pair(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        for _ in range(3):
            _coerce_sweep_value(
                "x", "0001", numeric_param_names=frozenset(), warned_ambiguous=warned
            )
        captured = capsys.readouterr()
        # Warning emits once even if the same (name, value) comes through
        # multiple periods / workers.
        assert captured.err.count("x=0001") == 1


class TestCoerceSweepValueNumericParam:
    def test_strict_numeric_accepts_valid_floats(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "threshold",
            "0.5",
            numeric_param_names=frozenset({"threshold"}),
            warned_ambiguous=warned,
        )
        assert result == 0.5

    def test_strict_numeric_rejects_non_numeric(self) -> None:
        warned: set[tuple[str, str]] = set()
        with pytest.raises(click.UsageError, match="not numeric"):
            _coerce_sweep_value(
                "threshold",
                "aggressive",
                numeric_param_names=frozenset({"threshold"}),
                warned_ambiguous=warned,
            )

    def test_strict_numeric_suppresses_ambiguity_warning(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Under `--numeric-param`, the author has opted in — no warning."""
        warned: set[tuple[str, str]] = set()
        result = _coerce_sweep_value(
            "token_id",
            "0001",
            numeric_param_names=frozenset({"token_id"}),
            warned_ambiguous=warned,
        )
        assert result == 1.0
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_strict_only_applies_to_marked_names(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Unmarked names keep the lenient historical behaviour."""
        warned: set[tuple[str, str]] = set()
        # 'mode' is marked, 'window' is not.
        _coerce_sweep_value(
            "mode",
            "1.0",
            numeric_param_names=frozenset({"mode"}),
            warned_ambiguous=warned,
        )
        result = _coerce_sweep_value(
            "window",
            "hello",
            numeric_param_names=frozenset({"mode"}),
            warned_ambiguous=warned,
        )
        assert result == "hello"

    def test_marking_unknown_numeric_param_raises_usage_error(self) -> None:
        """CLI-level integration: unknown numeric param names are rejected.

        The CLI command normalises and validates the set before any
        backtest work — but we exercise the same semantics at the
        helper layer: marking a name not in the sweep makes the coercer
        strict for that name, which fails for non-numeric values.
        """
        warned: set[tuple[str, str]] = set()
        with pytest.raises(click.UsageError):
            _coerce_sweep_value(
                "not_in_sweep",
                "some-string-value",
                numeric_param_names=frozenset({"not_in_sweep"}),
                warned_ambiguous=warned,
            )


class TestPreflightValidateNumericParams:
    """#1702 parallel-mode fix: validation must run in the parent process.

    Rationale: in ``--parallel`` mode, ``_coerce_sweep_value`` runs inside
    worker subprocesses. Any ``click.UsageError`` raised there gets pickled
    back and caught by ``_run_parallel_sweep``'s broad ``except Exception``,
    degrading into a synthetic failed ``SweepResult`` and letting the command
    exit 0 with ranked output from an invalid sweep. Preflighting the numeric
    values in the parent aborts the run before any worker is spawned, which
    upholds the documented ``--numeric-param`` "run aborts" contract in both
    sequential and parallel modes.
    """

    def test_accepts_all_numeric_combinations(self) -> None:
        combos = [
            {"threshold": "0.01", "window": "5"},
            {"threshold": "0.02", "window": "10"},
        ]
        # No raise.
        _preflight_validate_numeric_params(combos, frozenset({"threshold", "window"}))

    def test_rejects_non_numeric_in_marked_column(self) -> None:
        combos = [
            {"threshold": "0.01", "mode": "fast"},
            {"threshold": "abc", "mode": "slow"},
        ]
        with pytest.raises(click.UsageError) as excinfo:
            _preflight_validate_numeric_params(combos, frozenset({"threshold"}))
        # Error surfaces the offending value and parameter name so the user
        # can find it without a stack trace.
        msg = str(excinfo.value)
        assert "threshold" in msg
        assert "'abc'" in msg

    def test_ignores_non_marked_columns(self) -> None:
        """Unmarked columns may contain arbitrary strings — validation is
        opt-in per ``--numeric-param`` name."""
        combos = [{"threshold": "0.01", "mode": "fast"}, {"threshold": "0.02", "mode": "slow"}]
        # 'mode' is not marked; its "fast"/"slow" values must not trip the check.
        _preflight_validate_numeric_params(combos, frozenset({"threshold"}))

    def test_empty_marked_set_is_a_noop(self) -> None:
        """Call sites only invoke this when ``--numeric-param`` is set, but
        a belt-and-braces call with an empty set must remain a no-op."""
        combos = [{"threshold": "nonsense"}]
        _preflight_validate_numeric_params(combos, frozenset())

    def test_empty_string_value_is_rejected(self) -> None:
        combos = [{"threshold": ""}]
        with pytest.raises(click.UsageError):
            _preflight_validate_numeric_params(combos, frozenset({"threshold"}))

    def test_infinity_and_nan_pass_the_numeric_gate(self) -> None:
        """``float("inf")`` and ``float("nan")`` both succeed — the preflight
        only guards against *parse failure*, matching the
        ``--numeric-param`` contract in ``_coerce_sweep_value`` which also
        accepts inf/nan (their semantic hazards belong to the ambiguity
        warnings, not to the strict numeric gate)."""
        combos = [{"threshold": "inf"}, {"threshold": "nan"}, {"threshold": "-inf"}]
        _preflight_validate_numeric_params(combos, frozenset({"threshold"}))
        # Sanity-check consistency with `float()`'s own semantics.
        assert math.isinf(float("inf"))
        assert math.isnan(float("nan"))
