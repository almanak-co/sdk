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
    _preflight_emit_ambiguous_warnings,
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


class TestPreflightEmitAmbiguousWarnings:
    """#1756: sweep-scoped dedup of ambiguous-coercion warnings.

    The previous ``warned_ambiguous`` set was recreated per backtest call and
    per worker task, so a single ambiguous ``(name, value)`` pair could emit
    N × M duplicate stderr lines on a N-period × M-worker sweep. The parent
    now walks ``combinations`` once before any worker / period is dispatched
    and emits each unique ambiguous pair exactly once; workers run with
    ``emit_warnings=False`` and stay silent.
    """

    def test_single_ambiguous_value_across_simulated_periods_and_workers_emits_once(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Simulate a 2-period × 2-worker sweep carrying the same ``1e5``
        value through every worker invocation. Before #1756 this produced 4
        duplicate warnings; after #1756 the parent pre-pass emits exactly 1.
        """
        combinations = [{"size": "1e5"}, {"size": "1e5"}]  # 2 combos
        # Parent pre-pass runs once, before any worker spawns.
        _preflight_emit_ambiguous_warnings(combinations, frozenset())
        parent_stderr = capsys.readouterr().err

        # Simulate worker-side coercion across 2 periods × 2 workers. With
        # `emit_warnings=False` these must be silent.
        for _period in range(2):
            for combo in combinations:
                worker_warned: set[tuple[str, str]] = set()
                for name, value in combo.items():
                    _coerce_sweep_value(
                        name,
                        value,
                        numeric_param_names=frozenset(),
                        warned_ambiguous=worker_warned,
                        emit_warnings=False,
                    )
        worker_stderr = capsys.readouterr().err

        assert parent_stderr.count("size=1e5") == 1, (
            f"Expected exactly 1 parent-side warning, got:\n{parent_stderr!r}"
        )
        assert worker_stderr == "", (
            f"Workers must not emit duplicate warnings, got:\n{worker_stderr!r}"
        )

    def test_multiple_ambiguous_values_each_warn_once(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Each distinct ambiguous ``(name, value)`` pair warns once. A
        repeated combination in the sweep matrix does not re-warn."""
        combinations = [
            {"size": "1e5", "token_id": "0001"},
            {"size": "2e5", "token_id": "0001"},  # token_id repeats, size differs
            {"size": "1e5", "token_id": "0002"},  # size repeats, token_id differs
        ]
        _preflight_emit_ambiguous_warnings(combinations, frozenset())
        err = capsys.readouterr().err

        # Each unique ambiguous pair warns exactly once.
        assert err.count("size=1e5") == 1
        assert err.count("size=2e5") == 1
        assert err.count("token_id=0001") == 1
        assert err.count("token_id=0002") == 1
        # Total: 4 unique warnings, no duplicates.
        assert err.count("Warning:") == 4

    def test_clean_values_emit_no_warnings(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Round-tripping numeric strings and pure categorical strings must
        not trip the warning pre-pass."""
        combinations = [
            {"threshold": "1.5", "mode": "aggressive"},
            {"threshold": "2.5", "mode": "conservative"},
        ]
        _preflight_emit_ambiguous_warnings(combinations, frozenset())
        assert capsys.readouterr().err == ""

    def test_numeric_param_names_are_skipped(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Names marked via ``--numeric-param`` are author opt-in: the
        preflight must not second-guess them with an ambiguity warning.
        Strict-numeric validation lives in ``_preflight_validate_numeric_params``.
        """
        combinations = [{"token_id": "0001"}, {"token_id": "1e5"}]
        _preflight_emit_ambiguous_warnings(
            combinations, numeric_param_names=frozenset({"token_id"})
        )
        assert capsys.readouterr().err == ""

    def test_empty_combinations_is_a_noop(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _preflight_emit_ambiguous_warnings([], frozenset())
        assert capsys.readouterr().err == ""

    def test_worker_silent_mode_still_coerces_identically(self) -> None:
        """``emit_warnings=False`` must not change the resulting Python value
        — only the stderr side effect is suppressed."""
        warned: set[tuple[str, str]] = set()
        loud = _coerce_sweep_value(
            "x", "1e5", numeric_param_names=frozenset(), warned_ambiguous=set()
        )
        silent = _coerce_sweep_value(
            "x",
            "1e5",
            numeric_param_names=frozenset(),
            warned_ambiguous=warned,
            emit_warnings=False,
        )
        assert loud == silent == 100000.0
        # The silent-mode call still populates the dedup set so repeat calls
        # within the same worker stay idempotent.
        assert ("x", "1e5") in warned


class TestPublicHelpersStillWarnByDefault:
    """Regression guard: public `run_sweep_backtest` / `run_parallel_sweeps`
    helpers must keep the #1702 warning surface for direct programmatic
    callers (notebooks, tests, library usage) that do not go through the
    ``sweep_backtest`` CLI command. The #1756 dedup hoist only silences the
    CLI code path; it must not regress the helpers to the original hidden
    ``"0001" -> 1.0`` / ``"1e5" -> 100000.0`` behaviour that #1702 surfaced.
    """

    def test_run_sweep_backtest_signature_defaults_to_warning(self) -> None:
        """``run_sweep_backtest(emit_ambiguity_warnings=...)`` defaults to
        True. We introspect the parameter at the signature level rather than
        instantiating the full coroutine (which requires a live data
        provider) — this keeps the test hermetic while still pinning the
        public contract."""
        import inspect

        from almanak.framework.cli.backtest.sweep import run_sweep_backtest

        sig = inspect.signature(run_sweep_backtest)
        param = sig.parameters["emit_ambiguity_warnings"]
        assert param.default is True, (
            "Direct callers of run_sweep_backtest must keep #1702 warnings by default"
        )

    def test_run_parallel_sweeps_signature_defaults_to_warning(self) -> None:
        import inspect

        from almanak.framework.cli.backtest.sweep import run_parallel_sweeps

        sig = inspect.signature(run_parallel_sweeps)
        param = sig.parameters["emit_ambiguity_warnings"]
        assert param.default is True

    def test_run_parallel_sweep_signature_defaults_to_warning(self) -> None:
        """Internal multiprocessing helper also keeps the warnings-on default
        so a direct caller outside the CLI path still sees #1702 output."""
        import inspect

        from almanak.framework.cli.backtest.sweep import _run_parallel_sweep

        sig = inspect.signature(_run_parallel_sweep)
        param = sig.parameters["emit_ambiguity_warnings"]
        assert param.default is True
