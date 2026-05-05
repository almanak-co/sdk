"""Unit tests for ``scripts/ci/crap_diff_plugin.py``.

Pins the diff-cover plugin contract: config loading, CRAP formula edges,
omit-glob honoring, package-scope filtering, and per-line Violation emission.
The plugin is the load-bearing piece of the PR-time CRAP gate; misbehavior
either lets bad code merge or floods CI with false positives.
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest

from scripts.ci import crap_diff_plugin as plugin

# ──────────────────────────────────────────────────────────────────────────────
# _crap()
# ──────────────────────────────────────────────────────────────────────────────


class TestCrapFormula:
    """CRAP(f) = CC^2 * (1 - cov)^3 + CC. Mirrors scripts/crap_score.py."""

    def test_fully_covered_returns_complexity(self):
        assert plugin._crap(10, 1.0) == 10

    def test_zero_coverage_returns_full_penalty(self):
        # cc=10, cov=0 → 100 * 1 + 10 = 110.
        assert plugin._crap(10, 0.0) == 110

    def test_partial_coverage(self):
        # cc=10, cov=0.5 → 100 * 0.125 + 10 = 22.5.
        assert plugin._crap(10, 0.5) == pytest.approx(22.5)


# ──────────────────────────────────────────────────────────────────────────────
# _load_config()
# ──────────────────────────────────────────────────────────────────────────────


class TestLoadConfig:
    def test_missing_pyproject_returns_defaults(self, tmp_path: Path):
        cfg = plugin._load_config(tmp_path / "does_not_exist.toml")
        assert cfg.threshold == plugin.DEFAULT_CONFIG["threshold"]
        assert cfg.coverage_data == plugin.DEFAULT_CONFIG["coverage_data"]
        assert cfg.package_root == plugin.DEFAULT_CONFIG["package_root"]
        assert cfg.honor_omit is True
        assert cfg.omit_globs == []

    def test_pyproject_without_section_returns_defaults(self, tmp_path: Path):
        path = tmp_path / "pyproject.toml"
        path.write_text('[project]\nname = "x"\n')
        cfg = plugin._load_config(path)
        assert cfg.threshold == plugin.DEFAULT_CONFIG["threshold"]
        assert cfg.omit_globs == []

    def test_pyproject_overrides_threshold_and_paths(self, tmp_path: Path):
        path = tmp_path / "pyproject.toml"
        path.write_text(
            dedent("""
            [tool.crap-diff]
            threshold = 50
            coverage_data = "build/.coverage"
            package_root = "src"
            honor_omit = false
        """)
        )
        cfg = plugin._load_config(path)
        assert cfg.threshold == 50.0
        assert cfg.coverage_data == "build/.coverage"
        assert cfg.package_root == "src"
        assert cfg.honor_omit is False
        assert cfg.omit_globs == []  # honor_omit=false skips loading them

    def test_honor_omit_loads_coverage_omit_globs(self, tmp_path: Path):
        path = tmp_path / "pyproject.toml"
        path.write_text(
            dedent("""
            [tool.crap-diff]
            honor_omit = true

            [tool.coverage.run]
            omit = ["almanak/demo_strategies/*", "almanak/**/test_*.py"]
        """)
        )
        cfg = plugin._load_config(path)
        assert cfg.honor_omit is True
        assert cfg.omit_globs == ["almanak/demo_strategies/*", "almanak/**/test_*.py"]


# ──────────────────────────────────────────────────────────────────────────────
# _is_omitted()
# ──────────────────────────────────────────────────────────────────────────────


class TestIsOmitted:
    def test_matches_glob(self):
        assert plugin._is_omitted("almanak/demo_strategies/foo.py", ["almanak/demo_strategies/*"])

    def test_no_match(self):
        assert not plugin._is_omitted("almanak/framework/runner/x.py", ["almanak/demo_strategies/*"])

    def test_empty_globs_never_omits(self):
        assert not plugin._is_omitted("almanak/anything.py", [])


# ──────────────────────────────────────────────────────────────────────────────
# _function_coverage_ratio() / _iter_functions()
# ──────────────────────────────────────────────────────────────────────────────


class TestFunctionCoverage:
    def test_all_executable_lines_covered_returns_one(self):
        funcs = list(plugin._iter_functions("def f():\n    return 1\n"))
        assert len(funcs) == 1
        f = funcs[0]
        executable = {f.lineno + 1}  # only the `return 1` line is executable
        executed = {f.lineno + 1}
        assert plugin._function_coverage_ratio(executable, executed, f) == 1.0

    def test_no_executable_lines_covered_returns_zero(self):
        funcs = list(plugin._iter_functions("def f():\n    return 1\n"))
        f = funcs[0]
        executable = {f.lineno + 1}
        assert plugin._function_coverage_ratio(executable, set(), f) == 0.0

    def test_function_with_only_docstring_returns_one(self):
        # Pure docstring function — no executable lines → ratio = 1.0 (no risk).
        funcs = list(plugin._iter_functions('def f():\n    """just a docstring."""\n'))
        f = funcs[0]
        # No executable lines in range.
        assert plugin._function_coverage_ratio(set(), set(), f) == 1.0

    def test_docstring_lines_not_in_denominator(self):
        """Regression for the inflated-denominator bug: a function with a
        multi-line docstring whose only executable line is fully covered must
        report 100% coverage, not (1 / (1 + docstring_lines))."""
        src = 'def f():\n    """First line.\n\n    Second paragraph.\n\n    More text.\n    """\n    return 42\n'
        funcs = list(plugin._iter_functions(src))
        assert len(funcs) == 1
        f = funcs[0]
        # The `return 42` line is at f.endline.
        executable = {f.endline}
        executed = {f.endline}
        assert plugin._function_coverage_ratio(executable, executed, f) == 1.0

    def test_iter_functions_skips_syntax_error_silently(self):
        # cc_visit raises SyntaxError on bad source; iter wraps and yields nothing.
        assert list(plugin._iter_functions("def broken(:\n")) == []

    def test_iter_functions_does_not_duplicate_class_methods(self):
        """Regression: cc_visit returns class methods twice (under Class.methods
        AND at top level). Naive iteration double-yields each method; the
        plugin must dedupe at the source by skipping Class wrappers."""
        src = (
            "def top_level(x):\n"
            "    return x\n"
            "\n"
            "class Foo:\n"
            "    def method_a(self, x):\n"
            "        return x\n"
            "    def method_b(self, x):\n"
            "        return x\n"
        )
        funcs = list(plugin._iter_functions(src))
        names = sorted(f"{f.classname or '<top>'}.{f.name}" for f in funcs)
        assert names == ["<top>.top_level", "Foo.method_a", "Foo.method_b"]
        # Confirm exact count (not >=) so a regression that re-introduces the
        # double-yield trips immediately.
        assert len(funcs) == 3


# ──────────────────────────────────────────────────────────────────────────────
# _find_allowlist_reason()
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildAllowlistMap:
    """Pinned because the allowlist is the gate's only escape hatch — getting
    the placement semantics wrong either lets every PR bypass the gate or
    blocks even correctly-allowlisted code. Uses ast.parse to handle multi-
    line decorators that line-by-line scanning would mis-classify as 'real
    code' and abort on."""

    def test_directly_above_def(self):
        src = "# crap-allowlist: legacy state machine, see ADR-0042\ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {("f", 2): "legacy state machine, see ADR-0042"}

    def test_through_blank_line(self):
        src = "# crap-allowlist: documented reason\n\ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {("f", 3): "documented reason"}

    def test_through_single_line_decorator(self):
        src = "# crap-allowlist: tested manually via Anvil\n@pytest.fixture\ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {("f", 3): "tested manually via Anvil"}

    def test_through_multiline_decorator(self):
        """Regression for codex review on PR #2078: line-based scan saw the
        `)` continuation of a wrapped @parametrize call as 'real code' and
        aborted before reaching the allowlist comment, which would have
        blocked legitimate escape-hatch use once the gate becomes required."""
        src = (
            "# crap-allowlist: per-protocol coverage matrix lives here, can't decompose\n"
            "@pytest.mark.parametrize(\n"
            '    "chain,protocol",\n'
            '    [("ethereum", "uniswap_v3"), ("base", "aerodrome")],\n'
            ")\n"
            "def f():\n"
            "    return 1\n"
        )
        assert plugin._build_allowlist_map(src) == {
            ("f", 6): "per-protocol coverage matrix lives here, can't decompose"
        }

    def test_through_blanks_and_decorators(self):
        src = "# crap-allowlist: domain-irreducible complexity\n\n@cached\n@retry(3)\ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {("f", 5): "domain-irreducible complexity"}

    def test_allowlist_within_contiguous_comment_block_binds(self):
        """The allowlist comment can sit anywhere within the contiguous
        comment/blank/decorator block above the def — intervening unrelated
        doc comments do NOT invalidate it. This matches how people actually
        edit code: someone adding a doc comment shouldn't accidentally
        remove a year-old allowlist. Closest-to-def allowlist wins if there
        are multiple."""
        src = (
            "# crap-allowlist: state machine, see ADR-0042\n"
            "# Updated 2026-Q2 to handle partial fills.\n"
            "def f():\n"
            "    return 1\n"
        )
        assert plugin._build_allowlist_map(src) == {("f", 3): "state machine, see ADR-0042"}

    def test_empty_reason_returns_no_entry(self):
        # Empty reason after the colon — refuse, no allowlist. Map omits f.
        src = "# crap-allowlist:    \ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {}

    def test_no_comment_returns_empty(self):
        src = "def f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {}

    def test_comment_separated_by_code_returns_empty(self):
        # Comment exists but isn't in the contiguous "above-def" zone.
        src = "# crap-allowlist: looks valid but not for f\nx = 1\ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {}

    def test_case_insensitive(self):
        src = "# CRAP-ALLOWLIST: shouting also works\ndef f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {("f", 2): "shouting also works"}

    def test_lookback_window_bounded(self):
        # Allowlist comment 12 lines above the def — outside the 10-line window.
        src = "# crap-allowlist: too far away to be ours\n" + "\n" * 12 + "def f():\n    return 1\n"
        # def is on line 14
        assert plugin._build_allowlist_map(src) == {}

    def test_def_at_top_of_file(self):
        # No lines above to scan — returns empty gracefully.
        src = "def f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {}

    def test_async_function(self):
        """AsyncFunctionDef nodes must also pick up allowlist comments."""
        src = "# crap-allowlist: async dispatcher, awaits N protocol calls in parallel\nasync def f():\n    return 1\n"
        assert plugin._build_allowlist_map(src) == {("f", 2): "async dispatcher, awaits N protocol calls in parallel"}

    def test_class_method(self):
        """Methods inside a class are FunctionDef nodes too. Allowlist applies
        per-method using the method's own def lineno."""
        src = (
            "class Foo:\n"
            "    # crap-allowlist: state machine on Foo, intentional\n"
            "    def bad_method(self):\n"
            "        return 1\n"
        )
        assert plugin._build_allowlist_map(src) == {("bad_method", 3): "state machine on Foo, intentional"}

    def test_syntax_error_returns_empty(self):
        """Uncompilable file → no allowlist info. Plugin stays quiet rather
        than crashing the CI run."""
        assert plugin._build_allowlist_map("def broken(:\n") == {}

    def test_multiple_functions_independent(self):
        """Each function gets its own entry; allowlist on one doesn't apply
        to the other."""
        src = "# crap-allowlist: only for f\ndef f():\n    return 1\n\ndef g():\n    return 2\n"
        assert plugin._build_allowlist_map(src) == {("f", 2): "only for f"}


# ──────────────────────────────────────────────────────────────────────────────
# CrapReporter.violations() — the load-bearing path
# ──────────────────────────────────────────────────────────────────────────────


def _write_low_crap_module(path: Path) -> None:
    """CC=1 function, trivially clean. CRAP <= threshold."""
    path.write_text(
        dedent("""
        def trivial(x):
            return x + 1
    """).lstrip()
    )


def _write_high_crap_module(path: Path) -> None:
    """One function with deeply branched control flow. With 0% coverage this
    blows past the default threshold of 30 (CC ~12+, cov 0% → CRAP >= 1740)."""
    path.write_text(
        dedent('''
        def bad(x, y, z):
            """Intentionally branchy to land CC well above 10."""
            if x > 0:
                if y > 0:
                    if z > 0:
                        return 1
                    elif z < 0:
                        return 2
                    else:
                        return 3
                elif y < 0:
                    return 4
                else:
                    return 5
            elif x < 0:
                if y > 0:
                    return 6
                elif y < 0:
                    return 7
                else:
                    return 8
            else:
                return 9
    ''').lstrip()
    )


class TestCrapReporterViolations:
    """End-to-end: build a fake package on disk, invoke violations(), assert
    the right files trip and the wrong files don't."""

    def _make_reporter(
        self, tmp_path: Path, threshold: float = 30.0, omit: list[str] | None = None
    ) -> plugin.CrapReporter:
        cfg = plugin._Config(
            threshold=threshold,
            coverage_data=str(tmp_path / ".coverage_missing"),
            package_root=str(tmp_path / "almanak"),
            honor_omit=True,
            omit_globs=list(omit or []),
        )
        return plugin.CrapReporter(config=cfg)

    @staticmethod
    def _seed_uncovered(reporter: plugin.CrapReporter, target: Path) -> None:
        """Mark `target` as 'tracked by coverage, zero lines executed' — the
        state coverage.py would record for an in-source file with no tests.
        Treats every line in the file as executable (good enough for the
        single-function fixtures used by these tests; production reads the
        true executable set from `Coverage.analysis2`)."""
        line_count = len(target.read_text().splitlines())
        key = str(target.resolve())
        reporter._coverage_loaded = True
        reporter._coverage_available = True
        reporter._executable_by_path[key] = set(range(1, line_count + 1))
        reporter._executed_by_path[key] = set()

    def test_low_crap_function_yields_no_violations(self, tmp_path: Path):
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "trivial.py"
        _write_low_crap_module(target)
        reporter = self._make_reporter(tmp_path)
        self._seed_uncovered(reporter, target)
        assert reporter.violations(str(target)) == []

    def test_high_crap_function_yields_per_line_violations(self, tmp_path: Path):
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.py"
        _write_high_crap_module(target)
        reporter = self._make_reporter(tmp_path)
        self._seed_uncovered(reporter, target)
        violations = reporter.violations(str(target))
        assert violations, "Expected violations for high-CRAP function"
        # Per-line emission: every line in the function range gets a Violation.
        # Our fixture's `bad()` spans the entire file body.
        lines = {v.line for v in violations}
        # Function starts on line 1 and runs for 20+ lines. Don't pin exact
        # numbers (radon end-of-function semantics shift across versions); just
        # verify the range is contiguous and non-trivial.
        assert min(lines) == 1
        assert len(lines) >= 10
        assert max(lines) - min(lines) + 1 == len(lines)
        # Message must surface CRAP score + function name for the human reader.
        msg = violations[0].message
        assert "CRAP=" in msg
        assert "bad" in msg

    def test_path_outside_package_root_yields_no_violations(self, tmp_path: Path):
        # File outside `almanak/` package_root.
        outsider = tmp_path / "outside.py"
        _write_high_crap_module(outsider)
        reporter = self._make_reporter(tmp_path)
        assert reporter.violations(str(outsider)) == []

    def test_omitted_file_yields_no_violations(self, tmp_path: Path, monkeypatch):
        """The omit-glob check runs BEFORE the existence + scope checks, so
        a relative `src_path` matching an omit glob short-circuits without
        ever touching the filesystem. chdir into tmp_path is still needed
        because diff-cover hands us git-root-relative paths and the test
        contract is "matches glob → return [] regardless of file presence."
        """
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "test_bad.py"
        _write_high_crap_module(target)
        monkeypatch.chdir(tmp_path)
        reporter = self._make_reporter(tmp_path, omit=["almanak/test_*.py"])
        # `src_path` is what diff-cover hands us — the relative form.
        assert reporter.violations("almanak/test_bad.py") == []
        # And the same file WITHOUT the omit pattern would emit violations,
        # proving the test isn't passing on a different short-circuit.
        reporter_no_omit = self._make_reporter(tmp_path, omit=[])
        self._seed_uncovered(reporter_no_omit, target)
        assert reporter_no_omit.violations("almanak/test_bad.py") != []

    def test_non_python_extension_yields_no_violations(self, tmp_path: Path):
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.txt"
        target.write_text("plain text")
        reporter = self._make_reporter(tmp_path)
        assert reporter.violations(str(target)) == []

    def test_threshold_raised_above_score_yields_no_violations(self, tmp_path: Path):
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.py"
        _write_high_crap_module(target)
        reporter = self._make_reporter(tmp_path, threshold=10_000)
        self._seed_uncovered(reporter, target)
        assert reporter.violations(str(target)) == []

    def test_allowlisted_function_suppresses_violations_and_logs_reason(self, tmp_path: Path, capsys):
        """End-to-end allowlist: function above threshold + valid allowlist
        comment → zero Violations + one stderr audit line carrying the reason."""
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.py"
        # Prepend the allowlist comment to the high-CRAP fixture.
        original = "def bad(x, y, z):"
        _write_high_crap_module(target)
        body = target.read_text()
        target.write_text(f"# crap-allowlist: domain-irreducible state machine, see ADR-0042\n{body}")
        # Sanity: original fixture still has the def.
        assert original in body
        reporter = self._make_reporter(tmp_path)
        self._seed_uncovered(reporter, target)
        assert reporter.violations(str(target)) == []
        captured = capsys.readouterr()
        # Audit line must surface the reason and the function identity so
        # reviewers / governance can grep the CI logs.
        assert "crap-allowlist:" in captured.err
        assert "bad" in captured.err
        assert "domain-irreducible state machine" in captured.err
        assert "ADR-0042" in captured.err

    def test_allowlist_with_empty_reason_does_not_suppress(self, tmp_path: Path, capsys):
        """A bare `# crap-allowlist:` (no reason) is rejected — the comment
        is treated as if it weren't there. This is the discipline lever:
        forces authors to articulate why they're bypassing the gate."""
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.py"
        _write_high_crap_module(target)
        body = target.read_text()
        target.write_text(f"# crap-allowlist:    \n{body}")
        reporter = self._make_reporter(tmp_path)
        self._seed_uncovered(reporter, target)
        assert reporter.violations(str(target)) != []
        # No audit line — empty-reason isn't a valid allowlist.
        captured = capsys.readouterr()
        assert "crap-allowlist:" not in captured.err

    def test_violation_message_mentions_allowlist_syntax(self, tmp_path: Path):
        """When the gate fires, the message must tell the author exactly what
        the escape hatch is. Pinning the literal so we don't drift away from
        the docstring example again."""
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.py"
        _write_high_crap_module(target)
        reporter = self._make_reporter(tmp_path)
        self._seed_uncovered(reporter, target)
        violations = reporter.violations(str(target))
        assert violations
        assert "# crap-allowlist:" in violations[0].message

    def test_missing_coverage_data_returns_empty_with_warning(self, tmp_path: Path, capsys):
        """When .coverage is absent (local invocation outside the Makefile
        guard), the plugin must emit a single stderr warning and return [] for
        every file — NOT flood the diff with false positives by treating every
        function as 0% covered. The Makefile target's `@test -f .coverage`
        guard is the primary defense; this branch is the fallback for direct
        CLI users."""
        pkg = tmp_path / "almanak"
        pkg.mkdir()
        target = pkg / "bad.py"
        _write_high_crap_module(target)
        reporter = self._make_reporter(tmp_path)
        violations = reporter.violations(str(target))
        assert violations == []
        captured = capsys.readouterr()
        assert "coverage data file not found" in captured.err
        # Second call must NOT re-emit the warning (lazy-load idempotent).
        reporter.violations(str(target))
        captured2 = capsys.readouterr()
        assert captured2.err == ""


# ──────────────────────────────────────────────────────────────────────────────
# CrapReporter.measured_lines()
# ──────────────────────────────────────────────────────────────────────────────


class TestMeasuredLines:
    """Returning None tells diff-cover 'every changed line is measured' —
    standard linter idiom. Pinned because diff-cover's downstream behavior
    depends on the None sentinel, not on an empty list."""

    def test_returns_none(self, tmp_path: Path):
        cfg = plugin._Config(coverage_data=str(tmp_path / ".coverage"))
        reporter = plugin.CrapReporter(config=cfg)
        assert reporter.measured_lines("anything.py") is None


# ──────────────────────────────────────────────────────────────────────────────
# Plugin entry point
# ──────────────────────────────────────────────────────────────────────────────


class TestEntryPoint:
    def test_factory_returns_reporter_instance(self):
        # Mock _load_config so we don't depend on the repo's pyproject.toml.
        with patch.object(plugin, "_load_config", return_value=plugin._Config()):
            reporter = plugin.diff_cover_report_quality()
        assert isinstance(reporter, plugin.CrapReporter)

    def test_factory_accepts_arbitrary_kwargs(self):
        """pluggy may pass `reports=` / `options=` today and more tomorrow.
        Plugin must not break on a new kwarg."""
        with patch.object(plugin, "_load_config", return_value=plugin._Config()):
            reporter = plugin.diff_cover_report_quality(
                reports=[MagicMock()],
                options=MagicMock(),
                future_kwarg="anything",
            )
        assert isinstance(reporter, plugin.CrapReporter)

    def test_reporter_advertises_python_extension(self):
        assert plugin.CrapReporter.supported_extensions == ["py"]

    def test_reporter_name_is_crap(self, tmp_path: Path):
        cfg = plugin._Config(coverage_data=str(tmp_path / ".coverage"))
        reporter = plugin.CrapReporter(config=cfg)
        assert reporter.name() == "crap"
