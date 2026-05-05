"""Unit tests for ``scripts/crap_score.py``.

Pins the CRAP-formula correctness, the per-function coverage-ratio computation,
and the data-quality guards (stale data warning + narrow-scope warning) that
exist specifically to prevent the recurring "phantom hotspot" bug
(`docs/internal/coverage-improvement-plan.md` §7).
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# Load the script as a module — it lives under scripts/ which is not on sys.path.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "crap_score.py"
_spec = importlib.util.spec_from_file_location("crap_score", _SCRIPT_PATH)
crap_score = importlib.util.module_from_spec(_spec)
sys.modules["crap_score"] = crap_score
_spec.loader.exec_module(crap_score)


# ──────────────────────────────────────────────────────────────────────────────
# crap()
# ──────────────────────────────────────────────────────────────────────────────


class TestCrapFormula:
    """CRAP(f) = CC^2 * (1 - cov)^3 + CC."""

    def test_fully_covered_function_equals_complexity(self):
        # cov=1.0 → (1-1)^3=0 → just CC.
        assert crap_score.crap(10, 1.0) == 10
        assert crap_score.crap(1, 1.0) == 1

    def test_zero_covered_function_quadratic_in_complexity(self):
        # cov=0 → CC^2 + CC.
        assert crap_score.crap(10, 0.0) == 100 + 10
        assert crap_score.crap(20, 0.0) == 400 + 20

    def test_partial_coverage(self):
        # CC=10, cov=0.5 → 100 * 0.125 + 10 = 22.5
        assert crap_score.crap(10, 0.5) == pytest.approx(22.5)


# ──────────────────────────────────────────────────────────────────────────────
# function_coverage()
# ──────────────────────────────────────────────────────────────────────────────


def _make_func(lineno: int, endline: int):
    """Minimal radon Function-shaped object — only the two attrs the helper reads."""
    return SimpleNamespace(lineno=lineno, endline=endline)


class TestFunctionCoverage:
    def test_fully_covered(self):
        func = _make_func(1, 5)
        executed = {1, 2, 3, 4, 5}
        total, ratio = crap_score.function_coverage(executed, func)
        assert total == 5
        assert ratio == 1.0

    def test_half_covered(self):
        func = _make_func(10, 13)  # 4 lines: 10, 11, 12, 13
        executed = {10, 12}  # 2 of 4
        total, ratio = crap_score.function_coverage(executed, func)
        assert total == 4
        assert ratio == 0.5

    def test_no_coverage(self):
        func = _make_func(10, 20)
        total, ratio = crap_score.function_coverage(set(), func)
        assert total == 11
        assert ratio == 0.0

    def test_lines_outside_range_are_ignored(self):
        # Lines 1-100 executed, but the function is only lines 50-55.
        func = _make_func(50, 55)
        executed = set(range(1, 101))
        total, ratio = crap_score.function_coverage(executed, func)
        assert total == 6
        assert ratio == 1.0

    def test_zero_line_function_returns_full_coverage_default(self):
        # endline < lineno is impossible in real code but the helper must not div-by-zero.
        func = _make_func(10, 9)  # total = 9 - 10 + 1 = 0
        total, ratio = crap_score.function_coverage(set(), func)
        assert total == 0
        assert ratio == 1.0  # default — caller filters total==0 records anyway


# ──────────────────────────────────────────────────────────────────────────────
# check_coverage_freshness — data-quality guards
# ──────────────────────────────────────────────────────────────────────────────


class TestCheckCoverageFreshness:
    def test_missing_file_raises_systemexit_with_actionable_message(self, tmp_path):
        with pytest.raises(SystemExit) as exc:
            crap_score.check_coverage_freshness(
                coverage_path=tmp_path / "does-not-exist",
                package_root=tmp_path,
                max_age_hours=24.0,
                min_measured_fraction=0.5,
            )
        assert "make test-coverage" in str(exc.value)

    def test_fresh_data_with_full_scope_no_warnings(self, tmp_path):
        # Build a tiny fake package + measure all of it.
        pkg = tmp_path / "fakepkg"
        pkg.mkdir()
        (pkg / "a.py").write_text("def f(): return 1\n")
        (pkg / "b.py").write_text("def g(): return 2\n")

        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        # Make timestamp recent (touch already does that).

        # Patch Coverage.load + measured_files to claim full scope.
        with patch.object(crap_score, "Coverage") as fake_cov_cls:
            fake_data = SimpleNamespace(measured_files=lambda: [str(pkg / "a.py"), str(pkg / "b.py")])
            fake_cov = SimpleNamespace(
                load=lambda: None,
                get_data=lambda: fake_data,
            )
            fake_cov_cls.return_value = fake_cov
            cov, warnings = crap_score.check_coverage_freshness(
                coverage_path=coverage_file,
                package_root=pkg,
                max_age_hours=24.0,
                min_measured_fraction=0.5,
            )
        assert warnings == []

    def test_stale_data_emits_age_warning(self, tmp_path):
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "a.py").write_text("def f(): return 1\n")

        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        # Backdate the file to 48h ago.
        old = Path(__file__).stat().st_mtime - 48 * 3600
        os.utime(coverage_file, (old, old))

        with patch.object(crap_score, "Coverage") as fake_cov_cls:
            fake_data = SimpleNamespace(measured_files=lambda: [str(pkg / "a.py")])
            fake_cov_cls.return_value = SimpleNamespace(load=lambda: None, get_data=lambda: fake_data)
            _, warnings = crap_score.check_coverage_freshness(
                coverage_path=coverage_file,
                package_root=pkg,
                max_age_hours=24.0,
                min_measured_fraction=0.5,
            )
        assert any("stale" in w.lower() or "old" in w.lower() for w in warnings)
        assert any("crap-fresh" in w for w in warnings)

    def test_narrow_scope_emits_fraction_warning(self, tmp_path):
        # 10 files in package, only 2 measured → 20% scope.
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        for i in range(10):
            (pkg / f"f{i}.py").write_text("def x(): return 1\n")

        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()

        with patch.object(crap_score, "Coverage") as fake_cov_cls:
            measured = [str(pkg / "f0.py"), str(pkg / "f1.py")]  # only 2 of 10
            fake_data = SimpleNamespace(measured_files=lambda: measured)
            fake_cov_cls.return_value = SimpleNamespace(load=lambda: None, get_data=lambda: fake_data)
            _, warnings = crap_score.check_coverage_freshness(
                coverage_path=coverage_file,
                package_root=pkg,
                max_age_hours=24.0,
                min_measured_fraction=0.5,
            )
        assert any("narrow-scope" in w for w in warnings)
        assert any("2/10" in w or "20%" in w for w in warnings)
        assert any("crap-fresh" in w for w in warnings)

    def test_min_fraction_threshold_can_be_relaxed(self, tmp_path):
        # Same narrow scope, but threshold = 0.1 → no warning.
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        for i in range(10):
            (pkg / f"f{i}.py").write_text("def x(): return 1\n")

        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()

        with patch.object(crap_score, "Coverage") as fake_cov_cls:
            measured = [str(pkg / "f0.py"), str(pkg / "f1.py")]
            fake_data = SimpleNamespace(measured_files=lambda: measured)
            fake_cov_cls.return_value = SimpleNamespace(load=lambda: None, get_data=lambda: fake_data)
            _, warnings = crap_score.check_coverage_freshness(
                coverage_path=coverage_file,
                package_root=pkg,
                max_age_hours=24.0,
                min_measured_fraction=0.1,  # relaxed
            )
        assert not any("narrow-scope" in w for w in warnings)

    def test_directory_path_rejected_as_coverage_file(self, tmp_path):
        # is_file() guard: a directory at coverage_path must NOT pass the check.
        # exists() would let it through and explode later inside Coverage().
        coverage_dir = tmp_path / "not_a_file_dir"
        coverage_dir.mkdir()
        with pytest.raises(SystemExit) as exc:
            crap_score.check_coverage_freshness(
                coverage_path=coverage_dir,
                package_root=tmp_path,
                max_age_hours=24.0,
                min_measured_fraction=0.5,
            )
        assert "make test-coverage" in str(exc.value)

    def test_caller_supplied_package_files_skip_filesystem_walk(self, tmp_path):
        # When package_files is passed in, the function must NOT re-walk
        # package_root. Verify by giving an empty list and a populated
        # package_root — the empty list should win and suppress the warning.
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        for i in range(10):
            (pkg / f"f{i}.py").write_text("def x(): return 1\n")

        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()

        with patch.object(crap_score, "Coverage") as fake_cov_cls:
            fake_data = SimpleNamespace(measured_files=lambda: [])
            fake_cov_cls.return_value = SimpleNamespace(load=lambda: None, get_data=lambda: fake_data)
            _, warnings = crap_score.check_coverage_freshness(
                coverage_path=coverage_file,
                package_root=pkg,
                max_age_hours=24.0,
                min_measured_fraction=0.5,
                package_files=[],  # explicit empty → bypass the scope check
            )
        # Empty list → fraction check skipped → no narrow-scope warning.
        assert not any("narrow-scope" in w for w in warnings)


# ──────────────────────────────────────────────────────────────────────────────
# main() — CLI behavior
# ──────────────────────────────────────────────────────────────────────────────


def _build_minimal_pkg(tmp_path: Path) -> Path:
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "a.py").write_text("def f():\n    return 1\n")
    return pkg


def _patch_argv_and_freshness(
    monkeypatch,
    pkg: Path,
    coverage_file: Path,
    *,
    strict: bool,
    warnings: list[str],
):
    """Patch sys.argv + check_coverage_freshness for a main() invocation.

    The fake Coverage object also stubs ``data.lines()`` so that the main()
    body (which runs through to the per-file CRAP loop when strict is False
    or warnings are empty) does not blow up — we only care about the exit code.
    """
    fake_data = SimpleNamespace(
        measured_files=lambda: [str(pkg / "a.py")],
        lines=lambda _path: [1, 2],
    )
    fake_cov = SimpleNamespace(load=lambda: None, get_data=lambda: fake_data)
    monkeypatch.setattr(
        crap_score,
        "check_coverage_freshness",
        lambda **_kwargs: (fake_cov, list(warnings)),
    )
    argv = [
        "crap_score.py",
        "--package",
        str(pkg),
        "--coverage",
        str(coverage_file),
    ]
    if strict:
        argv.append("--strict")
    monkeypatch.setattr(sys, "argv", argv)


class TestMainStrictExit:
    """The --strict CLI flag is the contract for future CI gating."""

    def test_strict_with_warnings_returns_exit_code_2(self, tmp_path, monkeypatch):
        pkg = _build_minimal_pkg(tmp_path)
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        _patch_argv_and_freshness(
            monkeypatch,
            pkg,
            coverage_file,
            strict=True,
            warnings=["stale data"],
        )
        assert crap_score.main() == 2

    def test_warnings_without_strict_returns_zero(self, tmp_path, monkeypatch):
        pkg = _build_minimal_pkg(tmp_path)
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        _patch_argv_and_freshness(
            monkeypatch,
            pkg,
            coverage_file,
            strict=False,
            warnings=["stale data"],
        )
        # Warnings printed to stderr but exit is 0 (advisory mode).
        assert crap_score.main() == 0

    def test_strict_without_warnings_returns_zero(self, tmp_path, monkeypatch):
        pkg = _build_minimal_pkg(tmp_path)
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        _patch_argv_and_freshness(
            monkeypatch,
            pkg,
            coverage_file,
            strict=True,
            warnings=[],
        )
        assert crap_score.main() == 0


class TestMainArgValidation:
    """Threshold + path arguments must fail fast, not silently distort the analysis."""

    def test_min_measured_fraction_above_one_is_rejected(self, tmp_path, monkeypatch):
        pkg = _build_minimal_pkg(tmp_path)
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(pkg),
                "--coverage",
                str(coverage_file),
                "--min-measured-fraction",
                "1.5",
            ],
        )
        with pytest.raises(SystemExit) as exc:
            crap_score.main()
        # argparse.error() exits with code 2.
        assert exc.value.code == 2

    def test_min_measured_fraction_below_zero_is_rejected(self, tmp_path, monkeypatch):
        pkg = _build_minimal_pkg(tmp_path)
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(pkg),
                "--coverage",
                str(coverage_file),
                "--min-measured-fraction",
                "-0.1",
            ],
        )
        with pytest.raises(SystemExit) as exc:
            crap_score.main()
        assert exc.value.code == 2

    def test_zero_max_age_hours_is_rejected(self, tmp_path, monkeypatch):
        pkg = _build_minimal_pkg(tmp_path)
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(pkg),
                "--coverage",
                str(coverage_file),
                "--max-age-hours",
                "0",
            ],
        )
        with pytest.raises(SystemExit) as exc:
            crap_score.main()
        assert exc.value.code == 2

    def test_missing_package_dir_returns_exit_code_1(self, tmp_path, monkeypatch):
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(tmp_path / "does_not_exist"),
                "--coverage",
                str(coverage_file),
            ],
        )
        assert crap_score.main() == 1

    def test_empty_package_dir_returns_exit_code_1(self, tmp_path, monkeypatch):
        empty_pkg = tmp_path / "empty_pkg"
        empty_pkg.mkdir()
        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(empty_pkg),
                "--coverage",
                str(coverage_file),
            ],
        )
        assert crap_score.main() == 1


# ──────────────────────────────────────────────────────────────────────────────
# load_omit_globs() / filter_omitted()
# ──────────────────────────────────────────────────────────────────────────────


class TestLoadOmitGlobs:
    """The omit list is the parity bridge between this report and the
    diff-cover gate: both must consult the same `[tool.coverage.run].omit`
    so make crap and the PR-time CRAP gate score against the same set of
    files. Otherwise the report's top-N is dominated by phantoms (the bug
    §7 of the coverage plan was added to prevent)."""

    def test_missing_pyproject_returns_empty_list(self, tmp_path):
        assert crap_score.load_omit_globs(tmp_path / "no.toml") == []

    def test_no_section_returns_empty_list(self, tmp_path):
        path = tmp_path / "pyproject.toml"
        path.write_text('[project]\nname = "x"\n')
        assert crap_score.load_omit_globs(path) == []

    def test_globs_loaded_in_declaration_order(self, tmp_path):
        path = tmp_path / "pyproject.toml"
        path.write_text(
            "[tool.coverage.run]\n"
            "omit = [\n"
            '  "almanak/demo_strategies/*",\n'
            '  "almanak/framework/dashboard/pages/*",\n'
            '  "almanak/**/test_*.py",\n'
            "]\n"
        )
        assert crap_score.load_omit_globs(path) == [
            "almanak/demo_strategies/*",
            "almanak/framework/dashboard/pages/*",
            "almanak/**/test_*.py",
        ]

    def test_string_omit_value_treated_as_single_pattern(self, tmp_path):
        """Regression for PR #2089 gemini medium: coverage.py allows omit
        to be a string OR a list. Naive ``list(omit)`` on a string would
        iterate the characters, producing pattern set
        ``["a", "l", "m", ...]`` — none of which match anything. Catch the
        string case and wrap in a single-element list."""
        path = tmp_path / "pyproject.toml"
        path.write_text('[tool.coverage.run]\nomit = "almanak/demo_strategies/*"\n')
        assert crap_score.load_omit_globs(path) == ["almanak/demo_strategies/*"]


class TestFilterOmitted:
    """Filter must operate on repo-root-relative paths (matching how the omit
    list is written) and use coverage's own GlobMatcher so semantics align
    with the diff-cover plugin and with `[tool.coverage.run].omit` itself."""

    def _make_pkg(self, tmp_path: Path) -> tuple[Path, Path, list[Path]]:
        """Build a fixture repo: ``tmp_path/`` is the repo root,
        ``tmp_path/almanak/`` is the package."""
        repo_root = tmp_path
        pkg = repo_root / "almanak"
        (pkg / "demo_strategies").mkdir(parents=True)
        (pkg / "framework" / "dashboard" / "pages").mkdir(parents=True)
        (pkg / "framework" / "runner").mkdir(parents=True)
        files = [
            pkg / "demo_strategies" / "foo.py",
            pkg / "framework" / "dashboard" / "pages" / "overview.py",
            pkg / "framework" / "runner" / "strategy_runner.py",
            pkg / "framework" / "accounting_test.py",
        ]
        for f in files:
            f.write_text("# stub\n")
        return repo_root, pkg, [p.resolve() for p in files]

    def test_no_globs_passes_all_through(self, tmp_path):
        repo_root, _pkg, files = self._make_pkg(tmp_path)
        kept, dropped = crap_score.filter_omitted(files, repo_root, [])
        assert kept == files
        assert dropped == 0

    def test_drops_matching_files_relative_to_repo_root(self, tmp_path):
        repo_root, _pkg, files = self._make_pkg(tmp_path)
        kept, dropped = crap_score.filter_omitted(files, repo_root, ["almanak/demo_strategies/*"])
        assert dropped == 1
        kept_names = {p.name for p in kept}
        assert "foo.py" not in kept_names
        assert "overview.py" in kept_names
        assert "strategy_runner.py" in kept_names

    def test_double_star_globs_match_nested(self, tmp_path):
        """Regression for the fnmatch-vs-GlobMatcher trap: `**` semantics
        must traverse subdirectories the way coverage.py would."""
        repo_root, _pkg, files = self._make_pkg(tmp_path)
        kept, dropped = crap_score.filter_omitted(files, repo_root, ["almanak/framework/**/pages/*"])
        assert dropped == 1
        assert all(p.name != "overview.py" for p in kept)

    def test_multiple_globs_combine(self, tmp_path):
        repo_root, _pkg, files = self._make_pkg(tmp_path)
        kept, dropped = crap_score.filter_omitted(
            files,
            repo_root,
            ["almanak/demo_strategies/*", "almanak/framework/dashboard/pages/*"],
        )
        assert dropped == 2
        assert {p.name for p in kept} == {"strategy_runner.py", "accounting_test.py"}

    def test_files_outside_repo_root_are_kept(self, tmp_path):
        repo_root, _pkg, files = self._make_pkg(tmp_path)
        outsider = tmp_path.parent / "elsewhere.py"  # cannot relative_to(repo_root)
        kept, dropped = crap_score.filter_omitted(
            files + [outsider],
            repo_root,
            ["almanak/demo_strategies/*"],
        )
        assert outsider in kept
        assert dropped == 1

    def test_subpackage_path_still_matches_repo_root_relative_globs(self, tmp_path):
        """Regression for PR #2089 codex P2 / gemini medium: with
        ``--package=almanak/framework`` the previous code derived
        ``repo_root = package_root.parent = tmp_path/almanak/``, so files
        resolved as ``framework/...`` and stopped matching omit globs
        written as ``almanak/framework/...``. The fix is to anchor matching
        at the directory that owns ``pyproject.toml`` — passed explicitly
        via this function's ``repo_root`` parameter."""
        repo_root, _pkg, files = self._make_pkg(tmp_path)
        # Caller passes the actual repo root, NOT framework's parent.
        kept, dropped = crap_score.filter_omitted(
            files,
            repo_root,
            ["almanak/framework/dashboard/pages/*"],
        )
        assert dropped == 1
        assert all(p.name != "overview.py" for p in kept)


class TestMainHonorsOmitGlobs:
    """End-to-end: with a pyproject.toml carrying omit globs, files matching
    those globs must NOT appear in the analyzed set. The default is to
    honor; --no-honor-omit opts out for a raw view."""

    def _setup(self, tmp_path: Path) -> tuple[Path, Path, Path, Path]:
        pkg = tmp_path / "almanak"
        (pkg / "demo_strategies").mkdir(parents=True)
        (pkg / "framework").mkdir(parents=True)
        # A "real" framework file we expect to keep.
        kept_file = pkg / "framework" / "kept.py"
        kept_file.write_text("def f():\n    return 1\n")
        # An "omitted" demo file we expect to drop.
        omitted_file = pkg / "demo_strategies" / "omitted.py"
        omitted_file.write_text("def g():\n    return 2\n")

        coverage_file = tmp_path / ".coverage"
        coverage_file.touch()
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text('[tool.coverage.run]\nomit = ["almanak/demo_strategies/*"]\n')
        return pkg, coverage_file, pyproject, omitted_file

    def test_default_honor_omit_drops_matching_files(
        self,
        tmp_path,
        monkeypatch,
        capsys,
    ):
        pkg, coverage_file, pyproject, omitted_file = self._setup(tmp_path)

        captured_files: list[Path] = []

        def spy(*, package_files, **kwargs):
            captured_files.extend(package_files)
            # Don't actually load coverage — return a stub.
            return SimpleNamespace(
                get_data=lambda: SimpleNamespace(
                    measured_files=lambda: [],
                    lines=lambda _f: [],
                )
            ), []

        monkeypatch.setattr(crap_score, "check_coverage_freshness", spy)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(pkg),
                "--coverage",
                str(coverage_file),
                "--pyproject",
                str(pyproject),
            ],
        )
        crap_score.main()

        assert omitted_file.resolve() not in captured_files
        assert any(p.name == "kept.py" for p in captured_files)
        # info line surfaces the drop count for transparency.
        captured = capsys.readouterr()
        assert "dropped 1 files matching" in captured.err

    def test_no_honor_omit_keeps_matching_files(
        self,
        tmp_path,
        monkeypatch,
    ):
        pkg, coverage_file, pyproject, omitted_file = self._setup(tmp_path)

        captured_files: list[Path] = []

        def spy(*, package_files, **kwargs):
            captured_files.extend(package_files)
            return SimpleNamespace(
                get_data=lambda: SimpleNamespace(
                    measured_files=lambda: [],
                    lines=lambda _f: [],
                )
            ), []

        monkeypatch.setattr(crap_score, "check_coverage_freshness", spy)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "crap_score.py",
                "--package",
                str(pkg),
                "--coverage",
                str(coverage_file),
                "--pyproject",
                str(pyproject),
                "--no-honor-omit",
            ],
        )
        crap_score.main()

        assert omitted_file.resolve() in captured_files
