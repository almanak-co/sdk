import ast
import json
import subprocess
from fractions import Fraction
from pathlib import Path

import pytest
from coverage import Coverage

from scripts import check_crap
from scripts.check_crap import (
    Functions,
    Score,
    arguments,
    changed_lines,
    collect_coverage,
    main,
    python_files,
    score_file,
)
from tests.evidence_guard import EVIDENCE


@pytest.mark.parametrize(
    ("complexity", "covered", "statements", "expected"),
    [(1, 1, 1, 1), (10, 0, 4, 110), (10, 2, 4, Fraction(45, 2)), (30, 4, 4, 30)],
)
def test_crap_formula(complexity, covered, statements, expected):
    score = Score("example.py", "example", 1, complexity, statements, covered)
    assert score.crap == expected


@pytest.mark.parametrize(
    ("complexity", "covered", "statements", "violation"),
    [(5, 0, 1, False), (30, 1, 1, False), (31, 1, 1, True), (30, 49999, 50000, True)],
)
def test_threshold_is_inclusive_and_never_rounded(complexity, covered, statements, violation):
    score = Score("example.py", "example", 1, complexity, statements, covered)
    assert score.record(30)["violation"] is violation


def test_empty_body_does_not_claim_measured_coverage():
    score = Score("example.py", "example", 1, 1, 0, 0)
    assert score.coverage == 0
    assert score.record(30)["statements"] == 0


def test_discovers_nested_async_methods_and_repeated_names():
    source = """class Example:
    @property
    def value(self):
        return 1
    @value.setter
    def value(self, value):
        self._value = value
    async def outer(self):
        async def inner(value):
            if value:
                return value
        class Local:
            def method(self):
                return 2
        return inner
"""
    visitor = Functions()
    visitor.visit(ast.parse(source))
    rows = [(item.name, item.line, item.complexity) for item in visitor.functions]
    assert rows == [
        ("Example.value", 3, 1),
        ("Example.value", 6, 1),
        ("Example.outer", 8, 1),
        ("Example.outer.inner", 9, 2),
        ("Example.outer.Local.method", 13, 1),
    ]
    assert visitor.functions[2].lines == {9, 12, 15}


@pytest.fixture
def repository(tmp_path, monkeypatch):
    # A commit hook exports Git paths for its own checkout. Temporary repositories
    # must not inherit them, or even git init can modify the real repository.
    variables = subprocess.check_output(["git", "rev-parse", "--local-env-vars"], text=True)
    for name in variables.splitlines():
        monkeypatch.delenv(name, raising=False)
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "pyproject.toml").write_text("[tool.crap]\nmax-score = 30\n")
    (tmp_path / ".gitignore").write_text("ignored/\nartifacts/\n")
    return tmp_path


def test_discovers_tracked_and_untracked_files_in_any_directory(repository):
    for name in ("tracked.py", "deleted.py", "tests/test_a.py", "new dir/new.py", "ignored/a.py"):
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def example():\n    return 1\n")
    subprocess.run(["git", "add", "tracked.py", "deleted.py"], cwd=repository, check=True)
    (repository / "deleted.py").unlink()
    assert [path.relative_to(repository).as_posix() for path in python_files(repository)] == [
        "new dir/new.py",
        "tests/test_a.py",
        "tracked.py",
    ]


def test_refuses_symlinked_source(repository, tmp_path):
    (repository / "linked.py").symlink_to(tmp_path / "missing.py")
    with pytest.raises(ValueError, match="regular repository file"):
        python_files(repository)


def test_no_source_cannot_pass(repository):
    with pytest.raises(ValueError, match="No repository Python files"):
        python_files(repository)


def test_coverage_is_per_body_including_unimported_and_excluded_code(tmp_path):
    path = tmp_path / "sample.py"
    path.write_text("""def decorator(function):
    return function
@decorator
def unused():
    return 1
def outer():
    def inner(value):
        if value:
            return 1
        return 2
    return inner
def partial(value):
    if value:
        return 1
    return 2  # pragma: no cover
""")
    unimported = tmp_path / "unimported.py"
    unimported.write_text("def never():  # pragma: no cover\n    return 1\n")
    (tmp_path / "test_sample.py").write_text(
        "import sample\ndef test_sample():\n"
        "    assert callable(sample.outer())\n    assert sample.partial(True) == 1\n"
    )
    coverage = collect_coverage(tmp_path, [path, unimported], tmp_path, ())
    scores = {row.name: row for row in score_file(tmp_path, path, coverage)}
    assert scores["unused"].coverage == 0
    assert scores["outer"].coverage == 1
    assert scores["outer.inner"].coverage == 0
    assert scores["partial"].coverage == Fraction(2, 3)
    assert scores["partial"].crap == Fraction(58, 27)
    assert score_file(tmp_path, unimported, coverage)[0].coverage == 0


@pytest.fixture
def runnable_repository(repository):
    (repository / "sample.py").write_text("def example():\n    return 1\n")
    (repository / "test_sample.py").write_text(
        "from sample import example\ndef test_example():\n    assert example() == 1\n"
    )
    return repository


def test_command_runs_fresh_tests_and_reports_all_functions(runnable_repository):
    assert main(runnable_repository) == 0
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["passed"] is True
    assert report["maximum"] == 30
    assert report["function_count"] == 2
    assert all(row["coverage"] == 1 for row in report["functions"])
    assert (runnable_repository / "artifacts/crap/coverage.json").is_file()


def test_parallel_coverage_combines_worker_hits(runnable_repository):
    (runnable_repository / "sample.py").write_text(
        "def example(value):\n    if value:\n        return 1\n    return 2\n"
    )
    for name, value, expected in (("yes", True, 1), ("no", False, 2)):
        (runnable_repository / f"test_{name}.py").write_text(
            "from sample import example\n"
            f"def test_{name}():\n    assert example({value}) == {expected}\n"
        )
    (runnable_repository / "test_sample.py").unlink()
    assert main(runnable_repository, ("-n", "2", "--dist=loadfile")) == 0
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["function_count"] == 3
    assert all(row["coverage"] == 1 for row in report["functions"])


def test_command_fails_for_unimported_complex_function(runnable_repository):
    (runnable_repository / "untested.py").write_text(
        "def complex(value):\n" + "    if value:\n        value -= 1\n" * 5 + "    return value\n"
    )
    assert main(runnable_repository) == 1
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["violation_count"] == 1
    assert report["functions"][0]["crap"] == 42
    assert report["functions"][0]["path"] == "untested.py"


@pytest.mark.parametrize("pytest_args", [(), ("-n", "2")])
def test_failed_tests_remove_previous_success_report(runnable_repository, pytest_args):
    output = runnable_repository / "artifacts/crap"
    output.mkdir(parents=True)
    for name in ("scores.json", "coverage.json"):
        (output / name).write_text('{"passed": true}')
    (runnable_repository / "test_sample.py").write_text("def test_fail():\n    assert False\n")
    assert main(runnable_repository, pytest_args) == 2
    assert list(output.iterdir()) == []


def test_source_changes_during_collection_cannot_use_old_coverage(runnable_repository, monkeypatch):
    def collect_and_modify(*args):
        coverage = collect_coverage(*args)
        (runnable_repository / "sample.py").write_text("def example():\n    return 2\n")
        return coverage

    monkeypatch.setattr(check_crap, "collect_coverage", collect_and_modify)
    assert main(runnable_repository) == 2
    assert not (runnable_repository / "artifacts/crap/scores.json").exists()


@pytest.mark.parametrize("maximum", ["31", "inf", "true", "0"])
def test_invalid_threshold_cannot_disable_the_gate(runnable_repository, maximum):
    (runnable_repository / "pyproject.toml").write_text(f"[tool.crap]\nmax-score = {maximum}\n")
    assert main(runnable_repository) == 2


def test_unparseable_source_is_a_measurement_error(tmp_path):
    path = tmp_path / "broken.py"
    path.write_text("def broken(:\n")
    with pytest.raises(SyntaxError):
        score_file(tmp_path, path, Coverage(config_file=False))


def test_sharded_runs_score_combined_coverage_without_rerunning_tests(runnable_repository):
    import sys

    config = runnable_repository / "shard.coveragerc"
    data = runnable_repository / ".coverage.shard-1"
    assert main(runnable_repository, coveragerc=config, data_file=data) == 0
    # The score reads statement hits only, so the shard config must not pay
    # for branch tracing.
    assert "branch = true" not in config.read_text()
    assert str(data.resolve()) in config.read_text()
    assert "sample.py" in config.read_text()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--cov",
            f"--cov-config={config}",
            "--cov-report=",
        ],
        cwd=runnable_repository,
        check=True,
    )
    manifest = data.with_name("coverage.shard-1.sources.json")
    assert main(runnable_repository, verify_sources=manifest) == 0
    assert main(runnable_repository, coverage_data=data, manifests=(manifest,)) == 0
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["function_count"] == 2
    assert all(row["coverage"] == 1 for row in report["functions"])
    (runnable_repository / "sample.py").write_text("def example():\n    return 2\n")
    assert main(runnable_repository, verify_sources=manifest) == 2
    assert main(runnable_repository, coverage_data=data, manifests=(manifest,)) == 2
    assert not (runnable_repository / "artifacts/crap/scores.json").exists()


def test_missing_or_empty_shard_coverage_is_a_measurement_error(runnable_repository):
    config = runnable_repository / "shard.coveragerc"
    data = runnable_repository / ".coverage.shard-1"
    assert main(runnable_repository, coveragerc=config, data_file=data) == 0
    manifest = data.with_name("coverage.shard-1.sources.json")
    assert main(runnable_repository, coverage_data=runnable_repository / "absent") == 2
    assert main(runnable_repository, coverage_data=data, manifests=(manifest,)) == 2
    assert main(runnable_repository, coveragerc=runnable_repository / "c") == 2


def test_command_line_separates_gate_options_from_pytest_arguments():
    options, pytest_args = arguments(
        [
            "-n",
            "auto",
            "--coverage-data",
            ".coverage",
            "--source-manifest",
            "a.json",
            "--source-manifest",
            "b.json",
            "--skip-missing-evidence",
            "-p",
            "no:cov",
        ]
    )
    assert options.coverage_data == Path(".coverage")
    assert options.source_manifest == [Path("a.json"), Path("b.json")]
    assert options.write_coveragerc is None
    assert options.skip_missing_evidence is True
    assert options.changed_since is None
    assert pytest_args == ("-n", "auto", "-p", "no:cov")


def commit(repository, message):
    subprocess.run(["git", "add", "-A"], cwd=repository, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.invalid",
            "commit",
            "-qm",
            message,
        ],
        cwd=repository,
        check=True,
    )


def test_changed_lines_cover_modified_hunks_and_untracked_files(runnable_repository):
    (runnable_repository / "edited.py").write_text("a = 1\nb = 2\nc = 3\n")
    commit(runnable_repository, "base")
    subprocess.run(["git", "branch", "base"], cwd=runnable_repository, check=True)
    (runnable_repository / "edited.py").write_text("a = 1\nb = 20\nc = 3\nd = 4\n")
    (runnable_repository / "fresh.py").write_text("x = 1\ny = 2\n")
    commit(runnable_repository, "edit")
    (runnable_repository / "untracked.py").write_text("z = 1\n")
    changed = changed_lines(runnable_repository, "base")
    assert changed == {
        runnable_repository / "edited.py": {2, 4},
        runnable_repository / "fresh.py": {1, 2},
        runnable_repository / "untracked.py": {1},
    }


def test_changed_lines_survive_diff_prefix_settings_and_plus_plus_source(runnable_repository):
    (runnable_repository / "edited.py").write_text("a = 1\nb = 2\n")
    commit(runnable_repository, "base")
    subprocess.run(["git", "branch", "base"], cwd=runnable_repository, check=True)
    subprocess.run(["git", "config", "diff.noprefix", "true"], cwd=runnable_repository, check=True)
    subprocess.run(
        ["git", "config", "diff.mnemonicPrefix", "true"], cwd=runnable_repository, check=True
    )
    (runnable_repository / "edited.py").write_text("a = 1\n++ b\nc = 3\n")
    commit(runnable_repository, "edit")
    assert changed_lines(runnable_repository, "base") == {runnable_repository / "edited.py": {2, 3}}


def test_diff_scope_gates_only_functions_with_changed_executable_lines(runnable_repository):
    complex_source = (
        "def complex(value):\n" + "    if value:\n        value -= 1\n" * 5 + "    return value\n"
    )
    (runnable_repository / "untested.py").write_text(complex_source)
    commit(runnable_repository, "base")
    subprocess.run(["git", "branch", "base"], cwd=runnable_repository, check=True)
    assert main(runnable_repository) == 1
    assert main(runnable_repository, changed_since="base") == 0
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["changed_since"] == "base"
    assert report["above_maximum_count"] == 1
    assert report["violation_count"] == 0
    assert report["passed"] is True
    (runnable_repository / "untested.py").write_text("# touched\n" + complex_source)
    assert main(runnable_repository, changed_since="base") == 0
    (runnable_repository / "untested.py").write_text(complex_source.replace("-= 1", "-= 2", 1))
    assert main(runnable_repository, changed_since="base") == 1
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["violation_count"] == 1
    assert report["functions"][0]["changed"] is True
    assert main(runnable_repository, changed_since="no-such-ref") == 2


def test_skip_missing_evidence_leaves_uncollectable_modules_unscored(runnable_repository):
    tests = runnable_repository / "tests"
    tests.mkdir()
    (tests / "evidence_guard.py").write_bytes(
        (Path(__file__).parent / "evidence_guard.py").read_bytes()
    )
    (runnable_repository / "conftest.py").write_text('pytest_plugins = ["tests.evidence_guard"]\n')
    (tests / "test_needs_evidence.py").write_text(
        "from pathlib import Path\n"
        "def helper(value):\n" + "    if value:\n        value -= 1\n" * 5 + "    return value\n"
        "def test_capture():\n"
        f"    assert Path({EVIDENCE + '/a'!r}).read_text()\n"
    )
    assert main(runnable_repository) == 2
    # The copied plugin has no covering test here, so only the report is asserted.
    assert main(runnable_repository, skip_missing_evidence=True) != 2
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["excluded_modules"] == ["tests/test_needs_evidence.py"]
    assert {row["path"] for row in report["functions"]} == {
        "sample.py",
        "test_sample.py",
        "tests/evidence_guard.py",
    }


def write_floor(repository, floors):
    (repository / "pyproject.toml").write_text(
        "[tool.crap]\nmax-score = 30\n[tool.crap.coverage-floor]\n" + floors
    )


def test_coverage_floor_counts_unimported_files_below_its_directory(runnable_repository):
    package = runnable_repository / "src"
    package.mkdir()
    (package / "used.py").write_text("def used():\n    return 1\n")
    (package / "unused.py").write_text("def unused():\n    return 1\n")
    (runnable_repository / "test_used.py").write_text(
        "from src.used import used\ndef test_used():\n    assert used() == 1\n"
    )
    write_floor(runnable_repository, "src = 50\n")
    assert main(runnable_repository) == 0
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["coverage_floors"] == [
        {
            "path": "src",
            "minimum": 50,
            "statements": 4,
            "covered": 2,
            "coverage": 0.5,
            "passed": True,
        }
    ]
    assert report["passed"] is True
    write_floor(runnable_repository, "src = 50.1\n")
    assert main(runnable_repository) == 1
    report = json.loads((runnable_repository / "artifacts/crap/scores.json").read_text())
    assert report["coverage_floors"][0]["passed"] is False
    assert report["violation_count"] == 0
    assert report["passed"] is False


def test_coverage_floor_applies_to_the_whole_directory_in_diff_scope(runnable_repository):
    (runnable_repository / "src").mkdir()
    (runnable_repository / "src/unused.py").write_text("def unused():\n    return 1\n")
    commit(runnable_repository, "base")
    subprocess.run(["git", "branch", "base"], cwd=runnable_repository, check=True)
    write_floor(runnable_repository, "src = 1\n")
    assert main(runnable_repository, changed_since="base") == 1


@pytest.mark.parametrize(
    "floor", ["missing = 50", "src = 0", "src = 101", "src = true", 'src = "87"']
)
def test_invalid_or_vacuous_coverage_floor_is_a_measurement_error(runnable_repository, floor):
    (runnable_repository / "src").mkdir()
    (runnable_repository / "src/module.py").write_text("def module():\n    return 1\n")
    write_floor(runnable_repository, floor + "\n")
    assert main(runnable_repository) == 2


def test_coverage_floor_that_is_not_a_table_is_a_measurement_error(runnable_repository):
    (runnable_repository / "pyproject.toml").write_text(
        "[tool.crap]\nmax-score = 30\ncoverage-floor = 87\n"
    )
    assert main(runnable_repository) == 2
