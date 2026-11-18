from pathlib import Path

import pytest

from tests.evidence_guard import (
    ACTIVE,
    EVIDENCE,
    KEY,
    REASON,
    dependent_modules,
    imported_modules,
    module_path,
    pytest_terminal_summary,
)

pytest_plugins = ("pytester",)


def write_suite(pytester):
    tests = pytester.path / "tests"
    tests.mkdir()
    (tests / "helper.py").write_text(
        f'from pathlib import Path\nROOT = Path(__file__).parents[1] / "{EVIDENCE}"\n'
        "def load():\n    return (ROOT / 'capture.json').read_text()\n"
    )
    (tests / "test_direct.py").write_text(
        "from pathlib import Path\n"
        f'DATA = (Path(__file__).parents[1] / "{EVIDENCE}" / "x.json").read_text()\n'
        "def test_direct():\n    assert DATA\n"
    )
    (tests / "test_indirect.py").write_text(
        "from tests.helper import load\ndef test_indirect():\n    assert load()\n"
    )
    (tests / "test_dynamic.py").write_text(
        "from pathlib import Path\nimport pytest\n"
        'EVIDENCE = "/".join(("docs", "internal", "reports", "evidence"))\n'
        "@pytest.fixture\ndef capture(request):\n"
        '    root = request.config.rootpath.joinpath(*EVIDENCE.split("/"))\n'
        "    return (root / 'y.json').read_text()\n"
        "def test_dynamic(capture):\n    assert capture\n"
    )
    (tests / "test_unrelated.py").write_text(
        "from pathlib import Path\ndef test_unrelated(tmp_path):\n"
        "    (tmp_path / 'absent.json').read_text()\n"
        "def test_passes():\n    assert True\n"
    )
    repository = Path(__file__).parents[1]
    pytester.makeconftest(
        f"import sys\nsys.path.insert(0, {str(repository)!r})\n"
        'pytest_plugins = ("tests.evidence_guard",)\n'
    )
    return tests


def test_missing_evidence_fails_without_the_option(pytester):
    write_suite(pytester)
    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", "--continue-on-collection-errors"
    )
    result.assert_outcomes(passed=1, failed=2, errors=2)


def test_option_skips_dependent_modules_and_dynamic_reads_only(pytester):
    write_suite(pytester)
    result = pytester.runpytest_subprocess(
        "--skip-missing-evidence", "-p", "no:cacheprovider", "-rs"
    )
    result.assert_outcomes(passed=1, failed=1, skipped=1)
    result.stdout.fnmatch_lines(["*evidence-skips: 2 test modules and 1 helper modules*"])
    result.stdout.fnmatch_lines(["*missing retained evidence: y.json*"])


def test_option_is_inert_when_evidence_exists(pytester):
    write_suite(pytester)
    root = pytester.path / EVIDENCE
    root.mkdir(parents=True)
    for name in ("x.json", "y.json", "capture.json"):
        (root / name).write_text("{}")
    result = pytester.runpytest_subprocess("--skip-missing-evidence", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=4, failed=1)
    assert "evidence-skips" not in result.stdout.str()


def test_missing_capture_inside_a_present_evidence_tree_still_fails(pytester):
    write_suite(pytester)
    root = pytester.path / EVIDENCE
    root.mkdir(parents=True)
    (root / "x.json").write_text("{}")
    (root / "capture.json").write_text("{}")
    result = pytester.runpytest_subprocess("--skip-missing-evidence", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=3, failed=1, errors=1)


def test_dependency_closure_resolves_relative_and_package_imports(tmp_path):
    tests = tmp_path / "tests"
    (tests / "qa").mkdir(parents=True)
    (tests / "qa" / "__init__.py").write_text("")
    (tests / "qa" / "catalog.py").write_text(f'ROOT = "{EVIDENCE}"\n')
    (tests / "qa" / "test_cases.py").write_text("from .catalog import ROOT\n")
    (tests / "test_pkg.py").write_text("from tests.qa import catalog\n")
    (tests / "test_free.py").write_text("import json\n")
    (tests / "broken.py").write_text("def (\n")
    dependent = dependent_modules(tests)
    assert dependent == {
        tests / "qa" / "catalog.py",
        tests / "qa" / "test_cases.py",
        tests / "test_pkg.py",
    }
    assert module_path("tests.qa", tests) == tests / "qa" / "__init__.py"
    assert module_path("tests.absent", tests) is None
    assert module_path("tests", tests) is None


def test_imported_modules_lists_submodule_names(tmp_path):
    import ast

    path = tmp_path / "tests" / "test_x.py"
    source = ast.parse("import os\nfrom tests.qa import worker\nfrom tests import helper\n")
    assert imported_modules(source, path, tmp_path / "tests") == [
        "tests.qa",
        "tests.qa.worker",
        "tests",
        "tests.helper",
    ]


@pytest.mark.parametrize("value", [None, "/elsewhere/file.json"])
def test_missing_evidence_file_ignores_unrelated_errors(value, tmp_path):
    from tests.evidence_guard import missing_evidence_file

    class Info:
        def __init__(self):
            self.value = FileNotFoundError(2, "absent", value)

    assert missing_evidence_file(None, tmp_path) is None
    assert missing_evidence_file(Info(), tmp_path) is None


class Reporter:
    def __init__(self, stats):
        self.stats = stats
        self.lines = []

    def write_line(self, line):
        self.lines.append(line)


class Config:
    def __init__(self, active, modules):
        self.stash = {ACTIVE: active, KEY: modules}


class Skip:
    def __init__(self, longrepr):
        self.longrepr = longrepr


def test_terminal_summary_counts_modules_and_run_time_skips(tmp_path):
    modules = {tmp_path / "test_a.py", tmp_path / "test_b.py", tmp_path / "helper.py"}
    skips = [
        Skip(("test_a.py", 1, f"{REASON}: y.json")),
        Skip(("test_c.py", 1, "unrelated skip")),
        Skip("not a location tuple"),
    ]
    reporter = Reporter({"skipped": skips})
    pytest_terminal_summary(reporter, Config(True, modules))
    assert reporter.lines == [
        "evidence-skips: 2 test modules and 1 helper modules not collected, 1 tests "
        f"skipped at run time; retained evidence under {EVIDENCE} is absent. "
        "Skipped cases are not passes."
    ]


def test_terminal_summary_is_silent_when_the_guard_is_inactive():
    reporter = Reporter({"skipped": [Skip(("test_a.py", 1, f"{REASON}: y.json"))]})
    pytest_terminal_summary(reporter, Config(False, set()))
    assert reporter.lines == []


def test_guard_and_its_tests_are_never_evidence_dependent():
    # The scorer's tests are the hosted gate's only coverage of the scorer.
    tests = Path(__file__).parent
    dependent = dependent_modules(tests)
    assert tests / "evidence_guard.py" not in dependent
    assert Path(__file__) not in dependent
    assert tests / "test_crap.py" not in dependent
