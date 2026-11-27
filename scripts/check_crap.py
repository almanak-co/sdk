"""Run fresh tests and enforce the per-function CRAP limit and directory coverage floors."""

import argparse
import ast
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
import tokenize
import tomllib
from dataclasses import asdict, dataclass
from fractions import Fraction
from pathlib import Path
from tempfile import TemporaryDirectory

from coverage import Coverage
from coverage.exceptions import CoverageException
from radon.visitors import ComplexityVisitor

ROOT = Path(__file__).resolve().parents[1]
FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef)


@dataclass(frozen=True)
class Function:
    name: str
    line: int
    complexity: int
    lines: set[int]


def body_lines(node):
    return set(range(node.body[0].lineno, node.end_lineno + 1))


class Functions(ast.NodeVisitor):
    def __init__(self):
        self.scope = []
        self.functions = []

    def visit_ClassDef(self, node):
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node):
        lines = body_lines(node)
        for child in ast.walk(node):
            if child is not node and isinstance(child, (*FUNCTIONS, ast.ClassDef)):
                lines.difference_update(body_lines(child))
        complexity = ComplexityVisitor.from_ast(node).functions[0].complexity
        self.functions.append(
            Function(".".join([*self.scope, node.name]), node.lineno, complexity, lines)
        )
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef


@dataclass(frozen=True)
class Score:
    path: str
    name: str
    line: int
    complexity: int
    statements: int
    covered: int
    changed: bool = True

    @property
    def coverage(self):
        # No executable statements is not evidence of an observed invocation.
        return Fraction(self.covered, self.statements) if self.statements else Fraction(0)

    @property
    def crap(self):
        return self.complexity**2 * (1 - self.coverage) ** 3 + self.complexity

    def record(self, maximum):
        return {
            **asdict(self),
            "coverage": float(self.coverage),
            "crap": float(self.crap),
            "violation": self.crap > maximum,
        }


def nested_projects(root):
    """Directories below root with their own pyproject.toml; each carries its own CRAP gate."""
    return sorted(path.parent for path in root.glob("*/pyproject.toml"))


def in_nested_project(root, name):
    return any(
        Path(name).is_relative_to(project.relative_to(root)) for project in nested_projects(root)
    )


def python_files(root):
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.py"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    names = sorted(set(result.stdout.decode().split("\0")) - {""})
    files = []
    for name in names:
        if in_nested_project(root, name):
            continue
        path = root / name
        if path.is_symlink():
            raise ValueError(f"Python source must be a regular repository file: {name}")
        if path.is_file():
            files.append(path)
    if not files:
        raise ValueError("No repository Python files found")
    return files


def score_file(root, path, coverage, changed=None):
    with tokenize.open(path) as source:
        tree = ast.parse(source.read(), filename=str(path))
    visitor = Functions()
    visitor.visit(tree)
    _, statements, _, missing, _ = coverage.analysis2(str(path))
    executable, uncovered = set(statements), set(missing)
    scores = []
    for function in visitor.functions:
        lines = function.lines & executable
        scores.append(
            Score(
                path.relative_to(root).as_posix(),
                function.name,
                function.line,
                function.complexity,
                len(lines),
                len(lines - uncovered),
                changed is None or bool(lines & changed),
            )
        )
    return scores


HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def merge_base(root, reference):
    return subprocess.run(
        ["git", "-C", str(root), "merge-base", reference, "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


TEST_MODULE = re.compile(r"(?:.*/)?test_[^/]*\.py")


def changed_tests(root, reference):
    """Test modules added, modified or renamed since the merge base with reference.

    Untracked modules count. A changed conftest or support module selects
    nothing: only a module that is itself changed runs.
    """
    git = ["git", "-C", str(root)]
    commands = (
        ["diff", "--name-only", "--diff-filter=AMR", "--no-relative", "-z"],
        ["ls-files", "--others", "--exclude-standard", "-z"],
    )
    names: set[str] = set()
    for index, command in enumerate(commands):
        revision = [merge_base(root, reference)] if index == 0 else []
        listed = subprocess.run(
            [*git, *command, *revision, "--", "*.py"], check=True, capture_output=True
        ).stdout.decode()
        names.update(filter(None, listed.split("\0")))
    return sorted(
        name
        for name in names
        if TEST_MODULE.fullmatch(name)
        and not in_nested_project(root, name)
        and (root / name).is_file()
    )


def changed_lines(root, reference):
    """Lines added or modified per file since the merge base with reference.

    Untracked files are entirely new, so every line of them counts.
    """
    git = ["git", "-C", str(root)]
    base = merge_base(root, reference)
    diff = subprocess.run(
        [
            *git,
            "diff",
            "-U0",
            "--no-color",
            "--no-ext-diff",
            "--no-relative",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            base,
            "--",
            "*.py",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    lines: dict[Path, set[int]] = {}
    path = None
    previous = ""
    for line in diff.splitlines():
        # A file header is a `+++` line paired with a `---` line; an added source
        # line that itself starts with `++ ` is hunk content.
        if line.startswith("+++ ") and previous.startswith("--- "):
            name = line[4:]
            path = None if name == "/dev/null" else root / name.removeprefix("b/")
        elif path is not None and (hunk := HUNK.match(line)):
            start, count = int(hunk.group(1)), int(hunk.group(2) or 1)
            lines.setdefault(path, set()).update(range(start, start + count))
        previous = line
    untracked = subprocess.run(
        [*git, "ls-files", "--others", "--exclude-standard", "-z", "--", "*.py"],
        check=True,
        capture_output=True,
    ).stdout.decode()
    for name in filter(None, untracked.split("\0")):
        if in_nested_project(root, name):
            continue
        lines[root / name] = set(range(1, len((root / name).read_bytes().splitlines()) + 1))
    return lines


def unmeasurable_modules(root):
    """Test modules the evidence guard refuses to collect while the evidence tree is absent."""
    if (root / "docs/internal/reports/evidence").exists():
        return set()
    location = root / "tests/evidence_guard.py"
    spec = importlib.util.spec_from_file_location("evidence_guard", location)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load the evidence guard from {location}")
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    return set(guard.dependent_modules(root / "tests"))


def source_snapshot(root):
    return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in python_files(root)}


def check_sources_unchanged(root, before):
    after = source_snapshot(root)
    changed = sorted(
        str(path.relative_to(root))
        for path in before.keys() | after.keys()
        if before.get(path) != after.get(path)
    )
    if changed:
        raise ValueError(f"Python sources changed during tests; rerun the CRAP check: {changed}")


def write_config(files, config, data_file):
    # A unique data file prevents stale or concurrent runs from supplying hits.
    # The score reads statement hits only (analysis2 statements/missing), so
    # branch tracing stays off: it would slow the run without changing the
    # metric.
    config.write_text(
        "[run]\ndata_file = "
        + str(data_file)
        + "\ninclude =\n"
        + "".join(f"    {path}\n" for path in files)
        + "[report]\nexclude_lines =\n",
        encoding="utf-8",
    )
    return config


def collect_coverage(root, files, directory, pytest_args, selection=None):
    """Statement hits of one fresh pytest run.

    selection, when given, names the only test modules that run. An empty one
    runs nothing, so every statement counts as missed.
    """
    config = write_config(files, directory / ".coveragerc", directory / ".coverage")
    if selection is None or selection:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                *pytest_args,
                "--cov",
                f"--cov-config={config}",
                "--cov-report=",
                *(selection or ()),
            ],
            cwd=root,
            check=False,
        )
        if result.returncode:
            raise ValueError(f"pytest exited {result.returncode}; no valid CRAP report produced")
    coverage = Coverage(config_file=str(config))
    coverage.load()
    # A whole-suite run that measured nothing is a broken run. A selection can
    # consist of modules that are skipped, which is reported, not refused.
    if selection is None and not coverage.get_data().measured_files():
        raise ValueError("No coverage data collected")
    return coverage


def load_settings(root):
    """The CRAP maximum and the per-directory statement coverage floors in percent."""
    settings = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["crap"]
    maximum = settings["max-score"]
    if type(maximum) is not int or not 0 < maximum <= 30:
        raise ValueError("tool.crap.max-score must be an integer from 1 to 30")
    floors = settings.get("coverage-floor", {})
    if not isinstance(floors, dict):
        raise ValueError("tool.crap.coverage-floor must be a table of directory minimums")
    for directory, minimum in floors.items():
        if type(minimum) not in (int, float) or not 0 < minimum <= 100:
            raise ValueError(
                f"tool.crap.coverage-floor.{directory} must be a percentage above 0 and at most 100"
            )
    return maximum, floors


@dataclass(frozen=True)
class Floor:
    path: str
    minimum: int | float
    statements: int
    covered: int

    @property
    def passed(self):
        return Fraction(self.covered * 100, self.statements) >= Fraction(self.minimum)

    def record(self):
        return {
            **asdict(self),
            "coverage": float(Fraction(self.covered, self.statements)),
            "passed": self.passed,
        }


def measure_floors(root, files, coverage, floors):
    """Statement coverage over every scored file below each floor's directory.

    Unimported files count with every statement missed. A directory without
    measured statements is an error, so a misspelled floor cannot pass vacuously.
    """
    results = []
    for directory, minimum in floors.items():
        statements = covered = 0
        for path in files:
            if path.relative_to(root).is_relative_to(directory):
                _, executable, _, missing, _ = coverage.analysis2(str(path))
                statements += len(executable)
                covered += len(executable) - len(missing)
        if not statements:
            raise ValueError(f"Coverage floor directory has no measured statements: {directory}")
        results.append(Floor(directory, minimum, statements, covered))
    return results


def write_report(
    scores, maximum, output, changed_since=None, excluded=(), floors=(), selection=None
):
    """selection is the list of test modules that ran, or None for the whole suite."""
    if not scores:
        raise ValueError("No Python functions found")
    scores = sorted(scores, key=lambda item: (-item.crap, item.path, item.line))
    above = [item for item in scores if item.crap > maximum]
    violations = [item for item in above if item.changed]
    below = [floor for floor in floors if not floor.passed]
    payload = {
        "maximum": maximum,
        "formula": "complexity**2 * (1 - coverage)**3 + complexity",
        "coverage_metric": "function body statement coverage in pytest processes",
        "changed_since": changed_since,
        "selected_tests": selection,
        "excluded_modules": sorted(excluded),
        "function_count": len(scores),
        "above_maximum_count": len(above),
        "violation_count": len(violations),
        "coverage_floors": [floor.record() for floor in floors],
        "passed": not violations and not below,
        "functions": [item.record(maximum) for item in scores],
    }
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    scope = f", {len(violations)} on lines changed since {changed_since}" if changed_since else ""
    print(f"CRAP: {len(scores)} functions, {len(above)} above {maximum}{scope}.")
    if selection is not None:
        print(
            f"Tests: {len(selection)} test modules changed since {changed_since} ran; "
            "coverage is theirs alone and the coverage floors are not scored."
        )
        for name in selection:
            print(f"  {name}")
    if excluded:
        print(f"Excluded {len(excluded)} test modules that need absent retained evidence.")
    for floor in floors:
        print(
            f"Coverage: {floor.path}/ {floor.covered / floor.statements:.2%} "
            f"({floor.covered}/{floor.statements} statements), floor {floor.minimum}%"
            + ("." if floor.passed else ", BELOW FLOOR.")
        )
    print("    CRAP   CC  Coverage  Function")
    for item in violations:
        print(
            f"{float(item.crap):8.2f} {item.complexity:4} {float(item.coverage):8.1%}  "
            f"{item.path}:{item.line} {item.name}"
        )
    print(f"Full report: {output}")
    return int(bool(violations or below))


def main(
    root=ROOT,
    pytest_args=(),
    changed_since=None,
    changed_tests_only=False,
    skip_missing_evidence=False,
):
    """Run the tests and score the repository.

    changed_since gates only functions with an executable line changed since the
    merge base with that reference. changed_tests_only also runs only the test
    modules changed since it, so a changed function is scored on the coverage of
    those modules alone, and leaves the coverage floors unscored: they are a
    statement about the whole suite. skip_missing_evidence passes the pytest
    option through and leaves the modules it refuses to collect out of the score
    and out of the changed test modules that run.
    """
    try:
        if changed_tests_only and not changed_since:
            raise ValueError("--changed-tests requires --changed-since")
        output = root / "artifacts/crap"
        output.mkdir(parents=True, exist_ok=True)
        report = output / "scores.json"
        report.unlink(missing_ok=True)
        (output / "coverage.json").unlink(missing_ok=True)
        maximum, floors = load_settings(root)
        snapshot = source_snapshot(root)
        excluded = unmeasurable_modules(root) if skip_missing_evidence else set()
        files = [path for path in snapshot if path not in excluded]
        if skip_missing_evidence:
            pytest_args = ("--skip-missing-evidence", *pytest_args)
        changed = changed_lines(root, changed_since) if changed_since else None
        # pytest collects a module named on its command line even when the
        # evidence guard would ignore it, so a changed module that needs absent
        # evidence is left out here and reported with the excluded modules.
        selection = (
            [name for name in changed_tests(root, changed_since) if root / name not in excluded]
            if changed_tests_only
            else None
        )
        with TemporaryDirectory(prefix="crap-") as temporary:
            coverage = collect_coverage(root, files, Path(temporary), pytest_args, selection)
            check_sources_unchanged(root, snapshot)
            scores = [
                score
                for path in files
                for score in score_file(
                    root, path, coverage, None if changed is None else changed.get(path, set())
                )
            ]
            measured_floors = (
                measure_floors(root, files, coverage, floors) if selection is None else []
            )
            coverage.json_report(
                morfs=[str(path) for path in files], outfile=str(output / "coverage.json")
            )
        return write_report(
            scores,
            maximum,
            report,
            changed_since,
            [path.relative_to(root).as_posix() for path in excluded],
            measured_floors,
            selection,
        )
    except (
        OSError,
        ValueError,
        SyntaxError,
        KeyError,
        CoverageException,
        subprocess.CalledProcessError,
    ) as error:
        print(f"CRAP measurement failed: {error}", file=sys.stderr)
        return 2


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "--changed-since",
        metavar="REF",
        help="gate only functions with an executable line changed since the merge base with REF",
    )
    parser.add_argument(
        "--changed-tests",
        action="store_true",
        help="with --changed-since: run only the test modules changed since REF, "
        "score on their coverage alone and leave the coverage floors unscored",
    )
    parser.add_argument(
        "--skip-missing-evidence",
        action="store_true",
        help="hosted CI only: skip and leave unscored the modules that need absent evidence",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="project to score",
    )
    options, pytest_args = parser.parse_known_args(argv)
    return options, tuple(pytest_args)


if __name__ == "__main__":
    OPTIONS, PYTEST_ARGS = arguments(sys.argv[1:])
    raise SystemExit(
        main(
            root=OPTIONS.root.resolve(),
            pytest_args=PYTEST_ARGS,
            changed_since=OPTIONS.changed_since,
            changed_tests_only=OPTIONS.changed_tests,
            skip_missing_evidence=OPTIONS.skip_missing_evidence,
        )
    )
