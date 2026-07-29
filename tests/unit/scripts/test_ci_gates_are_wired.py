"""The two new CI gates must themselves be wired into a workflow.

Why this test exists
--------------------
``check_orphan_scripts.py`` documents a deliberate limitation: a Makefile
reference counts as wiring, because ``make <target>`` is a real entrypoint
(see that module's "Known vacuous passes" #2). For most scripts that is the
right call.

For these two gates specifically it is not, and the audit of PR #3494 found the
trap: the orphan gate reports **itself** as ``wired <- Makefile`` the moment a
``check-orphan-scripts`` target exists, so it cannot detect its own
orphanhood. Ship the Makefile targets without the workflow steps and both
gates are inert while the gate designed to catch inert scripts says everything
is fine — the PRD's own thesis reproduced by the PR that implements it.

So this asserts the stronger property for the gates themselves: each must
appear in a ``run:`` step of some workflow, not merely in the Makefile.

If this test fails, the fix is to wire the gate into
``.github/workflows/template_lint.yml`` — never to weaken the test.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts.ci._shell_lex import drop_heredoc_bodies, segments_with_spans, strip_comments

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

#: (make target, script) pairs that must run in CI, not just be callable.
REQUIRED_GATES = [
    ("check-orphan-scripts", "scripts/ci/check_orphan_scripts.py"),
    ("check-import-provenance", "scripts/ci/check_import_provenance.py"),
]


def _command_segments(run_block: str) -> list[str]:
    """The command segments of a `run:` block that could actually EXECUTE.

    Three layers, each closing a way the guard was satisfiable without the gate
    ever running:

    1. **Shell comments** — `# make check-orphan-scripts` above a real step.
    2. **Heredoc bodies** — `cat <<'EOF' ... make check-orphan-scripts ... EOF`
       is documentation being printed, not a command. Stripping comments alone
       left this readable, which is finding #1 one layer along.
    3. **`echo` / `printf` segments** — emitting the command name is not
       running it.

    Returning segments rather than whole blocks also means a substring match
    cannot span a separator.
    """
    cleaned = drop_heredoc_bodies(strip_comments(run_block))
    segments: list[str] = []
    for line in cleaned.splitlines():
        if not line.strip():
            continue
        for _, _, segment in segments_with_spans(line):
            head = segment.split()
            if head and Path(head[0]).name in {"echo", "printf", ":"}:
                continue
            segments.append(segment)
    return segments


def _run_steps() -> list[str]:
    """Every ``run:`` scalar across all workflows, with shell comments stripped.

    Two layers of comment, and both matter:

    * **YAML comments** are excluded by parsing rather than grepping, so a
      ``# see make check-orphan-scripts`` line in the workflow header cannot
      satisfy this test.
    * **Shell comments inside a ``run: |`` block** are part of the YAML string
      and survive parsing. Without stripping them, this passes::

          - run: |
              # TODO(VIB-9999): re-enable make check-orphan-scripts once triaged
              make lint-check

      That is not hypothetical — commenting a step out to unblock a red build is
      exactly the pressure that produces an inert gate, and it is the moment
      this guard most needs to fire. Round 3 of the audit found the guard
      against inert gates had the very defect it guards against.
    """
    blocks: list[str] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "run" and isinstance(value, str):
                    blocks.extend(_command_segments(value))
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for wf in sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml")):
        try:
            walk(yaml.safe_load(wf.read_text(encoding="utf-8")))
        except yaml.YAMLError:  # pragma: no cover - malformed workflow
            continue
    return blocks


@pytest.mark.parametrize(("target", "script"), REQUIRED_GATES, ids=lambda v: str(v))
def test_gate_runs_in_a_workflow(target: str, script: str) -> None:
    """A Makefile target alone does not satisfy this — the gate must actually run."""
    steps = _run_steps()
    assert steps, "no workflow run: steps found at all — is .github/workflows/ present?"
    wired = any(target in step or script in step for step in steps)
    assert wired, (
        f"{script} is not invoked by any workflow run: step.\n"
        f"Expected `make {target}` (or the script path) in a workflow.\n"
        f"A Makefile target on its own is NOT enough for these two gates: the "
        f"orphan gate counts a Makefile reference as wiring, so it would report "
        f"itself as wired while never executing in CI."
    )


def test_a_commented_out_step_does_not_satisfy_the_guard(tmp_path, monkeypatch):
    """Negative control: the guard must not accept a commented-out invocation.

    This is the realistic failure — someone comments the step out to unblock a
    red build, and the guard that exists to notice stays green. Before the fix,
    the workflow below satisfied the test for BOTH gates.
    """
    wf_dir = tmp_path / ".github" / "workflows"
    wf_dir.mkdir(parents=True)
    monkeypatch.setattr("tests.unit.scripts.test_ci_gates_are_wired.WORKFLOW_DIR", wf_dir)
    (wf_dir / "lint.yml").write_text(
        "name: Lint\n"
        "on: [push]\n"
        "jobs:\n"
        "  lint:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - run: |\n"
        "          # TODO(VIB-9999): re-enable make check-orphan-scripts once triaged\n"
        "          # make check-import-provenance\n"
        "          make lint-check\n",
        encoding="utf-8",
    )
    steps = _run_steps()
    for target, script in REQUIRED_GATES:
        assert not any(target in step or script in step for step in steps), (
            f"a commented-out `{target}` must not satisfy the wiring guard"
        )


def test_a_real_step_does_satisfy_the_guard(tmp_path, monkeypatch):
    """The positive control, so the negative one above cannot pass vacuously."""
    wf_dir = tmp_path / ".github" / "workflows"
    wf_dir.mkdir(parents=True)
    monkeypatch.setattr("tests.unit.scripts.test_ci_gates_are_wired.WORKFLOW_DIR", wf_dir)
    (wf_dir / "lint.yml").write_text(
        "name: Lint\n"
        "on: [push]\n"
        "jobs:\n"
        "  lint:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - run: make check-orphan-scripts\n"
        "      - run: make check-import-provenance\n",
        encoding="utf-8",
    )
    steps = _run_steps()
    for target, script in REQUIRED_GATES:
        assert any(target in step or script in step for step in steps), target


@pytest.mark.parametrize(
    ("run_block", "satisfied"),
    [
        # A heredoc BODY naming the gate is documentation being printed, not a
        # command. Stripping shell comments alone left this readable — the same
        # defect as the commented-out case, one layer along.
        ("cat <<'EOF'\nmake check-orphan-scripts\nEOF\nmake lint-check\n", False),
        ("# make check-orphan-scripts\nmake lint-check\n", False),
        ('echo "remember to run make check-orphan-scripts"\nmake lint-check\n', False),
        ('printf "make check-orphan-scripts"\n', False),
        # Real invocations must still satisfy it.
        ("make check-orphan-scripts\n", True),
        ("make lint-check && make check-orphan-scripts\n", True),
        ("uv run python scripts/ci/check_orphan_scripts.py\n", True),
    ],
)
def test_only_an_executable_invocation_satisfies_the_guard(run_block, satisfied):
    """The guard must require a command, not a text match anywhere in the block."""
    segments = _command_segments(run_block)
    found = any("check-orphan-scripts" in s or "check_orphan_scripts.py" in s for s in segments)
    assert found is satisfied, run_block
