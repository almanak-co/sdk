"""Anti-vacuity tests for ``scripts/ci/check_import_provenance.py``.

Two requirements from ``docs/internal/PRD-ImprovsJuly28.md`` §P5 are load-bearing
and each has a test here that fails if the gate is weakened to a grep:

* **It must not flag** ``python3 -c "print(hex(...))"``. That expression is live
  at ``accounting_regression_check.sh:157,161``. A gate that false-positives on
  safe arithmetic gets switched off by the first maintainer it annoys, so this
  is a correctness requirement, not politeness.
* **It must match the resolved entrypoint, not the literal.** The motivating
  defect was ``python3 "$ASSERT"`` — the script's name appears nowhere on that
  line. A literal grep would have missed the exact bug it was written for while
  appearing to work.
"""

from __future__ import annotations

import textwrap

import pytest

import scripts.ci.check_import_provenance as gate


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(gate, "WORKFLOW_DIR", tmp_path / ".github" / "workflows")
    (tmp_path / "scripts" / "ci").mkdir(parents=True)
    return tmp_path


def _almanak_importer(repo, name: str = "assert_engine.py"):
    path = repo / "scripts" / "ci" / name
    path.write_text("from almanak.framework.accounting.ledger_guard import landed_sql\n", encoding="utf-8")
    return path


def _stdlib_only(repo, name: str = "plain.py"):
    path = repo / "scripts" / "ci" / name
    path.write_text("import json\nprint(json.dumps({}))\n", encoding="utf-8")
    return path


def _scan(repo, body: str, name: str = "harness.sh"):
    path = repo / "scripts" / "ci" / name
    path.write_text(body, encoding="utf-8")
    return gate._scan_shell_text(body, f"scripts/ci/{name}", path)


# --------------------------------------------------------------------------
# The two named PRD requirements
# --------------------------------------------------------------------------


def test_inline_arithmetic_is_not_flagged(repo):
    """`python3 -c "print(hex(...))"` imports nothing — live at check.sh:157,161."""
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        eth_wei=$(python3 -c "print(hex(int(${eth_amount} * 10**18)))")
        usdc_units=$(python3 -c "print(int(${usdc_amount} * 10**6))")
        """)
    assert _scan(repo, body) == []


def test_bare_interpreter_via_shell_variable_is_flagged(repo):
    """The motivating defect: `python3 "$ASSERT"` names no script on that line.

    A gate matching the literal filename passes this case while believing it
    works — which is the failure mode the whole PRD is about.
    """
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
        ASSERT="$SCRIPT_DIR/assert_engine.py"
        python3 "$ASSERT" --strategy lp --db "$DB"
        """)
    violations = _scan(repo, body)
    assert len(violations) == 1
    assert "assert_engine.py" in violations[0].detail
    assert "imports almanak" in violations[0].detail


# --------------------------------------------------------------------------
# Correct forms must stay silent
# --------------------------------------------------------------------------


def test_uv_run_is_not_flagged(repo):
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        ASSERT="scripts/ci/assert_engine.py"
        uv run python "$ASSERT" --strategy lp
        """)
    assert _scan(repo, body) == []


def test_stdlib_only_script_is_not_flagged(repo):
    """No almanak import means no provenance to get wrong."""
    _stdlib_only(repo)
    body = '#!/usr/bin/env bash\npython3 "scripts/ci/plain.py"\n'
    assert _scan(repo, body) == []


def test_poetry_and_pipenv_run_are_not_flagged(repo):
    _almanak_importer(repo)
    for runner in ("poetry run", "pipenv run"):
        body = f'#!/usr/bin/env bash\n{runner} python "scripts/ci/assert_engine.py"\n'
        assert _scan(repo, body) == [], runner


# --------------------------------------------------------------------------
# Other invocation shapes
# --------------------------------------------------------------------------


def test_dash_m_almanak_is_flagged(repo):
    body = "#!/usr/bin/env bash\npython3 -m almanak.framework.cli --help\n"
    violations = _scan(repo, body)
    assert len(violations) == 1
    assert "-m almanak" in violations[0].detail


def test_inline_c_importing_almanak_is_flagged(repo):
    body = '#!/usr/bin/env bash\npython3 -c "import almanak; print(almanak.__file__)"\n'
    violations = _scan(repo, body)
    assert len(violations) == 1
    assert "-c" in violations[0].detail


def test_heredoc_importing_almanak_is_flagged(repo):
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        LANDED_SQL="$(python3 - <<'PYEOF'
        from almanak.framework.accounting.ledger_guard import landed_sql
        print(landed_sql())
        PYEOF
        )"
        """)
    violations = _scan(repo, body)
    assert len(violations) == 1
    assert "heredoc" in violations[0].detail


def test_heredoc_without_almanak_is_not_flagged(repo):
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        OUT="$(python3 - <<'PYEOF'
        import json
        print(json.dumps({"a": 1}))
        PYEOF
        )"
        """)
    assert _scan(repo, body) == []


def test_python_variants_are_recognised(repo):
    """`python`, `python3.12` and friends all resolve from ambient site-packages."""
    _almanak_importer(repo)
    for interp in ("python", "python3", "python3.12"):
        body = f'#!/usr/bin/env bash\n{interp} "scripts/ci/assert_engine.py"\n'
        assert len(_scan(repo, body)) == 1, interp


# --------------------------------------------------------------------------
# Command-shape holes found by the Codex auditor on PR #3494
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flag",
    [
        # Separated single letters — the forms the first fix handled.
        "-u",
        "-B",
        "-E",
        "-O",
        "-s",
        "-I",
        # CLUSTERED forms. Round 2 found these evaded the gate entirely,
        # because the fix matched whole tokens against a set of single flags.
        "-uB",
        "-bb",
        "-BE",
        "-Ou",
    ],
)
def test_passthrough_interpreter_flags_do_not_hide_the_target(repo, flag):
    """`python3 -u script.py` is exactly as ambient as `python3 script.py`.

    Before the fix the flag itself was read as the target, failed to look like a
    `.py` path, and the invocation was silently classified safe — a one-character
    evasion of the whole gate.
    """
    _almanak_importer(repo)
    body = f'#!/usr/bin/env bash\npython3 {flag} "scripts/ci/assert_engine.py"\n'
    violations = _scan(repo, body)
    assert len(violations) == 1, flag
    assert "assert_engine.py" in violations[0].detail, "must flag the SCRIPT, not the flag"


@pytest.mark.parametrize(
    "flag",
    [
        "-W ignore",  # value as the next token
        "-X utf8",
        "-Wignore",  # ATTACHED value — the common spelling, previously missed
        "-Xutf8",
    ],
)
def test_value_taking_flags_are_stepped_over(repo, flag):
    """`-W` / `-X` take a value, attached or separate; both spellings must resolve.

    The docstring originally named both `-W` and `-X` while the body tested only
    `-W ignore`, so three of the four spellings were unverified and all three
    were broken — the set was compared for token equality, which no attached
    form can satisfy.
    """
    _almanak_importer(repo)
    body = f'#!/usr/bin/env bash\npython3 {flag} "scripts/ci/assert_engine.py"\n'
    violations = _scan(repo, body)
    assert len(violations) == 1, flag
    assert "assert_engine.py" in violations[0].detail


def test_pinned_runner_does_not_vouch_for_a_later_command_on_the_same_line(repo):
    """One `uv run` must not certify every subsequent command on the line.

    `uv run python -c "..." && python3 x.py` — the second invocation is bare and
    is the exact defect this gate exists to reject.
    """
    _almanak_importer(repo)
    body = '#!/usr/bin/env bash\nuv run python -c "print(1)" && python3 "scripts/ci/assert_engine.py"\n'
    violations = _scan(repo, body)
    assert len(violations) == 1


def test_inline_snippet_does_not_bleed_across_command_separators(repo):
    """A later command's `-c` code must not incriminate an earlier safe one.

    Found by CodeRabbit's GitHub review. The inline tail was the whole line
    remainder, so `python3 -c "print(1)" && uv run python -c "import almanak"`
    flagged the FIRST, safe invocation. The tail cannot simply stop at the first
    `;` either — that is a legal Python statement separator inside the snippet —
    so the bound is quote-aware.
    """
    body = '#!/usr/bin/env bash\npython3 -c "print(1)" && uv run python -c "import almanak"\n'
    assert _scan(repo, body) == []


def test_semicolons_inside_an_inline_snippet_are_still_scanned(repo):
    """The bound must not truncate at a `;` that belongs to the Python code."""
    body = '#!/usr/bin/env bash\npython3 -c "import sys; import almanak; print(1)"\n'
    assert len(_scan(repo, body)) == 1


def test_variable_expansion_is_prefix_safe(repo):
    """`$ROOT_DIR` must not be eaten by a shorter `ROOT` defined earlier.

    Found by CodeRabbit. Boundary-less `str.replace` in declaration order turned
    `$ROOT_DIR/x.py` into `<root-value>_DIR/x.py`, which fails to resolve — so
    the invocation was classified safe. A false negative that depended only on
    the order two variables happened to be declared in.
    """
    _almanak_importer(repo)
    body = (
        "#!/usr/bin/env bash\n"
        'ROOT="/somewhere/else"\n'
        f'ROOT_DIR="{repo}/scripts/ci"\n'
        'python3 "$ROOT_DIR/assert_engine.py"\n'
    )
    assert len(_scan(repo, body)) == 1


def test_workflow_violation_reports_the_real_file_line(repo, tmp_path, monkeypatch):
    """A `run:` scalar's violations must map back to the workflow's own lines."""
    wf_dir = repo / ".github" / "workflows"
    wf_dir.mkdir(parents=True)
    monkeypatch.setattr(gate, "WORKFLOW_DIR", wf_dir)
    _almanak_importer(repo)
    (wf_dir / "w.yml").write_text(
        textwrap.dedent("""\
        name: T
        on: [push]
        jobs:
          j:
            runs-on: ubuntu-latest
            steps:
              - name: safe
                run: echo hello
              - name: bad
                run: |
                  echo preparing
                  python3 scripts/ci/assert_engine.py
        """),
        encoding="utf-8",
    )
    violations = [v for v in gate.collect_violations() if v.file.endswith("w.yml")]
    assert len(violations) == 1
    assert violations[0].line == 12, "line must be the workflow file's own line, not an offset into the run: scalar"


@pytest.mark.parametrize(
    "command",
    [
        # The shape the original test covered — the one that already worked.
        'echo start && uv run python "scripts/ci/assert_engine.py"',
        # Round 2 of the audit: the segment splitter broke on `$(`, `;` and `|`
        # even INSIDE quotes, truncating `uv run` out of its own lookback — so
        # the CORRECT form was reported as the defect. All three regressed.
        'uv run --directory "$(pwd)" python "scripts/ci/assert_engine.py"',
        'uv run --project "$(git rev-parse --show-toplevel)" python "scripts/ci/assert_engine.py"',
        'uv run --with "pkg;extra" python "scripts/ci/assert_engine.py"',
        'uv run --with "a|b" python "scripts/ci/assert_engine.py"',
        'uv run --directory `pwd` python "scripts/ci/assert_engine.py"',
    ],
)
def test_pinned_runner_still_applies_within_its_own_segment(repo, command):
    """The scoping fix must not report correct code as a violation.

    The original single case was the one shape that still worked, so it vouched
    for a fix that had broken three common `uv run` invocations. A test whose
    only case is the passing one cannot detect the regression it was added for.
    """
    _almanak_importer(repo)
    assert _scan(repo, f"#!/usr/bin/env bash\n{command}\n") == [], command


def test_heredoc_is_matched_to_its_own_invocation(repo):
    """A stdlib-only heredoc must not be flagged because a LATER one imports almanak.

    Searching every heredoc body in the file reported the safe invocation as a
    violation — a false positive on correct code, which is how a gate gets
    switched off.
    """
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        A="$(python3 - <<'PY'
        import json
        print(json.dumps({}))
        PY
        )"
        B="$(python3 - <<'PY2'
        from almanak.x import y
        PY2
        )"
        """)
    violations = _scan(repo, body)
    assert len(violations) == 1
    assert violations[0].line == 7, "the almanak heredoc is on line 7, not the safe one on line 2"


# --------------------------------------------------------------------------
# Shell-function wrappers
# --------------------------------------------------------------------------


def test_function_wrapping_uv_run_is_not_flagged(repo):
    """`py() { uv run python "$@"; }` is already the correct form.

    Live shape in ``scripts/qa/snapshot_identity_audit.sh``. Treating the
    function name as a bare interpreter produced six false alarms there, and
    false alarms are how a gate gets switched off.
    """
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        py() { uv run python "$@"; }
        py "scripts/ci/assert_engine.py" --flag
        """)
    assert _scan(repo, body) == []


def test_guarded_fallback_definition_is_not_flagged(repo):
    """The uv-preferred / python3-fallback idiom is deliberate and documented."""
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        if command -v uv >/dev/null 2>&1; then
          py() { uv run python "$@"; }
        else
          py() { python3 "$@"; }
        fi
        py "scripts/ci/assert_engine.py"
        """)
    assert _scan(repo, body) == []


def test_function_wrapping_only_bare_python_is_flagged(repo):
    """A wrapper with no pinned definition anywhere still resolves to bare python."""
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        py() { python3 "$@"; }
        py "scripts/ci/assert_engine.py"
        """)
    assert len(_scan(repo, body)) == 1


# --------------------------------------------------------------------------
# Comments and exemptions
# --------------------------------------------------------------------------


def test_commented_out_invocation_is_not_flagged(repo):
    _almanak_importer(repo)
    body = '#!/usr/bin/env bash\n# python3 "scripts/ci/assert_engine.py"  # old way\necho hi\n'
    assert _scan(repo, body) == []


def test_exemption_marker_with_reason_suppresses(repo):
    """For sites where ambient resolution is the property under test."""
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        # provenance-exempt: resolving the INSTALLED sdk is the point here
        python3 "scripts/ci/assert_engine.py"
        """)
    assert _scan(repo, body) == []


def test_exemption_marker_is_found_above_a_multiline_comment_block(repo):
    """A marker usually comes with justification; the whole block is searched."""
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        # provenance-exempt: intentionally resolves the installed distribution
        # so that version skew and import-masking stubs stay visible.
        # `uv run python` here would pin the worktree and delete the signal.
        python3 "scripts/ci/assert_engine.py"
        """)
    assert _scan(repo, body) == []


def test_bare_marker_without_reason_does_not_suppress(repo):
    """A marker with no justification is not a declaration of intent."""
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        # provenance-exempt:
        python3 "scripts/ci/assert_engine.py"
        """)
    assert len(_scan(repo, body)) == 1


def test_exemption_does_not_leak_to_later_invocations(repo):
    """An exemption applies to its own call site, not the rest of the file."""
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        # provenance-exempt: this one is deliberate
        python3 "scripts/ci/assert_engine.py" --first
        echo separator
        python3 "scripts/ci/assert_engine.py" --second
        """)
    violations = _scan(repo, body)
    assert len(violations) == 1
    assert "--second" in violations[0].snippet


# --------------------------------------------------------------------------
# End-to-end
# --------------------------------------------------------------------------


def test_main_passes_on_a_clean_tree(repo):
    (repo / ".github" / "workflows").mkdir(parents=True)
    _stdlib_only(repo)
    (repo / "scripts" / "ci" / "ok.sh").write_text(
        '#!/usr/bin/env bash\nuv run python "scripts/ci/plain.py"\n', encoding="utf-8"
    )
    assert gate.main([]) == 0


def test_main_fails_on_a_violating_tree(repo):
    (repo / ".github" / "workflows").mkdir(parents=True)
    _almanak_importer(repo)
    (repo / "scripts" / "ci" / "bad.sh").write_text(
        '#!/usr/bin/env bash\npython3 "scripts/ci/assert_engine.py"\n', encoding="utf-8"
    )
    assert gate.main([]) == 1


def test_a_pinned_definition_anywhere_disables_the_check_for_that_name(repo):
    """Pins a KNOWN LIMIT, so a refactor cannot widen it silently.

    A pinned definition in a dead branch and the legitimate `command -v uv`
    guarded fallback are textually identical — separating them would require
    evaluating the branch condition, which a static textual check cannot do.
    This is therefore a boundary of the analysis, not a deferred to-do, and
    this test exists so that boundary is explicit and stable rather than
    accidental.
    """
    _almanak_importer(repo)
    body = textwrap.dedent("""\
        #!/usr/bin/env bash
        if false; then
          py() { uv run python "$@"; }
        fi
        py() { python3 "$@"; }
        py "scripts/ci/assert_engine.py"
        """)
    assert _scan(repo, body) == [], (
        "current, documented behaviour: any pinned definition of the name "
        "disables the check for it. If this starts failing, the analysis "
        "changed — update the Known limits section deliberately."
    )
