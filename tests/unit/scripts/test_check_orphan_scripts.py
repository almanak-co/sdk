"""Anti-vacuity tests for ``scripts/ci/check_orphan_scripts.py``.

The gate's whole premise is that a check must prove its own scope
(``docs/internal/PRD-ImprovsJuly28.md`` §3). A gate for orphaned scripts that
itself passes for the wrong reason would be the defect wearing the costume of
the fix, so most of what follows is deliberately adversarial: each test asserts
that a *plausible-looking* reference does **not** count as wiring.

The load-bearing one is
``test_read_text_reference_does_not_count_as_wiring`` — that is the exact shape
of the motivating defect, and a gate that fails it ships inert.
"""

from __future__ import annotations

import datetime as _dt
import textwrap

import pytest

import scripts.ci.check_orphan_scripts as gate

#: A date inside the allowlist horizon. Computed rather than hardcoded: a
#: far-future literal like 2999-01-01 satisfies a mandatory `expires` while
#: defeating its purpose, which is exactly what the horizon cap rejects.
VALID_EXPIRY = (_dt.date.today() + _dt.timedelta(days=30)).isoformat()


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    """A miniature repo with the same shape as the real one.

    Every module-level path constant is redirected, so these tests never read
    the real tree and cannot pass merely because the real repo happens to be
    tidy.
    """
    ci = tmp_path / "scripts" / "ci"
    workflows = tmp_path / ".github" / "workflows"
    tests_dir = tmp_path / "tests"
    container = tmp_path / "nightly-test-builds"
    for directory in (ci, workflows, tests_dir, container):
        directory.mkdir(parents=True)

    monkeypatch.setattr(gate, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(gate, "CI_DIR", ci)
    monkeypatch.setattr(gate, "WORKFLOW_DIR", workflows)
    monkeypatch.setattr(gate, "TESTS_DIR", tests_dir)
    monkeypatch.setattr(gate, "CONTAINER_ENTRYPOINT_DIR", container)
    monkeypatch.setattr(gate, "MAKEFILE", tmp_path / "Makefile")
    monkeypatch.setattr(gate, "ALLOWLIST", ci / "orphan-script-allowlist.yml")
    return tmp_path


def _script(repo, name: str, body: str = "#!/usr/bin/env bash\necho hi\n"):
    path = repo / "scripts" / "ci" / name
    path.write_text(body, encoding="utf-8")
    return path


def _workflow(repo, name: str, run: str):
    (repo / ".github" / "workflows" / name).write_text(
        textwrap.dedent(f"""\
        name: T
        on: [push]
        jobs:
          j:
            runs-on: ubuntu-latest
            steps:
              - run: {run}
        """),
        encoding="utf-8",
    )


def _workflow_block(repo, name: str, lines: list[str]):
    """A workflow whose `run:` is a BLOCK scalar, for multi-line commands.

    `_workflow` emits a plain scalar, so embedded newlines are not valid there.
    """
    body = "\n".join(f"              {line}" for line in lines)
    (repo / ".github" / "workflows" / name).write_text(
        "name: T\non: [push]\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: |\n" + body + "\n",
        encoding="utf-8",
    )


def _orphans(repo) -> set[str]:
    candidates = gate.collect_candidates()
    gate.resolve_reachable(candidates, gate.build_reference_sites())
    return {c.rel for c in candidates if not c.referrers}


# --------------------------------------------------------------------------
# The motivating defect
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        # One-line form — the spelling the real file happens to use today.
        'import subprocess\nsrc = (ROOT / "scripts/ci/harness.sh").read_text()\nassert "foo" in src\n',
        # Two-line form — the conventional split. The audit of PR #3494 found
        # the gate passed vacuously here: the path-defining line survived the
        # data-read filter, so the motivating orphan read as "wired". The
        # one-line test alone vouched for a gate blind to its own motivating
        # case, which is why this is parametrised rather than replaced.
        'import subprocess\nharness = ROOT / "scripts/ci/harness.sh"\nsrc = harness.read_text()\n',
        # Same, via a plain string.
        'import subprocess\nP = "scripts/ci/harness.sh"\nsrc = open(P).read()\n',
        # `subprocess` named only in prose must not confer spawn capability.
        '# We deliberately do not subprocess this harness.\nREADERS = ["scripts/ci/harness.sh"]\n',
        # A bare path in a data structure is not a call site — this is the
        # exact shape of LANDED_SQL_READERS in the real accounting test.
        'import subprocess\nREADERS = ["scripts/ci/harness.sh"]\n',
    ],
    ids=["one-line", "two-line", "open-via-var", "subprocess-in-comment", "path-in-list"],
)
def test_read_text_reference_does_not_count_as_wiring(fake_repo, body):
    """A test asserting on a harness's SOURCE TEXT does not run the harness.

    This mirrors ``tests/unit/accounting/test_landed_population_readers.py``,
    which reads ``accounting_regression_check.sh`` via ``.read_text()`` to
    assert on its contents. Counting that as wiring would mark the accounting
    ship-gate green — the precise failure this gate was built to expose.

    Every spelling below must reach the same verdict; a gate that only handles
    the spelling the real file happens to use today is one reformat away from
    inert.
    """
    _script(fake_repo, "harness.sh")
    (fake_repo / "tests" / "test_reads.py").write_text(body, encoding="utf-8")
    assert "scripts/ci/harness.sh" in _orphans(fake_repo)


def test_importing_a_script_does_count_as_wiring(fake_repo):
    """The flip side: an imported module's code genuinely executes in CI."""
    _script(fake_repo, "helper.py", "#!/usr/bin/env python3\nif __name__ == '__main__':\n    pass\n")
    (fake_repo / "tests" / "test_imports.py").write_text(
        "import scripts.ci.helper as helper\n\n\ndef test_x():\n    assert helper\n",
        encoding="utf-8",
    )
    assert "scripts/ci/helper.py" not in _orphans(fake_repo)


def test_subprocess_invocation_counts_as_wiring(fake_repo):
    """A spawn call with the path in its arguments is genuine wiring."""
    _script(fake_repo, "picker.py", "#!/usr/bin/env python3\nif __name__ == '__main__':\n    pass\n")
    (fake_repo / "tests" / "test_spawn.py").write_text(
        "import subprocess\n"
        "PICKER = ROOT / 'scripts' / 'ci' / 'picker.py'\n"
        "def test_x():\n"
        "    subprocess.run([PICKER])\n",
        encoding="utf-8",
    )
    assert "scripts/ci/picker.py" not in _orphans(fake_repo)


def test_the_positive_and_negative_cases_differ_by_behaviour_not_layout(fake_repo):
    """The discriminating mutation: swap the spawn for a read, keep everything else.

    This is the test the audit asked for. The old positive test asserted
    "subprocess invocation counts as wiring", but the implementation only
    checked *the file contains a spawn token somewhere* AND *some line mentions
    the name* — so replacing `subprocess.run([PICKER])` with `PICKER.read_text()`
    while keeping `import subprocess` still passed. The positive and negative
    tests were then distinguished only by whether `.read_text()` happened to
    land on the same physical line as the path, which is layout, not behaviour.

    Same file, same import, same variable — only the final call differs, and the
    verdict must flip.
    """
    common = "import subprocess\nPICKER = ROOT / 'scripts' / 'ci' / 'picker.py'\ndef test_x():\n    "

    _script(fake_repo, "picker.py", "#!/usr/bin/env python3\nif __name__ == '__main__':\n    pass\n")
    (fake_repo / "tests" / "t.py").write_text(common + "subprocess.run([PICKER])\n", encoding="utf-8")
    assert "scripts/ci/picker.py" not in _orphans(fake_repo), "spawn must count as wiring"

    (fake_repo / "tests" / "t.py").write_text(common + "src = PICKER.read_text()\n", encoding="utf-8")
    assert "scripts/ci/picker.py" in _orphans(fake_repo), (
        "a read of the same variable, in a file that still imports subprocess, must NOT count as wiring"
    )


# --------------------------------------------------------------------------
# References that look like wiring but are not
# --------------------------------------------------------------------------


def test_docstring_mention_in_a_wired_script_does_not_vouch(fake_repo):
    """Prose in a wired .py must not vouch for the script it describes.

    Regression test for a real defect in this gate's own development: this
    file's docstring names the orphans it exists to catch, so before docstrings
    were stripped the gate silently marked them wired.
    """
    _script(fake_repo, "orphan.sh")
    _script(
        fake_repo,
        "wired.py",
        "#!/usr/bin/env python3\n"
        '"""Docs mention scripts/ci/orphan.sh as a sibling gate."""\n'
        "if __name__ == '__main__':\n    pass\n",
    )
    _workflow(fake_repo, "w.yml", "python scripts/ci/wired.py")
    orphans = _orphans(fake_repo)
    assert "scripts/ci/wired.py" not in orphans
    assert "scripts/ci/orphan.sh" in orphans


def test_shell_comment_reference_does_not_vouch(fake_repo):
    """A `#` comment in a wired shell script is documentation, not a call."""
    _script(fake_repo, "orphan.sh")
    _script(fake_repo, "wired.sh", "#!/usr/bin/env bash\n# complements scripts/ci/orphan.sh\necho hi\n")
    _workflow(fake_repo, "w.yml", "bash scripts/ci/wired.sh")
    orphans = _orphans(fake_repo)
    assert "scripts/ci/wired.sh" not in orphans
    assert "scripts/ci/orphan.sh" in orphans


def test_workflow_yaml_comment_does_not_vouch(fake_repo):
    """A comment in workflow YAML never reaches a `run:` scalar.

    This is the real disposition of ``check_connector_gateway_compliance.sh``
    (VIB-6210): its only workflow mention is a header comment.
    """
    _script(fake_repo, "orphan.sh")
    (fake_repo / ".github" / "workflows" / "w.yml").write_text(
        textwrap.dedent("""\
        # Complements scripts/ci/orphan.sh (static, grep).
        name: T
        on: [push]
        jobs:
          j:
            runs-on: ubuntu-latest
            steps:
              - run: echo unrelated
        """),
        encoding="utf-8",
    )
    assert "scripts/ci/orphan.sh" in _orphans(fake_repo)


def test_echo_of_a_path_is_not_an_invocation(fake_repo):
    """Printing a path is a help message, not wiring."""
    _script(fake_repo, "orphan.sh")
    _workflow(fake_repo, "w.yml", "echo 'run scripts/ci/orphan.sh to fix'")
    assert "scripts/ci/orphan.sh" in _orphans(fake_repo)


@pytest.mark.parametrize(
    "command",
    [
        "cat scripts/ci/orphan.sh",
        "cp scripts/ci/orphan.sh artifact/",
        "mv scripts/ci/orphan.sh /tmp/x.sh",
        "rm -f scripts/ci/orphan.sh",
        "test -f scripts/ci/orphan.sh",
        "shellcheck scripts/ci/orphan.sh",
        "chmod +x scripts/ci/orphan.sh",
        "grep -q foo scripts/ci/orphan.sh",
        "wc -l scripts/ci/orphan.sh",
        "tar czf out.tgz scripts/ci/orphan.sh",
    ],
)
def test_handling_a_file_is_not_running_it(fake_repo, command):
    """Naming a script in a command that never executes it is not wiring.

    Found by the Codex auditor on PR #3494: matching on "the command text
    mentions this path" let `cat`, `cp`, `test -f` and friends certify a genuine
    orphan. The gate's whole premise is that a reference must actually run the
    code.
    """
    _script(fake_repo, "orphan.sh")
    _workflow(fake_repo, "w.yml", command)
    assert "scripts/ci/orphan.sh" in _orphans(fake_repo)


@pytest.mark.parametrize(
    "command",
    [
        # Commands absent from any hand-written deny-list. Round 2 of the audit
        # found each of these certified an orphan, which is why the polarity was
        # inverted to an allow-list of EXECUTING commands: a missing entry now
        # produces a false alarm someone notices, not a vacuous pass nobody does.
        "ls -l scripts/ci/orphan.sh",
        "git add scripts/ci/orphan.sh",
        "jq . scripts/ci/orphan.sh",
        "nl scripts/ci/orphan.sh",
        "du -h scripts/ci/orphan.sh",
        "rsync scripts/ci/orphan.sh /tmp/",
        # `find` executes only with -exec; the search form does not.
        "find scripts/ci -name orphan.sh",
        # A wrapper prefix must not launder the inner command: `uv` executes,
        # but `ruff` merely lints the file it is handed.
        "uv run ruff check scripts/ci/orphan.sh",
        "poetry run black scripts/ci/orphan.sh",
        "command cat scripts/ci/orphan.sh",
        "env cat scripts/ci/orphan.sh",
        # A QUOTED separator must not split the line and smuggle the path past
        # the guard on the left-hand fragment.
        "grep -E 'foo|scripts/ci/orphan.sh' f.txt",
        "sed -e 's|a|b|' -f scripts/ci/orphan.sh",
    ],
)
def test_naming_a_file_without_running_it_is_not_wiring(fake_repo, command):
    """Cases chosen to DEFEAT the implementation, not to mirror its constant.

    The earlier version of this test drew all ten of its cases from the shape
    the deny-list already handled — a parametrisation that cannot detect that
    the constant is incomplete. These are the shapes that were failing.
    """
    _script(fake_repo, "orphan.sh")
    _workflow(fake_repo, "w.yml", command)
    assert "scripts/ci/orphan.sh" in _orphans(fake_repo)


@pytest.mark.parametrize(
    "command",
    [
        "bash scripts/ci/used.sh",
        "./scripts/ci/used.sh",  # direct path invocation via shebang
        "uv run python scripts/ci/used.sh",
        "find scripts/ci -name '*.sh' -exec bash scripts/ci/used.sh {} ;",
        "DEBUG=1 timeout 60 bash scripts/ci/used.sh",
    ],
)
def test_genuine_invocations_still_count(fake_repo, command):
    """The allow-list must not swing so far that real wiring reads as orphaned.

    `./scripts/ci/x.sh` is the one no allow-list entry could cover — it runs via
    its own shebang and names no interpreter.
    """
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", command)
    assert "scripts/ci/used.sh" not in _orphans(fake_repo)


def test_sibling_invocation_via_dirname_counts(fake_repo):
    """`bash "$(dirname "$0")/x.sh"` is the dominant sibling idiom.

    The filename fallback exists precisely for this, but its lookbehind rejected
    a name preceded by `/` — so the docstring claimed a behaviour the regex
    denied, and a wired sibling read as an orphan. This is the exact form
    `accounting_regression_check.sh` uses.
    """
    _script(fake_repo, "outer.sh", '#!/usr/bin/env bash\nbash "$(dirname "$0")/inner.sh"\n')
    _script(fake_repo, "inner.sh")
    _workflow(fake_repo, "w.yml", "bash scripts/ci/outer.sh")
    assert "scripts/ci/inner.sh" not in _orphans(fake_repo)


def test_similarly_named_script_is_not_confused_for_another(fake_repo):
    """The lookbehind still has to reject `myinner.sh` matching `inner.sh`."""
    _script(fake_repo, "outer.sh", "#!/usr/bin/env bash\nbash scripts/ci/myinner.sh\n")
    _script(fake_repo, "myinner.sh")
    _script(fake_repo, "inner.sh")
    _workflow(fake_repo, "w.yml", "bash scripts/ci/outer.sh")
    orphans = _orphans(fake_repo)
    assert "scripts/ci/inner.sh" in orphans
    assert "scripts/ci/myinner.sh" not in orphans


def test_unparseable_workflow_fails_the_gate(fake_repo):
    """An unscanned reference site must never read as a clean one.

    A malformed workflow silently removed itself from coverage: scripts it wires
    would read as orphans, and the gate reported success regardless. "The check
    didn't run" is not "the check found nothing".
    """
    _script(fake_repo, "used.sh")
    (fake_repo / ".github" / "workflows" / "broken.yml").write_text(
        "name: T\non: [push]\njobs:\n  j:\n    steps:\n      - run: [\n", encoding="utf-8"
    )
    assert gate.main([]) == 1


def test_a_real_invocation_chained_after_a_data_command_still_counts(fake_repo):
    """Segment-level filtering, not line-level.

    Dropping the whole line because it starts with `cat` would discard the real
    invocation after `&&` — trading a vacuous pass for a false alarm.
    """
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", "cat scripts/ci/used.sh && bash scripts/ci/used.sh")
    assert "scripts/ci/used.sh" not in _orphans(fake_repo)


def test_env_prefixed_invocation_still_counts(fake_repo):
    """`FOO=bar bash scripts/ci/x.sh` runs the script; the assignment is not the command."""
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", "DEBUG=1 bash scripts/ci/used.sh")
    assert "scripts/ci/used.sh" not in _orphans(fake_repo)


def test_self_reference_does_not_vouch(fake_repo):
    """A script's own usage banner must not make it look wired."""
    _script(
        fake_repo,
        "lonely.sh",
        "#!/usr/bin/env bash\nusage() { printf 'bash scripts/ci/lonely.sh --flag'; }\nusage\n",
    )
    assert "scripts/ci/lonely.sh" in _orphans(fake_repo)


def test_orphan_calling_an_orphan_stays_orphaned(fake_repo):
    """Reachability flows from the roots outward, never in a cycle.

    ``accounting_regression_check.sh`` calls ``accounting_regression_assert.py``;
    if scripts vouched for each other regardless of their own reachability, an
    unreachable pair would certify itself.
    """
    _script(fake_repo, "caller.sh", "#!/usr/bin/env bash\nbash scripts/ci/callee.sh\n")
    _script(fake_repo, "callee.sh")
    assert {"scripts/ci/caller.sh", "scripts/ci/callee.sh"} <= _orphans(fake_repo)


# --------------------------------------------------------------------------
# Genuine wiring
# --------------------------------------------------------------------------


def test_workflow_run_step_wires_a_script(fake_repo):
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", "bash scripts/ci/used.sh")
    assert "scripts/ci/used.sh" not in _orphans(fake_repo)


def test_makefile_recipe_wires_a_script(fake_repo):
    _script(fake_repo, "used.sh")
    (fake_repo / "Makefile").write_text("target:\n\tbash scripts/ci/used.sh\n", encoding="utf-8")
    assert "scripts/ci/used.sh" not in _orphans(fake_repo)


def test_makefile_comment_does_not_wire_a_script(fake_repo):
    """Only recipe lines count; this repo's Makefile is full of prose blocks."""
    _script(fake_repo, "orphan.sh")
    (fake_repo / "Makefile").write_text(
        "# see scripts/ci/orphan.sh for details\ntarget:\n\techo hi\n", encoding="utf-8"
    )
    assert "scripts/ci/orphan.sh" in _orphans(fake_repo)


def test_container_entrypoint_wires_a_script(fake_repo):
    """The nightly image runs on a cron and is reachable from no workflow."""
    _script(fake_repo, "snapshot.py", "#!/usr/bin/env python3\nif __name__ == '__main__':\n    pass\n")
    (fake_repo / "nightly-test-builds" / "entrypoint.sh").write_text(
        '#!/usr/bin/env bash\npython3 "scripts/ci/snapshot.py"\n', encoding="utf-8"
    )
    assert "scripts/ci/snapshot.py" not in _orphans(fake_repo)


def test_transitive_wiring_through_a_wired_script(fake_repo):
    _script(fake_repo, "outer.sh", "#!/usr/bin/env bash\nbash scripts/ci/inner.sh\n")
    _script(fake_repo, "inner.sh")
    _workflow(fake_repo, "w.yml", "bash scripts/ci/outer.sh")
    assert not {"scripts/ci/outer.sh", "scripts/ci/inner.sh"} & _orphans(fake_repo)


# --------------------------------------------------------------------------
# Candidate selection
# --------------------------------------------------------------------------


def test_data_files_are_not_candidates(fake_repo):
    """A baseline .txt/.yml is data. Treating it as a script is the noise that
    would make the allowlist unreadable (PRD P4 anti-vacuity requirement)."""
    (fake_repo / "scripts" / "ci" / "some-baseline.txt").write_text("x\n", encoding="utf-8")
    (fake_repo / "scripts" / "ci" / "allow.yml").write_text("a: 1\n", encoding="utf-8")
    assert gate.collect_candidates() == []


def test_library_module_without_entrypoint_is_not_a_candidate(fake_repo):
    (fake_repo / "scripts" / "ci" / "lib.py").write_text("VALUE = 1\n", encoding="utf-8")
    assert gate.collect_candidates() == []


def test_init_and_pycache_are_skipped(fake_repo):
    (fake_repo / "scripts" / "ci" / "__init__.py").write_text("", encoding="utf-8")
    cache = fake_repo / "scripts" / "ci" / "__pycache__"
    cache.mkdir()
    (cache / "x.py").write_text("if __name__ == '__main__':\n    pass\n", encoding="utf-8")
    assert gate.collect_candidates() == []


# --------------------------------------------------------------------------
# Allowlist lifecycle
# --------------------------------------------------------------------------


def _write_allowlist(repo, body: str):
    (repo / "scripts" / "ci" / "orphan-script-allowlist.yml").write_text(body, encoding="utf-8")


def test_allowlist_entry_requires_reason_and_ticket(fake_repo):
    _write_allowlist(fake_repo, "allowed:\n  scripts/ci/x.sh:\n    owner: someone\n")
    _, errors = gate.load_allowlist()
    assert any("reason" in e for e in errors)
    assert any("ticket" in e for e in errors)


def test_allowlist_entry_requires_both_owner_and_expiry(fake_repo):
    """`expires` is mandatory, not an alternative to `owner`.

    The earlier rule accepted either one, so `owner: someone` with no `expires`
    produced an entry that never expired — the allowlist file's own "the expiry
    is the point" comment describing a lifecycle the code did not enforce. The
    test that encoded `owner OR expires` was itself codifying the loophole,
    which is why it was rewritten rather than adjusted.
    """
    _write_allowlist(fake_repo, "allowed:\n  scripts/ci/x.sh:\n    reason: r\n    ticket: VIB-1\n")
    _, errors = gate.load_allowlist()
    assert any("owner" in e for e in errors)
    assert any("expires" in e for e in errors)

    # owner alone must NOT satisfy the schema
    _write_allowlist(fake_repo, "allowed:\n  scripts/ci/x.sh:\n    reason: r\n    ticket: VIB-1\n    owner: o\n")
    _, errors = gate.load_allowlist()
    assert any("expires" in e for e in errors), "owner alone must not buy a permanent exemption"


def test_far_future_expiry_is_rejected(fake_repo):
    """A mandatory `expires` is decorative if `2999-01-01` satisfies it."""
    _write_allowlist(
        fake_repo,
        'allowed:\n  scripts/ci/x.sh:\n    reason: r\n    ticket: VIB-1\n    owner: o\n    expires: "2999-01-01"\n',
    )
    _, errors = gate.load_allowlist()
    assert any("cap is" in e for e in errors)


@pytest.mark.parametrize(
    "content",
    ["allowed:\n  - just\n  - a list\n", "just a scalar\n", "allowed:\n  x: [\n"],
    ids=["top-level-list", "scalar", "malformed-yaml"],
)
def test_malformed_allowlist_fails_in_the_gates_own_format(fake_repo, content):
    """A bad allowlist must fail closed with a message, not a traceback.

    Found by CodeRabbit and the Claude auditor. `yaml.safe_load` was unguarded
    and `raw.get` assumed a mapping, so a top-level list raised AttributeError —
    contradicting `load_allowlist`'s own contract that malformed entries are
    errors rather than crashes.
    """
    _write_allowlist(fake_repo, content)
    entries, errors = gate.load_allowlist()
    assert entries == {}
    assert errors, "a malformed allowlist must produce a reported error"


def test_expired_allowlist_entry_fails(fake_repo):
    """The expiry is the forcing function; without this the list is a graveyard."""
    _write_allowlist(
        fake_repo,
        'allowed:\n  scripts/ci/x.sh:\n    reason: r\n    ticket: VIB-1\n    owner: o\n    expires: "2000-01-01"\n',
    )
    _, errors = gate.load_allowlist()
    assert any("expired" in e for e in errors)


def test_valid_allowlist_entry_produces_no_errors(fake_repo):
    _write_allowlist(
        fake_repo,
        f'allowed:\n  scripts/ci/x.sh:\n    reason: r\n    ticket: VIB-1\n    owner: o\n    expires: "{VALID_EXPIRY}"\n',
    )
    entries, errors = gate.load_allowlist()
    assert errors == []
    assert "scripts/ci/x.sh" in entries


def test_stale_allowlist_entry_fails_the_gate(fake_repo):
    """Once a script is wired, its entry must go — the list only shrinks."""
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", "bash scripts/ci/used.sh")
    _write_allowlist(
        fake_repo,
        f'allowed:\n  scripts/ci/used.sh:\n    reason: r\n    ticket: VIB-1\n    owner: o\n    expires: "{VALID_EXPIRY}"\n',
    )
    assert gate.main([]) == 1


def test_unallowlisted_orphan_fails_the_gate(fake_repo):
    _script(fake_repo, "orphan.sh")
    assert gate.main([]) == 1


def test_allowlisted_orphan_passes_the_gate(fake_repo):
    _script(fake_repo, "orphan.sh")
    _write_allowlist(
        fake_repo,
        "allowed:\n  scripts/ci/orphan.sh:\n    reason: r\n    ticket: VIB-1\n"
        f'    owner: o\n    expires: "{VALID_EXPIRY}"\n',
    )
    assert gate.main([]) == 0


# --------------------------------------------------------------------------
# Round 4: flag arity and one-line function definitions
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        # Flag ARITY was modelled with a deny-list of "valueless" long flags —
        # the same wrong polarity that was inverted one layer up, and on the
        # wrong side of the base rate since most long flags are booleans. Each
        # wrong guess ate the real command. The tell was instability: `docker
        # run --rm img bash x.sh` worked while `--rm -v a:b` did not, purely on
        # token count. Arity is no longer modelled at all.
        "docker run --rm img bash scripts/ci/used.sh",
        "docker run --rm -v /a:/b img bash scripts/ci/used.sh",
        "docker run --rm --pull always img bash scripts/ci/used.sh",
        "docker run --privileged --network host img bash scripts/ci/used.sh",
        "kubectl exec --stdin --tty pod -- bash scripts/ci/used.sh",
        "kubectl exec -it pod -- bash scripts/ci/used.sh",
        "uv run --project . python scripts/ci/used.sh",
        "uv run --frozen python scripts/ci/used.sh",
        "uv run --no-progress --all-extras python scripts/ci/used.sh",
        "npm run build && bash scripts/ci/used.sh",
    ],
)
def test_flags_and_their_values_never_hide_the_command(fake_repo, command):
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", command)
    assert "scripts/ci/used.sh" not in _orphans(fake_repo), command


@pytest.mark.parametrize(
    "definition",
    [
        "run_gate() { bash scripts/ci/used.sh; }",
        "run_gate(){ bash scripts/ci/used.sh; }",
        "function run_gate { bash scripts/ci/used.sh; }",
        "py() { uv run python scripts/ci/used.sh; }",
    ],
)
def test_one_line_function_definitions_do_not_hide_invocations(fake_repo, definition):
    """`run_gate() { bash x.sh; }` led with a token matching no category.

    The walker gave up before reaching `{`, so every invocation inside the
    one-liner was invisible. The multi-line form worked, so this bit only the
    one-liner — which is the idiom this repo actually uses.
    """
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", definition)
    assert "scripts/ci/used.sh" not in _orphans(fake_repo), definition


def test_assignment_and_expansion_pair_across_functions(fake_repo):
    """The file-scoped pairing must survive both sides being inside functions.

    This shape was still an orphan until the one-line-function fix landed: the
    pairing itself was correct, but `b(){ bash "$S"; }` never registered as an
    executing segment, so `S` was never recorded as expanded.
    """
    _script(fake_repo, "used.sh")
    _workflow_block(
        fake_repo,
        "w.yml",
        ['a(){ S="scripts/ci/used.sh"; }', 'b(){ bash "$S"; }', "a; b"],
    )
    assert "scripts/ci/used.sh" not in _orphans(fake_repo)


def test_a_linter_run_inside_a_function_still_does_not_vouch(fake_repo):
    """The function fix must not become a blanket vouch for anything inside one."""
    _script(fake_repo, "orphan.sh")
    _workflow(fake_repo, "w.yml", "lint() { uv run ruff check scripts/ci/orphan.sh; }")
    assert "scripts/ci/orphan.sh" in _orphans(fake_repo)


@pytest.mark.parametrize(
    ("command", "wired"),
    [
        # Subshell grouping. `{ ... }` was already covered but `( ... )` was
        # not, so the brace form worked and the paren form silently dropped the
        # invocation — an asymmetry that only shows up if you probe both.
        ("( bash scripts/ci/used.sh )", True),
        ("( cd /tmp && bash scripts/ci/used.sh )", True),
        ("subshell() ( bash scripts/ci/used.sh )", True),
        # ...without becoming a blanket vouch for anything inside parens.
        ("( cat scripts/ci/used.sh )", False),
        ("( uv run ruff check scripts/ci/used.sh )", False),
    ],
)
def test_subshell_grouping_does_not_hide_invocations(fake_repo, command, wired):
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", command)
    assert ("scripts/ci/used.sh" not in _orphans(fake_repo)) is wired, command


@pytest.mark.parametrize(
    "command",
    [
        # A flag VALUE that collides with an allow-listed command name must not
        # derail the forward scan. These all still execute something, so the
        # verdict is right either way — recorded because it is the failure mode
        # a reader will worry about when they see the scan ignore arity.
        "docker run --user root bash scripts/ci/used.sh",
        "docker run --entrypoint bash img scripts/ci/used.sh",
        "docker run --name python img bash scripts/ci/used.sh",
        "uv run --python python3.12 python scripts/ci/used.sh",
    ],
)
def test_flag_values_colliding_with_command_names(fake_repo, command):
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", command)
    assert "scripts/ci/used.sh" not in _orphans(fake_repo), command


@pytest.mark.parametrize(
    ("command", "wired"),
    [
        # File HANDLERS inside a container or tool-runner segment must not
        # vouch. The forward scan used to skip straight past them and land on
        # the script path further right, so the deny-list was largely moot.
        ("docker run --rm img cat scripts/ci/used.sh", False),
        ("kubectl exec pod -- cat scripts/ci/used.sh", False),
        ("docker run --rm img tar czf out.tgz scripts/ci/used.sh", False),
        ("uv run ruff check scripts/ci/used.sh", False),
        # ...while real invocations through the same shapes still wire.
        ("docker run --rm img bash scripts/ci/used.sh", True),
        ("uv run python scripts/ci/used.sh", True),
        # `. x.sh` is the POSIX source shorthand. `Path(".").name` is "", so the
        # `.` entry in _EXECUTING_COMMANDS was dead code and this was dropped.
        (". scripts/ci/used.sh", True),
        ("source scripts/ci/used.sh", True),
        # A wrapper's own positional argument must not be read as the command.
        ("flock /tmp/lock bash scripts/ci/used.sh", True),
        ("su someuser bash scripts/ci/used.sh", True),
    ],
)
def test_handlers_and_wrapper_arguments(fake_repo, command, wired):
    _script(fake_repo, "used.sh")
    _workflow(fake_repo, "w.yml", command)
    assert ("scripts/ci/used.sh" not in _orphans(fake_repo)) is wired, command
