"""Behaviour of the :mod:`scripts.ci.check_connector_registry` gate.

The gate has two layers (AST scan + registry-import) and several edge
cases that must report cleanly. We test in two passes:

1. **Unit-test the helpers** with synthetic ``__init__.py`` files written
   to a tempdir. Cheap, fast, no test pollution.
2. **Integration-test the main flow** by monkeypatching ``CONNECTORS_DIR``
   to point at a temp tree assembled per scenario, then invoking ``main``
   and inspecting the exit code and stderr.

The real-repo flow ("run the gate on the actual repo after back-fill")
is exercised by ``make lint`` in CI and locally — that's the final
authority on "the gate works against reality".
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from pathlib import Path

import pytest

import scripts.ci.check_connector_registry as gate  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_registry() -> None:
    from almanak.connectors._strategy_base.registry import ConnectorRegistry

    ConnectorRegistry._clear()
    yield
    ConnectorRegistry._clear()


# ---------------------------------------------------------------------------
# Layer 1 unit tests — _count_register_connector_calls
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, src: str) -> Path:
    init = tmp_path / "__init__.py"
    init.write_text(textwrap.dedent(src), encoding="utf-8")
    return init


def test_counter_counts_zero_when_absent(tmp_path: Path) -> None:
    init = _write(tmp_path, '"""empty stub."""\n')
    assert gate._count_register_connector_calls(init) == 0


def test_counter_counts_one_module_level_call(tmp_path: Path) -> None:
    init = _write(
        tmp_path,
        """
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(
            name="foo",
            intents=(IntentType.SWAP,),
            chains=("ethereum",),
        )
        """,
    )
    assert gate._count_register_connector_calls(init) == 1


def test_counter_counts_two_when_duplicated(tmp_path: Path) -> None:
    init = _write(
        tmp_path,
        """
        register_connector(name="foo", intents=(), chains=None)
        register_connector(name="foo", intents=(), chains=None)
        """,
    )
    assert gate._count_register_connector_calls(init) == 2


def test_counter_ignores_nested_calls(tmp_path: Path) -> None:
    # The whole point of the static + import double-layer is that nested
    # calls don't count — the registry-import pass catches whether they
    # actually fired. Confirming the static layer behaves as advertised.
    init = _write(
        tmp_path,
        """
        if False:
            register_connector(name="foo", intents=(), chains=None)
        try:
            register_connector(name="bar", intents=(), chains=None)
        except Exception:
            pass
        def f():
            register_connector(name="baz", intents=(), chains=None)
        """,
    )
    assert gate._count_register_connector_calls(init) == 0


def test_counter_ignores_attribute_call(tmp_path: Path) -> None:
    # ``module.register_connector(...)`` is not the bare imported name.
    # Convention is to import the function directly; this catches a
    # connector that imports it via the module instead.
    init = _write(
        tmp_path,
        """
        import almanak.connectors._strategy_base.registry as r
        r.register_connector(name="foo", intents=(), chains=None)
        """,
    )
    assert gate._count_register_connector_calls(init) == 0


def test_counter_handles_unreadable_file(tmp_path: Path) -> None:
    init = tmp_path / "__init__.py"
    init.write_bytes(b"\xff\xfe\x00invalid utf-8")
    assert gate._count_register_connector_calls(init) == 0


def test_counter_handles_syntax_error(tmp_path: Path) -> None:
    init = _write(tmp_path, "this is not valid python (((\n")
    # Syntax-broken __init__.py reports 0 from the AST pass; the
    # registry-import pass will produce a real import error later.
    assert gate._count_register_connector_calls(init) == 0


# ---------------------------------------------------------------------------
# Layer 2 integration tests — main() against synthetic connector trees
# ---------------------------------------------------------------------------


def _build_fake_connectors_tree(root: Path, dirs_with_content: dict[str, str]) -> Path:
    """Construct a fake ``connectors/`` tree under ``root``.

    Each entry in ``dirs_with_content`` maps a connector-dir name to the
    contents of its ``__init__.py``. The returned path is the ``connectors``
    directory.
    """
    conn = root / "connectors"
    conn.mkdir(parents=True)
    for name, content in dirs_with_content.items():
        sub = conn / name
        sub.mkdir()
        (sub / "__init__.py").write_text(textwrap.dedent(content), encoding="utf-8")
    return conn


def _force_excluded(monkeypatch: pytest.MonkeyPatch, entries: tuple[tuple[str, str], ...]) -> None:
    monkeypatch.setattr(gate, "EXCLUDED_SUPPORT_MODULES", entries)


def _force_connectors_dir(monkeypatch: pytest.MonkeyPatch, path: Path) -> None:
    monkeypatch.setattr(gate, "CONNECTORS_DIR", path)


def test_main_fails_on_missing_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _build_fake_connectors_tree(
        tmp_path,
        {"foo_connector": '"""no registration here."""\n'},
    )
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, ())

    rc = gate.main([])

    assert rc == 1
    err = capsys.readouterr().err
    assert "missing-registration" in err
    assert "foo_connector" in err


def test_main_fails_on_duplicate_registration_in_one_init(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _build_fake_connectors_tree(
        tmp_path,
        {
            "foo": """
                register_connector(name="foo", intents=(), chains=None)
                register_connector(name="foo", intents=(), chains=None)
                """,
        },
    )
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, ())

    rc = gate.main([])
    assert rc == 1
    assert "duplicate-registration" in capsys.readouterr().err


def test_main_fails_on_excluded_dir_with_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Scenario 13 — excluded dir ALSO has register_connector. Decision was
    # FAIL: one source of truth per dir; excluded means excluded.
    conn = _build_fake_connectors_tree(
        tmp_path,
        {
            "base": """
                register_connector(name="base", intents=(), chains=None)
                """,
        },
    )
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, (("base", "support module"),))

    rc = gate.main([])
    assert rc == 1
    assert "excluded-also-registered" in capsys.readouterr().err


def test_main_fails_on_stale_excluded_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Scenario 14 — EXCLUDED names a dir that no longer exists.
    conn = _build_fake_connectors_tree(tmp_path, {})
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, (("ghost", "stale"),))

    rc = gate.main([])
    assert rc == 1
    err = capsys.readouterr().err
    assert "stale-excluded-entry" in err
    assert "ghost" in err


def test_main_fails_on_missing_init_py(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn = tmp_path / "connectors"
    (conn / "foo").mkdir(parents=True)
    # Note: no __init__.py written.
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, ())

    rc = gate.main([])
    assert rc == 1
    err = capsys.readouterr().err
    assert "missing-init" in err


def test_main_exit_2_when_connectors_dir_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _force_connectors_dir(monkeypatch, tmp_path / "does-not-exist")
    _force_excluded(monkeypatch, ())

    rc = gate.main([])
    assert rc == 2
    assert "INTERNAL ERROR" in capsys.readouterr().err


def _stub_import_verify(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bypass the registry-import layer for tests that only exercise the static scan.

    ``_registry_import_verify`` imports the *real* ``almanak.framework.connectors``
    package — it cannot be redirected via ``CONNECTORS_DIR`` monkeypatching.
    For tests that fake the dir tree, stub the second layer to a no-op so we
    only assert the static-scan behaviour. The import-verify integration is
    covered by the real-repo ``make check-connector-registry`` run.
    """
    monkeypatch.setattr(gate, "_registry_import_verify", lambda _non_excluded: [])


def test_main_skips_dunder_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ``__pycache__`` and similar are not connectors and must be ignored.
    conn = tmp_path / "connectors"
    conn.mkdir()
    (conn / "__pycache__").mkdir()
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, ())
    _stub_import_verify(monkeypatch)

    rc = gate.main([])
    assert rc == 0


def test_main_skips_loose_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ``bridge_base.py``, ``contract_registry.py`` etc. — module-level
    # files, not connector dirs.
    conn = tmp_path / "connectors"
    conn.mkdir()
    (conn / "bridge_base.py").write_text("# loose file\n")
    _force_connectors_dir(monkeypatch, conn)
    _force_excluded(monkeypatch, ())
    _stub_import_verify(monkeypatch)

    rc = gate.main([])
    assert rc == 0


def test_excluded_names_helper_returns_frozen_set() -> None:
    s = gate._excluded_names()
    assert isinstance(s, frozenset)
    # ``base`` / ``vaults`` foundation moved to ``_strategy_base/`` in
    # VIB-4835 Phase 2 and is excluded by the leading-underscore rule
    # in ``_enumerate_connector_dirs``, so no entry is needed here.
    assert "flash_loan" in s
