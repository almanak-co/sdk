"""Tests for ``scripts/ci/check_config_boundary.py``.

PR #2107 / CodeRabbit #2: regex-on-tokenised-source caught literal
spellings (``os.environ.get(...)``, ``os.getenv(...)``) but not aliased
imports — ``import os as _os`` then ``_os.getenv(...)``, ``from os
import getenv`` then ``getenv(...)``, ``from dotenv import load_dotenv
as ld`` then ``ld(...)`` etc. all bypassed the gate.

The scanner now runs an AST pass first and falls back to the regex only
when AST parsing fails. These tests pin the AST pass against the alias
forms a determined PR author might use to slip new env reads past CI.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_config_boundary.py"
    spec = importlib.util.spec_from_file_location("check_config_boundary", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_config_boundary"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def module():
    return _load_module()


def _scan(module, text: str, tmp_path: Path) -> list[str]:
    """Helper: write ``text`` to a tmp ``.py`` file and return ``(label, snippet)``-like strings."""
    f = tmp_path / "sample.py"
    f.write_text(text)
    hits = module._scan_file(f)
    return [f"{h.pattern}::{h.snippet}" for h in hits]


def test_canonical_os_environ_get(module, tmp_path: Path) -> None:
    src = (
        "import os\n"
        "def f():\n"
        "    return os.environ.get('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ.<method>" in h and "os.environ.get('X')" in h for h in hits), hits


def test_canonical_os_getenv(module, tmp_path: Path) -> None:
    src = (
        "import os\n"
        "def f():\n"
        "    return os.getenv('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.getenv" in h for h in hits), hits


def test_aliased_module_import(module, tmp_path: Path) -> None:
    """``import os as _os`` then ``_os.getenv(...)`` must be caught."""
    src = (
        "import os as _os\n"
        "def f():\n"
        "    return _os.getenv('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.getenv" in h for h in hits), (
        f"aliased ``import os as _os`` -> ``_os.getenv`` not caught: {hits}"
    )


def test_from_os_import_getenv(module, tmp_path: Path) -> None:
    """``from os import getenv`` then a bare ``getenv(...)`` call must be caught."""
    src = (
        "from os import getenv\n"
        "def f():\n"
        "    return getenv('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.getenv" in h for h in hits), hits


def test_from_os_import_getenv_aliased(module, tmp_path: Path) -> None:
    """``from os import getenv as g`` then ``g(...)`` must be caught."""
    src = (
        "from os import getenv as g\n"
        "def f():\n"
        "    return g('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.getenv" in h for h in hits), hits


def test_from_os_import_environ(module, tmp_path: Path) -> None:
    """``from os import environ`` then ``environ['X']`` (subscript) must be caught."""
    src = (
        "from os import environ\n"
        "def f():\n"
        "    return environ['X']\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ[]" in h for h in hits), hits


def test_from_os_import_environ_method(module, tmp_path: Path) -> None:
    """``from os import environ`` then ``environ.get('X')`` (method) must be caught."""
    src = (
        "from os import environ\n"
        "def f():\n"
        "    return environ.get('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ.<method>" in h for h in hits), hits


def test_aliased_environ_subscript_assign(module, tmp_path: Path) -> None:
    """``environ['X'] = v`` (write through alias) must be caught."""
    src = (
        "from os import environ\n"
        "def f():\n"
        "    environ['X'] = 'v'\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ[]" in h for h in hits), hits


def test_aliased_load_dotenv(module, tmp_path: Path) -> None:
    """``from dotenv import load_dotenv as ld`` then ``ld(...)`` must be caught."""
    src = (
        "from dotenv import load_dotenv as ld\n"
        "def f():\n"
        "    ld('.env')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("load_dotenv" in h for h in hits), hits


def test_load_dotenv_bare(module, tmp_path: Path) -> None:
    src = (
        "from dotenv import load_dotenv\n"
        "load_dotenv()\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("load_dotenv" in h for h in hits), hits


def test_docstring_mentions_are_not_violations(module, tmp_path: Path) -> None:
    """Docstrings/comments referencing the API must not match (regression)."""
    src = (
        '"""This module documents os.environ.get usage.\n'
        '\n'
        'See also load_dotenv() in the dotenv package.\n'
        '"""\n'
        '# os.environ.get fictitious example in a comment\n'
        'x = 1\n'
    )
    hits = _scan(module, src, tmp_path)
    assert hits == [], f"docstring/comment hits should be empty: {hits}"


def test_unrelated_attribute_chain_not_flagged(module, tmp_path: Path) -> None:
    """A name that *looks* like ``environ`` but isn't from ``os`` must not match."""
    src = (
        "from someother import environ  # not from os\n"
        "def f():\n"
        "    return environ.get('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    # The alias resolves to ``someother.environ``, not ``os.environ`` — no hit.
    assert hits == [], f"unrelated environ alias should not match: {hits}"


# ---------------------------------------------------------------------------
# Scope-aware resolution (CodeRabbit round-3): the flat alias map missed
# assignment rebinding (false negative bypass) and parameter shadowing
# (false positive blocking valid code). The walker now tracks per-scope
# bindings; these tests pin both correctness properties.
# ---------------------------------------------------------------------------


def test_assignment_rebinding_caught(module, tmp_path: Path) -> None:
    """``env = os.environ; env.get('X')`` must not bypass the gate.

    Without scope-aware alias propagation, the alias ``env`` -> ``os.environ``
    isn't tracked and ``env.get('X')`` looks like a call on an unrelated name.
    Regression test for CodeRabbit's bypass demonstration on PR #2107.
    """
    src = (
        "import os\n"
        "env = os.environ\n"
        "def f():\n"
        "    return env.get('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ.<method>" in h for h in hits), (
        f"assignment rebinding bypass not caught: {hits}"
    )


def test_assignment_rebinding_through_attr_chain(module, tmp_path: Path) -> None:
    """``my_os = os; my_os.environ['X']`` must also be caught."""
    src = (
        "import os\n"
        "my_os = os\n"
        "def f():\n"
        "    return my_os.environ['X']\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ[]" in h for h in hits), (
        f"transitive alias rebinding not caught: {hits}"
    )


def test_assignment_rebinding_to_getenv(module, tmp_path: Path) -> None:
    """``g = os.getenv; g('X')`` must be caught."""
    src = (
        "import os\n"
        "g = os.getenv\n"
        "def f():\n"
        "    return g('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.getenv" in h for h in hits), hits


def test_parameter_shadowing_not_flagged(module, tmp_path: Path) -> None:
    """``def f(environ): environ.get('X')`` must NOT be flagged.

    A function parameter named ``environ`` shadows any module-level
    ``from os import environ``; the call resolves to the parameter, not
    the module attribute. Regression test for CodeRabbit's false-positive
    demonstration on PR #2107.
    """
    src = (
        "from os import environ\n"
        "def f(environ):\n"
        "    return environ.get('X')\n"
    )
    hits = _scan(module, src, tmp_path)
    # The function call resolves the parameter; it must not match.
    in_func_hits = [h for h in hits if h.endswith("environ.get('X')")]
    assert in_func_hits == [], (
        f"parameter shadowing wrongly flagged: {in_func_hits}"
    )


def test_nested_function_param_shadowing(module, tmp_path: Path) -> None:
    """Inner function parameter shadows outer function's alias."""
    src = (
        "import os\n"
        "def outer():\n"
        "    env = os.environ  # inner alias from rebinding\n"
        "    def inner(env):\n"
        "        return env.get('X')  # parameter — must NOT match\n"
        "    return env.get('Y')  # outer rebinding — MUST match\n"
    )
    hits = _scan(module, src, tmp_path)
    inner_hits = [h for h in hits if "env.get('X')" in h]
    outer_hits = [h for h in hits if "env.get('Y')" in h]
    assert inner_hits == [], f"inner parameter shadowing wrongly flagged: {inner_hits}"
    assert outer_hits, f"outer assignment-rebinding not caught: {hits}"


def test_function_arg_with_default_uses_caller_scope(module, tmp_path: Path) -> None:
    """A default-value expression executes in the *enclosing* scope, not the function's."""
    src = (
        "import os\n"
        "def f(x=os.environ.get('X')):\n"  # the call happens here, in the caller scope
        "    return x\n"
    )
    hits = _scan(module, src, tmp_path)
    assert any("os.environ.<method>" in h for h in hits), (
        f"default-value forbidden call not caught: {hits}"
    )
