from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _module():
    script = Path(__file__).resolve().parents[3] / "scripts/ci/check_qa_scalar_projection.py"
    spec = importlib.util.spec_from_file_location("check_qa_scalar_projection_test", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_scalar_projection_lint_allows_denominators_but_rejects_grades(tmp_path: Path) -> None:
    checker = _module()
    allowed = tmp_path / "allowed.py"
    allowed.write_text("summary = '126/130 market keys resolve'\n")
    forbidden = tmp_path / "forbidden.py"
    forbidden.write_text("summary = '3/8 green'\n")

    assert checker.violations((allowed,)) == []
    assert any("ratio-as-grade" in row for row in checker.violations((forbidden,)))


def test_scalar_projection_lint_rejects_empty_renderer_census(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checker = _module()
    monkeypatch.setattr(checker, "QA_RENDERERS", ())

    assert checker.main() == 1
    assert "no QA renderers discovered" in capsys.readouterr().err
