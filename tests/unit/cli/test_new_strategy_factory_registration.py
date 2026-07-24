"""Branch coverage for register_strategy_in_factory.

Covers factory-file creation, import placement relative to docstrings and
existing imports, and the idempotency guards.
"""

import ast

from almanak.framework.cli.new_strategy import register_strategy_in_factory


def _register(tmp_path, name="my-strategy"):
    register_strategy_in_factory(name, tmp_path)
    return (tmp_path / "__init__.py").read_text()


def test_creates_factory_file_when_missing(tmp_path):
    content = _register(tmp_path)
    assert "from .my_strategy import StrategyMyStrategy" in content
    assert 'register_strategy("my_strategy", StrategyMyStrategy)' in content
    assert "STRATEGY_REGISTRY" in content
    ast.parse(content)  # emitted factory must be valid Python


def test_import_inserted_after_multiline_docstring_and_imports(tmp_path):
    (tmp_path / "__init__.py").write_text(
        '"""\nExisting factory.\n"""\n\nfrom typing import Any\n\nSTATE = 1\n'
    )
    content = _register(tmp_path)
    lines = content.split("\n")
    import_idx = lines.index("from .my_strategy import StrategyMyStrategy")
    assert import_idx > lines.index("from typing import Any")
    assert import_idx < lines.index("STATE = 1")
    assert lines[-1] == 'register_strategy("my_strategy", StrategyMyStrategy)'
    ast.parse(content)


def test_import_inserted_after_single_line_docstring(tmp_path):
    (tmp_path / "__init__.py").write_text('"""One line."""\nVALUE = 1\n')
    content = _register(tmp_path)
    lines = content.split("\n")
    assert lines.index("from .my_strategy import StrategyMyStrategy") == 1
    ast.parse(content)


def test_no_docstring_import_goes_after_existing_imports(tmp_path):
    (tmp_path / "__init__.py").write_text("import json\n\nVALUE = 1\n")
    content = _register(tmp_path)
    lines = content.split("\n")
    assert lines.index("from .my_strategy import StrategyMyStrategy") == 1
    ast.parse(content)


def test_empty_file_gets_import_at_top(tmp_path):
    (tmp_path / "__init__.py").write_text("")
    content = _register(tmp_path)
    assert content.split("\n")[0] == "from .my_strategy import StrategyMyStrategy"
    ast.parse(content)


def test_existing_import_is_not_duplicated(tmp_path):
    first = _register(tmp_path)
    second = _register(tmp_path)
    assert first == second
    assert second.count("from .my_strategy import StrategyMyStrategy") == 1
    assert second.count('register_strategy("my_strategy", StrategyMyStrategy)') == 1


def test_existing_register_line_is_not_duplicated(tmp_path):
    (tmp_path / "__init__.py").write_text(
        '"""Factory."""\n\nregister_strategy("my_strategy", StrategyMyStrategy)\n'
    )
    content = _register(tmp_path)
    assert content.count('register_strategy("my_strategy", StrategyMyStrategy)') == 1
    assert "from .my_strategy import StrategyMyStrategy" in content


def test_appends_blank_line_before_register_when_missing(tmp_path):
    (tmp_path / "__init__.py").write_text('"""Factory."""\nVALUE = 1')
    content = _register(tmp_path)
    lines = content.split("\n")
    assert lines[-1] == 'register_strategy("my_strategy", StrategyMyStrategy)'
    assert lines[-2] == ""


def test_pascal_and_snake_case_derivation(tmp_path):
    # Every capital gets its own underscore: "LP" -> "l_p" -> "LP".
    content = _register(tmp_path, name="Cross Chain LP")
    assert "from .cross_chain_l_p import StrategyCrossChainLP" in content
    assert 'register_strategy("cross_chain_l_p", StrategyCrossChainLP)' in content
