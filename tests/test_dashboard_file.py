"""dashboard.py beside strategy.py: discovery, fallback, conflict, preview before
strategy.py exists, the shared parameters.py, and what the source hash covers."""

from pathlib import Path

import pytest

from almanak.strategy import StrategyDefinition

CONFIG = """\
strategy:
  decision_interval: 60
  parameters:
    loops: 2
"""

PARAMETERS = """\
from pydantic import Field

from almanak.api import TypedParameters


class StrategyParams(TypedParameters):
    loops: int = Field(ge=1, le=5)
"""

DASHBOARD = """\
from almanak import dashboard as ui
from almanak.dashboard import build as b


def dashboard(context):
    loops = context.config.parameters.loops
    return b.dashboard("Loops", b.callout("loops", "Plan", f"{loops} rounds", "info"))
"""

HOOK_IN_STRATEGY = """\
from almanak.dashboard import build as b


def decide(context):
    return context.decision()


def dashboard(context):
    return b.dashboard("Fallback", b.callout("hook", "Plan", "from strategy.py", "info"))
"""

PLAIN_STRATEGY = """\
def decide(context):
    return context.decision()
"""


def workspace(tmp_path: Path, **files: str) -> Path:
    for name, text in files.items():
        (tmp_path / f"{name}.py" if name != "config" else tmp_path / "config.yaml").write_text(text)
    return tmp_path


def callout_text(document) -> str:
    return document.blocks[0].text


def test_a_hook_in_both_files_is_refused(tmp_path):
    root = workspace(tmp_path, config=CONFIG, strategy=HOOK_IN_STRATEGY, dashboard=DASHBOARD)
    with pytest.raises(ValueError, match="both dashboard.py and strategy.py define dashboard"):
        StrategyDefinition.read(root / "strategy.py")


def test_parameters_py_is_part_of_the_source_hash_and_dashboard_py_is_not(tmp_path):
    root = workspace(tmp_path, config=CONFIG, strategy=PLAIN_STRATEGY)
    alone = StrategyDefinition.read(root / "strategy.py")
    # Without siblings the hash is the strategy source's, as before.
    assert alone.model_dump() == {"filename": "strategy.py", "source": PLAIN_STRATEGY}
    (root / "dashboard.py").write_text(DASHBOARD)
    with_dashboard = StrategyDefinition.read(root / "strategy.py")
    assert with_dashboard.sha256 == alone.sha256 and with_dashboard == alone
    (root / "parameters.py").write_text(PARAMETERS)
    with_parameters = StrategyDefinition.read(root / "strategy.py")
    assert with_parameters.sha256 != alone.sha256 and with_parameters != alone


def test_strategy_hooks_get_the_parameters_py_model(tmp_path):
    from almanak.strategy_worker import load
    from almanak.typed_parameters import model_fields, parameters_type

    root = workspace(tmp_path, config=CONFIG, strategy=PLAIN_STRATEGY, parameters=PARAMETERS)
    namespace = load(StrategyDefinition.read(root / "strategy.py"))
    assert parameters_type(namespace).__name__ == "StrategyParams"
    assert model_fields(root / "strategy.py") == {"loops"}


NO_DOCUMENT = """\
def dashboard(context):
    return None
"""
