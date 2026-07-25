"""Full-close regression tests for the gmx_perp_lifecycle demo.

VIB-5950 / ALM-2976 regression pin: both close paths (the iteration-lane
``_create_close_intent`` and the teardown-lane ``generate_teardown_intents``)
must emit ``size_usd=None`` so the GMX compiler live-reads the on-chain
position size at compile time. Passing a cached notional (``_position_size_usd``)
strands residual dust whenever the position has drifted from the remembered
size — exactly the customer shape reported in ALM-2976.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

_SEED_DIR = (
    Path(__file__).resolve().parents[3]
    / "almanak"
    / "demo_strategies"
    / "gmx_perp_lifecycle"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("gmx_lifecycle_seed", _SEED_DIR / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def strat():
    module = _load_module()
    cls = module.GMXPerpLifecycleStrategy
    cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = cls.__new__(cls)
        s._config = cfg
        s.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(s)
    return s


class TestFullCloseSemantics:
    def test_iteration_close_emits_size_none_even_with_cached_size(self, strat):
        # Simulate a tracked (cached) notional that has drifted from on-chain state.
        strat._position_size_usd = Decimal("100")
        intent = strat._create_close_intent()
        assert intent.intent_type.value == "PERP_CLOSE"
        # The cached size must NEVER leak into the close intent.
        assert intent.size_usd is None

    def test_teardown_close_emits_size_none_even_with_cached_size(self, strat):
        from almanak.framework.teardown import TeardownMode

        strat._loop_state = "open"
        strat._position_size_usd = Decimal("100")
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "PERP_CLOSE"
        assert intents[0].size_usd is None
