"""Regression tests for the pendle_basics demo (golden/codegen-seed candidate).

Locks in the two CodeRabbit Major findings fixed on the M1 decouple PR (#2995):

1. ``redeem_within_days`` code default is 0 — Pendle ``redeemPyToToken`` is
   post-expiry only, so a non-zero code default would emit a pre-expiry redeem
   Intent that reverts on-chain.
2. ``on_intent_executed`` rolls back the optimistic entry/exit state flags when
   execution fails, so a reverted buy (or exit) can be retried instead of
   permanently locking the strategy out of entry.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_SEED_DIR = (
    Path(__file__).resolve().parents[3]
    / "strategies"
    / "internal"
    / "demo_catalog"
    / "pendle_basics"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("pendle_basics_seed", _SEED_DIR / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_strategy(config_overrides: dict | None = None, drop_keys: list | None = None):
    """Construct PendleBasicsStrategy without the framework base __init__.

    Mirrors tests/unit/demo_strategies/test_gmx_v2_directional_perp.py: build the
    instance with ``__new__``, wire a ``get_config`` backed by the real
    config.json (plus any override), then run the demo's own ``__init__``.

    ``drop_keys`` removes keys from the config so the demo's OWN code default is
    exercised (``get_config`` returns its default only when the key is absent).
    """
    module = _load_module()
    cls = module.PendleBasicsStrategy
    cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
    if config_overrides:
        cfg.update(config_overrides)
    for key in drop_keys or []:
        cfg.pop(key, None)
    strat = cls.__new__(cls)
    strat._config = cfg
    strat.get_config = lambda k, d=None: cfg.get(k, d)
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        cls.__init__(strat)
    return module, strat


class TestRedeemWithinDaysDefault:
    def test_code_default_is_zero_post_expiry_only(self):
        """A user relying on the CODE default must get post-expiry-only redeem."""
        # Drop redeem_within_days from config so the demo's code default applies.
        module, strat = _make_strategy(drop_keys=["redeem_within_days"])
        assert strat.redeem_within_days == 0


class TestOnIntentExecutedRollback:
    """on_intent_executed must roll back optimistic flags when execution fails."""

    def test_failed_entry_resets_entered_flag_so_entry_can_retry(self):
        module, strat = _make_strategy()
        # decide() sets this True when it EMITS the buy, before the outcome is known.
        strat._has_entered_position = True
        strat._has_exited_position = False
        strat._consecutive_holds = 5

        strat.on_intent_executed(MagicMock(), success=False, result=MagicMock())

        # Rolled back -> next decide() routes to entry again, not _decide_exit.
        assert strat._has_entered_position is False
        assert strat._has_exited_position is False
        # The hold counter is part of the same rollback contract.
        assert strat._consecutive_holds == 0

    def test_failed_exit_resets_exited_flag_so_exit_can_retry(self):
        module, strat = _make_strategy()
        strat._has_entered_position = True
        strat._has_exited_position = True  # set when the exit intent was emitted
        strat._consecutive_holds = 7

        strat.on_intent_executed(MagicMock(), success=False, result=MagicMock())

        # Only the exit flag rolls back; the position is still considered open.
        assert strat._has_exited_position is False
        assert strat._has_entered_position is True
        assert strat._consecutive_holds == 0

    def test_successful_execution_is_a_noop(self):
        module, strat = _make_strategy()
        strat._has_entered_position = True
        strat._has_exited_position = False
        strat._consecutive_holds = 3

        strat.on_intent_executed(MagicMock(), success=True, result=MagicMock())

        # Success must NOT roll anything back (flags or hold counter).
        assert strat._has_entered_position is True
        assert strat._has_exited_position is False
        assert strat._consecutive_holds == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
