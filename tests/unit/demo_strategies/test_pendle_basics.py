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
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class _FakeBalance:
    def __init__(self, bal: "str | Decimal") -> None:
        self.balance = Decimal(str(bal))
        self.balance_usd = Decimal("0")


class _FakeMarket:
    """Minimal MarketSnapshot stand-in exposing ``balance(symbol)``.

    ``raise_for`` marks symbols whose read is UNMEASURED (raises ValueError,
    mirroring MarketSnapshot.balance on a gateway/resolution fault).
    """

    def __init__(self, balances: dict, raise_for: "set[str] | None" = None) -> None:
        self._balances = balances
        self._raise_for = raise_for or set()

    def balance(self, symbol: str):  # noqa: ANN201 - test stub
        if symbol in self._raise_for:
            raise ValueError(f"unmeasured balance for {symbol}")
        return _FakeBalance(self._balances.get(symbol, Decimal("0")))

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


class TestChainDerivedTeardown:
    """VIB-5590 facet B: teardown knowledge must derive from on-chain PT balance,
    not the volatile in-memory ``_has_entered_position`` flag (lost on restart).
    """

    def _wire(self, strat, market):
        strat._chain = "ethereum"
        strat.create_market_snapshot = lambda: market

    def test_restart_with_held_pt_emits_exit_and_is_covered(self):
        """The real strand: after a restart the in-memory flag is False, but a PT
        balance is still held on-chain. Chain-derived teardown must report the
        position AND emit a covering exit (pre-fix produced neither → strand)."""
        from almanak.framework.teardown import TeardownMode
        from almanak.framework.teardown.completeness import check_intent_coverage

        module, strat = _make_strategy()
        market = _FakeMarket({strat.pt_token: Decimal("0.01"), strat.base_token: Decimal("0")})
        self._wire(strat, market)
        # Restart: in-memory position flag wiped.
        strat._has_entered_position = False
        strat._has_exited_position = False

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1, "chain-held PT must be reported open"
        pos = summary.positions[0]
        assert pos.protocol == "pendle"

        intents = strat.generate_teardown_intents(TeardownMode.HARD, market=market)
        assert len(intents) == 1, "a held PT must emit a teardown exit from chain-truth"

        report = check_intent_coverage(summary, intents)
        assert report.complete, f"teardown exit must cover the PT; uncovered={report.uncovered}"

    def test_no_position_when_pt_balance_zero(self):
        """Measured-zero PT (already exited) → no position, no intent, clean exit."""
        from almanak.framework.teardown import TeardownMode

        module, strat = _make_strategy()
        market = _FakeMarket({strat.pt_token: Decimal("0"), strat.base_token: Decimal("1")})
        self._wire(strat, market)
        strat._has_entered_position = True  # stale in-memory flag; chain says flat
        strat._has_exited_position = False

        assert strat.get_open_positions().positions == []
        assert strat.generate_teardown_intents(TeardownMode.HARD, market=market) == []

    def test_unmeasured_pt_balance_does_not_silently_strand(self):
        """Empty != Zero: an UNMEASURED PT read must NOT be treated as 'no
        position' (silent strand) — get_open_positions raises so the framework
        no-intents gate fails loud, and generate_teardown_intents never fabricates
        an exit on an unmeasured balance."""
        from almanak.framework.teardown import TeardownMode

        module, strat = _make_strategy()
        market = _FakeMarket({}, raise_for={strat.pt_token})
        self._wire(strat, market)
        strat._has_entered_position = False
        strat._has_exited_position = False

        with pytest.raises(ValueError):
            strat.get_open_positions()
        with pytest.raises(ValueError):
            strat.generate_teardown_intents(TeardownMode.HARD, market=market)

    def test_declares_chain_derived_posture(self):
        """The posture flag must be set so the runner boot WARNING is silenced and
        the framework trusts the chain-derived open set (VIB-5464 / TD-06)."""
        module, strat = _make_strategy()
        assert strat.teardown_state_derived_from_chain is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
