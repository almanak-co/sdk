"""Money-path regression tests for the hyperliquid_trailing_perp demo.

Locks in the exit logic that makes this seed distinctive — the ratcheting
trailing stop — plus the take-profit / hard-stop precedence and the venue
invariants a Hyperliquid perp close must honour:

  * closes are FULL reduce-only (``size_usd is None``) on protocol ``hyperliquid``;
  * the trailing stop engages ONLY after the high-water PnL clears
    ``trail_activation_pct`` (before that it must never fire);
  * take-profit and hard-stop are checked before the trailing stop;
  * config that makes the trailing stop unreachable is rejected at construction.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.teardown import TeardownMode as _TeardownMode

_SEED_DIR = (
    Path(__file__).resolve().parents[3]
    / "almanak"
    / "demo_strategies"
    / "hyperliquid_trailing_perp"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("hl_trailing_seed", _SEED_DIR / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _SEED_CONFIG():
    """The demo's on-disk config.json as a fresh dict."""
    return json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))


def _make_from_cfg(module, cfg):
    """Construct the strategy from an explicit config dict (no seed merge)."""
    cls = module.HyperliquidTrailingPerp
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(strat)
    return strat


def _make(module, **overrides):
    """Construct the strategy with the demo config, applying any overrides."""
    cls = module.HyperliquidTrailingPerp
    cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
    cfg.update(overrides)
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(strat)
    return strat


@pytest.fixture
def module():
    return _load_module()


def _market(price: str):
    m = MagicMock()
    m.price.return_value = Decimal(price)
    return m


def _in_position(strat, module, *, entry: str, side=None, high_water="0"):
    strat._position_side = side or module.LONG
    strat._entry_price = Decimal(entry)
    strat._high_water_pnl = Decimal(high_water)
    # A live, managed position is one whose fill has been CONFIRMED (VIB-5597).
    strat._fill_confirmed = True


class TestExitPrecedence:
    """Defaults: TP=2%, stop=3%, trail activates at +1% and trails 1.5%."""

    def test_take_profit_fires_at_threshold(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        intent = strat._manage(_market("102"))  # +2.0% == TP
        assert intent.intent_type.value == "PERP_CLOSE"

    def test_hard_stop_fires_at_threshold(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        intent = strat._manage(_market("97"))  # -3.0% == stop
        assert intent.intent_type.value == "PERP_CLOSE"

    def test_holds_inside_the_band(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        intent = strat._manage(_market("100.5"))  # +0.5%: no exit
        assert intent.intent_type.value == "HOLD"


class TestTrailingStop:
    def test_trailing_stop_fires_after_activation_and_giveback(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        # Tick 1: +1.5% -> above the 1% activation, below the 2% TP. Sets the
        # high-water mark; nothing given back yet -> hold.
        assert strat._manage(_market("101.5")).intent_type.value == "HOLD"
        assert strat._high_water_pnl == Decimal("0.015")
        # Tick 2: back to entry -> gave back 1.5% from the peak == trail_pct -> close.
        intent = strat._manage(_market("100"))
        assert intent.intent_type.value == "PERP_CLOSE"

    def test_trailing_stop_dormant_before_activation(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        # Peak only +0.5% (below the 1% activation): the trailing stop must NOT
        # arm, even after price falls back and "gives back" the move.
        assert strat._manage(_market("100.5")).intent_type.value == "HOLD"
        assert strat._high_water_pnl == Decimal("0.005")
        intent = strat._manage(_market("100"))
        assert intent.intent_type.value == "HOLD"

    def test_high_water_ratchets_up_only(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        strat._manage(_market("101.5"))  # peak 1.5%
        strat._manage(_market("100.8"))  # 0.8% -- peak must not drop
        assert strat._high_water_pnl == Decimal("0.015")


class TestCloseShape:
    def test_close_is_full_reduce_only_on_hyperliquid(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        intent = strat._manage(_market("102"))
        assert intent.intent_type.value == "PERP_CLOSE"
        assert intent.size_usd is None  # full reduce-only close
        assert intent.protocol == "hyperliquid"
        assert intent.is_long is True

    def test_short_pnl_is_inverted(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100", side=module.SHORT)
        # Price falls 3% -> a short is +3% -> take-profit path (>= 2%).
        assert strat._manage(_market("97")).intent_type.value == "PERP_CLOSE"
        # Price rises 3% -> a short is -3% -> hard stop.
        _in_position(strat, module, entry="100", side=module.SHORT)
        assert strat._manage(_market("103")).intent_type.value == "PERP_CLOSE"


class TestConfigValidation:
    def test_rejects_unreachable_trailing_stop(self, module):
        with pytest.raises(ValueError, match="trail_activation_pct"):
            _make(module, trail_activation_pct="0.02", take_profit_pct="0.02")

    def test_rejects_sub_minimum_leverage(self, module):
        with pytest.raises(ValueError, match="leverage"):
            _make(module, leverage="0.5")

    def test_accept_venue_leverage_string_false_does_not_opt_in(self, module):
        # The classic bool-coercion trap: the STRING "false" must NOT opt in.
        # bool("false") is True; the strict parser must resolve it to False so a
        # non-1x leverage still fails closed at compile.
        strat = _make(module, accept_venue_leverage="false")
        assert strat.accept_venue_leverage is False

    def test_accept_venue_leverage_real_bool_true_opts_in(self, module):
        strat = _make(module, accept_venue_leverage=True)
        assert strat.accept_venue_leverage is True

    def test_accept_venue_leverage_string_true_opts_in(self, module):
        strat = _make(module, accept_venue_leverage="true")
        assert strat.accept_venue_leverage is True

    def test_accept_venue_leverage_default_is_fail_closed(self, module):
        # Omitted entirely → runtime default False (fail-closed).
        cfg = {k: v for k, v in _SEED_CONFIG().items() if k != "accept_venue_leverage"}
        strat = _make_from_cfg(module, cfg)
        assert strat.accept_venue_leverage is False

    def test_accept_venue_leverage_garbage_raises(self, module):
        with pytest.raises(ValueError, match="accept_venue_leverage"):
            _make(module, accept_venue_leverage="maybe")

    def test_accept_venue_leverage_int_0_1_contract(self, module):
        # Documented 0/1 int contract: 1 opts in, 0 does not.
        assert _make(module, accept_venue_leverage=1).accept_venue_leverage is True
        assert _make(module, accept_venue_leverage=0).accept_venue_leverage is False

    @pytest.mark.parametrize("bad_int", [2, -1])
    def test_accept_venue_leverage_out_of_contract_int_raises(self, module, bad_int):
        # A stray nonzero int must NOT silently opt in — fail fast like bad strings.
        with pytest.raises(ValueError, match="accept_venue_leverage"):
            _make(module, accept_venue_leverage=bad_int)


class TestLifecycleState:
    def test_open_then_close_toggles_flat(self, module):
        strat = _make(module)
        strat._position_side = None
        open_intent = SimpleNamespace(intent_type=SimpleNamespace(value="PERP_OPEN"), is_long=True)
        strat.on_intent_executed(open_intent, True, SimpleNamespace(entry_price="100"))
        assert strat._position_side == module.LONG
        assert strat._entry_price == Decimal("100")
        # VIB-5597: submission does NOT confirm the fill — position starts PENDING.
        assert strat._fill_confirmed is False

        close_intent = SimpleNamespace(intent_type=SimpleNamespace(value="PERP_CLOSE"), is_long=True)
        strat.on_intent_executed(close_intent, True, None)
        assert strat._position_side is None
        assert strat._entry_price is None

    def test_one_shot_latches_after_close(self, module):
        strat = _make(module, reenter_after_close=False)
        strat._position_side = module.LONG
        close_intent = SimpleNamespace(intent_type=SimpleNamespace(value="PERP_CLOSE"), is_long=True)
        strat.on_intent_executed(close_intent, True, None)
        # decide() must now HOLD rather than re-open.
        assert strat.decide(_market("100")).intent_type.value == "HOLD"

    def test_resolve_fill_price_degrades_garbage_to_none(self, module):
        cls = module.HyperliquidTrailingPerp
        # Decimal("not-a-number") raises decimal.InvalidOperation (an
        # ArithmeticError, not ValueError/TypeError) — must degrade to None,
        # never crash the strategy.
        assert cls._resolve_fill_price(SimpleNamespace(entry_price="not-a-number")) is None
        assert cls._resolve_fill_price(SimpleNamespace(entry_price="123.5")) == Decimal("123.5")

    def test_failed_intent_leaves_state_untouched(self, module):
        strat = _make(module)
        _in_position(strat, module, entry="100")
        open_intent = SimpleNamespace(intent_type=SimpleNamespace(value="PERP_OPEN"), is_long=True)
        strat.on_intent_executed(open_intent, False, None)
        assert strat._position_side == module.LONG
        assert strat._entry_price == Decimal("100")


class TestFillReconciliation:
    """VIB-5597: position state is driven by the OBSERVED fill, not submission.

    A CoreWriter submission returning status 1 only proves *submission*; the IOC
    order may partial-fill or be rejected off-EVM. The strategy must not manage /
    close a position that may not exist.
    """

    def _submit_open(self, strat, module):
        open_intent = SimpleNamespace(intent_type=SimpleNamespace(value="PERP_OPEN"), is_long=True)
        strat._position_side = None
        strat._fill_confirmed = False
        strat.on_intent_executed(open_intent, True, SimpleNamespace(entry_price="100"))

    def test_submission_does_not_commit_a_confirmed_position(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        # Side is latched (so teardown can cover a possibly-filled order) but the
        # fill is UNCONFIRMED — the position is PENDING, not live.
        assert strat._position_side == module.LONG
        assert strat._fill_confirmed is False

    def test_pending_position_holds_rather_than_manage_or_reopen(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        # decide() must HOLD while pending — never manage (maybe-nonexistent) nor
        # re-open (maybe-existing). Even a price that WOULD trip take-profit holds.
        assert strat.decide(_market("102")).intent_type.value == "HOLD"

    def test_confirmed_fill_promotes_to_a_live_managed_position(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        strat.reconcile_fill("PERP_OPEN", module.FillStatus.FILLED)
        assert strat._fill_confirmed is True
        # Now management is live: a +2% move trips take-profit → PERP_CLOSE.
        assert strat.decide(_market("102")).intent_type.value == "PERP_CLOSE"

    def test_rejected_submission_clears_the_phantom_position(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        strat.reconcile_fill("PERP_OPEN", module.FillStatus.REJECTED)
        # No phantom position — back to FLAT; decide() re-enters (opens).
        assert strat._position_side is None
        assert strat._fill_confirmed is False
        assert strat.decide(_market("100")).intent_type.value == "PERP_OPEN"

    def test_unmeasured_status_keeps_position_pending(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        strat.reconcile_fill("PERP_OPEN", module.FillStatus.UNMEASURED)
        # Empty ≠ Zero: an unmeasured read neither confirms nor rejects — stay PENDING.
        assert strat._position_side == module.LONG
        assert strat._fill_confirmed is False
        assert strat.decide(_market("102")).intent_type.value == "HOLD"

    def test_partial_fill_is_treated_as_a_confirmed_fill(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        strat.reconcile_fill("PERP_OPEN", module.FillStatus.PARTIALLY_FILLED)
        assert strat._fill_confirmed is True

    def test_reconcile_ignored_once_confirmed(self, module):
        strat = _make(module)
        self._submit_open(strat, module)
        strat.reconcile_fill("PERP_OPEN", module.FillStatus.FILLED)
        # A later spurious REJECT must not tear down an already-confirmed position.
        strat.reconcile_fill("PERP_OPEN", module.FillStatus.REJECTED)
        assert strat._position_side == module.LONG
        assert strat._fill_confirmed is True

    def test_pending_position_is_surfaced_for_teardown(self, module):
        strat = _make(module)
        strat._chain = "hyperevm"
        strat._deployment_id = "deployment:test"
        self._submit_open(strat, module)
        # Fail-closed: a pending order may have filled on HyperCore, so teardown
        # must still cover it.
        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].details["fill_confirmed"] is False
        intents = strat.generate_teardown_intents(_TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "PERP_CLOSE"
