"""Tests for ``MultiStepStrategy.__init__`` validation and wiring.

Unlike ``test_multi_step_strategy.py`` (which bypasses the real ``__init__``
to unit-test the state machine mechanics), these tests construct subclasses
through the genuine ``MultiStepStrategy.__init__`` -> ``IntentStrategy.__init__``
chain, exercising every validation branch:

    1. STEPS must be a non-empty tuple.
    2. Duplicate step names are rejected with the offending names listed.
    3. A step name colliding with TERMINAL_STATE is rejected.
    4. A ``next`` reference to an undefined step is rejected.
    5. ``terminal_check`` must resolve to an existing, callable attribute.
    6. Happy path wires lookup structures and state machine defaults.
"""

from __future__ import annotations

import pytest

from almanak.framework.intents import Intent
from almanak.framework.strategies.multi_step_strategy import MultiStepStrategy, Step

_WALLET = "0x000000000000000000000000000000000000dEaD"


# ---------------------------------------------------------------------------
# Concrete base: stubs the abstract surface unrelated to __init__
# ---------------------------------------------------------------------------


class _ConcreteMultiStep(MultiStepStrategy):
    """Minimal concrete subclass so the real ``__init__`` chain can run."""

    STRATEGY_NAME = "msinit_test"
    STEPS = (Step(name="supply", next=None),)

    def intent_for_step(self, step_name, market):
        return Intent.hold(reason=f"test step {step_name}")

    def get_open_positions(self):
        from almanak.framework.teardown.models import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.deployment_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        return []


def _make(cls):
    """Instantiate a MultiStepStrategy subclass with minimal kwargs."""
    return cls(
        config={"deployment_id": "msinit-test"},
        chain="arbitrum",
        wallet_address=_WALLET,
    )


def _class_with(steps, terminal_state="complete", **extra_ns):
    """Create a concrete subclass with the given STEPS / TERMINAL_STATE."""
    ns = {"STEPS": steps, "TERMINAL_STATE": terminal_state, **extra_ns}
    return type("_ParamMultiStep", (_ConcreteMultiStep,), ns)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestInitHappyPath:
    def test_linear_steps_wire_lookup_and_state_defaults(self):
        """A valid step chain builds lookups and seeds the state machine."""
        steps = (
            Step(name="supply", next="borrow"),
            Step(name="borrow", next=None),
        )
        strategy = _make(_class_with(steps))

        assert strategy._step_map == {"supply": steps[0], "borrow": steps[1]}
        assert strategy._step_order == ["supply", "borrow"]
        assert strategy._ms_state == "idle"
        assert strategy._ms_previous_stable == "idle"
        assert strategy._ms_retry_count == 0
        assert strategy._ms_step_data == {}
        assert strategy.current_step_state == "idle"
        assert strategy.step_data is strategy._ms_step_data
        assert strategy.is_terminal is False

    def test_super_init_receives_construction_kwargs(self):
        """``super().__init__`` runs for real: chain/wallet wiring lands."""
        strategy = _make(_ConcreteMultiStep)
        assert strategy.chain == "arbitrum"
        assert strategy.wallet_address == _WALLET

    def test_valid_terminal_check_resolves_to_bound_method(self):
        """A terminal_check naming a real method passes validation."""
        steps = (
            Step(name="loop", next="loop", terminal_check="is_done"),
        )

        def is_done(self, market):
            return True

        strategy = _make(_class_with(steps, is_done=is_done))
        assert strategy._step_map["loop"].terminal_check == "is_done"

    def test_single_terminal_step_with_no_next(self):
        """next=None and terminal_check=None skip both reference checks."""
        strategy = _make(_class_with((Step(name="only"),)))
        assert strategy._step_order == ["only"]


# ---------------------------------------------------------------------------
# STEPS shape validation
# ---------------------------------------------------------------------------


class TestStepsValidation:
    def test_empty_steps_raises(self):
        with pytest.raises(ValueError, match="must define STEPS"):
            _make(_class_with(()))

    def test_empty_steps_error_names_the_subclass(self):
        with pytest.raises(ValueError, match="_ParamMultiStep"):
            _make(_class_with(()))

    def test_duplicate_step_names_raises_and_lists_dupes(self):
        steps = (
            Step(name="a", next="b"),
            Step(name="b", next="a"),
            Step(name="a"),
        )
        with pytest.raises(ValueError, match=r"Duplicate step names detected: \['a'\]"):
            _make(_class_with(steps))

    def test_step_name_colliding_with_terminal_state_raises(self):
        steps = (Step(name="finished"),)
        with pytest.raises(ValueError, match="collides with TERMINAL_STATE='finished'"):
            _make(_class_with(steps, terminal_state="finished"))

    def test_next_referencing_unknown_step_raises(self):
        steps = (Step(name="a", next="ghost"),)
        with pytest.raises(ValueError, match="references next='ghost'"):
            _make(_class_with(steps))

    def test_next_unknown_error_lists_valid_steps(self):
        steps = (
            Step(name="a", next="b"),
            Step(name="b", next="ghost"),
        )
        with pytest.raises(ValueError, match=r"Valid steps: \['a', 'b'\]"):
            _make(_class_with(steps))


# ---------------------------------------------------------------------------
# terminal_check validation
# ---------------------------------------------------------------------------


class TestTerminalCheckValidation:
    def test_missing_terminal_check_method_raises(self):
        steps = (Step(name="a", terminal_check="no_such_method"),)
        with pytest.raises(ValueError, match="no such method"):
            _make(_class_with(steps))

    def test_terminal_check_attribute_set_to_none_raises_as_missing(self):
        """An attribute that exists but is None hits the same 'no such method' branch."""
        steps = (Step(name="a", terminal_check="is_done"),)
        with pytest.raises(ValueError, match="no such method"):
            _make(_class_with(steps, is_done=None))

    def test_non_callable_terminal_check_raises(self):
        steps = (Step(name="a", terminal_check="is_done"),)
        with pytest.raises(ValueError, match="is not callable"):
            _make(_class_with(steps, is_done=True))
