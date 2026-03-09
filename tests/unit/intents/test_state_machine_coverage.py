"""Guard test: every IntentType must have state machine wiring.

VIB-494: Ensures that when new IntentType enum values are added, the
IntentStateMachine has corresponding PREPARING, VALIDATING, and SADFLOW
states. Without this guard, missing states cause cryptic runtime errors
("Unknown state: IntentState.IDLE") when users try to execute the intent.
"""

import pytest

from almanak.framework.intents.state_machine import (
    IntentState,
    get_preparing_state,
    get_sadflow_state,
    get_validating_state,
)
from almanak.framework.intents.vocabulary import IntentType

# These IntentTypes intentionally bypass the standard state machine flow:
# - BRIDGE: handled by cross-chain orchestrator, not single-intent state machine
# - ENSURE_BALANCE: utility intent resolved before compilation
# - FLASH_LOAN: wrapper intent that delegates to inner intent's state machine
# - HOLD: no-op intent, no transaction to execute
EXCLUDED_TYPES = {
    IntentType.BRIDGE,
    IntentType.ENSURE_BALANCE,
    IntentType.FLASH_LOAN,
    IntentType.HOLD,
}


@pytest.mark.parametrize(
    "intent_type",
    [t for t in IntentType if t not in EXCLUDED_TYPES],
    ids=lambda t: t.name,
)
def test_intent_type_has_preparing_state(intent_type):
    """Every non-excluded IntentType must have a PREPARING state."""
    state = get_preparing_state(intent_type)
    assert state != IntentState.IDLE, (
        f"{intent_type.name} has no PREPARING state in IntentStateMachine. "
        f"Add PREPARING_{intent_type.name} to IntentState enum and get_preparing_state() map."
    )


@pytest.mark.parametrize(
    "intent_type",
    [t for t in IntentType if t not in EXCLUDED_TYPES],
    ids=lambda t: t.name,
)
def test_intent_type_has_validating_state(intent_type):
    """Every non-excluded IntentType must have a VALIDATING state."""
    state = get_validating_state(intent_type)
    assert state != IntentState.IDLE, (
        f"{intent_type.name} has no VALIDATING state in IntentStateMachine. "
        f"Add VALIDATING_{intent_type.name} to IntentState enum and get_validating_state() map."
    )


@pytest.mark.parametrize(
    "intent_type",
    [t for t in IntentType if t not in EXCLUDED_TYPES],
    ids=lambda t: t.name,
)
def test_intent_type_has_sadflow_state(intent_type):
    """Every non-excluded IntentType must have a SADFLOW state."""
    state = get_sadflow_state(intent_type)
    assert state != IntentState.IDLE, (
        f"{intent_type.name} has no SADFLOW state in IntentStateMachine. "
        f"Add SADFLOW_{intent_type.name} to IntentState enum and get_sadflow_state() map."
    )
