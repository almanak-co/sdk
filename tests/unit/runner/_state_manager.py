"""State-manager test doubles with the production async absence contract."""

from unittest.mock import MagicMock

from almanak.framework.state.state_manager import StateManager, StateNotFoundError


def absent_state_manager() -> MagicMock:
    """Return a state-manager mock whose async load reports genuine absence."""
    manager = MagicMock(spec=StateManager)
    manager.load_state.side_effect = StateNotFoundError("test deployment")
    return manager
