"""Every declared binding rejects unsupported authority, actions and models."""

import pytest

from almanak.connectors.composition import installed_connectors
from almanak.connectors.support import EconomicAction, RunMode

BINDINGS = [
    (name, support)
    for name, connector in installed_connectors()
    for support in connector.chain_support
]


@pytest.mark.parametrize("name,support", BINDINGS, ids=[f"{n}-{s.chain}" for n, s in BINDINGS])
def test_support_refuses_unadvertised_combinations(name, support):
    if not support.modes:
        # A chain named before any lifecycle proof exists admits no run in any mode.
        for mode in RunMode:
            with pytest.raises(ValueError, match="unsupported"):
                support.require(mode, model="unknown_model")
        return
    # A backtest-only binding (HyperCore) is checked in its own mode and refuses forks.
    mode = RunMode.FORK if RunMode.FORK in support.modes else RunMode.BACKTEST
    model = support.historical[0].model if mode == RunMode.BACKTEST else None
    with pytest.raises(ValueError, match="account profile"):
        support.require(mode, account_profile="owner_key", model=model)
    with pytest.raises(ValueError, match="unsupported"):
        support.require(mode, action="arbitrary_call", model=model)
    if RunMode.FORK not in support.modes:
        with pytest.raises(ValueError, match="fork is unsupported"):
            support.require(RunMode.FORK)
    model_error = (
        "historical model" if RunMode.BACKTEST in support.modes else "backtest is unsupported"
    )
    with pytest.raises(ValueError, match=model_error):
        support.require(RunMode.BACKTEST, model="unknown_model")
    for historical in support.historical:
        for action in set(EconomicAction) - set(historical.actions):
            with pytest.raises(ValueError, match="unsupported"):
                support.require(RunMode.BACKTEST, action, model=historical.model)
