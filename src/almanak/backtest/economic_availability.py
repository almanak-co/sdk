"""Retain missing historical risk intervals by fund and Spoke across replacement."""

from almanak.backtest.dataset import InputUnavailable
from almanak.work import ChainAvailability, ChainFault


def scope(target):
    return target.fund, target.reserve.spoke


def unavailable(state):
    return tuple(gap for event in state.events for gap in event.transition.result.unavailable)


def blocked(state, target):
    return next(
        (group.input for group in unavailable(state) if scope(group.targets[0]) == scope(target)),
        None,
    )


def require_history(state, target):
    if (gap := blocked(state, target)) is not None:
        raise InputUnavailable(gap.key, gap.time, gap.reason)


def availability(state, observations):
    observed = {o.target for o in observations}
    chains = dict.fromkeys(p.payload.target.fund.chain for p in state.portfolio.positions)
    result = []
    for chain in chains:
        missing = tuple(
            p.payload.target
            for p in state.portfolio.positions
            if p.payload.target.fund.chain == chain and p.payload.target not in observed
        )
        result.append(
            ChainAvailability(
                chain=chain,
                status="unavailable" if missing else "ready",
                fault=ChainFault(
                    kind="prepared_input_unavailable",
                    message="Missing prepared risk history for "
                    + ", ".join(t.account + "/" + t.allocation for t in missing),
                )
                if missing
                else None,
            )
        )
    return tuple(result)
