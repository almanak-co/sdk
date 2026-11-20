"""Deployments whose decisions come from operator requests, not from a strategy.

An operator deployment retains no strategy code. Its authority is the capability
set its connector declares from the provisioned configuration, and the gateway
admits its decisions only under an operator trigger. The marker is recorded at
creation and never changes, so a strategy deployment cannot start taking operator
requests and an operator deployment cannot start running scheduled decisions.
"""


def is_operator(state) -> bool:
    return getattr(state, "operator", False)


def supports_operator(checkpoint_type) -> bool:
    return "operator" in getattr(checkpoint_type, "model_fields", {})


def adopts(before, after) -> bool:
    """Whether this update is an operator deployment's initialization.

    It is the one update that may set ownership without execution evidence: the
    deployment adopts the position its verified baseline observations state.
    """
    return is_operator(before) and not before.initialized and after.initialized


def refuse_adoption(positions) -> None:
    """Refuse positions that state what a deployment adopts at its start.

    Only a direct action's operator deployment may carry that statement. A strategy
    deployment and a backtest start from funded capital alone, so every path that
    builds one from a configuration calls this. A connector states it as a model or as
    a flag, absent as None or False.
    """
    if any(getattr(position, "adopt", None) not in (None, False) for position in positions):
        raise ValueError("only an operator deployment adopts inventory or a position")


def validate_origin(state) -> None:
    if state.operator and state.strategy is not None:
        raise ValueError("an operator deployment retains no strategy")


def validate_exclusive_origin(state) -> None:
    """A deployment is an operator deployment or retains its strategy, never neither.

    For a checkpoint whose strategy was always required: making it optional for operator
    deployments must not admit a strategy deployment without one.
    """
    validate_origin(state)
    if not state.operator and state.strategy is None:
        raise ValueError("a strategy deployment retains its strategy")
    if not state.operator:
        refuse_adoption(getattr(state.config, "positions", ()))


def request_executed(outcomes, event) -> bool:
    """Whether an outcome already records this operator request, whenever it was admitted."""
    triggers = (getattr(getattr(item, "decision", None), "trigger", None) for item in outcomes)
    return any(getattr(trigger, "request", None) == event.request for trigger in triggers)


def accounting_blocked(outcomes) -> bool:
    """Whether a recorded outcome carries an unexplained wallet movement.

    It blocks requests that increase exposure, as it does for a strategy. A reverted
    step does not: it is a known, delivered outcome, and the operator who reads it
    chooses the next request, where a strategy would repeat its decision unattended.
    """
    return any(getattr(item, "accounting_issues", ()) for item in outcomes)


def accounting_refusal(outcomes) -> str:
    """Why recorded accounting issues block new exposure, naming the first of them."""
    issues = [issue for item in outcomes for issue in getattr(item, "accounting_issues", ())]
    more = f" and {len(issues) - 1} more" if len(issues) > 1 else ""
    return f"recorded accounting issue ({issues[0]}{more}) blocks new exposure"
