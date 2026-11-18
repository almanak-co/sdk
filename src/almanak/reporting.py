"""Connector-owned reporting selected from the retained validated configuration."""

from almanak.connectors.composition import connector_for


def report(state):
    result = connector_for(state.config).projection.report(state)
    result["decision_trigger"] = state.config.decision_trigger.model_dump(mode="json")
    result["decision_schedule"] = state.schedule.model_dump(mode="json")
    if getattr(state, "repayment", ()):
        # Measured at the latest decision while debt is open; projections are labeled
        # model outputs.
        result["repayment"] = [row.model_dump(mode="json") for row in state.repayment]
    if state.work:
        from almanak.work import availability

        result["chains"] = [item.model_dump(mode="json") for item in availability(state)]
        result["work"] = [item.model_dump(mode="json") for item in state.work]
        if any(item.fault and item.fault.invalidated for item in state.work):
            result["evidence_status"] = "partially_invalidated"
    return result
