"""Financial checkpoint updates shared by runtime recording and gateway verification."""


def protocol_ownership(state, outcome=None):
    if outcome is None:
        return {"owned": state.owned}
    from almanak.connectors.composition import connector_for

    return {"owned": connector_for(state.config).lifecycle.commit_owned(state.owned, outcome)}
