"""Recover shared market accounting from action and event generations in the run journal."""

from almanak.connectors.morpho_blue.economic_market import committed, latest, reanchor, seed


def changes(state):
    actions = tuple(
        step.transition.result.market_change
        for step in state.completed
        if step.transition.result.market_change is not None
    )
    events = tuple(
        change for event in state.events for change in event.transition.result.market_changes
    )
    return (*actions, *events)


def retained(book, state, source):
    first = book.rates[source].rows[0]
    return latest(seed(first.payload, first.available_at), changes(state))


def require_anchor(book, ledger, time):
    record = book.rates[ledger.source].at(time)
    if record.available_at != ledger.anchor_time:
        raise ValueError("modeled Morpho accounting boundary has not been durably applied")


def anchor(book, ledger, time):
    timeline = book.rates[ledger.source]
    following = timeline.at(time)
    if following.available_at == ledger.anchor_time:
        return None
    previous = timeline.at(ledger.anchor_time)
    if (
        timeline.next(ledger.anchor_time) != following.available_at
        or following.available_at != time
    ):
        raise ValueError("modeled Morpho accounting boundary cannot be skipped")
    after = reanchor(ledger, previous.payload, following.payload, time)
    return committed(ledger, after)
