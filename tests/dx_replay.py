"""Compare current author decisions against independently retained protocol effects."""

from almanak.dex_intents import ExitExchange


def memory_values(memory):
    return memory.model_dump(exclude={"version", "values", "schema_id"})


def assert_intent(state, intent, outcome):
    if isinstance(intent, ExitExchange):
        from almanak.connectors.uniswap_v4.planning import operation

        assert operation(state.config, state, intent, outcome.before) == outcome.operation
    else:
        assert intent == outcome.decision.intent


def current_memory(strategy, memory):
    from almanak.memory_codec import freeze, hydrate

    namespace = {}
    exec(compile(strategy.source, "strategy.py", "exec"), namespace)
    selected = (
        hydrate(namespace, memory)
        if memory.schema_id is not None
        else namespace["State"](**memory.values)
    )
    return freeze(namespace, selected)


def assert_economics(actual, captured):
    """The composite exit may convert residuals at an earlier eligible sample."""
    assert actual.memory == captured.memory
    assert actual.mode == captured.mode
    assert actual.evaluations == captured.evaluations
    assert len(actual.completed) == len(captured.completed)
    differences = {}
    for result, expected in zip(actual.completed, captured.completed, strict=True):
        observed, old = result.transition.result, expected.transition.result
        assert (observed.steps, observed.fees0, observed.fees1, observed.gas_paid) == (
            old.steps,
            old.fees0,
            old.fees1,
            old.gas_paid,
        )
        if observed.effective_at == old.effective_at:
            assert (observed.delta0, observed.delta1) == (old.delta0, old.delta1)
        else:
            assert result.pending.intent.kind == "exit_exchange"
            assert observed.effective_at < old.effective_at
            assert observed.fill is not None
        current_deltas = {d.key: d.amount for d in result.transition.effects.assets}
        old_deltas = {d.key: d.amount for d in expected.transition.effects.assets}
        for key in current_deltas.keys() | old_deltas.keys():
            differences[key] = (
                differences.get(key, 0) + current_deltas.get(key, 0) - old_deltas.get(key, 0)
            )
    for balance, old in zip(actual.portfolio.balances, captured.portfolio.balances, strict=True):
        assert balance.key == old.key
        assert balance.amount - old.amount == differences.get(balance.key, 0)
    for position, old in zip(actual.portfolio.positions, captured.portfolio.positions, strict=True):
        assert position.key == old.key
        assert position.payload.model_dump(exclude={"owned", "allowances"}) == (
            old.payload.model_dump(exclude={"owned", "allowances"})
        )
        assert position.payload.owned.position is old.payload.owned.position is None
        assert position.payload.owned.entry_complete == old.payload.owned.entry_complete
