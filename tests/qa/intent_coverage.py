"""Completed author intents remain distinct from nested protocol operations."""

from almanak.connectors.support import EconomicAction
from almanak.connectors.target_routing import target_connector
from almanak.external_effects import ExternalEffect
from tests.qa.variants import matches


def required_direct(actions):
    # Revocation is transaction-level cleanup, excluded from economic models.
    # Its native operation evidence is retained separately from core author intents.
    return frozenset(action for action in actions if action != EconomicAction.REVOKE)


def direct_results(results, config, chain, *, connector=None, variant=None):
    targets = tuple(config.target(position.name) for position in config.positions)
    observed = set()
    for result in results:
        if result.status != "completed":
            continue
        target = result.intent.target
        if target.fund.chain.chain_id != chain or target not in targets:
            continue
        if connector is not None and target_connector(target) != connector:
            continue
        if not matches(connector, target.model_dump(mode="json"), variant):
            continue
        observed.add(result.intent.kind)
    return observed


def native_direct(state, chain, *, connector=None, variant=None):
    results = (
        outcome.strategy_result()
        for outcome in state.outcomes[: state.delivered]
        if not isinstance(outcome, ExternalEffect)
    )
    return direct_results(results, state.config, chain, connector=connector, variant=variant)


def economic_direct(state, config, chain, *, connector=None, variant=None):
    results = (item.transition.result for item in state.completed[: state.delivered])
    return direct_results(results, config, chain, connector=connector, variant=variant)


def assert_native_direct(case, state):
    for chain in case.chains:
        for variant in case.variants or (None,):
            observed = native_direct(state, chain, connector=case.connector, variant=variant)
            missing = required_direct(case.actions) - observed
            assert not missing, (
                f"{chain}/{variant}: missing direct completed author intents {missing}"
            )


def assert_economic_direct(case, state, config):
    for chain in case.chains:
        for variant in case.variants or (None,):
            observed = economic_direct(
                state, config, chain, connector=case.connector, variant=variant
            )
            missing = required_direct(case.actions) - observed
            assert not missing, (
                f"{chain}/{variant}: missing direct completed author intents {missing}"
            )
