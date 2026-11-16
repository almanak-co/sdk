"""LP share changes remain position effects; only coins enter shared fund inventory."""


def effects(outcome):
    outcome.validate_target(outcome.operation.target)
    return outcome.asset_changes if outcome.success else ()
