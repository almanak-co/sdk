"""Bind operator-owned accounts to the parameters in the single author configuration."""

from almanak.connectors.composition import connector_for
from almanak.models import StrategyAccounts


def production_configuration(author, bindings):
    if author.production is None:
        raise ValueError(
            "author configuration requires explicit production providers and gas policies"
        )
    fields = {
        "accounts": StrategyAccounts(bindings=tuple(bindings)),
        "allocations": author.allocations,
        "production": author.production,
        "decision_trigger": author.decision_trigger,
        "asset_metadata": author.asset_metadata,
        "parameters": author.parameters,
    }
    if hasattr(author, "debts"):
        fields["debts"] = author.debts
    if hasattr(author, "inventory"):
        fields["inventory"] = author.inventory
    return connector_for(author).codec.config(**fields)
