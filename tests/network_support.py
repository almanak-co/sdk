"""Explicit production configuration fixtures, without credentials or submission authority."""

from pathlib import Path

from almanak.connectors.composition import connector_for
from almanak.gateway.provisioning import production_configuration
from almanak.models import Account, AccountBinding, Eoa
from almanak.runtime import strategy_capabilities
from almanak.strategy import StrategyDefinition

ROOT = Path(__file__).parents[1]


def production_config(name="aave_v4_multichain_borrowing"):
    from scripts.legacy_configuration.author import read_author
    from tests.legacy_references import harness_directory
    from tests.reference_parameters import authored_values

    path = harness_directory(name)

    author = read_author(path / "config.yaml")
    accounts = tuple(
        AccountBinding(
            name=name,
            profile=Eoa(
                kind="eoa",
                fund=Account(
                    chain=account.chain,
                    address="0x" + "12" * 20,
                ),
            ),
        )
        for name, account in author.author_accounts.items()
    )
    config = production_configuration(author, accounts)
    # The legacy harness input predates strategy.parameters; the knobs are the reference's.
    return config.model_copy(update={"parameters": authored_values(name)})


def initial_state(name="aave_v4_multichain_borrowing"):
    config = production_config(name)
    strategy = StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py")
    capabilities = strategy_capabilities(strategy, config)
    return connector_for(config).lifecycle.initial(config, strategy, capabilities, None)
