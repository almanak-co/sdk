"""Single author configs preserve explicit Morpho fork targets and funding obligations."""

import pytest

from almanak.connectors.morpho_blue.fork import ForkConfig
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support


@pytest.fixture(params=["ethereum", "base"])
def fork_config(request, candidate_morpho_support):
    path = harness_directory(f"morpho_blue_{request.param}_lending") / "config.yaml"
    config = read_author(path).fork_config()
    assert isinstance(config, ForkConfig)
    return config


def test_fork_funding_preserves_account_asset_and_chain_scope(fork_config):
    from tests.fork.test_aave_v3_base_eoa import eoa_configuration

    assert ForkConfig.model_validate_json(fork_config.model_dump_json()) == fork_config
    eoa = eoa_configuration(fork_config)
    assert len(eoa.forks) == 1 and len(eoa.funds) == 2 and not eoa.operators
    assert all(f.native_balance.asset.chain == eoa.forks[0].chain.chain_id for f in eoa.funds)


def test_fork_refuses_missing_funding_and_duplicate_allocations(fork_config):
    borrower = fork_config.funds[1]
    collateral = next(a for a in fork_config.allocations if not a.reserve.is_loan)
    assert collateral.capital > 0
    unfunded = tuple(a for a in borrower.assets if a.balance.asset != collateral.reserve.asset)
    changed = borrower.model_copy(update={"assets": unfunded})
    with pytest.raises(ValueError, match="must cover the allocation capital"):
        ForkConfig.model_validate_json(
            fork_config.model_copy(
                update={"funds": (fork_config.funds[0], changed)}
            ).model_dump_json()
        )
    with pytest.raises(ValueError, match="allocation names"):
        ForkConfig.model_validate_json(
            fork_config.model_copy(
                update={"allocations": (*fork_config.allocations, fork_config.allocations[0])}
            ).model_dump_json()
        )
