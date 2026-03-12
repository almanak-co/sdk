"""Guard test: every Chain enum value must be present in framework CHAIN_IDS dicts.

Iter 29c Ticket D: Ensures that when new chains are added to the Chain enum
and core/constants.py, they are also propagated to execution/config.py and
fork_manager.py CHAIN_IDS dicts. Without this guard, chains can be "partially
supported" (gateway recognizes them but framework execution rejects them),
causing cryptic ConfigurationError at runtime.

Inspired by VIB-494 (IntentType -> IntentState coverage guard).
"""

import pytest

from almanak.core.constants import CHAIN_IDS as CANONICAL_CHAIN_IDS
from almanak.core.enums import Chain
from almanak.framework.anvil.fork_manager import CHAIN_IDS as FORK_MANAGER_CHAIN_IDS
from almanak.framework.execution.config import CHAIN_IDS as EXECUTION_CHAIN_IDS


# After VIB-708, all config dicts use canonical names matching Chain enum.
# No aliases needed - "bsc" is used everywhere (not "bnb").
CHAIN_ENUM_TO_CONFIG_NAME: dict[str, str] = {}
CONFIG_NAME_TO_ENUM_NAME: dict[str, str] = {}

# Chains excluded from fork_manager checks (no Anvil fork support yet).
# Each exclusion MUST have a documented reason. Remove entries as support is added.
FORK_MANAGER_EXCLUSIONS: set[Chain] = {
    Chain.SOLANA,  # Non-EVM chain, Anvil cannot fork Solana
}

# Chains excluded from execution config checks.
# Each exclusion MUST have a documented reason.
EXECUTION_CONFIG_EXCLUSIONS: set[Chain] = set()


@pytest.mark.parametrize(
    "chain",
    list(Chain),
    ids=lambda c: c.name,
)
def test_chain_in_canonical_chain_ids(chain):
    """Every Chain enum value must have an entry in core/constants.py CHAIN_IDS."""
    assert chain in CANONICAL_CHAIN_IDS, (
        f"Chain.{chain.name} is in the Chain enum but missing from "
        f"core/constants.py CHAIN_IDS. Add it to the canonical mapping."
    )


@pytest.mark.parametrize(
    "chain",
    [c for c in Chain if c not in EXECUTION_CONFIG_EXCLUSIONS],
    ids=lambda c: c.name,
)
def test_chain_in_execution_config(chain):
    """Every Chain enum value must have an entry in execution/config.py CHAIN_IDS.

    Without this, strategy execution fails with ConfigurationError for the chain.
    """
    chain_name = chain.name.lower()
    config_name = CHAIN_ENUM_TO_CONFIG_NAME.get(chain_name, chain_name)

    assert config_name in EXECUTION_CHAIN_IDS, (
        f"Chain.{chain.name} (config name '{config_name}') is in the Chain enum "
        f"but missing from execution/config.py CHAIN_IDS. Add "
        f'"{config_name}": {CANONICAL_CHAIN_IDS[chain]} to the dict.'
    )

    # Verify chain_id matches canonical source
    expected_id = CANONICAL_CHAIN_IDS[chain]
    actual_id = EXECUTION_CHAIN_IDS[config_name]
    assert actual_id == expected_id, (
        f"Chain ID mismatch for {chain.name}: execution/config.py has "
        f"{actual_id} but core/constants.py has {expected_id}."
    )


@pytest.mark.parametrize(
    "chain",
    [c for c in Chain if c not in FORK_MANAGER_EXCLUSIONS],
    ids=lambda c: c.name,
)
def test_chain_in_fork_manager(chain):
    """Every Chain enum value must have an entry in fork_manager.py CHAIN_IDS.

    Without this, Anvil fork creation fails for the chain.
    """
    chain_name = chain.name.lower()
    config_name = CHAIN_ENUM_TO_CONFIG_NAME.get(chain_name, chain_name)

    assert config_name in FORK_MANAGER_CHAIN_IDS, (
        f"Chain.{chain.name} (config name '{config_name}') is in the Chain enum "
        f"but missing from fork_manager.py CHAIN_IDS. Add "
        f'"{config_name}": {CANONICAL_CHAIN_IDS[chain]} to the dict.'
    )

    # Verify chain_id matches canonical source
    expected_id = CANONICAL_CHAIN_IDS[chain]
    actual_id = FORK_MANAGER_CHAIN_IDS[config_name]
    assert actual_id == expected_id, (
        f"Chain ID mismatch for {chain.name}: fork_manager.py has "
        f"{actual_id} but core/constants.py has {expected_id}."
    )


def test_execution_config_no_extra_chains():
    """execution/config.py CHAIN_IDS should not have chains missing from Chain enum.

    Extra chains in config that aren't in the enum indicate stale entries
    or chains that were removed from the enum but not cleaned up.
    """
    canonical_names = {c.name.lower() for c in Chain}

    # Special case: linea is in config but not in Chain enum (no strategy coverage yet)
    known_extra = {"linea"}

    for config_name in EXECUTION_CHAIN_IDS:
        canonical = CONFIG_NAME_TO_ENUM_NAME.get(config_name, config_name)
        assert canonical in canonical_names or config_name in known_extra, (
            f"execution/config.py has '{config_name}' but it's not in the Chain enum. "
            f"Either add Chain.{config_name.upper()} to the enum or remove the stale entry."
        )
