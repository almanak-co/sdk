"""Guard test: every supported chain must be present in framework CHAIN_IDS dicts.

Iter 29c Ticket D: Ensures that when new chains are added to the chain
inventory and core/constants.py, they are also propagated to execution/config.py
and fork_manager.py CHAIN_IDS dicts. Without this guard, chains can be
"partially supported" (gateway recognizes them but framework execution rejects
them), causing cryptic ConfigurationError at runtime.

Inspired by VIB-494 (IntentType -> IntentState coverage guard).

``ALL_CHAINS`` is a FROZEN literal inventory (the Chain enum was removed,
VIB-4851). Do NOT derive it from ``ChainRegistry`` — a descriptor module
silently dropped from discovery would shrink both sides and pass unnoticed
(mirror of tests/unit/core/test_chain_identity_freeze.py).
"""

import pytest

from almanak.core.constants import CHAIN_IDS as CANONICAL_CHAIN_IDS
from almanak.framework.anvil.fork_manager import CHAIN_IDS as FORK_MANAGER_CHAIN_IDS
from almanak.framework.execution.config import CHAIN_IDS as EXECUTION_CHAIN_IDS

# Frozen, human-reviewed inventory of supported chain names (sorted).
ALL_CHAINS: list[str] = [
    "arbitrum",
    "avalanche",
    "base",
    "berachain",
    "blast",
    "bsc",
    "ethereum",
    "hyperevm",
    "linea",
    "mantle",
    "monad",
    "optimism",
    "plasma",
    "polygon",
    "robinhood",
    "solana",
    "sonic",
    "xlayer",
    "zerog",
]

# After VIB-708, all config dicts use canonical names matching the inventory.
# No aliases needed - "bsc" is used everywhere (not "bnb").
CHAIN_ENUM_TO_CONFIG_NAME: dict[str, str] = {}
CONFIG_NAME_TO_ENUM_NAME: dict[str, str] = {}

# Chains excluded from fork_manager checks (no Anvil fork support yet).
# Each exclusion MUST have a documented reason. Remove entries as support is added.
FORK_MANAGER_EXCLUSIONS: set[str] = {
    "solana",  # Non-EVM chain, Anvil cannot fork Solana
}

# Chains excluded from execution config checks.
# Each exclusion MUST have a documented reason.
EXECUTION_CONFIG_EXCLUSIONS: set[str] = set()


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_in_canonical_chain_ids(chain: str):
    """Every supported chain must have an entry in core/constants.py CHAIN_IDS."""
    assert chain in CANONICAL_CHAIN_IDS, (
        f"{chain!r} is in the frozen chain inventory but missing from "
        f"core/constants.py CHAIN_IDS. Add it to the canonical mapping."
    )


@pytest.mark.parametrize(
    "chain",
    [c for c in ALL_CHAINS if c not in EXECUTION_CONFIG_EXCLUSIONS],
)
def test_chain_in_execution_config(chain: str):
    """Every supported chain must have an entry in execution/config.py CHAIN_IDS.

    Without this, strategy execution fails with ConfigurationError for the chain.
    """
    config_name = CHAIN_ENUM_TO_CONFIG_NAME.get(chain, chain)

    assert config_name in EXECUTION_CHAIN_IDS, (
        f"{chain!r} (config name '{config_name}') is in the frozen chain "
        f"inventory but missing from execution/config.py CHAIN_IDS. Add "
        f'"{config_name}": {CANONICAL_CHAIN_IDS[chain]} to the dict.'
    )

    # Verify chain_id matches canonical source
    expected_id = CANONICAL_CHAIN_IDS[chain]
    actual_id = EXECUTION_CHAIN_IDS[config_name]
    assert actual_id == expected_id, (
        f"Chain ID mismatch for {chain}: execution/config.py has "
        f"{actual_id} but core/constants.py has {expected_id}."
    )


@pytest.mark.parametrize(
    "chain",
    [c for c in ALL_CHAINS if c not in FORK_MANAGER_EXCLUSIONS],
)
def test_chain_in_fork_manager(chain: str):
    """Every supported chain must have an entry in fork_manager.py CHAIN_IDS.

    Without this, Anvil fork creation fails for the chain.
    """
    config_name = CHAIN_ENUM_TO_CONFIG_NAME.get(chain, chain)

    assert config_name in FORK_MANAGER_CHAIN_IDS, (
        f"{chain!r} (config name '{config_name}') is in the frozen chain "
        f"inventory but missing from fork_manager.py CHAIN_IDS. Add "
        f'"{config_name}": {CANONICAL_CHAIN_IDS[chain]} to the dict.'
    )

    # Verify chain_id matches canonical source
    expected_id = CANONICAL_CHAIN_IDS[chain]
    actual_id = FORK_MANAGER_CHAIN_IDS[config_name]
    assert actual_id == expected_id, (
        f"Chain ID mismatch for {chain}: fork_manager.py has "
        f"{actual_id} but core/constants.py has {expected_id}."
    )


def test_execution_config_no_extra_chains():
    """execution/config.py CHAIN_IDS should not have chains missing from the inventory.

    Extra chains in config that aren't in the inventory indicate stale entries
    or chains that were removed from the inventory but not cleaned up.
    """
    canonical_names = set(ALL_CHAINS)

    known_extra: set[str] = set()

    for config_name in EXECUTION_CHAIN_IDS:
        canonical = CONFIG_NAME_TO_ENUM_NAME.get(config_name, config_name)
        assert canonical in canonical_names or config_name in known_extra, (
            f"execution/config.py has '{config_name}' but it's not in the frozen "
            f"chain inventory. Either add '{config_name}' to ALL_CHAINS or remove "
            f"the stale entry."
        )
