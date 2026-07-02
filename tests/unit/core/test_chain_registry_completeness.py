"""Guard test: every supported chain must appear in ALL required registries.

This test catches the recurring "fragmented chain support" bug where a new chain
is added to the chain inventory but not to every downstream dict/map.  Without
this guard, partial chain support causes silent runtime failures:

  * Iter 29c (Sonic): missing from adapter config dicts
  * Iter 51  (Mantle): missing from NATIVE_TOKEN_SYMBOLS
  * Iter 166 (Linea): missing from 6 registries

VIB-2723: turns a recurring P1 runtime bug into a P0 CI failure.

``ALL_CHAINS`` is a FROZEN literal inventory (the Chain enum was removed,
VIB-4851). Do NOT derive it from ``ChainRegistry`` — that would be tautological:
a descriptor module silently dropped from discovery would shrink both sides and
pass unnoticed. Editing this list is the deliberate review act when adding or
removing a chain (mirror of tests/unit/core/test_chain_identity_freeze.py).
"""

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.constants import CHAIN_IDS as CANONICAL_CHAIN_IDS
from almanak.core.constants import _CHAIN_ALIASES
from almanak.core.enums import ChainFamily
from almanak.framework.anvil.fork_manager import CHAIN_IDS as FORK_MANAGER_CHAIN_IDS
from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE
from almanak.framework.execution.config import CHAIN_IDS as EXECUTION_CHAIN_IDS
from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS
from almanak.gateway.data.price.dexscreener import CHAIN_TO_DEXSCREENER_PLATFORM
from almanak.gateway.managed import ManagedGateway

# ---------------------------------------------------------------------------
# Frozen inventory + derived sets
# ---------------------------------------------------------------------------

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
    "solana",
    "sonic",
    "xlayer",
    "zerog",
]
EVM_CHAINS = [c for c in ALL_CHAINS if ChainRegistry.get(c).family == ChainFamily.EVM]
NON_EVM_CHAINS = {c for c in ALL_CHAINS if ChainRegistry.get(c).family != ChainFamily.EVM}

# Chains excluded from specific registries with documented reasons.
# Remove entries as support is added.
FORK_MANAGER_EXCLUSIONS: set[str] = {
    "solana",  # Non-EVM chain, Anvil cannot fork Solana
}

# Chains that appear in config dicts but not yet in the frozen inventory.
# These are forward declarations for chains with partial infra support.
# Remove entries as inventory entries are added.
REVERSE_CHECK_KNOWN_EXTRA: set[str] = set()

# ---------------------------------------------------------------------------
# 1. ChainDescriptor.family — every chain must resolve to a descriptor family
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_has_descriptor_family(chain: str):
    """Every supported chain must resolve to a descriptor that declares a family.

    ``family`` is a required ``ChainDescriptor`` field (VIB-4801) and is the
    single source of truth for chain->family since VIB-4851 removed the parallel
    ``CHAIN_FAMILY_MAP`` literal from ``core/enums.py``.
    """
    assert isinstance(ChainRegistry.get(chain).family, ChainFamily), (
        f"{chain!r} is in the frozen chain inventory but its descriptor declares "
        f"no ChainFamily. Add family= to its file under almanak/core/chains/."
    )


# ---------------------------------------------------------------------------
# 2. CHAIN_IDS (canonical) — every chain must have a numeric chain ID
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_in_canonical_chain_ids(chain: str):
    """Every supported chain must have an entry in core/constants.py CHAIN_IDS."""
    assert chain in CANONICAL_CHAIN_IDS, (
        f"{chain!r} is in the frozen chain inventory but missing from "
        f"core/constants.py CHAIN_IDS. Add it to the canonical mapping."
    )


# ---------------------------------------------------------------------------
# 3. _CHAIN_ALIASES — every chain must have at least its canonical name as alias
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_in_chain_aliases(chain: str):
    """Every supported chain must be reachable via _CHAIN_ALIASES.

    At minimum, the canonical lowercase name (e.g. 'bsc') must map back to
    itself.  Extra aliases (e.g. 'bnb' -> 'bsc') are optional.
    """
    assert chain in _CHAIN_ALIASES, (
        f"{chain!r} has no alias entry in core/constants.py "
        f"_CHAIN_ALIASES. Add at least: '{chain}': '{chain}'"
    )
    assert _CHAIN_ALIASES[chain] == chain, (
        f"_CHAIN_ALIASES['{chain}'] maps to {_CHAIN_ALIASES[chain]!r} instead of {chain!r}."
    )


# ---------------------------------------------------------------------------
# 4. CHAIN_IDS in execution/config.py — every chain must be executable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_in_execution_config(chain: str):
    """Every supported chain must have an entry in execution/config.py CHAIN_IDS."""
    assert chain in EXECUTION_CHAIN_IDS, (
        f"{chain!r} is in the frozen chain inventory but missing from "
        f'execution/config.py CHAIN_IDS. Add "{chain}": '
        f"{CANONICAL_CHAIN_IDS.get(chain, '???')}."
    )
    # Verify chain_id matches canonical source
    if chain in CANONICAL_CHAIN_IDS:
        assert EXECUTION_CHAIN_IDS[chain] == CANONICAL_CHAIN_IDS[chain], (
            f"Chain ID mismatch for {chain}: execution/config.py has "
            f"{EXECUTION_CHAIN_IDS[chain]} but core/constants.py has "
            f"{CANONICAL_CHAIN_IDS[chain]}."
        )


# ---------------------------------------------------------------------------
# 5. CHAIN_IDS in fork_manager.py — every EVM chain must be forkable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain",
    [c for c in ALL_CHAINS if c not in FORK_MANAGER_EXCLUSIONS],
)
def test_chain_in_fork_manager(chain: str):
    """Every non-excluded chain must have an entry in fork_manager.py CHAIN_IDS."""
    assert chain in FORK_MANAGER_CHAIN_IDS, (
        f"{chain!r} is in the frozen chain inventory but missing from "
        f'fork_manager.py CHAIN_IDS. Add "{chain}": '
        f"{CANONICAL_CHAIN_IDS.get(chain, '???')}."
    )
    if chain in CANONICAL_CHAIN_IDS:
        assert FORK_MANAGER_CHAIN_IDS[chain] == CANONICAL_CHAIN_IDS[chain], (
            f"Chain ID mismatch for {chain}: fork_manager.py has "
            f"{FORK_MANAGER_CHAIN_IDS[chain]} but core/constants.py has "
            f"{CANONICAL_CHAIN_IDS[chain]}."
        )


# ---------------------------------------------------------------------------
# 6. WRAPPED_NATIVE — every chain must have a wrapped native token address
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_in_wrapped_native(chain: str):
    """Every supported chain must have a wrapped native token address in defaults.py."""
    assert chain in WRAPPED_NATIVE, (
        f"{chain!r} is in the frozen chain inventory but missing from "
        f"WRAPPED_NATIVE in framework/data/tokens/defaults.py. "
        f"Add the wrapped native token address for {chain}."
    )


# ---------------------------------------------------------------------------
# 7. CHAIN_TO_DEXSCREENER_PLATFORM — every chain should have a DexScreener slug
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS)
def test_chain_in_dexscreener(chain: str):
    """Every supported chain must have a DexScreener platform slug.

    Without this, price lookups via DexScreener silently fail for the chain.
    """
    assert chain in CHAIN_TO_DEXSCREENER_PLATFORM, (
        f"{chain!r} is in the frozen chain inventory but missing from "
        f"CHAIN_TO_DEXSCREENER_PLATFORM in gateway/data/price/dexscreener.py. "
        f'Add "{chain}": "<dexscreener-slug>".'
    )


# ---------------------------------------------------------------------------
# 8. NATIVE_TOKEN_SYMBOLS — every EVM chain should have a native symbol
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", EVM_CHAINS)
def test_chain_in_native_token_symbols(chain: str):
    """Every EVM chain must have a native token symbol in web3_provider.py.

    Without this, native balance queries return an incorrect symbol.
    """
    assert chain in NATIVE_TOKEN_SYMBOLS, (
        f"{chain!r} is in the frozen chain inventory (EVM) but missing from "
        f"NATIVE_TOKEN_SYMBOLS in gateway/data/balance/web3_provider.py. "
        f'Add "{chain}": "<NATIVE_SYMBOL>".'
    )


# ---------------------------------------------------------------------------
# 9. CHAIN_NATIVE_SYMBOL — every EVM chain should have a managed gateway symbol
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", EVM_CHAINS)
def test_chain_in_managed_gateway_native_symbol(chain: str):
    """Every EVM chain must have a native symbol in ManagedGateway.CHAIN_NATIVE_SYMBOL.

    Without this, Anvil wallet funding skips native gas for this chain.
    """
    assert chain in ManagedGateway.CHAIN_NATIVE_SYMBOL, (
        f"{chain!r} is in the frozen chain inventory (EVM) but missing from "
        f"ManagedGateway.CHAIN_NATIVE_SYMBOL in gateway/managed.py. "
        f'Add "{chain}": "<NATIVE_SYMBOL>".'
    )
    # Verify symbol consistency with web3_provider registry
    expected_symbol = NATIVE_TOKEN_SYMBOLS.get(chain)
    if expected_symbol:
        actual_symbol = ManagedGateway.CHAIN_NATIVE_SYMBOL[chain]
        assert actual_symbol == expected_symbol, (
            f"Native symbol mismatch for {chain}: managed.py has {actual_symbol} "
            f"but web3_provider.py has {expected_symbol}."
        )


# ---------------------------------------------------------------------------
# Reverse checks — no stale entries in registries
# ---------------------------------------------------------------------------


def test_no_extra_chains_in_execution_config():
    """execution/config.py CHAIN_IDS should not have chains missing from the inventory."""
    canonical_names = set(ALL_CHAINS)
    for config_name in EXECUTION_CHAIN_IDS:
        assert config_name in canonical_names or config_name in REVERSE_CHECK_KNOWN_EXTRA, (
            f"execution/config.py CHAIN_IDS has '{config_name}' but it's not in "
            f"the frozen chain inventory. Either add '{config_name}' to "
            f"ALL_CHAINS or remove the stale entry."
        )


def test_no_extra_chains_in_fork_manager():
    """fork_manager.py CHAIN_IDS should not have chains missing from the inventory."""
    canonical_names = set(ALL_CHAINS)
    for config_name in FORK_MANAGER_CHAIN_IDS:
        assert config_name in canonical_names or config_name in REVERSE_CHECK_KNOWN_EXTRA, (
            f"fork_manager.py CHAIN_IDS has '{config_name}' but it's not in "
            f"the frozen chain inventory. Either add '{config_name}' to "
            f"ALL_CHAINS or remove the stale entry."
        )


def test_no_extra_chains_in_wrapped_native():
    """WRAPPED_NATIVE should not have chains missing from the inventory."""
    canonical_names = set(ALL_CHAINS)
    for chain_name in WRAPPED_NATIVE:
        assert chain_name in canonical_names or chain_name in REVERSE_CHECK_KNOWN_EXTRA, (
            f"WRAPPED_NATIVE has '{chain_name}' but it's not in the frozen chain "
            f"inventory. Either add '{chain_name}' to ALL_CHAINS or remove the "
            f"stale entry."
        )
