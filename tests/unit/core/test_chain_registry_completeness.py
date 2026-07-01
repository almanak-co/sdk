"""Guard test: every Chain enum member must appear in ALL required registries.

This test catches the recurring "fragmented chain support" bug where a new chain
is added to the Chain enum but not to every downstream dict/map.  Without this
guard, partial chain support causes silent runtime failures:

  * Iter 29c (Sonic): missing from adapter config dicts
  * Iter 51  (Mantle): missing from NATIVE_TOKEN_SYMBOLS
  * Iter 166 (Linea): missing from 6 registries

VIB-2723: turns a recurring P1 runtime bug into a P0 CI failure.
"""

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.constants import CHAIN_IDS as CANONICAL_CHAIN_IDS
from almanak.core.constants import _CHAIN_ALIASES
from almanak.core.enums import Chain, ChainFamily
from almanak.framework.anvil.fork_manager import CHAIN_IDS as FORK_MANAGER_CHAIN_IDS
from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE
from almanak.framework.execution.config import CHAIN_IDS as EXECUTION_CHAIN_IDS
from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS
from almanak.gateway.data.price.dexscreener import CHAIN_TO_DEXSCREENER_PLATFORM
from almanak.gateway.managed import ManagedGateway

# ---------------------------------------------------------------------------
# Derived sets
# ---------------------------------------------------------------------------

ALL_CHAINS = list(Chain)
EVM_CHAINS = [c for c in Chain if ChainRegistry.get(c).family == ChainFamily.EVM]
NON_EVM_CHAINS = {c for c in Chain if ChainRegistry.get(c).family != ChainFamily.EVM}

# Chains excluded from specific registries with documented reasons.
# Remove entries as support is added.
FORK_MANAGER_EXCLUSIONS: set[Chain] = {
    Chain.SOLANA,  # Non-EVM chain, Anvil cannot fork Solana
}

# Chains that appear in config dicts but not yet in the Chain enum.
# These are forward declarations for chains with partial infra support.
# Remove entries as Chain enum members are added.
REVERSE_CHECK_KNOWN_EXTRA: set[str] = {
    "linea",  # Config entries pre-staged; Chain.LINEA added in pending PR
}

# ---------------------------------------------------------------------------
# 1. ChainDescriptor.family — every Chain must resolve to a descriptor family
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS, ids=lambda c: c.name)
def test_chain_has_descriptor_family(chain: Chain):
    """Every Chain enum member must resolve to a descriptor that declares a family.

    ``family`` is a required ``ChainDescriptor`` field (VIB-4801) and is the
    single source of truth for chain->family since VIB-4851 removed the parallel
    ``CHAIN_FAMILY_MAP`` literal from ``core/enums.py``.
    """
    assert isinstance(ChainRegistry.get(chain).family, ChainFamily), (
        f"Chain.{chain.name} is in the Chain enum but its descriptor declares "
        f"no ChainFamily. Add family= to its file under almanak/core/chains/."
    )


# ---------------------------------------------------------------------------
# 2. CHAIN_IDS (canonical) — every Chain must have a numeric chain ID
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS, ids=lambda c: c.name)
def test_chain_in_canonical_chain_ids(chain: Chain):
    """Every Chain enum member must have an entry in core/constants.py CHAIN_IDS."""
    assert chain in CANONICAL_CHAIN_IDS, (
        f"Chain.{chain.name} is in the Chain enum but missing from "
        f"core/constants.py CHAIN_IDS. Add it to the canonical mapping."
    )


# ---------------------------------------------------------------------------
# 3. _CHAIN_ALIASES — every Chain must have at least its lowercase name as alias
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS, ids=lambda c: c.name)
def test_chain_in_chain_aliases(chain: Chain):
    """Every Chain enum member must be reachable via _CHAIN_ALIASES.

    At minimum, the lowercase enum name (e.g. 'bsc') must map back to the
    Chain member.  Extra aliases (e.g. 'bnb' -> BSC) are optional.
    """
    canonical = chain.name.lower()
    assert canonical in _CHAIN_ALIASES, (
        f"Chain.{chain.name} has no alias entry in core/constants.py "
        f"_CHAIN_ALIASES. Add at least: '{canonical}': Chain.{chain.name}"
    )
    assert _CHAIN_ALIASES[canonical] is chain, (
        f"_CHAIN_ALIASES['{canonical}'] maps to {_CHAIN_ALIASES[canonical]} "
        f"instead of Chain.{chain.name}."
    )


# ---------------------------------------------------------------------------
# 4. CHAIN_IDS in execution/config.py — every Chain must be executable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS, ids=lambda c: c.name)
def test_chain_in_execution_config(chain: Chain):
    """Every Chain enum member must have an entry in execution/config.py CHAIN_IDS."""
    chain_name = chain.name.lower()
    assert chain_name in EXECUTION_CHAIN_IDS, (
        f"Chain.{chain.name} is in the Chain enum but missing from "
        f'execution/config.py CHAIN_IDS. Add "{chain_name}": '
        f"{CANONICAL_CHAIN_IDS.get(chain, '???')}."
    )
    # Verify chain_id matches canonical source
    if chain in CANONICAL_CHAIN_IDS:
        assert EXECUTION_CHAIN_IDS[chain_name] == CANONICAL_CHAIN_IDS[chain], (
            f"Chain ID mismatch for {chain.name}: execution/config.py has "
            f"{EXECUTION_CHAIN_IDS[chain_name]} but core/constants.py has "
            f"{CANONICAL_CHAIN_IDS[chain]}."
        )


# ---------------------------------------------------------------------------
# 5. CHAIN_IDS in fork_manager.py — every EVM Chain must be forkable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain",
    [c for c in Chain if c not in FORK_MANAGER_EXCLUSIONS],
    ids=lambda c: c.name,
)
def test_chain_in_fork_manager(chain: Chain):
    """Every non-excluded Chain must have an entry in fork_manager.py CHAIN_IDS."""
    chain_name = chain.name.lower()
    assert chain_name in FORK_MANAGER_CHAIN_IDS, (
        f"Chain.{chain.name} is in the Chain enum but missing from "
        f'fork_manager.py CHAIN_IDS. Add "{chain_name}": '
        f"{CANONICAL_CHAIN_IDS.get(chain, '???')}."
    )
    if chain in CANONICAL_CHAIN_IDS:
        assert FORK_MANAGER_CHAIN_IDS[chain_name] == CANONICAL_CHAIN_IDS[chain], (
            f"Chain ID mismatch for {chain.name}: fork_manager.py has "
            f"{FORK_MANAGER_CHAIN_IDS[chain_name]} but core/constants.py has "
            f"{CANONICAL_CHAIN_IDS[chain]}."
        )


# ---------------------------------------------------------------------------
# 6. WRAPPED_NATIVE — every Chain must have a wrapped native token address
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS, ids=lambda c: c.name)
def test_chain_in_wrapped_native(chain: Chain):
    """Every Chain must have a wrapped native token address in defaults.py."""
    chain_name = chain.name.lower()
    assert chain_name in WRAPPED_NATIVE, (
        f"Chain.{chain.name} is in the Chain enum but missing from "
        f"WRAPPED_NATIVE in framework/data/tokens/defaults.py. "
        f"Add the wrapped native token address for {chain.name}."
    )


# ---------------------------------------------------------------------------
# 7. CHAIN_TO_DEXSCREENER_PLATFORM — every Chain should have a DexScreener slug
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", ALL_CHAINS, ids=lambda c: c.name)
def test_chain_in_dexscreener(chain: Chain):
    """Every Chain must have a DexScreener platform slug.

    Without this, price lookups via DexScreener silently fail for the chain.
    """
    chain_name = chain.name.lower()
    assert chain_name in CHAIN_TO_DEXSCREENER_PLATFORM, (
        f"Chain.{chain.name} is in the Chain enum but missing from "
        f"CHAIN_TO_DEXSCREENER_PLATFORM in gateway/data/price/dexscreener.py. "
        f'Add "{chain_name}": "<dexscreener-slug>".'
    )


# ---------------------------------------------------------------------------
# 8. NATIVE_TOKEN_SYMBOLS — every EVM Chain should have a native symbol
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", EVM_CHAINS, ids=lambda c: c.name)
def test_chain_in_native_token_symbols(chain: Chain):
    """Every EVM Chain must have a native token symbol in web3_provider.py.

    Without this, native balance queries return an incorrect symbol.
    """
    chain_name = chain.name.lower()
    assert chain_name in NATIVE_TOKEN_SYMBOLS, (
        f"Chain.{chain.name} is in the Chain enum (EVM) but missing from "
        f"NATIVE_TOKEN_SYMBOLS in gateway/data/balance/web3_provider.py. "
        f'Add "{chain_name}": "<NATIVE_SYMBOL>".'
    )


# ---------------------------------------------------------------------------
# 9. CHAIN_NATIVE_SYMBOL — every EVM Chain should have a managed gateway symbol
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", EVM_CHAINS, ids=lambda c: c.name)
def test_chain_in_managed_gateway_native_symbol(chain: Chain):
    """Every EVM Chain must have a native symbol in ManagedGateway.CHAIN_NATIVE_SYMBOL.

    Without this, Anvil wallet funding skips native gas for this chain.
    """
    chain_name = chain.name.lower()
    assert chain_name in ManagedGateway.CHAIN_NATIVE_SYMBOL, (
        f"Chain.{chain.name} is in the Chain enum (EVM) but missing from "
        f"ManagedGateway.CHAIN_NATIVE_SYMBOL in gateway/managed.py. "
        f'Add "{chain_name}": "<NATIVE_SYMBOL>".'
    )
    # Verify symbol consistency with web3_provider registry
    expected_symbol = NATIVE_TOKEN_SYMBOLS.get(chain_name)
    if expected_symbol:
        actual_symbol = ManagedGateway.CHAIN_NATIVE_SYMBOL[chain_name]
        assert actual_symbol == expected_symbol, (
            f"Native symbol mismatch for {chain.name}: managed.py has {actual_symbol} "
            f"but web3_provider.py has {expected_symbol}."
        )


# ---------------------------------------------------------------------------
# Reverse checks — no stale entries in registries
# ---------------------------------------------------------------------------


def test_no_extra_chains_in_execution_config():
    """execution/config.py CHAIN_IDS should not have chains missing from Chain enum."""
    canonical_names = {c.name.lower() for c in Chain}
    for config_name in EXECUTION_CHAIN_IDS:
        assert config_name in canonical_names or config_name in REVERSE_CHECK_KNOWN_EXTRA, (
            f"execution/config.py CHAIN_IDS has '{config_name}' but it's not in "
            f"the Chain enum. Either add Chain.{config_name.upper()} or remove "
            f"the stale entry."
        )


def test_no_extra_chains_in_fork_manager():
    """fork_manager.py CHAIN_IDS should not have chains missing from Chain enum."""
    canonical_names = {c.name.lower() for c in Chain}
    for config_name in FORK_MANAGER_CHAIN_IDS:
        assert config_name in canonical_names or config_name in REVERSE_CHECK_KNOWN_EXTRA, (
            f"fork_manager.py CHAIN_IDS has '{config_name}' but it's not in "
            f"the Chain enum. Either add Chain.{config_name.upper()} or remove "
            f"the stale entry."
        )


def test_no_extra_chains_in_wrapped_native():
    """WRAPPED_NATIVE should not have chains missing from Chain enum."""
    canonical_names = {c.name.lower() for c in Chain}
    for chain_name in WRAPPED_NATIVE:
        assert chain_name in canonical_names or chain_name in REVERSE_CHECK_KNOWN_EXTRA, (
            f"WRAPPED_NATIVE has '{chain_name}' but it's not in the Chain enum. "
            f"Either add Chain.{chain_name.upper()} or remove the stale entry."
        )
