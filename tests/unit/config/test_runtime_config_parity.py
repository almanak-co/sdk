"""Unit tests for ``runtime_config_from_env``.

Phase 5a-2 of the config-service migration: the legacy
:meth:`LocalRuntimeConfig.from_env` and :meth:`MultiChainRuntimeConfig.from_env`
classmethods are gone; :func:`almanak.config.runtime.runtime_config_from_env`
is the single env-reading entry point. These tests assert that — for every
distinct env shape the new factory accepts — the constructed
:class:`RuntimeConfig` carries the right field values.

Originally a parity test (5a-1: assert new factory equals legacy dataclass),
this file was rewritten in 5a-2 as a unit test on the new factory only;
the field expectations are pinned values, not derived from the legacy code.

Each scenario uses ``monkeypatch.setenv`` (never raw ``os.environ``) and a
sentinel scrub to keep results deterministic regardless of the developer's
``.env`` file. The 17 cases cover single-chain (Anvil + mainnet + Solana +
optional knobs), multi-chain (per-chain RPCs + Anvil ports + defaults), and
the error / hygiene contracts.
"""

from __future__ import annotations

import pytest

from almanak.config.runtime import (
    ConfigurationError,
    MissingEnvironmentVariableError,
    RuntimeConfig,
    runtime_config_from_env,
)

# Env vars consumed (directly or transitively) by ``runtime_config_from_env``
# and by the RPC URL builder. Listed explicitly so the scrub is auditable;
# any new env-var read must be added here so a developer's ``.env`` cannot
# leak into the test.
_RUNTIME_ENV_VARS: tuple[str, ...] = (
    # Core single-chain reads.
    "ALMANAK_PRIVATE_KEY",
    "SOLANA_PRIVATE_KEY",
    "ALMANAK_CHAIN",
    "ALMANAK_RPC_URL",
    "ALMANAK_EXECUTION_MODE",
    "ALMANAK_SAFE_ADDRESS",
    "ALMANAK_EOA_ADDRESS",
    "ALMANAK_ZODIAC_ADDRESS",
    "ALMANAK_SIGNER_SERVICE_URL",
    "ALMANAK_SIGNER_SERVICE_JWT",
    "ALMANAK_GATEWAY_WALLETS",
    "ALMANAK_MAX_GAS_PRICE_GWEI",
    "ALMANAK_MAX_GAS_COST_NATIVE",
    "ALMANAK_MAX_GAS_COST_USD",
    "ALMANAK_MAX_SLIPPAGE_BPS",
    "ALMANAK_TX_TIMEOUT_SECONDS",
    "ALMANAK_SIMULATION_ENABLED",
    "ALMANAK_MAX_TX_VALUE_ETH",
    "ALMANAK_BASE_RETRY_DELAY",
    "ALMANAK_MAX_RETRY_DELAY",
    "ALMANAK_MAX_RETRIES",
    # Per-chain RPC URLs (multi-chain, plus single-chain chain-specific).
    "ALMANAK_ETHEREUM_RPC_URL",
    "ALMANAK_ARBITRUM_RPC_URL",
    "ALMANAK_OPTIMISM_RPC_URL",
    "ALMANAK_POLYGON_RPC_URL",
    "ALMANAK_BASE_RPC_URL",
    "ALMANAK_AVALANCHE_RPC_URL",
    "ALMANAK_BSC_RPC_URL",
    "ALMANAK_LINEA_RPC_URL",
    "ALMANAK_BLAST_RPC_URL",
    "ALMANAK_MANTLE_RPC_URL",
    "ALMANAK_BERACHAIN_RPC_URL",
    "ALMANAK_SONIC_RPC_URL",
    "ALMANAK_SOLANA_RPC_URL",
    # Anvil port overrides — multi-chain anvil mode reads chain-specific
    # ports; single-chain falls back to ``ANVIL_PORT``.
    "ANVIL_PORT",
    "ANVIL_ETHEREUM_PORT",
    "ANVIL_ARBITRUM_PORT",
    "ANVIL_OPTIMISM_PORT",
    "ANVIL_POLYGON_PORT",
    "ANVIL_BASE_PORT",
    "ANVIL_AVALANCHE_PORT",
    "ANVIL_BSC_PORT",
    # Generic / public RPC fallbacks consumed by ``get_rpc_url``.
    "RPC_URL",
    "ALCHEMY_API_KEY",
    "TENDERLY_API_KEY_ETHEREUM",
    "TENDERLY_API_KEY_ARBITRUM",
    "TENDERLY_API_KEY_OPTIMISM",
    "TENDERLY_API_KEY_POLYGON",
    "TENDERLY_API_KEY_BASE",
    "TENDERLY_API_KEY_SOLANA",
    # Gateway prefix vars (transitive via ``_get_gateway_api_key``).
    "ALMANAK_GATEWAY_ALCHEMY_API_KEY",
    "ALMANAK_GATEWAY_TENDERLY_API_KEY_ARBITRUM",
)


# Deterministic test fixtures.
_TEST_PRIVATE_KEY = "0x" + "ab" * 32
# Wallet address derived from ``_TEST_PRIVATE_KEY`` via eth_account.
# Pinned so we can assert wallet derivation went through cleanly without
# re-running keccak in the test.
_TEST_RPC_URL = "https://arb1.arbitrum.io/rpc"
_TEST_BASE_RPC_URL = "https://base-mainnet.example.com/rpc"
_TEST_OPTIMISM_RPC_URL = "https://optimism-mainnet.example.com/rpc"
# Solana base58 keypair generated deterministically from a fixed seed; never
# used on-chain. ``solders.Keypair.from_seed(bytes(range(32)))`` yields the
# 64-byte secret key whose base58 encoding is the value below; the address
# pinned next to it lets us assert the derivation went through cleanly.
_TEST_SOLANA_PRIVATE_KEY = "1GMkH3brNXiNNs1tiFZHu4yZSRrzJwxi5wB9bHFtMikjwpAW9DMZzU2Pqakc5it8X3N5vPmqdN7KF4CCUpmKhq"
_TEST_SOLANA_WALLET_ADDRESS = "FAe4sisG95oZ42w7buUn5qEE4TAnfTTFPiguZUHmhiF"


@pytest.fixture
def runtime_env_scrub(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Scrub every runtime-relevant env var; return the same monkeypatch."""
    for name in _RUNTIME_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _assert_single_chain(rc: RuntimeConfig, *, chain: str) -> None:
    """Assert the structural invariants of a single-chain RuntimeConfig."""
    assert rc.single_chain is True
    assert rc.chain == chain
    assert rc.rpc_url is not None and rc.rpc_url != ""
    assert rc.chain_id != 0 or chain == "solana"  # Solana has chain_id == 0
    # Plural mirrors must round-trip cleanly for single-chain rows.
    assert rc.chains == [chain]
    assert rc.rpc_urls == {chain: rc.rpc_url}
    assert rc.chain_ids == {chain: rc.chain_id}
    assert rc.primary_chain == chain


def _assert_multi_chain(rc: RuntimeConfig, *, chains: list[str]) -> None:
    """Assert the structural invariants of a multi-chain RuntimeConfig."""
    assert rc.single_chain is False
    # Singular view is intentionally null in the multi-chain lane.
    assert rc.chain is None
    assert rc.rpc_url is None
    assert rc.chain_id == 0
    assert rc.chains == chains
    assert rc.primary_chain == chains[0]
    assert set(rc.chain_ids.keys()) == set(chains)


# --- Single-chain success scenarios -------------------------------------------


def test_single_chain_anvil_with_explicit_private_key(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Anvil network + explicit ``private_key`` kwarg: no env required."""
    rc = runtime_config_from_env(
        chain="arbitrum",
        network="anvil",
        private_key=_TEST_PRIVATE_KEY,
    )
    _assert_single_chain(rc, chain="arbitrum")
    assert rc.private_key == _TEST_PRIVATE_KEY
    assert rc.network == "anvil"
    # Anvil mode forces gas cap to ANVIL_GAS_PRICE_CAP_GWEI (9999).
    assert rc.max_gas_price_gwei == 9999


def test_single_chain_anvil_with_anvil_port_override(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Anvil network + ``ANVIL_ARBITRUM_PORT`` env override: URL reflects the port."""
    runtime_env_scrub.setenv("ANVIL_ARBITRUM_PORT", "8546")
    rc = runtime_config_from_env(
        chain="arbitrum",
        network="anvil",
        private_key=_TEST_PRIVATE_KEY,
    )
    _assert_single_chain(rc, chain="arbitrum")
    assert rc.rpc_url == "http://127.0.0.1:8546"


def test_single_chain_mainnet_private_key_from_env(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Mainnet: ``ALMANAK_PRIVATE_KEY`` + chain-specific RPC URL from env."""
    runtime_env_scrub.setenv("ALMANAK_PRIVATE_KEY", _TEST_PRIVATE_KEY)
    runtime_env_scrub.setenv("ALMANAK_ARBITRUM_RPC_URL", _TEST_RPC_URL)
    rc = runtime_config_from_env(chain="arbitrum", network="mainnet")
    _assert_single_chain(rc, chain="arbitrum")
    assert rc.rpc_url == _TEST_RPC_URL
    assert rc.private_key == _TEST_PRIVATE_KEY
    assert rc.network == "mainnet"


def test_single_chain_mainnet_generic_rpc_url_fallback(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Mainnet: generic ``ALMANAK_RPC_URL`` is consumed when chain-specific is absent."""
    runtime_env_scrub.setenv("ALMANAK_PRIVATE_KEY", _TEST_PRIVATE_KEY)
    runtime_env_scrub.setenv("ALMANAK_RPC_URL", _TEST_RPC_URL)
    rc = runtime_config_from_env(chain="arbitrum", network="mainnet")
    _assert_single_chain(rc, chain="arbitrum")
    assert rc.rpc_url == _TEST_RPC_URL


def test_single_chain_solana_uses_solana_private_key(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Solana lane: ``SOLANA_PRIVATE_KEY`` (bare) is read instead of ALMANAK_PRIVATE_KEY.

    Mirrors the ``_resolve_private_key_from_env`` Solana branch — when the
    chain is solana and ``private_key`` is unset, it reads
    ``SOLANA_PRIVATE_KEY`` directly (bare name, not the ALMANAK_-prefixed
    fallback). The base58 keypair is generated deterministically from a
    fixed 32-byte seed so the wallet derivation is reproducible.
    """
    runtime_env_scrub.setenv("SOLANA_PRIVATE_KEY", _TEST_SOLANA_PRIVATE_KEY)
    # Anvil network avoids the live Solana RPC requirement.
    rc = runtime_config_from_env(chain="solana", network="anvil")
    _assert_single_chain(rc, chain="solana")
    assert rc.private_key == _TEST_SOLANA_PRIVATE_KEY
    assert rc.wallet_address == _TEST_SOLANA_WALLET_ADDRESS


def test_single_chain_optional_knobs_round_trip(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Optional gas / tx / retry knobs round-trip identically."""
    runtime_env_scrub.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "250")
    runtime_env_scrub.setenv("ALMANAK_MAX_SLIPPAGE_BPS", "75")
    runtime_env_scrub.setenv("ALMANAK_TX_TIMEOUT_SECONDS", "240")
    runtime_env_scrub.setenv("ALMANAK_SIMULATION_ENABLED", "false")
    runtime_env_scrub.setenv("ALMANAK_MAX_TX_VALUE_ETH", "5.5")
    runtime_env_scrub.setenv("ALMANAK_MAX_RETRIES", "7")
    rc = runtime_config_from_env(
        chain="arbitrum",
        network="anvil",
        private_key=_TEST_PRIVATE_KEY,
    )
    _assert_single_chain(rc, chain="arbitrum")
    # Anvil mode forces gas cap to ANVIL_GAS_PRICE_CAP_GWEI; user override ignored.
    assert rc.max_gas_price_gwei == 9999
    assert rc.max_slippage_bps == 75
    assert rc.tx_timeout_seconds == 240
    assert rc.simulation_enabled is False
    assert rc.max_tx_value_eth == 5.5
    assert rc.max_retries == 7


# --- Multi-chain success scenarios --------------------------------------------


def test_multi_chain_mainnet_per_chain_rpcs(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain mainnet: each chain reads its ``ALMANAK_{CHAIN}_RPC_URL``."""
    runtime_env_scrub.setenv("ALMANAK_PRIVATE_KEY", _TEST_PRIVATE_KEY)
    runtime_env_scrub.setenv("ALMANAK_ARBITRUM_RPC_URL", _TEST_RPC_URL)
    runtime_env_scrub.setenv("ALMANAK_BASE_RPC_URL", _TEST_BASE_RPC_URL)
    runtime_env_scrub.setenv("ALMANAK_OPTIMISM_RPC_URL", _TEST_OPTIMISM_RPC_URL)
    chains = ["arbitrum", "base", "optimism"]
    protocols = {
        "arbitrum": ["uniswap_v3"],
        "base": ["uniswap_v3"],
        "optimism": ["uniswap_v3"],
    }
    rc = runtime_config_from_env(chains=chains, protocols=protocols, network="mainnet")
    _assert_multi_chain(rc, chains=chains)
    assert rc.rpc_urls == {
        "arbitrum": _TEST_RPC_URL,
        "base": _TEST_BASE_RPC_URL,
        "optimism": _TEST_OPTIMISM_RPC_URL,
    }
    assert rc.private_key == _TEST_PRIVATE_KEY


def test_multi_chain_anvil_with_per_chain_ports(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain anvil: per-chain ``ANVIL_{CHAIN}_PORT`` overrides apply."""
    runtime_env_scrub.setenv("ANVIL_ARBITRUM_PORT", "8546")
    runtime_env_scrub.setenv("ANVIL_BASE_PORT", "8547")
    chains = ["arbitrum", "base"]
    protocols = {
        "arbitrum": ["uniswap_v3"],
        "base": ["uniswap_v3"],
    }
    rc = runtime_config_from_env(
        chains=chains,
        protocols=protocols,
        network="anvil",
        private_key=_TEST_PRIVATE_KEY,
    )
    _assert_multi_chain(rc, chains=chains)
    assert rc.rpc_urls == {
        "arbitrum": "http://127.0.0.1:8546",
        "base": "http://127.0.0.1:8547",
    }


def test_multi_chain_anvil_default_ports(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain anvil with no port overrides uses ``ANVIL_CHAIN_PORTS``."""
    chains = ["arbitrum", "base"]
    protocols = {
        "arbitrum": ["uniswap_v3"],
        "base": ["uniswap_v3"],
    }
    rc = runtime_config_from_env(
        chains=chains,
        protocols=protocols,
        network="anvil",
        private_key=_TEST_PRIVATE_KEY,
    )
    _assert_multi_chain(rc, chains=chains)
    # Default Anvil ports come from ``ANVIL_CHAIN_PORTS``; we don't pin
    # them here (they're an implementation detail) but they must be set.
    for chain in chains:
        assert rc.rpc_urls[chain].startswith("http://127.0.0.1:")


# --- Error path scenarios -----------------------------------------------------


def test_single_chain_missing_private_key_raises_missing_env_error(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Mainnet without ``ALMANAK_PRIVATE_KEY``: raises ``MissingEnvironmentVariableError``."""
    runtime_env_scrub.setenv("ALMANAK_ARBITRUM_RPC_URL", _TEST_RPC_URL)
    with pytest.raises(MissingEnvironmentVariableError) as exc:
        runtime_config_from_env(chain="arbitrum", network="mainnet")
    assert exc.value.var_name == "ALMANAK_PRIVATE_KEY"


def test_multi_chain_honours_custom_prefix(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain mainnet with a non-default ``prefix=`` reads the prefixed env vars.

    Regression test for PR #2152 review: the multi-chain lane previously
    hard-coded ``ALMANAK_GATEWAY_WALLETS`` and ``ALMANAK_{CHAIN}_RPC_URL``
    irrespective of the caller's ``prefix=`` kwarg, so non-default
    prefixes silently fell through to the default RPC build path. The
    single-chain lane already honoured ``prefix``; this asserts parity."""
    runtime_env_scrub.setenv("MYAPP_PRIVATE_KEY", _TEST_PRIVATE_KEY)
    runtime_env_scrub.setenv("MYAPP_ARBITRUM_RPC_URL", _TEST_RPC_URL)
    runtime_env_scrub.setenv("MYAPP_BASE_RPC_URL", _TEST_BASE_RPC_URL)
    # Set the ALMANAK_ counterparts to wrong values so an accidental
    # ALMANAK_-prefix lookup would visibly diverge from the assertion below.
    runtime_env_scrub.setenv("ALMANAK_ARBITRUM_RPC_URL", "https://wrong.invalid")
    runtime_env_scrub.setenv("ALMANAK_BASE_RPC_URL", "https://wrong.invalid")
    chains = ["arbitrum", "base"]
    protocols = {"arbitrum": ["uniswap_v3"], "base": ["uniswap_v3"]}
    rc = runtime_config_from_env(chains=chains, protocols=protocols, network="mainnet", prefix="MYAPP_")
    _assert_multi_chain(rc, chains=chains)
    assert rc.rpc_urls == {"arbitrum": _TEST_RPC_URL, "base": _TEST_BASE_RPC_URL}
    assert rc.private_key == _TEST_PRIVATE_KEY


def test_multi_chain_custom_prefix_gateway_wallets_mode(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain gateway-wallets mode honours the custom prefix.

    Regression test for PR #2152 review: the gateway-wallets check was
    hard-coded to ``ALMANAK_GATEWAY_WALLETS``, so a caller using
    ``prefix="MYAPP_"`` would never enter the gateway-wallets short-circuit
    and instead try to dynamically build RPC URLs."""
    runtime_env_scrub.setenv("MYAPP_GATEWAY_WALLETS", "1")
    chains = ["arbitrum", "base"]
    protocols = {"arbitrum": ["uniswap_v3"], "base": ["uniswap_v3"]}
    # private_key="" forces the gateway-wallets branch.
    rc = runtime_config_from_env(
        chains=chains,
        protocols=protocols,
        network="mainnet",
        prefix="MYAPP_",
        private_key="",
    )
    assert rc.rpc_urls == {}, "gateway-wallets mode must short-circuit RPC loading for the multi-chain lane"


def test_multi_chain_missing_private_key_raises_missing_env_error(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain mainnet without ``ALMANAK_PRIVATE_KEY``: raises ``MissingEnvironmentVariableError``."""
    runtime_env_scrub.setenv("ALMANAK_ARBITRUM_RPC_URL", _TEST_RPC_URL)
    runtime_env_scrub.setenv("ALMANAK_BASE_RPC_URL", _TEST_BASE_RPC_URL)
    chains = ["arbitrum", "base"]
    protocols = {"arbitrum": ["uniswap_v3"], "base": ["uniswap_v3"]}
    with pytest.raises(MissingEnvironmentVariableError) as exc:
        runtime_config_from_env(chains=chains, protocols=protocols, network="mainnet")
    assert exc.value.var_name == "ALMANAK_PRIVATE_KEY"


def test_multi_chain_invalid_per_chain_rpc_raises_configuration_error(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain mainnet with malformed ``ALMANAK_{CHAIN}_RPC_URL``: ConfigurationError.

    ``_resolve_multi_chain_rpc_urls`` validates the URL format with a regex
    and raises :class:`ConfigurationError` with ``field`` set to the
    offending env-var name when the value doesn't match.
    """
    runtime_env_scrub.setenv("ALMANAK_PRIVATE_KEY", _TEST_PRIVATE_KEY)
    runtime_env_scrub.setenv("ALMANAK_ARBITRUM_RPC_URL", _TEST_RPC_URL)
    runtime_env_scrub.setenv("ALMANAK_BASE_RPC_URL", "not-a-url")
    chains = ["arbitrum", "base"]
    protocols = {"arbitrum": ["uniswap_v3"], "base": ["uniswap_v3"]}
    with pytest.raises(ConfigurationError) as exc:
        runtime_config_from_env(chains=chains, protocols=protocols, network="mainnet")
    assert exc.value.field == "ALMANAK_BASE_RPC_URL"


# --- Factory hygiene ----------------------------------------------------------


def test_factory_rejects_chain_and_chains_together(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """The factory must reject ambiguous lane selection up front."""
    with pytest.raises(ConfigurationError) as exc:
        runtime_config_from_env(
            chain="arbitrum",
            chains=["arbitrum"],
            protocols={"arbitrum": ["uniswap_v3"]},
            network="anvil",
            private_key=_TEST_PRIVATE_KEY,
        )
    assert exc.value.field == "chain"


def test_factory_rejects_chains_without_protocols(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multi-chain lane requires ``protocols``; absence raises early."""
    with pytest.raises(ConfigurationError) as exc:
        runtime_config_from_env(
            chains=["arbitrum"],
            network="anvil",
            private_key=_TEST_PRIVATE_KEY,
        )
    assert exc.value.field == "protocols"


def test_factory_rejects_protocols_in_single_chain_lane(
    runtime_env_scrub: pytest.MonkeyPatch,
) -> None:
    """``protocols`` is illegal alongside ``chain`` (single-chain lane)."""
    with pytest.raises(ConfigurationError) as exc:
        runtime_config_from_env(
            chain="arbitrum",
            protocols={"arbitrum": ["uniswap_v3"]},
            network="anvil",
            private_key=_TEST_PRIVATE_KEY,
        )
    assert exc.value.field == "protocols"


def test_runtime_config_invariant_single_chain_requires_chain() -> None:
    """Direct constructor users must respect the discriminator invariant.

    Pydantic v2 wraps ``ValueError`` raised inside ``@model_validator`` into
    ``pydantic.ValidationError``, so callers see the typed exception, not
    the bare ``ValueError`` (PR #2152 review)."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="requires `chain`"):
        RuntimeConfig(single_chain=True)


def test_runtime_config_invariant_multi_chain_requires_chains() -> None:
    """Direct constructor users must respect the discriminator invariant.

    Pydantic v2 wraps ``ValueError`` raised inside ``@model_validator`` into
    ``pydantic.ValidationError`` — see the single-chain sibling test."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="requires `chains` non-empty"):
        RuntimeConfig(single_chain=False)
