"""Tests for ``almanak.config.cli_runtime``.

Phase 5e of the config-service migration. These tests pin the contract
that :func:`cli_runtime_config_from_env` is the single env reader for
the CLI-specific cluster — gateway-wallets discriminator, Safe-mode
preflight inputs, Solana fork URL/port, Anvil per-chain ports,
reconciliation / hardcoded-prices toggles, and the legacy unprefixed
``GATEWAY_AUTH_TOKEN`` fallback:

* Empty env → defaults match the legacy callsite hard-codes
  (``DEFAULT_SOLANA_RPC_URL``, ``DEFAULT_SOLANA_VALIDATOR_PORT``,
  ``False`` boolean toggles).
* Each documented env var is honoured.
* The chain-RPC ladder walks in the documented order.
* Boolean truthy parsing matches the legacy callsite ladders.
* The :func:`subprocess_env_with_overrides` boundary helper produces
  a fresh dict that respects the parent env.
"""

from __future__ import annotations

import os

import pytest

from almanak.config.cli_runtime import (
    DEFAULT_ANVIL_PORT,
    DEFAULT_SOLANA_RPC_URL,
    DEFAULT_SOLANA_VALIDATOR_PORT,
    CliRuntimeConfig,
    anvil_port_for_chain,
    chain_rpc_url_from_env,
    cli_runtime_config_from_env,
    gas_risk_override_presence,
    max_value_usd_override,
    subprocess_env_with_overrides,
)

# Every env var the factory reads. Listed explicitly so a future field
# addition that forgets to wire up the scrub fails loudly here rather
# than as an order-dependent flake elsewhere.
_CLI_RUNTIME_ENV_VARS: tuple[str, ...] = (
    # Gateway client auth + legacy host/port/timeout aliases (Phase 6).
    "GATEWAY_AUTH_TOKEN",
    "ALMANAK_GATEWAY_AUTH_TOKEN",
    "GATEWAY_HOST",
    "ALMANAK_GATEWAY_HOST",
    "GATEWAY_PORT",
    "ALMANAK_GATEWAY_PORT",
    "GATEWAY_TIMEOUT",
    "ALMANAK_GATEWAY_TIMEOUT",
    # Gateway-wallets discriminator + Safe-mode preflight.
    "ALMANAK_GATEWAY_WALLETS",
    "ALMANAK_GATEWAY_SAFE_MODE",
    "ALMANAK_GATEWAY_SAFE_ADDRESS",
    "ALMANAK_SAFE_ADDRESS",
    "ALMANAK_EOA_ADDRESS",
    "ALMANAK_EXECUTION_MODE",
    "ALMANAK_CHAIN",
    # Solana fork.
    "SOLANA_RPC_URL",
    "SOLANA_VALIDATOR_PORT",
    # Boolean toggles.
    "ALMANAK_RECONCILIATION_ENFORCEMENT",
    "ALMANAK_ALLOW_HARDCODED_PRICES",
    # CI hint.
    "CI",
    # Gas/risk override env vars (all the prefixed + legacy unprefixed
    # forms that the override resolver checks for presence).
    "ALMANAK_MAX_GAS_PRICE_GWEI",
    "MAX_GAS_PRICE_GWEI",
    "ALMANAK_MAX_GAS_COST_NATIVE",
    "MAX_GAS_COST_NATIVE",
    "ALMANAK_MAX_GAS_COST_USD",
    "MAX_GAS_COST_USD",
    "ALMANAK_MAX_SLIPPAGE_BPS",
    "MAX_SLIPPAGE_BPS",
    "ALMANAK_MAX_VALUE_USD",
    "MAX_VALUE_USD",
    # Chain-RPC ladder consumers.
    "ALMANAK_RPC_URL",
    "RPC_URL",
)

# Anvil ports — the factory walks the union of supported chains; scrub
# every chain so a stray ``ANVIL_ARBITRUM_PORT=8546`` in the developer's
# .env doesn't silently land in the typed dict.
_ANVIL_PORT_VARS: tuple[str, ...] = tuple(
    f"ANVIL_{chain}_PORT"
    for chain in (
        "ETHEREUM",
        "ARBITRUM",
        "OPTIMISM",
        "POLYGON",
        "BASE",
        "AVALANCHE",
        "BSC",
        "LINEA",
        "BLAST",
        "MANTLE",
        "BERACHAIN",
        "SONIC",
        "MONAD",
        "XLAYER",
        "ZEROG",
        "PLASMA",
    )
)


# Per-chain RPC URL ladder — same per-chain shape as Anvil ports.
_CHAIN_RPC_VARS: tuple[str, ...] = tuple(
    f"ALMANAK_{chain}_RPC_URL"
    for chain in ("ETHEREUM", "ARBITRUM", "OPTIMISM", "POLYGON", "BASE", "AVALANCHE", "BSC", "LINEA", "BLAST", "MANTLE")
) + tuple(
    f"{chain}_RPC_URL"
    for chain in ("ETHEREUM", "ARBITRUM", "OPTIMISM", "POLYGON", "BASE", "AVALANCHE", "BSC", "LINEA", "BLAST", "MANTLE")
)


@pytest.fixture(autouse=True)
def _scrub_cli_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Scrub every env var the factory reads.

    Without the scrub these tests are non-deterministic — a developer's
    ``.env`` (or a prior test that called ``setenv``) would silently
    populate a "default" assertion.
    """
    for name in _CLI_RUNTIME_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    for name in _ANVIL_PORT_VARS:
        monkeypatch.delenv(name, raising=False)
    for name in _CHAIN_RPC_VARS:
        monkeypatch.delenv(name, raising=False)


# =============================================================================
# Defaults
# =============================================================================


class TestDefaults:
    def test_legacy_gateway_auth_defaults_to_none(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.legacy_gateway_auth_token is None

    def test_gateway_wallets_defaults_to_false(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_wallets_configured is False

    def test_safe_mode_inputs_default_to_none(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_safe_mode is None
        assert cfg.gateway_safe_address is None
        assert cfg.safe_address is None
        assert cfg.eoa_address is None
        assert cfg.execution_mode is None

    def test_solana_defaults_match_legacy_constants(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.solana_rpc_url == DEFAULT_SOLANA_RPC_URL
        assert cfg.solana_validator_port == DEFAULT_SOLANA_VALIDATOR_PORT

    def test_anvil_ports_empty_when_no_env(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.anvil_ports == {}

    def test_boolean_toggles_default_to_false(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.reconciliation_enforcement is False
        assert cfg.allow_hardcoded_prices is False
        assert cfg.is_ci is False


# =============================================================================
# Per-field env reads
# =============================================================================


class TestEnvReads:
    def test_legacy_gateway_auth_token_honoured(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "legacy-token")
        cfg = cli_runtime_config_from_env()
        assert cfg.legacy_gateway_auth_token == "legacy-token"

    def test_gateway_wallets_configured_truthy_for_any_value(self, monkeypatch: pytest.MonkeyPatch):
        # Mirrors the legacy ``bool(os.environ.get(...))`` check — even ``"0"``
        # is truthy because the legacy gate didn't apply truthy-string parsing.
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "0")
        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_wallets_configured is True

    def test_safe_mode_lowercased(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "ZODIAC")
        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_safe_mode == "zodiac"

    def test_execution_mode_lowercased(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "SAFE_ZODIAC")
        cfg = cli_runtime_config_from_env()
        assert cfg.execution_mode == "safe_zodiac"

    def test_safe_address_pair(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", "0xgateway")
        monkeypatch.setenv("ALMANAK_SAFE_ADDRESS", "0xframework")
        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_safe_address == "0xgateway"
        assert cfg.safe_address == "0xframework"

    def test_solana_rpc_override(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("SOLANA_RPC_URL", "https://custom.solana")
        cfg = cli_runtime_config_from_env()
        assert cfg.solana_rpc_url == "https://custom.solana"

    def test_solana_validator_port_override(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("SOLANA_VALIDATOR_PORT", "9000")
        cfg = cli_runtime_config_from_env()
        assert cfg.solana_validator_port == 9000

    def test_solana_validator_port_malformed_raises(self, monkeypatch: pytest.MonkeyPatch):
        """Malformed port env vars must fail loud at boot rather than fall
        back to the default (PR #2152 review): a silent default points the
        process at the wrong local node and is far harder to diagnose."""
        monkeypatch.setenv("SOLANA_VALIDATOR_PORT", "not-a-number")
        with pytest.raises(ValueError, match="SOLANA_VALIDATOR_PORT"):
            cli_runtime_config_from_env()

    def test_anvil_ports_lowercased_keys(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ANVIL_ARBITRUM_PORT", "8546")
        monkeypatch.setenv("ANVIL_BASE_PORT", "8547")
        cfg = cli_runtime_config_from_env()
        assert cfg.anvil_ports == {"arbitrum": 8546, "base": 8547}

    def test_anvil_ports_malformed_raises(self, monkeypatch: pytest.MonkeyPatch):
        """Malformed Anvil port env vars must fail loud at boot, mirroring
        SOLANA_VALIDATOR_PORT — see ``test_solana_validator_port_malformed_raises``."""
        monkeypatch.setenv("ANVIL_ARBITRUM_PORT", "garbage")
        with pytest.raises(ValueError, match="ANVIL_ARBITRUM_PORT"):
            cli_runtime_config_from_env()

    def test_reconciliation_truthy_ladder(self, monkeypatch: pytest.MonkeyPatch):
        for truthy in ("1", "true", "TRUE", "yes", "  yes  "):
            monkeypatch.setenv("ALMANAK_RECONCILIATION_ENFORCEMENT", truthy)
            cfg = cli_runtime_config_from_env()
            assert cfg.reconciliation_enforcement is True, f"truthy={truthy!r}"
        for falsy in ("0", "false", "no", "", "garbage"):
            monkeypatch.setenv("ALMANAK_RECONCILIATION_ENFORCEMENT", falsy)
            cfg = cli_runtime_config_from_env()
            assert cfg.reconciliation_enforcement is False, f"falsy={falsy!r}"

    def test_allow_hardcoded_prices_strict_one_only(self, monkeypatch: pytest.MonkeyPatch):
        # Legacy callsite tested ``== "1"`` exactly — preserve that strictness.
        monkeypatch.setenv("ALMANAK_ALLOW_HARDCODED_PRICES", "1")
        assert cli_runtime_config_from_env().allow_hardcoded_prices is True
        for falsy in ("true", "yes", "0", ""):
            monkeypatch.setenv("ALMANAK_ALLOW_HARDCODED_PRICES", falsy)
            assert cli_runtime_config_from_env().allow_hardcoded_prices is False, f"value={falsy!r}"

    def test_is_ci_any_non_empty(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("CI", "true")
        assert cli_runtime_config_from_env().is_ci is True
        monkeypatch.setenv("CI", "1")
        assert cli_runtime_config_from_env().is_ci is True
        monkeypatch.setenv("CI", "")
        assert cli_runtime_config_from_env().is_ci is False


# =============================================================================
# gRPC client resolved values — Phase 6 ``ALMANAK_GATEWAY_*`` >
# ``GATEWAY_*`` > hardcoded ladder
# =============================================================================


class TestGatewayClientResolved:
    """The resolved fields encode the precedence ladder once so
    :meth:`GatewayClientConfig.from_env` doesn't have to re-read env.
    """

    def test_defaults_match_legacy_hardcoded_ladder(self):
        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_client_host_resolved == "localhost"
        assert cfg.gateway_client_port_resolved == 50051
        assert cfg.gateway_client_timeout_resolved == 30.0
        assert cfg.gateway_client_auth_token_resolved is None

    def test_almanak_prefix_wins_over_legacy(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "almanak.local")
        monkeypatch.setenv("GATEWAY_HOST", "legacy.local")
        monkeypatch.setenv("ALMANAK_GATEWAY_PORT", "60051")
        monkeypatch.setenv("GATEWAY_PORT", "50052")
        monkeypatch.setenv("ALMANAK_GATEWAY_TIMEOUT", "120.0")
        monkeypatch.setenv("GATEWAY_TIMEOUT", "60.0")
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "almanak-token")
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "legacy-token")

        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_client_host_resolved == "almanak.local"
        assert cfg.gateway_client_port_resolved == 60051
        assert cfg.gateway_client_timeout_resolved == 120.0
        assert cfg.gateway_client_auth_token_resolved == "almanak-token"

    def test_legacy_used_when_almanak_unset(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GATEWAY_HOST", "legacy.local")
        monkeypatch.setenv("GATEWAY_PORT", "50052")
        monkeypatch.setenv("GATEWAY_TIMEOUT", "60.0")
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "legacy-token")

        cfg = cli_runtime_config_from_env()
        assert cfg.gateway_client_host_resolved == "legacy.local"
        assert cfg.gateway_client_port_resolved == 50052
        assert cfg.gateway_client_timeout_resolved == 60.0
        assert cfg.gateway_client_auth_token_resolved == "legacy-token"

    def test_legacy_aliases_populated_independently(self, monkeypatch: pytest.MonkeyPatch):
        # The legacy_*/resolved fields are independent — operators
        # diagnosing a deprecation warning need to know which env var
        # was actually set, not just the resolved value.
        monkeypatch.setenv("GATEWAY_HOST", "legacy.local")
        monkeypatch.setenv("GATEWAY_PORT", "50052")
        monkeypatch.setenv("GATEWAY_TIMEOUT", "60.0")
        cfg = cli_runtime_config_from_env()
        assert cfg.legacy_gateway_host == "legacy.local"
        assert cfg.legacy_gateway_port == 50052
        assert cfg.legacy_gateway_timeout == 60.0

    def test_gateway_client_auth_token_repr_suppressed(self):
        cfg = CliRuntimeConfig(gateway_client_auth_token_resolved="VERY-SECRET-VALUE")
        assert "VERY-SECRET-VALUE" not in repr(cfg)

    def test_malformed_legacy_port_raises(self, monkeypatch: pytest.MonkeyPatch):
        # Codex review on PR 2156: silent fallback on a present-but-malformed
        # port value is unsafe — a typo could make a strategy dial the
        # default localhost gateway already serving another strategy.
        monkeypatch.setenv("GATEWAY_PORT", "not-a-number")
        with pytest.raises(ValueError, match="GATEWAY_PORT"):
            cli_runtime_config_from_env()

    def test_malformed_almanak_port_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_PORT", "garbage")
        with pytest.raises(ValueError, match="ALMANAK_GATEWAY_PORT"):
            cli_runtime_config_from_env()

    def test_malformed_legacy_timeout_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GATEWAY_TIMEOUT", "abc")
        with pytest.raises(ValueError, match="GATEWAY_TIMEOUT"):
            cli_runtime_config_from_env()

    def test_legacy_port_zero_raises(self, monkeypatch: pytest.MonkeyPatch):
        # CodeRabbit review on PR 2156: port=0 is meaningless and would
        # later become a socket failure; reject at boot.
        monkeypatch.setenv("GATEWAY_PORT", "0")
        with pytest.raises(ValueError, match="must be in 1..65535"):
            cli_runtime_config_from_env()

    def test_legacy_port_too_large_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GATEWAY_PORT", "70000")
        with pytest.raises(ValueError, match="must be in 1..65535"):
            cli_runtime_config_from_env()

    def test_almanak_port_negative_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_PORT", "-1")
        with pytest.raises(ValueError, match="must be in 1..65535"):
            cli_runtime_config_from_env()

    def test_legacy_timeout_zero_raises(self, monkeypatch: pytest.MonkeyPatch):
        # CodeRabbit review on PR 2156: timeout <= 0 collapses every
        # gRPC call into an immediate deadline failure; reject at boot.
        monkeypatch.setenv("GATEWAY_TIMEOUT", "0")
        with pytest.raises(ValueError, match="must be > 0"):
            cli_runtime_config_from_env()

    def test_almanak_timeout_negative_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_TIMEOUT", "-5.0")
        with pytest.raises(ValueError, match="must be > 0"):
            cli_runtime_config_from_env()


# =============================================================================
# anvil_port_for_chain — direct env helper for hot paths
# =============================================================================


class TestAnvilPortForChain:
    """Gemini review on PR 2156: the intent compiler called
    ``cli_runtime_config_from_env()`` on every chain RPC resolution,
    rebuilding a Pydantic model just to read a single env var. The
    direct helper is a cheap variant that preserves the dynamic env
    read (``managed.py`` mutates ``ANVIL_<CHAIN>_PORT`` at runtime).
    """

    def test_unset_returns_none(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("ANVIL_ARBITRUM_PORT", raising=False)
        assert anvil_port_for_chain("arbitrum") is None

    def test_whitespace_returns_none(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ANVIL_BASE_PORT", "   ")
        assert anvil_port_for_chain("base") is None

    def test_uppercases_chain_name(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ANVIL_OPTIMISM_PORT", "8545")
        assert anvil_port_for_chain("optimism") == 8545
        # Mixed case in caller — the helper still normalises.
        assert anvil_port_for_chain("Optimism") == 8545

    def test_malformed_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ANVIL_POLYGON_PORT", "not-an-int")
        with pytest.raises(ValueError, match="ANVIL_POLYGON_PORT"):
            anvil_port_for_chain("polygon")

    def test_observes_runtime_mutation(self, monkeypatch: pytest.MonkeyPatch):
        # ``managed.py`` writes the port AFTER the strategy process
        # starts. The compiler must see the post-mutation value.
        monkeypatch.delenv("ANVIL_BSC_PORT", raising=False)
        assert anvil_port_for_chain("bsc") is None
        monkeypatch.setenv("ANVIL_BSC_PORT", "8546")
        assert anvil_port_for_chain("bsc") == 8546


# =============================================================================
# Repr / extra-forbid
# =============================================================================


class TestModel:
    def test_legacy_gateway_auth_token_suppressed_in_repr(self):
        cfg = CliRuntimeConfig(legacy_gateway_auth_token="secret-token")
        assert "secret-token" not in repr(cfg)

    def test_extra_kwargs_forbidden(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="unknown_field"):
            CliRuntimeConfig(unknown_field="oops")  # type: ignore[call-arg]


# =============================================================================
# chain_rpc_url_from_env — ladder walk
# =============================================================================


class TestChainRpcLadder:
    def test_first_step_wins(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_ARBITRUM_RPC_URL", "https://prefixed-chain")
        monkeypatch.setenv("ARBITRUM_RPC_URL", "https://bare-chain")
        monkeypatch.setenv("ALMANAK_RPC_URL", "https://prefixed-generic")
        url, names = chain_rpc_url_from_env("arbitrum")
        assert url == "https://prefixed-chain"
        assert names == [
            "ALMANAK_ARBITRUM_RPC_URL",
            "ARBITRUM_RPC_URL",
            "ALMANAK_RPC_URL",
            "RPC_URL",
        ]

    def test_falls_through_to_bare_chain(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ARBITRUM_RPC_URL", "https://bare-chain")
        url, _ = chain_rpc_url_from_env("arbitrum")
        assert url == "https://bare-chain"

    def test_falls_through_to_generic_prefixed(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_RPC_URL", "https://generic-prefixed")
        url, _ = chain_rpc_url_from_env("arbitrum")
        assert url == "https://generic-prefixed"

    def test_falls_through_to_bare_generic(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RPC_URL", "https://bare-generic")
        url, _ = chain_rpc_url_from_env("arbitrum")
        assert url == "https://bare-generic"

    def test_missing_returns_none(self):
        url, _ = chain_rpc_url_from_env("arbitrum")
        assert url is None

    def test_chain_case_insensitive(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_ARBITRUM_RPC_URL", "https://x")
        url, _ = chain_rpc_url_from_env("arbitrum")
        assert url == "https://x"
        url, _ = chain_rpc_url_from_env("ARBITRUM")
        assert url == "https://x"


# =============================================================================
# gas_risk_override_presence + max_value_usd_override
# =============================================================================


class TestGasRiskOverrides:
    def test_no_overrides_when_unset(self):
        presence = gas_risk_override_presence()
        assert presence == {
            "max_gas_price_gwei": False,
            "max_gas_cost_native": False,
            "max_gas_cost_usd": False,
            "max_slippage_bps": False,
        }

    def test_prefixed_form_detected(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "100")
        presence = gas_risk_override_presence()
        assert presence["max_gas_price_gwei"] is True

    def test_legacy_unprefixed_form_detected(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MAX_SLIPPAGE_BPS", "50")
        presence = gas_risk_override_presence()
        assert presence["max_slippage_bps"] is True

    def test_max_value_usd_prefixed_wins(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALMANAK_MAX_VALUE_USD", "9999")
        monkeypatch.setenv("MAX_VALUE_USD", "1234")
        assert max_value_usd_override() == "9999"

    def test_max_value_usd_legacy_fallback(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MAX_VALUE_USD", "1234")
        assert max_value_usd_override() == "1234"

    def test_max_value_usd_missing_returns_none(self):
        assert max_value_usd_override() is None


# =============================================================================
# subprocess_env_with_overrides
# =============================================================================


class TestSubprocessEnv:
    def test_returns_fresh_dict(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FOO", "bar")
        env = subprocess_env_with_overrides({})
        env["FOO"] = "mutated"
        # Parent env stays clean.
        assert os.environ["FOO"] == "bar"

    def test_overrides_merged(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FOO", "bar")
        env = subprocess_env_with_overrides({"GATEWAY_HOST": "localhost", "GATEWAY_PORT": "50071"})
        assert env["FOO"] == "bar"
        assert env["GATEWAY_HOST"] == "localhost"
        assert env["GATEWAY_PORT"] == "50071"

    def test_overrides_win_over_parent(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GATEWAY_HOST", "old")
        env = subprocess_env_with_overrides({"GATEWAY_HOST": "new"})
        assert env["GATEWAY_HOST"] == "new"


# =============================================================================
# DEFAULT_ANVIL_PORT — sanity check the constant is what the legacy form had.
# =============================================================================


def test_default_anvil_port_matches_legacy_constant():
    assert DEFAULT_ANVIL_PORT == 8545
