"""Tests for ``almanak.config.connectors``.

Phase 5b of the config-service migration. These tests pin the contract
that :func:`connectors_config_from_env` is the single env reader for
every connector under ``almanak/framework/connectors/*``:

* Empty env → all secret fields are ``None`` and base URLs are the
  public production defaults.
* Each documented env var (and the bare-name / ``ALMANAK_*``-prefix
  fallback ladder for Polymarket) is honoured.
* Secret fields are suppressed in ``repr()`` / ``model_dump()`` output
  so a stray ``logger.info(repr(cfg))`` cannot leak credentials.
"""

from __future__ import annotations

import pytest

from almanak.config.connectors import (
    DEFAULT_DRIFT_DATA_API_BASE_URL,
    DEFAULT_METEORA_API_BASE_URL,
    DEFAULT_ORCA_API_BASE_URL,
    DEFAULT_RAYDIUM_API_BASE_URL,
    ConnectorsConfig,
    connectors_config_from_env,
)

# Every env var the factory reads. The list is kept explicit so a future
# field addition that forgets to wire up the scrub fails loudly here
# rather than as an order-dependent flake elsewhere.
_CONNECTOR_ENV_VARS: tuple[str, ...] = (
    "ENSO_API_KEY",
    "JUPITER_API_KEY",
    "LIFI_API_KEY",
    "KRAKEN_API_KEY",
    "KRAKEN_API_SECRET",
    "SOLANA_RPC_URL",
    "POLYMARKET_WALLET_ADDRESS",
    "ALMANAK_POLYMARKET_WALLET_ADDRESS",
    "POLYMARKET_PRIVATE_KEY",
    "ALMANAK_POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "ALMANAK_POLYMARKET_API_KEY",
    "POLYMARKET_SECRET",
    "ALMANAK_POLYMARKET_SECRET",
    "POLYMARKET_PASSPHRASE",
    "ALMANAK_POLYMARKET_PASSPHRASE",
    "POLYGON_RPC_URL",
    "POLYMARKET_CLOB_URL",
    "POLYMARKET_GAMMA_URL",
    "POLYMARKET_DATA_API_URL",
    "ALMANAK_SIGNER_SERVICE_URL",
    "ALMANAK_SIGNER_SERVICE_JWT",
    "DRIFT_DATA_API_BASE_URL",
    "METEORA_API_BASE_URL",
    "ORCA_API_BASE_URL",
    "RAYDIUM_API_BASE_URL",
)


@pytest.fixture(autouse=True)
def _scrub_connector_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Scrub every env var the factory reads.

    Without the scrub these tests are non-deterministic — a developer's
    ``.env`` (or a prior test that called ``setenv``) would silently
    populate a "default" assertion.
    """
    for name in _CONNECTOR_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# =============================================================================
# Defaults
# =============================================================================


class TestDefaults:
    def test_all_secrets_default_to_none(self):
        cfg = connectors_config_from_env()
        assert cfg.enso_api_key is None
        assert cfg.jupiter_api_key is None
        assert cfg.lifi_api_key is None
        assert cfg.kraken_api_key is None
        assert cfg.kraken_api_secret is None
        assert cfg.polymarket_wallet_address is None
        assert cfg.polymarket_private_key is None
        assert cfg.polymarket_api_key is None
        assert cfg.polymarket_secret is None
        assert cfg.polymarket_passphrase is None
        assert cfg.polymarket_signer_service_url is None
        assert cfg.polymarket_signer_service_jwt is None
        assert cfg.polygon_rpc_url is None
        assert cfg.polymarket_clob_url is None
        assert cfg.polymarket_gamma_url is None
        assert cfg.polymarket_data_api_url is None
        assert cfg.solana_rpc_url is None

    def test_base_urls_default_to_public_production(self):
        cfg = connectors_config_from_env()
        assert cfg.drift_data_api_base_url == DEFAULT_DRIFT_DATA_API_BASE_URL
        assert cfg.meteora_api_base_url == DEFAULT_METEORA_API_BASE_URL
        assert cfg.orca_api_base_url == DEFAULT_ORCA_API_BASE_URL
        assert cfg.raydium_api_base_url == DEFAULT_RAYDIUM_API_BASE_URL


# =============================================================================
# Env overrides — one test per field for a paper trail.
# =============================================================================


class TestEnvOverrides:
    def test_enso_api_key(self, monkeypatch):
        monkeypatch.setenv("ENSO_API_KEY", "enso-secret")
        assert connectors_config_from_env().enso_api_key == "enso-secret"

    def test_jupiter_api_key(self, monkeypatch):
        monkeypatch.setenv("JUPITER_API_KEY", "jup-secret")
        assert connectors_config_from_env().jupiter_api_key == "jup-secret"

    def test_lifi_api_key(self, monkeypatch):
        monkeypatch.setenv("LIFI_API_KEY", "lifi-secret")
        assert connectors_config_from_env().lifi_api_key == "lifi-secret"

    def test_kraken_credentials(self, monkeypatch):
        monkeypatch.setenv("KRAKEN_API_KEY", "k-key")
        monkeypatch.setenv("KRAKEN_API_SECRET", "k-secret")
        cfg = connectors_config_from_env()
        assert cfg.kraken_api_key == "k-key"
        assert cfg.kraken_api_secret == "k-secret"

    def test_solana_rpc_url(self, monkeypatch):
        monkeypatch.setenv("SOLANA_RPC_URL", "https://my-solana.example/rpc")
        assert connectors_config_from_env().solana_rpc_url == "https://my-solana.example/rpc"

    def test_drift_data_api_base_url(self, monkeypatch):
        monkeypatch.setenv("DRIFT_DATA_API_BASE_URL", "https://drift.test")
        assert connectors_config_from_env().drift_data_api_base_url == "https://drift.test"

    def test_meteora_api_base_url(self, monkeypatch):
        monkeypatch.setenv("METEORA_API_BASE_URL", "https://meteora.test")
        assert connectors_config_from_env().meteora_api_base_url == "https://meteora.test"

    def test_orca_api_base_url(self, monkeypatch):
        monkeypatch.setenv("ORCA_API_BASE_URL", "https://orca.test")
        assert connectors_config_from_env().orca_api_base_url == "https://orca.test"

    def test_raydium_api_base_url(self, monkeypatch):
        monkeypatch.setenv("RAYDIUM_API_BASE_URL", "https://raydium.test")
        assert connectors_config_from_env().raydium_api_base_url == "https://raydium.test"


# =============================================================================
# Polymarket — the bare-name vs ALMANAK_-prefixed alias ladder.
# =============================================================================


class TestPolymarketFallbackLadder:
    def test_bare_name_wins_when_set(self, monkeypatch):
        monkeypatch.setenv("POLYMARKET_API_KEY", "bare")
        monkeypatch.setenv("ALMANAK_POLYMARKET_API_KEY", "almanak-prefixed")
        assert connectors_config_from_env().polymarket_api_key == "bare"

    def test_almanak_prefix_used_when_bare_unset(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_POLYMARKET_API_KEY", "almanak-prefixed")
        assert connectors_config_from_env().polymarket_api_key == "almanak-prefixed"

    def test_wallet_address_alias_ladder(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_POLYMARKET_WALLET_ADDRESS", "0xfromalmanak")
        assert connectors_config_from_env().polymarket_wallet_address == "0xfromalmanak"

    def test_private_key_alias_ladder(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_POLYMARKET_PRIVATE_KEY", "0xpkfromalmanak")
        assert connectors_config_from_env().polymarket_private_key == "0xpkfromalmanak"

    def test_secret_alias_ladder(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_POLYMARKET_SECRET", "secret-from-almanak")
        assert connectors_config_from_env().polymarket_secret == "secret-from-almanak"

    def test_passphrase_alias_ladder(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_POLYMARKET_PASSPHRASE", "pass-from-almanak")
        assert connectors_config_from_env().polymarket_passphrase == "pass-from-almanak"

    def test_polygon_rpc_url(self, monkeypatch):
        monkeypatch.setenv("POLYGON_RPC_URL", "https://polygon-rpc.test")
        assert connectors_config_from_env().polygon_rpc_url == "https://polygon-rpc.test"

    def test_signer_service(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_URL", "https://signer.test")
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_JWT", "jwt-token")
        cfg = connectors_config_from_env()
        assert cfg.polymarket_signer_service_url == "https://signer.test"
        assert cfg.polymarket_signer_service_jwt == "jwt-token"

    def test_clob_url_overrides(self, monkeypatch):
        monkeypatch.setenv("POLYMARKET_CLOB_URL", "https://clob.test")
        monkeypatch.setenv("POLYMARKET_GAMMA_URL", "https://gamma.test")
        monkeypatch.setenv("POLYMARKET_DATA_API_URL", "https://data.test")
        cfg = connectors_config_from_env()
        assert cfg.polymarket_clob_url == "https://clob.test"
        assert cfg.polymarket_gamma_url == "https://gamma.test"
        assert cfg.polymarket_data_api_url == "https://data.test"


# =============================================================================
# Secret repr suppression — credentials must never reach a log line.
# =============================================================================


class TestSecretReprSuppression:
    """Every ``repr=False`` field is excluded from ``__repr__`` output.

    A stray ``logger.info(f"config={cfg!r}")`` is the most likely
    accident; the model has to make sure that line never leaks the
    secret string. ``model_dump(mode="python")`` is the other common
    serialisation path; we check it includes the field but the test is
    primarily about ``repr``.
    """

    @pytest.fixture
    def populated(self) -> ConnectorsConfig:
        return ConnectorsConfig(
            enso_api_key="enso-secret-VALUE",
            jupiter_api_key="jup-secret-VALUE",
            lifi_api_key="lifi-secret-VALUE",
            kraken_api_key="k-key-VALUE",
            kraken_api_secret="k-secret-VALUE",
            polymarket_wallet_address="0xWALLET-VALUE",
            polymarket_private_key="0xPK-VALUE",
            polymarket_api_key="poly-api-VALUE",
            polymarket_secret="poly-sec-VALUE",
            polymarket_passphrase="poly-pass-VALUE",
            polymarket_signer_service_url="https://signer.test",
            polymarket_signer_service_jwt="jwt-VALUE",
            polygon_rpc_url="https://polygon.test",
            polymarket_clob_url="https://clob.test",
            polymarket_gamma_url="https://gamma.test",
            polymarket_data_api_url="https://data.test",
            solana_rpc_url="https://solana.test",
        )

    def test_repr_suppresses_secret_fields(self, populated: ConnectorsConfig):
        text = repr(populated)
        secret_values = [
            "enso-secret-VALUE",
            "jup-secret-VALUE",
            "lifi-secret-VALUE",
            "k-key-VALUE",
            "k-secret-VALUE",
            "0xWALLET-VALUE",
            "0xPK-VALUE",
            "poly-api-VALUE",
            "poly-sec-VALUE",
            "poly-pass-VALUE",
            "jwt-VALUE",
        ]
        for s in secret_values:
            assert s not in text, f"Secret {s!r} leaked into repr: {text}"

    def test_model_dump_still_returns_secrets(self, populated: ConnectorsConfig):
        # ``repr=False`` only affects the ``__repr__`` path — explicit
        # ``model_dump()`` is the API for downstream serialisation and
        # must still yield the actual values (the connector reads them
        # this way at construction).
        dumped = populated.model_dump()
        assert dumped["enso_api_key"] == "enso-secret-VALUE"
        assert dumped["polymarket_private_key"] == "0xPK-VALUE"


# =============================================================================
# Forbid extra — typo at the service boundary fails loud.
# =============================================================================


class TestForbidExtra:
    def test_unknown_field_rejected(self):
        with pytest.raises(ValueError):
            ConnectorsConfig(unknown_secret="oops")  # type: ignore[call-arg]
