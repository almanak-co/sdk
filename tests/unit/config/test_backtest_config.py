"""Tests for ``almanak.config.backtest``.

Phase 5c of the config-service migration. These tests pin the contract
that :func:`backtest_config_from_env` is the single env reader for the
backtesting cluster — paper-trading and the PnL providers under
``almanak/framework/backtesting/*``:

* Empty env → every secret is ``None``, the dict-shaped fields are
  empty, and ``ssl_cert_file`` falls back to certifi (when available).
* Each documented env var is honoured.
* Secret fields are suppressed in ``repr()`` output so a stray
  ``logger.info(repr(cfg))`` cannot leak credentials.
* The dict-shaped ``archive_rpc_urls`` and ``gas_api.api_keys`` fields
  carry the per-chain mapping in lowercase keys.
* :func:`apply_ssl_cert_file` is idempotent when the env already
  points at a usable cert and respects ``ssl_cert_file=None``.
"""

from __future__ import annotations

import os
import sys
import types

import pytest

import almanak.config.backtest as backtest_config
from almanak.config.backtest import (
    DEFAULT_ARCHIVE_RPC_CHAINS,
    DEFAULT_GAS_API_KEY_ENV_VARS,
    BacktestConfig,
    GasApiConfig,
    apply_ssl_cert_file,
    backtest_config_from_env,
)

# Every env var the factory reads. Listed explicitly so a future field
# addition that forgets to wire up the scrub fails loudly here rather
# than as an order-dependent flake elsewhere.
_BACKTEST_ENV_VARS: tuple[str, ...] = (
    "COINGECKO_API_KEY",
    "THEGRAPH_API_KEY",
    # ARCHIVE_RPC_URL_<CHAIN> cluster.
    "ARCHIVE_RPC_URL_ETHEREUM",
    "ARCHIVE_RPC_URL_ARBITRUM",
    "ARCHIVE_RPC_URL_BASE",
    "ARCHIVE_RPC_URL_OPTIMISM",
    "ARCHIVE_RPC_URL_POLYGON",
    "ARCHIVE_RPC_URL_AVALANCHE",
    "ARCHIVE_RPC_URL_BSC",
    # Gas API keys — the full Etherscan-family ladder.
    "ETHERSCAN_API_KEY",
    "ARBISCAN_API_KEY",
    "OPTIMISTIC_ETHERSCAN_API_KEY",
    "BASESCAN_API_KEY",
    "POLYGONSCAN_API_KEY",
    "BSCSCAN_API_KEY",
    "SNOWTRACE_API_KEY",
    # SSL cert hint.
    "SSL_CERT_FILE",
)


@pytest.fixture(autouse=True)
def _scrub_backtest_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Scrub every env var the factory reads.

    Without the scrub these tests are non-deterministic — a developer's
    ``.env`` (or a prior test that called ``setenv``) would silently
    populate a "default" assertion.
    """
    for name in _BACKTEST_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# =============================================================================
# Defaults
# =============================================================================


class TestDefaults:
    def test_secrets_default_to_none(self):
        cfg = backtest_config_from_env()
        assert cfg.coingecko_api_key is None
        assert cfg.thegraph_api_key is None

    def test_archive_rpc_urls_empty_when_no_env(self):
        cfg = backtest_config_from_env()
        assert cfg.archive_rpc_urls == {}

    def test_gas_api_empty_when_no_env(self):
        cfg = backtest_config_from_env()
        assert cfg.gas_api.api_keys == {}

    def test_default_archive_rpc_chains_match_legacy(self):
        # The chain list mirrors the union of chains the legacy provider
        # modules covered. A change here would silently break the
        # ARCHIVE_RPC_URL_<CHAIN> lookup; the assertion is a paper trail.
        assert set(DEFAULT_ARCHIVE_RPC_CHAINS) == {
            "ethereum",
            "arbitrum",
            "base",
            "optimism",
            "polygon",
            "avalanche",
        }

    def test_default_gas_api_env_vars_match_legacy(self):
        # Same paper-trail assertion for the gas-API env-var ladder.
        assert DEFAULT_GAS_API_KEY_ENV_VARS == {
            "ethereum": "ETHERSCAN_API_KEY",
            "arbitrum": "ARBISCAN_API_KEY",
            "optimism": "OPTIMISTIC_ETHERSCAN_API_KEY",
            "base": "BASESCAN_API_KEY",
            "polygon": "POLYGONSCAN_API_KEY",
            "bsc": "BSCSCAN_API_KEY",
            "avalanche": "SNOWTRACE_API_KEY",
        }


# =============================================================================
# Env overrides — one test per field for a paper trail.
# =============================================================================


class TestEnvOverrides:
    def test_coingecko_api_key(self, monkeypatch):
        monkeypatch.setenv("COINGECKO_API_KEY", "cg-secret")
        assert backtest_config_from_env().coingecko_api_key == "cg-secret"

    def test_thegraph_api_key(self, monkeypatch):
        monkeypatch.setenv("THEGRAPH_API_KEY", "tg-secret")
        assert backtest_config_from_env().thegraph_api_key == "tg-secret"

    def test_empty_string_treated_as_unset(self, monkeypatch):
        # Mirrors the legacy ``os.environ.get("X", "")`` → ``not key``
        # guard. Setting the var to an empty string must not produce an
        # empty-string secret on the typed config.
        monkeypatch.setenv("COINGECKO_API_KEY", "")
        assert backtest_config_from_env().coingecko_api_key is None


class TestArchiveRpcUrls:
    """ARCHIVE_RPC_URL_<CHAIN> cluster → ``archive_rpc_urls`` dict."""

    def test_single_chain_populated(self, monkeypatch):
        monkeypatch.setenv("ARCHIVE_RPC_URL_ETHEREUM", "https://eth-archive.example.com")
        cfg = backtest_config_from_env()
        assert cfg.archive_rpc_urls == {"ethereum": "https://eth-archive.example.com"}

    def test_multiple_chains_populated(self, monkeypatch):
        monkeypatch.setenv("ARCHIVE_RPC_URL_ETHEREUM", "https://eth.example")
        monkeypatch.setenv("ARCHIVE_RPC_URL_ARBITRUM", "https://arb.example")
        cfg = backtest_config_from_env()
        assert cfg.archive_rpc_urls == {
            "ethereum": "https://eth.example",
            "arbitrum": "https://arb.example",
        }

    def test_empty_value_skipped(self, monkeypatch):
        # The legacy lookup treated ``""`` as "not configured"; the
        # typed factory must preserve that — empty values are dropped
        # from the dict entirely.
        monkeypatch.setenv("ARCHIVE_RPC_URL_ETHEREUM", "")
        cfg = backtest_config_from_env()
        assert "ethereum" not in cfg.archive_rpc_urls

    def test_keys_are_lowercase(self, monkeypatch):
        # The env var is uppercase; the dict key is the lowercase chain
        # name so providers can do ``cfg.archive_rpc_urls.get(self._chain.lower())``.
        monkeypatch.setenv("ARCHIVE_RPC_URL_OPTIMISM", "https://op.example")
        cfg = backtest_config_from_env()
        assert "optimism" in cfg.archive_rpc_urls
        assert "OPTIMISM" not in cfg.archive_rpc_urls

    def test_custom_chain_list_via_factory_kwarg(self, monkeypatch):
        # Defaults skip BSC (no archive provider covers it) — but the
        # factory kwarg lets callers extend the list.
        monkeypatch.setenv("ARCHIVE_RPC_URL_BSC", "https://bsc.example")
        cfg = backtest_config_from_env(archive_rpc_chains=("bsc",))
        assert cfg.archive_rpc_urls == {"bsc": "https://bsc.example"}


class TestGasApiKeys:
    """Etherscan-family API keys → ``gas_api.api_keys`` dict."""

    def test_ethereum_key_populated(self, monkeypatch):
        monkeypatch.setenv("ETHERSCAN_API_KEY", "etherscan-key")
        cfg = backtest_config_from_env()
        assert cfg.gas_api.api_keys == {"ethereum": "etherscan-key"}

    def test_full_chain_ladder(self, monkeypatch):
        monkeypatch.setenv("ETHERSCAN_API_KEY", "k-eth")
        monkeypatch.setenv("ARBISCAN_API_KEY", "k-arb")
        monkeypatch.setenv("OPTIMISTIC_ETHERSCAN_API_KEY", "k-op")
        monkeypatch.setenv("BASESCAN_API_KEY", "k-base")
        monkeypatch.setenv("POLYGONSCAN_API_KEY", "k-poly")
        monkeypatch.setenv("BSCSCAN_API_KEY", "k-bsc")
        monkeypatch.setenv("SNOWTRACE_API_KEY", "k-avax")
        cfg = backtest_config_from_env()
        assert cfg.gas_api.api_keys == {
            "ethereum": "k-eth",
            "arbitrum": "k-arb",
            "optimism": "k-op",
            "base": "k-base",
            "polygon": "k-poly",
            "bsc": "k-bsc",
            "avalanche": "k-avax",
        }

    def test_empty_keys_skipped(self, monkeypatch):
        # Same ``""`` → "not configured" semantics as the archive RPC cluster.
        monkeypatch.setenv("ETHERSCAN_API_KEY", "")
        cfg = backtest_config_from_env()
        assert cfg.gas_api.api_keys == {}

    def test_custom_env_var_map(self, monkeypatch):
        # Factory kwarg lets callers override the chain → env-var name
        # map (used by tests that want to exercise an unusual provider).
        monkeypatch.setenv("MY_CUSTOM_KEY", "custom")
        cfg = backtest_config_from_env(gas_api_key_env_vars={"chain_x": "MY_CUSTOM_KEY"})
        assert cfg.gas_api.api_keys == {"chain_x": "custom"}


# =============================================================================
# SSL cert resolution
# =============================================================================


class TestSslCertFile:
    def test_explicit_env_value_wins_when_path_exists(self, monkeypatch, tmp_path):
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("dummy-cert")
        monkeypatch.setenv("SSL_CERT_FILE", str(cert_file))
        cfg = backtest_config_from_env()
        assert cfg.ssl_cert_file == str(cert_file)

    def test_explicit_env_value_ignored_when_file_missing(self, monkeypatch):
        # Mirrors the legacy ``if "SSL_CERT_FILE" not in os.environ`` plus
        # ``os.path.exists`` ladder. A pointer at a non-existent file
        # falls back to certifi / OS paths instead of accepting it.
        monkeypatch.setenv("SSL_CERT_FILE", "/nonexistent/path.pem")
        cfg = backtest_config_from_env()
        assert cfg.ssl_cert_file != "/nonexistent/path.pem"

    def test_falls_back_to_certifi_when_env_unset(self, monkeypatch):
        # certifi is a transitive dep here. If the host has it (CI does),
        # the resolver should pick it up before walking OS paths.
        try:
            import certifi
        except ImportError:
            pytest.skip("certifi not installed")

        cfg = backtest_config_from_env()
        # Don't pin the exact path — just confirm certifi's bundle was
        # picked. ``cfg.ssl_cert_file`` may be the certifi path or one
        # of the OS fallbacks if certifi's path was unreadable.
        assert cfg.ssl_cert_file is not None
        assert os.path.exists(cfg.ssl_cert_file)
        # Either certifi's path or a legacy OS path; both are accepted.
        assert cfg.ssl_cert_file in (certifi.where(), "/private/etc/ssl/cert.pem", "/etc/ssl/cert.pem")

    def test_falls_back_to_os_path_when_certifi_missing(self, monkeypatch, tmp_path):
        fallback = tmp_path / "fallback.pem"
        fallback.write_text("fallback")
        monkeypatch.setitem(sys.modules, "certifi", None)
        monkeypatch.setattr(
            backtest_config,
            "_SSL_CERT_FALLBACK_PATHS",
            (str(tmp_path / "missing.pem"), str(fallback)),
        )

        cfg = backtest_config_from_env()

        assert cfg.ssl_cert_file == str(fallback)

    def test_falls_back_to_os_path_when_certifi_path_missing(self, monkeypatch, tmp_path):
        fallback = tmp_path / "fallback.pem"
        fallback.write_text("fallback")
        fake_certifi = types.ModuleType("certifi")
        fake_certifi.where = lambda: str(tmp_path / "missing-certifi.pem")  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "certifi", fake_certifi)
        monkeypatch.setattr(backtest_config, "_SSL_CERT_FALLBACK_PATHS", (str(fallback),))

        cfg = backtest_config_from_env()

        assert cfg.ssl_cert_file == str(fallback)

    def test_returns_none_when_no_ssl_candidate_exists(self, monkeypatch, tmp_path):
        monkeypatch.setitem(sys.modules, "certifi", None)
        monkeypatch.setattr(backtest_config, "_SSL_CERT_FALLBACK_PATHS", (str(tmp_path / "missing.pem"),))

        cfg = backtest_config_from_env()

        assert cfg.ssl_cert_file is None


class TestApplySslCertFile:
    """Tests for the boundary helper that writes ``os.environ["SSL_CERT_FILE"]``.

    ``apply_ssl_cert_file`` is one of the few sites that mutates env directly
    (paper-trading needs the value to survive ``multiprocessing.Process.start()``
    on macOS — see the helper's docstring). Tests need an explicit save/restore
    because ``monkeypatch.delenv`` does NOT track teardown for vars that started
    unset; a raw write inside the test body would leak past teardown and break
    later tests that import ``httpx`` (which reads the env at client-creation
    time and fails SSL validation when the path is stale).

    The autouse fixture below captures the var once per test and restores it
    deterministically in ``finally``.
    """

    @pytest.fixture(autouse=True)
    def _restore_ssl_cert_file_env(self):
        original = os.environ.get("SSL_CERT_FILE")
        try:
            yield
        finally:
            if original is None:
                os.environ.pop("SSL_CERT_FILE", None)
            else:
                os.environ["SSL_CERT_FILE"] = original

    def test_writes_env_when_unset(self, monkeypatch, tmp_path):
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("dummy")
        cfg = BacktestConfig(ssl_cert_file=str(cert_file))
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        apply_ssl_cert_file(cfg)
        assert os.environ.get("SSL_CERT_FILE") == str(cert_file)

    def test_preserves_existing_env_when_path_exists(self, monkeypatch, tmp_path):
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("dummy")
        existing = tmp_path / "existing.pem"
        existing.write_text("existing")
        monkeypatch.setenv("SSL_CERT_FILE", str(existing))
        cfg = BacktestConfig(ssl_cert_file=str(cert_file))
        apply_ssl_cert_file(cfg)
        # Operator override survives.
        assert os.environ["SSL_CERT_FILE"] == str(existing)

    def test_overrides_stale_existing_env(self, monkeypatch, tmp_path):
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("dummy")
        monkeypatch.setenv("SSL_CERT_FILE", "/nonexistent.pem")
        cfg = BacktestConfig(ssl_cert_file=str(cert_file))
        apply_ssl_cert_file(cfg)
        # Legacy code's ``not in os.environ`` guard would have left the
        # bad value in place; the typed helper checks ``os.path.exists``
        # so a stale env value gets replaced by a usable one.
        assert os.environ["SSL_CERT_FILE"] == str(cert_file)

    def test_noop_when_cfg_has_none(self, monkeypatch):
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        cfg = BacktestConfig(ssl_cert_file=None)
        apply_ssl_cert_file(cfg)
        assert "SSL_CERT_FILE" not in os.environ

    def test_noop_when_resolved_path_stale(self, monkeypatch, tmp_path):
        # If the resolved path goes away between construction and apply
        # (e.g. user uninstalled certifi), the helper must fall through
        # silently rather than writing a bad value.
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        cfg = BacktestConfig(ssl_cert_file=str(tmp_path / "missing.pem"))
        apply_ssl_cert_file(cfg)
        assert "SSL_CERT_FILE" not in os.environ


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
    def populated(self) -> BacktestConfig:
        return BacktestConfig(
            coingecko_api_key="cg-VALUE",
            thegraph_api_key="tg-VALUE",
            archive_rpc_urls={"ethereum": "https://eth-archive-VALUE.example"},
            gas_api=GasApiConfig(api_keys={"ethereum": "etherscan-VALUE"}),
            ssl_cert_file="/path/to/cert.pem",
        )

    def test_repr_suppresses_secret_fields(self, populated: BacktestConfig):
        text = repr(populated)
        secret_values = [
            "cg-VALUE",
            "tg-VALUE",
            "https://eth-archive-VALUE.example",
            "etherscan-VALUE",
        ]
        for s in secret_values:
            assert s not in text, f"Secret {s!r} leaked into repr: {text}"

    def test_model_dump_still_returns_secrets(self, populated: BacktestConfig):
        # ``repr=False`` only affects the ``__repr__`` path — explicit
        # ``model_dump()`` is the API for downstream serialisation and
        # must still yield the actual values (the consumer reads them
        # this way at construction).
        dumped = populated.model_dump()
        assert dumped["coingecko_api_key"] == "cg-VALUE"
        assert dumped["thegraph_api_key"] == "tg-VALUE"
        assert dumped["archive_rpc_urls"]["ethereum"] == "https://eth-archive-VALUE.example"
        assert dumped["gas_api"]["api_keys"]["ethereum"] == "etherscan-VALUE"


# =============================================================================
# Forbid extra — typo at the service boundary fails loud.
# =============================================================================


class TestForbidExtra:
    def test_unknown_field_rejected_on_root_model(self):
        with pytest.raises(ValueError):
            BacktestConfig(unknown_secret="oops")  # type: ignore[call-arg]

    def test_unknown_field_rejected_on_gas_api(self):
        with pytest.raises(ValueError):
            GasApiConfig(extra_field="oops")  # type: ignore[call-arg]
