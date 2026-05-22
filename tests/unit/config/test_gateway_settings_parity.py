"""Parity test: ``GatewayConfig`` must equal ``GatewaySettings`` field-by-field.

Phase 0 shipped ``GatewayConfig`` as a literal alias for ``GatewaySettings``
so this test trivially passed. Phase 1 deleted the in-class
``_fallback_env_vars`` validator and the polymarket resolvers; both classes
are still equal at construction time because they are still the same class,
but neither now applies the unprefixed-fallback ladders by itself —
:func:`almanak.config.env.gateway_config_from_env` does. The cutover
validation lives in ``test_env_fallbacks.py``; this file keeps the
trivial-equality contract and adds one Phase 1-specific assertion that
``gateway_config_from_env`` actually applies the unprefixed fallbacks.

Each scenario constructs both classes from a controlled env corpus and
asserts ``model_dump()`` equality. Using ``monkeypatch.setenv`` /
``monkeypatch.delenv`` (never raw ``os.environ``) keeps the suite
deterministic and immune to interpreter-leak bugs.
"""

from __future__ import annotations

import pytest

from almanak.config import GatewayConfig, HostedConfig, LocalConfig, load_config
from almanak.config.env import gateway_config_from_env
from almanak.gateway.core.settings import GatewaySettings


def _dump(model: object) -> dict[str, object]:
    """Stable model_dump() shape for parity comparisons."""
    return model.model_dump()  # type: ignore[union-attr]


def test_parity_empty_env(gateway_env_scrub: pytest.MonkeyPatch) -> None:
    """With every gateway-relevant env var unset, the two classes agree."""
    settings = GatewaySettings()
    config = GatewayConfig()
    assert _dump(settings) == _dump(config)


@pytest.mark.parametrize(
    ("env_var", "value"),
    [
        ("ALMANAK_GATEWAY_GRPC_PORT", "50071"),
        ("ALMANAK_GATEWAY_NETWORK", "anvil"),
        ("ALMANAK_GATEWAY_CHAINS", "arbitrum,base"),
    ],
)
def test_parity_prefixed_env(gateway_env_scrub: pytest.MonkeyPatch, env_var: str, value: str) -> None:
    """Prefixed env vars resolve identically through both classes."""
    gateway_env_scrub.setenv(env_var, value)
    settings = GatewaySettings()
    config = GatewayConfig()
    assert _dump(settings) == _dump(config)


def test_parity_prefixed_env_combined(
    gateway_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Multiple prefixed env vars combine the same way in both classes."""
    gateway_env_scrub.setenv("ALMANAK_GATEWAY_GRPC_PORT", "50071")
    gateway_env_scrub.setenv("ALMANAK_GATEWAY_NETWORK", "anvil")
    gateway_env_scrub.setenv("ALMANAK_GATEWAY_CHAINS", "arbitrum,base")
    settings = GatewaySettings()
    config = GatewayConfig()
    dumped = _dump(settings)
    assert dumped == _dump(config)
    # Sanity: the env vars actually changed the field values.
    assert dumped["grpc_port"] == 50071
    assert dumped["network"] == "anvil"
    assert dumped["chains"] == ["arbitrum", "base"]


def test_parity_unprefixed_fallback(
    gateway_env_scrub: pytest.MonkeyPatch,
) -> None:
    """``ALMANAK_PRIVATE_KEY`` (no prefix) is now applied by the service boundary.

    Phase 0: trivially passed because ``GatewayConfig is GatewaySettings``
    *and* the in-class validator applied the fallback.
    Phase 1: ``GatewaySettings()`` no longer reads unprefixed ``ALMANAK_*``;
    the resolution moved to :func:`gateway_config_from_env`. The two
    siblings still parity-match at construction time (same class, no env
    fallbacks applied), and the *new* assertion below locks in that the
    boundary helper now owns the read.
    """
    private_key = "0x" + "ab" * 32
    gateway_env_scrub.setenv("ALMANAK_PRIVATE_KEY", private_key)
    settings = GatewaySettings()
    config = GatewayConfig()
    assert _dump(settings) == _dump(config)
    # Phase 1 contract: the constructors no longer hydrate the unprefixed
    # ``ALMANAK_PRIVATE_KEY``.  The ladder must run via the service helper.
    assert settings.private_key is None
    assert config.private_key is None
    full = gateway_config_from_env()
    assert full.private_key == private_key


def test_gateway_config_from_env_applies_fallbacks(
    gateway_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Phase 1 contract: ``gateway_config_from_env()`` applies unprefixed fallbacks.

    The bare ``GatewayConfig()`` / ``GatewaySettings()`` constructors only
    read ``ALMANAK_GATEWAY_*``-prefixed env (pydantic-settings); the
    unprefixed-and-bare-name fallbacks live at the service boundary.
    """
    private_key = "0x" + "ab" * 32
    gateway_env_scrub.setenv("ALMANAK_PRIVATE_KEY", private_key)
    bare = GatewayConfig()
    full = gateway_config_from_env()
    assert bare.private_key is None
    assert full.private_key == private_key


def test_load_config_returns_local_when_hosted_flag_unset(
    gateway_env_scrub: pytest.MonkeyPatch,
) -> None:
    """``load_config()`` picks ``LocalConfig`` in the local-mode (default)."""
    config = load_config()
    assert isinstance(config, LocalConfig)
    assert not isinstance(config, HostedConfig)


def test_load_config_returns_hosted_when_hosted_flag_set(
    gateway_env_scrub: pytest.MonkeyPatch,
) -> None:
    """``load_config()`` picks ``HostedConfig`` in hosted mode."""
    gateway_env_scrub.setenv("ALMANAK_IS_HOSTED", "true")
    gateway_env_scrub.setenv("ALMANAK_DEPLOYMENT_ID", "test-agent")
    config = load_config()
    assert isinstance(config, HostedConfig)
    # Mirror the inverse test — guard against a future subclass relationship
    # between LocalConfig and HostedConfig silently passing this assertion.
    assert not isinstance(config, LocalConfig)


def test_config_factory_returns_local(
    config_factory,  # type: ignore[no-untyped-def]
) -> None:
    """Smoke check the shared fixture against the public surface."""
    config = config_factory(mode="local", gateway={"grpc_port": 50099})
    assert isinstance(config, LocalConfig)
    assert config.gateway.grpc_port == 50099


def test_config_factory_returns_hosted(
    config_factory,  # type: ignore[no-untyped-def]
) -> None:
    config = config_factory(mode="hosted", gateway={"network": "anvil"})
    assert isinstance(config, HostedConfig)
    assert config.gateway.network == "anvil"


# =============================================================================
# Timeout > 0 validator (CodeRabbit review on PR 2156).
# =============================================================================


def test_timeout_zero_rejected(gateway_env_scrub: pytest.MonkeyPatch) -> None:
    """``timeout=0`` would collapse every gRPC call into an immediate
    deadline failure; reject at the model boundary so the typo
    surfaces at boot rather than after first request."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="timeout must be > 0"):
        GatewaySettings(timeout=0)


def test_timeout_negative_rejected(gateway_env_scrub: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="timeout must be > 0"):
        GatewaySettings(timeout=-5.0)


def test_timeout_positive_accepted(gateway_env_scrub: pytest.MonkeyPatch) -> None:
    settings = GatewaySettings(timeout=15.0)
    assert settings.timeout == 15.0


def test_timeout_default_passes_validator(gateway_env_scrub: pytest.MonkeyPatch) -> None:
    """The hard-coded 30.0 default must survive the validator unchanged."""
    settings = GatewaySettings()
    assert settings.timeout == 30.0


def test_timeout_zero_via_env_rejected(gateway_env_scrub: pytest.MonkeyPatch) -> None:
    """Env-derived ``ALMANAK_GATEWAY_TIMEOUT=0`` also goes through the validator."""
    from pydantic import ValidationError

    gateway_env_scrub.setenv("ALMANAK_GATEWAY_TIMEOUT", "0")
    with pytest.raises(ValidationError, match="timeout must be > 0"):
        GatewaySettings()
