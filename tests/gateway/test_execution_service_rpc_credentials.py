"""VIB-4429: gateway-side public-RPC-fallback alerting.

The gateway-side IntentCompiler built by `_get_compiler` has no gateway_client
to defer to (it IS the gateway), so it resolves RPC URLs directly. When no
credentialed provider (Alchemy / Tenderly / chain-specific or generic RPC URL)
is configured, resolution falls through to free public RPC — a real,
rate-limited egress from the gateway pod.

`_warn_if_resolved_to_public_rpc` inspects the *resolved* URL and emits one
ERROR per chain per gateway lifetime so Infra can alert on it. Inspecting the
resolved URL (rather than re-deriving the provider priority list) means every
credentialed provider path is recognised without drift risk. These tests pin
both the direct helper behaviour and the env-var resolution paths.
"""

import logging

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.utils.rpc_provider import PUBLIC_RPC_URLS

# Every env var that could give a chain a credentialed (non-public) provider.
ALCHEMY_ENV_VARS = ("ALCHEMY_API_KEY", "ALMANAK_GATEWAY_ALCHEMY_API_KEY")
ARBITRUM_URL_VARS = ("ARBITRUM_RPC_URL", "ALMANAK_ARBITRUM_RPC_URL", "RPC_URL", "ALMANAK_RPC_URL")
ARBITRUM_TENDERLY_VAR = "TENDERLY_API_KEY_ARBITRUM"
ARBITRUM_PUBLIC_URL = PUBLIC_RPC_URLS["arbitrum"]


@pytest.fixture
def no_rpc_credentials(monkeypatch):
    """Hosted mode with no provider credentials of any kind for arbitrum."""
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "test-agent")
    for var in (*ALCHEMY_ENV_VARS, *ARBITRUM_URL_VARS, ARBITRUM_TENDERLY_VAR):
        monkeypatch.delenv(var, raising=False)


def _service() -> ExecutionServiceServicer:
    return ExecutionServiceServicer(GatewaySettings())


def _resolve_and_warn(service: ExecutionServiceServicer, chain: str) -> None:
    """Mirror _get_compiler's resolve-then-warn sequence."""
    rpc_url = get_rpc_url(chain, network="mainnet")
    service._warn_if_resolved_to_public_rpc(chain, rpc_url)


def _public_rpc_errors(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if "free public RPC" in r.message]


# --------------------------------------------------------------------------
# Direct helper behaviour (no env resolution)
# --------------------------------------------------------------------------


def test_hosted_public_url_logs_error(no_rpc_credentials, caplog):
    """Hosted gateway + a resolved public RPC URL → ERROR."""
    service = _service()
    with caplog.at_level(logging.ERROR):
        service._warn_if_resolved_to_public_rpc("arbitrum", ARBITRUM_PUBLIC_URL)

    errors = _public_rpc_errors(caplog)
    assert len(errors) == 1
    assert errors[0].levelno == logging.ERROR


def test_hosted_non_public_url_does_not_log(no_rpc_credentials, caplog):
    """A credentialed (non-public) resolved URL → no ERROR."""
    service = _service()
    with caplog.at_level(logging.ERROR):
        service._warn_if_resolved_to_public_rpc("arbitrum", "https://arb-mainnet.g.alchemy.com/v2/key")

    assert not _public_rpc_errors(caplog)


def test_error_logged_once_per_chain(no_rpc_credentials, caplog):
    """Repeated public-RPC resolutions for one chain emit the ERROR only once."""
    service = _service()
    with caplog.at_level(logging.ERROR):
        for _ in range(3):
            service._warn_if_resolved_to_public_rpc("arbitrum", ARBITRUM_PUBLIC_URL)

    assert len(_public_rpc_errors(caplog)) == 1


def test_distinct_chains_each_log_once(no_rpc_credentials, caplog):
    """Each chain gets its own one-time ERROR."""
    service = _service()
    with caplog.at_level(logging.ERROR):
        service._warn_if_resolved_to_public_rpc("arbitrum", PUBLIC_RPC_URLS["arbitrum"])
        service._warn_if_resolved_to_public_rpc("base", PUBLIC_RPC_URLS["base"])

    chains = {r.args[0] for r in _public_rpc_errors(caplog)}
    assert chains == {"arbitrum", "base"}


def test_local_mode_does_not_log(monkeypatch, caplog):
    """In local mode the strategy/gateway boundary doesn't apply — no ERROR."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)
    service = _service()
    with caplog.at_level(logging.ERROR):
        service._warn_if_resolved_to_public_rpc("arbitrum", ARBITRUM_PUBLIC_URL)

    assert not _public_rpc_errors(caplog)


# --------------------------------------------------------------------------
# End-to-end resolution: each credentialed provider must suppress the ERROR
# --------------------------------------------------------------------------


def test_no_credentials_resolves_to_public_and_logs(no_rpc_credentials, caplog):
    """With no credentials, get_rpc_url falls through to public RPC → ERROR."""
    service = _service()
    with caplog.at_level(logging.ERROR):
        _resolve_and_warn(service, "arbitrum")

    assert _public_rpc_errors(caplog)


def test_alchemy_key_suppresses_error(no_rpc_credentials, monkeypatch, caplog):
    """A configured Alchemy key resolves to an Alchemy URL — no ERROR."""
    monkeypatch.setenv("ALMANAK_GATEWAY_ALCHEMY_API_KEY", "test-key")
    service = _service()
    with caplog.at_level(logging.ERROR):
        _resolve_and_warn(service, "arbitrum")

    assert not _public_rpc_errors(caplog)


def test_chain_specific_url_suppresses_error(no_rpc_credentials, monkeypatch, caplog):
    """A chain-specific RPC URL resolves to that URL — no ERROR."""
    monkeypatch.setenv("ARBITRUM_RPC_URL", "https://arb.example.com")
    service = _service()
    with caplog.at_level(logging.ERROR):
        _resolve_and_warn(service, "arbitrum")

    assert not _public_rpc_errors(caplog)


def test_generic_url_suppresses_error(no_rpc_credentials, monkeypatch, caplog):
    """A generic RPC_URL resolves to that URL — no ERROR (CodeRabbit/Gemini gap)."""
    monkeypatch.setenv("RPC_URL", "https://generic.example.com")
    service = _service()
    with caplog.at_level(logging.ERROR):
        _resolve_and_warn(service, "arbitrum")

    assert not _public_rpc_errors(caplog)


def test_tenderly_key_suppresses_error(no_rpc_credentials, monkeypatch, caplog):
    """A Tenderly API key resolves to a Tenderly URL — no ERROR (CodeRabbit/Gemini gap)."""
    monkeypatch.setenv(ARBITRUM_TENDERLY_VAR, "test-tenderly-key")
    service = _service()
    with caplog.at_level(logging.ERROR):
        _resolve_and_warn(service, "arbitrum")

    assert not _public_rpc_errors(caplog)


def test_get_compiler_invokes_the_guard(no_rpc_credentials, caplog):
    """_get_compiler routes through the guard on a cache miss."""
    service = _service()
    with caplog.at_level(logging.ERROR):
        service._get_compiler("arbitrum", "0x1234567890123456789012345678901234567890")

    assert _public_rpc_errors(caplog)
