"""Live V4 verifier injection must not contaminate synthetic permission discovery."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.sdk import (
    MODIFY_LIQUIDITIES_SELECTOR,
    PERMIT2_ADDRESS,
    PERMIT2_APPROVE_SELECTOR,
)
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.permissions.discovery import discover_permissions
from tests.intents.conftest import _wire_v4_verified_gateway


@pytest.fixture
def verified_gateway_fixture(monkeypatch):
    gateway = Mock()
    gateway.is_connected = True
    request = SimpleNamespace(
        path=Path("tests/intents/optimism/test_uniswap_v4_lp_open.py"),
        getfixturevalue=lambda name: gateway,
    )
    _wire_v4_verified_gateway.__wrapped__(request, monkeypatch)
    return gateway


def test_live_compiler_still_uses_verified_fork(verified_gateway_fixture):
    compiler = IntentCompiler(chain="optimism", price_oracle={})
    assert compiler._gateway_client is verified_gateway_fixture
    assert compiler._venue_verification_gateway_factory() is verified_gateway_fixture


def test_explicit_execution_gateway_is_preserved(verified_gateway_fixture):
    gateway = Mock()
    factory = Mock()
    compiler = IntentCompiler(
        chain="optimism", price_oracle={}, gateway_client=gateway, venue_verification_gateway_factory=factory
    )
    assert compiler._gateway_client is gateway
    assert compiler._venue_verification_gateway_factory is factory


def test_discovery_compiler_has_no_fixture_transport(verified_gateway_fixture):
    compiler = IntentCompiler(chain="optimism", price_oracle={}, config=IntentCompilerConfig(permission_discovery=True))
    assert compiler._gateway_client is None
    assert compiler._venue_verification_gateway_factory is None


def test_real_lp_discovery_retains_permit2_under_verified_fixture(verified_gateway_fixture, monkeypatch):
    monkeypatch.setattr(IntentCompiler, "_get_chain_rpc_url", lambda self: None)
    permissions, warnings = discover_permissions("optimism", ["uniswap_v4"], ["LP_OPEN"])
    pairs = {
        (permission.target.lower(), selector.selector.lower())
        for permission in permissions
        for selector in permission.function_selectors
    }
    assert not warnings
    assert (PERMIT2_ADDRESS.lower(), PERMIT2_APPROVE_SELECTOR) in pairs
    assert (UNISWAP_V4["optimism"]["position_manager"].lower(), MODIFY_LIQUIDITIES_SELECTOR) in pairs
    assert ("0x4200000000000000000000000000000000000006", "0x095ea7b3") in pairs
    assert ("0x0b2c639c533813f4aa9d7837caf62653d097ff85", "0x095ea7b3") in pairs
    assert not verified_gateway_fixture.mock_calls
