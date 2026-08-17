"""Generated permission seed remains bounded and outside runtime trust."""

from __future__ import annotations

import socket
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import addresses, market_catalog
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.connectors.gmx_v2.permission_seed import permission_market, permission_markets
from almanak.connectors.gmx_v2.sdk import GMX_V2_SDK_ADDRESSES
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.hints import get_permission_hints
from scripts.gmx_v2.generate_permission_seed import render_seed


def test_committed_seed_is_deterministic_generator_output() -> None:
    path = Path(addresses.__file__).with_name("permission_seed.json")
    assert render_seed(dict(permission_markets())) == path.read_text(encoding="utf-8")


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_permission_seed_never_populates_runtime_catalog(chain: str) -> None:
    seed = permission_markets()[chain]
    market_catalog.clear()

    assert permission_market(chain, seed.label) == seed
    assert market_catalog.by_address(chain, seed.market_token) is None


def test_permission_discovery_rejects_markets_outside_bounded_seed() -> None:
    ctx = SimpleNamespace(chain="arbitrum", permission_discovery=True)

    result = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), "SOL/USD", "permission-intent")

    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert "regenerate permission_seed.json" in (result.error or "")


def test_core_address_surfaces_contain_no_market_or_token_mirror() -> None:
    assert not hasattr(addresses, "GMX_V2_TOKENS")
    for contracts in addresses.GMX_V2.values():
        assert all("market" not in key for key in contracts)
    for sdk_addresses in GMX_V2_SDK_ADDRESSES.values():
        assert set(sdk_addresses) == {"EXCHANGE_ROUTER", "ROUTER", "DATA_STORE", "ORDER_VAULT", "READER", "WETH"}


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_permission_manifest_is_complete_without_network(monkeypatch: pytest.MonkeyPatch, chain: str) -> None:
    calls: list[object] = []

    def blocked_connect(_socket: socket.socket, address: object) -> None:
        calls.append(address)
        raise AssertionError(f"GMX permission discovery attempted network I/O: {address!r}")

    monkeypatch.setattr(socket.socket, "connect", blocked_connect)
    permissions, warnings = discover_permissions(
        chain,
        ["gmx_v2"],
        ["PERP_OPEN", "PERP_CLOSE", "PERP_CANCEL_ORDER"],
    )

    assert get_permission_hints("gmx_v2").offline_discovery is True
    assert calls == []
    assert warnings == []
    assert permissions
    selectors = {selector.selector for permission in permissions for selector in permission.function_selectors}
    assert {"0x095ea7b3", "0x7489ec23", "0xac9650d8"} <= selectors
