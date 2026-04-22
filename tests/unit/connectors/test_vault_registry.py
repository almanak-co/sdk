"""Tests for the ERC-4626 vault adapter registry."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.connectors.vaults import (
    build_vault_adapter,
    register_vault_adapter,
    supported_vault_protocols,
)


class _StubAdapter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sdk = object()


def _stub_factory(**kwargs: Any) -> _StubAdapter:
    return _StubAdapter(**kwargs)


def test_metamorpho_registered_by_default() -> None:
    assert "metamorpho" in supported_vault_protocols()


def test_register_then_dispatch() -> None:
    register_vault_adapter("test_proto_xyz", _stub_factory)
    try:
        assert "test_proto_xyz" in supported_vault_protocols()
        adapter = build_vault_adapter(
            "test_proto_xyz",
            chain="base",
            wallet_address="0xabc",
            gateway_client=object(),
        )
        assert isinstance(adapter, _StubAdapter)
        assert adapter.kwargs["chain"] == "base"
        assert adapter.kwargs["wallet_address"] == "0xabc"
        assert adapter.kwargs["token_resolver"] is None
    finally:
        # Clean up — registry is module-level and shared across tests.
        from almanak.framework.connectors.vaults import _REGISTRY

        _REGISTRY.pop("test_proto_xyz", None)


def test_dispatch_is_case_insensitive() -> None:
    register_vault_adapter("CASEINSENS_xyz", _stub_factory)
    try:
        adapter = build_vault_adapter(
            "caseinsens_XYZ",
            chain="ethereum",
            wallet_address="0xdef",
            gateway_client=object(),
        )
        assert isinstance(adapter, _StubAdapter)
    finally:
        from almanak.framework.connectors.vaults import _REGISTRY

        _REGISTRY.pop("caseinsens_xyz", None)


def test_unknown_protocol_raises_with_registry_listing() -> None:
    with pytest.raises(ValueError) as exc_info:
        build_vault_adapter(
            "definitely_not_a_protocol",
            chain="ethereum",
            wallet_address="0xabc",
            gateway_client=object(),
        )
    msg = str(exc_info.value)
    assert "Unknown vault protocol" in msg
    assert "definitely_not_a_protocol" in msg
    # Registered protocols must be enumerated so the user knows what's valid.
    assert "metamorpho" in msg
