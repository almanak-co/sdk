"""VIB-4989 PR-A commit 2: Polymarket provider/handler relocation guards.

Commit 2 relocates ``PredictionMarketDataProvider`` and ``ClobActionHandler`` into
the polymarket connector (the framework originals stay live until PR B) and wires
the ``_strategy_base`` registries to the connector specs. These tests pin:

1. **Byte-equivalence** — every method of the connector copy has source IDENTICAL
   to the framework original (no drift). Combined with the framework original's
   existing coverage, this transitively proves the relocated copy is correct,
   without re-mocking 20 provider methods / the async CLOB handler. This is a
   PR-A-only transition guard: PR B deletes the framework original and this file.
2. **Registry wiring** — the real polymarket registration resolves through the
   read / execute / stub registries, and the spec factories construct the
   *relocated* (connector) classes end-to-end.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from almanak.connectors._strategy_base.gateway_stub_registry import GatewayStubRegistry
from almanak.connectors._strategy_base.prediction_execute_registry import PredictionExecuteRegistry
from almanak.connectors._strategy_base.prediction_read_base import PredictionProvider
from almanak.connectors._strategy_base.prediction_read_registry import PredictionReadRegistry
from almanak.connectors.polymarket.clob_handler import ClobActionHandler as ConnHandler
from almanak.connectors.polymarket.prediction_provider import PredictionMarketDataProvider as ConnProvider
from almanak.framework.execution.clob_handler import ClobActionHandler as FwHandler
from almanak.framework.data.prediction_provider import PredictionMarketDataProvider as FwProvider


def _method_sources(cls) -> dict[str, str]:
    """Map every method name on ``cls`` to its source text (functions + classmethods)."""
    out: dict[str, str] = {}
    for name, member in inspect.getmembers(cls):
        if inspect.isfunction(member) or inspect.ismethod(member):
            out[name] = inspect.getsource(member)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Byte-equivalence: connector copy must not drift from the framework original
# ──────────────────────────────────────────────────────────────────────────────


def test_provider_method_surface_matches():
    assert _method_sources(FwProvider).keys() == _method_sources(ConnProvider).keys()


def test_provider_methods_byte_identical():
    fw, conn = _method_sources(FwProvider), _method_sources(ConnProvider)
    drift = sorted(n for n in fw if fw[n] != conn.get(n))
    assert not drift, f"connector provider drifted from framework original in: {drift}"


def test_handler_method_surface_matches():
    assert _method_sources(FwHandler).keys() == _method_sources(ConnHandler).keys()


def test_handler_methods_byte_identical():
    fw, conn = _method_sources(FwHandler), _method_sources(ConnHandler)
    drift = sorted(n for n in fw if fw[n] != conn.get(n))
    assert not drift, f"connector handler drifted from framework original in: {drift}"


# ──────────────────────────────────────────────────────────────────────────────
# Registry wiring: the real polymarket registration resolves
# ──────────────────────────────────────────────────────────────────────────────


def test_polymarket_registered_in_read_and_execute_registries():
    assert "polymarket" in PredictionReadRegistry.supported_protocols()
    assert PredictionReadRegistry.supports_chain("polymarket", "polygon") is True
    assert "polymarket" in PredictionExecuteRegistry.supported_protocols()
    assert "polymarket" in PredictionExecuteRegistry.protocols_for_chain("polygon")


def test_polymarket_registered_in_stub_registry():
    assert "polymarket" in GatewayStubRegistry.stub_names()
    channel = MagicMock(name="grpc_channel")
    stubs = GatewayStubRegistry.build_stubs(channel)
    from almanak.connectors.polymarket.proto import polymarket_pb2_grpc

    assert isinstance(stubs["polymarket"], polymarket_pb2_grpc.PolymarketServiceStub)


def test_read_spec_builds_relocated_connector_provider():
    gw = MagicMock(name="gateway_client")
    gw.is_connected = True  # GatewayPolymarketClient requires a connected gateway
    provider = PredictionReadRegistry.build_provider("polymarket", gateway_client=gw, wallet="0xabc")
    assert isinstance(provider, ConnProvider)
    # The relocated provider satisfies the venue-neutral read Protocol.
    assert isinstance(provider, PredictionProvider)


def test_execute_spec_builds_relocated_connector_handler():
    gw = MagicMock(name="gateway_client")
    gw.is_connected = True
    handler = PredictionExecuteRegistry.build_handler("polymarket", gateway_client=gw, wallet="0xabc")
    assert isinstance(handler, ConnHandler)
