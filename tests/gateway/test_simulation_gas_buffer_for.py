"""Gateway unit tests for ``_simulation_gas_buffer_for``.

The helper is a thin wrapper around ``ChainRegistry.try_resolve`` used by
``_parse_alchemy_results`` and ``_simulate_tenderly`` to read the per-chain
simulation gas buffer with a safe fallback to ``DEFAULT_SIMULATION_BUFFER``.
CodeRabbit (PR #2418 round 3) flagged three branches needing direct gateway
unit coverage:

  1. known chain with a concrete ``descriptor.gas.simulation_buffer``
  2. registered chain whose ``simulation_buffer`` is ``None``
  3. unknown chain (``try_resolve`` returns ``None``)

The wider Alchemy / Tenderly dispatch surface is covered by the existing
characterization suite in
``tests/gateway/test_simulation_service_characterization.py``; this file
exercises the lookup helper in isolation so CRAP coverage on the simulation
service does not regress when the legacy ``SIMULATION_GAS_BUFFERS`` dict is
deleted in a future cleanup.

VIB-4801: matches the pattern used by
``tests/unit/runner/test_resolve_gas_context.py`` for the sibling
ChainRegistry cutover (real registry for happy/None-field paths,
monkeypatch only for the synthetic unknown-chain case).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.execution.gas.constants import DEFAULT_SIMULATION_BUFFER
from almanak.gateway.services.simulation_service import _simulation_gas_buffer_for


class TestSimulationGasBufferForKnownChain:
    """Branch 1: descriptor exists, ``simulation_buffer`` set → return it."""

    def test_ethereum_returns_registered_buffer(self) -> None:
        # ethereum descriptor pins simulation_buffer=0.1 (see
        # almanak/core/chains/ethereum.py). Mirrors the historical
        # CHAIN_SIMULATION_BUFFERS["ethereum"].
        assert _simulation_gas_buffer_for("ethereum") == 0.1

    def test_arbitrum_returns_registered_buffer(self) -> None:
        # arbitrum descriptor pins simulation_buffer=0.5.
        assert _simulation_gas_buffer_for("arbitrum") == 0.5

    def test_optimism_returns_registered_buffer(self) -> None:
        assert _simulation_gas_buffer_for("optimism") == 0.5

    def test_base_returns_registered_buffer(self) -> None:
        assert _simulation_gas_buffer_for("base") == 0.5

    def test_polygon_returns_registered_buffer(self) -> None:
        assert _simulation_gas_buffer_for("polygon") == 0.2

    def test_resolution_is_case_insensitive(self) -> None:
        # ChainRegistry.try_resolve does ``key.lower().strip()`` itself.
        # Pinning this so a future case-sensitive change can't silently
        # downgrade the gateway to DEFAULT_SIMULATION_BUFFER.
        assert _simulation_gas_buffer_for("ETHEREUM") == 0.1
        assert _simulation_gas_buffer_for("  arbitrum  ") == 0.5


class TestSimulationGasBufferForMissingBuffer:
    """Branch 2: descriptor exists but ``simulation_buffer`` is ``None``
    → fall back to ``DEFAULT_SIMULATION_BUFFER``.
    """

    def test_solana_falls_back_to_default(self) -> None:
        # solana descriptor sets simulation_buffer=None explicitly
        # (almanak/core/chains/solana.py). This is the live "registered
        # but no buffer" case in the registry today.
        assert _simulation_gas_buffer_for("solana") == DEFAULT_SIMULATION_BUFFER

    def test_descriptor_without_buffer_falls_back(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Synthesize a descriptor whose ``gas.simulation_buffer`` is None
        # so the assertion does not depend on Solana's specific config.
        fake_descriptor = SimpleNamespace(gas=SimpleNamespace(simulation_buffer=None))
        from almanak.core import chains as chains_pkg

        monkeypatch.setattr(
            chains_pkg.ChainRegistry, "try_resolve", lambda _name: fake_descriptor
        )
        assert _simulation_gas_buffer_for("any-chain") == DEFAULT_SIMULATION_BUFFER


class TestSimulationGasBufferForUnknownChain:
    """Branch 3: ``try_resolve`` returns ``None`` → ``DEFAULT_SIMULATION_BUFFER``."""

    def test_unknown_chain_falls_back_to_default(self) -> None:
        # Real registry, unregistered name. No monkeypatch needed —
        # ``try_resolve`` returns None for unknown names without raising.
        assert _simulation_gas_buffer_for("not-a-real-chain") == DEFAULT_SIMULATION_BUFFER

    def test_empty_string_falls_back_to_default(self) -> None:
        assert _simulation_gas_buffer_for("") == DEFAULT_SIMULATION_BUFFER

    def test_explicit_none_resolution_falls_back(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Pin the wrapper's contract: if try_resolve returns None, the
        # helper MUST return DEFAULT_SIMULATION_BUFFER (never raise).
        from almanak.core import chains as chains_pkg

        monkeypatch.setattr(
            chains_pkg.ChainRegistry, "try_resolve", lambda _name: None
        )
        assert _simulation_gas_buffer_for("any-chain") == DEFAULT_SIMULATION_BUFFER


def test_helper_is_module_local() -> None:
    """Static guard: the helper must remain importable from
    ``simulation_service`` under its original name. The Alchemy and
    Tenderly call sites both reference ``_simulation_gas_buffer_for(chain)``
    directly; renaming or removing it without updating the call sites
    would be a silent break.
    """
    from almanak.gateway.services import simulation_service

    assert hasattr(simulation_service, "_simulation_gas_buffer_for")
    assert callable(simulation_service._simulation_gas_buffer_for)
