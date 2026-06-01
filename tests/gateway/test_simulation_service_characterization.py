"""Characterization tests for ``almanak.gateway.services.simulation_service``.

SimulationService routes a transaction-simulation request to one of two HTTP
backends (Tenderly primary, Alchemy fallback) chosen by ``_select_simulator``.
The selector is the heart of the service — it enforces per-backend
constraints (tx count limit, state-override support, supported chains).

Phase 5a (second demo of the gRPC harness pattern after lifecycle_service).

Coverage focus:

  TestSelectSimulator         — every branch of ``_select_simulator``: explicit
                                 preference (tenderly/alchemy) with each
                                 failure mode, auto-select with each
                                 simulator-availability + chain-support combo.

  TestSimulateBundle          — the public RPC: empty bundle short-circuit,
                                 dispatch to tenderly vs alchemy, ValueError
                                 → INVALID_ARGUMENT, unexpected exception →
                                 INTERNAL.

  TestCloseContract           — ``close()`` is an awaitable no-op (the servicer
                                 no longer owns an aiohttp session).

Since VIB-4851 the service delegates egress to the framework simulator
hierarchy. The dispatch tests patch the delegation seam
(``_framework_simulator_for``) with a fake simulator whose ``.simulate`` is an
AsyncMock; the real HTTP egress is covered (with aiohttp mocked at the framework
layer) in the sibling ``test_simulation_service_egress_characterization.py``
(VIB-4079).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.config.env import gateway_config_from_env
from almanak.framework.execution.interfaces import SimulationResult
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.simulation_service import SimulationServiceServicer
from tests.gateway.grpc_harness import (
    assert_grpc_error,
    make_grpc_context,
)


def _fake_simulator(result: SimulationResult | None = None) -> MagicMock:
    """A stand-in framework simulator whose ``.simulate`` is an AsyncMock.

    Returned by patching ``_framework_simulator_for`` — this is the delegation
    seam the gateway crosses after VIB-4851 (it no longer owns HTTP egress).
    """
    sim = MagicMock()
    sim.simulate = AsyncMock(return_value=result or SimulationResult(success=True, simulated=True, gas_estimates=[]))
    return sim


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


def _make_settings(
    *,
    tenderly_account: str | None = None,
    tenderly_project: str | None = None,
    tenderly_key: str | None = None,
    alchemy_key: str | None = None,
) -> SimpleNamespace:
    """Build a minimal GatewaySettings shim. ``SimulationServiceServicer``
    only reads four optional credential attributes."""
    return SimpleNamespace(
        tenderly_account_slug=tenderly_account,
        tenderly_project_slug=tenderly_project,
        tenderly_access_key=tenderly_key,
        alchemy_api_key=alchemy_key,
    )


@pytest.fixture(autouse=True)
def _isolate_simulator_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip TENDERLY_*/ALCHEMY_* from os.environ so a developer's local
    .env doesn't leak into tests and silently flip availability flags.

    Both unprefixed and ``ALMANAK_GATEWAY_*`` variants are scrubbed: the
    prefixed names are read by ``GatewaySettings`` via pydantic-settings,
    so leaving them set would override anything the test injects through
    the unprefixed fallback path in ``almanak.config.env``.
    """
    for key in (
        "TENDERLY_ACCOUNT_SLUG",
        "TENDERLY_PROJECT_SLUG",
        "TENDERLY_ACCESS_KEY",
        "ALCHEMY_API_KEY",
        "ALMANAK_GATEWAY_TENDERLY_ACCOUNT_SLUG",
        "ALMANAK_GATEWAY_TENDERLY_PROJECT_SLUG",
        "ALMANAK_GATEWAY_TENDERLY_ACCESS_KEY",
        "ALMANAK_GATEWAY_ALCHEMY_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def context() -> MagicMock:
    return make_grpc_context()


def _both_configured() -> SimulationServiceServicer:
    return SimulationServiceServicer(
        _make_settings(
            tenderly_account="acct", tenderly_project="proj", tenderly_key="tk",
            alchemy_key="ak",
        )
    )


def _tenderly_only() -> SimulationServiceServicer:
    return SimulationServiceServicer(
        _make_settings(tenderly_account="acct", tenderly_project="proj", tenderly_key="tk")
    )


def _alchemy_only() -> SimulationServiceServicer:
    return SimulationServiceServicer(_make_settings(alchemy_key="ak"))


def _none_configured() -> SimulationServiceServicer:
    return SimulationServiceServicer(_make_settings())


# ──────────────────────────────────────────────────────────────────────────────
# Init / availability flags
# ──────────────────────────────────────────────────────────────────────────────


class TestInit:
    def test_both_simulators_available_when_all_creds_set(self):
        svc = _both_configured()
        assert svc._tenderly_available is True
        assert svc._alchemy_available is True

    def test_tenderly_unavailable_when_one_credential_missing(self):
        # Account + project set but no access key.
        svc = SimulationServiceServicer(
            _make_settings(tenderly_account="acct", tenderly_project="proj")
        )
        assert svc._tenderly_available is False

    def test_credentials_fall_back_to_env_vars(self, monkeypatch):
        # VIB-4424: the env-fallback ladder lives in
        # ``almanak.config.env._apply_gateway_env_fallbacks`` now — the
        # servicer only reads attributes off ``settings``. Construct settings
        # the same way production does (via ``gateway_config_from_env``) so
        # the unprefixed env vars hydrate the fields and the resolved config
        # reaches the servicer through its public boundary.
        monkeypatch.setenv("TENDERLY_ACCOUNT_SLUG", "env_acct")
        monkeypatch.setenv("TENDERLY_PROJECT_SLUG", "env_proj")
        monkeypatch.setenv("TENDERLY_ACCESS_KEY", "env_key")
        monkeypatch.setenv("ALCHEMY_API_KEY", "env_alchemy")
        svc = SimulationServiceServicer(gateway_config_from_env())
        assert svc._tenderly_account == "env_acct"
        assert svc._tenderly_project == "env_proj"
        assert svc._tenderly_key == "env_key"
        assert svc._alchemy_key == "env_alchemy"
        assert svc._tenderly_available is True
        assert svc._alchemy_available is True

    def test_settings_take_precedence_over_env(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_API_KEY", "env_key_should_be_ignored")
        svc = SimulationServiceServicer(_make_settings(alchemy_key="settings_key"))
        assert svc._alchemy_key == "settings_key"


# ──────────────────────────────────────────────────────────────────────────────
# _select_simulator — explicit preference
# ──────────────────────────────────────────────────────────────────────────────


class TestSelectSimulatorPreferTenderly:
    def test_returns_tenderly_when_available_and_chain_supported(self):
        svc = _both_configured()
        assert svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=False, preferred="tenderly") == "tenderly"

    def test_raises_when_tenderly_not_configured(self):
        svc = _alchemy_only()
        with pytest.raises(ValueError, match="Tenderly not configured"):
            svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=False, preferred="tenderly")

    def test_raises_when_chain_unsupported_by_tenderly(self):
        svc = _tenderly_only()
        with pytest.raises(ValueError, match="Tenderly does not support chain: solana"):
            svc._select_simulator(chain="solana", tx_count=1, has_state_overrides=False, preferred="tenderly")


class TestSelectSimulatorPreferAlchemy:
    def test_returns_alchemy_when_available_and_chain_supported(self):
        svc = _alchemy_only()
        assert svc._select_simulator(chain="base", tx_count=2, has_state_overrides=False, preferred="alchemy") == "alchemy"

    def test_raises_when_alchemy_not_configured(self):
        svc = _tenderly_only()
        with pytest.raises(ValueError, match="Alchemy not configured"):
            svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=False, preferred="alchemy")

    def test_raises_when_chain_unsupported_by_alchemy(self):
        svc = _alchemy_only()
        # Polygon is in TENDERLY_NETWORK_IDS but NOT in ALCHEMY_NETWORKS.
        with pytest.raises(ValueError, match="Alchemy does not support chain: polygon"):
            svc._select_simulator(chain="polygon", tx_count=1, has_state_overrides=False, preferred="alchemy")

    def test_raises_when_bundle_exceeds_alchemy_max(self):
        svc = _alchemy_only()
        with pytest.raises(ValueError, match=r"Alchemy supports max 3 transactions"):
            svc._select_simulator(chain="ethereum", tx_count=4, has_state_overrides=False, preferred="alchemy")

    def test_raises_when_state_overrides_requested(self):
        svc = _alchemy_only()
        with pytest.raises(ValueError, match="Alchemy does not support state overrides"):
            svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=True, preferred="alchemy")


# ──────────────────────────────────────────────────────────────────────────────
# _select_simulator — auto-select (preferred = "")
# ──────────────────────────────────────────────────────────────────────────────


class TestSelectSimulatorAuto:
    def test_prefers_tenderly_when_both_available(self):
        svc = _both_configured()
        assert svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=False, preferred="") == "tenderly"

    def test_falls_back_to_alchemy_when_tenderly_not_configured(self):
        svc = _alchemy_only()
        assert svc._select_simulator(chain="base", tx_count=1, has_state_overrides=False, preferred="") == "alchemy"

    def test_uses_tenderly_for_chain_alchemy_does_not_support(self):
        svc = _both_configured()
        # Polygon: Tenderly yes, Alchemy no → auto picks Tenderly.
        assert svc._select_simulator(chain="polygon", tx_count=1, has_state_overrides=False, preferred="") == "tenderly"

    def test_uses_tenderly_when_state_overrides_requested(self):
        # Even when both are available, state overrides force Tenderly.
        svc = _both_configured()
        assert svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=True, preferred="") == "tenderly"

    def test_uses_tenderly_when_bundle_exceeds_alchemy_max(self):
        svc = _both_configured()
        assert svc._select_simulator(chain="ethereum", tx_count=10, has_state_overrides=False, preferred="") == "tenderly"

    def test_alchemy_only_with_oversized_bundle_falls_through_to_tenderly_then_raises(self):
        # Only Alchemy configured, but bundle too big AND no Tenderly fallback → must raise.
        svc = _alchemy_only()
        with pytest.raises(ValueError, match="Alchemy"):
            svc._select_simulator(chain="ethereum", tx_count=10, has_state_overrides=False, preferred="")

    def test_no_simulator_configured_raises(self):
        svc = _none_configured()
        with pytest.raises(ValueError, match="No simulation backend configured"):
            svc._select_simulator(chain="ethereum", tx_count=1, has_state_overrides=False, preferred="")

    def test_tenderly_unsupported_chain_raises_with_chain_list(self):
        # Tenderly available but doesn't support solana, no Alchemy → raises listing the supported set.
        svc = _tenderly_only()
        with pytest.raises(ValueError, match="Chain solana not supported. Tenderly supports"):
            svc._select_simulator(chain="solana", tx_count=1, has_state_overrides=False, preferred="")


# ──────────────────────────────────────────────────────────────────────────────
# SimulateBundle — orchestration
# ──────────────────────────────────────────────────────────────────────────────


def _make_request(
    chain: str = "ethereum",
    txs: int = 1,
    *,
    state_overrides: int = 0,
    simulator: str = "",
) -> gateway_pb2.SimulateBundleRequest:
    return gateway_pb2.SimulateBundleRequest(
        chain=chain,
        transactions=[
            gateway_pb2.SimulateTransaction(from_address="0xfrom", to_address="0xto", data="0x", value="0", gas_limit=0)
            for _ in range(txs)
        ],
        state_overrides=[
            gateway_pb2.SimulateStateOverride(address=f"0x{i:040x}", balance="1000")
            for i in range(state_overrides)
        ],
        simulator=simulator,
    )


class TestSimulateBundleOrchestration:
    @pytest.mark.asyncio
    async def test_empty_transactions_short_circuits_to_no_simulation(self, context):
        svc = _both_configured()
        request = _make_request(txs=0)
        response = await svc.SimulateBundle(request, context)
        assert response.success is True
        assert response.simulated is False
        assert response.simulator_used == "none"
        # No selector call required when txs is empty.
        context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_chain_lowercased_before_dispatch(self, context):
        # Capital-cased chain in the request must still resolve to "ethereum"
        # when threaded through delegation (selector + simulate() call).
        svc = _tenderly_only()
        sim = _fake_simulator()
        with patch.object(svc, "_framework_simulator_for", return_value=sim):
            request = _make_request(chain="ETHEREUM", txs=1)
            await svc.SimulateBundle(request, context)
            sim.simulate.assert_awaited_once()
            # simulate(txs, chain, state_overrides=...) — chain is the 2nd positional arg.
            args, _ = sim.simulate.call_args
            assert args[1] == "ethereum"  # lowercased

    @pytest.mark.asyncio
    async def test_dispatches_to_tenderly_when_selected(self, context):
        svc = _both_configured()
        sim = _fake_simulator()
        with patch.object(svc, "_framework_simulator_for", return_value=sim) as build:
            response = await svc.SimulateBundle(_make_request(), context)
            assert response.simulator_used == "tenderly"
            build.assert_called_once_with("tenderly")
            sim.simulate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispatches_to_alchemy_when_selected(self, context):
        svc = _alchemy_only()
        sim = _fake_simulator()
        with patch.object(svc, "_framework_simulator_for", return_value=sim) as build:
            response = await svc.SimulateBundle(_make_request(chain="base"), context)
            assert response.simulator_used == "alchemy"
            build.assert_called_once_with("alchemy")
            sim.simulate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_value_error_from_selector_returns_invalid_argument(self, context):
        # No simulator configured → selector raises ValueError → INVALID_ARGUMENT.
        svc = _none_configured()
        response = await svc.SimulateBundle(_make_request(), context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="No simulation backend",
        )
        assert response.simulated is False

    @pytest.mark.asyncio
    async def test_unexpected_exception_returns_internal(self, context):
        # Framework infra failures surface as exceptions out of simulate();
        # SimulateBundle's broad-exception guard maps them to INTERNAL.
        svc = _both_configured()
        sim = MagicMock()
        sim.simulate = AsyncMock(side_effect=RuntimeError("network broke"))
        with patch.object(svc, "_framework_simulator_for", return_value=sim):
            response = await svc.SimulateBundle(_make_request(), context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INTERNAL,
            error_substring="network broke",
        )
        assert response.simulated is False

    @pytest.mark.asyncio
    async def test_explicit_simulator_preference_lowercased(self, context):
        svc = _both_configured()
        sim = _fake_simulator()
        with patch.object(svc, "_framework_simulator_for", return_value=sim) as build:
            request = _make_request(chain="base", simulator="ALCHEMY")  # capital
            await svc.SimulateBundle(request, context)
            build.assert_called_once_with("alchemy")
            sim.simulate.assert_awaited_once()


# ──────────────────────────────────────────────────────────────────────────────
# Shutdown contract
# ──────────────────────────────────────────────────────────────────────────────


class TestCloseContract:
    """After VIB-4851 the servicer holds no aiohttp session — the framework
    simulators own (and close) their own per-request sessions. ``close()`` is
    retained as an awaitable no-op so ``server.py`` shutdown, which calls
    ``close()`` on every gateway-owned servicer, keeps working.
    """

    @pytest.mark.asyncio
    async def test_close_is_awaitable_noop(self):
        svc = _both_configured()
        assert await svc.close() is None

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        svc = _both_configured()
        await svc.close()
        assert await svc.close() is None
