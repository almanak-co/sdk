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

  TestSessionLifecycle        — ``_get_session`` lazy-initialises and reuses,
                                 ``close`` releases.

The two backend simulators (`_simulate_tenderly` / `_simulate_alchemy`) are
HTTP-based; their response-parsing branches are stubbed out via patching for
the dispatch tests here, and covered with aiohttp-mocked tests in the sibling
``test_simulation_service_egress_characterization.py`` (VIB-4079).

Brings ``simulation_service.py`` from 12% → ~80% on the unit-scope.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest
import pytest_asyncio

from almanak.config.env import gateway_config_from_env
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.simulation_service import SimulationServiceServicer
from tests.gateway.grpc_harness import (
    assert_grpc_error,
    make_grpc_context,
)


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
        # Capital-cased chain in the request must still match TENDERLY_NETWORK_IDS["ethereum"].
        svc = _tenderly_only()
        with patch.object(svc, "_simulate_tenderly", new=AsyncMock(return_value=gateway_pb2.SimulateBundleResponse(success=True))) as fake:
            request = _make_request(chain="ETHEREUM", txs=1)
            await svc.SimulateBundle(request, context)
            fake.assert_awaited_once()
            args, _ = fake.call_args
            assert args[0] == "ethereum"  # lowercased

    @pytest.mark.asyncio
    async def test_dispatches_to_tenderly_when_selected(self, context):
        svc = _both_configured()
        with patch.object(svc, "_simulate_tenderly", new=AsyncMock(return_value=gateway_pb2.SimulateBundleResponse(success=True, simulator_used="tenderly"))) as t, \
             patch.object(svc, "_simulate_alchemy", new=AsyncMock()) as a:
            response = await svc.SimulateBundle(_make_request(), context)
            assert response.simulator_used == "tenderly"
            t.assert_awaited_once()
            a.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatches_to_alchemy_when_selected(self, context):
        svc = _alchemy_only()
        with patch.object(svc, "_simulate_alchemy", new=AsyncMock(return_value=gateway_pb2.SimulateBundleResponse(success=True, simulator_used="alchemy"))) as a, \
             patch.object(svc, "_simulate_tenderly", new=AsyncMock()) as t:
            response = await svc.SimulateBundle(_make_request(chain="base"), context)
            assert response.simulator_used == "alchemy"
            a.assert_awaited_once()
            t.assert_not_awaited()

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
        svc = _both_configured()
        with patch.object(
            svc, "_simulate_tenderly",
            new=AsyncMock(side_effect=RuntimeError("network broke")),
        ):
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
        with patch.object(svc, "_simulate_alchemy", new=AsyncMock(return_value=gateway_pb2.SimulateBundleResponse(success=True))) as a:
            request = _make_request(chain="base", simulator="ALCHEMY")  # capital
            await svc.SimulateBundle(request, context)
            a.assert_awaited_once()


# ──────────────────────────────────────────────────────────────────────────────
# Session lifecycle
# ──────────────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def lifecycle_svc():
    """Yield a fully-configured SimulationServiceServicer, guaranteeing
    ``close()`` is awaited even if a test assertion fails. Prevents
    aiohttp ClientSession leaks (and the noisy unclosed-session warnings
    they produce in the test runner).
    """
    svc = _both_configured()
    try:
        yield svc
    finally:
        await svc.close()


class TestSessionLifecycle:
    @pytest.mark.asyncio
    async def test_get_session_lazy_initialises(self, lifecycle_svc):
        assert lifecycle_svc._http_session is None
        session = await lifecycle_svc._get_session()
        assert session is not None
        assert lifecycle_svc._http_session is session

    @pytest.mark.asyncio
    async def test_get_session_reuses_existing(self, lifecycle_svc):
        first = await lifecycle_svc._get_session()
        second = await lifecycle_svc._get_session()
        assert first is second

    @pytest.mark.asyncio
    async def test_close_idempotent_when_never_opened(self, lifecycle_svc):
        # Fixture's finally-block close will be the first close call;
        # explicitly calling close here verifies idempotency.
        await lifecycle_svc.close()
        assert lifecycle_svc._http_session is None

    @pytest.mark.asyncio
    async def test_close_releases_session(self, lifecycle_svc):
        session = await lifecycle_svc._get_session()
        await lifecycle_svc.close()
        assert lifecycle_svc._http_session is None
        assert session.closed is True
