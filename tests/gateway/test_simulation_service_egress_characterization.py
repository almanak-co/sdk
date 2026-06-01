"""HTTP-egress characterization tests for ``SimulationServiceServicer`` (VIB-4079 / VIB-4851).

Phase 0 (VIB-4079) pinned the gateway's own Tenderly/Alchemy egress before the
VIB-4851 consolidation. Phase 1 (VIB-4851) moved that egress into the framework
simulator hierarchy: ``SimulateBundle`` now **delegates** to
``almanak.framework.execution.simulator.{tenderly,alchemy}`` — there is ONE
egress implementation and ONE chain -> network-id map.

This file therefore drives the **public** ``SimulateBundle`` RPC with the
framework simulators' ``aiohttp`` layer mocked (patched at the ``aiohttp`` module
level, which both framework simulators import), so the gateway's end-to-end
SimulateBundle outputs stay pinned across the consolidation.

What this LOCKS (must stay invariant across the consolidation):

* **The gas-buffer math.** The gRPC path multiplies raw gas by
  ``1 + ChainDescriptor.gas.simulation_buffer`` *inside the service* — there is
  no downstream orchestrator on this path. ``TestBufferedGasTable`` is the
  canary: if the consolidation delegates buffering to a non-existent
  orchestrator, or re-applies the wrong field, these exact integers change.
* The Tenderly success / revert branches and the outbound Tenderly payload
  fields (network_id, from/to/input/value, per-bundle ``state_objects``).
* The Alchemy success / call-error-revert branches.

What deliberately CHANGED with the consolidation (the framework error taxonomy
is now authoritative — that is the whole point of "ONE egress"):

* Infra failures (HTTP non-200, bad JSON, Alchemy JSON-RPC ``error``, missing
  ``result``) now surface as framework ``SimulationError`` (or other
  exceptions) and map to gRPC ``INTERNAL`` + ``response.error`` — they are no
  longer reported as ``simulated=True, success=False`` with an ``error``
  string baked in the service. Genuine EVM reverts still surface as
  ``revert_reason``.
* The Alchemy JSON-RPC ``error`` payload is a *recoverable* infra error in the
  framework (VIB-4588 cascade fix), not an authoritative revert. Through the
  gateway it is therefore an ``INTERNAL`` error, not a ``revert_reason``.
* The outbound Tenderly payload no longer carries a ``gas`` field (PR #817: let
  the API estimate gas freely) and sends ``value`` as 0x-hex (framework
  contract) rather than a decimal string.
* ``state_objects`` is attached to the **first** bundle simulation only
  (framework behaviour), not to every simulation (old gateway behaviour).
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc
import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.simulation_service import SimulationServiceServicer
from tests.gateway.grpc_harness import make_grpc_context

# ──────────────────────────────────────────────────────────────────────────────
# Credential isolation + servicer builders (local copies — keep this file standalone)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _isolate_simulator_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip TENDERLY_*/ALCHEMY_* from the environment so a developer's local
    ``.env`` cannot flip availability flags under the tests."""
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


def _make_settings(
    *,
    tenderly_account: str | None = None,
    tenderly_project: str | None = None,
    tenderly_key: str | None = None,
    alchemy_key: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        tenderly_account_slug=tenderly_account,
        tenderly_project_slug=tenderly_project,
        tenderly_access_key=tenderly_key,
        alchemy_api_key=alchemy_key,
    )


def _tenderly_only() -> SimulationServiceServicer:
    return SimulationServiceServicer(
        _make_settings(tenderly_account="acct", tenderly_project="proj", tenderly_key="tk")
    )


def _alchemy_only() -> SimulationServiceServicer:
    return SimulationServiceServicer(_make_settings(alchemy_key="ak"))


# ──────────────────────────────────────────────────────────────────────────────
# aiohttp mock — patch the module-level ClientSession/TCPConnector that the
# framework simulators import, so the canned response flows through delegation.
# ──────────────────────────────────────────────────────────────────────────────


class _FakeResp:
    def __init__(self, status: int, text: str) -> None:
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text


class _FakePost:
    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp

    async def __aenter__(self) -> _FakeResp:
        return self._resp

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeSession:
    """Stand-in for ``aiohttp.ClientSession`` — records posts, returns a canned response."""

    def __init__(self, status: int, text: str) -> None:
        self._status = status
        self._text = text
        self.calls: list[tuple[str, dict]] = []

    def post(self, url: str, **kwargs: object) -> _FakePost:
        self.calls.append((url, kwargs))
        return _FakePost(_FakeResp(self._status, self._text))

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


@contextmanager
def _mock_egress(status: int, text: str):
    """Patch the ``aiohttp`` surface the framework simulators use so a single
    POST yields ``(status, text)``. Yields the ``_FakeSession`` for payload
    inspection. ``TCPConnector`` is stubbed so the gateway's ssl-context path
    does not allocate a real connector under the test event loop.
    """
    session = _FakeSession(status, text)
    with (
        patch("aiohttp.ClientSession", return_value=session),
        patch("aiohttp.TCPConnector", return_value=MagicMock()),
    ):
        yield session


def _tx(
    *,
    from_address: str = "0xfrom",
    to_address: str = "0xto",
    data: str = "0xdeadbeef",
    value: str = "1000",
    gas_limit: int = 21000,
) -> gateway_pb2.SimulateTransaction:
    return gateway_pb2.SimulateTransaction(
        from_address=from_address,
        to_address=to_address,
        data=data,
        value=value,
        gas_limit=gas_limit,
    )


def _request(
    chain: str,
    txs: list[gateway_pb2.SimulateTransaction],
    *,
    state_overrides: list[gateway_pb2.SimulateStateOverride] | None = None,
    simulator: str = "",
) -> gateway_pb2.SimulateBundleRequest:
    return gateway_pb2.SimulateBundleRequest(
        chain=chain,
        transactions=txs,
        state_overrides=state_overrides or [],
        simulator=simulator,
    )


# Raw gas the mocked backends report; 200000 makes every buffered value exact.
_RAW_GAS = 200_000


def _tenderly_success_body(gas_used: int = _RAW_GAS, sim_id: str = "sim-abc") -> str:
    return json.dumps(
        {
            "simulation_results": [
                {"transaction": {"status": True, "gas_used": gas_used}, "simulation": {"status": True, "id": sim_id}}
            ]
        }
    )


def _alchemy_success_body(gas_used_hex: str = "0x30d40") -> str:  # 0x30d40 == 200000
    return json.dumps({"result": [{"calls": [{"gasUsed": gas_used_hex}]}]})


# ──────────────────────────────────────────────────────────────────────────────
# Tenderly egress (through the public SimulateBundle RPC)
# ──────────────────────────────────────────────────────────────────────────────


class TestTenderlyEgress:
    @pytest.mark.asyncio
    async def test_success_buffers_gas_and_emits_url(self):
        svc = _tenderly_only()
        with _mock_egress(200, _tenderly_success_body()):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        assert resp.success is True
        assert resp.simulated is True
        assert resp.simulator_used == "tenderly"
        assert list(resp.gas_estimates) == [220_000]  # 200000 * (1 + 0.1)
        assert "sim-abc" in resp.simulation_url

    @pytest.mark.asyncio
    async def test_revert_maps_to_failure_with_reason(self):
        svc = _tenderly_only()
        body = json.dumps(
            {
                "simulation_results": [
                    {
                        "simulation": {"status": False, "error_info": {"error_message": "execution reverted: bad"}},
                        "transaction": {"status": False},
                    }
                ]
            }
        )
        with _mock_egress(200, body):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        assert resp.success is False
        assert resp.simulated is True
        assert resp.revert_reason == "execution reverted: bad"

    @pytest.mark.asyncio
    async def test_http_non_200_is_infra_error(self):
        # Framework raises SimulationError on HTTP non-200 -> gRPC INTERNAL.
        svc = _tenderly_only()
        ctx = make_grpc_context()
        with _mock_egress(500, "upstream boom"):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), ctx)
        assert resp.success is False
        assert resp.simulated is False
        assert "HTTP 500" in resp.error
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_bad_json_is_infra_error(self):
        svc = _tenderly_only()
        ctx = make_grpc_context()
        with _mock_egress(200, "definitely not json"):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), ctx)
        assert resp.success is False
        assert "Invalid JSON" in resp.error
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_missing_gas_used_falls_back_to_conservative_default(self):
        # gas_used absent -> framework uses the 100000 conservative default,
        # then the gateway buffers it.
        svc = _tenderly_only()
        body = json.dumps({"simulation_results": [{"simulation": {"status": True}, "transaction": {}}]})
        with _mock_egress(200, body):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        assert list(resp.gas_estimates) == [110_000]  # 100000 * 1.1

    @pytest.mark.asyncio
    async def test_request_payload_shape(self):
        # Post-consolidation the outbound Tenderly payload is the framework's
        # shape: network_id from the framework map, value as 0x-hex, NO gas
        # field (PR #817), and the zero-address default for `to`/`from`.
        svc = _tenderly_only()
        with _mock_egress(200, _tenderly_success_body()) as session:
            await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        sim = session.calls[0][1]["json"]["simulations"][0]
        assert sim["network_id"] == "1"  # ethereum
        assert sim["from"] == "0xfrom"
        assert sim["input"] == "0xdeadbeef"
        assert sim["to"] == "0xto"
        assert sim["value"] == hex(1000)  # framework sends 0x-hex, only when > 0
        assert sim["save"] is False
        assert "gas" not in sim  # framework omits gas (let the API estimate freely)

    @pytest.mark.asyncio
    async def test_state_overrides_attached_to_first_bundle_sim_only(self):
        # CONSOLIDATED behaviour: the framework simulator attaches state_objects
        # to the FIRST simulation only (it applies to the whole bundle). This
        # replaces the old gateway behaviour of attaching it to every sim.
        svc = _tenderly_only()
        overrides = [gateway_pb2.SimulateStateOverride(address="0xover", balance="555")]
        with _mock_egress(200, _tenderly_success_body()) as session:
            await svc.SimulateBundle(
                _request("ethereum", [_tx(), _tx()], state_overrides=overrides),
                make_grpc_context(),
            )
        sims = session.calls[0][1]["json"]["simulations"]
        assert len(sims) == 2
        assert sims[0]["state_objects"] == {"0xover": {"balance": "555"}}
        assert "state_objects" not in sims[1]


# ──────────────────────────────────────────────────────────────────────────────
# Alchemy egress (through the public SimulateBundle RPC)
# ──────────────────────────────────────────────────────────────────────────────


class TestAlchemyEgress:
    @pytest.mark.asyncio
    async def test_success_buffers_gas(self):
        svc = _alchemy_only()
        with _mock_egress(200, _alchemy_success_body()):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        assert resp.success is True
        assert resp.simulator_used == "alchemy"
        assert list(resp.gas_estimates) == [220_000]  # 200000 * 1.1

    @pytest.mark.asyncio
    async def test_call_error_with_revert_reason(self):
        svc = _alchemy_only()
        body = json.dumps({"result": [{"calls": [{"error": "Reverted", "revertReason": "boom"}]}]})
        with _mock_egress(200, body):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        assert resp.success is False
        assert resp.revert_reason == "boom"

    @pytest.mark.asyncio
    async def test_explicit_null_revert_reason_falls_back_to_error(self):
        # Alchemy can emit ``"revertReason": null`` (key present, value null)
        # alongside an ``error``. The framework AlchemySimulator must fall back to
        # the error string — NOT crash on ``None.lower()``. Regression guard: the
        # consolidation routes gateway Alchemy traffic through the framework
        # simulator (and the operator-CLI path already does), so the framework's
        # explicit-null handling must match the old gateway's clean revert_reason.
        svc = _alchemy_only()
        body = json.dumps({"result": [{"calls": [{"error": "exec error", "revertReason": None}]}]})
        with _mock_egress(200, body):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), make_grpc_context())
        assert resp.success is False
        assert resp.revert_reason == "exec error"

    @pytest.mark.asyncio
    async def test_http_non_200_is_infra_error(self):
        svc = _alchemy_only()
        ctx = make_grpc_context()
        with _mock_egress(503, "down"):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), ctx)
        assert resp.success is False
        assert "HTTP 503" in resp.error
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_missing_result_is_infra_error(self):
        # Framework raises SimulationError("Alchemy response missing result")
        # on HTTP 200 with no error and no result -> gRPC INTERNAL. (Old
        # gateway returned its own "Malformed Alchemy response" error string.)
        svc = _alchemy_only()
        ctx = make_grpc_context()
        with _mock_egress(200, json.dumps({"foo": 1})):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), ctx)
        assert resp.success is False
        assert "missing result" in resp.error
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_jsonrpc_error_is_infra_error(self):
        # VIB-4588: a JSON-RPC `error` is a *recoverable* infra error in the
        # framework (the simulation never ran), not an authoritative revert.
        # Through the gateway it is therefore INTERNAL, not a revert_reason.
        svc = _alchemy_only()
        ctx = make_grpc_context()
        with _mock_egress(200, json.dumps({"error": {"message": "rpc down"}})):
            resp = await svc.SimulateBundle(_request("ethereum", [_tx()]), ctx)
        assert resp.success is False
        assert "rpc down" in resp.error
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)


# ──────────────────────────────────────────────────────────────────────────────
# Buffered-gas value table — the canary for the consolidation
# ──────────────────────────────────────────────────────────────────────────────


class TestBufferedGasTable:
    """Pin ``int(raw_gas * (1 + simulation_buffer))`` per chain for raw_gas=200000.

    bsc and sonic are the canaries: their ``gas.simulation_buffer`` (0.1) differs
    from their ``gas.buffer`` (1.2 / None->DEFAULT). If the consolidation ever
    applies ``gas.buffer`` here (or drops the buffer entirely), these flip.
    """

    @pytest.mark.parametrize(
        ("chain", "expected"),
        [
            ("ethereum", 220_000),  # 0.1
            ("arbitrum", 300_000),  # 0.5
            ("bsc", 220_000),  # 0.1  (canary: NOT 240000 from gas.buffer=1.2)
            ("polygon", 240_000),  # 0.2
            ("sonic", 220_000),  # 0.1  (canary: gas.buffer is None)
        ],
    )
    @pytest.mark.asyncio
    async def test_tenderly_buffered_gas_per_chain(self, chain: str, expected: int):
        svc = _tenderly_only()
        with _mock_egress(200, _tenderly_success_body()):
            resp = await svc.SimulateBundle(_request(chain, [_tx()]), make_grpc_context())
        assert list(resp.gas_estimates) == [expected]

    @pytest.mark.parametrize(
        ("chain", "expected"),
        [
            ("ethereum", 220_000),  # 0.1
            ("base", 300_000),  # 0.5
        ],
    )
    @pytest.mark.asyncio
    async def test_alchemy_buffered_gas_per_chain(self, chain: str, expected: int):
        svc = _alchemy_only()
        with _mock_egress(200, _alchemy_success_body()):
            resp = await svc.SimulateBundle(_request(chain, [_tx()]), make_grpc_context())
        assert list(resp.gas_estimates) == [expected]
