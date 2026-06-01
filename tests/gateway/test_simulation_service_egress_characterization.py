"""HTTP-egress characterization tests for ``SimulationServiceServicer`` (VIB-4079).

The sibling ``test_simulation_service_characterization.py`` covers
``_select_simulator`` and ``SimulateBundle`` *dispatch* with the two HTTP
backends stubbed. This file is the follow-up it promised: it exercises the
**real** ``_simulate_tenderly`` / ``_simulate_alchemy`` response-parsing
branches with the ``aiohttp`` layer mocked, so the gateway's current
SimulateBundle outputs are pinned before the VIB-4851 consolidation moves the
egress into the framework simulator hierarchy.

What this LOCKS (must stay invariant across the consolidation):

* **The gas-buffer math.** The gRPC path multiplies raw gas by
  ``1 + ChainDescriptor.gas.simulation_buffer`` *inside the service* — there is
  no downstream orchestrator on this path. That fraction differs from the
  ``gas.buffer`` multiplier the execution orchestrator applies on the other
  path (bsc 0.1 vs 1.2, sonic 0.1 vs None/DEFAULT). ``TestBufferedGasTable`` is
  the canary: if the consolidation delegates buffering to a non-existent
  orchestrator, or re-applies the wrong field, these exact integers change.
* The Tenderly request payload shape (network_id, from/to/input/value/gas,
  per-bundle ``state_objects``) and the success / revert / HTTP-error / bad-JSON
  branches.
* The Alchemy success / call-error-revert / explicit-null-revertReason /
  HTTP-error / malformed-result / JSON-RPC-error branches.

What this deliberately does NOT assert: the plasma network id (currently the
buggy ``1648``) and the Tenderly/Alchemy *membership* set — both are
intentionally changed by the later consolidation phases, so pinning them here
would falsely flag the intended fixes.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.simulation_service import SimulationServiceServicer

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
# aiohttp mock — a fake ClientSession whose .post() returns a canned response
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
        self.closed = False

    def post(self, url: str, **kwargs: object) -> _FakePost:
        self.calls.append((url, kwargs))
        return _FakePost(_FakeResp(self._status, self._text))

    async def close(self) -> None:
        self.closed = True


def _install_session(svc: SimulationServiceServicer, status: int, text: str) -> _FakeSession:
    """Plant a fake session so ``_get_session`` returns it (it is non-None and not closed)."""
    session = _FakeSession(status, text)
    svc._http_session = session  # type: ignore[assignment]
    return session


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


# Raw gas the mocked backends report; 200000 makes every buffered value exact.
_RAW_GAS = 200_000


def _tenderly_success_body(gas_used: int = _RAW_GAS, sim_id: str = "sim-abc") -> str:
    return json.dumps(
        {"simulation_results": [{"transaction": {"status": True, "gas_used": gas_used}, "simulation": {"id": sim_id}}]}
    )


def _alchemy_success_body(gas_used_hex: str = "0x30d40") -> str:  # 0x30d40 == 200000
    return json.dumps({"result": [{"gasUsed": gas_used_hex}]})


# ──────────────────────────────────────────────────────────────────────────────
# Tenderly egress
# ──────────────────────────────────────────────────────────────────────────────


class TestTenderlyEgress:
    @pytest.mark.asyncio
    async def test_success_buffers_gas_and_emits_url(self):
        svc = _tenderly_only()
        _install_session(svc, 200, _tenderly_success_body())
        resp = await svc._simulate_tenderly("ethereum", [_tx()], [])
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
                    {"transaction": {"status": False, "error_info": {"error_message": "execution reverted: bad"}}}
                ]
            }
        )
        _install_session(svc, 200, body)
        resp = await svc._simulate_tenderly("ethereum", [_tx()], [])
        assert resp.success is False
        assert resp.simulated is True
        assert resp.revert_reason == "execution reverted: bad"

    @pytest.mark.asyncio
    async def test_http_non_200_is_infra_error(self):
        svc = _tenderly_only()
        _install_session(svc, 500, "upstream boom")
        resp = await svc._simulate_tenderly("ethereum", [_tx()], [])
        assert resp.success is False
        assert resp.error == "Tenderly API error: HTTP 500"

    @pytest.mark.asyncio
    async def test_bad_json_is_infra_error(self):
        svc = _tenderly_only()
        _install_session(svc, 200, "definitely not json")
        resp = await svc._simulate_tenderly("ethereum", [_tx()], [])
        assert resp.success is False
        assert resp.error.startswith("Invalid JSON response")

    @pytest.mark.asyncio
    async def test_missing_gas_used_falls_back_to_conservative_default(self):
        # gas_used absent -> service uses the 100000 conservative default, then buffers.
        svc = _tenderly_only()
        body = json.dumps({"simulation_results": [{"transaction": {"status": True}}]})
        _install_session(svc, 200, body)
        resp = await svc._simulate_tenderly("ethereum", [_tx()], [])
        assert list(resp.gas_estimates) == [110_000]  # 100000 * 1.1

    @pytest.mark.asyncio
    async def test_request_payload_shape(self):
        svc = _tenderly_only()
        session = _install_session(svc, 200, _tenderly_success_body())
        await svc._simulate_tenderly("ethereum", [_tx()], [])
        sim = session.calls[0][1]["json"]["simulations"][0]
        assert sim["network_id"] == "1"  # ethereum chain_id (correct + stable)
        assert sim["from"] == "0xfrom"
        assert sim["input"] == "0xdeadbeef"
        assert sim["to"] == "0xto"
        assert sim["value"] == "1000"
        assert sim["gas"] == 21000
        assert sim["save"] is False

    @pytest.mark.asyncio
    async def test_state_overrides_attached_to_every_bundle_sim(self):
        # CURRENT gateway behaviour: state_objects is attached to EVERY simulation
        # in the bundle. The framework simulator attaches it to the first only;
        # the consolidation will consciously change this and update this test.
        svc = _tenderly_only()
        session = _install_session(svc, 200, _tenderly_success_body())
        overrides = [gateway_pb2.SimulateStateOverride(address="0xover", balance="555")]
        await svc._simulate_tenderly("ethereum", [_tx(), _tx()], overrides)
        sims = session.calls[0][1]["json"]["simulations"]
        assert len(sims) == 2
        for sim in sims:
            assert sim["state_objects"] == {"0xover": {"balance": "555"}}


# ──────────────────────────────────────────────────────────────────────────────
# Alchemy egress
# ──────────────────────────────────────────────────────────────────────────────


class TestAlchemyEgress:
    @pytest.mark.asyncio
    async def test_success_buffers_gas(self):
        svc = _alchemy_only()
        _install_session(svc, 200, _alchemy_success_body())
        resp = await svc._simulate_alchemy("ethereum", [_tx()])
        assert resp.success is True
        assert resp.simulator_used == "alchemy"
        assert list(resp.gas_estimates) == [220_000]  # 200000 * 1.1

    @pytest.mark.asyncio
    async def test_call_error_with_revert_reason(self):
        svc = _alchemy_only()
        body = json.dumps({"result": [{"calls": [{"error": "Reverted", "revertReason": "boom"}]}]})
        _install_session(svc, 200, body)
        resp = await svc._simulate_alchemy("ethereum", [_tx()])
        assert resp.success is False
        assert resp.revert_reason == "boom"

    @pytest.mark.asyncio
    async def test_explicit_null_revert_reason_falls_back_to_error(self):
        # Alchemy can emit "revertReason": null alongside an "error" — the explicit
        # null must NOT be treated as "no error" (would report success=True).
        svc = _alchemy_only()
        body = json.dumps({"result": [{"calls": [{"error": "exec error", "revertReason": None}]}]})
        _install_session(svc, 200, body)
        resp = await svc._simulate_alchemy("ethereum", [_tx()])
        assert resp.success is False
        assert resp.revert_reason == "exec error"

    @pytest.mark.asyncio
    async def test_http_non_200_is_infra_error(self):
        svc = _alchemy_only()
        _install_session(svc, 503, "down")
        resp = await svc._simulate_alchemy("ethereum", [_tx()])
        assert resp.success is False
        assert resp.error == "Alchemy API error: HTTP 503"

    @pytest.mark.asyncio
    async def test_malformed_result_list_is_failure_not_green(self):
        svc = _alchemy_only()
        _install_session(svc, 200, json.dumps({"foo": 1}))
        resp = await svc._simulate_alchemy("ethereum", [_tx()])
        assert resp.success is False
        assert "Malformed Alchemy response" in resp.error

    @pytest.mark.asyncio
    async def test_jsonrpc_error_maps_to_revert_reason(self):
        svc = _alchemy_only()
        _install_session(svc, 200, json.dumps({"error": {"message": "rpc down"}}))
        resp = await svc._simulate_alchemy("ethereum", [_tx()])
        assert resp.success is False
        assert resp.revert_reason == "rpc down"


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
        _install_session(svc, 200, _tenderly_success_body())
        resp = await svc._simulate_tenderly(chain, [_tx()], [])
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
        _install_session(svc, 200, _alchemy_success_body())
        resp = await svc._simulate_alchemy(chain, [_tx()])
        assert list(resp.gas_estimates) == [expected]
