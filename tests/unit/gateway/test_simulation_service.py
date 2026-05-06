"""Unit tests for ``_simulate_alchemy`` and its module-private helpers.

VIB-4079 W2 Sub-B: lifts ``simulation_service.py`` Alchemy branch coverage
by exercising the post + parse pipeline against a mocked HTTP client.
The wider gRPC dispatch surface is covered by the characterization test
in ``tests/gateway/test_simulation_service_characterization.py``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.simulation_service import (
    SimulationServiceServicer,
    _build_alchemy_payload,
)


def _make_settings(*, alchemy_key: str = "test_key") -> SimpleNamespace:
    return SimpleNamespace(
        tenderly_account_slug=None,
        tenderly_project_slug=None,
        tenderly_access_key=None,
        alchemy_api_key=alchemy_key,
    )


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip simulator env vars so a developer's local .env doesn't leak in."""
    for key in ("TENDERLY_ACCOUNT_SLUG", "TENDERLY_PROJECT_SLUG", "TENDERLY_ACCESS_KEY", "ALCHEMY_API_KEY"):
        monkeypatch.delenv(key, raising=False)


def _make_tx(
    *,
    from_address: str = "0xfrom",
    to_address: str = "0xto",
    data: str = "0x",
    value: str = "",
    gas_limit: int = 0,
) -> gateway_pb2.SimulateTransaction:
    return gateway_pb2.SimulateTransaction(
        from_address=from_address,
        to_address=to_address,
        data=data,
        value=value,
        gas_limit=gas_limit,
    )


def _patch_session_post(svc: SimulationServiceServicer, *, status: int, body: str) -> MagicMock:
    """Patch ``svc._get_session`` so a single .post(...) call yields ``(status, body)``.

    Returns the mock so a test can assert on the URL / payload it received.
    """
    mock_response = MagicMock()
    mock_response.status = status
    mock_response.text = AsyncMock(return_value=body)

    post_cm = MagicMock()
    post_cm.__aenter__ = AsyncMock(return_value=mock_response)
    post_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=post_cm)
    svc._get_session = AsyncMock(return_value=mock_session)  # type: ignore[method-assign]
    return mock_session


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildAlchemyPayload:
    def test_payload_shape_and_tx_dict_branches(self):
        """One pure-helper test covers payload envelope + per-tx field rules."""
        # Three transactions exercise every branch of _build_alchemy_tx_dict:
        #   tx0: contract creation (empty to_address) + invalid value fallback
        #   tx1: decimal value + gas_limit
        #   tx2: already-hex value
        txs = [
            _make_tx(to_address="", data="0xdeadbeef", value="not-a-number"),
            _make_tx(value="1000", gas_limit=21_000),
            _make_tx(value="0x10", gas_limit=0),
        ]
        payload = _build_alchemy_payload(txs)

        assert payload["jsonrpc"] == "2.0"
        assert payload["method"] == "alchemy_simulateExecutionBundle"
        assert payload["id"] == 1
        assert payload["params"][1] == "latest"

        params_txs = payload["params"][0]
        assert "to" not in params_txs[0]
        assert params_txs[0]["data"] == "0xdeadbeef"
        assert params_txs[0]["value"] == "0x0"  # invalid → fallback

        assert params_txs[1]["value"] == hex(1000)
        assert params_txs[1]["gas"] == hex(21_000)

        assert params_txs[2]["value"] == "0x10"  # preserved
        assert "gas" not in params_txs[2]  # gas_limit==0 omitted


# ──────────────────────────────────────────────────────────────────────────────
# _simulate_alchemy — end-to-end with mocked HTTP
# ──────────────────────────────────────────────────────────────────────────────


class TestSimulateAlchemy:
    @pytest.mark.asyncio
    async def test_happy_path_returns_buffered_gas_and_calls_correct_url(self):
        svc = SimulationServiceServicer(_make_settings(alchemy_key="abc123"))
        body = json.dumps({"result": [{"gasUsed": hex(100_000)}]})
        session = _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("base", [_make_tx()])

        assert response.success is True
        assert response.simulated is True
        assert response.simulator_used == "alchemy"
        assert len(response.gas_estimates) == 1
        # Gas estimate is raw + per-chain buffer; must be ≥ raw.
        assert response.gas_estimates[0] >= 100_000

        # URL + payload assertions.
        call_args, call_kwargs = session.post.call_args
        assert "base-mainnet.g.alchemy.com" in call_args[0]
        assert "abc123" in call_args[0]
        assert call_kwargs["json"]["method"] == "alchemy_simulateExecutionBundle"

    @pytest.mark.asyncio
    async def test_http_non_200_returns_error_response(self):
        svc = SimulationServiceServicer(_make_settings())
        _patch_session_post(svc, status=503, body="upstream down")

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert response.simulated is True
        assert response.simulator_used == "alchemy"
        assert "HTTP 503" in response.error

    @pytest.mark.asyncio
    async def test_malformed_json_returns_error_response(self):
        svc = SimulationServiceServicer(_make_settings())
        _patch_session_post(svc, status=200, body="not-json{")

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert response.simulator_used == "alchemy"
        assert "Invalid JSON response" in response.error

    @pytest.mark.asyncio
    async def test_rpc_error_payload_yields_revert_reason(self):
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({"error": {"code": -32000, "message": "execution reverted: bad"}})
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert response.revert_reason == "execution reverted: bad"
        assert response.simulator_used == "alchemy"

    @pytest.mark.asyncio
    async def test_call_level_error_yields_revert_reason(self):
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps(
            {
                "result": [
                    {"calls": [{"error": "execution reverted", "revertReason": "INSUFFICIENT_LIQUIDITY"}]},
                ],
            }
        )
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert response.revert_reason == "INSUFFICIENT_LIQUIDITY"

    @pytest.mark.asyncio
    async def test_falls_back_to_first_call_gas_when_result_lacks_gas_used(self):
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({"result": [{"calls": [{"gasUsed": hex(50_000)}]}]})
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is True
        assert len(response.gas_estimates) == 1
        assert response.gas_estimates[0] >= 50_000

    @pytest.mark.asyncio
    async def test_missing_result_field_is_rejected(self):
        """HTTP 200 with neither error nor result must NOT pass as a green simulation.

        Without this guard, an Alchemy protocol failure would land as
        ``success=True`` with empty gas estimates — a silent green light on a
        broken upstream.
        """
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({})  # no error, no result
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert response.simulated is True
        assert response.simulator_used == "alchemy"
        assert "Malformed Alchemy response" in response.error

    @pytest.mark.asyncio
    async def test_empty_result_list_is_rejected(self):
        """``"result": []`` is treated as malformed, not as a green zero-tx simulation."""
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({"result": []})
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert "Malformed Alchemy response" in response.error

    @pytest.mark.asyncio
    async def test_non_dict_result_item_is_rejected(self):
        """Each ``result[i]`` must be an object — string/None items are upstream corruption."""
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({"result": ["unexpected"]})
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert "invalid result item" in response.error

    @pytest.mark.asyncio
    async def test_bogus_gas_used_string_does_not_raise(self):
        """``"gasUsed": "bogus"`` must coerce to 0, not bubble ``ValueError`` into INTERNAL.

        Without the type-guarded ``_parse_hex_or_int``, an unparseable gas string
        from Alchemy would land in ``SimulateBundle``'s generic ``except Exception``
        handler and surface as a meaningless ``INTERNAL`` gRPC error.
        """
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({"result": [{"gasUsed": "bogus"}]})
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is True
        assert len(response.gas_estimates) == 1
        # Gas falls back to _ALCHEMY_DEFAULT_GAS_USED (100_000) when gasUsed is unparseable.
        assert response.gas_estimates[0] >= 100_000

    @pytest.mark.asyncio
    async def test_explicit_null_revert_reason_does_not_pass_as_success(self):
        """``"revertReason": null`` alongside ``"error"`` must NOT report success.

        ``call.get("revertReason", default)`` returns ``None`` when the key is
        present-and-explicitly-null (the default only fires for *missing* keys).
        Without the ``isinstance(..., str)`` check in ``_find_alchemy_call_error``,
        ``revert_reason`` would be ``None`` → the ``is not None`` check in
        ``_parse_alchemy_results`` would be False → the failed tx would be
        reported as ``success=True``.
        """
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps(
            {
                "result": [
                    {
                        "calls": [
                            {"error": "execution reverted: bad input", "revertReason": None},
                        ],
                    },
                ],
            }
        )
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert "execution reverted" in response.revert_reason

    @pytest.mark.asyncio
    async def test_non_string_call_error_is_coerced_to_string(self):
        """A non-string ``error`` value (dict / int) must be stringified for the proto.

        Proto string fields can't carry a Python dict; without coercion, the
        gRPC response would either crash on type mismatch or leak a Python repr.
        """
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps(
            {
                "result": [
                    {
                        "calls": [
                            {"error": {"code": -32000, "message": "internal"}},
                        ],
                    },
                ],
            }
        )
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        assert response.success is False
        assert isinstance(response.revert_reason, str)
        assert response.revert_reason  # non-empty

    @pytest.mark.asyncio
    async def test_calls_dict_instead_of_list_does_not_raise(self):
        """``"calls": {}`` (dict, not list) must be ignored, not iterated as a dict.

        Iterating a dict via ``for call in result.get("calls", [])`` would yield
        keys, then ``call.get("error", ...)`` would raise ``AttributeError`` — and
        the same shape feeds ``_extract_alchemy_gas_used``, where ``calls[0]`` on
        a dict raises ``KeyError``.
        """
        svc = SimulationServiceServicer(_make_settings())
        body = json.dumps({"result": [{"calls": {}}]})
        _patch_session_post(svc, status=200, body=body)

        response = await svc._simulate_alchemy("ethereum", [_make_tx()])

        # No error reason from a malformed calls field; gas falls through to default.
        assert response.success is True
        assert response.revert_reason == ""

    @pytest.mark.asyncio
    async def test_transport_error_propagates(self):
        # _get_session raising surfaces to the caller (SimulateBundle's
        # broad-exception → INTERNAL guard handles it); we verify the
        # method itself does not swallow it.
        svc = SimulationServiceServicer(_make_settings())
        svc._get_session = AsyncMock(side_effect=RuntimeError("connect failed"))  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="connect failed"):
            await svc._simulate_alchemy("ethereum", [_make_tx()])
