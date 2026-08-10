"""Gateway-routed BENQI live lending-rate capability."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.connectors.benqi.adapter import BENQI_QI_TOKENS
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer


class _MockContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _servicer_with_payload(payload: Any) -> tuple[RateHistoryServiceServicer, list[dict[str, Any]]]:
    captured: list[dict[str, Any]] = []
    response = AsyncMock()
    response.raise_for_status = MagicMock()
    response.json = AsyncMock(return_value=payload)

    def _post(_url: str, *, json: dict[str, Any]) -> Any:
        captured.append(json)
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=response)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    session = MagicMock()
    session.post = _post
    servicer = RateHistoryServiceServicer(GatewaySettings())
    servicer._get_http_session = AsyncMock(return_value=session)  # type: ignore[method-assign]
    return servicer, captured


def _servicer(raw_rate: int) -> tuple[RateHistoryServiceServicer, list[dict[str, Any]]]:
    return _servicer_with_payload({"jsonrpc": "2.0", "id": 1, "result": hex(raw_rate)})


def _request(side: str, asset: str = "USDC", market_id: str = ""):
    return gateway_pb2.GetLendingRateCurrentRequest(
        protocol="benqi",
        chain="avalanche",
        asset_symbol=asset,
        side=side,
        market_id=market_id,
    )


def test_benqi_is_registered_for_avalanche_live_rates() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    provider = servicer._lending_providers["benqi"]
    assert type(provider).__name__ == "BenqiGatewayConnector"
    assert provider.lending_supported_chains() == frozenset({"avalanche"})


def test_synthetic_account_market_scope_still_targets_asset_qitoken() -> None:
    raw_rate = 1_000_000_000  # 1e-9 per second, scaled by 1e18
    servicer, captured = _servicer(raw_rate)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(_request("supply", market_id="benqi"), ctx))  # type: ignore[arg-type]

    assert ctx.code is None
    assert response.success is True, response.error
    assert response.market_id == "benqi"
    # Literal negative control: do not reproduce the production conversion
    # formula here, or a shared scaling/compounding regression can self-bless.
    assert Decimal(response.point.supply_apy_pct) == Decimal("3.203712239279479694886441600")
    assert response.point.borrow_apy_pct == ""  # unselected side remains unmeasured
    call = captured[0]["params"][0]
    assert call["to"] == BENQI_QI_TOKENS["USDC"]["qi_token"]
    assert call["data"] == "0xd3bd2c72"  # supplyRatePerTimestamp()


def test_rpc_transport_error_is_logged_but_credentials_do_not_cross_boundary(caplog: Any) -> None:
    secret_url = "https://rpc.example/v2/secret-api-key"
    servicer, _ = _servicer(1)
    session = MagicMock()
    session.post.side_effect = RuntimeError(secret_url)
    servicer._get_http_session = AsyncMock(return_value=session)  # type: ignore[method-assign]

    with patch("almanak.gateway.utils.get_rpc_url", return_value=secret_url):
        ctx = _MockContext()
        with caplog.at_level("WARNING", logger="almanak.connectors.benqi.gateway.provider"):
            response = asyncio.run(servicer.GetLendingRateCurrent(_request("supply"), ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert response.error == "benqi: supplyRatePerTimestamp RPC request failed"
    assert secret_url not in response.error
    assert secret_url in caplog.text


def test_rpc_json_decode_error_is_logged_but_safe_at_client_boundary(caplog: Any) -> None:
    secret_detail = "decoder failed for https://rpc.example/v2/secret-api-key"
    response = AsyncMock()
    response.raise_for_status = MagicMock()
    response.json.side_effect = ValueError(secret_detail)
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=response)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.post.return_value = context_manager
    servicer = RateHistoryServiceServicer(GatewaySettings())
    servicer._get_http_session = AsyncMock(return_value=session)  # type: ignore[method-assign]

    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        ctx = _MockContext()
        with caplog.at_level("WARNING", logger="almanak.connectors.benqi.gateway.provider"):
            result = asyncio.run(servicer.GetLendingRateCurrent(_request("supply"), ctx))  # type: ignore[arg-type]

    assert result.success is False
    assert result.error == "benqi: supplyRatePerTimestamp RPC request failed"
    assert secret_detail not in result.error
    assert secret_detail in caplog.text


@pytest.mark.parametrize(
    ("payload", "safe_error"),
    [
        (None, "supplyRatePerTimestamp RPC returned an invalid response"),
        ([], "supplyRatePerTimestamp RPC returned an invalid response"),
        ({"jsonrpc": "2.0", "id": 1}, "supplyRatePerTimestamp RPC returned an invalid response"),
        (
            {"jsonrpc": "2.0", "id": 1, "error": "upstream https://rpc.example/secret-key failed"},
            "supplyRatePerTimestamp RPC call failed",
        ),
        ({"jsonrpc": "2.0", "id": 1, "result": 123}, "supplyRatePerTimestamp returned malformed uint256"),
        ({"jsonrpc": "2.0", "id": 1, "result": "not-hex"}, "supplyRatePerTimestamp returned malformed uint256"),
    ],
)
def test_malformed_rpc_shapes_fail_closed_with_safe_client_error(payload: Any, safe_error: str) -> None:
    servicer, _ = _servicer_with_payload(payload)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(_request("supply"), ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert response.error == f"benqi: {safe_error}"
    assert "secret-key" not in response.error


def test_borrow_rate_uses_borrow_selector_and_preserves_measured_zero() -> None:
    servicer, captured = _servicer(0)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(_request("borrow", "USDT"), ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert response.point.supply_apy_pct == ""
    assert Decimal(response.point.borrow_apy_pct) == Decimal(0)
    call = captured[0]["params"][0]
    assert call["to"] == BENQI_QI_TOKENS["USDT"]["qi_token"]
    assert call["data"] == "0xcd91801c"  # borrowRatePerTimestamp()


def test_unknown_asset_fails_closed_without_rpc() -> None:
    servicer, captured = _servicer(1)
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateCurrent(_request("supply", "NOT_A_BENQI_MARKET"), ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "No qiToken market" in response.error
    assert captured == []


def test_wavax_address_resolves_native_qiavax_market() -> None:
    from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

    servicer, captured = _servicer(1)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        ctx = _MockContext()
        response = asyncio.run(
            servicer.GetLendingRateCurrent(_request("supply", WRAPPED_NATIVE["avalanche"]), ctx)  # type: ignore[arg-type]
        )

    assert response.success is True, response.error
    assert captured[0]["params"][0]["to"] == BENQI_QI_TOKENS["AVAX"]["qi_token"]


def test_missing_rpc_configuration_is_not_misattributed_as_transport_failure() -> None:
    servicer, captured = _servicer(1)
    with patch("almanak.gateway.utils.get_rpc_url", side_effect=ValueError("missing config")):
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(_request("supply"), ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert response.error == "benqi: No RPC URL configured for chain 'avalanche'"
    assert captured == []


def test_wrong_market_id_fails_closed_without_rpc() -> None:
    servicer, captured = _servicer(1)
    ctx = _MockContext()
    response = asyncio.run(
        servicer.GetLendingRateCurrent(_request("supply", market_id="not-benqi"), ctx)  # type: ignore[arg-type]
    )

    assert response.success is False
    assert "Unknown BENQI market id" in response.error
    assert captured == []
