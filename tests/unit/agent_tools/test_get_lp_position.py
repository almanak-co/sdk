"""Focused branch coverage for the get_lp_position executor path."""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.errors import AgentErrorCode
from almanak.framework.agent_tools.executor import (
    ToolExecutor,
    _DecodedLPPosition,
    _ResolvedLPPositionRequest,
)
from almanak.framework.agent_tools.policy import AgentPolicy

_MANAGER = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
_FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"
_TOKEN0 = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_TOKEN1 = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def _hex_word(value: int) -> str:
    return f"{value & ((1 << 256) - 1):064x}"


def _address_word(address: str) -> str:
    return address.removeprefix("0x").zfill(64)


def _rpc_response(*, success: bool, result: str = "", error: str = "") -> Any:
    return SimpleNamespace(success=success, result=json.dumps("0x" + result), error=error)


def _position_hex(*, owed0: int = 0, owed1: int = 0) -> str:
    return "".join(
        [
            _hex_word(0),
            _hex_word(0),
            _address_word(_TOKEN0),
            _address_word(_TOKEN1),
            _hex_word(500),
            _hex_word(-100),
            _hex_word(100),
            _hex_word(123),
            _hex_word(0),
            _hex_word(0),
            _hex_word(owed0),
            _hex_word(owed1),
        ]
    )


def _executor(gateway: Any, *, allowed_chains: set[str] | None = None) -> ToolExecutor:
    return ToolExecutor(
        gateway,
        policy=AgentPolicy(
            allowed_chains=allowed_chains if allowed_chains is not None else {"arbitrum", "base", "ethereum"},
            max_tool_calls_per_minute=1000,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
            require_rebalance_check=False,
        ),
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-get-lp-position",
        default_chain="arbitrum",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_tick", "expected_in_range"),
    [
        pytest.param(-100, True, id="lower-bound-inclusive"),
        pytest.param(100, False, id="upper-bound-exclusive"),
    ],
)
async def test_get_lp_position_forwards_network_and_applies_tick_bounds(
    current_tick: int, expected_in_range: bool
) -> None:
    gateway = MagicMock()

    def _call(request: Any, **_kwargs: Any) -> Any:
        if request.id == "lp_position":
            return _rpc_response(success=True, result=_position_hex())
        if request.id == "lp_factory_get_pool":
            return _rpc_response(success=True, result=_address_word(_POOL))
        if request.id == "lp_pool_slot0":
            return _rpc_response(success=True, result=_hex_word(2**96) + _hex_word(current_tick))
        raise AssertionError(f"unexpected RPC id: {request.id}")

    gateway.rpc.Call.side_effect = _call
    with patch("almanak.framework.data.tokens.get_token_resolver", return_value=MagicMock()):
        result = await _executor(gateway).execute(
            "get_lp_position",
            {"position_id": "42", "chain": "arbitrum", "network": "mainnet"},
        )

    assert result.status == "success", result.error
    assert result.data["current_tick"] == current_tick
    assert result.data["in_range"] is expected_in_range
    assert [call.args[0].id for call in gateway.rpc.Call.call_args_list] == [
        "lp_position",
        "lp_factory_get_pool",
        "lp_pool_slot0",
    ]
    assert all(call.args[0].network == "mainnet" for call in gateway.rpc.Call.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("position_id", ["not-an-int", "-1", str(1 << 256)])
async def test_get_lp_position_rejects_invalid_uint256_before_rpc(position_id: str) -> None:
    gateway = MagicMock()

    result = await _executor(gateway).execute("get_lp_position", {"position_id": position_id})

    assert result.status == "error"
    assert result.error["error_code"] == AgentErrorCode.VALIDATION_ERROR
    assert "unsigned 256-bit integer" in result.error["message"]
    gateway.rpc.Call.assert_not_called()


@pytest.mark.asyncio
async def test_get_lp_position_accepts_max_uint256() -> None:
    gateway = MagicMock()
    gateway.rpc.Call.return_value = _rpc_response(success=False, error="stopped after calldata capture")

    result = await _executor(gateway).execute("get_lp_position", {"position_id": str((1 << 256) - 1)})

    assert result.error["error_code"] == AgentErrorCode.RPC_FAILED
    request = gateway.rpc.Call.call_args.args[0]
    calldata = json.loads(request.params)[0]["data"]
    assert calldata == "0x99fbab88" + "f" * 64


@pytest.mark.asyncio
async def test_get_lp_position_reports_unsupported_protocol_before_rpc() -> None:
    gateway = MagicMock()
    executor = _executor(gateway)
    executor._policy_engine.policy.allowed_protocols = None

    result = await executor.execute(
        "get_lp_position",
        {"position_id": "42", "protocol": "unsupported_protocol"},
    )

    assert result.status == "error"
    assert result.error["error_code"] == AgentErrorCode.VALIDATION_ERROR
    assert result.error["recoverable"] is True
    assert "Unsupported protocol" in result.error["message"]
    gateway.rpc.Call.assert_not_called()


@pytest.mark.asyncio
async def test_get_lp_position_preserves_policy_error_precedence() -> None:
    gateway = MagicMock()

    result = await _executor(gateway, allowed_chains={"arbitrum"}).execute(
        "get_lp_position",
        {"position_id": "not-an-int", "chain": "base"},
    )

    assert result.status == "error"
    assert result.error["error_code"] == AgentErrorCode.RISK_BLOCKED
    gateway.rpc.Call.assert_not_called()


@pytest.mark.asyncio
async def test_get_lp_position_rpc_failure_precedes_decode_and_pool_reads() -> None:
    gateway = MagicMock()
    gateway.rpc.Call.return_value = _rpc_response(success=False, error="provider unavailable")

    result = await _executor(gateway).execute("get_lp_position", {"position_id": "42"})

    assert result.status == "error"
    assert result.error["error_code"] == AgentErrorCode.RPC_FAILED
    assert result.error["message"] == "positions() failed: provider unavailable"
    assert result.error["recoverable"] is True
    assert gateway.rpc.Call.call_count == 1


@pytest.mark.asyncio
async def test_get_lp_position_short_payload_is_invalid_position() -> None:
    gateway = MagicMock()
    gateway.rpc.Call.return_value = _rpc_response(success=True, result=_hex_word(0))

    result = await _executor(gateway).execute("get_lp_position", {"position_id": "42"})

    assert result.status == "error"
    assert result.error["error_code"] == AgentErrorCode.INVALID_POSITION
    assert result.error["message"] == "Position 42 not found or burned"
    assert gateway.rpc.Call.call_count == 1


@pytest.mark.asyncio
async def test_get_lp_position_malformed_payload_remains_internal_error() -> None:
    gateway = MagicMock()
    gateway.rpc.Call.return_value = SimpleNamespace(success=True, result="not-json", error="")

    result = await _executor(gateway).execute("get_lp_position", {"position_id": "42"})

    assert result.status == "error"
    assert result.error["error_code"] == AgentErrorCode.INTERNAL_ERROR
    assert gateway.rpc.Call.call_count == 1


@pytest.mark.parametrize(
    "responses",
    [
        pytest.param([], id="no-reviewed-factory"),
        pytest.param([_rpc_response(success=False, error="factory unavailable")], id="factory-rpc-failure"),
        pytest.param([_rpc_response(success=True, result="12")], id="short-factory-payload"),
        pytest.param([_rpc_response(success=True, result=_hex_word(0))], id="zero-pool"),
        pytest.param(
            [
                _rpc_response(success=True, result=_address_word(_POOL)),
                _rpc_response(success=False, error="pool unavailable"),
            ],
            id="slot0-rpc-failure",
        ),
        pytest.param(
            [
                _rpc_response(success=True, result=_address_word(_POOL)),
                _rpc_response(success=True, result=_hex_word(2**96)),
            ],
            id="short-slot0-payload",
        ),
    ],
)
def test_lp_current_tick_read_failures_are_unmeasured(responses: list[Any]) -> None:
    gateway = MagicMock()
    gateway.rpc.Call.side_effect = responses
    capability = MagicMock()
    capability.factory_address_for_position_manager.return_value = _FACTORY if responses else None
    capability.get_pool_selector.return_value = "0x1698ee82"
    request = _ResolvedLPPositionRequest(
        chain="arbitrum",
        network="mainnet",
        position_id=42,
        protocol="uniswap_v3",
        capability=capability,
        position_manager=_MANAGER,
    )
    position = _DecodedLPPosition(_TOKEN0, _TOKEN1, 500, -100, 100, 123, 0, 0)

    current_tick = _executor(gateway)._read_lp_position_current_tick(request, position)

    assert current_tick is None


def test_lp_fee_enrichment_is_per_token_fail_open() -> None:
    gateway = MagicMock()
    gateway.market.GetPrice.side_effect = [SimpleNamespace(price="1.25"), RuntimeError("price unavailable")]
    resolver = MagicMock()
    resolver.resolve.side_effect = [SimpleNamespace(decimals=6), SimpleNamespace(decimals=18)]
    request = _ResolvedLPPositionRequest(
        chain="arbitrum",
        network="",
        position_id=42,
        protocol="uniswap_v3",
        capability=MagicMock(),
        position_manager=_MANAGER,
    )
    position = _DecodedLPPosition(_TOKEN0, _TOKEN1, 500, -100, 100, 123, 2_000_000, 3 * 10**18)
    data: dict[str, Any] = {}

    with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
        _executor(gateway)._enrich_lp_position_fee_usd(data, request, position)

    assert data == {"fees_a_usd": 2.5, "total_fees_usd": 2.5}


def test_lp_fee_enrichment_resolver_failure_is_fail_open() -> None:
    gateway = MagicMock()
    request = _ResolvedLPPositionRequest(
        chain="arbitrum",
        network="",
        position_id=42,
        protocol="uniswap_v3",
        capability=MagicMock(),
        position_manager=_MANAGER,
    )
    position = _DecodedLPPosition(_TOKEN0, _TOKEN1, 500, -100, 100, 123, 1, 1)
    data: dict[str, Any] = {}

    with patch("almanak.framework.data.tokens.get_token_resolver", side_effect=RuntimeError("resolver unavailable")):
        _executor(gateway)._enrich_lp_position_fee_usd(data, request, position)

    assert data == {}
    gateway.market.GetPrice.assert_not_called()
