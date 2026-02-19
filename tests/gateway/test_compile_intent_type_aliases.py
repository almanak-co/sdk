"""Tests for gateway intent-type alias normalization compatibility."""

from dataclasses import dataclass

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer


@dataclass
class _AliasCase:
    raw_type: str
    expected_class: str


@pytest.mark.parametrize(
    "case",
    [
        _AliasCase("swap", "SwapIntent"),
        _AliasCase("SWAP", "SwapIntent"),
        _AliasCase("lp_open", "LPOpenIntent"),
        _AliasCase("lp-open", "LPOpenIntent"),
        _AliasCase("lpopen", "LPOpenIntent"),
        _AliasCase("LP_OPEN", "LPOpenIntent"),
        _AliasCase("lp_close", "LPCloseIntent"),
        _AliasCase("lp-close", "LPCloseIntent"),
        _AliasCase("lpclose", "LPCloseIntent"),
        _AliasCase("perp_open", "PerpOpenIntent"),
        _AliasCase("perp-open", "PerpOpenIntent"),
        _AliasCase("perpopen", "PerpOpenIntent"),
        _AliasCase("perp_close", "PerpCloseIntent"),
        _AliasCase("perp-close", "PerpCloseIntent"),
        _AliasCase("perpclose", "PerpCloseIntent"),
        _AliasCase("bridge", "BridgeIntent"),
        _AliasCase("BRIDGE", "BridgeIntent"),
    ],
)
def test_create_intent_accepts_aliases(case: _AliasCase) -> None:
    service = ExecutionServiceServicer(GatewaySettings())

    base_data = {
        "intent_id": "i1",
        "created_at": "2026-02-11T00:00:00+00:00",
    }

    payload_by_intent = {
        "SwapIntent": {
            **base_data,
            "from_token": "USDC",
            "to_token": "ETH",
            "amount": "1",
            "chain": "arbitrum",
            "max_slippage": "0.005",
        },
        "LPOpenIntent": {
            **base_data,
            "pool": "0x0000000000000000000000000000000000000001",
            "amount0": "1",
            "amount1": "1",
            "range_lower": "1000",
            "range_upper": "2000",
            "protocol": "uniswap_v3",
            "chain": "arbitrum",
        },
        "LPCloseIntent": {
            **base_data,
            "position_id": "1",
            "collect_fees": True,
            "protocol": "uniswap_v3",
            "chain": "arbitrum",
        },
        "PerpOpenIntent": {
            **base_data,
            "market": "ETH-USD",
            "size_usd": "100",
            "is_long": True,
            "leverage": "2",
            "collateral_token": "USDC",
            "collateral_amount": "50",
            "protocol": "gmx_v2",
            "chain": "arbitrum",
        },
        "PerpCloseIntent": {
            **base_data,
            "market": "ETH-USD",
            "collateral_token": "USDC",
            "is_long": True,
            "size_usd": "100",
            "max_slippage": "0.01",
            "protocol": "gmx_v2",
            "chain": "arbitrum",
        },
        "BridgeIntent": {
            **base_data,
            "token": "USDC",
            "amount": "1",
            "from_chain": "base",
            "to_chain": "arbitrum",
            "max_slippage": "0.005",
        },
    }

    intent = service._create_intent(case.raw_type, payload_by_intent[case.expected_class])
    assert intent.__class__.__name__ == case.expected_class
