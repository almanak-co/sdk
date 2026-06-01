"""Read-tool response goldens (VIB-4860 / W8, plan §0 + §7.3).

The real regression risk of W8 is the **ABI-decode logic** that moves out
of ``executor.py`` and into per-connector ``agent_read_provider`` modules
(plan §8: "moving the slot0/positions()/getUserAccountData() decode logic
into connectors can silently change a decoded value"). The W6 byte-
equivalence discipline is the mitigation: pin the decoded ``ToolResponse``
for a fixed RPC-response fixture per ``(protocol, chain)`` and assert it
never changes.

These goldens drive the ``get_pool_state`` / ``get_lp_position`` /
``list_lending_positions`` tools through a *scripted* gateway (the
``rpc.Call`` / ``market.GetPrice`` responses are fixed hex), so the only
thing under test is the decode + address/selector resolution path. The
tools still route through ``ToolExecutor.execute()`` so the PolicyEngine
gate is exercised too.

If a W8 step changes any decoded number, this fails — STOP per the
CRAP-refactor protocol (any PASS->FAIL is a regression, not a test to
"fix").
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy


def _hex_word(value: int) -> str:
    """ABI-encode ``value`` as a single 32-byte (64 hex char) word."""
    return f"{value & ((1 << 256) - 1):064x}"


def _addr_word(addr: str) -> str:
    """Right-align a 20-byte address into a 32-byte word (no 0x)."""
    return addr.lower().removeprefix("0x").zfill(64)


def _rpc_ok(result_hex: str) -> Any:
    """Build a successful RpcResponse-like object with ``result`` = JSON hex string."""
    resp = MagicMock()
    resp.success = True
    resp.result = json.dumps("0x" + result_hex)
    resp.error = ""
    return resp


# Real Arbitrum USDC/WETH 0.05% pool, used so token-address ordering is
# deterministic (token0 = whichever address sorts lower).
_POOL_ADDR = "0xc6962004f452be9203591991d15f6b388e09e8d0"
_USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def _slot0_hex(sqrt_price_x96: int, tick: int) -> str:
    """slot0() returns 7 fields; we only decode the first two words."""
    # tick is int24, ABI sign-extends to 256 bits — model that here.
    tick_word = _hex_word(tick & ((1 << 256) - 1) if tick >= 0 else (tick + (1 << 256)))
    # Remaining 5 words (observationIndex, ... , unlocked) — arbitrary.
    return _hex_word(sqrt_price_x96) + tick_word + _hex_word(0) * 5


def _make_executor(gateway: Any) -> ToolExecutor:
    """ToolExecutor with a permissive read-tool policy + scripted gateway."""
    policy = AgentPolicy(
        allowed_protocols={
            "uniswap_v3",
            "aerodrome_slipstream",
            "pancakeswap_v3",
            "sushiswap_v3",
            "aave_v3",
        },
        allowed_chains={"arbitrum", "base", "ethereum"},
        max_tool_calls_per_minute=1000,
    )
    return ToolExecutor(
        gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-golden",
        default_chain="arbitrum",
    )


# ---------------------------------------------------------------------------
# get_pool_state — Uniswap V3 on arbitrum (explicit pool_address shortcut so
# the factory.getPool() call is skipped; the decode path is the target).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_pool_state_uniswap_v3_arbitrum_golden() -> None:
    sqrt_price_x96 = 1_500_000_000_000_000_000_000_000_000_000
    tick = -201_000
    liquidity = 123_456_789_000_000

    gateway = MagicMock()

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.id == "pool_slot0":
            return _rpc_ok(_slot0_hex(sqrt_price_x96, tick))
        if req.id == "pool_liquidity":
            return _rpc_ok(_hex_word(liquidity))
        raise AssertionError(f"unexpected rpc id {req.id}")

    gateway.rpc.Call.side_effect = _call

    executor = _make_executor(gateway)
    result = await executor.execute(
        "get_pool_state",
        {
            "token_a": "USDC",
            "token_b": "WETH",
            "fee_tier": 500,
            "chain": "arbitrum",
            "protocol": "uniswap_v3",
            "pool_address": _POOL_ADDR,
        },
    )

    assert result.status == "success", result.error
    d = result.data
    # The decoded primitives are the byte-equivalence contract.
    assert d["pool_address"] == _POOL_ADDR
    assert d["tick"] == tick
    assert d["liquidity"] == str(liquidity)
    assert d["sqrt_price_x96"] == str(sqrt_price_x96)
    assert d["fee_tier"] == 500
    # Price is derived deterministically from sqrt_price + token decimals.
    expected_raw = (sqrt_price_x96 / 2**96) ** 2
    assert d["current_price_raw"] == str(expected_raw)


@pytest.mark.asyncio
async def test_get_pool_state_unsupported_protocol_returns_error() -> None:
    """An unregistered protocol returns a validation error (not a crash).

    Allow *any* protocol through policy (``allowed_protocols = None``) and pass a
    genuinely unregistered protocol so we exercise the handler's own registry
    guard — not a policy denial. The previous version set
    ``allowed_protocols = set()`` (deny-all) with a *supported* protocol, so it
    blocked at the policy layer and never reached the unsupported-protocol path
    it claims to cover (CodeRabbit review).
    """
    gateway = MagicMock()
    executor = _make_executor(gateway)
    executor._policy_engine.policy.allowed_protocols = None
    result = await executor.execute(
        "get_pool_state",
        {
            "token_a": "USDC",
            "token_b": "WETH",
            "fee_tier": 500,
            "chain": "arbitrum",
            "protocol": "unsupported_protocol",
            "pool_address": _POOL_ADDR,
        },
    )
    assert result.status == "error"
    # Proves the handler's registry guard produced the error, not policy.
    assert "Unsupported protocol" in (result.error or {}).get("message", "")


# ---------------------------------------------------------------------------
# get_lp_position — Uniswap V3 on arbitrum. positions(uint256) returns 12
# words; the decode (token0/1, fee, tick_lower/upper, liquidity, owed) is the
# byte-equivalence contract.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_lp_position_uniswap_v3_arbitrum_golden() -> None:
    fee = 500
    tick_lower = -202_000
    tick_upper = -200_000
    liquidity = 987_654_321_000
    owed0 = 1_000_000  # 1 USDC (6 decimals)
    owed1 = 500_000_000_000_000  # 0.0005 WETH (18 decimals)
    position_id = 424242

    # positions() layout: [nonce, operator, token0, token1, fee, tickLower,
    # tickUpper, liquidity, feeGrowthInside0, feeGrowthInside1, owed0, owed1]
    def _tick_word(t: int) -> str:
        return _hex_word(t if t >= 0 else (t + (1 << 256)))

    words = [
        _hex_word(0),  # nonce
        _hex_word(0),  # operator
        _addr_word(_USDC_ARB),  # token0
        _addr_word(_WETH_ARB),  # token1
        _hex_word(fee),
        _tick_word(tick_lower),
        _tick_word(tick_upper),
        _hex_word(liquidity),
        _hex_word(0),  # feeGrowthInside0
        _hex_word(0),  # feeGrowthInside1
        _hex_word(owed0),
        _hex_word(owed1),
    ]
    positions_hex = "".join(words)

    gateway = MagicMock()

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.id == "lp_position":
            return _rpc_ok(positions_hex)
        # Pool-tick lookup (in-range check): return a zero-address from
        # factory.getPool so the handler skips the slot0 read (in_range=None).
        if req.id == "lp_factory_get_pool":
            return _rpc_ok(_hex_word(0))
        raise AssertionError(f"unexpected rpc id {req.id}")

    gateway.rpc.Call.side_effect = _call
    # Fee USD enrichment calls market.GetPrice; make it fail-open (price 0).
    price_resp = MagicMock()
    price_resp.price = "0"
    gateway.market.GetPrice.return_value = price_resp

    executor = _make_executor(gateway)
    result = await executor.execute(
        "get_lp_position",
        {
            "position_id": str(position_id),
            "chain": "arbitrum",
            "protocol": "uniswap_v3",
        },
    )

    assert result.status == "success", result.error
    d = result.data
    assert d["position_id"] == str(position_id)
    assert d["token_a"].lower() == _USDC_ARB
    assert d["token_b"].lower() == _WETH_ARB
    assert d["fee_tier"] == fee
    assert d["tick_lower"] == tick_lower
    assert d["tick_upper"] == tick_upper
    assert d["liquidity"] == str(liquidity)
    assert d["tokens_owed_a"] == str(owed0)
    assert d["tokens_owed_b"] == str(owed1)
    # Factory returned zero pool -> in_range stays None (no slot0 read).
    assert d["in_range"] is None


# ---------------------------------------------------------------------------
# list_lending_positions — Aave V3 on arbitrum. getUserAccountData returns 6
# words; the base->USD scaling (8 decimals) + health-factor (1e18) decode is
# the byte-equivalence contract.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_lending_positions_aave_v3_arbitrum_golden() -> None:
    total_collateral_base = 1_000_000_000_000  # 10,000.00 USD (8 decimals)
    total_debt_base = 250_000_000_000  # 2,500.00 USD
    available_borrows_base = 500_000_000_000  # 5,000.00 USD
    liq_threshold_bps = 8250
    ltv_bps = 8000
    health_factor_raw = 3_200_000_000_000_000_000  # 3.2 (1e18)

    words = [
        _hex_word(total_collateral_base),
        _hex_word(total_debt_base),
        _hex_word(available_borrows_base),
        _hex_word(liq_threshold_bps),
        _hex_word(ltv_bps),
        _hex_word(health_factor_raw),
    ]
    acct_hex = "".join(words)

    gateway = MagicMock()

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.id == "aave_acct_data":
            return _rpc_ok(acct_hex)
        raise AssertionError(f"unexpected rpc id {req.id}")

    gateway.rpc.Call.side_effect = _call

    executor = _make_executor(gateway)
    result = await executor.execute(
        "list_lending_positions",
        {"chain": "arbitrum", "protocol": "aave_v3"},
    )

    assert result.status == "success", result.error
    d = result.data
    assert d["protocol"] == "aave_v3"
    assert d["total_collateral_usd"] == str(Decimal(total_collateral_base) / Decimal(10**8))
    assert d["total_debt_usd"] == str(Decimal(total_debt_base) / Decimal(10**8))
    assert d["available_borrows_usd"] == str(Decimal(available_borrows_base) / Decimal(10**8))
    assert d["current_liquidation_threshold_bps"] == liq_threshold_bps
    assert d["ltv_bps"] == ltv_bps
    assert d["health_factor"] == str(Decimal(health_factor_raw) / Decimal(10**18))


@pytest.mark.asyncio
async def test_list_lending_positions_no_debt_infinite_hf_golden() -> None:
    """MAX_UINT256 health factor -> infinity symbol (decode edge case)."""
    max_uint = (1 << 256) - 1
    words = [
        _hex_word(1_000_000_000_000),
        _hex_word(0),  # no debt
        _hex_word(500_000_000_000),
        _hex_word(8250),
        _hex_word(8000),
        _hex_word(max_uint),
    ]
    gateway = MagicMock()
    gateway.rpc.Call.side_effect = lambda req, **kw: _rpc_ok("".join(words))
    executor = _make_executor(gateway)
    result = await executor.execute(
        "list_lending_positions",
        {"chain": "arbitrum", "protocol": "aave_v3"},
    )
    assert result.status == "success", result.error
    assert result.data["health_factor"] == "∞"
