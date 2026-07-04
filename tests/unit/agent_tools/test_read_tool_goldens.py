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


# ---------------------------------------------------------------------------
# list_lending_reserves — Aave V3 on polygon (VIB-4925). The tool enumerates the
# live on-chain reserve set via getAllReservesTokens(), then decodes each
# reserve's getReserveConfigurationData (10 words). Both decodes are the byte-
# equivalence contract. Mirrors the real colleague report: WMATIC is supply/
# collateral-only (borrowingEnabled=false), USDC is borrowable.
# ---------------------------------------------------------------------------

# Real Polygon Aave V3 reserve addresses (lowercased) — used so the on-chain
# enumeration fixture and the row assertions line up with reality.
_WMATIC_POLYGON = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
_USDC_POLYGON = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
_DAI_POLYGON = "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063"


def _reserve_cfg_hex(*, ltv: int, usage: bool, borrow: bool, active: bool, frozen: bool) -> str:
    """Encode the 10-word getReserveConfigurationData tuple (only the decoded words matter)."""
    return (
        _hex_word(6)  # decimals (unused by decoder)
        + _hex_word(ltv)
        + _hex_word(8250)  # liquidationThreshold (word 2 — decoded)
        + _hex_word(10500)  # liquidationBonus (unused)
        + _hex_word(1000)  # reserveFactor (unused)
        + _hex_word(1 if usage else 0)
        + _hex_word(1 if borrow else 0)
        + _hex_word(0)  # stableBorrowRateEnabled (unused)
        + _hex_word(1 if active else 0)
        + _hex_word(1 if frozen else 0)
    )


def _all_reserves_hex(tokens: list[tuple[str, str]]) -> str:
    """ABI-encode getAllReservesTokens() -> TokenData[] (string symbol, address)."""
    from eth_abi import encode as _abi_encode

    return _abi_encode(["(string,address)[]"], [tokens]).hex()


def _make_reserves_executor(gateway: Any) -> ToolExecutor:
    """Executor allowing any chain so the polygon market is reachable."""
    executor = _make_executor(gateway)
    # Read-only discovery isn't wallet- or chain-scoped to the test set.
    executor._policy_engine.policy.allowed_chains = None
    return executor


def _reserves_gateway(enumeration: list[tuple[str, str]], cfg_for: Any) -> Any:
    """Scripted gateway: answers getAllReservesTokens() then per-symbol config.

    ``cfg_for(symbol)`` returns either a config-hex string (wrapped via _rpc_ok)
    or the literal ``"FAIL"`` to simulate a per-reserve RPC failure.
    """

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.id == "aave_all_reserves":
            return _rpc_ok(_all_reserves_hex(enumeration))
        symbol = req.id.split(":", 1)[1]
        out = cfg_for(symbol)
        if out == "FAIL":
            resp = MagicMock()
            resp.success = False
            resp.error = "execution reverted"
            return resp
        return _rpc_ok(out)

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    return gateway


@pytest.mark.asyncio
async def test_list_lending_reserves_polygon_golden() -> None:
    # On-chain enumeration returns WMATIC, USDC, DAI. WMATIC is collateral-
    # capable but NOT borrowable (the reported case); USDC is fully borrowable;
    # DAI's config read is forced to fail to prove the fail-open per-reserve
    # contract (row carries an error, flags None, the rest still reported).
    enumeration = [("WMATIC", _WMATIC_POLYGON), ("USDC", _USDC_POLYGON), ("DAI", _DAI_POLYGON)]

    def _cfg_for(symbol: str) -> str:
        if symbol == "DAI":
            return "FAIL"
        if symbol == "WMATIC":
            # Real Polygon values: collateral-enabled, ltv 6800, NOT borrowable.
            return _reserve_cfg_hex(ltv=6800, usage=True, borrow=False, active=True, frozen=False)
        return _reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False)

    executor = _make_reserves_executor(_reserves_gateway(enumeration, _cfg_for))
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3"},
    )

    assert result.status == "success", result.error
    d = result.data
    assert d["schema_version"] == 1
    assert d["chain"] == "polygon"
    assert d["pool_data_provider"]  # resolved from the chain table
    assert d["count"] == 3  # every enumerated reserve reported (incl. the failed one)
    assert d["truncated"] is False  # well under the safety cap
    assert d["truncation_reason"] == ""
    assert d["total_matched"] == d["count"]
    by_symbol = {r["symbol"]: r for r in d["reserves"]}

    # Headline contract: WMATIC is collateral-capable but not borrowable; USDC is borrowable.
    assert by_symbol["WMATIC"]["borrowing_enabled"] is False
    assert by_symbol["WMATIC"]["usage_as_collateral_enabled"] is True
    assert by_symbol["WMATIC"]["is_active"] is True  # pins the isActive (word 8) decode bit
    assert by_symbol["WMATIC"]["is_frozen"] is False  # pins the isFrozen (word 9) decode bit
    assert by_symbol["WMATIC"]["ltv_bps"] == 6800
    assert by_symbol["WMATIC"]["liquidation_threshold_bps"] == 8250  # pins the word-2 decode
    assert by_symbol["WMATIC"]["address"] == _WMATIC_POLYGON  # address comes from on-chain enumeration
    assert by_symbol["USDC"]["borrowing_enabled"] is True
    assert by_symbol["USDC"]["usage_as_collateral_enabled"] is True
    assert by_symbol["USDC"]["is_active"] is True
    assert by_symbol["USDC"]["ltv_bps"] == 7000
    assert by_symbol["USDC"]["liquidation_threshold_bps"] == 8250

    # Fail-open: a dead reserve read surfaces an error row, ALL flags stay None
    # (Empty != Zero — not fabricated booleans), and the rest is still reported.
    assert by_symbol["DAI"]["borrowing_enabled"] is None
    assert by_symbol["DAI"]["is_active"] is None
    assert by_symbol["DAI"]["is_frozen"] is None
    assert by_symbol["DAI"]["liquidation_threshold_bps"] is None
    assert by_symbol["DAI"]["error"]


@pytest.mark.asyncio
async def test_list_lending_reserves_asset_filter_golden() -> None:
    enumeration = [("WMATIC", _WMATIC_POLYGON), ("USDC", _USDC_POLYGON)]
    gateway = _reserves_gateway(
        enumeration,
        lambda sym: _reserve_cfg_hex(ltv=6800, usage=True, borrow=False, active=True, frozen=False),
    )
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3", "asset": "wmatic"},  # case-insensitive
    )
    assert result.status == "success", result.error
    assert result.data["count"] == 1
    assert result.data["reserves"][0]["symbol"] == "WMATIC"


@pytest.mark.asyncio
async def test_list_lending_reserves_unknown_asset_returns_error() -> None:
    enumeration = [("WMATIC", _WMATIC_POLYGON), ("USDC", _USDC_POLYGON)]
    gateway = _reserves_gateway(
        enumeration,
        lambda sym: _reserve_cfg_hex(ltv=0, usage=False, borrow=False, active=True, frozen=False),
    )
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3", "asset": "DOGE"},
    )
    assert result.status == "error"
    assert "not a listed reserve" in result.error["message"]


@pytest.mark.asyncio
async def test_list_lending_reserves_truncation_is_signaled() -> None:
    # A market exceeding the 512 safety cap must NOT be silently truncated:
    # the response flags truncated=true and reports total_matched.
    enumeration = [(f"T{i}", "0x" + f"{i:040x}") for i in range(513)]
    gateway = _reserves_gateway(
        enumeration,
        lambda sym: _reserve_cfg_hex(ltv=5000, usage=True, borrow=True, active=True, frozen=False),
    )
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3"},
    )
    assert result.status == "success", result.error
    d = result.data
    assert d["truncated"] is True
    assert d["truncation_reason"] == "max_reserves"
    assert d["count"] == 512
    assert d["total_matched"] == 513


@pytest.mark.asyncio
async def test_list_lending_reserves_asset_past_cap_still_found() -> None:
    # Filter runs BEFORE the DOS cap, so an asset sitting past the 512-reserve
    # boundary is still found rather than wrongly reported as not-active.
    enumeration = [(f"T{i}", "0x" + f"{i:040x}") for i in range(600)]
    gateway = _reserves_gateway(
        enumeration,
        lambda sym: _reserve_cfg_hex(ltv=5000, usage=True, borrow=True, active=True, frozen=False),
    )
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3", "asset": "T599"},
    )
    assert result.status == "success", result.error
    assert result.data["count"] == 1
    assert result.data["truncated"] is False
    assert result.data["reserves"][0]["symbol"] == "T599"


@pytest.mark.asyncio
async def test_list_lending_reserves_latency_budget_truncates(monkeypatch) -> None:
    # An exhausted wall-clock budget stops the per-reserve fan-out early and is
    # surfaced (truncated + truncation_reason), never silently returning a
    # partial list as if complete. Force the budget negative so the check trips
    # on the first iteration, deterministically (no dependence on the clock).
    from almanak.framework.agent_tools import executor as _executor_mod

    monkeypatch.setattr(_executor_mod, "_LENDING_RESERVES_LATENCY_BUDGET_S", -1.0)
    enumeration = [("WMATIC", _WMATIC_POLYGON), ("USDC", _USDC_POLYGON), ("DAI", _DAI_POLYGON)]
    gateway = _reserves_gateway(
        enumeration,
        lambda sym: _reserve_cfg_hex(ltv=5000, usage=True, borrow=True, active=True, frozen=False),
    )
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3"},
    )
    assert result.status == "success", result.error
    d = result.data
    assert d["truncated"] is True
    assert d["truncation_reason"] == "latency_budget_exceeded"
    assert d["total_matched"] == 3
    assert d["count"] < d["total_matched"]  # stopped early


@pytest.mark.asyncio
async def test_list_lending_reserves_enumeration_failure_is_fatal() -> None:
    # getAllReservesTokens() failing is fatal — a partial list would silently
    # reintroduce the discovery blind spot.
    def _call(req: Any, **kwargs: Any) -> Any:
        resp = MagicMock()
        resp.success = False
        resp.error = "UNAVAILABLE"
        return resp

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3"},
    )
    assert result.status == "error"
    assert "reserve enumeration failed" in result.error["message"]


@pytest.mark.asyncio
async def test_list_lending_reserves_unsupported_protocol_returns_error() -> None:
    gateway = MagicMock()
    executor = _make_reserves_executor(gateway)
    executor._policy_engine.policy.allowed_protocols = None  # reach the handler's own guard
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "venus"},  # genuinely unregistered
    )
    assert result.status == "error"
    assert "unsupported protocol" in result.error["message"]
    # VIB-4951: the supported list is registry-derived, not hardcoded.
    for proto in ("aave_v3", "compound_v3", "morpho_blue", "spark"):
        assert proto in result.error["message"]


@pytest.mark.asyncio
async def test_list_lending_reserves_unsupported_chain_returns_error() -> None:
    gateway = MagicMock()
    executor = _make_reserves_executor(gateway)
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "solana", "protocol": "aave_v3"},
    )
    assert result.status == "error"
    # Aave V3 has no PoolDataProvider on solana → fail before any RPC.
    gateway.rpc.Call.assert_not_called()


def test_decode_all_reserves_tokens_roundtrip_and_garbage() -> None:
    """Pure decoder: round-trips a TokenData[] payload, returns None on garbage."""
    from almanak.connectors._strategy_base.base.lending.aave_helpers import decode_all_reserves_tokens

    tokens = [("WMATIC", _WMATIC_POLYGON), ("USDC", _USDC_POLYGON)]
    decoded = decode_all_reserves_tokens("0x" + _all_reserves_hex(tokens))
    assert decoded == [("WMATIC", _WMATIC_POLYGON), ("USDC", _USDC_POLYGON)]
    assert decode_all_reserves_tokens("0x1234") is None
    assert decode_all_reserves_tokens("") is None
    # Oversized declared array length is rejected before the full eth_abi decode.
    oversized = (32).to_bytes(32, "big") + (10**9).to_bytes(32, "big")
    assert decode_all_reserves_tokens("0x" + oversized.hex()) is None


def test_reserves_handler_has_no_aave_coupling() -> None:
    """VIB-4951 static guard: the reserves path carries no Aave-specific
    imports and no protocol literal branches — dispatch is registry-only."""
    import inspect

    src = inspect.getsource(ToolExecutor._execute_list_lending_reserves)
    assert "aave_helpers" not in src
    assert 'protocol != "aave_v3"' not in src and 'protocol == "aave_v3"' not in src
    assert "STRATEGY_AGENT_READ_REGISTRY" in src


@pytest.mark.asyncio
async def test_list_lending_reserves_morpho_static_golden() -> None:
    """Morpho Blue rows come from the static market catalogue — zero RPC."""
    gateway = MagicMock()
    executor = _make_reserves_executor(gateway)
    executor._policy_engine.policy.allowed_protocols = None
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "morpho_blue"},
    )
    assert result.status == "success", result.error
    gateway.rpc.Call.assert_not_called()
    rows = {r["symbol"]: r for r in result.data["reserves"]}
    wsteth_usdc = rows["wstETH/USDC"]
    assert wsteth_usdc["ltv_bps"] == 8600
    assert wsteth_usdc["liquidation_threshold_bps"] == 8600
    assert wsteth_usdc["borrowing_enabled"] is True
    assert wsteth_usdc["usage_as_collateral_enabled"] is True
    assert wsteth_usdc["detail"]["loan_token"] == "USDC"
    assert result.data["pool_data_provider"].lower().startswith("0xbbbbbbbbbb")


@pytest.mark.asyncio
async def test_list_lending_reserves_compound_comet_golden() -> None:
    """Compound comet semantics: base row static (borrowable, not collateral);
    collateral rows decode LIVE getAssetInfoByAddress factors to bps."""

    def _asset_info_blob() -> str:
        words = [0] * 8
        words[2] = 0xDEAD  # priceFeed (nonzero)
        words[4] = 825 * 10**15  # borrowCollateralFactor = 0.825e18
        words[5] = 895 * 10**15  # liquidateCollateralFactor = 0.895e18
        return "".join(f"{w:064x}" for w in words)

    def _call(req: Any, **kwargs: Any) -> Any:
        assert req.id.startswith("compound_asset_info:")
        return _rpc_ok(_asset_info_blob())

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    executor = _make_reserves_executor(gateway)
    executor._policy_engine.policy.allowed_protocols = None
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "compound_v3"},
    )
    assert result.status == "success", result.error
    rows = result.data["reserves"]
    base = next(r for r in rows if (r.get("detail") or {}).get("role") == "base" and r["symbol"] == "USDC")
    assert base["borrowing_enabled"] is True
    assert base["usage_as_collateral_enabled"] is False
    assert base["ltv_bps"] is None  # not applicable to a comet base asset
    collateral = next(r for r in rows if "role" not in (r.get("detail") or {}) and r["symbol"] == "WETH")
    assert collateral["borrowing_enabled"] is False
    assert collateral["usage_as_collateral_enabled"] is True
    assert collateral["ltv_bps"] == 8250
    assert collateral["liquidation_threshold_bps"] == 8950
    # Codex review (PR #3197): comets present only in the address table are
    # NOT omitted (blind spot) and aliased comet ids are NOT duplicated.
    base_rows = [r for r in rows if (r.get("detail") or {}).get("role") == "base"]
    assert {r["symbol"] for r in base_rows} >= {"USDC", "WETH", "USDT", "WSTETH", "USDS"}
    comet_keys = [r["detail"]["comet"] for r in base_rows]
    assert len(comet_keys) == len(set(comet_keys))
    uncatalogued = next(r for r in base_rows if r["symbol"] == "WSTETH")
    assert uncatalogued["detail"]["metadata"] == "uncatalogued"
    assert uncatalogued["borrowing_enabled"] is True


@pytest.mark.asyncio
async def test_list_lending_reserves_spark_reuses_aave_fork_path() -> None:
    """Spark (Aave fork) flows through the same enumeration + config decode."""
    cfg_hex = _reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False)
    gateway = _reserves_gateway([("wstETH", "0x" + "11" * 20)], lambda s: cfg_hex)
    executor = _make_reserves_executor(gateway)
    executor._policy_engine.policy.allowed_protocols = None
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "spark"},
    )
    assert result.status == "success", result.error
    row = result.data["reserves"][0]
    assert row["symbol"] == "wstETH"
    assert row["ltv_bps"] == 7000
    assert row["borrowing_enabled"] is True
    assert result.data["pool_data_provider"] == "0xFc21d6d146E6086B8359705C8b28512a983db0cb"


# ---------------------------------------------------------------------------
# VIB-4951 Multicall3 batching lane
# ---------------------------------------------------------------------------


def _mc3_enumeration(n: int) -> list[tuple[str, str]]:
    return [(f"TOK{i}", "0x" + f"{i + 1:040x}") for i in range(n)]


def _mc3_gateway(n: int, aggregate3_blob_for: Any, code: str = "0x60") -> tuple[Any, list[str]]:
    """Scripted gateway for the batched lane; records every request id/method."""
    from eth_abi import encode as _abi_encode  # noqa: F401 (used by callers)

    seen: list[str] = []

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.method == "eth_getCode":
            seen.append("getcode")
            return _rpc_ok(code.removeprefix("0x"))
        seen.append(req.id)
        if req.id == "aave_all_reserves":
            return _rpc_ok(_all_reserves_hex(_mc3_enumeration(n)))
        if req.id.startswith("multicall3_reserves:"):
            return _rpc_ok(aggregate3_blob_for(req.id))
        # serial per-reserve config fallback
        return _rpc_ok(_reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False))

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    return gateway, seen


@pytest.mark.asyncio
async def test_reserves_multicall3_batched_chunked_and_fail_open(monkeypatch: Any) -> None:
    """A large read set batches via aggregate3 in bounded chunks; a reverted
    inner call fail-opens its row only; no per-reserve serial calls happen."""
    from eth_abi import encode as _abi_encode

    from almanak.framework.agent_tools import multicall as _mc

    monkeypatch.setattr(_mc, "MULTICALL3_MAX_BATCH", 4)
    cfg = bytes.fromhex(_reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False))

    def _blob_for(req_id: str) -> str:
        start = int(req_id.split(":", 1)[1])
        size = min(4, 10 - start)
        results = [(i != 5, cfg) for i in range(start, start + size)]  # global index 5 reverts
        return _abi_encode(["(bool,bytes)[]"], [results]).hex()

    gateway, seen = _mc3_gateway(10, _blob_for)
    executor = _make_reserves_executor(gateway)
    result = await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert result.status == "success", result.error
    assert seen == ["aave_all_reserves", "getcode", "multicall3_reserves:0", "multicall3_reserves:4", "multicall3_reserves:8"]
    rows = result.data["reserves"]
    assert len(rows) == 10
    assert rows[5]["error"] == "execution reverted (multicall3 aggregate3 inner call)"
    assert rows[5]["ltv_bps"] is None
    assert all(r["ltv_bps"] == 7000 and r["error"] == "" for i, r in enumerate(rows) if i != 5)


@pytest.mark.asyncio
async def test_reserves_multicall3_absent_contract_uses_serial(monkeypatch: Any) -> None:
    """eth_getCode returning empty code means no Multicall3 — clean serial path."""
    gateway, seen = _mc3_gateway(10, lambda _id: "00", code="0x")
    executor = _make_reserves_executor(gateway)
    result = await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert result.status == "success", result.error
    assert "getcode" in seen
    assert not any(i.startswith("multicall3_reserves") for i in seen)
    assert sum(1 for i in seen if i.startswith("aave_reserve_cfg:")) == 10
    assert all(r["ltv_bps"] == 7000 for r in result.data["reserves"])


@pytest.mark.asyncio
async def test_reserves_multicall3_bad_blob_falls_back_serial() -> None:
    """An undecodable aggregate3 blob degrades to serial — rows still filled,
    never fabricated from a malformed batch response."""
    gateway, seen = _mc3_gateway(10, lambda _id: "deadbeef")
    executor = _make_reserves_executor(gateway)
    result = await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert result.status == "success", result.error
    assert any(i.startswith("multicall3_reserves") for i in seen)  # batch attempted
    assert sum(1 for i in seen if i.startswith("aave_reserve_cfg:")) == 10  # serial completed
    assert all(r["ltv_bps"] == 7000 and r["error"] == "" for r in result.data["reserves"])


@pytest.mark.asyncio
async def test_multicall3_probe_fault_is_not_cached(monkeypatch: Any) -> None:
    """codex review (PR #3197): a TRANSIENT probe failure must not pin the
    serial lane for the executor lifetime — only MEASURED outcomes cache."""
    from eth_abi import encode as _abi_encode

    from almanak.framework.agent_tools import multicall as _mc

    monkeypatch.setattr(_mc, "MULTICALL3_MAX_BATCH", 100)
    cfg = bytes.fromhex(_reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False))
    state = {"probe_calls": 0}

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.method == "eth_getCode":
            state["probe_calls"] += 1
            if state["probe_calls"] == 1:
                raise RuntimeError("transient gateway hiccup")
            return _rpc_ok("60")
        if req.id == "aave_all_reserves":
            return _rpc_ok(_all_reserves_hex(_mc3_enumeration(10)))
        if req.id.startswith("multicall3_reserves:"):
            results = [(True, cfg)] * 10
            return _rpc_ok(_abi_encode(["(bool,bytes)[]"], [results]).hex())
        return _rpc_ok(_reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False))

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    executor = _make_reserves_executor(gateway)

    # Call 1: probe faults -> serial lane, fault NOT cached.
    r1 = await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert r1.status == "success" and all(x["ltv_bps"] == 7000 for x in r1.data["reserves"])
    assert executor._multicall3_probe_cache == {}

    # Call 2: probe retried, succeeds -> batched lane; POSITIVE outcome cached.
    r2 = await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert r2.status == "success"
    assert state["probe_calls"] == 2
    assert executor._multicall3_probe_cache == {("polygon", ""): True}

    # Call 3: cache hit — probe NOT re-run.
    await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert state["probe_calls"] == 2


@pytest.mark.asyncio
async def test_multicall3_measured_empty_code_is_cached(monkeypatch: Any) -> None:
    """A MEASURED empty-code miss (contract genuinely absent) caches False —
    the probe is not re-run per call on chains without Multicall3."""
    state = {"probe_calls": 0}

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.method == "eth_getCode":
            state["probe_calls"] += 1
            return _rpc_ok("")  # 0x — measured: no contract
        if req.id == "aave_all_reserves":
            return _rpc_ok(_all_reserves_hex(_mc3_enumeration(10)))
        return _rpc_ok(_reserve_cfg_hex(ltv=7000, usage=True, borrow=True, active=True, frozen=False))

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    executor = _make_reserves_executor(gateway)
    await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    await executor.execute("list_lending_reserves", {"chain": "polygon", "protocol": "aave_v3"})
    assert state["probe_calls"] == 1
    assert executor._multicall3_probe_cache == {("polygon", ""): False}
