"""Unit tests for VIB-3701: Aave V3 collateral eligibility pre-flight check.

Confirms `_compile_supply_aave_compatible` consults the on-chain reserve
configuration (`PoolDataProvider.getReserveConfigurationData`) before emitting
a `setUserUseReserveAsCollateral` TX. Asset-level reverts like
`0x0cafc072 UnderlyingCannotBeUsedAsCollateral` (USDE on Aave V3 Ethereum,
DAI on Polygon V3, ...) now surface as a typed compile-time error instead of
an opaque on-chain revert.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.base.lending import aave_helpers as cl
from almanak.framework.intents import SupplyIntent
from almanak.framework.intents.compiler_models import CompilationStatus

TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_POOL = "0xpooladdress000000000000000000000000000001"
TEST_ASSET_ADDR = "0x" + "ab" * 20

AAVE_ADAPTER_CLS = "almanak.framework.intents.compiler_adapters.AaveV3Adapter"


def _word(value: int) -> str:
    """ABI-encode a uint256 / bool word as 64 hex chars."""
    return f"{value:064x}"


def _encode_reserve_config(
    *,
    decimals: int = 6,
    ltv: int = 7500,
    liquidation_threshold: int = 8000,
    liquidation_bonus: int = 10500,
    reserve_factor: int = 1000,
    usage_as_collateral_enabled: bool = True,
    borrowing_enabled: bool = True,
    stable_borrow_rate_enabled: bool = False,
    is_active: bool = True,
    is_frozen: bool = False,
) -> str:
    """Mimic the ABI-encoded return of `getReserveConfigurationData(address)`."""
    words = [
        decimals,
        ltv,
        liquidation_threshold,
        liquidation_bonus,
        reserve_factor,
        1 if usage_as_collateral_enabled else 0,
        1 if borrowing_enabled else 0,
        1 if stable_borrow_rate_enabled else 0,
        1 if is_active else 0,
        1 if is_frozen else 0,
    ]
    return "0x" + "".join(_word(w) for w in words)


def _mock_token(symbol: str = "USDC", decimals: int = 6) -> MagicMock:
    tok = MagicMock()
    tok.symbol = symbol
    tok.address = TEST_ASSET_ADDR
    tok.decimals = decimals
    tok.is_native = False
    tok.to_dict.return_value = {
        "symbol": symbol,
        "address": tok.address,
        "decimals": decimals,
        "is_native": False,
    }
    return tok


def _mock_compiler(*, gateway_response: str | None, chain: str = "ethereum") -> MagicMock:
    """Build a compiler mock with an optional gateway client reply."""
    compiler = MagicMock()
    compiler.chain = chain
    compiler.wallet_address = TEST_WALLET
    compiler.rpc_timeout = 5.0
    compiler._is_solana_chain.return_value = False
    compiler._format_amount.side_effect = lambda amount, decimals: str(amount)
    compiler._get_wrapped_native_address.return_value = "0x" + "ee" * 20

    approve_tx = cl.TransactionData(
        to="0x" + "cc" * 20,
        value=0,
        data="0x0000",
        gas_estimate=60_000,
        description="approve",
        tx_type="approve",
    )
    compiler._build_approve_tx.return_value = [approve_tx]

    if gateway_response is None:
        compiler._gateway_client = None
    else:
        gateway = MagicMock()
        gateway.is_connected = True
        rpc_resp = MagicMock()
        rpc_resp.success = True
        # Match the wire contract: gateway.rpc.Call wraps eth_call hex results
        # in json.dumps (see almanak/gateway/services/rpc_service.py), so a real
        # response.result looks like '"0x..."'. Tests that hand-feed bare hex
        # would mask a JSON-decode regression in the eligibility parser.
        rpc_resp.result = json.dumps(gateway_response)
        rpc_resp.error = ""
        gateway.rpc.Call.return_value = rpc_resp
        compiler._gateway_client = gateway

    return compiler


def _supply_intent(*, use_as_collateral: bool = True, protocol: str = "aave_v3") -> SupplyIntent:
    return SupplyIntent(
        protocol=protocol,
        token="USDC",
        amount=Decimal("100"),
        use_as_collateral=use_as_collateral,
    )


@patch(AAVE_ADAPTER_CLS)
def test_collateral_blocked_when_usage_disabled(mock_adapter_cls):
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(usage_as_collateral_enabled=False, ltv=0))
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.FAILED
    assert "not collateral-eligible" in result.error
    assert "USDC" in result.error
    assert "ethereum" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_collateral_blocked_when_ltv_zero(mock_adapter_cls):
    """Polygon DAI / USDC.e style: usage flag stays True but LTV got zeroed out."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(usage_as_collateral_enabled=True, ltv=0))
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.FAILED
    assert "ltv=0" in result.error.lower()


@patch(AAVE_ADAPTER_CLS)
def test_eligible_asset_compiles_with_collateral_tx(mock_adapter_cls):
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=7500))
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
    mock_adapter.estimate_set_collateral_gas.return_value = 70_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.SUCCESS
    assert "lending_set_collateral" in [tx.tx_type for tx in result.transactions]


@patch(AAVE_ADAPTER_CLS)
def test_collateral_check_skipped_when_use_as_collateral_false(mock_adapter_cls):
    """Supply-only flows must not run the *collateral* eligibility check.

    They DO still run the reserve-active pre-flight (VIB-3749), so a frozen
    reserve fails fast even on supply-only flows. We assert here only that
    the collateral-specific pre-flight is skipped (so the LTV/usageAsCollateral
    decoding doesn't run for an intent that never builds the
    ``setUserUseReserveAsCollateral`` TX).
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=7500))
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=False)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.SUCCESS
    # Reserve-active pre-flight runs (VIB-3749) but only once — no second call
    # for collateral eligibility, since `use_as_collateral=False`. The single
    # call must route to the reserve-active selector path (cached on the
    # _lending_reserve_active_cache, not _aave_collateral_eligibility_cache).
    assert compiler._gateway_client.rpc.Call.call_count == 1
    assert isinstance(getattr(compiler, "_lending_reserve_active_cache", None), dict)


@patch(AAVE_ADAPTER_CLS)
def test_v2_fork_runs_reserve_active_preflight(mock_adapter_cls):
    """Radiant V2 (V2 fork) runs the reserve-active pre-flight (VIB-3749).

    The collateral pre-flight is still skipped — V2 forks expose the same
    `getReserveConfigurationData` ABI for active/frozen status but the
    `setUserUseReserveAsCollateral` semantics differ enough that we don't
    interpret LTV/usageAsCollateral for them. Bonus: V2 forks have their
    own `LENDING_POOL_DATA_PROVIDERS` entry, so the call routes to
    Radiant-specific addresses, not Aave V3's.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=0))
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True  # Radiant V2 sets this
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
    mock_adapter.estimate_set_collateral_gas.return_value = 70_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True, protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    # Active and unfrozen reserve in this fixture (the LTV=0 only matters for
    # the V3 collateral check, which is correctly skipped for V2 forks).
    assert result.status == CompilationStatus.SUCCESS
    # Reserve-active pre-flight ran exactly once — and routed to the Radiant V2
    # PoolDataProvider, not Aave V3's.
    assert compiler._gateway_client.rpc.Call.call_count == 1
    args, _ = compiler._gateway_client.rpc.Call.call_args
    request = args[0]
    # Radiant V2 Ethereum AaveProtocolDataProvider (verified from Radiant docs).
    assert "0x362f3BB63Cff83bd169aE1793979E9e537993813" in request.params


@patch(AAVE_ADAPTER_CLS)
def test_v2_fork_blocks_compile_when_pool_returns_json_wrapped_0x(mock_adapter_cls):
    """V2 fork (Radiant V2) regression for the load-bearing VIB-3749 failure mode.

    Production wire-format: gateway.rpc.Call wraps eth_call results in
    json.dumps. A shut-down PoolDataProvider proxy returns `success=True` with
    `result='"0x"'` (literal JSON-quoted "0x"). The pre-flight must classify
    that as frozen and FAIL the compile — not fail-open and emit a SUPPLY that
    reverts on-chain.

    Pairs with `test_empty_0x_response_classified_as_frozen` (the
    Aave-V3-shaped twin) and explicitly anchors the V2-fork code path so a
    regression that re-introduced the fail-open branch couldn't slip past
    this suite. CodeRabbit follow-up.
    """
    compiler = _mock_compiler(gateway_response="0x")
    # Sanity-check the fixture: result must arrive json-encoded (the production
    # shape). A bare-hex regression in the fixture would mask the bug.
    assert compiler._gateway_client.rpc.Call.return_value.result == json.dumps("0x")

    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True  # Radiant V2 path
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True, protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.FAILED
    # Error must surface the specific frozen / inactive signal so a strategy
    # can match on it deterministically.
    assert "is not active" in result.error
    assert "isFrozen=True" in result.error
    # Routed to the Radiant V2 PoolDataProvider — not Aave V3's.
    args, _ = compiler._gateway_client.rpc.Call.call_args
    assert "0x362f3BB63Cff83bd169aE1793979E9e537993813" in args[0].params


@patch(AAVE_ADAPTER_CLS)
def test_fails_open_when_gateway_unavailable(mock_adapter_cls):
    """No gateway → can't pre-flight; rely on on-chain revert as final guard."""
    compiler = _mock_compiler(gateway_response=None)
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
    mock_adapter.estimate_set_collateral_gas.return_value = 70_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    # Must still produce calldata; we must not block compilation when the
    # pre-flight cannot run. The user gets the on-chain revert if they
    # picked an ineligible asset, which is the same as the pre-VIB-3701
    # behavior.
    assert result.status == CompilationStatus.SUCCESS


@patch(AAVE_ADAPTER_CLS)
def test_fails_open_on_rpc_success_false(mock_adapter_cls):
    """response.success=False — same fail-open as gateway exception."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=7500))
    compiler._gateway_client.rpc.Call.return_value.success = False
    compiler._gateway_client.rpc.Call.return_value.result = ""
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
    mock_adapter.estimate_set_collateral_gas.return_value = 70_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.SUCCESS


@patch(AAVE_ADAPTER_CLS)
def test_short_partial_response_fails_open(mock_adapter_cls):
    """Truncated partial response (some bytes but < 320) → fail-open.

    A `0x` empty payload IS now classified as frozen (CodeRabbit follow-up:
    real-world Radiant V2 Arbitrum proxy returns exactly that). But a
    *partial* response (e.g. `0x12...` with fewer than 320 bytes) still
    indicates an RPC anomaly we cannot interpret, so fail-open as before.
    """
    # 64 hex chars = 32 bytes = 1 ABI word, well below the expected 320.
    short_payload = "0x" + ("ab" * 32)
    compiler = _mock_compiler(gateway_response=short_payload)
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
    mock_adapter.estimate_set_collateral_gas.return_value = 70_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.SUCCESS


@patch(AAVE_ADAPTER_CLS)
def test_empty_0x_response_classified_as_frozen(mock_adapter_cls):
    """Real-world Radiant V2 Arbitrum: shut-down proxy returns `0x` with
    `success=True`. Pre-flight must classify that as frozen, not fail-open.
    Without this guard the SUPPLY would compile cleanly and revert on-chain
    (the original VIB-3749 bug).
    """
    compiler = _mock_compiler(gateway_response="0x")
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True, protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.FAILED
    assert "is not active" in result.error
    assert "isFrozen=True" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_eligibility_result_is_cached(mock_adapter_cls):
    """Two compiles in a row must hit the gateway exactly once per cache.

    With VIB-3749 we run two distinct pre-flights on a setCollateral compile:
      - reserve-active (cached in `_lending_reserve_active_cache`)
      - collateral-eligibility (cached in `_aave_collateral_eligibility_cache`)
    Both share the same gateway, so the first compile makes 2 calls and
    subsequent compiles make 0 calls. The cache contract is what matters:
    a strategy iterating N times pays the gateway tax exactly twice, not 2*N.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=7500))
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
    mock_adapter.estimate_set_collateral_gas.return_value = 70_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))
    cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))
    cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    # 1 call for reserve-active + 1 call for collateral-eligibility, then both
    # cached for the remaining two compiles.
    assert compiler._gateway_client.rpc.Call.call_count == 2


def test_helper_returns_none_for_unsupported_chain():
    """The chain table has no Aave V3 deployment for berachain → no-op."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=0), chain="berachain")
    result = cl._check_aave_v3_collateral_eligibility(compiler, asset_address=TEST_ASSET_ADDR, asset_symbol="USDC")
    assert result is None
    compiler._gateway_client.rpc.Call.assert_not_called()


def test_asset_not_collateral_eligible_error_subclasses_value_error() -> None:
    assert issubclass(cl.AssetNotCollateralEligibleError, ValueError)


def test_helper_decodes_json_wrapped_gateway_result() -> None:
    """Production wire contract: gateway.rpc.Call wraps eth_call results in
    json.dumps (see almanak/gateway/services/rpc_service.py). A bare hex string
    bypassing json.dumps would shift the ABI word offsets and silently
    fail-open the eligibility check — that regression must trip this test.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(usage_as_collateral_enabled=False, ltv=0))
    # Sanity-check the fixture: result must arrive json-encoded, not bare hex.
    assert compiler._gateway_client.rpc.Call.return_value.result.startswith('"0x')

    reason = cl._check_aave_v3_collateral_eligibility(compiler, asset_address=TEST_ASSET_ADDR, asset_symbol="USDC")
    assert reason is not None
    assert "not collateral-eligible" in reason


def test_helper_does_not_cache_transient_gateway_failure() -> None:
    """A transient gateway exception on iteration N must not permanently
    suppress the pre-flight on iteration N+1 (CodeRabbit-flagged regression)."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(ltv=7500))
    boom_then_ok = compiler._gateway_client.rpc.Call
    boom_then_ok.side_effect = [
        RuntimeError("network blip"),
        boom_then_ok.return_value,
    ]

    first = cl._check_aave_v3_collateral_eligibility(compiler, asset_address=TEST_ASSET_ADDR, asset_symbol="USDC")
    second = cl._check_aave_v3_collateral_eligibility(compiler, asset_address=TEST_ASSET_ADDR, asset_symbol="USDC")
    # First call fails open (no cached miss); second call retries and resolves.
    assert first is None
    assert second is None  # eligible (LTV=7500), so still None — but via a real RPC call
    assert boom_then_ok.call_count == 2
