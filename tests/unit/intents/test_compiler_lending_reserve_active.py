"""Unit tests for VIB-3749: lending pool reserve-active pre-flight check.

Confirms `_compile_supply_aave_compatible` and the strategy-facing
`assert_lending_reserve_active` helper consult the on-chain reserve
configuration before emitting a SUPPLY TX. Frozen / inactive reserves now
surface as a typed `PoolReserveFrozenError` (compile-time) or as a clean
`Intent.hold(...)` (strategy-time) instead of an opaque on-chain revert.

Reusable across any Aave-compatible pool — Aave V3 markets where governance
pauses a reserve, and any future fork that exposes the same
`getReserveConfigurationData(address)` selector.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending import aave_helpers as cl
from almanak.connectors._strategy_base.base.lending.aave_helpers import (
    PoolReserveFrozenError,
    assert_lending_reserve_active,
)
from almanak.framework.intents import SupplyIntent
from almanak.framework.intents.compiler_models import CompilationStatus

TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_POOL = "0xpooladdress000000000000000000000000000001"
TEST_ASSET_ADDR = "0x" + "ab" * 20

# Aave V3 Ethereum AaveProtocolDataProvider.
ETH_AAVE_V3_DATA_PROVIDER = "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"

AAVE_ADAPTER_CLS = "almanak.framework.intents.compiler_adapters.AaveV3Adapter"


def _word(value: int) -> str:
    """ABI-encode a uint256 / bool word as 64 hex chars."""
    return f"{value:064x}"


def _encode_reserve_config(
    *,
    decimals: int = 18,
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


def _mock_token(symbol: str = "WETH", decimals: int = 18) -> MagicMock:
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
        # in json.dumps (see almanak/gateway/services/rpc_service.py).
        rpc_resp.result = json.dumps(gateway_response)
        rpc_resp.error = ""
        gateway.rpc.Call.return_value = rpc_resp
        compiler._gateway_client = gateway

    return compiler


def _supply_intent(
    *,
    use_as_collateral: bool = False,
    protocol: str = "aave_v3",
    token: str = "WETH",
) -> SupplyIntent:
    return SupplyIntent(
        protocol=protocol,
        token=token,
        amount=Decimal("0.5"),
        use_as_collateral=use_as_collateral,
    )


# =============================================================================
# Connector / compile-time pre-flight
# =============================================================================


@patch(AAVE_ADAPTER_CLS)
def test_frozen_reserve_blocks_compile(mock_adapter_cls):
    """Generic VIB-3749 case: a reserve reporting ``isFrozen=True`` must
    surface as a typed compile-time failure rather than producing calldata
    that would revert on-chain. The mechanism is reusable across chains and
    protocols: any reserve a governance pauses lands on this same path.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="ethereum")
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="aave_v3")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.FAILED
    assert "is not active" in result.error
    assert "isFrozen=True" in result.error
    assert "aave_v3" in result.error
    assert "ethereum" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_inactive_reserve_blocks_compile(mock_adapter_cls):
    """isActive=False is also a hard block (e.g. retired reserve)."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_active=False), chain="ethereum")
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="aave_v3")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.FAILED
    assert "isActive=False" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_active_unfrozen_reserve_compiles(mock_adapter_cls):
    """Healthy reserve: compile must succeed and produce SUPPLY calldata."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(), chain="ethereum")
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="aave_v3")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.SUCCESS
    tx_types = [tx.tx_type for tx in result.transactions]
    assert "lending_supply" in tx_types


@patch(AAVE_ADAPTER_CLS)
def test_aave_v3_frozen_reserve_blocks_compile(mock_adapter_cls):
    """The pre-flight is reusable for any Aave V3 market.

    If governance ever freezes an Aave V3 reserve (the WETH-on-Arbitrum
    incident from 2026-04-20 is the closest precedent), the same pre-flight
    short-circuits before any on-chain submission.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="arbitrum")
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="aave_v3")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.FAILED
    assert "is not active" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_supply_fails_open_when_gateway_unavailable(mock_adapter_cls):
    """No gateway → can't pre-flight; rely on on-chain revert as final guard."""
    compiler = _mock_compiler(gateway_response=None, chain="arbitrum")
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="aave_v3")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    # Must still produce calldata; we must not block compilation when the
    # pre-flight cannot run. The user gets the on-chain revert if they
    # picked a frozen asset, which is the same as the pre-VIB-3749 behavior.
    assert result.status == CompilationStatus.SUCCESS


@patch(AAVE_ADAPTER_CLS)
def test_chain_without_data_provider_fails_open(mock_adapter_cls):
    """Chains with no registered data provider must not block compile.

    The adapter's pool_address-zero check still rejects unsupported chains
    upstream — this test exercises the explicit data-provider lookup miss.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(), chain="berachain")
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="aave_v3")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))
    assert result.status == CompilationStatus.SUCCESS
    # Call must have been short-circuited before the gateway was hit.
    compiler._gateway_client.rpc.Call.assert_not_called()


def test_pool_reserve_frozen_error_is_value_error_subclass() -> None:
    """Strategies typically catch `ValueError` for safety; the typed error
    should still be catchable by that broader except path."""
    assert issubclass(PoolReserveFrozenError, ValueError)


# =============================================================================
# Strategy-facing helper: assert_lending_reserve_active
# =============================================================================


def test_assert_helper_raises_on_frozen_reserve():
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="ethereum")
    with pytest.raises(PoolReserveFrozenError) as exc_info:
        assert_lending_reserve_active(
            compiler,
            asset_address=TEST_ASSET_ADDR,
            asset_symbol="WETH",
            protocol="aave_v3",
        )
    msg = str(exc_info.value)
    assert "WETH" in msg
    assert "aave_v3" in msg
    assert "isFrozen=True" in msg


def test_assert_helper_raises_on_inactive_reserve():
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_active=False), chain="ethereum")
    with pytest.raises(PoolReserveFrozenError):
        assert_lending_reserve_active(
            compiler,
            asset_address=TEST_ASSET_ADDR,
            asset_symbol="WETH",
            protocol="aave_v3",
        )


def test_assert_helper_passes_on_active_reserve():
    """Healthy pool: helper must NOT raise (a plain return is the contract)."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(), chain="ethereum")
    # No exception → no false HOLD on a healthy pool.
    assert (
        assert_lending_reserve_active(
            compiler,
            asset_address=TEST_ASSET_ADDR,
            asset_symbol="WETH",
            protocol="aave_v3",
        )
        is None
    )


def test_assert_helper_fails_open_when_gateway_down():
    """No gateway → helper must NOT raise; strategy proceeds to compile and the
    compile-time pre-flight (or on-chain revert) becomes the final guard.

    Mirrors the same fail-open contract as the compile-time check — together
    they ensure a strategy never wedges on infra failures.
    """
    compiler = _mock_compiler(gateway_response=None)
    # Helper must return cleanly when the gateway is missing.
    assert_lending_reserve_active(
        compiler,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="aave_v3",
    )


def test_assert_helper_caches_result():
    """The strategy-facing helper shares the compiler-side cache — repeated
    calls within a strategy iteration loop must not re-hit the gateway.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="ethereum")
    for _ in range(5):
        with pytest.raises(PoolReserveFrozenError):
            assert_lending_reserve_active(
                compiler,
                asset_address=TEST_ASSET_ADDR,
                asset_symbol="WETH",
                protocol="aave_v3",
            )
    assert compiler._gateway_client.rpc.Call.call_count == 1


def test_assert_helper_routes_to_correct_data_provider():
    """Ethereum Aave V3 uses 0x7B4E...8a3. A chain with no registered
    PoolDataProvider (berachain) makes the helper fail open without hitting
    the gateway.
    """
    compiler_eth = _mock_compiler(gateway_response=_encode_reserve_config(), chain="ethereum")
    assert_lending_reserve_active(
        compiler_eth,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="aave_v3",
    )
    args, _ = compiler_eth._gateway_client.rpc.Call.call_args
    assert ETH_AAVE_V3_DATA_PROVIDER in args[0].params

    # A chain with no provider entry — the helper must short-circuit before
    # any RPC happens (caller fails open).
    compiler_bera = _mock_compiler(gateway_response=_encode_reserve_config(), chain="berachain")
    assert_lending_reserve_active(
        compiler_bera,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="aave_v3",
    )
    compiler_bera._gateway_client.rpc.Call.assert_not_called()


def test_resolve_pool_data_provider_falls_back_to_aave_v3_table():
    """`AAVE_V3[chain]['pool_data_provider']` is the source of truth for Aave V3
    data-provider lookups via the derived `LENDING_POOL_DATA_PROVIDERS` view.
    """
    addr = cl._resolve_pool_data_provider("ethereum", "aave_v3")
    assert addr == ETH_AAVE_V3_DATA_PROVIDER


def test_resolve_pool_data_provider_returns_none_when_missing():
    """Unsupported (chain, protocol) — caller must fail-open."""
    assert cl._resolve_pool_data_provider("unsupported_chain", "aave_v3") is None
    assert cl._resolve_pool_data_provider("ethereum", "unsupported_protocol") is None


def test_pool_data_provider_revert_is_treated_as_frozen():
    """A healthy data provider would never revert on
    ``getReserveConfigurationData``, so a revert is a strong "pool is broken"
    signal and must be surfaced as a frozen reserve — not silently fail-open
    like a network error would.
    """
    compiler = MagicMock()
    compiler.chain = "ethereum"
    compiler.rpc_timeout = 5.0
    gateway = MagicMock()
    gateway.is_connected = True
    rpc_resp = MagicMock()
    rpc_resp.success = False
    rpc_resp.result = ""
    rpc_resp.error = json.dumps({"code": 3, "message": "execution reverted"})
    gateway.rpc.Call.return_value = rpc_resp
    compiler._gateway_client = gateway

    config = cl._fetch_reserve_config(
        compiler,
        TEST_ASSET_ADDR,
        "WETH",
        protocol="aave_v3",
        pre_flight_label="test",
    )
    assert config is not None
    assert config.is_active is False
    assert config.is_frozen is True

    # The strategy-facing helper must then raise.
    with pytest.raises(PoolReserveFrozenError):
        assert_lending_reserve_active(
            compiler,
            asset_address=TEST_ASSET_ADDR,
            asset_symbol="WETH",
            protocol="aave_v3",
        )


def test_pool_data_provider_network_error_still_fails_open():
    """Distinguish revert from network-error: only revert is a "pool broken"
    signal. A `success=False` without revert wording (e.g. timeout, rate
    limit) must still fail-open so transient infra issues don't wedge the
    strategy.
    """
    compiler = MagicMock()
    compiler.chain = "ethereum"
    compiler.rpc_timeout = 5.0
    gateway = MagicMock()
    gateway.is_connected = True
    rpc_resp = MagicMock()
    rpc_resp.success = False
    rpc_resp.result = ""
    rpc_resp.error = json.dumps({"code": -32000, "message": "request timeout"})
    gateway.rpc.Call.return_value = rpc_resp
    compiler._gateway_client = gateway

    config = cl._fetch_reserve_config(
        compiler,
        TEST_ASSET_ADDR,
        "WETH",
        protocol="aave_v3",
        pre_flight_label="test",
    )
    assert config is None  # Fail-open

    # The helper must NOT raise on a transient network error.
    assert_lending_reserve_active(
        compiler,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="aave_v3",
    )
