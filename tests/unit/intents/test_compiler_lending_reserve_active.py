"""Unit tests for VIB-3749: lending pool reserve-active pre-flight check.

Confirms `_compile_supply_aave_compatible` and the strategy-facing
`assert_lending_reserve_active` helper consult the on-chain reserve
configuration before emitting a SUPPLY TX. Frozen / inactive reserves now
surface as a typed `PoolReserveFrozenError` (compile-time) or as a clean
`Intent.hold(...)` (strategy-time) instead of an opaque on-chain revert.

Reusable across any Aave-compatible pool — Aave V3 markets where governance
pauses a reserve, Aave V2 forks (Radiant V2) whose pool was frozen post-attack,
and any future fork that exposes the same `getReserveConfigurationData(address)`
selector.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.base.lending import aave_helpers as cl
from almanak.framework.connectors.base.lending.aave_helpers import (
    PoolReserveFrozenError,
    assert_lending_reserve_active,
)
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


def _mock_compiler(*, gateway_response: str | None, chain: str = "arbitrum") -> MagicMock:
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
    protocol: str = "radiant_v2",
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
def test_radiant_v2_frozen_reserve_blocks_compile(mock_adapter_cls):
    """Generic VIB-3749 case: a radiant_v2 reserve reporting ``isFrozen=True``
    must surface as a typed compile-time failure rather than producing
    calldata that would revert on-chain.

    Pre-#1842 this test pinned the *Arbitrum* deployment because the post-Oct-2024
    pool surfaced as frozen-reserve. Arbitrum is now excluded at the address-table
    level (compile fails earlier with "not available on chain"), so the helper's
    behaviour is exercised against ethereum here. The mechanism is reusable across
    chains: any future Aave V2 fork reserve governance pauses will land on this
    same path.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="ethereum")
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.FAILED
    assert "is not active" in result.error
    assert "isFrozen=True" in result.error
    assert "radiant_v2" in result.error
    assert "ethereum" in result.error


def test_radiant_v2_arbitrum_supply_compile_fails_with_not_available_message():
    """Regression guard for issues #1842 / #1847 / #1889.

    With ``LENDING_POOL_ADDRESSES["arbitrum"]["radiant_v2"]`` removed, the real
    ``AaveV3Adapter`` falls back to the zero address. The compile-time supply
    path returns ``CompilationStatus.FAILED`` with a "not available on chain"
    error long before the frozen-reserve pre-flight (and any RPC) runs. This
    test exercises the *real* adapter (no mocking) so a future re-introduction
    of the dead Arbitrum entry would silently restart routing user funds
    through the stub pool — and this assertion would catch it.
    """
    compiler = _mock_compiler(gateway_response=None, chain="arbitrum")

    intent = _supply_intent(protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.FAILED
    assert "not available on chain" in result.error
    assert "arbitrum" in result.error
    assert "radiant_v2" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_radiant_v2_inactive_reserve_blocks_compile(mock_adapter_cls):
    """isActive=False is also a hard block (e.g. retired reserve)."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_active=False), chain="ethereum")
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.FAILED
    assert "isActive=False" in result.error


@patch(AAVE_ADAPTER_CLS)
def test_active_unfrozen_radiant_reserve_compiles(mock_adapter_cls):
    """Healthy reserve: compile must succeed and produce SUPPLY calldata."""
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(), chain="ethereum")
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    assert result.status == CompilationStatus.SUCCESS
    tx_types = [tx.tx_type for tx in result.transactions]
    assert "lending_supply" in tx_types


@patch(AAVE_ADAPTER_CLS)
def test_aave_v3_frozen_reserve_blocks_compile(mock_adapter_cls):
    """The pre-flight is reusable for any Aave V2 fork OR Aave V3 market.

    If governance ever freezes an Aave V3 reserve (the WETH-on-Arbitrum
    incident from 2026-04-20 is the closest precedent), the same pre-flight
    short-circuits before any on-chain submission.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="arbitrum")
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = False
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
    mock_adapter._is_v2_fork = True
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="radiant_v2")
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("0.5"))

    # Must still produce calldata; we must not block compilation when the
    # pre-flight cannot run. The user gets the on-chain revert if they
    # picked a frozen asset, which is the same as the pre-VIB-3749 behavior.
    assert result.status == CompilationStatus.SUCCESS


@patch(AAVE_ADAPTER_CLS)
def test_chain_without_data_provider_fails_open(mock_adapter_cls):
    """Chains where Radiant V2 isn't deployed must not block compile.

    The adapter's pool_address-zero check still rejects unsupported chains
    upstream — this test exercises the explicit data-provider lookup miss.
    """
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(), chain="berachain")
    mock_adapter = MagicMock()
    mock_adapter._is_v2_fork = True
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(protocol="radiant_v2")
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
    # Use ``ethereum`` because it has a registered radiant_v2 data provider.
    # The historical "arbitrum" parametrisation was retired alongside the
    # arbitrum entry in ``LENDING_POOL_DATA_PROVIDERS`` (#1842 / #1847 /
    # #1889). The helper's behaviour is chain-agnostic — what we exercise
    # here is the gateway-decoded ``isFrozen`` path.
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="ethereum")
    with pytest.raises(PoolReserveFrozenError) as exc_info:
        assert_lending_reserve_active(
            compiler,
            asset_address=TEST_ASSET_ADDR,
            asset_symbol="WETH",
            protocol="radiant_v2",
        )
    msg = str(exc_info.value)
    assert "WETH" in msg
    assert "radiant_v2" in msg
    assert "isFrozen=True" in msg


def test_assert_helper_raises_on_inactive_reserve():
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_active=False), chain="ethereum")
    with pytest.raises(PoolReserveFrozenError):
        assert_lending_reserve_active(
            compiler,
            asset_address=TEST_ASSET_ADDR,
            asset_symbol="WETH",
            protocol="radiant_v2",
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
            protocol="radiant_v2",
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
        protocol="radiant_v2",
    )


def test_assert_helper_caches_result():
    """The strategy-facing helper shares the compiler-side cache — repeated
    calls within a strategy iteration loop must not re-hit the gateway.
    """
    # Ethereum is the only chain where radiant_v2 has a registered
    # PoolDataProvider; see ``test_assert_helper_raises_on_frozen_reserve``.
    compiler = _mock_compiler(gateway_response=_encode_reserve_config(is_frozen=True), chain="ethereum")
    for _ in range(5):
        with pytest.raises(PoolReserveFrozenError):
            assert_lending_reserve_active(
                compiler,
                asset_address=TEST_ASSET_ADDR,
                asset_symbol="WETH",
                protocol="radiant_v2",
            )
    assert compiler._gateway_client.rpc.Call.call_count == 1


def test_assert_helper_routes_to_correct_data_provider():
    """Ethereum Radiant V2 uses 0x362f...3813. Arbitrum has no registered
    PoolDataProvider for radiant_v2 (issues #1842 / #1847 / #1889 — the pool
    was reduced to a stub post-Oct-2024 attack), so the helper fails open
    without hitting the gateway.
    """
    compiler_eth = _mock_compiler(gateway_response=_encode_reserve_config(), chain="ethereum")
    assert_lending_reserve_active(
        compiler_eth,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="radiant_v2",
    )
    args, _ = compiler_eth._gateway_client.rpc.Call.call_args
    assert "0x362f3BB63Cff83bd169aE1793979E9e537993813" in args[0].params

    # Arbitrum has no provider entry — the helper must short-circuit before
    # any RPC happens (caller fails open). This is the regression guard
    # mirroring ``test_arbitrum_radiant_v2_data_provider_not_registered``
    # in tests/unit/connectors/test_radiant_v2.py.
    compiler_arb = _mock_compiler(gateway_response=_encode_reserve_config(), chain="arbitrum")
    assert_lending_reserve_active(
        compiler_arb,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="radiant_v2",
    )
    compiler_arb._gateway_client.rpc.Call.assert_not_called()


def test_resolve_pool_data_provider_falls_back_to_aave_v3_table():
    """`AAVE_V3[chain]['pool_data_provider']` must remain the source of truth
    for Aave V3 entries — `LENDING_POOL_DATA_PROVIDERS` is just the V2-fork
    extension. If the V2 entry is ever dropped, V3 must keep working.
    """
    addr = cl._resolve_pool_data_provider("ethereum", "aave_v3")
    assert addr == "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"


def test_resolve_pool_data_provider_returns_none_when_missing():
    """Unsupported (chain, protocol) — caller must fail-open."""
    assert cl._resolve_pool_data_provider("unsupported_chain", "radiant_v2") is None
    assert cl._resolve_pool_data_provider("ethereum", "unsupported_protocol") is None


def test_pool_data_provider_revert_is_treated_as_frozen():
    """A healthy data provider would never revert on
    ``getReserveConfigurationData``, so a revert is a strong "pool is broken"
    signal and must be surfaced as a frozen reserve — not silently fail-open
    like a network error would.

    Originally exercised against Arbitrum, whose PoolDataProvider reverted on
    every call after the October 2024 hack. After dropping arbitrum from the
    radiant_v2 address tables (#1842 / #1847 / #1889) we exercise the helper
    against ethereum, which still has a registered provider — the
    revert-handling behaviour itself is chain-agnostic.
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
        protocol="radiant_v2",
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
            protocol="radiant_v2",
        )


def test_pool_data_provider_network_error_still_fails_open():
    """Distinguish revert from network-error: only revert is a "pool broken"
    signal. A `success=False` without revert wording (e.g. timeout, rate
    limit) must still fail-open so transient infra issues don't wedge the
    strategy.
    """
    # ``radiant_v2`` is registered on ``ethereum`` (not arbitrum, where it
    # was dropped per #1842/#1847/#1889). Routing the test through ethereum
    # ensures ``_resolve_pool_data_provider`` returns a real address and the
    # mocked gateway RPC actually runs — otherwise the helper short-circuits
    # before the network-error branch is exercised and this test becomes a
    # silent no-op.
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
        protocol="radiant_v2",
        pre_flight_label="test",
    )
    assert config is None  # Fail-open

    # The helper must NOT raise on a transient network error.
    assert_lending_reserve_active(
        compiler,
        asset_address=TEST_ASSET_ADDR,
        asset_symbol="WETH",
        protocol="radiant_v2",
    )
