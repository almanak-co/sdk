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


# =============================================================================
# VIB-5864: absent reserve must not be reported as a governance pause
# =============================================================================
#
# Aave's PoolDataProvider reads an empty mapping slot for an unlisted asset and
# returns a SUCCESSFUL, well-formed, ALL-ZERO tuple. The guards read
# isActive=False off it and told users to HOLD until governance reactivated a
# reserve that will never exist (ALM-2911 ETH/base, ALM-2775 Case 2
# POL/polygon). Measured live: absent -> decimals=0 + all words zero; a
# genuinely frozen reserve (CRV/polygon) -> decimals=18, isActive=1, isFrozen=1.


def _absent_reserve_hex() -> str:
    """The all-zero tuple a PoolDataProvider returns for an unlisted asset."""
    return "0x" + _word(0) * 10


def _all_reserves_hex(tokens: list[tuple[str, str]]) -> str:
    """ABI-encode getAllReservesTokens() -> TokenData[] (string symbol, address)."""
    from eth_abi import encode as _abi_encode

    return "0x" + _abi_encode(["(string,address)[]"], [tokens]).hex()


def _routed_compiler(
    *,
    config_hex: str,
    reserves: list[tuple[str, str]] | None,
    chain: str = "base",
) -> MagicMock:
    """Compiler whose gateway answers per-selector.

    ``reserves=None`` simulates getAllReservesTokens() being unavailable
    (the enumeration reverts), which must NOT re-mask the absent reserve.
    """
    compiler = _mock_compiler(gateway_response=config_hex, chain=chain)

    def _call(request, timeout=None):  # noqa: ARG001
        params = json.loads(request.params)
        data = params[0]["data"]
        resp = MagicMock()
        resp.error = ""
        if data.startswith(cl._AAVE_GET_ALL_RESERVES_TOKENS_SELECTOR):
            if reserves is None:
                resp.success = False
                resp.result = ""
                return resp
            resp.success = True
            resp.result = json.dumps(_all_reserves_hex(reserves))
            return resp
        resp.success = True
        resp.result = json.dumps(config_hex)
        return resp

    compiler._gateway_client.rpc.Call.side_effect = _call
    return compiler


# Base Aave V3 lists WETH but has no reserve for native ETH (ALM-2911).
BASE_WETH = "0x4200000000000000000000000000000000000006"
ETH_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
BASE_RESERVES = [("WETH", BASE_WETH), ("USDC", "0x" + "11" * 20)]


def test_decoder_marks_all_zero_tuple_as_absent():
    """The pure decoder must expose word 0 and flag the empty-slot read."""
    cfg = cl.decode_reserve_configuration_data(_absent_reserve_hex())
    assert cfg is not None
    assert cfg.decimals == 0
    assert cfg.exists is False


def test_decoder_marks_real_frozen_reserve_as_existing():
    """CRV/polygon control: a genuinely frozen reserve carries decimals=18."""
    cfg = cl.decode_reserve_configuration_data(_encode_reserve_config(decimals=18, is_frozen=True))
    assert cfg is not None
    assert cfg.decimals == 18
    assert cfg.exists is True


def test_decoder_marks_real_deactivated_reserve_as_existing():
    """A deactivated-but-listed reserve must stay on the governance-pause path."""
    cfg = cl.decode_reserve_configuration_data(_encode_reserve_config(decimals=18, is_active=False))
    assert cfg is not None
    assert cfg.exists is True


def test_absent_reserve_reports_no_reserve_not_paused():
    """ALM-2911 repro: borrowing ETH on Base must not report a governance pause."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    reason = cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    assert reason is not None
    # The bug: the user was told to wait for governance.
    assert "is not active" not in reason
    assert "until governance reactivates" not in reason
    # The fix: name the real problem and the resolved address.
    assert "No aave_v3 reserve exists for ETH on base" in reason
    assert ETH_SENTINEL in reason
    assert "will not resolve by waiting" in reason


def test_absent_reserve_suggests_wrapped_token_the_pool_actually_lists():
    """The wrapped suggestion must be verified against the pool's own universe."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    reason = cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    assert "did you mean WETH?" in reason
    assert "confirmed against getAllReservesTokens()" in reason


def test_absent_reserve_offers_no_suggestion_for_ordinary_unsupported_erc20():
    """No WFOO reserve exists -> do not invent a suggestion (TRAP 1 control).

    A random unsupported ERC20 hits the identical all-zero path as the native
    sentinel — which is why a sentinel-address check would have been
    insufficient — but it has no wrapped equivalent to suggest.
    """
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    reason = cl._check_lending_reserve_active(compiler, "0x" + "22" * 20, "FOO", "aave_v3")

    assert "No aave_v3 reserve exists for FOO on base" in reason
    assert "did you mean" not in reason


def test_absent_reserve_still_reported_when_enumeration_unavailable():
    """Enumeration failure must not fall back to the phantom-pause message."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=None)

    reason = cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    assert "No aave_v3 reserve exists for ETH on base" in reason
    assert "is not active" not in reason
    assert "getAllReservesTokens() unavailable" in reason


def test_enumeration_listing_the_asset_wins_over_all_zero_config():
    """Contradicting reads -> defer to the definitive enumeration, not absence."""
    compiler = _routed_compiler(
        config_hex=_absent_reserve_hex(),
        reserves=[("ETH", ETH_SENTINEL)],
    )

    reason = cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    # Listed but all-zero: report the honest paused verdict, never "no reserve".
    assert reason is not None
    assert "No aave_v3 reserve exists" not in reason
    assert "is not active" in reason


def test_deactivated_reserve_still_reports_governance_pause():
    """Regression guard: the fix must NOT green a real deactivated reserve."""
    compiler = _routed_compiler(
        config_hex=_encode_reserve_config(decimals=18, is_active=False),
        reserves=BASE_RESERVES,
    )

    reason = cl._check_lending_reserve_active(compiler, BASE_WETH, "WETH", "aave_v3")

    assert "is not active" in reason
    assert "isActive=False" in reason
    assert "until governance reactivates" in reason
    assert "No aave_v3 reserve exists" not in reason


def test_absent_reserve_on_borrowable_guard_reports_absence():
    """The borrowable guard must not claim governance disabled borrowing."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    reason = cl._check_lending_reserve_borrowable(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    assert "No aave_v3 reserve exists for ETH on base" in reason
    assert "is not borrowable" not in reason


def test_absent_reserve_on_collateral_guard_reports_absence():
    """The collateral guard must not claim ltv=0 / not collateral-eligible."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    reason = cl._check_aave_v3_collateral_eligibility(compiler, ETH_SENTINEL, "ETH")

    assert "No aave_v3 reserve exists for ETH on base" in reason
    assert "not collateral-eligible" not in reason


def test_borrow_disabled_reserve_still_reports_borrow_disabled():
    """WMATIC/WPOL control (ALM-2775 Case 1 — explicitly OUT of scope).

    WPOL's reserve genuinely exists, is active, and Aave governance has
    genuinely disabled borrowing. That path MUST keep failing with its honest
    message — greening it would fabricate market state.
    """
    compiler = _routed_compiler(
        config_hex=_encode_reserve_config(decimals=18, borrowing_enabled=False),
        reserves=[("WPOL", BASE_WETH)],
        chain="polygon",
    )

    reason = cl._check_lending_reserve_borrowable(compiler, BASE_WETH, "WPOL", "aave_v3")

    assert "is not borrowable" in reason
    assert "borrowingEnabled=False" in reason
    assert "No aave_v3 reserve exists" not in reason


def test_reverting_data_provider_still_reports_frozen_not_absent():
    """A reverting/stubbed provider says nothing about listing — stay frozen.

    Guards the synthesised-config default: those paths mean "pool is broken ->
    treat as frozen", and must not be re-routed into the absent branch.
    """
    compiler = MagicMock()
    compiler.chain = "ethereum"
    compiler.rpc_timeout = 5.0
    gateway = MagicMock()
    gateway.is_connected = True
    rpc_resp = MagicMock()
    rpc_resp.success = False
    rpc_resp.result = ""
    rpc_resp.error = "execution reverted"
    gateway.rpc.Call.return_value = rpc_resp
    compiler._gateway_client = gateway

    config = cl._fetch_reserve_config(
        compiler, TEST_ASSET_ADDR, "WETH", protocol="aave_v3", pre_flight_label="test"
    )
    assert config is not None
    assert config.exists is True
    assert config.decimals is None  # unmeasured, NOT a measured zero

    reason = cl._check_lending_reserve_active(compiler, TEST_ASSET_ADDR, "WETH", "aave_v3")
    assert "is not active" in reason
    assert "No aave_v3 reserve exists" not in reason


# =============================================================================
# VIB-5864 follow-ups (gemini-code-assist review on PR #3296)
# =============================================================================


def test_absent_non_native_pair_suggests_without_claiming_nativeness():
    """stETH/wstETH: a real X/WX pair that is NOT native.

    The `W` + symbol match finds a genuinely useful suggestion here (wstETH is
    a real listed Base reserve), but stETH is not a native token. The message
    must offer the suggestion WITHOUT claiming nativeness — the nativeness
    claim is gated on the registry, not on symbol shape.
    """
    reserves = [("WETH", BASE_WETH), ("wstETH", "0x" + "33" * 20)]
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=reserves)

    reason = cl._check_lending_reserve_active(compiler, "0x" + "44" * 20, "stETH", "aave_v3")

    assert "No aave_v3 reserve exists for stETH on base" in reason
    # The suggestion survives — wstETH is a plausible thing to have meant.
    assert "did you mean wstETH?" in reason
    # ...but the false nativeness claim must be gone.
    assert "native gas token" not in reason
    assert "unwrapped native" not in reason


def test_absent_native_asset_still_claims_nativeness():
    """ETH/base: genuinely native — the native phrasing must survive the gate."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    reason = cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    assert "ETH is base's native gas token" in reason
    assert "did you mean WETH?" in reason


def test_native_detection_is_registry_derived_not_symbol_shaped():
    """Nativeness comes from the ERC-7528 sentinel OR ChainDescriptor.native."""
    # Sentinel address alone is sufficient.
    assert cl._is_native_asset("base", ETH_SENTINEL, "ETH") is True
    # Descriptor symbol alone is sufficient (polygon -> {"MATIC", "POL"}).
    assert cl._is_native_asset("polygon", "0x" + "55" * 20, "POL") is True
    assert cl._is_native_asset("polygon", "0x" + "55" * 20, "MATIC") is True
    # A real ERC20 with a W-pair partner is NOT native.
    assert cl._is_native_asset("base", "0x" + "44" * 20, "stETH") is False
    assert cl._is_native_asset("base", BASE_WETH, "WETH") is False


def test_reserve_universe_fetched_once_per_compiler_across_guards():
    """One compile tripping several guards must issue ONE enumeration."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)

    cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")
    cl._check_lending_reserve_borrowable(compiler, ETH_SENTINEL, "ETH", "aave_v3")
    cl._check_aave_v3_collateral_eligibility(compiler, ETH_SENTINEL, "ETH")

    enumeration_calls = [
        c
        for c in compiler._gateway_client.rpc.Call.call_args_list
        if json.loads(c.args[0].params)[0]["data"].startswith(cl._AAVE_GET_ALL_RESERVES_TOKENS_SELECTOR)
    ]
    assert len(enumeration_calls) == 1, f"expected 1 enumeration, got {len(enumeration_calls)}"


def test_failed_reserve_universe_read_is_not_cached():
    """A transient enumeration failure must NOT poison the compiler.

    Mirrors the sibling reserve caches' contract: a gateway error on iteration
    N must not permanently disable the check on iteration N+1. Caching the
    None would be worse than the redundant call it saves.
    """
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=None)

    # Iteration N — enumeration unavailable, absence inferred from all-zero.
    first = cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")
    assert "getAllReservesTokens() unavailable" in first
    assert compiler._lending_reserve_universe_cache == {}, "a failed read must not be cached"

    # Iteration N+1 — enumeration recovers; a fresh compiler must now confirm.
    recovered = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)
    second = cl._check_lending_reserve_active(recovered, ETH_SENTINEL, "ETH", "aave_v3")
    assert "confirmed against getAllReservesTokens()" in second
    assert recovered._lending_reserve_universe_cache, "a successful read SHOULD be cached"


def test_reserve_universe_cache_is_keyed_per_chain_and_protocol():
    """The cache must not leak one market's universe into another's."""
    compiler = _routed_compiler(config_hex=_absent_reserve_hex(), reserves=BASE_RESERVES)
    cl._check_lending_reserve_active(compiler, ETH_SENTINEL, "ETH", "aave_v3")

    assert ("base", "aave_v3") in compiler._lending_reserve_universe_cache
