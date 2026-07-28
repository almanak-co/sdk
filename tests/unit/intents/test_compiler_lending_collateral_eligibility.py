"""Unit tests for VIB-3701: Aave V3 collateral eligibility pre-flight check.

Confirms `_compile_supply_aave_compatible` consults the on-chain reserve
configuration (`PoolDataProvider.getReserveConfigurationData`) before emitting
a `setUserUseReserveAsCollateral` TX. Asset-level reverts like
`0xd0739dae UnderlyingCannotBeUsedAsCollateral` (USDE on Aave V3 Ethereum,
DAI on Polygon V3, ...) and `0x21e5c4ae UserHasAssetWithZeroLtv` (every reserve
on Aave V3 Mantle, where governance set `ltv = 0`) now surface as a typed
compile-time error instead of an opaque on-chain revert.

VIB-6111: the selector previously documented here (`0x0cafc072`) was
keccak-wrong; `cast sig "UnderlyingCannotBeUsedAsCollateral()"` == `0xd0739dae`.
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
def test_blocks_compile_when_pool_returns_json_wrapped_0x(mock_adapter_cls):
    """Regression for the load-bearing VIB-3749 failure mode.

    Production wire-format: gateway.rpc.Call wraps eth_call results in
    json.dumps. A shut-down PoolDataProvider proxy returns `success=True` with
    `result='"0x"'` (literal JSON-quoted "0x"). The pre-flight must classify
    that as frozen and FAIL the compile — not fail-open and emit a SUPPLY that
    reverts on-chain.

    Pairs with `test_empty_0x_response_classified_as_frozen` (the bare-hex
    twin) and explicitly anchors the JSON-wrapped path so a regression that
    re-introduced the fail-open branch couldn't slip past this suite.
    CodeRabbit follow-up.
    """
    compiler = _mock_compiler(gateway_response="0x")
    # Sanity-check the fixture: result must arrive json-encoded (the production
    # shape). A bare-hex regression in the fixture would mask the bug.
    assert compiler._gateway_client.rpc.Call.return_value.result == json.dumps("0x")

    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
    result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token(), Decimal("100"))

    assert result.status == CompilationStatus.FAILED
    # Error must surface the specific frozen / inactive signal so a strategy
    # can match on it deterministically.
    assert "is not active" in result.error
    assert "isFrozen=True" in result.error
    # Routed to the Aave V3 Ethereum PoolDataProvider.
    args, _ = compiler._gateway_client.rpc.Call.call_args
    assert "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3" in args[0].params


@patch(AAVE_ADAPTER_CLS)
def test_fails_open_when_gateway_unavailable(mock_adapter_cls):
    """No gateway → can't pre-flight; rely on on-chain revert as final guard."""
    compiler = _mock_compiler(gateway_response=None)
    mock_adapter = MagicMock()
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
    a real-world shut-down proxy returns exactly that). But a
    *partial* response (e.g. `0x12...` with fewer than 320 bytes) still
    indicates an RPC anomaly we cannot interpret, so fail-open as before.
    """
    # 64 hex chars = 32 bytes = 1 ABI word, well below the expected 320.
    short_payload = "0x" + ("ab" * 32)
    compiler = _mock_compiler(gateway_response=short_payload)
    mock_adapter = MagicMock()
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
    """A shut-down proxy returns `0x` with `success=True`. Pre-flight must
    classify that as frozen, not fail-open. Without this guard the SUPPLY
    would compile cleanly and revert on-chain (the original VIB-3749 bug).

    The bare-hex twin of `test_blocks_compile_when_pool_returns_json_wrapped_0x`:
    here the gateway result is the raw, *unwrapped* ``0x`` an RPC node returns
    directly, so the two tests together pin both wire shapes.
    """
    compiler = _mock_compiler(gateway_response="0x")
    # Override the fixture's default json-wrapping so this test exercises the
    # raw/bare ``0x`` wire shape (the json-wrapped shape is covered by the twin).
    compiler._gateway_client.rpc.Call.return_value.result = "0x"
    mock_adapter = MagicMock()
    mock_adapter.get_pool_address.return_value = TEST_POOL
    mock_adapter.get_supply_calldata.return_value = b"\x01"
    mock_adapter.estimate_supply_gas.return_value = 150_000
    mock_adapter_cls.return_value = mock_adapter

    intent = _supply_intent(use_as_collateral=True)
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


# ===========================================================================
# VIB-6111 — the gateway-internal transport seam
#
# The lending risk-parameter pre-flights read through the compiler's gateway
# client. A compiler constructed INSIDE the gateway has none (it IS the
# gateway), and the production runner compiles exactly there — via
# ``execution.CompileIntent``, not in-process. So every one of these guards
# was silently inert on the only path that ships.
#
# Measured on an Anvil Mantle fork at head (ltv=0 on all ten reserves),
# driving the real ``execution.CompileIntent`` RPC:
#   gateway_internal_preflight=False -> success=True,  3 txs (bundle reverts
#                                       on-chain with 0x21e5c4ae)
#   gateway_internal_preflight=True  -> success=False, 0 txs
#
# These tests pin the seam so it cannot silently regress to fail-open.
# ===========================================================================


class _FakeCompiler:
    """Minimal compiler stand-in with NO gateway client."""

    def __init__(self, *, gateway_internal_preflight: bool, eth_call_result: str | None):
        self.chain = "mantle"
        self.wallet_address = TEST_WALLET
        self.rpc_url = "http://127.0.0.1:8545"
        self.rpc_timeout = 5.0
        self._gateway_client = None
        self._gateway_internal_preflight = gateway_internal_preflight
        self._eth_call_result = eth_call_result
        self.eth_call_count = 0

    def _eth_call(self, to: str, data: str, *, chain: str | None = None) -> str | None:
        self.eth_call_count += 1
        return self._eth_call_result


def _zero_ltv_payload() -> str:
    return "0x" + _encode_reserve_config(ltv=0, liquidation_threshold=8300, is_active=True, is_frozen=False)


class TestGatewayInternalPreflightSeam:
    """``_fetch_reserve_config`` transport selection (VIB-6111)."""

    def test_no_gateway_and_flag_off_fails_open_without_any_rpc(self) -> None:
        """An offline / strategy-side compiler must NOT reach the network.

        ``IntentCompiler`` backfills ``rpc_url`` with a resolvable public
        endpoint for any known chain, so an unconditional fallback would turn
        every offline compile — including this very unit suite — into a live
        mainnet read whose verdict depends on real market state.
        """
        compiler = _FakeCompiler(gateway_internal_preflight=False, eth_call_result=_zero_ltv_payload())

        config = cl._fetch_reserve_config(
            compiler,
            TEST_ASSET_ADDR,
            "WETH",
            protocol="aave_v3",
            pre_flight_label="test",
        )

        assert config is None, "flag off must fail open"
        assert compiler.eth_call_count == 0, "flag off must not issue ANY eth_call"

    def test_gateway_internal_flag_on_reads_and_reports_zero_ltv(self) -> None:
        """A gateway-side compiler measures the reserve instead of failing open."""
        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=_zero_ltv_payload())

        config = cl._fetch_reserve_config(
            compiler,
            TEST_ASSET_ADDR,
            "WETH",
            protocol="aave_v3",
            pre_flight_label="test",
        )

        assert compiler.eth_call_count == 1
        assert config is not None, "flag on must actually measure the reserve"
        assert config.ltv == 0

    def test_flag_on_but_read_fails_FAILS_CLOSED(self) -> None:
        """A CONFIGURED pre-flight that cannot read must not fail open.

        This test previously asserted the opposite (``config is None``), on the
        rationale that "an unmeasured LTV is never reported as a measured zero".
        That rationale is correct for the DIAGNOSTIC annotation path — which
        must never manufacture a measured zero — but it is the wrong conclusion
        for the safety gate, where ``None`` does not mean "don't claim zero", it
        means "emit the bundle unverified".

        Once ``gateway_internal_preflight`` is set, the compiler has declared it
        CAN read. A failed read is therefore "could not verify", and proceeding
        reproduces the exact defect this PR exists to fix, one level up: the
        emitted ``setUserUseReserveAsCollateral`` leg reverts on-chain
        ``0x21e5c4ae UserHasAssetWithZeroLtv()`` and burns gas (VIB-6111).
        """
        import pytest

        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)

        with pytest.raises(cl.ReserveConfigUnverifiableError):
            cl._fetch_reserve_config(
                compiler,
                TEST_ASSET_ADDR,
                "WETH",
                protocol="aave_v3",
                pre_flight_label="test",
            )
        assert compiler.eth_call_count == 1, "the pre-flight must actually have attempted the read"

    def test_flag_on_and_eth_call_raises_FAILS_CLOSED(self) -> None:
        """An exception from the read is also 'could not verify', not 'fine'."""
        import pytest

        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)

        def _boom(to: str, data: str, *, chain: str | None = None) -> str:
            raise RuntimeError("rpc exploded")

        compiler._eth_call = _boom  # type: ignore[assignment]

        with pytest.raises(cl.ReserveConfigUnverifiableError):
            cl._fetch_reserve_config(
                compiler,
                TEST_ASSET_ADDR,
                "WETH",
                protocol="aave_v3",
                pre_flight_label="test",
            )

    def test_flag_on_and_payload_undecodable_FAILS_CLOSED(self) -> None:
        """A successful read whose payload can't be decoded is still unverified.

        Closes the same hole one layer below the ``eth_call`` itself: a short /
        malformed PoolDataProvider response used to fall through to ``None``.
        """
        import pytest

        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result="0xdeadbeef")

        with pytest.raises(cl.ReserveConfigUnverifiableError):
            cl._fetch_reserve_config(
                compiler,
                TEST_ASSET_ADDR,
                "WETH",
                protocol="aave_v3",
                pre_flight_label="test",
            )

    def test_flag_OFF_and_read_fails_still_fails_OPEN(self) -> None:
        """The unconfigured case is unchanged — and must stay that way.

        No read was ever expected for a strategy-side / offline compile, so the
        guards keep failing open and the on-chain revert stays the final guard.
        Narrowing this too would make every offline compile depend on live
        market state, which is the regression the flag exists to prevent.
        """
        compiler = _FakeCompiler(gateway_internal_preflight=False, eth_call_result=None)

        config = cl._fetch_reserve_config(
            compiler,
            TEST_ASSET_ADDR,
            "WETH",
            protocol="aave_v3",
            pre_flight_label="test",
        )

        assert config is None
        assert compiler.eth_call_count == 0, "an unconfigured pre-flight must not touch the network"


class TestUnverifiablePreflightReachesTheRightConsumers:
    """The two consumers of ``_fetch_reserve_config`` must diverge (VIB-6111).

    Raising at the producer is only correct if the SAFETY guards fail closed and
    the DIAGNOSTIC annotation keeps falling back. Testing the producer alone
    would not show that — a single shared behaviour would be wrong for one of
    them either way.
    """

    def test_collateral_eligibility_guard_fails_closed(self) -> None:
        """The guard must BLOCK — and it blocks by RAISING, not by returning a reason.

        Returning a reason string would be indistinguishable from a measured
        protocol fact once a caller wraps it in a typed error, and one of those
        typed errors classifies non-retryable. See
        ``TestUnverifiableIsNeverLaunderedIntoAGovernanceFact``.
        """
        import pytest

        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)

        with patch.object(
            cl.AddressRegistry, "addresses_for", return_value={"pool_data_provider": TEST_ASSET_ADDR}
        ):
            with pytest.raises(cl.ReserveConfigUnverifiableError) as excinfo:
                cl._check_aave_v3_collateral_eligibility(compiler, TEST_ASSET_ADDR, "WETH")

        assert "verif" in str(excinfo.value).lower()

        # And the miss must NOT be cached — the next iteration has to retry.
        cache = getattr(compiler, "_aave_collateral_eligibility_cache", {})
        assert ("mantle", TEST_ASSET_ADDR.lower()) not in cache

    def test_zero_ltv_annotation_still_falls_back(self) -> None:
        """The diagnostic enricher must never block on a failed read.

        It runs on an ALREADY-failing borrow path; replacing the caller's
        precise capacity error with an incidental read failure would be a
        regression, and an unmeasured LTV must never be reported as a measured
        zero.
        """
        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)
        token = MagicMock()
        token.address = TEST_ASSET_ADDR
        token.symbol = "WETH"

        out = cl._annotate_zero_ltv_collateral(
            compiler,
            "not enough borrowing power; supply more collateral first",
            collateral_token=token,
            protocol="aave_v3",
        )

        assert out == "not enough borrowing power; supply more collateral first"
        assert "ltv=0" not in out


class TestGatewayExecutionServiceSetsTheFlag:
    """The seam is worthless unless the gateway actually opts in."""

    def test_execution_service_constructs_compiler_with_flag_enabled(self) -> None:
        """Pins the one call site that closes the production hole.

        If this assertion is ever relaxed, the Aave frozen-reserve, borrowable
        and zero-LTV guards all go silently inert again on the runner's path.
        """
        import inspect

        from almanak.gateway.services import execution_service

        source = inspect.getsource(execution_service)
        assert "gateway_internal_preflight=True" in source, (
            "almanak/gateway/services/execution_service.py must construct its "
            "IntentCompiler with gateway_internal_preflight=True (VIB-6111)"
        )


class TestAbsentReservePathsUnderTheEnabledPreflight:
    """VIB-6111 — enabling the gateway pre-flight made these paths reachable.

    Before the flag, a gateway-side compile never got a decoded reserve config,
    so neither the reserve-universe confirmation nor the zero-LTV annotation
    could run on that path. Both are now live and both had defects.
    """

    def test_slotted_adapter_exposes_the_reserve_universe_cache(self) -> None:
        """``_fetch_reserve_universe`` ASSIGNS this attribute when it is not a
        dict. ``_LendingCompilerAdapter`` is slotted, so a missing property
        turns an actionable 'no such reserve' error into an AttributeError that
        blows up compilation."""
        from almanak.connectors.aave_v3.compiler import _LendingCompilerAdapter

        for attr in (
            "_aave_collateral_eligibility_cache",
            "_lending_reserve_active_cache",
            "_lending_borrowable_cache",
            "_lending_borrow_capacity_cache",
            "_lending_reserve_universe_cache",
        ):
            assert hasattr(_LendingCompilerAdapter, attr), f"{attr} must be backed by ctx.cache"
            assert isinstance(getattr(_LendingCompilerAdapter, attr), property)

    def test_absent_reserve_is_not_annotated_as_a_governance_ltv_cut(self) -> None:
        """An ABSENT reserve decodes all-zero, so it also reads ltv == 0.

        Annotating it would assert two false things: that governance zeroed this
        reserve's LTV, and that supplying without collateral still works — there
        is no reserve to supply into.
        """
        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)
        token = MagicMock()
        token.address = TEST_ASSET_ADDR
        token.symbol = "NOTLISTED"

        absent = cl._DecodedReserveConfig(
            ltv=0,
            liquidation_threshold=0,
            usage_as_collateral_enabled=False,
            borrowing_enabled=False,
            is_active=False,
            is_frozen=False,
            decimals=0,
            exists=False,
        )
        base = "not enough borrowing power; supply more collateral first"
        with patch.object(cl, "_fetch_reserve_config", return_value=absent):
            out = cl._annotate_zero_ltv_collateral(
                compiler, base, collateral_token=token, protocol="aave_v3"
            )
        assert out == base, "an absent reserve must not be described as a governance LTV cut"
        assert "ltv=0" not in out

    def test_existing_zero_ltv_reserve_IS_annotated(self) -> None:
        """Positive control — without this, the test above passes vacuously."""
        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)
        token = MagicMock()
        token.address = TEST_ASSET_ADDR
        token.symbol = "WETH"

        zeroed = cl._DecodedReserveConfig(
            ltv=0,
            liquidation_threshold=8300,
            usage_as_collateral_enabled=True,
            borrowing_enabled=True,
            is_active=True,
            is_frozen=False,
            decimals=18,
            exists=True,
        )
        base = "not enough borrowing power; supply more collateral first"
        with patch.object(cl, "_fetch_reserve_config", return_value=zeroed):
            out = cl._annotate_zero_ltv_collateral(
                compiler, base, collateral_token=token, protocol="aave_v3"
            )
        assert out != base and "ltv=0" in out
        # The capacity figure is ACCOUNT-WIDE while collateral_token is only the
        # intent's metadata, so the annotation must offer the zero-LTV reserve as
        # a cause without asserting it is THE cause — otherwise it declares a
        # root cause the read cannot support.
        assert "account-wide" in out, "must disclose that other causes are possible"
        assert "does not rule out" in out


class TestUnverifiableIsNeverLaunderedIntoAGovernanceFact:
    """VIB-6111 — "could not verify" must not wear a typed protocol error.

    ``LendingBorrowNotEnabledError.ERROR_PREFIX`` is in
    ``error_keywords.permanent_keywords``, so wrapping an unverifiable read in
    it makes ``categorize_error`` return COMPILATION_PERMANENT — NON-RETRYABLE.
    A transient RPC 429 on an uncredentialed gateway pod would then tell the
    strategy to HOLD until governance enables borrowing on a healthy asset.
    """

    def test_the_typed_borrow_error_really_is_non_retryable(self) -> None:
        """Pins WHY the guards must not return a reason for an unverifiable read.

        If this ever stops being permanent, the laundering is no longer
        dangerous and this suite should be revisited — not silently kept.
        """
        from almanak.framework.intents.error_keywords import categorize_error
        from almanak.framework.intents.intent_errors import LendingBorrowNotEnabledError

        verdict = categorize_error(f"{LendingBorrowNotEnabledError.ERROR_PREFIX}: whatever")
        assert verdict is not None and "PERMANENT" in str(verdict).upper()

    def test_guards_propagate_rather_than_returning_a_reason(self) -> None:
        """All three guards must raise, not convert to a reason string."""
        import pytest

        compiler = _FakeCompiler(gateway_internal_preflight=True, eth_call_result=None)
        with patch.object(
            cl.AddressRegistry, "addresses_for", return_value={"pool_data_provider": TEST_ASSET_ADDR}
        ), patch.object(cl, "_resolve_pool_data_provider", return_value=TEST_ASSET_ADDR):
            with pytest.raises(cl.ReserveConfigUnverifiableError):
                cl._check_aave_v3_collateral_eligibility(compiler, TEST_ASSET_ADDR, "WETH")
            with pytest.raises(cl.ReserveConfigUnverifiableError):
                cl._check_lending_reserve_borrowable(compiler, TEST_ASSET_ADDR, "WETH", "aave_v3")
            with pytest.raises(cl.ReserveConfigUnverifiableError):
                cl._check_lending_reserve_active(compiler, TEST_ASSET_ADDR, "WETH", "aave_v3")

    def test_unverifiable_message_is_not_classified_permanent(self) -> None:
        """The neutral CompilationResult text must stay unclassified."""
        from almanak.framework.intents.error_keywords import categorize_error

        msg = (
            "aave_v3 reserve-borrowable pre-flight could not verify reserve risk parameters "
            "on mantle: eth_call failed (429 Too Many Requests). "
            "Refusing to compile an unverified bundle."
        )
        assert categorize_error(msg) != "COMPILATION_PERMANENT"
        verdict = categorize_error(msg)
        assert verdict is None or "PERMANENT" not in str(verdict).upper()


class TestGatewayFlagIsPinnedBehaviourally:
    """The source-substring pin passes on a COMMENT; assert behaviour instead."""

    def test_config_stores_the_flag_as_a_real_bool(self) -> None:
        """NOTE: this exercises the CONFIG only — it does not touch
        ``execution_service``. Named accordingly so it cannot be mistaken for a
        wiring pin; the gateway's actual construction is covered by
        ``test_execution_service_constructs_compiler_with_flag_enabled``.
        """
        from almanak.framework.intents.compiler_models import IntentCompilerConfig

        config = IntentCompilerConfig(allow_placeholder_prices=True, gateway_internal_preflight=True)
        # `is True`, not truthiness: the pre-flight seam tests identity, so a
        # truthy non-bool here would silently disable the guard in production.
        assert config.gateway_internal_preflight is True

    def test_execution_service_sets_the_flag_in_CODE_not_prose(self) -> None:
        """Pin the ONE call site that makes this PR non-inert — via the AST.

        The sibling assertion greps ``inspect.getsource`` for the literal
        ``gateway_internal_preflight=True``, which passes on any occurrence in
        the module INCLUDING a comment — and this file's real call site sits
        directly under an 8-line explanatory comment block, exactly the artifact
        that would survive deleting the kwarg. Comments do not appear in the
        AST, so parsing for the keyword argument on an actual
        ``IntentCompilerConfig(...)`` call cannot be satisfied by prose.
        """
        import ast
        import inspect

        from almanak.gateway.services import execution_service

        tree = ast.parse(inspect.getsource(execution_service))
        enabled_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", getattr(node.func, "attr", None)) == "IntentCompilerConfig"
            and any(
                kw.arg == "gateway_internal_preflight"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value is True
                for kw in node.keywords
            )
        ]
        assert enabled_calls, (
            "almanak/gateway/services/execution_service.py must CONSTRUCT its "
            "IntentCompilerConfig with gateway_internal_preflight=True (VIB-6111). "
            "If this fails, the Aave frozen-reserve, borrowable and zero-LTV guards "
            "are all silently inert again on the runner's path."
        )

    def test_adapter_propagates_the_flag_as_a_real_bool(self) -> None:
        """End-to-end pin on the diff's highest-risk invariant."""
        from almanak.connectors.aave_v3.compiler import _LendingCompilerAdapter

        ctx = MagicMock()
        ctx.gateway_internal_preflight = True
        adapter = _LendingCompilerAdapter(ctx)
        assert adapter._gateway_internal_preflight is True
