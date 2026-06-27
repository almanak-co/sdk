"""Compile-layer doomed→FAILED proofs for the four RC-2 preflight adapters (VIB-5374).

Each test proves the *doomed* case (expired Pendle market, GMX/Stargate native-fee
shortfall, Euler over-LTV / disabled-collateral borrow) now yields a clean
``INFEASIBLE`` verdict with the venue's stable prefix — which the seam converts to a
permanent compile FAIL the state machine routes to HOLD — instead of an on-chain
revert. These are compile-layer tests: the gateway read is mocked (no fork needed).

The *feasible* / *risk-reducing* / *read-gap* paths are also covered so the gate is
proven to be narrow (it never blocks closing/selling, and never false-rejects on a
transient data gap).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.base.compiler import PreflightOutcome
from almanak.connectors._strategy_base.bridge_compiler import BridgeCompiler
from almanak.connectors.euler_v2.compiler import EulerV2Compiler
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.connectors.pendle.compiler import PendleCompiler
from almanak.framework.intents.vocabulary import IntentType


def _token(symbol: str, *, is_native: bool = False, decimals: int = 18):
    return SimpleNamespace(symbol=symbol, address="0x" + "ab" * 20, decimals=decimals, is_native=is_native)


# ---------------------------------------------------------------------------
# Pendle maturity (reference adapter — no gateway sub-task dependency)
# ---------------------------------------------------------------------------


def _pendle_ctx():
    return SimpleNamespace(
        chain="ethereum",
        wallet_address="0x" + "1" * 40,
        rpc_url=None,
        rpc_timeout=10.0,
        price_oracle=None,
        default_protocol="pendle",
        default_deadline_seconds=600,
        token_resolver=None,
        gateway_client=SimpleNamespace(is_connected=True),
        services=MagicMock(),
    )


def _swap_intent(from_token="USDC", to_token="PT-wstETH"):
    return SimpleNamespace(
        intent_type=IntentType.SWAP, intent_id="p-1", from_token=from_token, to_token=to_token
    )


def test_pendle_buying_into_expired_market_is_infeasible():
    compiler = PendleCompiler()
    with (
        patch.object(PendleCompiler, "chains", frozenset({"ethereum", "arbitrum"})),
        patch(
            "almanak.connectors.pendle.compiler._preflight_market_for_new_exposure",
            return_value="0xmarket",
        ),
        patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader") as reader_cls,
    ):
        reader_cls.return_value.is_market_expired.return_value = True
        verdict = compiler.preflight(_pendle_ctx(), _swap_intent())
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "PENDLE_MARKET_EXPIRED"


def test_pendle_buying_into_live_market_is_feasible():
    compiler = PendleCompiler()
    with (
        patch.object(PendleCompiler, "chains", frozenset({"ethereum", "arbitrum"})),
        patch(
            "almanak.connectors.pendle.compiler._preflight_market_for_new_exposure",
            return_value="0xmarket",
        ),
        patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader") as reader_cls,
    ):
        reader_cls.return_value.is_market_expired.return_value = False
        verdict = compiler.preflight(_pendle_ctx(), _swap_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_pendle_selling_pt_is_never_gated():
    """Selling PT (risk-reducing) is not an open → market never resolved → FEASIBLE."""
    compiler = PendleCompiler()
    with patch.object(PendleCompiler, "chains", frozenset({"ethereum", "arbitrum"})):
        verdict = compiler.preflight(_pendle_ctx(), _swap_intent(from_token="PT-wstETH", to_token="USDC"))
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_pendle_read_failure_fails_open_not_false_reject():
    """An unreadable expiry must defer (FEASIBLE) — only a CONFIRMED expiry is doomed."""
    compiler = PendleCompiler()
    with (
        patch.object(PendleCompiler, "chains", frozenset({"ethereum", "arbitrum"})),
        patch(
            "almanak.connectors.pendle.compiler._preflight_market_for_new_exposure",
            return_value="0xmarket",
        ),
        patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader") as reader_cls,
    ):
        reader_cls.return_value.is_market_expired.side_effect = RuntimeError("rpc down")
        verdict = compiler.preflight(_pendle_ctx(), _swap_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_pendle_maturity_read_is_gateway_only_no_rpc_fallback():
    """Gateway-boundary (CodeRabbit / AGENTS.md): without a connected gateway the
    maturity read fails open and does NOT fall back to direct RPC.

    The reader must never be constructed when no connected gateway is present — that
    would grow the framework-owned Pendle direct-egress surface. A ``ctx`` carrying an
    ``rpc_url`` but no gateway must still defer (FEASIBLE) and never instantiate the
    reader.
    """
    compiler = PendleCompiler()
    ctx = _pendle_ctx()
    ctx.gateway_client = None
    ctx.rpc_url = "http://localhost:8545"  # present, but must NOT be used as a fallback
    with (
        patch.object(PendleCompiler, "chains", frozenset({"ethereum", "arbitrum"})),
        patch(
            "almanak.connectors.pendle.compiler._preflight_market_for_new_exposure",
            return_value="0xmarket",
        ),
        patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader") as reader_cls,
    ):
        verdict = compiler.preflight(ctx, _swap_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE
    reader_cls.assert_not_called()


def test_pendle_maturity_read_disconnected_gateway_no_rpc_fallback():
    """A present-but-disconnected gateway must also fail open without an RPC fallback."""
    compiler = PendleCompiler()
    ctx = _pendle_ctx()
    ctx.gateway_client = SimpleNamespace(is_connected=False)
    ctx.rpc_url = "http://localhost:8545"
    with (
        patch.object(PendleCompiler, "chains", frozenset({"ethereum", "arbitrum"})),
        patch(
            "almanak.connectors.pendle.compiler._preflight_market_for_new_exposure",
            return_value="0xmarket",
        ),
        patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader") as reader_cls,
    ):
        verdict = compiler.preflight(ctx, _swap_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE
    reader_cls.assert_not_called()


# ---------------------------------------------------------------------------
# GMX exec-fee (no gateway sub-task dependency)
# ---------------------------------------------------------------------------


def _gmx_ctx(native_balance_wei):
    services = MagicMock()
    services.query_native_balance_for_chain.return_value = native_balance_wei
    return SimpleNamespace(
        chain="arbitrum",
        wallet_address="0x" + "1" * 40,
        rpc_url="http://localhost:8545",
        gateway_client=None,
        services=services,
    )


def _perp_open_intent():
    return SimpleNamespace(intent_type=IntentType.PERP_OPEN, intent_id="g-1")


def _perp_close_intent():
    return SimpleNamespace(intent_type=IntentType.PERP_CLOSE, intent_id="g-2")


def test_gmx_insufficient_native_fee_is_infeasible():
    compiler = GMXV2Compiler()
    sdk = MagicMock()
    sdk.get_execution_fee.return_value = 5_000_000_000_000_000  # 0.005 native keeper fee
    with patch.object(GMXV2Compiler, "_build_sdk", return_value=sdk):
        verdict = compiler.preflight(_gmx_ctx(native_balance_wei=1_000_000_000_000_000), _perp_open_intent())
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "GMX_INSUFFICIENT_NATIVE_FEE"


def test_gmx_sufficient_native_balance_is_feasible():
    """A VIB-5068-style inflated balance correctly passes the real-balance comparison."""
    compiler = GMXV2Compiler()
    sdk = MagicMock()
    sdk.get_execution_fee.return_value = 5_000_000_000_000_000
    with patch.object(GMXV2Compiler, "_build_sdk", return_value=sdk):
        verdict = compiler.preflight(
            _gmx_ctx(native_balance_wei=100 * 10**18), _perp_open_intent()  # 100 ETH (managed Anvil)
        )
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_gmx_balance_read_gap_fails_open():
    compiler = GMXV2Compiler()
    sdk = MagicMock()
    sdk.get_execution_fee.return_value = 5_000_000_000_000_000
    with patch.object(GMXV2Compiler, "_build_sdk", return_value=sdk):
        verdict = compiler.preflight(_gmx_ctx(native_balance_wei=None), _perp_open_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_gmx_perp_close_is_also_gated_by_native_fee():
    """The keeper exec-fee gate covers PERP_CLOSE too — closing a position is itself a
    keeper order paying ``msg.value``. A shortfall must reject the close (it can never
    land), and the fee is priced on the ``decrease`` order type.

    (CodeRabbit / VIB-5374: PERP_CLOSE shares the open path's preflight branch; a real
    on-chain 4-layer close test is not reliable on managed Anvil because the GMX keeper
    does not fill there, so the close path is proven at the compile layer here.)
    """
    compiler = GMXV2Compiler()
    sdk = MagicMock()
    sdk.get_execution_fee.return_value = 5_000_000_000_000_000  # 0.005 native keeper fee
    with patch.object(GMXV2Compiler, "_build_sdk", return_value=sdk):
        verdict = compiler.preflight(_gmx_ctx(native_balance_wei=1_000_000_000_000_000), _perp_close_intent())
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "GMX_INSUFFICIENT_NATIVE_FEE"
    # The fee must be priced for a DECREASE order on the close path.
    sdk.get_execution_fee.assert_called_once_with(order_type="decrease")


def test_gmx_perp_close_within_budget_is_feasible():
    """A funded PERP_CLOSE passes the gate (the close path is not over-rejected)."""
    compiler = GMXV2Compiler()
    sdk = MagicMock()
    sdk.get_execution_fee.return_value = 5_000_000_000_000_000
    with patch.object(GMXV2Compiler, "_build_sdk", return_value=sdk):
        verdict = compiler.preflight(_gmx_ctx(native_balance_wei=100 * 10**18), _perp_close_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


# ---------------------------------------------------------------------------
# Stargate native fee (shared BridgeCompiler — gated to stargate, Across untouched)
# ---------------------------------------------------------------------------


def _bridge_ctx(native_balance_wei):
    services = MagicMock()
    services.resolve_token.return_value = _token("USDC", decimals=6)
    services.query_native_balance_for_chain.return_value = native_balance_wei
    return SimpleNamespace(
        chain="arbitrum",
        wallet_address="0x" + "1" * 40,
        rpc_url=None,
        gateway_client=None,
        token_resolver=None,
        services=services,
    )


def _bridge_intent():
    return SimpleNamespace(
        intent_type=IntentType.BRIDGE,
        intent_id="b-1",
        from_chain="arbitrum",
        to_chain="base",
        token="USDC",
        amount=Decimal("100"),
        max_slippage=Decimal("0.005"),
        preferred_bridge=None,
        destination_address=None,
    )


def _stub_selection(bridge_name, lz_fee_wei):
    quote = SimpleNamespace(route_data={"lz_fee_wei": str(lz_fee_wei)}, gas_fee_amount=None)
    bridge = SimpleNamespace(name=bridge_name)
    return SimpleNamespace(is_success=True, bridge=bridge, quote=quote)


def test_stargate_insufficient_native_fee_is_infeasible():
    compiler = BridgeCompiler()
    with (
        patch.object(BridgeCompiler, "_build_selector", return_value=MagicMock()),
        patch.object(
            BridgeCompiler,
            "_select_bridge",
            return_value=_stub_selection("Stargate", lz_fee_wei=3_000_000_000_000_000),
        ),
    ):
        verdict = compiler.preflight(_bridge_ctx(native_balance_wei=1_000_000_000_000_000), _bridge_intent())
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "STARGATE_INSUFFICIENT_NATIVE_FEE"


def test_across_bridge_is_never_gated():
    """The shared compiler must NOT gate Across (no native messaging fee)."""
    compiler = BridgeCompiler()
    with (
        patch.object(BridgeCompiler, "_build_selector", return_value=MagicMock()),
        patch.object(
            BridgeCompiler,
            "_select_bridge",
            return_value=_stub_selection("Across", lz_fee_wei=3_000_000_000_000_000),
        ),
    ):
        verdict = compiler.preflight(_bridge_ctx(native_balance_wei=1), _bridge_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_stargate_sufficient_native_is_feasible():
    compiler = BridgeCompiler()
    with (
        patch.object(BridgeCompiler, "_build_selector", return_value=MagicMock()),
        patch.object(
            BridgeCompiler,
            "_select_bridge",
            return_value=_stub_selection("Stargate", lz_fee_wei=1_000_000_000_000_000),
        ),
    ):
        verdict = compiler.preflight(_bridge_ctx(native_balance_wei=10 * 10**18), _bridge_intent())
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def _native_bridge_ctx(native_balance_wei):
    """Bridge ctx whose token resolves to a NATIVE asset (ETH)."""
    services = MagicMock()
    services.resolve_token.return_value = _token("ETH", is_native=True, decimals=18)
    services.query_native_balance_for_chain.return_value = native_balance_wei
    return SimpleNamespace(
        chain="arbitrum",
        wallet_address="0x" + "1" * 40,
        rpc_url=None,
        gateway_client=None,
        token_resolver=None,
        services=services,
    )


def _native_bridge_all_intent():
    intent = _bridge_intent()
    intent.token = "ETH"
    intent.amount = "all"
    return intent


def test_stargate_native_all_includes_transfer_amount_in_gate():
    """VIB-5374 (CodeRabbit): a native ``amount="all"`` Stargate bridge must NOT skip
    the gate. ``msg.value = lz_fee + transfer_amount``, so a balance that covers the
    bare fee but not (fee + amount) is still INFEASIBLE.

    Wallet holds 1 ETH. ``_resolve_all_amount`` reserves 0.001 ETH for gas, so the
    transfer amount is ~0.999 ETH and the required msg.value (fee + amount) exceeds
    the 1 ETH balance — the lz_fee is not covered once the transfer is included.
    """
    compiler = BridgeCompiler()
    one_eth = 10**18
    with (
        patch.object(BridgeCompiler, "_build_selector", return_value=MagicMock()),
        patch.object(
            BridgeCompiler,
            "_select_bridge",
            return_value=_stub_selection("Stargate", lz_fee_wei=3_000_000_000_000_000),
        ),
    ):
        verdict = compiler.preflight(_native_bridge_ctx(native_balance_wei=one_eth), _native_bridge_all_intent())
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "STARGATE_INSUFFICIENT_NATIVE_FEE"


def test_erc20_all_bridge_defers_to_compile_path():
    """ERC20 ``amount="all"`` still defers (its native fee excludes the transfer amount)."""
    compiler = BridgeCompiler()
    intent = _bridge_intent()
    intent.amount = "all"  # USDC (ERC20) → defer
    with (
        patch.object(BridgeCompiler, "_build_selector", return_value=MagicMock()),
        patch.object(
            BridgeCompiler,
            "_select_bridge",
            return_value=_stub_selection("Stargate", lz_fee_wei=3_000_000_000_000_000),
        ),
    ):
        verdict = compiler.preflight(_bridge_ctx(native_balance_wei=1), intent)
    assert verdict.outcome is PreflightOutcome.FEASIBLE


# ---------------------------------------------------------------------------
# Euler LTV (uses the new ctx.services.eth_call gateway passthrough)
# ---------------------------------------------------------------------------


def _euler_ctx(*, eth_call_result, prices=None):
    services = MagicMock()
    services.resolve_token.side_effect = lambda sym: _token(sym, decimals=6 if sym == "USDC" else 18)
    services.eth_call.return_value = eth_call_result
    prices = prices or {}
    services.require_token_price.side_effect = lambda sym: prices.get(sym, Decimal("1"))
    return SimpleNamespace(
        chain="ethereum",
        wallet_address="0x" + "1" * 40,
        rpc_url=None,
        gateway_client=SimpleNamespace(is_connected=True),
        cache={},
        token_resolver=object(),
        price_oracle=None,
        rpc_timeout=10.0,
        services=services,
    )


def _borrow_intent(collateral="WETH", borrow="USDC", collateral_amount=Decimal("1"), borrow_amount=Decimal("100")):
    return SimpleNamespace(
        intent_type=IntentType.BORROW,
        intent_id="e-1",
        collateral_token=collateral,
        borrow_token=borrow,
        collateral_amount=collateral_amount,
        borrow_amount=borrow_amount,
    )


def _euler_vault(symbol):
    return SimpleNamespace(vault_symbol=symbol, vault_address="0x" + "cd" * 20)


def _patch_euler_adapter():
    """Patch the Euler adapter so vault resolution succeeds without on-chain state."""
    adapter = MagicMock()
    adapter.find_vault_for_asset.side_effect = lambda sym: _euler_vault(f"e{sym}")
    return patch("almanak.connectors.euler_v2.adapter.EulerV2Adapter", return_value=adapter), patch(
        "almanak.connectors.euler_v2.adapter.EulerV2Config"
    )


def _hex_uint(value: int) -> str:
    return "0x" + format(value, "064x")


def test_euler_collateral_not_enabled_is_infeasible():
    """LTVBorrow == 0 → collateral not enabled for the borrow vault → INFEASIBLE."""
    compiler = EulerV2Compiler()
    p_adapter, p_config = _patch_euler_adapter()
    with p_adapter, p_config:
        verdict = compiler.preflight(_euler_ctx(eth_call_result=_hex_uint(0)), _borrow_intent())
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "EULER_BORROW_INFEASIBLE"


def test_euler_over_ltv_borrow_is_infeasible():
    """Borrow value > collateral value × LTV → INFEASIBLE."""
    compiler = EulerV2Compiler()
    p_adapter, p_config = _patch_euler_adapter()
    # LTV 50% (5000/1e4); 1 WETH @ $2000 → max borrow $1000; request $5000 USDC.
    ctx = _euler_ctx(eth_call_result=_hex_uint(5000), prices={"WETH": Decimal("2000"), "USDC": Decimal("1")})
    with p_adapter, p_config:
        verdict = compiler.preflight(ctx, _borrow_intent(borrow_amount=Decimal("5000")))
    assert verdict.outcome is PreflightOutcome.INFEASIBLE
    assert verdict.error_prefix == "EULER_BORROW_INFEASIBLE"


def test_euler_within_ltv_is_feasible():
    compiler = EulerV2Compiler()
    p_adapter, p_config = _patch_euler_adapter()
    ctx = _euler_ctx(eth_call_result=_hex_uint(8000), prices={"WETH": Decimal("2000"), "USDC": Decimal("1")})
    with p_adapter, p_config:
        verdict = compiler.preflight(ctx, _borrow_intent(borrow_amount=Decimal("500")))
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_euler_zero_collateral_borrow_is_feasible():
    """Borrow that supplies no NEW collateral (collateral_amount == 0) draws against
    collateral already on-chain — the supply-then-borrow lifecycle. The intent carries
    nothing to size capacity from, so the capacity check must fail open (defer to the
    on-chain EVC solvency check) rather than guaranteed-false-reject; the LTV-enabled
    structural check still runs first.
    """
    compiler = EulerV2Compiler()
    p_adapter, p_config = _patch_euler_adapter()
    # Collateral IS enabled (LTV 80%) but this intent adds zero NEW collateral. Without
    # the fail-open guard the new-collateral lower bound is 0 → max borrow 0 → the
    # positive borrow would be wrongly rejected. The guard must keep it FEASIBLE.
    ctx = _euler_ctx(eth_call_result=_hex_uint(8000), prices={"WETH": Decimal("2000"), "USDC": Decimal("1")})
    intent = _borrow_intent(collateral_amount=Decimal("0"), borrow_amount=Decimal("500"))
    with p_adapter, p_config:
        verdict = compiler.preflight(ctx, intent)
    assert verdict.outcome is PreflightOutcome.FEASIBLE


def test_euler_read_gap_is_unavailable():
    compiler = EulerV2Compiler()
    p_adapter, p_config = _patch_euler_adapter()
    with p_adapter, p_config:
        verdict = compiler.preflight(_euler_ctx(eth_call_result=None), _borrow_intent())
    assert verdict.outcome is PreflightOutcome.UNAVAILABLE


def test_euler_supply_intent_is_not_gated():
    compiler = EulerV2Compiler()
    supply = SimpleNamespace(intent_type=IntentType.SUPPLY, intent_id="e-2")
    verdict = compiler.preflight(_euler_ctx(eth_call_result=_hex_uint(0)), supply)
    assert verdict.outcome is PreflightOutcome.FEASIBLE
