"""Characterization tests for IntentCompiler._compile_swap and ._compile_lp_open.

Unit-level characterization tests for ``_compile_swap`` and ``_compile_lp_open``.
These tests pin current observable behaviour with mocked SDK/adapter/oracle seams
so a regression during refactor is caught in seconds instead of ~30 minutes of
Anvil-fork intent tests.

Non-scope (by construction):
    - No production code changes.
    - No Web3 / RPC constructed (SDK classes are module-level mocked).
    - Not a replacement for ``tests/intents/`` end-to-end coverage — this
      isolates the compile step only.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent
from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.intents import LPOpenIntent
from almanak.framework.intents.compiler import CompilationStatus
from almanak.framework.intents.vocabulary import Intent

# ---------------------------------------------------------------------------
# Module-level patch targets for framework swap compilation and connector-owned
# Uniswap V3-family LP compilation.
# ---------------------------------------------------------------------------

SWAP_ADAPTER_CLS = "almanak.framework.intents.compiler.DefaultSwapAdapter"
LP_ADAPTER_CLS = "almanak.framework.connectors.uniswap_v3.adapter.UniswapV3LPAdapter"
VALIDATE_V3_POOL = "almanak.framework.intents.pool_validation.validate_v3_pool"
FETCH_SLOT0 = "almanak.framework.intents.pool_validation.fetch_v3_pool_sqrt_price_x96"


# Realistic oracle shared across most tests. Deliberately small so derived
# int wei quantities are easy to reason about in asserts.
_DEFAULT_PRICES: dict[str, Decimal] = {
    "ETH": Decimal("2000"),
    "WETH": Decimal("2000"),
    "USDC": Decimal("1"),
    "USDT": Decimal("1"),
    "WBTC": Decimal("60000"),
    "WBNB": Decimal("600"),
    "DAI": Decimal("1"),
}


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_compiler(
    *,
    chain: str = "arbitrum",
    price_oracle: dict[str, Decimal] | None = None,
    max_price_impact_pct: Decimal = Decimal("0.30"),
    allow_placeholder_prices: bool = False,
) -> IntentCompiler:
    """Construct a compiler wired to a real price oracle (not placeholders).

    The price-impact guard only activates when ``_using_placeholders`` is
    False, so these tests pass a real oracle dict to exercise the full
    ``_compile_swap`` path.
    """

    config = IntentCompilerConfig(
        allow_placeholder_prices=allow_placeholder_prices,
        max_price_impact_pct=max_price_impact_pct,
    )
    return IntentCompiler(
        chain=chain,
        wallet_address="0x1111111111111111111111111111111111111111",
        config=config,
        price_oracle=price_oracle if price_oracle is not None else _DEFAULT_PRICES,
    )


def _make_mock_swap_adapter(
    *,
    quoter_amount: int | None = 49_850_000_000_000_000,
    router_address: str = "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    selected_fee_tier: int | None = None,
    swap_calldata: bytes = b"\xab\xcd\xef",
    gas_estimate: int = 200_000,
) -> MagicMock:
    """Mock ``DefaultSwapAdapter`` with controlled quoter + calldata output.

    ``selected_fee_tier=None`` short-circuits the pool_validation branch so
    tests that don't care about pool validation don't need a validate_v3_pool
    patch.
    """

    adapter = MagicMock(name="MockSwapAdapter")
    adapter.get_router_address.return_value = router_address
    adapter.select_fee_tier.return_value = 3000
    adapter.get_quoted_amount_out.return_value = quoter_amount
    adapter.last_fee_selection = {
        "selected_fee_tier": selected_fee_tier,
        "candidate_fee_tiers": [3000],
        "source": "mock",
    }
    adapter.get_swap_calldata.return_value = swap_calldata
    adapter.estimate_gas.return_value = gas_estimate
    return adapter


def _make_mock_lp_adapter(
    *,
    position_manager: str = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    mint_calldata: bytes = b"\x00" * 32,
    mint_gas: int = 500_000,
) -> MagicMock:
    """Mock ``UniswapV3LPAdapter`` with deterministic mint output."""

    adapter = MagicMock(name="MockLPAdapter")
    adapter.get_position_manager_address.return_value = position_manager
    adapter.get_mint_calldata.return_value = mint_calldata
    adapter.estimate_mint_gas.return_value = mint_gas
    return adapter


def _mock_pool_validation_ok(pool_address: str | None = None) -> MagicMock:
    """Return a ``PoolValidationResult``-shaped mock that passes ``_validate_pool``.

    ``pool_address=None`` skips the slot0 recompute branch in
    ``_compile_lp_open`` which is what keeps most tests simple.
    """

    result = MagicMock(name="PoolValidationResult")
    result.exists = True
    result.is_valid = True
    result.pool_address = pool_address
    result.reason = None
    result.error = None
    result.warning = None
    return result


def _make_swap_intent(
    *,
    from_token: str = "USDC",
    to_token: str = "WETH",
    amount_usd: Decimal | None = Decimal("100"),
    amount: Any = None,
    max_slippage: Decimal = Decimal("0.005"),
    max_price_impact: Decimal | None = None,
    protocol: str | None = None,
) -> SwapIntent:
    """Small builder that keeps intent construction noise out of the tests."""

    kwargs: dict[str, Any] = {
        "from_token": from_token,
        "to_token": to_token,
        "max_slippage": max_slippage,
    }
    if amount_usd is not None:
        kwargs["amount_usd"] = amount_usd
    if amount is not None:
        kwargs["amount"] = amount
    if max_price_impact is not None:
        kwargs["max_price_impact"] = max_price_impact
    if protocol is not None:
        kwargs["protocol"] = protocol
    return SwapIntent(**kwargs)


def _make_lp_intent(
    *,
    pool: str = "USDC/WETH/3000",
    amount0: Decimal = Decimal("1000"),
    amount1: Decimal = Decimal("0.5"),
    range_lower: Decimal = Decimal("1500"),
    range_upper: Decimal = Decimal("2500"),
    protocol: str = "uniswap_v3",
    protocol_params: dict | None = None,
) -> LPOpenIntent:
    kwargs: dict[str, Any] = {
        "pool": pool,
        "amount0": amount0,
        "amount1": amount1,
        "range_lower": range_lower,
        "range_upper": range_upper,
        "protocol": protocol,
    }
    if protocol_params is not None:
        kwargs["protocol_params"] = protocol_params
    return LPOpenIntent(**kwargs)


# ---------------------------------------------------------------------------
# ``_compile_swap`` characterization tests
# ---------------------------------------------------------------------------


class TestCompileSwapHappyPaths:
    """Per-protocol happy paths through the default router swap body.

    Non-folded router protocols still compile in ``IntentCompiler``; Uniswap
    V3-family protocols must dispatch to connector compilers instead.
    """

    @pytest.mark.parametrize(
        "protocol",
        ["uniswap_v3", "uniswap_v2", "sushiswap", "pancakeswap_v3"],
    )
    @patch(SWAP_ADAPTER_CLS)
    def test_happy_path_per_protocol(self, mock_adapter_cls: MagicMock, protocol: str) -> None:
        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()

        intent = _make_swap_intent(protocol=protocol)
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == protocol
        # At minimum the swap tx — plus an approve for USDC (not native).
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "swap" in tx_types

    def test_aerodrome_dispatches_to_connector_compiler(self) -> None:
        """Aerodrome protocol dispatches to the connector compiler registry."""

        compiler = _make_compiler(chain="base")
        sentinel = MagicMock(name="aerodrome-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel

        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_swap_intent(protocol="aerodrome")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("aerodrome")
        connector_compiler.compile.assert_called_once()


class TestCompileSwapPriceImpactGuard:
    """The price impact guard is one of the highest-value branches."""

    @patch(SWAP_ADAPTER_CLS)
    def test_price_impact_guard_trips_on_low_quoter(self, mock_adapter_cls: MagicMock) -> None:
        """Quoter returning a fraction of oracle => FAILED with clear error."""

        # Oracle: 100 USDC / 2000 * 0.997 ~= 0.04985 WETH. Quoter returns
        # 0.0002 WETH (99.6% impact), well above the 30% default threshold.
        mock_adapter_cls.return_value = _make_mock_swap_adapter(
            quoter_amount=200_000_000_000_000,
        )
        compiler = _make_compiler()

        result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Price impact too high" in (result.error or "")
        assert "insufficient liquidity" in (result.error or "")

    @patch(SWAP_ADAPTER_CLS)
    def test_none_quoter_fails_closed(self, mock_adapter_cls: MagicMock) -> None:
        """No quoter reading must fail compilation (VIB-3160)."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter(quoter_amount=None)
        compiler = _make_compiler()

        result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.FAILED
        assert "on-chain quoter returned no amount" in (result.error or "").lower()


class TestCompileSwapSlippage:
    """Slippage math is applied to ``min_amount_out`` and drives metadata."""

    @patch(SWAP_ADAPTER_CLS)
    def test_slippage_applied_to_min_amount_out(self, mock_adapter_cls: MagicMock) -> None:
        """min_amount_out == quoter_amount * (1 - slippage) when quoter beats oracle."""

        # Oracle estimate ~49_850_000_000_000_000. Quoter equal to oracle so
        # the safety clamp (min(quoter, oracle)) stays at oracle exactly.
        quoter_amount = 49_850_000_000_000_000
        mock_adapter_cls.return_value = _make_mock_swap_adapter(quoter_amount=quoter_amount)
        compiler = _make_compiler()

        # 1% slippage
        intent = _make_swap_intent(max_slippage=Decimal("0.01"))
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        expected_min = int(Decimal(str(quoter_amount)) * Decimal("0.99"))
        assert result.action_bundle.metadata["min_amount_out"] == str(expected_min)

    @patch(SWAP_ADAPTER_CLS)
    def test_slippage_zero_allowed_boundary(self, mock_adapter_cls: MagicMock) -> None:
        """max_slippage=0 is a valid boundary and yields min_amount_out==quote."""

        quoter_amount = 49_850_000_000_000_000
        mock_adapter_cls.return_value = _make_mock_swap_adapter(quoter_amount=quoter_amount)
        compiler = _make_compiler()

        intent = _make_swap_intent(max_slippage=Decimal("0"))
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["min_amount_out"] == str(quoter_amount)


class TestCompileSwapApprovalChain:
    """Approval chain construction differs per gateway-allowance state."""

    @patch(SWAP_ADAPTER_CLS)
    def test_approval_chain_new_token(self, mock_adapter_cls: MagicMock) -> None:
        """No prior allowance => swap includes an approve transaction."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()
        # Force the approve path to emit a tx regardless of gateway state.
        approve_tx = MagicMock(name="approve-tx")
        approve_tx.to_dict.return_value = {"type": "approve"}
        approve_tx.gas_estimate = 50_000
        approve_tx.tx_type = "approve"
        with patch.object(compiler, "_build_approve_tx", return_value=[approve_tx]):
            result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["approve", "swap"]

    @patch(SWAP_ADAPTER_CLS)
    def test_no_approval_when_allowance_sufficient(self, mock_adapter_cls: MagicMock) -> None:
        """Sufficient existing allowance => only the swap tx (no approve)."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()

        # _build_approve_tx returns [] when allowance >= amount.
        with patch.object(compiler, "_build_approve_tx", return_value=[]):
            result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["swap"]


class TestCompileSwapErrorPaths:
    """Explicit failure modes the compile path exposes to the caller."""

    @patch(SWAP_ADAPTER_CLS)
    def test_missing_token_resolver_entry_fails(self, mock_adapter_cls: MagicMock) -> None:
        """Unknown from_token => CompilationStatus.FAILED with clear error."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()

        with patch.object(compiler, "_resolve_token", return_value=None):
            result = compiler.compile(_make_swap_intent(from_token="NOTAREALTOKEN"))

        assert result.status == CompilationStatus.FAILED
        assert "Unknown token" in (result.error or "")

    @patch(SWAP_ADAPTER_CLS)
    def test_unknown_router_fails(self, mock_adapter_cls: MagicMock) -> None:
        """Zero router address from adapter => FAILED."""

        mock_adapter = _make_mock_swap_adapter()
        mock_adapter.get_router_address.return_value = "0x0000000000000000000000000000000000000000"
        mock_adapter_cls.return_value = mock_adapter
        compiler = _make_compiler()

        result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Unknown router" in (result.error or "")


class TestCompileSwapDispatch:
    """Dispatch decisions that route away from the default router swap body."""

    def test_lifi_protocol_dispatches_to_lifi_helper(self) -> None:
        """protocol='lifi' routes to ``_compile_lifi_swap``."""

        compiler = _make_compiler()
        sentinel = MagicMock(name="lifi-result")
        with patch.object(compiler, "_compile_lifi_swap", return_value=sentinel) as mock_lifi:
            intent = _make_swap_intent(protocol="lifi")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_lifi.assert_called_once()

    def test_enso_protocol_dispatches_to_enso_helper(self) -> None:
        """protocol='enso' routes to ``_compile_enso_swap``."""

        compiler = _make_compiler()
        sentinel = MagicMock(name="enso-result")
        with patch.object(compiler, "_compile_enso_swap", return_value=sentinel) as mock_enso:
            intent = _make_swap_intent(protocol="enso")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_enso.assert_called_once()

    def test_curve_protocol_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        sentinel = MagicMock(name="curve-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_swap_intent(protocol="curve", from_token="USDC", to_token="DAI")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("curve")
        connector_compiler.compile.assert_called_once()
        args, kwargs = connector_compiler.compile.call_args
        assert len(args) == 2
        ctx, dispatched_intent = args
        assert isinstance(ctx, BaseCompilerContext)
        assert dispatched_intent is intent
        assert kwargs == {}

    def test_uniswap_v4_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler()
        sentinel = MagicMock(name="v4-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_swap_intent(protocol="uniswap_v4")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("uniswap_v4")
        connector_compiler.compile.assert_called_once()

    def test_fluid_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler()
        sentinel = MagicMock(name="fluid-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_swap_intent(protocol="fluid")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("fluid")
        connector_compiler.compile.assert_called_once()
        args, kwargs = connector_compiler.compile.call_args
        assert len(args) == 2
        ctx, dispatched_intent = args
        assert isinstance(ctx, BaseCompilerContext)
        assert dispatched_intent is intent
        assert kwargs == {}

    def test_traderjoe_v2_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler(chain="avalanche", price_oracle={"AVAX": Decimal("30"), "USDC": Decimal("1")})
        sentinel = MagicMock(name="tj-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_swap_intent(protocol="traderjoe_v2", from_token="USDC", to_token="AVAX")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("traderjoe_v2")
        connector_compiler.compile.assert_called_once()

    def test_pendle_pt_token_autodetect(self) -> None:
        """PT-prefixed to_token auto-routes to Pendle even without protocol set."""

        compiler = _make_compiler()
        sentinel = MagicMock(name="pendle-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_swap_intent(from_token="USDC", to_token="PT-sUSDe")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("pendle")
        connector_compiler.compile.assert_called_once()

    def test_cross_chain_swap_routes_to_cross_chain(self) -> None:
        """destination_chain != chain and protocol!=lifi => _compile_cross_chain_swap."""

        compiler = _make_compiler(chain="base")
        sentinel = MagicMock(name="cc-result")

        # Cross-chain swaps require protocol="enso" (or "lifi"). Enso is handled
        # below in _compile_cross_chain_swap.
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            chain="base",
            destination_chain="arbitrum",
            protocol="enso",
        )
        with patch.object(compiler, "_compile_cross_chain_swap", return_value=sentinel) as mock_cc:
            result = compiler.compile(intent)

        assert result is sentinel
        mock_cc.assert_called_once()


class TestCompileSwapAmountShapes:
    """Input-amount parsing: amount, amount_usd, 'all', and missing."""

    @patch(SWAP_ADAPTER_CLS)
    def test_token_amount_path(self, mock_adapter_cls: MagicMock) -> None:
        """``amount=Decimal('1.5')`` (token terms) produces correct wei amount_in."""

        compiler = _make_compiler()

        # 1.5 USDC (6 decimals) => amount_in = 1_500_000
        intent = _make_swap_intent(amount_usd=None, amount=Decimal("1.5"))
        # Force a reasonable quoter output so guard doesn't trip on tiny amount.
        # Oracle estimate: 1.5 USDC / $2000 * 0.997 * 1e18 ~= 7.4775e14. Tiny
        # trade, so set quoter close to oracle (~0.99 of it) to pass guard.
        mock_adapter_cls.return_value = _make_mock_swap_adapter(
            quoter_amount=740_272_500_000_000,  # 0.99 of oracle ~= 1% impact
        )
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["amount_in"] == str(1_500_000)

    @patch(SWAP_ADAPTER_CLS)
    def test_unresolved_amount_all_fails(self, mock_adapter_cls: MagicMock) -> None:
        """amount='all' must be resolved before compile; unresolved => FAILED."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()
        # Bypass the amount_resolver by short-circuiting resolve_amount_all.
        # We construct the intent directly with amount='all'; the compiler's
        # amount resolver would normally resolve it, but with no balance source
        # it returns the intent unchanged. Then the inner check fires.
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
        )
        # Short-circuit the resolver so the 'all' check inside _compile_swap fires.
        with patch(
            "almanak.framework.intents.amount_resolver.resolve_amount_all",
            return_value=intent,
        ):
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "amount='all'" in (result.error or "")

    @patch(SWAP_ADAPTER_CLS)
    def test_missing_to_token_fails(self, mock_adapter_cls: MagicMock) -> None:
        """Unknown to_token fails with a clear error."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()

        # Resolve from_token OK, to_token None.
        from_token_info = _make_token_info("USDC", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6)

        def resolve(token: str, chain: str | None = None) -> Any:
            if token == "USDC":
                return from_token_info
            return None

        with patch.object(compiler, "_resolve_token", side_effect=resolve):
            result = compiler.compile(_make_swap_intent(from_token="USDC", to_token="NOTATOKEN"))

        assert result.status == CompilationStatus.FAILED
        assert "Unknown token" in (result.error or "")

    @patch(SWAP_ADAPTER_CLS)
    def test_price_oracle_missing_fails_closed(self, mock_adapter_cls: MagicMock) -> None:
        """Oracle missing a price for the pair => FAILED with slippage-protection error."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler()

        # Force _calculate_expected_output to raise ValueError (missing price).
        with patch.object(
            compiler,
            "_calculate_expected_output",
            side_effect=ValueError("no price for SOMETOKEN"),
        ):
            result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Cannot calculate slippage protection" in (result.error or "")


class TestCompileSwapNativeToken:
    """Native from/to token wrap/unwrap paths."""

    @patch(SWAP_ADAPTER_CLS)
    def test_native_from_token_sets_value(self, mock_adapter_cls: MagicMock) -> None:
        """Swapping FROM native => tx.value = amount_in and WETH used as actual_from."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler(chain="arbitrum")

        # Build a native token (ETH) info; force _resolve_token to return it.
        native_eth = _make_token_info("ETH", "0x0000000000000000000000000000000000000000", 18)
        native_eth.is_native = True
        usdc = _make_token_info("USDC", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6)

        def resolve(token: str, chain: str | None = None) -> Any:
            if token == "ETH":
                return native_eth
            if token == "USDC":
                return usdc
            return None

        # amount_usd 2000 -> ~1 ETH in = 10**18 wei.
        with (
            patch.object(compiler, "_resolve_token", side_effect=resolve),
            patch.object(
                compiler,
                "_get_wrapped_native_address",
                return_value="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            ),
        ):
            intent = _make_swap_intent(from_token="ETH", to_token="USDC", amount_usd=Decimal("2000"))
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        swap_txs = [tx for tx in result.transactions if tx.tx_type == "swap"]
        assert len(swap_txs) == 1
        # Native swap => value > 0 on the swap tx.
        assert swap_txs[0].value > 0
        # No approve needed for native.
        assert all(tx.tx_type != "approve" for tx in result.transactions)

    @patch(SWAP_ADAPTER_CLS)
    def test_native_to_token_adds_warning(self, mock_adapter_cls: MagicMock) -> None:
        """Swapping TO native => warning about WETH output."""

        mock_adapter_cls.return_value = _make_mock_swap_adapter()
        compiler = _make_compiler(chain="arbitrum")

        native_eth = _make_token_info("ETH", "0x0000000000000000000000000000000000000000", 18)
        native_eth.is_native = True
        usdc = _make_token_info("USDC", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6)

        def resolve(token: str, chain: str | None = None) -> Any:
            if token == "ETH":
                return native_eth
            if token == "USDC":
                return usdc
            return None

        with (
            patch.object(compiler, "_resolve_token", side_effect=resolve),
            patch.object(
                compiler,
                "_get_wrapped_native_address",
                return_value="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            ),
        ):
            intent = _make_swap_intent(from_token="USDC", to_token="ETH", amount_usd=Decimal("100"))
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert any("unwrap" in w.lower() or "receive WETH" in w for w in result.warnings)


class TestCompileSwapPoolValidation:
    """Pool validation branch (selected_fee_tier set)."""

    @patch(VALIDATE_V3_POOL)
    @patch(SWAP_ADAPTER_CLS)
    def test_pool_validation_failure_propagates(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """Pool validation returns NOT_FOUND => _compile_swap returns that FAILED result."""

        # Configure adapter with a selected fee tier to trigger pool validation.
        mock_adapter_cls.return_value = _make_mock_swap_adapter(selected_fee_tier=3000)
        # Pool validation fails NOT_FOUND.
        bad_pool = MagicMock()
        bad_pool.exists = False
        from almanak.framework.intents.pool_validation import PoolValidationReason

        bad_pool.reason = PoolValidationReason.NOT_FOUND
        bad_pool.error = "Pool does not exist"
        bad_pool.warning = None
        bad_pool.pool_address = None
        mock_validate.return_value = bad_pool

        compiler = _make_compiler()
        result = compiler.compile(_make_swap_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Pool does not exist" in (result.error or "")


# ---------------------------------------------------------------------------
# ``_compile_lp_open`` characterization tests
# ---------------------------------------------------------------------------


def _make_token_info(symbol: str, address: str, decimals: int = 18) -> MagicMock:
    ti = MagicMock(name=f"TokenInfo({symbol})")
    ti.symbol = symbol
    ti.address = address
    ti.decimals = decimals
    ti.is_native = False
    ti.to_dict.return_value = {"symbol": symbol, "address": address, "decimals": decimals}
    return ti


# Tokens used across LP tests — real chain-agnostic addresses. USDC < WETH so
# ``_parse_pool_info`` reports ``tokens_swapped=False`` for "USDC/WETH/3000".
_USDC = _make_token_info("USDC", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6)
_WETH = _make_token_info("WETH", "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", 18)
# WBNB > USDT so "WBNB/USDT/500" triggers the tokens_swapped=True path.
_WBNB = _make_token_info("WBNB", "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", 18)
_USDT = _make_token_info("USDT", "0x55d398326f99059fF775485246999027B3197955", 18)


class TestCompileLPOpenHappyPaths:
    """Per-protocol LP_OPEN happy paths through the connector compiler."""

    @patch(FETCH_SLOT0)
    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_uniswap_v3_in_range(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
        mock_slot0: MagicMock,
    ) -> None:
        """In-range LP_OPEN: current price sits between lower and upper."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            intent = _make_lp_intent(
                pool="USDC/WETH/3000",
                range_lower=Decimal("1500"),
                range_upper=Decimal("2500"),
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "uniswap_v3"
        assert result.action_bundle.metadata["fee_tier"] == 3000
        # tick_lower strictly less than tick_upper after spacing alignment.
        tl = result.action_bundle.metadata["tick_lower"]
        tu = result.action_bundle.metadata["tick_upper"]
        assert tl < tu
        # slot0 didn't need to fire (pool_address=None on mock).
        mock_slot0.assert_not_called()

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_uniswap_v3_out_of_range_upper(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """Out-of-range (both bounds above current price) still compiles."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            intent = _make_lp_intent(
                pool="USDC/WETH/3000",
                # Both bounds far above spot (2000).
                range_lower=Decimal("10000"),
                range_upper=Decimal("20000"),
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_crossed_range_rejected_by_intent_validator(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """range_lower > range_upper is rejected at LPOpenIntent construction time."""

        # The intent model's own validator refuses range_lower >= range_upper,
        # so the compiler never even sees a crossed range. Pin that behaviour.
        with pytest.raises(ValueError, match="range_lower must be less than range_upper"):
            LPOpenIntent(
                pool="USDC/WETH/3000",
                amount0=Decimal("1000"),
                amount1=Decimal("0.5"),
                range_lower=Decimal("3000"),
                range_upper=Decimal("2000"),
                protocol="uniswap_v3",
            )

    def test_aerodrome_slipstream_dispatches_to_connector_compiler(self) -> None:
        """protocol='aerodrome_slipstream' routes through the connector compiler registry."""

        compiler = _make_compiler(chain="base")
        sentinel = MagicMock(name="slipstream-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel

        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_intent(
                pool="USDC/WETH/500",
                protocol="aerodrome_slipstream",
                # Slipstream accepts tick-based ranges (negative allowed), but
                # for dispatch-only we just need a valid positive range.
                range_lower=Decimal("1000"),
                range_upper=Decimal("3000"),
            )
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("aerodrome_slipstream")
        connector_compiler.compile.assert_called_once()

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_pancakeswap_v3(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="bsc", price_oracle={"WBNB": Decimal("600"), "USDT": Decimal("1")})

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDT, _WBNB, 500, True)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            intent = _make_lp_intent(
                pool="WBNB/USDT/500",
                protocol="pancakeswap_v3",
                amount0=Decimal("0.165"),  # user's WBNB
                amount1=Decimal("100"),  # user's USDT
                range_lower=Decimal("550"),
                range_upper=Decimal("670"),
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["protocol"] == "pancakeswap_v3"

    def test_traderjoe_v2_dispatches_to_connector_compiler(self) -> None:
        """protocol='traderjoe_v2' routes through the connector compiler registry."""

        compiler = _make_compiler(chain="avalanche", price_oracle={"AVAX": Decimal("30"), "USDC": Decimal("1")})
        sentinel = MagicMock(name="tj-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_intent(pool="USDC/AVAX/20", protocol="traderjoe_v2")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("traderjoe_v2")
        connector_compiler.compile.assert_called_once()


class TestCompileLPOpenTokenOrdering:
    """Token0/token1 ordering normalization + price-range inversion."""

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_tokens_swapped_amounts_and_range_inverted(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """When _parse_pool_info returns tokens_swapped=True, amounts and
        range are inverted so (token0 < token1) address order is preserved.
        """

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="bsc", price_oracle={"WBNB": Decimal("600"), "USDT": Decimal("1")})

        approve_calls: list[tuple[str, int]] = []

        def mock_build_approve(token_addr: str, spender: str, amount: int) -> list:
            approve_calls.append((token_addr, amount))
            return []

        # Spy on _price_to_tick to capture the (possibly inverted) price inputs.
        # _price_to_tick is a @staticmethod, so the spy signature matches the
        # real function exactly (no self). The real implementation still runs
        # via side_effect so tick math / spacing alignment produce valid output.
        price_to_tick_calls: list[Decimal] = []
        real_price_to_tick = IntentCompiler._price_to_tick

        def spy_price_to_tick(price, *, token0_decimals, token1_decimals):  # type: ignore[no-untyped-def]
            price_to_tick_calls.append(price)
            return real_price_to_tick(
                price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
            )

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDT, _WBNB, 500, True)),
            patch.object(compiler, "_build_approve_tx", side_effect=mock_build_approve),
            patch.object(IntentCompiler, "_price_to_tick", side_effect=spy_price_to_tick),
        ):
            intent = _make_lp_intent(
                pool="WBNB/USDT/500",
                protocol="pancakeswap_v3",
                amount0=Decimal("0.165"),  # user's WBNB
                amount1=Decimal("100"),  # user's USDT
                range_lower=Decimal("550"),
                range_upper=Decimal("670"),
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        # After swap: USDT is token0 (receives user's amount1=100),
        # WBNB is token1 (receives user's amount0=0.165). Both tokens are
        # 18 decimals here so the wei conversion is * 10**18.
        approve_by_token = dict(approve_calls)
        assert approve_by_token[_USDT.address] == int(Decimal("100") * Decimal(10**18))
        assert approve_by_token[_WBNB.address] == int(Decimal("0.165") * Decimal(10**18))
        # Pin that the price range was inverted for the new token order. The
        # user specified [550, 670] as WBNB/USDT; after swap the compiler must
        # feed _price_to_tick the reciprocal range (1/670, 1/550) so ticks
        # are computed in the (USDT, WBNB) domain that matches the pool.
        # The inverted lower/upper are fed in order.
        assert len(price_to_tick_calls) == 2
        assert price_to_tick_calls[0] == Decimal(1) / Decimal("670")
        assert price_to_tick_calls[1] == Decimal(1) / Decimal("550")


class TestCompileLPOpenApprovalChain:
    """Approval chain covers both tokens (unless native)."""

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_approve_built_for_both_tokens(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")

        approve_calls: list[str] = []

        def mock_build_approve(token_addr: str, spender: str, amount: int) -> list:
            approve_calls.append(token_addr)
            return []

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", side_effect=mock_build_approve),
        ):
            compiler.compile(_make_lp_intent())

        # Both tokens are ERC20 here (is_native=False) and both amounts > 0.
        assert _USDC.address in approve_calls
        assert _WETH.address in approve_calls


class TestCompileLPOpenSlippage:
    """LP slippage applied to amountN_min; ``protocol_params.lp_slippage`` overrides."""

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_default_lp_slippage_applied(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")
        # Use a known default slippage (99% default => 1% min multiplier).
        compiler.default_lp_slippage = Decimal("0.20")  # 20% slippage => 80% min

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            intent = _make_lp_intent(
                amount0=Decimal("1000"),
                amount1=Decimal("0.5"),
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        a0_desired = int(Decimal("1000") * Decimal(10**6))  # USDC 6 decimals
        a1_desired = int(Decimal("0.5") * Decimal(10**18))  # WETH 18 decimals
        assert result.action_bundle.metadata["amount0_desired"] == str(a0_desired)
        assert result.action_bundle.metadata["amount1_desired"] == str(a1_desired)
        # 20% slippage => min = 80% of desired (truncated to int).
        assert result.action_bundle.metadata["amount0_min"] == str(int(a0_desired * Decimal("0.8")))
        assert result.action_bundle.metadata["amount1_min"] == str(int(a1_desired * Decimal("0.8")))

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_protocol_params_lp_slippage_override(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """protocol_params.lp_slippage=1.0 yields zero minimums (safe-for-testing)."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            intent = _make_lp_intent(
                protocol_params={"lp_slippage": 1.0},
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["amount0_min"] == "0"
        assert result.action_bundle.metadata["amount1_min"] == "0"


class TestCompileLPOpenErrorPaths:
    """Explicit failure modes that strategies rely on."""

    def test_unsupported_solana_protocol_fails(self) -> None:
        """Non-Solana LP protocol on Solana => FAILED with clear error."""

        # We can't easily build a Solana-flavoured compiler end-to-end here
        # (chain name resolution + solana detection), so exercise the dispatch
        # by forcing _is_solana_chain=True.
        compiler = _make_compiler(chain="arbitrum")
        with patch.object(compiler, "_is_solana_chain", return_value=True):
            intent = _make_lp_intent(protocol="uniswap_v3")
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not supported for LP_OPEN on Solana" in (result.error or "")

    @patch(LP_ADAPTER_CLS)
    def test_unknown_position_manager_fails(self, mock_adapter_cls: MagicMock) -> None:
        """Adapter returning zero position manager => FAILED."""

        mock_adapter = _make_mock_lp_adapter(
            position_manager="0x0000000000000000000000000000000000000000",
        )
        mock_adapter_cls.return_value = mock_adapter
        compiler = _make_compiler(chain="arbitrum")

        result = compiler.compile(_make_lp_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Unknown position manager" in (result.error or "")

    @patch(LP_ADAPTER_CLS)
    def test_unparseable_pool_fails(self, mock_adapter_cls: MagicMock) -> None:
        """_parse_pool_info returning None => FAILED with clear error."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler(chain="arbitrum")

        with patch.object(compiler, "_parse_pool_info", return_value=None):
            result = compiler.compile(_make_lp_intent(pool="garbage"))

        assert result.status == CompilationStatus.FAILED
        assert "Could not parse pool info" in (result.error or "")

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_collapsed_tick_range_fails(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """Range that collapses to a single tick after spacing alignment => FAILED."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")

        # Force _price_to_tick to return identical ticks so tick_lower >=
        # tick_upper after spacing alignment.
        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(IntentCompiler, "_price_to_tick", return_value=100),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            result = compiler.compile(_make_lp_intent())

        assert result.status == CompilationStatus.FAILED
        assert "tick range collapsed" in (result.error or "").lower()


class TestCompileLPOpenSlot0Recompute:
    """Slot0 recompute branch corrects amounts when pool price diverges."""

    @patch(FETCH_SLOT0)
    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_slot0_recompute_corrects_amounts(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
        mock_fetch_slot0: MagicMock,
    ) -> None:
        """When pool_address is present, slot0 fetch triggers amount recompute."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        # Pool exists WITH address so the slot0 recompute branch fires.
        mock_validate.return_value = _mock_pool_validation_ok(pool_address="0xC6962004f452bE9203591991D15f6b388e09E8D0")
        # Return a plausible sqrtPriceX96 for ~2000 USDC/WETH. The real
        # recomputed amounts aren't important for this pin — we just need to
        # exercise the branch and verify the compile still succeeds.
        mock_fetch_slot0.return_value = (int(2**96), 0)

        compiler = _make_compiler(chain="arbitrum")
        compiler.rpc_url = "http://localhost:8545"

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
            patch(
                "almanak.framework.intents.lp_math.recompute_lp_amounts",
                return_value=(100, 200),
            ) as mock_recompute,
        ):
            intent = _make_lp_intent()
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        mock_fetch_slot0.assert_called_once()
        # Pin that _compile_lp_open actually USES the recomputed amounts and
        # not just that the branch ran. recompute_lp_amounts returned (100, 200);
        # those values must flow into action_bundle metadata unchanged.
        mock_recompute.assert_called_once()
        assert result.action_bundle.metadata["amount0_desired"] == "100"
        assert result.action_bundle.metadata["amount1_desired"] == "200"


class TestCompileLPOpenSlipstreamSlot0Recompute:
    """VIB-3737 regression: Aerodrome Slipstream LP_OPEN must align desired
    amounts to the pool's slot0 sqrtPriceX96 and derive the on-chain mins
    from those pool-aligned amounts (NOT the raw oracle inputs).

    Before this fix, the slipstream compile path bypassed the V3 slot0
    recompute helper and the adapter then rebuilt mins from the raw amounts
    using a flat basis-points formula, causing on-chain "Price slippage check"
    reverts whenever the oracle ratio diverged from the pool ratio.
    """

    @patch("almanak.framework.connectors.aerodrome.AerodromeAdapter")
    @patch("almanak.framework.intents.pool_validation.fetch_v3_pool_sqrt_price_x96")
    @patch("almanak.framework.intents.pool_validation.validate_aerodrome_cl_pool")
    def test_slot0_recompute_flows_into_adapter_and_metadata(
        self,
        mock_validate_cl: MagicMock,
        mock_fetch_slot0: MagicMock,
        mock_adapter_cls: MagicMock,
    ) -> None:
        """Recomputed amounts (sentinel 100, 200) must reach the adapter call
        AND the action-bundle metadata; mins must be derived from those
        sentinels, not from intent.amount0 / intent.amount1.
        """

        mock_validate_cl.return_value = _mock_pool_validation_ok(
            pool_address="0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59",
        )
        # Plausible sqrtPriceX96 (~1:1); recompute_lp_amounts is mocked out so
        # the actual value doesn't drive arithmetic, only branch entry.
        mock_fetch_slot0.return_value = (int(2**96), 0)

        adapter_instance = MagicMock(name="AerodromeAdapterInstance")
        cl_result = MagicMock(name="CLLiquidityResult")
        cl_result.success = True
        # One mint tx is enough to satisfy the action-bundle assembly.
        mint_tx = MagicMock(name="mint-tx")
        mint_tx.gas_estimate = 500_000
        mint_tx.tx_type = "add_liquidity"
        mint_tx.to_dict.return_value = {"type": "add_liquidity"}
        cl_result.transactions = [mint_tx]
        adapter_instance.add_cl_liquidity.return_value = cl_result
        mock_adapter_cls.return_value = adapter_instance

        compiler = _make_compiler(chain="base")

        with (
            patch.object(compiler, "_resolve_token", side_effect=lambda sym: {"USDC": _USDC, "WETH": _WETH}[sym]),
            patch(
                "almanak.framework.intents.lp_math.recompute_lp_amounts",
                return_value=(100, 200),
            ) as mock_recompute,
        ):
            intent = _make_lp_intent(
                pool="USDC/WETH/200",
                amount0=Decimal("1"),  # 1 USDC
                amount1=Decimal("0.0005"),  # 0.0005 WETH
                # Tick bounds aligned to tick_spacing=200, lower < upper.
                range_lower=Decimal("-200"),
                range_upper=Decimal("200"),
                protocol="aerodrome_slipstream",
            )
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error

        mock_fetch_slot0.assert_called_once()
        mock_recompute.assert_called_once()

        # The recomputed sentinels (100, 200) must flow into the adapter call
        # via the wei-overload kwargs, not the raw Decimal amount_a / amount_b.
        adapter_instance.add_cl_liquidity.assert_called_once()
        kwargs = adapter_instance.add_cl_liquidity.call_args.kwargs
        assert kwargs["amount_a_wei"] == 100, "Pool-aligned amount0 must reach adapter as amount_a_wei"
        assert kwargs["amount_b_wei"] == 200, "Pool-aligned amount1 must reach adapter as amount_b_wei"

        # Mins must be derived from the POST-RECOMPUTE amounts (100, 200), NOT
        # from the raw intent.amount0 / intent.amount1 (1 USDC = 1_000_000 wei,
        # 0.0005 WETH = 5e14 wei). This is the heart of the VIB-3737 fix.
        assert kwargs["amount_a_min_wei"] is not None
        assert kwargs["amount_b_min_wei"] is not None
        assert kwargs["amount_a_min_wei"] <= 100, (
            f"amount_a_min_wei={kwargs['amount_a_min_wei']} exceeds the post-recompute "
            "amount0=100 — mins were derived from raw amounts, regression of VIB-3737"
        )
        assert kwargs["amount_b_min_wei"] <= 200, (
            f"amount_b_min_wei={kwargs['amount_b_min_wei']} exceeds the post-recompute "
            "amount1=200 — mins were derived from raw amounts, regression of VIB-3737"
        )

        # Metadata must expose the pool-aligned wei amounts (and mins) so the
        # orchestrator's _preflight_lp_open_requirements balance check can find
        # amount0_desired / amount1_desired (matching V3 metadata shape).
        metadata = result.action_bundle.metadata
        assert metadata["amount0_desired"] == "100"
        assert metadata["amount1_desired"] == "200"
        assert metadata["amount0_min"] == str(kwargs["amount_a_min_wei"])
        assert metadata["amount1_min"] == str(kwargs["amount_b_min_wei"])
        assert metadata["protocol"] == "aerodrome_slipstream"


class TestCompileLPOpenTickSpacing:
    """Tick alignment uses the correct spacing per fee tier."""

    @pytest.mark.parametrize(
        "fee_tier,expected_spacing",
        [(100, 1), (500, 10), (2500, 50), (3000, 60), (10_000, 200)],
    )
    def test_get_tick_spacing_per_fee_tier(self, fee_tier: int, expected_spacing: int) -> None:
        assert IntentCompiler._get_tick_spacing(fee_tier) == expected_spacing

    @patch(VALIDATE_V3_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_ticks_aligned_to_spacing(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """Final tick bounds are multiples of the fee tier's tick spacing."""

        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        mock_validate.return_value = _mock_pool_validation_ok()
        compiler = _make_compiler(chain="arbitrum")

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(_USDC, _WETH, 3000, False)),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            result = compiler.compile(_make_lp_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        spacing = IntentCompiler._get_tick_spacing(3000)  # => 60
        assert result.action_bundle.metadata["tick_lower"] % spacing == 0
        assert result.action_bundle.metadata["tick_upper"] % spacing == 0


# ---------------------------------------------------------------------------
# Fail-closed when a connector-owned compiler is missing (post-fold cutover)
# ---------------------------------------------------------------------------


class TestConnectorMissingFailsClosed:
    """A V3-fork with no registered connector compiler must fail closed.

    UNISWAP_V3_FORKS mirrors the registry exactly, so reaching the
    fall-through with a fork means the registry/import is broken. That must
    surface as a clear "not registered" error (mirroring ``_compile_swap``),
    not a misleading "unsupported protocol" using the raw intent.protocol
    (which can be ``None`` on default-protocol flows).
    """

    GET_CONNECTOR = "almanak.framework.intents.compiler.get_connector_compiler"
    _EXPECTED = "Connector compiler for protocol 'uniswap_v3' is not registered."

    def _assert_fail_closed(self, result) -> None:
        assert result.status == CompilationStatus.FAILED
        assert result.error == self._EXPECTED
        assert "None" not in result.error
        assert "is not supported" not in result.error

    def test_lp_open_fork_missing_connector_fails_closed(self) -> None:
        compiler = _make_compiler()
        with patch(self.GET_CONNECTOR, return_value=None):
            result = compiler.compile(_make_lp_intent(protocol="uniswap_v3"))
        self._assert_fail_closed(result)

    def test_lp_close_fork_missing_connector_fails_closed(self) -> None:
        compiler = _make_compiler()
        intent = Intent.lp_close(position_id="123", pool="USDC/WETH/3000", protocol="uniswap_v3")
        with patch(self.GET_CONNECTOR, return_value=None):
            result = compiler.compile(intent)
        self._assert_fail_closed(result)

    def test_collect_fees_fork_missing_connector_fails_closed(self) -> None:
        compiler = _make_compiler()
        intent = Intent.collect_fees("USDC/WETH/3000", protocol="uniswap_v3")
        with patch(self.GET_CONNECTOR, return_value=None):
            result = compiler.compile(intent)
        self._assert_fail_closed(result)
