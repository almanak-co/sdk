"""Unit tests for Curve Finance intent compilation paths.

Tests verify that IntentCompiler correctly compiles SwapIntent, LPOpenIntent,
and LPCloseIntent for the curve protocol by mocking the CurveAdapter.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Patch targets — lazy-imported from the source module inside compile methods.
# When compiler does `from almanak.connectors.curve.adapter import CurveAdapter`,
# it fetches the object from the source module, so we patch there.
ADAPTER_MODULE = "almanak.connectors.curve.adapter"
CURVE_ADAPTER_CLS = f"{ADAPTER_MODULE}.CurveAdapter"
CURVE_CONFIG_CLS = f"{ADAPTER_MODULE}.CurveConfig"
CURVE_POOLS_PATH = f"{ADAPTER_MODULE}.CURVE_POOLS"
CURVE_ADDRESSES_PATH = f"{ADAPTER_MODULE}.CURVE_ADDRESSES"

# Minimal CURVE_POOLS fixture — uses USDC/USDT/DAI which exist in the token registry.
# FRAX is not in the default registry, so we avoid it for unit tests.
MOCK_CURVE_POOLS = {
    "ethereum": {
        "usdc_usdt": {
            "address": "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2",
            "lp_token": "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC",
            "coins": ["USDC", "USDT"],
            "coin_addresses": [
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
        },
        "3pool": {
            "address": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            "lp_token": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
            "coins": ["DAI", "USDC", "USDT"],
            "coin_addresses": [
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
        },
        # Real registered 2-coin pool — coin/address combo copied verbatim from
        # CURVE_POOLS["ethereum"]["steth"], so the asset-set resolver tests
        # exercise a genuine prod pool (not a relabeled address).
        "steth": {
            "address": "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022",
            "lp_token": "0x06325440D014e39736583c165C2963BA99fAf14E",
            "coins": ["ETH", "stETH"],
            "coin_addresses": [
                "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
                "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
        },
    }
}

STETH_POOL = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"

MOCK_CURVE_ADDRESSES = {
    "ethereum": {
        "router": "0x16C6521Dff6baB339122a0FE25a9116693265353",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf",
        "twocrypto_factory": "0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F",
        "tricrypto_factory": "0x0c0e5f2fF0ff18a3be9b835635039256dC4B4963",
        "crv_token": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    },
}

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_USDT_POOL = "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2"
THREEPOOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"


def _make_mock_tx(description: str = "Curve swap USDC -> USDT", gas: int = 246_000) -> MagicMock:
    """Create a mock TransactionData (Curve adapter format)."""
    tx = MagicMock()
    tx.gas_estimate = gas
    tx.description = description
    tx.to_dict.return_value = {
        "to": USDC_USDT_POOL,
        "value": "0",
        "data": "0x3df02124" + "00" * 128,
        "gas_estimate": gas,
        "description": description,
        "tx_type": "swap",
    }
    return tx


def _make_mock_swap_result(success: bool = True, error: str | None = None) -> MagicMock:
    """Create a mock SwapResult.

    Sets concrete numeric values for ``amount_out_estimate`` and
    ``token_out_decimals`` (not bare MagicMocks) because the Phase B compile
    path reads them to enrich ``bundle_metadata["expected_output_human"]``
    via ``swap_result.amount_out_estimate > 0``. Left as MagicMock, the
    comparison raises ``'>' not supported between 'MagicMock' and 'int'``.
    Values below correspond to a realistic USDC -> USDT quote (~1000 out,
    USDT 6 decimals).
    """
    result = MagicMock()
    result.success = success
    result.error = error
    if success:
        approve_tx = _make_mock_tx("Approve USDC", 46_000)
        swap_tx = _make_mock_tx("Curve swap USDC -> USDT", 200_000)
        result.transactions = [approve_tx, swap_tx]
        result.amount_out_estimate = 1_000_000_000  # 1000 USDT in wei (6 decimals)
        result.token_out_decimals = 6
    else:
        result.transactions = []
        result.amount_out_estimate = 0
        result.token_out_decimals = -1
    return result


def _make_mock_liq_result(success: bool = True, error: str | None = None, op: str = "add_liquidity") -> MagicMock:
    """Create a mock LiquidityResult."""
    result = MagicMock()
    result.success = success
    result.error = error
    if success:
        tx = _make_mock_tx(f"Curve {op}", 250_000)
        result.transactions = [tx]
    else:
        result.transactions = []
    return result


@pytest.fixture
def compiler():
    """IntentCompiler for Ethereum with placeholder prices."""
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    return IntentCompiler(chain="ethereum", wallet_address=TEST_WALLET, config=config)


# =============================================================================
# SWAP
# =============================================================================


class TestCurveSwap:
    """Tests for connector-owned Curve swap compilation."""

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_success_auto_pool_lookup(self, mock_config_cls, mock_adapter_cls, compiler):
        """Curve swap auto-selects pool by token pair and returns success."""
        mock_adapter = MagicMock()
        mock_adapter.swap.return_value = _make_mock_swap_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        assert result.action_bundle.metadata["protocol"] == "curve"
        assert len(result.action_bundle.transactions) == 2  # approve + swap
        # VIB-3203 Phase B: the compile path must persist
        # ``expected_output_human`` in bundle_metadata so the ResultEnricher
        # forwards it to the Curve receipt parser for realized-slippage_bps.
        # Without this assertion, a future refactor that drops the metadata
        # write would silently re-introduce the "Curve parser is dead code"
        # regression that this PR restores the fix for (pr-auditor
        # Important #1 on #1606 orphaned 6454382a6).
        # amount_out_estimate=1_000_000_000, decimals=6 -> "1000" USDT.
        assert result.action_bundle.metadata["expected_output_human"] == "1000"
        mock_adapter.swap.assert_called_once()

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_with_token_amount(self, mock_config_cls, mock_adapter_cls, compiler):
        """Curve swap works with direct token amount (not USD)."""
        mock_adapter = MagicMock()
        mock_adapter.swap.return_value = _make_mock_swap_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount=Decimal("500"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # Same expected_output_human invariant as the USD-amount path —
        # the direct-token path must not bypass metadata enrichment.
        assert result.action_bundle.metadata["expected_output_human"] == "1000"
        call_kwargs = mock_adapter.swap.call_args
        # amount_in should be 500 (the direct token amount)
        assert call_kwargs.kwargs["amount_in"] == Decimal("500")

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_with_zero_quote_fails_closed(self, mock_config_cls, mock_adapter_cls, compiler):
        """Degenerate/drained pool returning amount_out_estimate=0 must
        fail compilation — the adapter floors amount_out_minimum at 1 wei
        which is effectively no slippage protection. Fail-closed matches
        the TJ V2 zero-quote guard in this same PR (Gemini audit fix,
        pr-auditor Potential #6 for consistency across protocols).
        """
        mock_adapter = MagicMock()
        zero_result = _make_mock_swap_result(success=True)
        zero_result.amount_out_estimate = 0  # drained pool / zero quote
        mock_adapter.swap.return_value = zero_result
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "non-positive amount_out_estimate" in result.error
        assert "no real slippage floor" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_with_missing_decimals_fails_closed(self, mock_config_cls, mock_adapter_cls, compiler):
        """``token_out_decimals < 0`` is the adapter's "unknown" sentinel
        (see SwapResult). Same fail-closed invariant as zero quote — if we
        can't convert the wei estimate to human units, we can't produce a
        valid ``expected_output_human`` anchor, and forwarding it blind
        would break the realized-slippage calculation downstream.
        """
        mock_adapter = MagicMock()
        weird_result = _make_mock_swap_result(success=True)
        weird_result.amount_out_estimate = 1_000_000_000
        weird_result.token_out_decimals = -1  # adapter sentinel for unknown
        mock_adapter.swap.return_value = weird_result
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "non-positive amount_out_estimate" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_swap_no_pool_found_returns_failed(self, compiler):
        """Swap fails with helpful error when no pool matches the token pair."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="WBTC",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "No Curve pool found" in result.error
        assert "WETH" in result.error or "WBTC" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, {})
    def test_swap_unsupported_chain_returns_failed(self):
        """Swap fails when chain is not supported by Curve."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="avalanche", wallet_address=TEST_WALLET, config=config)

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Curve is not supported on avalanche" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_adapter_failure_propagates(self, mock_config_cls, mock_adapter_cls, compiler):
        """Adapter failure is propagated as CompilationStatus.FAILED."""
        mock_adapter = MagicMock()
        mock_adapter.swap.return_value = _make_mock_swap_result(success=False, error="pool is paused")
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "pool is paused" in result.error


# =============================================================================
# LP OPEN
# =============================================================================


class TestCurveLPOpen:
    """Tests for connector-owned Curve LP open compilation."""

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_by_pool_name_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP open with pool name auto-resolves to address."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="usdc_usdt",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        assert result.action_bundle.metadata["protocol"] == "curve"
        mock_adapter.add_liquidity.assert_called_once()

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_by_pool_address_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP open with explicit pool address also resolves correctly."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool=USDC_USDT_POOL,
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_3pool_pads_amounts(self, mock_config_cls, mock_adapter_cls, compiler):
        """For 3-coin pools, amounts list is padded to n_coins with 0."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("500"),  # DAI
            amount1=Decimal("500"),  # USDC
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        call_kwargs = mock_adapter.add_liquidity.call_args
        amounts = call_kwargs.kwargs["amounts"]
        assert len(amounts) == 3  # padded to n_coins=3
        assert amounts[2] == Decimal("0")  # third coin = 0

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_open_unknown_pool_returns_failed(self, compiler):
        """LP open fails with helpful error for unknown pool."""
        intent = LPOpenIntent(
            pool="nonexistent_pool",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown Curve pool" in result.error


# =============================================================================
# LP ASSET-SET RESOLVER (VIB-3946)
# =============================================================================


class TestCurveLPAssetSetResolver:
    """Tests for the Curve LP asset-set resolver (VIB-3946).

    Strategy authors describe a *concept* ("USDT/USDC stable LP") rather than a
    protocol pool nickname. The compiler must resolve the asset set to the
    registered pool whose coins match exactly — the LP analogue of the SWAP pool
    resolver.
    """

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_asset_set_resolves_2coin_pool(self, mock_config_cls, mock_adapter_cls, compiler):
        """``ETH/stETH`` resolves to the real 2-coin ``steth`` pool (not "Unknown").

        Uses a genuine registered pool (coin/address combo copied verbatim from
        the prod CURVE_POOLS), so the 2-coin acceptance case is a real one.
        """
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="ETH/stETH",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["pool_name"] == "steth"
        assert result.action_bundle.metadata["pool_address"] == STETH_POOL

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_asset_set_resolves_3coin_pool(self, mock_config_cls, mock_adapter_cls, compiler):
        """``USDT/USDC/DAI`` (the human description of 3pool) resolves to ``3pool``.

        This is the exact QA-100 S-051 failure: the author passes the 3-asset
        description and previously hit "Unknown Curve pool".
        """
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="USDT/USDC/DAI",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["pool_name"] == "3pool"
        assert result.action_bundle.metadata["pool_address"] == THREEPOOL

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_asset_set_does_not_leak_into_accounting(self, mock_config_cls, mock_adapter_cls, compiler):
        """The asset-set string must NOT leak into accounting (VIB-3946 regression).

        Root-cause fix: the compiler resolves the canonical pool into
        ``action_bundle.metadata["pool_name"]`` ("3pool"); the accounting layer
        consumes THAT (threaded as ``resolved_pool``) instead of re-parsing the
        raw ``intent.pool`` user input. We assert the *accounting-facing*
        behaviour — what actually matters — by building the LP accounting event
        the way the runner does:

          * ``pool_address`` == "3pool" (the bare resolved label, no "/"), and
          * ``token0`` / ``token1`` empty — because "3pool" has no "/" to split,
            so no phantom ``token0="USDT"`` / ``token1="USDC"`` reaches
            position_events.

        Also asserts the frozen ``intent.pool`` is UNCHANGED — the fix does not
        mutate the immutable audit-trail intent; canonicalization lives entirely
        in the accounting derivation via the resolved-pool parameter.
        """
        from almanak.framework.accounting.lp_accounting import _get_pool_address, build_lp_accounting_event

        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="USDT/USDC/DAI",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, result.error

        # The compiler surfaces the canonical label here; the intent is untouched.
        resolved_pool = result.action_bundle.metadata["pool_name"]
        assert resolved_pool == "3pool"
        assert intent.pool == "USDT/USDC/DAI", "frozen intent must NOT be mutated by the fix"

        # Without the resolved label, the legacy path would leak token symbols.
        assert _get_pool_address(intent) == "usdt/usdc/dai"  # raw asset-set (no resolved_pool)
        assert _get_pool_address(intent, resolved_pool) == "3pool"  # resolved → canonical

        # The accounting-facing behaviour that actually matters: build the event
        # the way the runner does (threading resolved_pool) and confirm the
        # asset-set produced NO phantom token0/token1 and the canonical pool.
        event = build_lp_accounting_event(
            intent=intent,
            result=result,
            deployment_id="vib3946-unit",
            cycle_id="vib3946-cycle",
            execution_mode="paper",
            chain="ethereum",
            wallet_address=TEST_WALLET,
            resolved_pool=resolved_pool,
        )
        assert event is not None
        assert event.pool_address == "3pool"
        assert event.token0 == "", f"asset-set must not leak token0; got {event.token0!r}"
        assert event.token1 == "", f"asset-set must not leak token1; got {event.token1!r}"

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_asset_set_order_independent(self, mock_config_cls, mock_adapter_cls, compiler):
        """Asset-set matching is order-independent (set equality, not sequence)."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="DAI/USDT/USDC",  # different order than registry's [DAI, USDC, USDT]
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["pool_name"] == "3pool"

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_open_unknown_asset_set_errors_clearly(self, compiler):
        """An asset set with no exact pool match still fails with a clear error.

        The resolver must NOT loosely match a superset/subset pool — only an
        exact coin-set match resolves. ``WETH/WBTC`` matches no stable pool.
        """
        intent = LPOpenIntent(
            pool="WETH/WBTC",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown Curve pool" in result.error
        # Helpful: lists each pool's asset set so the author can self-correct.
        assert "asset sets" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_open_partial_asset_set_does_not_match_superset(self, compiler):
        """``USDC/DAI`` must NOT resolve to 3pool (which also contains USDT).

        Exact set equality prevents silently opening into the wrong pool — a
        2-asset request never matches a 3-asset pool.
        """
        intent = LPOpenIntent(
            pool="USDC/DAI",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown Curve pool" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_asset_set_resolves_pool(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP_CLOSE also accepts an asset-set string for the pool."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(success=True, op="remove_liquidity")
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(
            position_id="100.5",
            pool="ETH/stETH",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["pool_name"] == "steth"

    def test_lp_open_ambiguous_asset_set_raises_not_autopicks(self, compiler):
        """Two registered pools with the identical coin set => FAIL LOUDLY (VIB-3946).

        Curve routinely ships a legacy StableSwap and a StableSwap-NG for the same
        pair on the same chain. If both register, the asset-set request is genuinely
        ambiguous — alphabetical name order has no relationship to liquidity/safety.
        The resolver must refuse to auto-pick and name the colliding pools.
        """
        # Two distinct pools, identical coin set {USDC, USDT}.
        ambiguous_pools = {
            "ethereum": {
                "twocrypto_legacy": {
                    "address": "0x1111111111111111111111111111111111111111",
                    "lp_token": "0x1111111111111111111111111111111111111111",
                    "coins": ["USDC", "USDT"],
                    "pool_type": "stableswap",
                    "n_coins": 2,
                },
                "twocrypto_ng": {
                    "address": "0x2222222222222222222222222222222222222222",
                    "lp_token": "0x2222222222222222222222222222222222222222",
                    "coins": ["USDT", "USDC"],  # same set, different order
                    "pool_type": "stableswap",
                    "n_coins": 2,
                },
            }
        }

        with patch(CURVE_POOLS_PATH, ambiguous_pools), patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES):
            intent = LPOpenIntent(
                pool="USDC/USDT",
                amount0=Decimal("500"),
                amount1=Decimal("500"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
            )

            result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Ambiguous Curve asset set" in result.error
        # Both colliding pool names must be surfaced so the author can disambiguate.
        assert "twocrypto_legacy" in result.error
        assert "twocrypto_ng" in result.error
        # Must NOT have silently auto-picked one.
        assert result.action_bundle is None


# =============================================================================
# LP CLOSE
# =============================================================================


class TestCurveLPClose:
    """Tests for connector-owned Curve LP close compilation."""

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_by_pool_name_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP close by pool name succeeds and parses position_id as LP amount."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(success=True, op="remove_liquidity")
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(
            position_id="100.5",
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        assert result.action_bundle.metadata["lp_amount"] == "100.5"
        mock_adapter.remove_liquidity.assert_called_once_with(
            pool_address=USDC_USDT_POOL,
            lp_amount=Decimal("100.5"),
            slippage_bps=50,
        )

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_invalid_position_id_returns_failed(self, compiler):
        """LP close fails when position_id is not a valid decimal amount."""
        intent = LPCloseIntent(
            position_id="not_a_number",
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Invalid position_id" in result.error
        assert "not_a_number" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_missing_pool_returns_failed(self, compiler):
        """LP close fails when pool is not provided."""
        intent = LPCloseIntent(
            position_id="100.0",
            pool=None,
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "intent.pool must be set" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_adapter_failure_propagates(self, mock_config_cls, mock_adapter_cls, compiler):
        """Adapter remove_liquidity failure is propagated as FAILED."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(success=False, error="slippage exceeded")
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(
            position_id="100.0",
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "slippage exceeded" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_zero_balance_returns_no_op(self, compiler):
        """LP close returns no_op ActionBundle when on-chain LP balance is zero (VIB-3668).

        When the LP token address is passed as position_id (teardown flow) and the
        on-chain balance is 0 (position was on Anvil; mainnet gateway has no balance),
        compilation must succeed as a no_op — not FAILED — so teardown exits 0.
        """
        from almanak.framework.intents.vocabulary import IntentType

        LP_TOKEN = "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC"  # usdc_usdt LP token

        # Patch _query_erc20_balance to simulate zero on-chain balance
        compiler._query_erc20_balance = MagicMock(return_value=0)

        intent = LPCloseIntent(
            position_id=LP_TOKEN,  # Address form triggers on-chain balance query
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == IntentType.LP_CLOSE.value
        assert result.action_bundle.transactions == []
        assert result.action_bundle.metadata.get("no_op") is True

    def test_lp_close_unsupported_chain_returns_failed(self):
        """LP close fails when the chain is not supported by Curve."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="avalanche", wallet_address=TEST_WALLET, config=config)

        intent = LPCloseIntent(position_id="100.0", pool="usdc_usdt", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Curve is not supported on avalanche" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_unknown_pool_returns_failed(self, compiler):
        """LP close fails when the pool is neither a known name nor a known address."""
        intent = LPCloseIntent(
            position_id="100.0",
            pool="0xDEADBEEF00000000000000000000000000000000",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown Curve pool" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_by_pool_address_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP close resolves the pool by address (not name) and succeeds."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(success=True, op="remove_liquidity")
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(position_id="50", pool=USDC_USDT_POOL, protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        mock_adapter.remove_liquidity.assert_called_once_with(
            pool_address=USDC_USDT_POOL,
            lp_amount=Decimal("50"),
            slippage_bps=50,
        )

    @patch(
        CURVE_POOLS_PATH,
        {
            "ethereum": {
                "nolp_pool": {
                    "address": USDC_USDT_POOL,
                    "coins": ["USDC", "USDT"],
                    "pool_type": "stableswap",
                    "n_coins": 2,
                }
            }
        },
    )
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_pool_missing_lp_token_returns_failed(self, compiler):
        """LP close fails closed when the pool config has no lp_token field."""
        intent = LPCloseIntent(position_id="100.0", pool="nolp_pool", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "missing 'lp_token'" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_position_id_lp_token_mismatch_returns_failed(self, compiler):
        """LP close refuses when an address-form position_id != the pool's LP token."""
        wrong_lp_token = "0x" + "1" * 40

        intent = LPCloseIntent(position_id=wrong_lp_token, pool="usdc_usdt", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "does not match" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_balance_query_failure_returns_failed(self, compiler):
        """Address-form position_id with an unqueryable balance fails closed."""
        lp_token = "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC"
        compiler._query_erc20_balance = MagicMock(return_value=None)

        intent = LPCloseIntent(position_id=lp_token, pool="usdc_usdt", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Failed to query LP token balance" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_unresolvable_lp_token_decimals_returns_failed(self, compiler):
        """Address-form position_id whose LP token decimals can't be resolved fails closed."""
        lp_token = "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC"
        compiler._query_erc20_balance = MagicMock(return_value=100 * 10**18)
        compiler._resolve_token = MagicMock(return_value=None)

        intent = LPCloseIntent(position_id=lp_token, pool="usdc_usdt", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Could not resolve decimals" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_by_lp_token_address_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """Address-form position_id with a nonzero on-chain balance computes lp_amount and succeeds."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(success=True, op="remove_liquidity")
        mock_adapter_cls.return_value = mock_adapter

        lp_token = "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC"
        compiler._query_erc20_balance = MagicMock(return_value=100 * 10**18)
        compiler._resolve_token = MagicMock(return_value=MagicMock(decimals=18))

        intent = LPCloseIntent(position_id=lp_token, pool="usdc_usdt", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["lp_amount"] == "100"
        mock_adapter.remove_liquidity.assert_called_once_with(
            pool_address=USDC_USDT_POOL,
            lp_amount=Decimal("100"),
            slippage_bps=50,
        )

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_unexpected_exception_returns_failed(self, mock_config_cls, mock_adapter_cls, compiler):
        """An unexpected adapter exception is caught and surfaced as FAILED."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.side_effect = RuntimeError("boom")
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(position_id="100.0", pool="usdc_usdt", protocol="curve")

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "boom" in result.error
