"""Unit tests for TraderJoe V2 swap intent compilation (VIB-1928).

Tests verify that IntentCompiler correctly compiles SwapIntent for the
traderjoe_v2 protocol using the dedicated _compile_swap_traderjoe_v2() path
instead of the DefaultSwapAdapter.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Patch targets
TJ_ADAPTER_MODULE = "almanak.connectors.traderjoe_v2"
TJ_ADAPTER_CLS = f"{TJ_ADAPTER_MODULE}.TraderJoeV2Adapter"
TJ_CONFIG_CLS = f"{TJ_ADAPTER_MODULE}.TraderJoeV2Config"
TJ_SDK_MODULE = "almanak.connectors.traderjoe_v2.sdk"
TJ_ADDRESSES_MODULE = "almanak.connectors.traderjoe_v2.addresses"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _make_compiler(chain: str = "avalanche") -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestTraderJoeV2SwapRouting:
    """Verify SwapIntent(protocol='traderjoe_v2') routes to dedicated path."""

    def test_traderjoe_v2_swap_no_longer_blocked(self):
        """SwapIntent with protocol='traderjoe_v2' must NOT return VIB-1406 error."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        # VIB-1928: must NOT fail with VIB-1406 guard error
        if result.status == CompilationStatus.FAILED:
            assert "VIB-1406" not in (result.error or ""), (
                "TraderJoe V2 swap still blocked by VIB-1406 guard!"
            )
            assert "not yet supported" not in (result.error or ""), (
                "TraderJoe V2 swap still returns 'not yet supported' error!"
            )
        # If compilation succeeds (with placeholder prices + local RPC), verify bundle
        if result.status == CompilationStatus.SUCCESS:
            assert result.action_bundle is not None
            assert result.action_bundle.metadata["protocol"] == "traderjoe_v2"

    def test_unsupported_chain_fails_with_message(self):
        """SwapIntent on a chain without TJ V2 must fail with helpful error."""
        compiler = _make_compiler(chain="optimism")
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="optimism",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        assert "not supported" in result.error.lower(), (
            f"Expected chain-not-supported error, got: {result.error}"
        )


class TestTraderJoeV2SwapCompilation:
    """Test the full compilation path with mocked adapter."""

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch("almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch("almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.connectors.traderjoe_v2.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_swap_compiles_with_mocked_adapter(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """Full compilation with mocked adapter returns SUCCESS."""
        from almanak.framework.intents.compiler import TransactionData
        from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

        # Setup mocks
        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x1234"
        )

        # Mock approve TX
        approve_tx = TransactionData(
            to="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            value=0,
            data="0xapprove",
            gas_estimate=50000,
            description="Approve USDC",
            tx_type="approve",
        )
        mock_build_approve.return_value = [approve_tx]

        # Mock adapter
        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter

        # VIB-3203 Phase B audit fix: compile path is now fail-closed when
        # get_swap_quote raises, so we MUST return a valid quote here. The
        # baseline-metadata value is covered explicitly by
        # ``TestTraderJoeV2ExpectedOutputHumanPlumbing``.
        from almanak.connectors.traderjoe_v2.adapter import SwapQuote

        mock_adapter.get_swap_quote.return_value = SwapQuote(
            token_in="USDC",
            token_out="WAVAX",
            amount_in=Decimal("100"),
            amount_out=Decimal("4.0"),
            bin_step=20,
            price=Decimal("0.04"),
            price_impact=Decimal("0.1"),
            path=["0x" + "a" * 40, "0x" + "b" * 40],
            gas_estimate=200000,
        )

        # Mock build_swap_transaction to return adapter's TransactionData
        from almanak.connectors.traderjoe_v2.adapter import TransactionData as TJTransactionData

        mock_swap_tx = TJTransactionData(
            to="0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",
            data="0xswapdata",
            value=0,
            gas=200000,
            chain_id=43114,
        )
        mock_adapter.build_swap_transaction.return_value = mock_swap_tx

        # Mock pool auto-detection
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"TraderJoe V2 swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None
        assert len(result.transactions) == 2  # approve + swap
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "traderjoe_v2_swap"

        # Verify adapter was called correctly
        mock_adapter.build_swap_transaction.assert_called_once()
        call_kwargs = mock_adapter.build_swap_transaction.call_args
        assert call_kwargs.kwargs.get("bin_step") == 20 or call_kwargs[1].get("bin_step") == 20

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch("almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_swap_no_pool_found_fails_gracefully(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_get_rpc,
    ):
        """When no pool exists for the pair, compilation fails with helpful error."""
        mock_get_rpc.return_value = "http://localhost:8545"

        # Mock adapter - all pool lookups fail. Autodetect now probes via
        # get_lb_pair_information (VIB-3100); set the side_effect there and on
        # the legacy get_pool_address used by the build path.
        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_lb_pair_information.side_effect = Exception("Pool not found")
        mock_adapter.sdk.get_pool_address.side_effect = Exception("Pool not found")

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "No TraderJoe V2 pool found" in (result.error or "")

    def test_amount_all_rejected(self):
        """amount='all' must be rejected before compilation."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount="all",
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "all" in (result.error or "").lower()


class TestTraderJoeV2SwapMetadata:
    """Test ActionBundle metadata for TJ V2 swaps."""

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch("almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch("almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.connectors.traderjoe_v2.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_metadata_contains_protocol_and_bin_step(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """ActionBundle metadata must include protocol and bin_step."""
        from almanak.connectors.traderjoe_v2.adapter import TransactionData as TJTransactionData
        from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x1234"
        )
        mock_build_approve.return_value = []

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"
        mock_adapter.build_swap_transaction.return_value = TJTransactionData(
            to="0xrouter", data="0xdata", value=0, gas=200000, chain_id=43114,
        )
        # VIB-3203 Phase B audit fix: compile is fail-closed on quote failure,
        # so provide a valid quote here. This test asserts baseline metadata
        # (protocol/bin_step/router) so the specific quote value is irrelevant.
        from almanak.connectors.traderjoe_v2.adapter import SwapQuote

        mock_adapter.get_swap_quote.return_value = SwapQuote(
            token_in="WAVAX",
            token_out="USDC",
            amount_in=Decimal("1.0"),
            amount_out=Decimal("23.5"),
            bin_step=20,
            price=Decimal("23.5"),
            price_impact=Decimal("0.1"),
            path=["0x" + "a" * 40, "0x" + "b" * 40],
            gas_estimate=200000,
        )

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        metadata = result.action_bundle.metadata
        assert metadata["protocol"] == "traderjoe_v2"
        assert metadata["bin_step"] == 20  # auto-detected default
        assert metadata["chain"] == "avalanche"
        assert "router" in metadata


class TestTraderJoeV2ExpectedOutputHumanPlumbing:
    """VIB-3203 Phase B — the TJ V2 compile path now persists
    ``expected_output_human`` (Decimal string) on bundle metadata, sourced
    from a SINGLE ``get_swap_quote`` call that is also reused by
    ``build_swap_transaction`` so ``amount_out_min`` and the slippage
    baseline measure against the same on-chain read.
    """

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch("almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch("almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.connectors.traderjoe_v2.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_quote_persisted_and_reused_in_build_swap_transaction(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """When ``get_swap_quote`` returns a positive quote: metadata carries
        ``expected_output_human`` AND the same SwapQuote instance is forwarded
        to ``build_swap_transaction(quote=...)``."""
        from almanak.connectors.traderjoe_v2.adapter import (
            SwapQuote,
            TransactionData as TJTransactionData,
        )
        from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x1234"
        )
        mock_build_approve.return_value = []

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"

        single_quote = SwapQuote(
            token_in="WAVAX",
            token_out="USDC",
            amount_in=Decimal("1.0"),
            amount_out=Decimal("23.5"),
            bin_step=20,
            price=Decimal("23.5"),
            price_impact=Decimal("0.1"),
            path=["0x" + "a" * 40, "0x" + "b" * 40],
            gas_estimate=200000,
        )
        mock_adapter.get_swap_quote.return_value = single_quote
        mock_adapter.build_swap_transaction.return_value = TJTransactionData(
            to="0xrouter", data="0xdata", value=0, gas=200000, chain_id=43114,
        )

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        metadata = result.action_bundle.metadata
        assert metadata["expected_output_human"] == "23.5"

        # The compiler MUST quote exactly once and reuse — otherwise we lose
        # the no-double-quote guarantee that anchors amount_out_min and the
        # slippage baseline to the same on-chain read.
        mock_adapter.get_swap_quote.assert_called_once()

        # build_swap_transaction MUST receive the same SwapQuote instance —
        # this is the contract that prevents bin-drift between
        # amount_out_min and the slippage baseline.
        mock_adapter.build_swap_transaction.assert_called_once()
        call_kwargs = mock_adapter.build_swap_transaction.call_args.kwargs
        assert call_kwargs.get("quote") is single_quote

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch("almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch("almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.connectors.traderjoe_v2.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_quote_failure_fails_compilation_closed(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """Audit fix: when ``get_swap_quote`` raises a recognised TJ exception,
        the compile path fails CLOSED. Previously the compile returned SUCCESS
        with ``quote=None``, but the adapter's ``build_swap_transaction`` would
        then re-invoke ``get_swap_quote`` internally and hit the same error —
        so the "graceful degradation" was illusory (Codex P2, pr-auditor
        Potential #3). Fail-closed here yields a clearer error with a single
        on-chain read attempt."""
        from almanak.connectors.traderjoe_v2 import TraderJoeV2SDKError
        from almanak.connectors.traderjoe_v2.adapter import TransactionData as TJTransactionData
        from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x1234"
        )
        mock_build_approve.return_value = []

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"
        mock_adapter.get_swap_quote.side_effect = TraderJoeV2SDKError("RPC unavailable")
        mock_adapter.build_swap_transaction.return_value = TJTransactionData(
            to="0xrouter", data="0xdata", value=0, gas=200000, chain_id=43114,
        )

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        assert "TraderJoe V2 quote failed" in result.error
        assert "RPC unavailable" in result.error

        # Compiler MUST attempt the quote exactly once and MUST NOT fall
        # through to build_swap_transaction on failure.
        mock_adapter.get_swap_quote.assert_called_once()
        mock_adapter.build_swap_transaction.assert_not_called()

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch("almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch("almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.connectors.traderjoe_v2.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_zero_quote_amount_out_fails_closed(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """Audit fix (pr-auditor Potential #6): a quote with ``amount_out == 0``
        (e.g. drained or malformed pool) would produce ``amount_out_min = 0`` —
        a swap with no slippage floor. Refuse the compile instead."""
        from almanak.connectors.traderjoe_v2.adapter import SwapQuote, TransactionData as TJTransactionData
        from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x1234"
        )
        mock_build_approve.return_value = []

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"
        mock_adapter.get_swap_quote.return_value = SwapQuote(
            token_in="WAVAX",
            token_out="USDC",
            amount_in=Decimal("1.0"),
            amount_out=Decimal("0"),  # drained pool
            bin_step=20,
            price=Decimal("0"),
            price_impact=Decimal("0"),
            path=["0x" + "a" * 40, "0x" + "b" * 40],
            gas_estimate=200000,
        )
        mock_adapter.build_swap_transaction.return_value = TJTransactionData(
            to="0xrouter", data="0xdata", value=0, gas=200000, chain_id=43114,
        )

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        assert "zero amount_out" in result.error
        mock_adapter.build_swap_transaction.assert_not_called()


class TestTraderJoeV2BinStepAutodetect:
    """VIB-3100: autodetect must honour ignoredForRouting + pick the deepest pool.

    On Arbitrum WETH/USDC the first *existing* pool (bin_step=25) is an empty
    husk, bin_step=15 is deep but ``ignoredForRouting`` (deprecated), and the
    routable liquid pool sits elsewhere. A first-liquid probe builds a swap
    that reverts with ``LBPair__OutOfLiquidity``. These tests pin the new
    selection contract directly against the connector compiler's private
    autodetect helper.
    """

    @staticmethod
    def _compiler(chain: str = "arbitrum"):
        from almanak.connectors.traderjoe_v2.compiler import _TraderJoeV2CompileImpl

        compiler = _TraderJoeV2CompileImpl.__new__(_TraderJoeV2CompileImpl)
        compiler.chain = chain
        return compiler

    # Canonical pool token ordering for the WETH/USDC test pair: token_x = WETH
    # (18 decimals), token_y = USDC (6 decimals).
    _WETH_ADDR = "0x" + "a" * 40
    _USDC_ADDR = "0x" + "b" * 40

    @staticmethod
    def _token(symbol: str, address: str, decimals: int = 18):
        from almanak.framework.intents.compiler_models import TokenInfo

        return TokenInfo(symbol=symbol, address=address, decimals=decimals)

    @staticmethod
    def _intent():
        return SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="arbitrum",
        )

    def _run(self, *, pools, x_decimals: int = 18, y_decimals: int = 18):
        """pools: {bin_step: (pair_address, ignored, reserve_x, reserve_y)} or None.

        reserve_x/reserve_y are RAW on-chain units (pre-decimal). token_x maps
        to WETH (x_decimals), token_y to USDC (y_decimals), so callers can model
        the WETH(1e18)/USDC(1e6) scale mismatch the audit flagged.
        """
        from almanak.connectors.traderjoe_v2.sdk import LBPairInformation, PoolNotFoundError

        info_by_addr = {}
        for bs, spec in pools.items():
            if spec is None:
                continue
            addr, ignored, rx, ry = spec
            info_by_addr[addr] = SimpleNamespace(
                reserve_x=rx,
                reserve_y=ry,
                token_x=self._WETH_ADDR,
                token_y=self._USDC_ADDR,
            )

        def fake_lb_pair_info(token_a, token_b, bin_step):
            spec = pools.get(bin_step)
            if spec is None:
                raise PoolNotFoundError(token_a, token_b, bin_step)
            addr, ignored, _, _ = spec
            return LBPairInformation(pair_address=addr, bin_step=bin_step, ignored_for_routing=ignored)

        def fake_pool_info(pool_address):
            return info_by_addr[pool_address]

        sdk = MagicMock()
        sdk.get_lb_pair_information.side_effect = fake_lb_pair_info
        sdk.get_pool_info.side_effect = fake_pool_info
        adapter = MagicMock()
        adapter.sdk = sdk

        compiler = self._compiler()
        return compiler._autodetect_traderjoe_v2_bin_step(
            intent=self._intent(),
            tj_adapter=adapter,
            swap_from_token=self._token("WETH", self._WETH_ADDR, x_decimals),
            swap_to_token=self._token("USDC", self._USDC_ADDR, y_decimals),
            from_token_symbol="WETH",
            to_token_symbol="USDC",
            pool_not_found_exc=PoolNotFoundError,
        )

    def test_skips_empty_husk_and_picks_deepest(self):
        """First existing pool (25) is empty; 50 is liquid → pick the deepest liquid."""
        result = self._run(
            pools={
                20: None,
                25: ("0x" + "2" * 40, False, 0, 0),          # empty husk
                15: None,
                10: None,
                50: ("0x" + "5" * 40, False, 100, 200),       # liquid, total 300
                5: ("0x" + "6" * 40, False, 10, 10),          # liquid, total 20
            }
        )
        assert result == 50

    def test_skips_ignored_for_routing_even_when_deepest(self):
        """The deepest pool (15) is ignoredForRouting → must NOT be picked."""
        result = self._run(
            pools={
                20: None,
                25: ("0x" + "2" * 40, False, 50, 50),          # liquid, total 100
                15: ("0x" + "f" * 40, True, 5000, 5000),       # DEEPEST but ignored
                50: ("0x" + "5" * 40, False, 10, 10),          # liquid, total 20
            }
        )
        assert result == 25  # deepest *non-ignored*

    def test_only_ignored_or_empty_returns_no_pool_found(self):
        """If every candidate is ignored or empty → FAILED, no pool found."""
        result = self._run(
            pools={
                25: ("0x" + "2" * 40, True, 5000, 5000),       # ignored
                50: ("0x" + "5" * 40, False, 0, 0),            # empty
            }
        )
        assert result.status == CompilationStatus.FAILED
        assert "No TraderJoe V2 pool found" in (result.error or "")

    def test_tie_breaks_by_popularity_order(self):
        """Equal reserves → fall back to popularity order (20 before 25)."""
        result = self._run(
            pools={
                20: ("0x" + "1" * 40, False, 100, 100),
                25: ("0x" + "2" * 40, False, 100, 100),
            }
        )
        assert result == 20

    def test_decimal_normalized_depth_balanced_outranks_one_sided(self):
        """VIB-3100 audit item 1: a balanced 5-WETH/5M-USDC pool must outrank a
        one-sided 10-WETH/0-USDC pool, despite the one-sided pool's larger RAW
        reserve_x. Summing raw units (WETH 1e18 vs USDC 1e6) would pick the
        shallower one-sided pool — the exact mis-rank this fix prevents.

        token_x = WETH (18 decimals), token_y = USDC (6 decimals).
          - bin 20 one-sided: 10 WETH raw = 10e18, 0 USDC.
              raw sum   = 10e18           (wins on raw — WRONG)
              norm sum  = 10 + 0   = 10
          - bin 25 balanced: 5 WETH raw = 5e18, 5,000,000 USDC raw = 5e12.
              raw sum   = 5e18 + 5e12 ≈ 5e18  (loses on raw)
              norm sum  = 5 + 5,000,000 = 5,000,005   (wins on normalized — RIGHT)
        """
        result = self._run(
            pools={
                20: ("0x" + "1" * 40, False, 10 * 10**18, 0),          # one-sided
                25: ("0x" + "2" * 40, False, 5 * 10**18, 5_000_000 * 10**6),  # balanced, deep
            },
            x_decimals=18,
            y_decimals=6,
        )
        assert result == 25

    def test_unmeasurable_reserves_still_selectable(self):
        """When reserves can't be probed (RPC flake), keep the candidate (fail-open)."""
        from almanak.connectors.traderjoe_v2.sdk import LBPairInformation, PoolNotFoundError

        def fake_lb_pair_info(token_a, token_b, bin_step):
            if bin_step == 20:
                return LBPairInformation(pair_address="0x" + "1" * 40, bin_step=20, ignored_for_routing=False)
            raise PoolNotFoundError(token_a, token_b, bin_step)

        sdk = MagicMock()
        sdk.get_lb_pair_information.side_effect = fake_lb_pair_info
        sdk.get_pool_info.side_effect = RuntimeError("RPC down")
        adapter = MagicMock()
        adapter.sdk = sdk

        compiler = self._compiler()
        result = compiler._autodetect_traderjoe_v2_bin_step(
            intent=self._intent(),
            tj_adapter=adapter,
            swap_from_token=self._token("WETH", "0x" + "a" * 40),
            swap_to_token=self._token("USDC", "0x" + "b" * 40),
            from_token_symbol="WETH",
            to_token_symbol="USDC",
            pool_not_found_exc=PoolNotFoundError,
        )
        assert result == 20

    def test_unexpected_probe_error_fails_with_bin_step(self):
        """A non-not-found probe error surfaces the failing bin step."""
        from almanak.connectors.traderjoe_v2.sdk import PoolNotFoundError

        sdk = MagicMock()
        sdk.get_lb_pair_information.side_effect = RuntimeError("boom")
        adapter = MagicMock()
        adapter.sdk = sdk

        compiler = self._compiler()
        result = compiler._autodetect_traderjoe_v2_bin_step(
            intent=self._intent(),
            tj_adapter=adapter,
            swap_from_token=self._token("WETH", "0x" + "a" * 40),
            swap_to_token=self._token("USDC", "0x" + "b" * 40),
            from_token_symbol="WETH",
            to_token_symbol="USDC",
            pool_not_found_exc=PoolNotFoundError,
        )
        assert result.status == CompilationStatus.FAILED
        assert "Failed to probe TraderJoe V2 pool for bin_step=20" in (result.error or "")

    def test_transient_rpc_error_propagates_not_silently_skipped(self):
        """VIB-3100 Gemini HIGH: a transient RPC error (TraderJoeV2SDKError, NOT
        PoolNotFoundError) on a candidate must FAIL LOUD — never be swallowed as
        a skip that lets autodetect fall through to a shallower wrong pool.

        bin 20 errors with a transport error; bin 25 is a deep liquid pool. If
        the loop wrongly skipped bin 20 it would happily return 25; instead it
        must return FAILED naming bin 20.
        """
        from almanak.connectors.traderjoe_v2.sdk import (
            LBPairInformation,
            PoolNotFoundError,
            TraderJoeV2SDKError,
        )

        def fake_lb_pair_info(token_a, token_b, bin_step):
            if bin_step == 20:
                # Transport error masquerading as nothing — must propagate.
                raise TraderJoeV2SDKError("getLBPairInformation RPC call failed")
            return LBPairInformation(pair_address=self._USDC_ADDR, bin_step=bin_step, ignored_for_routing=False)

        sdk = MagicMock()
        sdk.get_lb_pair_information.side_effect = fake_lb_pair_info
        sdk.get_pool_info.return_value = SimpleNamespace(
            reserve_x=5 * 10**18, reserve_y=5 * 10**18, token_x=self._WETH_ADDR, token_y=self._USDC_ADDR
        )
        adapter = MagicMock()
        adapter.sdk = sdk

        compiler = self._compiler()
        result = compiler._autodetect_traderjoe_v2_bin_step(
            intent=self._intent(),
            tj_adapter=adapter,
            swap_from_token=self._token("WETH", self._WETH_ADDR),
            swap_to_token=self._token("USDC", self._USDC_ADDR),
            from_token_symbol="WETH",
            to_token_symbol="USDC",
            pool_not_found_exc=PoolNotFoundError,
        )
        # Did NOT return an int bin step (25) — failed loud on bin 20 instead.
        assert not isinstance(result, int)
        assert result.status == CompilationStatus.FAILED
        assert "Failed to probe TraderJoe V2 pool for bin_step=20" in (result.error or "")

    def test_genuine_absence_skips_to_next_candidate(self):
        """Companion to the RPC-error test: a GENUINE PoolNotFoundError (factory
        zero address) on early candidates is correctly skipped, and a later
        existing pool is selected."""
        from almanak.connectors.traderjoe_v2.sdk import LBPairInformation, PoolNotFoundError

        def fake_lb_pair_info(token_a, token_b, bin_step):
            if bin_step in (20, 25, 15):
                raise PoolNotFoundError(token_a, token_b, bin_step)  # absent → skip
            return LBPairInformation(pair_address=self._USDC_ADDR, bin_step=bin_step, ignored_for_routing=False)

        sdk = MagicMock()
        sdk.get_lb_pair_information.side_effect = fake_lb_pair_info
        sdk.get_pool_info.return_value = SimpleNamespace(
            reserve_x=3 * 10**18, reserve_y=3 * 10**18, token_x=self._WETH_ADDR, token_y=self._USDC_ADDR
        )
        adapter = MagicMock()
        adapter.sdk = sdk

        compiler = self._compiler()
        result = compiler._autodetect_traderjoe_v2_bin_step(
            intent=self._intent(),
            tj_adapter=adapter,
            swap_from_token=self._token("WETH", self._WETH_ADDR),
            swap_to_token=self._token("USDC", self._USDC_ADDR),
            from_token_symbol="WETH",
            to_token_symbol="USDC",
            pool_not_found_exc=PoolNotFoundError,
        )
        # 10 is the next candidate after 20/25/15 in popularity order.
        assert result == 10
