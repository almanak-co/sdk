"""Tests for Solana routing through ChainFamily and connector compilers.

VIB-4803 moved Solana dispatch behind :class:`SvmFamily.compile_intent`.
VIB-4300 moves the Jupiter swap compile body out of ``compiler_solana.py`` and
into ``connectors.jupiter.compiler``. These tests pin both contracts: family
dispatch remains the seam, and Jupiter is reached through the connector
compiler registry.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.chain_family import EvmFamily, SvmFamily, family_for
from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.compiler_registry import get_compiler
from almanak.framework.connectors.jupiter.compiler import JupiterCompiler
from almanak.framework.intents.compiler import (
    CompilationResult,
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import SwapIntent

TEST_CONFIG = IntentCompilerConfig(allow_placeholder_prices=True)


class TestCompilerFamilyAdapter:
    """The compiler resolves the right ChainFamily adapter at construction."""

    def test_solana_chain_resolves_to_svm_family(self):
        compiler = IntentCompiler(chain="solana", wallet_address="TestWallet123", config=TEST_CONFIG)
        assert isinstance(compiler._family, SvmFamily)

    def test_solana_chain_uppercase_resolves_to_svm_family(self):
        compiler = IntentCompiler(chain="SOLANA", wallet_address="TestWallet123", config=TEST_CONFIG)
        assert isinstance(compiler._family, SvmFamily)

    def test_evm_chain_arbitrum_resolves_to_evm_family(self):
        compiler = IntentCompiler(chain="arbitrum", config=TEST_CONFIG)
        assert isinstance(compiler._family, EvmFamily)

    def test_evm_chain_ethereum_resolves_to_evm_family(self):
        compiler = IntentCompiler(chain="ethereum", config=TEST_CONFIG)
        assert isinstance(compiler._family, EvmFamily)

    def test_unknown_chain_defaults_to_evm_family(self):
        compiler = IntentCompiler(chain="unknown_chain", config=TEST_CONFIG)
        assert isinstance(compiler._family, EvmFamily)


class TestCompileSwapSolanaRouting:
    def test_jupiter_is_registered_as_connector_compiler(self):
        assert isinstance(get_compiler("jupiter"), JupiterCompiler)

    def test_solana_routes_to_jupiter_connector(self):
        sentinel = CompilationResult(status=CompilationStatus.SUCCESS, intent_id="sentinel")
        mock_compiler = MagicMock()
        mock_compiler.context_type = BaseCompilerContext
        mock_compiler.chains = frozenset({"solana"})
        mock_compiler.compile.return_value = sentinel

        with patch("almanak.framework.chain_family._svm_dispatch.get_connector_compiler", return_value=mock_compiler):
            compiler = IntentCompiler(chain="solana", wallet_address="TestWallet123", config=TEST_CONFIG)
            intent = SwapIntent(
                from_token="USDC",
                to_token="SOL",
                amount=Decimal("100"),
                max_slippage=Decimal("0.005"),
            )
            result = compiler._compile_swap(intent)

        assert result is sentinel
        mock_compiler.compile.assert_called_once()

    def test_evm_does_not_route_to_jupiter(self):
        compiler = IntentCompiler(chain="arbitrum", config=TEST_CONFIG)
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        with patch("almanak.framework.chain_family._svm_dispatch.get_connector_compiler") as lookup:
            compiler._compile_swap(intent)

        lookup.assert_not_called()


class TestJupiterConnectorCompiler:
    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_successful_compilation(self, mock_client_cls):
        from almanak.framework.connectors.jupiter.models import (
            JupiterQuote,
            JupiterSwapTransaction,
        )

        mock_client = MagicMock()
        mock_quote = JupiterQuote(
            input_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            output_mint="So11111111111111111111111111111111111111112",
            in_amount="100000000",
            out_amount="666666",
            price_impact_pct="0.05",
            raw_response={"inputMint": "A", "outputMint": "B", "routePlan": []},
        )
        mock_swap_tx = JupiterSwapTransaction(
            swap_transaction="base64_tx_data",
            last_valid_block_height=280000000,
            priority_fee_lamports=5000,
            quote=mock_quote,
        )
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        mock_resolver = MagicMock()
        mock_resolved_usdc = MagicMock(
            address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            decimals=6,
            symbol="USDC",
        )
        mock_resolved_sol = MagicMock(
            address="So11111111111111111111111111111111111111112",
            decimals=9,
            symbol="SOL",
        )
        mock_resolver.resolve_for_swap.side_effect = lambda t, c: {
            "USDC": mock_resolved_usdc,
            "SOL": mock_resolved_sol,
        }[t]
        mock_resolver.resolve.side_effect = lambda t, c: {
            "USDC": mock_resolved_usdc,
            "SOL": mock_resolved_sol,
        }[t]

        compiler = IntentCompiler(
            chain="solana",
            wallet_address="TestWallet123",
            price_oracle={"USDC": Decimal("1"), "SOL": Decimal("150")},
            token_resolver=mock_resolver,
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "jupiter"
        assert result.action_bundle.metadata["chain_family"] == "SOLANA"
        assert len(result.action_bundle.transactions) == 1
        assert result.action_bundle.transactions[0]["serialized_transaction"] == "base64_tx_data"

    def test_compilation_failure_returns_failed_status(self):
        compiler = IntentCompiler(
            chain="solana",
            wallet_address="TestWallet123",
            config=TEST_CONFIG,
        )
        intent = SwapIntent(
            from_token="UNKNOWN_TOKEN",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        # Tighten: assert the failure is the unknown-token path, not any failure.
        assert "UNKNOWN_TOKEN" in result.error


class TestChainFamilyAntiBypass:
    """A hypothetical new SVM chain must route through the family seam."""

    def test_hypothetical_svm_chain_routes_through_svm_family(self):
        from unittest.mock import patch as _patch

        from almanak.core.chains import ChainRegistry
        from almanak.core.chains._descriptor import (
            ChainDescriptor,
            GasProfile,
            NativeToken,
        )
        from almanak.core.enums import Chain, ChainFamily

        fake_descriptor = ChainDescriptor(
            enum=Chain.SOLANA,
            name="solana",
            chain_id=0,
            family=ChainFamily.SOLANA,
            native=NativeToken(symbol="SVM2", name="HypotheticalSvm", decimals=9),
            gas=GasProfile(),
        )

        original = ChainRegistry.try_resolve

        def fake_try_resolve(name_or_alias: str):
            if (name_or_alias or "").strip().lower() == "hypothetical_svm":
                return fake_descriptor
            return original(name_or_alias)

        with _patch.object(ChainRegistry, "try_resolve", side_effect=fake_try_resolve):
            adapter = family_for("hypothetical_svm")
        assert isinstance(adapter, SvmFamily)

    def test_hypothetical_svm_chain_routes_swap_through_jupiter_connector(self):
        from unittest.mock import patch as _patch

        from almanak.core.chains import ChainRegistry
        from almanak.core.chains._descriptor import (
            ChainDescriptor,
            GasProfile,
            NativeToken,
        )
        from almanak.core.enums import Chain, ChainFamily

        fake_descriptor = ChainDescriptor(
            enum=Chain.SOLANA,
            name="solana",
            chain_id=0,
            family=ChainFamily.SOLANA,
            native=NativeToken(symbol="SVM2", name="HypotheticalSvm", decimals=9),
            gas=GasProfile(),
        )

        original = ChainRegistry.try_resolve

        def fake_try_resolve(name_or_alias: str):
            if (name_or_alias or "").strip().lower() == "hypothetical_svm":
                return fake_descriptor
            return original(name_or_alias)

        def fake_resolve_chain_name(name: str) -> str:
            if (name or "").strip().lower() == "hypothetical_svm":
                return "hypothetical_svm"
            return name

        sentinel = CompilationResult(status=CompilationStatus.SUCCESS, intent_id="sentinel")
        mock_compiler = MagicMock()
        mock_compiler.context_type = BaseCompilerContext
        mock_compiler.chains = frozenset({"solana"})
        mock_compiler.compile.return_value = sentinel

        with (
            _patch.object(ChainRegistry, "try_resolve", side_effect=fake_try_resolve),
            _patch("almanak.core.constants.resolve_chain_name", side_effect=fake_resolve_chain_name),
            _patch("almanak.framework.chain_family._svm_dispatch.get_connector_compiler", return_value=mock_compiler),
        ):
            compiler = IntentCompiler(
                chain="hypothetical_svm",
                wallet_address="TestWallet123",
                config=TEST_CONFIG,
            )
            assert isinstance(compiler._family, SvmFamily)

            intent = SwapIntent(
                from_token="USDC",
                to_token="SOL",
                amount=Decimal("100"),
                max_slippage=Decimal("0.005"),
            )
            result = compiler._compile_swap(intent)

        assert result is sentinel
        mock_compiler.compile.assert_called_once()


class TestSvmFamilySignerFactory:
    """The SVM signer namespace mirrors the EVM signer factory shape."""

    def test_evm_signer_factory_returns_signer_namespace(self):
        adapter = EvmFamily()
        signer_ns = adapter.signer_factory(descriptor=None)
        assert hasattr(signer_ns, "LocalKeySigner")
        assert hasattr(signer_ns, "create_safe_signer")

    def test_svm_signer_factory_returns_signer_namespace(self):
        adapter = SvmFamily()
        signer_ns = adapter.signer_factory(descriptor=None)
        assert hasattr(signer_ns, "SolanaSigner")
        assert hasattr(signer_ns, "SolanaSignerError")
        assert hasattr(signer_ns, "SolanaExecutionPlanner")

    def test_signer_factory_shape_parity(self):
        import types as _types

        evm_ns = EvmFamily().signer_factory(descriptor=None)
        svm_ns = SvmFamily().signer_factory(descriptor=None)
        assert isinstance(evm_ns, _types.ModuleType)
        assert isinstance(svm_ns, _types.ModuleType)

        from almanak.framework.execution.signer import LocalKeySigner as _Local
        from almanak.framework.execution.solana import SolanaSigner as _Solana

        assert getattr(evm_ns, "LocalKeySigner") is _Local
        assert getattr(svm_ns, "SolanaSigner") is _Solana
