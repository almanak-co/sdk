"""Tests for Solana routing in the IntentCompiler.

VIB-4803: dispatch was migrated from a ``self._is_solana_chain()`` switch to
polymorphic dispatch via :class:`SvmFamily.compile_intent` in
``almanak.framework.chain_family``. These tests verify the new dispatch path
through the family adapter, plus the anti-bypass invariant that registering
a new chain under SVM automatically routes to SVM compilation.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.chain_family import EvmFamily, SvmFamily, family_for
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import SwapIntent

# Test config that allows placeholder prices (no real price oracle needed)
TEST_CONFIG = IntentCompilerConfig(allow_placeholder_prices=True)


# ---------------------------------------------------------------------------
# Family-adapter selection (replaces former TestIsSolanaChain)
# ---------------------------------------------------------------------------


class TestCompilerFamilyAdapter:
    """The compiler resolves the right ChainFamily adapter at construction."""

    def test_solana_chain_resolves_to_svm_family(self):
        compiler = IntentCompiler(
            chain="solana", wallet_address="TestWallet123", config=TEST_CONFIG
        )
        assert isinstance(compiler._family, SvmFamily)

    def test_solana_chain_uppercase_resolves_to_svm_family(self):
        compiler = IntentCompiler(
            chain="SOLANA", wallet_address="TestWallet123", config=TEST_CONFIG
        )
        assert isinstance(compiler._family, SvmFamily)

    def test_evm_chain_arbitrum_resolves_to_evm_family(self):
        compiler = IntentCompiler(chain="arbitrum", config=TEST_CONFIG)
        assert isinstance(compiler._family, EvmFamily)

    def test_evm_chain_ethereum_resolves_to_evm_family(self):
        compiler = IntentCompiler(chain="ethereum", config=TEST_CONFIG)
        assert isinstance(compiler._family, EvmFamily)

    def test_unknown_chain_defaults_to_evm_family(self):
        # Matches the legacy "treat unknown as not-Solana" contract.
        compiler = IntentCompiler(chain="unknown_chain", config=TEST_CONFIG)
        assert isinstance(compiler._family, EvmFamily)


# ---------------------------------------------------------------------------
# SWAP dispatch tests (now via SvmFamily.compile_intent)
# ---------------------------------------------------------------------------


class TestCompileSwapSolanaRouting:
    @patch(
        "almanak.framework.intents.compiler_solana.compile_jupiter_swap",
        autospec=True,
    )
    def test_solana_routes_to_jupiter(self, mock_jupiter):
        """Solana chains route SWAP to compile_jupiter_swap via SvmFamily."""
        mock_jupiter.return_value = MagicMock(
            status=CompilationStatus.SUCCESS,
            action_bundle=MagicMock(),
        )

        compiler = IntentCompiler(
            chain="solana", wallet_address="TestWallet123", config=TEST_CONFIG
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        compiler._compile_swap(intent)
        mock_jupiter.assert_called_once_with(compiler, intent)

    @patch(
        "almanak.framework.intents.compiler_solana.compile_jupiter_swap",
        autospec=True,
    )
    def test_evm_does_not_route_to_jupiter(self, mock_jupiter):
        """EVM chains never route to Jupiter."""
        compiler = IntentCompiler(chain="arbitrum", config=TEST_CONFIG)
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        # This will fail at token resolution (no RPC), but Jupiter must not
        # be invoked on EVM chains.
        compiler._compile_swap(intent)
        mock_jupiter.assert_not_called()


# ---------------------------------------------------------------------------
# compile_jupiter_swap() integration tests (module-level helper)
# ---------------------------------------------------------------------------


class TestCompileJupiterSwap:
    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_successful_compilation(self, mock_client_cls):
        """Full Jupiter swap compilation path via compiler_solana helper."""
        from almanak.framework.connectors.jupiter.models import (
            JupiterQuote,
            JupiterSwapTransaction,
        )
        from almanak.framework.intents.compiler_solana import compile_jupiter_swap

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

        result = compile_jupiter_swap(compiler, intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "jupiter"
        assert result.action_bundle.metadata["chain_family"] == "SOLANA"
        assert len(result.action_bundle.transactions) == 1
        assert result.action_bundle.transactions[0]["serialized_transaction"] == "base64_tx_data"

    def test_compilation_failure_returns_failed_status(self):
        """Compilation errors produce FAILED status, not exceptions."""
        from almanak.framework.intents.compiler_solana import compile_jupiter_swap

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

        # Should not raise - returns a FAILED CompilationResult.
        result = compile_jupiter_swap(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert result.error is not None


# ---------------------------------------------------------------------------
# VIB-4803 anti-bypass: family is a real seam, not a label
# ---------------------------------------------------------------------------


class TestChainFamilyAntiBypass:
    """Anti-bypass: registering a hypothetical new chain under SvmFamily must
    automatically dispatch to SVM compilation, without any further code edits.

    The point of VIB-4803 is to make ``ChainFamily`` a real behavior seam.
    These tests prove that property — if someone re-introduces an
    ``if chain == "solana"`` check in a hot dispatch path, the polymorphic
    fallback for a *new* SVM-family chain would break.
    """

    def test_hypothetical_svm_chain_routes_through_svm_family(self):
        """A chain whose descriptor family is ChainFamily.SOLANA resolves to
        :class:`SvmFamily` via :func:`family_for`, even if the chain name is
        not the literal string "solana".

        We don't actually register a new ``Chain`` enum member (that would
        require touching the byte-identity snapshots in test_chain_registry).
        Instead, we monkey-patch the registry's ``try_resolve`` to claim a
        hypothetical chain belongs to the SOLANA family, and verify
        :func:`family_for` returns :class:`SvmFamily`. The point is exactly
        that ``family_for`` does NOT compare to the string ``"solana"`` —
        it consults the descriptor's family field, which is the seam.
        """
        from unittest.mock import patch as _patch

        from almanak.core.chains import ChainRegistry
        from almanak.core.chains._descriptor import (
            ChainDescriptor,
            GasProfile,
            NativeToken,
        )
        from almanak.core.enums import Chain, ChainFamily

        # A synthetic descriptor for a hypothetical second SVM chain.
        fake_descriptor = ChainDescriptor(
            enum=Chain.SOLANA,  # any valid Chain enum; we don't read it here
            name="solana",  # required by __post_init__; ignored by family_for
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
        assert isinstance(adapter, SvmFamily), (
            "family_for must dispatch via the descriptor's `family` field, "
            "not via a string compare against 'solana'. If this fails, "
            "someone re-introduced an `if chain == \"solana\"` check on the "
            "dispatch path."
        )

    def test_hypothetical_svm_chain_routes_swap_through_jupiter(self):
        """End-to-end: SVM compilation reaches Jupiter for a hypothetical SVM
        chain, without any code edits outside the family adapter.
        """
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

        original_resolve_chain_name = None
        try:
            from almanak.core.constants import (
                resolve_chain_name as _resolve_chain_name,
            )

            original_resolve_chain_name = _resolve_chain_name
        except ImportError:
            pass

        def fake_resolve_chain_name(name: str) -> str:
            if (name or "").strip().lower() == "hypothetical_svm":
                return "hypothetical_svm"
            if original_resolve_chain_name is None:
                raise ValueError(name)
            return original_resolve_chain_name(name)

        with (
            _patch.object(ChainRegistry, "try_resolve", side_effect=fake_try_resolve),
            _patch(
                "almanak.core.constants.resolve_chain_name",
                side_effect=fake_resolve_chain_name,
            ),
            _patch(
                "almanak.framework.intents.compiler_solana.compile_jupiter_swap",
                autospec=True,
            ) as mock_jupiter,
        ):
            mock_jupiter.return_value = MagicMock(
                status=CompilationStatus.SUCCESS, action_bundle=MagicMock()
            )

            compiler = IntentCompiler(
                chain="hypothetical_svm",
                wallet_address="TestWallet123",
                config=TEST_CONFIG,
            )
            # The hypothetical chain still resolves to the SvmFamily adapter,
            # because dispatch keys off ``descriptor.family`` — not a string
            # compare against "solana".
            assert isinstance(compiler._family, SvmFamily)

            intent = SwapIntent(
                from_token="USDC",
                to_token="SOL",
                amount=Decimal("100"),
                max_slippage=Decimal("0.005"),
            )
            compiler._compile_swap(intent)
            mock_jupiter.assert_called_once_with(compiler, intent)


# ---------------------------------------------------------------------------
# VIB-4804: SvmFamily.signer_factory wired (mirrors EvmFamily)
# ---------------------------------------------------------------------------


class TestSvmFamilySignerFactory:
    """The SVM signer namespace is now exposed via SvmFamily.signer_factory,
    mirroring the shape EvmFamily uses. Pinned in :issue:`VIB-4804`.
    """

    def test_evm_signer_factory_returns_signer_namespace(self):
        adapter = EvmFamily()
        signer_ns = adapter.signer_factory(descriptor=None)
        # The EVM signer factory hands back the framework signer module
        # (LocalKeySigner, DirectSafeSigner, ZodiacSigner, etc.).
        assert hasattr(signer_ns, "LocalKeySigner")
        assert hasattr(signer_ns, "create_safe_signer")

    def test_svm_signer_factory_returns_signer_namespace(self):
        adapter = SvmFamily()
        signer_ns = adapter.signer_factory(descriptor=None)
        # The SVM signer factory hands back the
        # almanak.framework.execution.solana module
        # (SolanaSigner, SolanaSignerError, SolanaExecutionPlanner, ...).
        assert hasattr(signer_ns, "SolanaSigner")
        assert hasattr(signer_ns, "SolanaSignerError")
        assert hasattr(signer_ns, "SolanaExecutionPlanner")

    def test_signer_factory_shape_parity(self):
        """Both families return the same KIND of value (a module namespace
        with a Signer class). This is the contract every future family
        adapter must honour, and it lets the gateway-side caller do a
        single isinstance-free lookup against ``family.signer_factory``.
        """
        evm_ns = EvmFamily().signer_factory(descriptor=None)
        svm_ns = SvmFamily().signer_factory(descriptor=None)
        # Both are modules.
        import types as _types

        assert isinstance(evm_ns, _types.ModuleType)
        assert isinstance(svm_ns, _types.ModuleType)
        # Both expose a concrete signer class.
        from almanak.framework.execution.signer import LocalKeySigner as _Local
        from almanak.framework.execution.solana import SolanaSigner as _Solana

        assert getattr(evm_ns, "LocalKeySigner") is _Local
        assert getattr(svm_ns, "SolanaSigner") is _Solana
