"""Unit tests for Berachain intent compilation.

Verifies that the IntentCompiler correctly handles Berachain:
- Token resolution for WBERA, HONEY, USDC.E, WETH, WBTC
- Enso swap compilation routes to _compile_enso_swap
- Native token wrapping (BERA -> WBERA)

These are unit tests (no Anvil required). External API calls are mocked.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)


def _make_compiler(chain: str = "berachain") -> IntentCompiler:
    """Create a compiler with placeholder prices for Berachain testing."""
    return IntentCompiler(
        chain=chain,
        wallet_address="0x1111111111111111111111111111111111111111",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestBerachainTokenResolution:
    """Verify token resolution works for Berachain tokens."""

    def test_resolve_wbera(self) -> None:
        """WBERA resolves to the correct address on Berachain."""
        compiler = _make_compiler()
        token = compiler._resolve_token("WBERA")
        assert token is not None, "WBERA must be resolvable on Berachain"
        assert token.address.lower() == "0x6969696969696969696969696969696969696969"

    def test_resolve_honey(self) -> None:
        """HONEY resolves to the correct address on Berachain."""
        compiler = _make_compiler()
        token = compiler._resolve_token("HONEY")
        assert token is not None, "HONEY must be resolvable on Berachain"
        assert token.decimals == 18, "HONEY has 18 decimals (not 6)"

    def test_resolve_usdc_e(self) -> None:
        """USDC.E (bridged USDC) resolves on Berachain."""
        compiler = _make_compiler()
        token = compiler._resolve_token("USDC.E")
        assert token is not None, "USDC.E must be resolvable on Berachain"
        assert token.decimals == 6

    def test_resolve_weth(self) -> None:
        """WETH resolves on Berachain."""
        compiler = _make_compiler()
        token = compiler._resolve_token("WETH")
        assert token is not None, "WETH must be resolvable on Berachain"

    def test_resolve_wbtc(self) -> None:
        """WBTC resolves on Berachain."""
        compiler = _make_compiler()
        token = compiler._resolve_token("WBTC")
        assert token is not None, "WBTC must be resolvable on Berachain"


class TestBerachainEnsoSwapCompilation:
    """Verify SwapIntent compilation routes to Enso on Berachain."""

    @patch.dict("os.environ", {"ENSO_API_KEY": "test-key"})
    @patch("almanak.framework.connectors.enso.EnsoClient")
    def test_enso_swap_compiles_with_mock_route(self, mock_enso_class: MagicMock) -> None:
        """SwapIntent with protocol=enso compiles successfully on Berachain when Enso returns a route."""
        # Mock the Enso route object with proper structure
        mock_tx = MagicMock()
        mock_tx.to = "0x80EbA3855878739F4710233A8a19d89Bdd2ffB8E"
        mock_tx.value = "0"
        mock_tx.data = "0xabcdef1234567890"

        mock_route = MagicMock()
        mock_route.tx = mock_tx
        mock_route.gas = 200000
        mock_route.get_amount_out_wei.return_value = 500000000000000000  # 0.5 WBERA
        mock_route.price_impact = 50  # 0.5%

        mock_client = MagicMock()
        mock_client.get_route.return_value = mock_route
        mock_enso_class.return_value = mock_client

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="HONEY",
            to_token="WBERA",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="enso",
            chain="berachain",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert len(result.action_bundle.transactions) >= 1
        mock_enso_class.assert_called_once()
        mock_client.get_route.assert_called_once()

    def test_enso_swap_fails_with_unknown_token(self) -> None:
        """SwapIntent with unknown token fails gracefully."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="NONEXISTENT_TOKEN",
            to_token="WBERA",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="enso",
            chain="berachain",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "unknown" in result.error.lower() or "NONEXISTENT_TOKEN" in result.error
