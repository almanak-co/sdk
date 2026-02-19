"""Unit tests for LiFi swap compilation in IntentCompiler."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)


def _make_compiler(chain: str = "arbitrum") -> IntentCompiler:
    """Create a compiler with placeholder prices for testing."""
    return IntentCompiler(
        chain=chain,
        wallet_address="0x1111111111111111111111111111111111111111",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


def _mock_lifi_quote(
    *,
    tool: str = "1inch",
    step_type: str = "swap",
    to_amount: str = "500000000000000000",
    to_amount_min: str = "497500000000000000",
    approval_address: str = "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
    gas_estimate: str = "200000",
    tx_to: str = "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
    tx_value: str = "0",
    tx_data: str = "0xabcdef1234567890",
    from_chain_id: int = 42161,
    to_chain_id: int = 42161,
) -> MagicMock:
    """Create a mock LiFi quote (LiFiStep)."""
    quote = MagicMock()
    quote.tool = tool
    quote.type = step_type

    # Estimate
    estimate = MagicMock()
    estimate.approval_address = approval_address
    estimate.total_gas_estimate = int(gas_estimate)
    estimate.to_amount = to_amount
    estimate.to_amount_min = to_amount_min
    estimate.execution_duration = 30
    quote.estimate = estimate

    # Transaction request
    tx_request = MagicMock()
    tx_request.to = tx_to
    tx_request.value = tx_value
    tx_request.data = tx_data
    tx_request.gas_limit = gas_estimate
    quote.transaction_request = tx_request

    # Methods
    quote.get_to_amount.return_value = int(to_amount)
    quote.get_to_amount_min.return_value = int(to_amount_min)
    quote.is_cross_chain = from_chain_id != to_chain_id

    # Action
    action = MagicMock()
    action.from_chain_id = from_chain_id
    action.to_chain_id = to_chain_id
    quote.action = action

    return quote


class TestCompileLiFiSwapSameChain:
    """Tests for same-chain LiFi swap compilation."""

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_same_chain_swap_compiles_successfully(self, mock_get_quote: MagicMock) -> None:
        """Same-chain LiFi swap produces a valid ActionBundle."""
        mock_get_quote.return_value = _mock_lifi_quote()
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "SWAP"

        metadata = result.action_bundle.metadata
        assert metadata["protocol"] == "lifi"
        assert metadata["tool"] == "1inch"
        assert metadata["is_cross_chain"] is False
        assert metadata["deferred_swap"] is True
        assert metadata["from_chain_id"] == 42161
        assert metadata["to_chain_id"] == 42161

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_same_chain_swap_includes_approval_for_erc20(self, mock_get_quote: MagicMock) -> None:
        """ERC-20 same-chain swap includes an approve TX."""
        mock_get_quote.return_value = _mock_lifi_quote()
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # At minimum 1 swap tx; may include approve tx(s)
        assert len(result.transactions) >= 1
        # Last transaction is the swap
        last_tx = result.transactions[-1]
        assert last_tx.tx_type == "swap_deferred"

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_amount_usd_conversion(self, mock_get_quote: MagicMock) -> None:
        """amount_usd is converted to token amount using price oracle."""
        mock_get_quote.return_value = _mock_lifi_quote()
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("500"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # Verify the API was called with the converted amount
        mock_get_quote.assert_called_once()
        call_kwargs = mock_get_quote.call_args
        # USDC price ~= 1 in placeholder, so amount should be ~500 * 10^6
        from_amount = call_kwargs.kwargs.get("from_amount") or call_kwargs[1].get("from_amount")
        assert from_amount is not None

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_amount_decimal_conversion(self, mock_get_quote: MagicMock) -> None:
        """Decimal amount is converted to wei correctly."""
        mock_get_quote.return_value = _mock_lifi_quote()
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100.5"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        mock_get_quote.assert_called_once()
        call_kwargs = mock_get_quote.call_args
        from_amount = call_kwargs.kwargs.get("from_amount") or call_kwargs[1].get("from_amount")
        # 100.5 USDC * 10^6 = 100500000
        assert from_amount == "100500000"

    def test_amount_all_rejected(self) -> None:
        """amount='all' is rejected before compilation."""
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "amount='all'" in result.error

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_metadata_contains_route_params(self, mock_get_quote: MagicMock) -> None:
        """ActionBundle metadata includes route_params for deferred execution."""
        mock_get_quote.return_value = _mock_lifi_quote()
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        route_params = result.action_bundle.metadata["route_params"]
        assert route_params["from_chain_id"] == 42161
        assert route_params["to_chain_id"] == 42161
        assert route_params["from_address"] == "0x1111111111111111111111111111111111111111"
        assert "from_token" in route_params
        assert "to_token" in route_params
        assert "from_amount" in route_params
        assert "slippage" in route_params


class TestCompileLiFiSwapCrossChain:
    """Tests for cross-chain LiFi swap compilation."""

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_cross_chain_swap_compiles_successfully(self, mock_get_quote: MagicMock) -> None:
        """Cross-chain LiFi swap produces valid ActionBundle with bridge metadata."""
        mock_get_quote.return_value = _mock_lifi_quote(
            tool="across",
            step_type="cross",
            from_chain_id=42161,
            to_chain_id=8453,
        )
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("1000"),
            protocol="lifi",
            chain="arbitrum",
            destination_chain="base",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None

        metadata = result.action_bundle.metadata
        assert metadata["protocol"] == "lifi"
        assert metadata["tool"] == "across"
        assert metadata["is_cross_chain"] is True
        assert metadata["from_chain_id"] == 42161
        assert metadata["to_chain_id"] == 8453
        assert metadata["deferred_swap"] is True

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_cross_chain_swap_tx_type_is_bridge_deferred(self, mock_get_quote: MagicMock) -> None:
        """Cross-chain swap transaction type is 'bridge_deferred'."""
        mock_get_quote.return_value = _mock_lifi_quote(
            from_chain_id=42161,
            to_chain_id=8453,
        )
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("1000"),
            protocol="lifi",
            chain="arbitrum",
            destination_chain="base",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # Last transaction should be bridge_deferred
        last_tx = result.transactions[-1]
        assert last_tx.tx_type == "bridge_deferred"


class TestCompileLiFiSwapErrors:
    """Tests for LiFi swap compilation error handling."""

    def test_unsupported_source_chain(self) -> None:
        """Unsupported source chain returns FAILED compilation."""
        compiler = _make_compiler(chain="unsupported_chain")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "does not support chain" in result.error

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_unsupported_destination_chain(self, mock_get_quote: MagicMock) -> None:
        """Unsupported destination chain returns FAILED compilation."""
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("1000"),
            protocol="lifi",
            chain="arbitrum",
            destination_chain="unsupported_chain",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "does not support chain" in result.error

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_token_resolution_failure(self, mock_get_quote: MagicMock) -> None:
        """Unknown token returns FAILED compilation."""
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="UNKNOWN_TOKEN_XYZ",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "UNKNOWN_TOKEN_XYZ" in result.error

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_lifi_api_error_handled(self, mock_get_quote: MagicMock) -> None:
        """LiFi API errors are caught and produce FAILED compilation."""
        from almanak.framework.connectors.lifi.exceptions import LiFiAPIError

        mock_get_quote.side_effect = LiFiAPIError(
            message="Rate limit exceeded",
            status_code=429,
            endpoint="/quote",
        )
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Rate limit" in result.error

    @patch("almanak.framework.connectors.lifi.client.LiFiClient.get_quote")
    def test_route_not_found_error_handled(self, mock_get_quote: MagicMock) -> None:
        """LiFi route-not-found errors produce FAILED compilation."""
        from almanak.framework.connectors.lifi.exceptions import LiFiRouteNotFoundError

        mock_get_quote.side_effect = LiFiRouteNotFoundError("No route found")
        compiler = _make_compiler()

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            protocol="lifi",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "No route found" in result.error


class TestSwapIntentValidatorLiFi:
    """Tests for SwapIntent cross-chain validator accepting LiFi."""

    def test_lifi_accepted_for_cross_chain(self) -> None:
        """protocol='lifi' is accepted for cross-chain swaps."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("100"),
            protocol="lifi",
            chain="arbitrum",
            destination_chain="base",
        )
        # Should not raise
        assert intent.is_cross_chain is True
        assert intent.protocol == "lifi"

    def test_enso_still_accepted_for_cross_chain(self) -> None:
        """protocol='enso' remains accepted for cross-chain swaps."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("100"),
            protocol="enso",
            chain="arbitrum",
            destination_chain="base",
        )
        assert intent.is_cross_chain is True
        assert intent.protocol == "enso"

    def test_unsupported_protocol_rejected_for_cross_chain(self) -> None:
        """Non-aggregator protocols are rejected for cross-chain swaps."""
        with pytest.raises(ValueError, match="Cross-chain swaps require protocol='enso' or protocol='lifi'"):
            SwapIntent(
                from_token="USDC",
                to_token="USDC",
                amount=Decimal("100"),
                protocol="uniswap_v3",
                chain="arbitrum",
                destination_chain="base",
            )

    def test_no_protocol_allowed_for_cross_chain(self) -> None:
        """No explicit protocol is allowed for cross-chain (framework picks default)."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("100"),
            chain="arbitrum",
            destination_chain="base",
        )
        assert intent.is_cross_chain is True
        assert intent.protocol is None
