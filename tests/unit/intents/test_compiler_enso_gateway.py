"""Tests for connector-owned Enso gateway routing.

Verifies the Enso compiler's _get_enso_route dispatcher correctly:
- Routes through gateway gRPC when gateway_client is connected
- Fails fast when gateway_client is configured but not connected
- Falls back to direct EnsoClient when no gateway_client (local dev)
- Fails in deployed mode without gateway_client

Also covers _compile_cross_chain_swap end-to-end: bundle assembly, validation
guards, and exception handling.
"""

import os
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.enso.compiler import EnsoCompiler
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import SwapIntent


def _make_ctx(gateway_client=None):
    """Create Enso compiler context with placeholder prices for testing."""
    framework_compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
        gateway_client=gateway_client,
    )
    enso_compiler = EnsoCompiler()
    return framework_compiler._build_compiler_context("enso", enso_compiler)


class TestEnsoGatewayRouting:
    """Tests for _get_enso_route dispatcher."""

    def test_gateway_connected_uses_grpc(self):
        """When gateway_client is connected, uses gRPC path."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=True,
            to="0xrouter",
            data="0xcalldata",
            value="0",
            gas="250000",
            gas_estimate="",
            amount_out="1000000",
            price_impact=15,
            is_cross_chain=False,
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=mock_client)
        result = compiler._get_enso_route(ctx, "0xtoken_in", "0xtoken_out", "1000000", 50)

        assert result["to"] == "0xrouter"
        assert result["data"] == "0xcalldata"
        assert result["gas"] == 250000
        assert result["amount_out"] == "1000000"
        assert result["price_impact"] == 15
        mock_client.enso.GetRoute.assert_called_once()

    def test_gateway_configured_but_disconnected_raises(self):
        """When gateway_client exists but is_connected is False, raises RuntimeError."""
        mock_client = MagicMock()
        mock_client.is_connected = False

        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=mock_client)

        with pytest.raises(RuntimeError, match="not connected"):
            compiler._get_enso_route(ctx, "0xtoken_in", "0xtoken_out", "1000000", 50)

    @patch.dict(os.environ, {"ALMANAK_IS_HOSTED": ""}, clear=False)
    def test_no_gateway_local_dev_uses_direct(self):
        """When no gateway_client (local dev), falls back to _get_enso_route_direct."""
        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=None)

        direct_result = {
            "to": "0xrouter_direct",
            "data": "0xcalldata_direct",
            "value": "0",
            "gas": 200000,
            "amount_out": "999000",
            "price_impact": 10,
        }

        with patch.object(compiler, "_get_enso_route_direct", return_value=direct_result) as mock_direct:
            result = compiler._get_enso_route(ctx, "0xtoken_in", "0xtoken_out", "1000000", 50)

        assert result["to"] == "0xrouter_direct"
        assert result["gas"] == 200000
        mock_direct.assert_called_once_with(
            ctx,
            "0xtoken_in",
            "0xtoken_out",
            1000000,
            50,
            chain=None,
            destination_chain_id=None,
            receiver=None,
            refund_receiver=None,
        )

    @patch.dict(
        os.environ,
        {"ALMANAK_IS_HOSTED": "true", "ALMANAK_DEPLOYMENT_ID": "agent-test-123"},
        clear=False,
    )
    def test_no_gateway_deployed_mode_raises(self):
        """When hosted mode has no gateway_client, raises RuntimeError."""
        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=None)

        with pytest.raises(RuntimeError, match="no gateway client configured"):
            compiler._get_enso_route(ctx, "0xtoken_in", "0xtoken_out", "1000000", 50)

    def test_gateway_gas_none_returns_none(self):
        """When gateway returns empty gas, result gas is None."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=True,
            to="0xrouter",
            data="0xcalldata",
            value="0",
            gas="",
            gas_estimate="",
            amount_out="1000000",
            price_impact=0,
            is_cross_chain=False,
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=mock_client)
        result = compiler._get_enso_route(ctx, "0xtoken_in", "0xtoken_out", "1000000", 50)

        assert result["gas"] is None

    def test_gateway_error_raises(self):
        """When gateway returns success=False, raises RuntimeError."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=False,
            error="Enso API key not configured",
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=mock_client)

        with pytest.raises(RuntimeError, match="Gateway Enso GetRoute failed"):
            compiler._get_enso_route(ctx, "0xtoken_in", "0xtoken_out", "1000000", 50)

    def test_cross_chain_params_forwarded(self):
        """Cross-chain params are forwarded to gateway gRPC."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=True,
            to="0xrouter",
            data="0xcalldata",
            value="0",
            gas="300000",
            gas_estimate="",
            amount_out="1000000",
            price_impact=5,
            is_cross_chain=True,
            bridge_fee="1000",
            estimated_time=180,
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = EnsoCompiler()
        ctx = _make_ctx(gateway_client=mock_client)
        result = compiler._get_enso_route(
            ctx,
            "0xtoken_in",
            "0xtoken_out",
            "1000000",
            50,
            chain="base",
            destination_chain_id=42161,
            refund_receiver="0xrefund",
        )

        assert result["is_cross_chain"] is True
        assert result["bridge_fee"] == "1000"
        assert result["estimated_time"] == 180

        # Verify the request was built with cross-chain params
        call_args = mock_client.enso.GetRoute.call_args
        request = call_args[0][0]
        assert request.chain == "base"
        assert request.destination_chain_id == 42161
        assert request.refund_receiver == "0xrefund"


# =============================================================================
# _compile_cross_chain_swap helpers
# =============================================================================


def _make_token(symbol: str, address: str, decimals: int = 18, is_native: bool = False) -> TokenInfo:
    return TokenInfo(symbol=symbol, address=address, decimals=decimals, is_native=is_native)


def _make_cross_chain_ctx(
    *,
    from_token: TokenInfo | None = None,
    to_token: TokenInfo | None = None,
    dest_wallet: str = "0xdest_wallet",
    usd_to_token: int = 1_000_000,
) -> SimpleNamespace:
    """Build a stub SwapCompilerContext for _compile_cross_chain_swap tests.

    Mocks `ctx.services` so each guard branch can be exercised independently.
    Pass ``from_token=None`` to simulate "unknown from_token", same for to_token.
    """
    services = MagicMock()

    def resolve_token(symbol, chain=None):
        if symbol == "FROM":
            return from_token
        if symbol == "TO":
            return to_token
        return None

    services.resolve_token.side_effect = resolve_token
    services.resolve_dest_wallet.return_value = dest_wallet
    services.usd_to_token_amount.return_value = usd_to_token
    services.build_approve_tx.return_value = [
        TransactionData(
            to="0xapprove",
            value=0,
            data="0xapprove_data",
            gas_estimate=50000,
            description="Approve",
            tx_type="approve",
        )
    ]
    services.format_amount.side_effect = lambda amt, dec: f"{amt}/{dec}"
    return SimpleNamespace(
        chain="base",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        services=services,
        gateway_client=None,
    )


def _swap_intent(
    *,
    destination_chain: str | None = "arbitrum",
    amount_usd: Decimal | None = Decimal("100"),
    amount: object = None,
) -> SwapIntent:
    return SwapIntent(
        from_token="FROM",
        to_token="TO",
        amount_usd=amount_usd,
        amount=amount,
        max_slippage=Decimal("0.005"),
        protocol="enso",
        chain="base",
        destination_chain=destination_chain,
    )


class TestCompileCrossChainSwap:
    """Tests for EnsoCompiler._compile_cross_chain_swap — happy path + guards."""

    def test_happy_path_builds_approve_plus_swap_bundle(self):
        compiler = EnsoCompiler()
        from_tok = _make_token("USDC", "0xfrom", decimals=6)
        to_tok = _make_token("WETH", "0xto", decimals=18)
        ctx = _make_cross_chain_ctx(from_token=from_tok, to_token=to_tok)
        intent = _swap_intent()

        route_data = {
            "to": "0xrouter",
            "value": "0",
            "data": "0xroute_calldata",
            "gas": 300000,
            "amount_out": "950000000000000",
            "price_impact": 12,
            "is_cross_chain": True,
            "bridge_fee": "1000",
            "estimated_time": 180,
        }
        with patch.object(compiler, "_get_enso_route", return_value=route_data) as mock_route:
            result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.SUCCESS
        assert len(result.transactions) == 2  # approve + swap
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "cross_chain_swap"
        assert result.action_bundle is not None
        meta = result.action_bundle.metadata
        assert meta["protocol"] == "enso"
        assert meta["is_cross_chain"] is True
        assert meta["source_chain"] == "base"
        assert meta["destination_chain"] == "arbitrum"
        assert meta["router"] == "0xrouter"
        assert meta["bridge_fee"] == "1000"
        assert meta["estimated_time"] == 180

        # destination wallet propagated to receiver / refund_receiver
        mock_route.assert_called_once()
        _, kwargs = mock_route.call_args
        assert kwargs["receiver"] == "0xdest_wallet"
        assert kwargs["refund_receiver"] == "0xdest_wallet"

    def test_native_token_skips_approve(self):
        compiler = EnsoCompiler()
        from_tok = _make_token("ETH", "0xfrom", decimals=18, is_native=True)
        to_tok = _make_token("WETH", "0xto", decimals=18)
        ctx = _make_cross_chain_ctx(from_token=from_tok, to_token=to_tok)
        intent = _swap_intent()

        route_data = {
            "to": "0xrouter",
            "value": "100",
            "data": "0xdata",
            "gas": 300000,
            "amount_out": "1000",
        }
        with patch.object(compiler, "_get_enso_route", return_value=route_data):
            result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.SUCCESS
        # No approve TX for native token, only the swap TX
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "cross_chain_swap"
        ctx.services.build_approve_tx.assert_not_called()

    def test_missing_destination_chain_fails(self):
        compiler = EnsoCompiler()
        ctx = _make_cross_chain_ctx(
            from_token=_make_token("USDC", "0xfrom"),
            to_token=_make_token("WETH", "0xto"),
        )
        intent = _swap_intent(destination_chain=None)

        result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "destination_chain" in (result.error or "")

    def test_unknown_from_token_fails(self):
        compiler = EnsoCompiler()
        ctx = _make_cross_chain_ctx(
            from_token=None,  # resolve_token returns None for FROM
            to_token=_make_token("WETH", "0xto"),
        )
        intent = _swap_intent()

        result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown token" in (result.error or "")
        assert "base" in (result.error or "")  # error names source chain

    def test_unknown_to_token_fails(self):
        compiler = EnsoCompiler()
        ctx = _make_cross_chain_ctx(
            from_token=_make_token("USDC", "0xfrom"),
            to_token=None,
        )
        intent = _swap_intent()

        result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown token" in (result.error or "")
        assert "arbitrum" in (result.error or "")  # error names dest chain

    def test_amount_all_unresolved_fails(self):
        compiler = EnsoCompiler()
        ctx = _make_cross_chain_ctx(
            from_token=_make_token("USDC", "0xfrom"),
            to_token=_make_token("WETH", "0xto"),
        )
        intent = _swap_intent(amount_usd=None, amount="all")

        result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "all" in (result.error or "")

    def test_unsupported_destination_chain_fails(self):
        compiler = EnsoCompiler()
        ctx = _make_cross_chain_ctx(
            from_token=_make_token("USDC", "0xfrom"),
            to_token=_make_token("WETH", "0xto"),
        )
        intent = _swap_intent(destination_chain="mars")  # not in CHAIN_MAPPING

        result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unsupported destination chain" in (result.error or "")
        assert "mars" in (result.error or "")

    def test_route_fetch_exception_marks_failed(self):
        compiler = EnsoCompiler()
        ctx = _make_cross_chain_ctx(
            from_token=_make_token("USDC", "0xfrom"),
            to_token=_make_token("WETH", "0xto"),
        )
        intent = _swap_intent()

        with patch.object(compiler, "_get_enso_route", side_effect=RuntimeError("api down")):
            result = compiler._compile_cross_chain_swap(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "api down" in (result.error or "")
