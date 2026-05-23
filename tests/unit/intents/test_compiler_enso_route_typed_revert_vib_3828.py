"""Integration guard for VIB-3828 wiring at both Enso route fetch boundaries.

The original VIB-3828 PR (#2013) added the typed
:class:`EnsoRouterRevertError` exception + a ``COMPILATION_PERMANENT``
keyword entry in the state machine — but no production code path raised
the typed exception. The wiring that landed in this follow-up PR connects
the two upstream boundaries to the typed exception via
:func:`check_known_router_revert`:

1. ``connectors/enso/compiler.py:_get_enso_route_via_gateway`` — used by the strategy
   compiler when a gateway client is configured (production hosted +
   gateway-backed local).
2. ``connectors/enso/client.py:_get_route_via_gateway`` — used by the
   direct ``EnsoClient`` when a gateway client is configured (local-dev
   convenience that still routes through the gateway).

This test pins the end-to-end property: when the gateway returns a
``success=False`` response carrying a known router-revert selector
(e.g. ``0xef3dcb2f``), each boundary raises ``EnsoRouterRevertError``,
and the resulting message is classified as ``COMPILATION_PERMANENT`` by
the state machine — preventing the retry storm the original ticket
targeted.

Unknown selectors and selector-free errors fall through to the existing
``RuntimeError`` / ``EnsoAPIError`` paths unchanged so operators retain
visibility for novel reverts.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.enso.exceptions import (
    EnsoAPIError,
    EnsoRouterRevertError,
)
from almanak.framework.connectors.enso.compiler import EnsoCompiler
from almanak.framework.connectors.base.compiler import SwapCompilerContext
from almanak.framework.intents.state_machine import IntentStateMachine


# Realistic gateway error string format produced by
# ``almanak/gateway/services/enso_service.py:_request`` when the upstream
# Enso REST API returns a 400 with the revert selector in the response body.
_GATEWAY_REVERT_ERROR = "HTTP 400: execution reverted: 0xef3dcb2f"
_GATEWAY_UNKNOWN_REVERT = "HTTP 400: execution reverted: 0xdeadbeef"
_GATEWAY_TRANSIENT_ERROR = "HTTP 503: upstream timeout"


def _make_route_response(*, success: bool, error: str = "") -> MagicMock:
    """Build a fake EnsoRouteResponse with the minimum fields each
    boundary inspects on the failure path."""
    response = MagicMock()
    response.success = success
    response.error = error
    return response


# ---------------------------------------------------------------------------
# Boundary 1: EnsoCompiler._get_enso_route_via_gateway
# ---------------------------------------------------------------------------


class TestCompilerGatewayEnsoRouteTypedRevert:
    """Pin the typed-revert behavior at the compiler's gateway boundary."""

    def _make_ctx(self) -> SwapCompilerContext:
        """Construct the minimal context the Enso gateway boundary reads."""
        return SwapCompilerContext(
            chain="base",
            wallet_address="0x0000000000000000000000000000000000000001",
            rpc_url=None,
            rpc_timeout=10.0,
            permission_discovery=False,
            allow_placeholder_prices=True,
            token_resolver=None,
            gateway_client=MagicMock(),
            price_oracle={},
            cache={},
            services=MagicMock(),
            max_price_impact_pct=MagicMock(),
            using_placeholders=True,
        )

    def test_known_selector_raises_typed_revert(self) -> None:
        compiler = EnsoCompiler()
        ctx = self._make_ctx()
        ctx.gateway_client.enso.GetRoute.return_value = _make_route_response(success=False, error=_GATEWAY_REVERT_ERROR)

        with pytest.raises(EnsoRouterRevertError) as excinfo:
            compiler._get_enso_route_via_gateway(
                ctx,
                token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                token_out="0x4200000000000000000000000000000000000006",
                amount_in="1000000",
                slippage_bps=50,
            )
        err = excinfo.value
        assert err.selector == "0xef3dcb2f"
        assert err.chain == "base"

    def test_typed_revert_message_state_machine_classifies_permanent(
        self,
    ) -> None:
        """End-to-end property: typed revert message reaches the state
        machine and gets classified as ``COMPILATION_PERMANENT`` instead
        of falling through to ``REVERT`` (the retry-storm class)."""
        compiler = EnsoCompiler()
        ctx = self._make_ctx()
        ctx.gateway_client.enso.GetRoute.return_value = _make_route_response(success=False, error=_GATEWAY_REVERT_ERROR)

        with pytest.raises(EnsoRouterRevertError) as excinfo:
            compiler._get_enso_route_via_gateway(
                ctx,
                token_in="USDC",
                token_out="WETH",
                amount_in="1000000",
                slippage_bps=50,
            )
        sm = IntentStateMachine.__new__(IntentStateMachine)
        assert sm._categorize_error(str(excinfo.value)) == "COMPILATION_PERMANENT"

    def test_unknown_selector_falls_through_to_runtime_error(self) -> None:
        compiler = EnsoCompiler()
        ctx = self._make_ctx()
        ctx.gateway_client.enso.GetRoute.return_value = _make_route_response(
            success=False, error=_GATEWAY_UNKNOWN_REVERT
        )

        with pytest.raises(RuntimeError) as excinfo:
            compiler._get_enso_route_via_gateway(
                ctx,
                token_in="0x0",
                token_out="0x1",
                amount_in="1",
                slippage_bps=50,
            )
        # Existing message preserved verbatim — operators see the unknown
        # selector and can investigate.
        assert "Gateway Enso GetRoute failed" in str(excinfo.value)
        assert "0xdeadbeef" in str(excinfo.value)

    def test_transient_error_falls_through_to_runtime_error(self) -> None:
        compiler = EnsoCompiler()
        ctx = self._make_ctx()
        ctx.gateway_client.enso.GetRoute.return_value = _make_route_response(
            success=False, error=_GATEWAY_TRANSIENT_ERROR
        )

        with pytest.raises(RuntimeError) as excinfo:
            compiler._get_enso_route_via_gateway(
                ctx,
                token_in="0x0",
                token_out="0x1",
                amount_in="1",
                slippage_bps=50,
            )
        assert "HTTP 503" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Boundary 2: connectors/enso/client.py:_get_route_via_gateway
# ---------------------------------------------------------------------------


class TestEnsoClientGatewayRouteTypedRevert:
    """Pin the typed-revert behavior at the EnsoClient's gateway boundary."""

    def _make_client(self) -> object:
        from almanak.framework.connectors.enso.client import EnsoClient

        client = EnsoClient.__new__(EnsoClient)
        client.config = MagicMock()
        client.config.chain_name = "base"
        client.config.chain_id = 8453
        client.config.timeout = 30.0
        client.config.gateway_client = MagicMock()
        return client

    def test_known_selector_raises_typed_revert(self) -> None:
        from almanak.framework.connectors.enso.models import RoutingStrategy

        client = self._make_client()
        client.config.gateway_client.enso.GetRoute.return_value = _make_route_response(
            success=False, error=_GATEWAY_REVERT_ERROR
        )

        with pytest.raises(EnsoRouterRevertError) as excinfo:
            client._get_route_via_gateway(
                token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                token_out="0x4200000000000000000000000000000000000006",
                amount_in=1_000_000,
                slippage_bps=50,
                from_addr="0x0000000000000000000000000000000000000001",
                receiver=None,
                strategy=RoutingStrategy.ROUTER,
                destination_chain_id=None,
                refund_receiver=None,
            )
        assert excinfo.value.selector == "0xef3dcb2f"

    def test_typed_revert_message_state_machine_classifies_permanent(
        self,
    ) -> None:
        from almanak.framework.connectors.enso.models import RoutingStrategy

        client = self._make_client()
        client.config.gateway_client.enso.GetRoute.return_value = _make_route_response(
            success=False, error=_GATEWAY_REVERT_ERROR
        )

        with pytest.raises(EnsoRouterRevertError) as excinfo:
            client._get_route_via_gateway(
                token_in="USDC",
                token_out="WETH",
                amount_in=1,
                slippage_bps=50,
                from_addr="0x0000000000000000000000000000000000000001",
                receiver=None,
                strategy=RoutingStrategy.ROUTER,
                destination_chain_id=None,
                refund_receiver=None,
            )
        sm = IntentStateMachine.__new__(IntentStateMachine)
        assert sm._categorize_error(str(excinfo.value)) == "COMPILATION_PERMANENT"

    def test_unknown_selector_falls_through_to_enso_api_error(self) -> None:
        from almanak.framework.connectors.enso.models import RoutingStrategy

        client = self._make_client()
        client.config.gateway_client.enso.GetRoute.return_value = _make_route_response(
            success=False, error=_GATEWAY_UNKNOWN_REVERT
        )

        with pytest.raises(EnsoAPIError) as excinfo:
            client._get_route_via_gateway(
                token_in="0x0",
                token_out="0x1",
                amount_in=1,
                slippage_bps=50,
                from_addr="0x0",
                receiver=None,
                strategy=RoutingStrategy.ROUTER,
                destination_chain_id=None,
                refund_receiver=None,
            )
        assert "Gateway Enso GetRoute failed" in str(excinfo.value)
        assert "0xdeadbeef" in str(excinfo.value)
