"""GetPrice on an unconfigured gateway (on-demand chain registration).

The AlmanakCode sidecar data gateway boots with ``settings.chains = []`` on
purpose — RpcService documents this as "empty settings.chains = accept any
chain (on-demand mode)". GetPrice was the one lane treating the empty
allowlist as deny-all: every chain-hinted price request bounced with
INVALID_ARGUMENT while the same gateway served that chain's balances, pool
state, and history (observed live on staging 2026-08-24; a balance call's
auto-reinit then "healed" price — order-dependent availability).

The fix and its two review-hardened invariants, all pinned here:

* GetPrice takes the same on-demand step as GetBalance before its gate.
* **``settings.chains`` is never mutated by request-learned chains** (codex
  P1): the settings object is shared with RpcService, whose open mode must
  not silently collapse into a one-chain allowlist after the first price
  request. On-demand chains live in ``_on_demand_chains``; gates never read
  that list.
* The on-demand path is **transactional** (coderabbit): a failed rebuild
  rolls the ledger back so a chain is never recorded servable without an
  aggregator.
* Once any chain IS provisioned (boot config / RegisterChains), the
  allowlist stays a hard boundary.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings


def _settings(chains):
    s = MagicMock(spec=GatewaySettings)
    s.chains = chains
    s.network = "mainnet"
    s.coingecko_api_key = None
    return s


def _servicer(chains):
    from almanak.gateway.services.market_service import MarketServiceServicer

    return MarketServiceServicer(settings=_settings(chains))


def _grpc_context():
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _wire_served_price(servicer, price="2493.69"):
    """Mock the post-gate pricing path to return a fixed aggregated price."""
    servicer._ensure_initialized = AsyncMock()
    servicer._resolve_token_for_pricing = AsyncMock(return_value=None)
    result = MagicMock(price=Decimal(price), source="aggregated", confidence=0.9, stale=False)
    result.timestamp = MagicMock()
    result.timestamp.timestamp.return_value = 1787420000
    aggregator = MagicMock()
    aggregator.get_aggregated_price = AsyncMock(return_value=result)
    aggregator.get_last_details = MagicMock(return_value=None)
    servicer._aggregator_for = MagicMock(return_value=aggregator)


class TestAutoReinitializeUnconfiguredChains:
    @pytest.mark.asyncio
    async def test_unconfigured_gateway_registers_requested_chain_on_demand(self):
        servicer = _servicer([])
        servicer.reinitialize = AsyncMock()

        await servicer._auto_reinitialize_unconfigured_chains(["arbitrum"])

        servicer.reinitialize.assert_awaited_once_with("arbitrum", provisioned=False)

    @pytest.mark.asyncio
    async def test_configured_gateway_is_a_hard_boundary(self):
        """Once ANY chain is provisioned, the helper must be a no-op."""
        servicer = _servicer(["base"])
        servicer.reinitialize = AsyncMock()

        await servicer._auto_reinitialize_unconfigured_chains(["arbitrum"])

        servicer.reinitialize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_already_registered_chain_is_not_rebuilt(self):
        servicer = _servicer([])
        servicer._on_demand_chains.append("arbitrum")
        servicer.reinitialize = AsyncMock()

        await servicer._auto_reinitialize_unconfigured_chains(["arbitrum"])

        servicer.reinitialize.assert_not_awaited()


class TestReinitializeOnDemandInvariants:
    @pytest.mark.asyncio
    async def test_on_demand_registration_never_mutates_shared_settings(self):
        """codex P1: settings.chains is shared with RpcService — a request-
        learned chain must not collapse its documented open mode."""
        servicer = _servicer([])
        servicer._do_initialize = MagicMock()
        servicer._close_aggregator_sources = AsyncMock()

        await servicer.reinitialize("arbitrum", provisioned=False)

        assert servicer.settings.chains == []
        assert servicer._on_demand_chains == ["arbitrum"]

    @pytest.mark.asyncio
    async def test_provisioned_registration_still_tightens_the_allowlist(self):
        """RegisterChains semantics unchanged: wallet-registry gateways keep
        their provisioned-allowlist protection."""
        servicer = _servicer([])
        servicer._do_initialize = MagicMock()
        servicer._close_aggregator_sources = AsyncMock()

        await servicer.reinitialize("base")

        assert servicer.settings.chains == ["base"]
        assert servicer._on_demand_chains == []

    @pytest.mark.asyncio
    async def test_failed_on_demand_build_rolls_back_the_ledger(self):
        """coderabbit: a failed rebuild must not leave the chain recorded as
        servable (and the old aggregator map keeps serving)."""
        servicer = _servicer([])
        servicer._do_initialize = MagicMock(side_effect=RuntimeError("source build failed"))
        servicer._close_aggregator_sources = AsyncMock()

        with pytest.raises(RuntimeError):
            await servicer.reinitialize("arbitrum", provisioned=False)

        assert servicer._on_demand_chains == []
        servicer._close_aggregator_sources.assert_not_awaited()


class TestGetPriceOnDemandChain:
    @pytest.mark.asyncio
    async def test_chain_hinted_price_serves_on_unconfigured_gateway(self):
        """The staging repro: chains=[], request.chain="arbitrum" must be
        served (after on-demand registration), not INVALID_ARGUMENT."""
        from almanak.gateway.proto import gateway_pb2

        servicer = _servicer([])

        async def _register(chain: str, *, provisioned: bool = True) -> None:
            assert provisioned is False
            servicer._on_demand_chains.append(chain)

        servicer.reinitialize = AsyncMock(side_effect=_register)
        _wire_served_price(servicer)

        ctx = _grpc_context()
        request = gateway_pb2.PriceRequest(token="ETH", quote="USD", chain="arbitrum")
        response = await servicer.GetPrice(request, ctx)

        servicer.reinitialize.assert_awaited_once_with("arbitrum", provisioned=False)
        assert servicer.settings.chains == []  # shared-settings invariant holds end-to-end
        ctx.set_code.assert_not_called()
        assert response.price == "2493.69"

    @pytest.mark.asyncio
    async def test_second_chain_also_serves_on_demand(self):
        """Open mode must survive the first registration: a Base request after
        an arbitrum registration serves too (the regression codex flagged)."""
        from almanak.gateway.proto import gateway_pb2

        servicer = _servicer([])
        servicer._on_demand_chains.append("arbitrum")

        async def _register(chain: str, *, provisioned: bool = True) -> None:
            servicer._on_demand_chains.append(chain)

        servicer.reinitialize = AsyncMock(side_effect=_register)
        _wire_served_price(servicer)

        ctx = _grpc_context()
        request = gateway_pb2.PriceRequest(token="ETH", quote="USD", chain="base")
        response = await servicer.GetPrice(request, ctx)

        servicer.reinitialize.assert_awaited_once_with("base", provisioned=False)
        ctx.set_code.assert_not_called()
        assert response.price == "2493.69"

    @pytest.mark.asyncio
    async def test_provisioned_gateway_still_rejects_other_chains(self):
        """A gateway launched for one chain must keep rejecting others —
        the wrong-chain-data protection the gate exists for."""
        from almanak.gateway.proto import gateway_pb2

        servicer = _servicer(["base"])
        servicer.reinitialize = AsyncMock()
        servicer._ensure_initialized = AsyncMock()

        ctx = _grpc_context()
        request = gateway_pb2.PriceRequest(token="ETH", quote="USD", chain="arbitrum")
        response = await servicer.GetPrice(request, ctx)

        servicer.reinitialize.assert_not_awaited()
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_once_with("Chain 'arbitrum' is not configured on this gateway")
        assert response.price == ""
