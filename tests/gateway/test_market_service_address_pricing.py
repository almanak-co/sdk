"""End-to-end test for MarketService pricing a token by contract address.

The request sends only a contract address (plus a chain hint). The service
must:
  1. Recognise the address.
  2. Resolve it on-chain via OnChainLookup (symbol/decimals).
  3. Forward a ResolvedToken to the PriceAggregator.
  4. CoinGecko then uses its contract-address endpoint to price the token.

This proves the full "resolve unknown token from address -> price it" path
without relying on any hardcoded entry for the test token (cbBTC on Base).
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.tokens import ResolvedToken
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer
from almanak.gateway.services.onchain_lookup import TokenMetadata

CBBTC_ADDRESS = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"


def _mock_coingecko_address_response(source, payload: dict, status: int = 200):
    """Patch the CoinGecko source's HTTP session to return one JSON payload."""
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value=payload)
    resp.text = AsyncMock(return_value="")

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.get = MagicMock(return_value=cm)
    return patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)


@pytest.mark.asyncio
async def test_getprice_resolves_cbbtc_from_address_without_hardcoded_entry():
    """cbBTC (not in any registry) should be priced via on-chain resolution
    plus the CoinGecko contract-address endpoint."""
    # Pre-flight: if cbBTC ever gets hardcoded this test stops proving anything.
    from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS, get_coingecko_id
    from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS

    assert "CBBTC" not in GLOBAL_TOKEN_IDS
    assert get_coingecko_id("CBBTC") is None
    for tok in DEFAULT_TOKENS:
        assert CBBTC_ADDRESS.lower() not in {a.lower() for a in (tok.addresses or {}).values()}

    # Build a settings stub covering only what _do_initialize touches.
    settings = MagicMock()
    settings.chains = ["base"]
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)

    # Stub OnChainLookup: pretend the ERC20 contract returned symbol="cbBTC",
    # decimals=8. This is what would happen in production via eth_call on Base.
    fake_metadata = TokenMetadata(
        symbol="cbBTC",
        name="Coinbase Wrapped BTC",
        decimals=8,
        address=CBBTC_ADDRESS,
        is_native=False,
    )
    fake_lookup = MagicMock()
    fake_lookup.lookup = AsyncMock(return_value=fake_metadata)

    async def fake_get_onchain_lookup(chain: str):
        return fake_lookup

    # Initialise the aggregator before we swap out sources so we can find the
    # CoinGecko source cleanly.
    await servicer._ensure_initialized()

    # Reduce the aggregator to CoinGecko only — the other sources (Chainlink,
    # Binance, DexScreener) would try real HTTP/RPC in unit tests.
    cg_source = next(
        s for s in servicer._price_aggregator.sources if s.source_name == "coingecko"
    )
    servicer._price_aggregator._sources = [cg_source]

    # Mock CoinGecko's contract-address endpoint.
    payload = {CBBTC_ADDRESS.lower(): {"usd": 65000.0}}

    with (
        patch.object(
            servicer,
            "_get_onchain_lookup",
            side_effect=fake_get_onchain_lookup,
        ),
        _mock_coingecko_address_response(cg_source, payload),
    ):
        request = gateway_pb2.PriceRequest(
            token=CBBTC_ADDRESS,
            quote="USD",
            chain="base",
        )
        context = MagicMock()
        context.set_code = MagicMock()
        context.set_details = MagicMock()

        response = await servicer.GetPrice(request, context)

    # Success case: price populated, no gRPC error set.
    context.set_code.assert_not_called()
    context.set_details.assert_not_called()
    assert Decimal(response.price) == Decimal("65000.0")
    # The aggregator labels the response "aggregated" but CoinGecko must be
    # credited in the sources_ok list — that's how we prove the price came
    # from the address endpoint and not a cached/symbol path.
    assert "coingecko" in list(response.sources_ok)
    # The on-chain lookup must have been called exactly once, on Base.
    fake_lookup.lookup.assert_awaited_once_with("base", CBBTC_ADDRESS)


@pytest.mark.asyncio
async def test_getprice_forwards_resolved_token_with_chain_to_aggregator():
    """Resolved address metadata must reach the aggregator with chain context.

    This is the contract the original bug violated: when the caller provides
    an address plus chain hint, ``GetPrice`` must forward a ``ResolvedToken``
    carrying that chain so downstream price sources can use address endpoints
    instead of symbol lookup.
    """
    settings = MagicMock()
    settings.chains = ["base"]
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)

    fake_metadata = TokenMetadata(
        symbol="cbBTC",
        name="Coinbase Wrapped BTC",
        decimals=8,
        address=CBBTC_ADDRESS,
        is_native=False,
    )
    fake_lookup = MagicMock()
    fake_lookup.lookup = AsyncMock(return_value=fake_metadata)

    async def fake_get_onchain_lookup(chain: str):
        return fake_lookup

    await servicer._ensure_initialized()

    captured: dict[str, object] = {}

    async def fake_get_aggregated_price(token: str, quote: str, *, resolved_token=None):
        captured["token"] = token
        captured["quote"] = quote
        captured["resolved_token"] = resolved_token

        class _Result:
            price = Decimal("65000")
            source = "aggregated"
            confidence = 1.0
            stale = False
            timestamp = datetime.now(UTC)

        return _Result()

    servicer._price_aggregator.get_aggregated_price = AsyncMock(side_effect=fake_get_aggregated_price)
    servicer._price_aggregator.get_last_details = MagicMock(return_value={})

    with patch.object(servicer, "_get_onchain_lookup", side_effect=fake_get_onchain_lookup):
        request = gateway_pb2.PriceRequest(token=CBBTC_ADDRESS, quote="USD", chain="base")
        context = MagicMock()
        context.set_code = MagicMock()
        context.set_details = MagicMock()

        response = await servicer.GetPrice(request, context)

    assert Decimal(response.price) == Decimal("65000")
    resolved = captured["resolved_token"]
    assert isinstance(resolved, ResolvedToken)
    assert resolved.address.lower() == CBBTC_ADDRESS.lower()
    assert resolved.chain.value.lower() == "base"
    assert resolved.symbol == "cbBTC"


@pytest.mark.asyncio
async def test_getprice_address_without_chain_uses_primary_chain():
    """Request with empty chain falls back to settings.chains[0]."""
    settings = MagicMock()
    settings.chains = ["base"]
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)

    fake_metadata = TokenMetadata(
        symbol="cbBTC",
        name="Coinbase Wrapped BTC",
        decimals=8,
        address=CBBTC_ADDRESS,
        is_native=False,
    )
    fake_lookup = MagicMock()
    fake_lookup.lookup = AsyncMock(return_value=fake_metadata)

    with patch.object(servicer, "_get_onchain_lookup", new=AsyncMock(return_value=fake_lookup)):
        resolved = await servicer._resolve_token_for_pricing(CBBTC_ADDRESS, "")

    assert isinstance(resolved, ResolvedToken)
    assert resolved.symbol == "cbBTC"
    assert resolved.chain.value.lower() == "base"
    # The fake was called with the primary chain.
    fake_lookup.lookup.assert_awaited_once_with("base", CBBTC_ADDRESS)


@pytest.mark.asyncio
async def test_getprice_multi_chain_gateway_requires_explicit_chain():
    """A gateway serving multiple chains must NOT silently pick one when the
    request omits the chain hint — that would query the wrong RPC.

    Phase 2 (VIB-3259) tightens this to a hard contract: the resolver raises
    MultiChainAmbiguousPriceRequest so GetPrice can surface INVALID_ARGUMENT
    rather than silently cascading into "Unknown token"."""
    from almanak.gateway.services.market_service import MultiChainAmbiguousPriceRequest

    settings = MagicMock()
    settings.chains = ["base", "arbitrum"]  # multi-chain
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)

    # OnChainLookup must not be constructed or called; if it is, the test fails.
    fake_lookup = MagicMock()
    fake_lookup.lookup = AsyncMock(return_value=None)

    with patch.object(servicer, "_get_onchain_lookup", new=AsyncMock(return_value=fake_lookup)):
        with pytest.raises(MultiChainAmbiguousPriceRequest) as exc_info:
            await servicer._resolve_token_for_pricing(CBBTC_ADDRESS, "")

    # Error must mention both configured chains for debuggability.
    message = str(exc_info.value)
    assert "base" in message
    assert "arbitrum" in message
    assert "PriceRequest.chain" in message
    fake_lookup.lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_getprice_multi_chain_empty_chain_returns_invalid_argument():
    """End-to-end: multi-chain gateway + EVM address + empty chain must return
    gRPC INVALID_ARGUMENT, not a silent pricing miss."""
    import grpc

    settings = MagicMock()
    settings.chains = ["base", "arbitrum"]  # multi-chain
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)
    await servicer._ensure_initialized()

    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()

    # Empty chain + EVM address on a multi-chain gateway → must reject.
    request = gateway_pb2.PriceRequest(token=CBBTC_ADDRESS, quote="USD", chain="")
    response = await servicer.GetPrice(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert context.set_details.call_count == 1
    details = context.set_details.call_args.args[0]
    assert "Multi-chain gateway" in details
    assert "PriceRequest.chain" in details
    # Configured chains must be enumerated in the error for debuggability.
    assert "base" in details
    assert "arbitrum" in details
    assert response.price == ""


@pytest.mark.asyncio
async def test_getprice_multi_chain_symbol_token_keeps_fallthrough():
    """Multi-chain gateway + SYMBOL token (not an EVM address) + empty chain
    must still fall through to the normal symbol-based aggregator path.
    Only address-based lookups are tightened by Phase 2."""
    settings = MagicMock()
    settings.chains = ["base", "arbitrum"]  # multi-chain
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)

    resolved = await servicer._resolve_token_for_pricing("ETH", "")
    # Symbol token → no raise, no resolution (fall through to aggregator).
    assert resolved is None


@pytest.mark.asyncio
async def test_getprice_symbol_input_skips_address_resolution():
    """Symbol input (e.g. 'ETH') must not trigger OnChainLookup."""
    settings = MagicMock()
    settings.chains = ["base"]
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)
    resolved = await servicer._resolve_token_for_pricing("ETH", "base")
    assert resolved is None  # symbols skip the on-chain path entirely


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_chain", ["invalid-chain", "unknown", "bsae"])
async def test_getprice_rejects_invalid_chain_with_invalid_argument(bad_chain):
    """A non-empty but invalid chain is caller error and must surface as
    gRPC INVALID_ARGUMENT, not a silent pricing miss. Mirrors GetBalance /
    RpcService behavior and enforces the gateway's input-validation boundary."""
    import grpc

    settings = MagicMock()
    settings.chains = ["base"]
    settings.network = "mainnet"
    settings.coingecko_api_key = ""
    settings.enable_manual_price_overrides = False

    servicer = MarketServiceServicer(settings)
    await servicer._ensure_initialized()

    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()

    request = gateway_pb2.PriceRequest(token="ETH", quote="USD", chain=bad_chain)
    response = await servicer.GetPrice(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    # Error detail should mention the offending chain for debuggability.
    assert context.set_details.call_count == 1
    assert bad_chain in context.set_details.call_args.args[0] or "not allowed" in context.set_details.call_args.args[0]
    assert response.price == ""
