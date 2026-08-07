"""Dynamic GMX market discovery regression tests (VIB-6561)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from eth_abi import encode

from almanak.connectors._base.gateway_capabilities import (
    PerpMarketCatalogueUnavailable,
    PerpMarketRecord,
    PerpMarketVerificationError,
)
from almanak.connectors.gmx_v2.addresses import GMX_V2_MARKETS, GMX_V2_TOKENS
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.connectors.gmx_v2.gateway.market_registry import GmxV2MarketRegistry
from almanak.connectors.gmx_v2.market_metadata import (
    GmxMarketNotFound,
    ResolvedGmxMarket,
    resolve_market_via_gateway,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer

HYPE_MARKET = "0xBcb8FE13d02b023e8f94f6881Cc0192fd918A5C0"
HYPE_INDEX = "0xfDFA0A749dA3bCcee20aE0B4AD50E39B26F58f7C"
WBTC = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

MARKETS = {
    "markets": [
        {
            "name": "HYPE/USD [WBTC.b-USDC]",
            "marketToken": HYPE_MARKET,
            "indexToken": HYPE_INDEX,
            "longToken": WBTC,
            "shortToken": USDC,
            "isListed": True,
        }
    ]
}
TOKENS = {
    "tokens": [
        {"symbol": "HYPE", "address": HYPE_INDEX, "decimals": 8, "synthetic": True},
        {"symbol": "WBTC.b", "address": WBTC, "decimals": 8},
        {"symbol": "USDC", "address": USDC, "decimals": 6},
    ]
}


def _reader_result(*, market: str = HYPE_MARKET, index: str = HYPE_INDEX) -> str:
    return (
        "0x"
        + encode(
            ["(address,address,address,address)"],
            [(market, index, WBTC, USDC)],
        ).hex()
    )


def _record(*, verified: bool = True) -> PerpMarketRecord:
    return PerpMarketRecord(
        protocol="gmx_v2",
        chain="arbitrum",
        label="HYPE/USD",
        market_token=HYPE_MARKET.lower(),
        index_token=HYPE_INDEX.lower(),
        index_symbol="HYPE",
        index_token_decimals=8,
        long_token=WBTC.lower(),
        long_token_symbol="WBTC.b",
        short_token=USDC.lower(),
        short_token_symbol="USDC",
        verified=verified,
    )


def _service(provider: object) -> MarketServiceServicer:
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service._perp_market_discovery_providers = {"gmx_v2": provider}
    service.settings = SimpleNamespace(network="mainnet")
    return service


@pytest.mark.asyncio
async def test_hype_is_resolved_from_api_and_verified_onchain() -> None:
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[MARKETS, TOKENS])):
        record = await registry.resolve(
            chain="arbitrum",
            market="HYPE/USD",
            eth_call=AsyncMock(return_value=_reader_result()),
        )

    assert record is not None
    assert record.market_token == HYPE_MARKET.lower()
    assert record.index_symbol == "HYPE"
    assert record.index_token_decimals == 8
    assert record.long_token == WBTC.lower()
    assert record.short_token == USDC.lower()
    assert record.verified is True


@pytest.mark.asyncio
async def test_api_row_is_rejected_when_reader_tuple_disagrees() -> None:
    registry = GmxV2MarketRegistry()
    wrong_market = "0x" + "9" * 40
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[MARKETS, TOKENS])):
        with pytest.raises(PerpMarketVerificationError, match="API/on-chain market mismatch"):
            await registry.resolve(
                chain="arbitrum",
                market="HYPE/USD",
                eth_call=AsyncMock(return_value=_reader_result(market=wrong_market)),
            )


@pytest.mark.asyncio
async def test_missing_token_catalogue_is_retryable_unavailability() -> None:
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[MARKETS, {}])):
        with pytest.raises(PerpMarketCatalogueUnavailable, match="tokens list"):
            await registry.resolve(
                chain="arbitrum",
                market="HYPE/USD",
                eth_call=AsyncMock(return_value=_reader_result()),
            )


@pytest.mark.asyncio
async def test_full_name_lookup_does_not_poison_ambiguous_short_label_cache() -> None:
    second_market = "0x" + "7" * 40
    markets = {
        "markets": [
            MARKETS["markets"][0],
            {
                **MARKETS["markets"][0],
                "name": "HYPE/USD [HYPE-USDC]",
                "marketToken": second_market,
            },
        ]
    }
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[markets, TOKENS])):
        record = await registry.resolve(
            chain="arbitrum",
            market="HYPE/USD [WBTC.b-USDC]",
            eth_call=AsyncMock(return_value=_reader_result()),
        )

    assert record is not None
    assert ("arbitrum", "hype/usd") not in registry._cache
    with pytest.raises(ValueError, match="ambiguous"):
        registry._select_market(markets, "HYPE/USD")


@pytest.mark.asyncio
async def test_price_history_resolves_index_equivalent_markets_without_a_static_catalogue() -> None:
    """Collateral variants may share one candle plane after independent verification."""
    second_market = "0x" + "7" * 40
    markets = {
        "markets": [
            MARKETS["markets"][0],
            {
                **MARKETS["markets"][0],
                "name": "HYPE/USD [HYPE-USDC]",
                "marketToken": second_market,
            },
        ]
    }
    eth_call = AsyncMock(
        side_effect=[
            _reader_result(),
            _reader_result(market=second_market),
        ]
    )
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[markets, TOKENS])):
        record = await registry.resolve(
            chain="arbitrum",
            market="HYPE/USD",
            eth_call=eth_call,
            allow_delisted_address=False,
            allow_index_equivalent=True,
        )

    assert record is not None
    assert record.market_token == min(HYPE_MARKET, second_market).lower()
    assert record.index_token == HYPE_INDEX.lower()
    assert eth_call.await_count == 2


@pytest.mark.asyncio
async def test_price_history_refuses_ambiguous_distinct_index_identities() -> None:
    """A short label may never collapse two different price planes."""
    second_market = "0x" + "7" * 40
    second_index = "0x" + "6" * 40
    markets = {
        "markets": [
            MARKETS["markets"][0],
            {
                **MARKETS["markets"][0],
                "name": "HYPE/USD [ALT-USDC]",
                "marketToken": second_market,
                "indexToken": second_index,
            },
        ]
    }
    tokens = {
        "tokens": [
            *TOKENS["tokens"],
            {"symbol": "ALT", "address": second_index, "decimals": 8, "synthetic": True},
        ]
    }
    eth_call = AsyncMock(
        side_effect=[
            _reader_result(),
            _reader_result(market=second_market, index=second_index),
        ]
    )
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[markets, tokens])):
        with pytest.raises(ValueError, match="distinct index-price identities"):
            await registry.resolve(
                chain="arbitrum",
                market="HYPE/USD",
                eth_call=eth_call,
                allow_delisted_address=False,
                allow_index_equivalent=True,
            )


@pytest.mark.asyncio
async def test_exact_address_can_resolve_delisted_market_but_label_cannot() -> None:
    markets = {"markets": [{**MARKETS["markets"][0], "isListed": False}]}
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[markets, TOKENS])):
        record = await registry.resolve(
            chain="arbitrum",
            market=HYPE_MARKET,
            eth_call=AsyncMock(return_value=_reader_result()),
        )

    assert record is not None
    assert registry._select_market(markets, "HYPE/USD") is None


@pytest.mark.asyncio
async def test_raw_catalogue_is_reused_across_unknown_market_queries() -> None:
    registry = GmxV2MarketRegistry()
    get_json = AsyncMock(side_effect=[MARKETS, TOKENS])
    with patch.object(registry, "_get_json", get_json):
        assert await registry.resolve(chain="arbitrum", market="NOPE/USD", eth_call=AsyncMock()) is None
        assert await registry.resolve(chain="arbitrum", market="STILL-NOPE/USD", eth_call=AsyncMock()) is None

    assert get_json.await_count == 2


@pytest.mark.asyncio
async def test_market_service_routes_verified_perp_record_through_capability() -> None:
    provider = SimpleNamespace(
        resolve_perp_market=AsyncMock(return_value=_record()),
        perp_market_discovery_chains=lambda: frozenset({"arbitrum"}),
    )
    service = _service(provider)
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())
    eth_call = AsyncMock()

    with patch(
        "almanak.gateway.services.pt_rpc_adapter.build_gateway_eth_call",
        return_value=eth_call,
    ):
        response = await service.GetPerpMarket(
            gateway_pb2.GetPerpMarketRequest(protocol="gmx_v2", chain="arbitrum", market="HYPE/USD"),
            context,
        )

    assert response.success is True
    assert response.market.market_token == HYPE_MARKET.lower()
    provider.resolve_perp_market.assert_awaited_once_with(
        chain="arbitrum",
        market="HYPE/USD",
        eth_call=eth_call,
    )
    context.set_code.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "expected_code", "expected_error"),
    [
        (None, grpc.StatusCode.NOT_FOUND, "does not exist"),
        (_record(verified=False), grpc.StatusCode.FAILED_PRECONDITION, "without on-chain verification"),
    ],
)
async def test_market_service_refuses_missing_or_unverified_records(
    record: PerpMarketRecord | None,
    expected_code: grpc.StatusCode,
    expected_error: str,
) -> None:
    provider = SimpleNamespace(
        resolve_perp_market=AsyncMock(return_value=record),
        perp_market_discovery_chains=lambda: frozenset({"arbitrum"}),
    )
    service = _service(provider)
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    with patch("almanak.gateway.services.pt_rpc_adapter.build_gateway_eth_call", return_value=AsyncMock()):
        response = await service.GetPerpMarket(
            gateway_pb2.GetPerpMarketRequest(protocol="gmx_v2", chain="arbitrum", market="HYPE/USD"),
            context,
        )

    assert response.success is False
    assert expected_error in response.error
    context.set_code.assert_called_once_with(expected_code)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rpc_request", "expected_error"),
    [
        (gateway_pb2.GetPerpMarketRequest(protocol="gmx_v2", chain="arbitrum"), "required"),
        (
            gateway_pb2.GetPerpMarketRequest(protocol="unknown", chain="arbitrum", market="HYPE/USD"),
            "unsupported protocol",
        ),
        (
            gateway_pb2.GetPerpMarketRequest(protocol="gmx_v2", chain="base", market="HYPE/USD"),
            "does not support",
        ),
    ],
)
async def test_market_service_rejects_invalid_routing(
    rpc_request: gateway_pb2.GetPerpMarketRequest,
    expected_error: str,
) -> None:
    provider = SimpleNamespace(perp_market_discovery_chains=lambda: frozenset({"arbitrum"}))
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    response = await _service(provider).GetPerpMarket(rpc_request, context)

    assert response.success is False
    assert expected_error in response.error
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_code", "expected_error"),
    [
        (ValueError("ambiguous market"), grpc.StatusCode.INVALID_ARGUMENT, "ambiguous market"),
        (
            PerpMarketCatalogueUnavailable("tokens unavailable"),
            grpc.StatusCode.UNAVAILABLE,
            "tokens unavailable",
        ),
        (RuntimeError("upstream failed"), grpc.StatusCode.UNAVAILABLE, "discovery unavailable"),
    ],
)
async def test_market_service_maps_discovery_failures(
    error: Exception,
    expected_code: grpc.StatusCode,
    expected_error: str,
) -> None:
    provider = SimpleNamespace(
        resolve_perp_market=AsyncMock(side_effect=error),
        perp_market_discovery_chains=lambda: frozenset({"arbitrum"}),
    )
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    with patch("almanak.gateway.services.pt_rpc_adapter.build_gateway_eth_call", return_value=AsyncMock()):
        response = await _service(provider).GetPerpMarket(
            gateway_pb2.GetPerpMarketRequest(protocol="gmx_v2", chain="arbitrum", market="HYPE/USD"),
            context,
        )

    assert response.success is False
    assert expected_error in response.error
    context.set_code.assert_called_once_with(expected_code)


def test_compiler_uses_verified_dynamic_metadata_outside_static_catalog() -> None:
    response = gateway_pb2.PerpMarketResponse(
        success=True,
        market=gateway_pb2.PerpMarket(
            protocol="gmx_v2",
            chain="arbitrum",
            label="HYPE/USD",
            market_token=HYPE_MARKET,
            index_token=HYPE_INDEX,
            index_symbol="HYPE",
            index_token_decimals=8,
            long_token=WBTC,
            long_token_symbol="WBTC.b",
            short_token=USDC,
            short_token_symbol="USDC",
            verified=True,
        ),
    )
    market_stub = SimpleNamespace(GetPerpMarket=lambda request, timeout: response)
    gateway = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=5), market=market_stub)
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    compiler = GMXV2Compiler()
    resolved = compiler._resolve_market(ctx, SimpleNamespace(), "HYPE/USD", "intent-1")

    assert resolved == HYPE_MARKET
    assert compiler._index_symbol_for_market("arbitrum", HYPE_MARKET) == "HYPE"
    assert compiler._index_token_decimals("arbitrum", HYPE_MARKET) == 8


def test_compiler_disambiguates_curated_label_with_exact_address_query() -> None:
    requested_markets: list[str] = []
    eth_market = GMX_V2_MARKETS["arbitrum"]["ETH/USD"]

    def resolve_curated_address(request: object, timeout: float) -> gateway_pb2.PerpMarketResponse:
        del timeout
        requested_markets.append(request.market)  # type: ignore[attr-defined]
        return gateway_pb2.PerpMarketResponse(
            success=True,
            market=gateway_pb2.PerpMarket(
                protocol="gmx_v2",
                chain="arbitrum",
                label="ETH/USD",
                market_token=eth_market,
                index_token="0x" + "2" * 40,
                index_symbol="ETH",
                index_token_decimals=18,
                long_token=GMX_V2_TOKENS["arbitrum"]["WETH"],
                long_token_symbol="WETH",
                short_token=GMX_V2_TOKENS["arbitrum"]["USDC"],
                short_token_symbol="USDC",
                verified=True,
            ),
        )

    gateway = SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=resolve_curated_address),
    )
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    resolved = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), "ETH/USD", "intent-1")

    assert resolved == eth_market
    assert requested_markets == [eth_market]


def test_connected_old_gateway_falls_back_to_curated_static_market() -> None:
    gateway = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=5), market=SimpleNamespace())
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    resolved = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), "ETH/USD", "intent-1")

    assert resolved == GMX_V2_MARKETS["arbitrum"]["ETH/USD"]


@pytest.mark.parametrize("configured_timeout", [None, "slow", False, 0, float("inf")])
def test_gateway_market_resolution_defaults_invalid_timeouts(configured_timeout: object) -> None:
    response = gateway_pb2.PerpMarketResponse(
        success=True,
        market=gateway_pb2.PerpMarket(
            protocol="gmx_v2",
            chain="arbitrum",
            label="HYPE/USD",
            market_token=HYPE_MARKET,
            index_token=HYPE_INDEX,
            index_symbol="HYPE",
            index_token_decimals=8,
            long_token=WBTC,
            long_token_symbol="WBTC.b",
            short_token=USDC,
            short_token_symbol="USDC",
            verified=True,
        ),
    )
    stub = Mock(return_value=response)
    gateway = SimpleNamespace(
        config=SimpleNamespace(timeout=configured_timeout), market=SimpleNamespace(GetPerpMarket=stub)
    )

    resolve_market_via_gateway(gateway, chain="arbitrum", market="HYPE/USD")

    assert stub.call_args.kwargs["timeout"] == 30.0


@pytest.mark.parametrize("market", ["ETH/USD", GMX_V2_MARKETS["arbitrum"]["ETH/USD"]])
def test_gateway_not_found_falls_back_to_curated_static_market(market: str) -> None:
    class NotFound(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.NOT_FOUND

    def missing_market(request: object, timeout: float) -> object:
        del request, timeout
        raise NotFound()

    gateway = SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=missing_market),
    )
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    resolved = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), market, "intent-1")

    assert resolved == GMX_V2_MARKETS["arbitrum"]["ETH/USD"]


def test_connector_collateral_alias_precedes_generic_symbol_resolution() -> None:
    wrong_usdc = SimpleNamespace(address="0x" + "1" * 40)
    ctx = SimpleNamespace(
        chain="arbitrum",
        token_resolver=object(),
        services=SimpleNamespace(resolve_token=lambda token: wrong_usdc),
    )

    resolved = GMXV2Compiler()._resolve_collateral(ctx, "USDC", "intent-1")

    assert resolved == GMX_V2_TOKENS["arbitrum"]["USDC"]


def test_receipt_parser_override_is_only_emitted_for_valid_decimals() -> None:
    from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

    assert GMXv2ReceiptParser.build_extract_kwargs(field="perp_fill", bundle_metadata={"index_token_decimals": 8}) == {
        "index_token_decimals_override": 8
    }
    assert (
        GMXv2ReceiptParser.build_extract_kwargs(field="perp_fill", bundle_metadata={"index_token_decimals": None}) == {}
    )


def test_managed_anvil_keeper_seeds_unlisted_market_from_verified_metadata() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as executor

    metadata = ResolvedGmxMarket(
        label="HYPE/USD",
        market_token=HYPE_MARKET,
        index_token=HYPE_INDEX,
        index_symbol="HYPE",
        index_token_decimals=8,
        long_token=WBTC,
        long_token_symbol="WBTC.b",
        short_token=USDC,
        short_token_symbol="USDC",
    )
    with (
        patch.object(executor, "resolve_market_via_gateway", return_value=metadata),
        patch.object(executor, "_gateway_usd_price", return_value=57),
    ):
        minimum, maximum = executor._index_price_bounds(SimpleNamespace(), "arbitrum", "HYPE/USD")

    assert minimum == maximum == 57 * 10**22


def test_managed_anvil_keeper_uses_static_fallback_when_dynamic_market_is_not_found() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as executor

    with (
        patch.object(executor, "resolve_market_via_gateway", side_effect=GmxMarketNotFound("missing")),
        patch.object(executor, "_gateway_usd_price", return_value=3_000),
    ):
        minimum, maximum = executor._index_price_bounds(
            SimpleNamespace(), "arbitrum", GMX_V2_MARKETS["arbitrum"]["ETH/USD"]
        )

    assert minimum == maximum == 3_000 * 10**12
