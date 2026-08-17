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
from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.connectors.gmx_v2.gateway.market_registry import GmxV2MarketRegistry
from almanak.connectors.gmx_v2.market_metadata import (
    GmxMarketDiscoveryUnavailable,
    GmxMarketNotFound,
    ResolvedGmxMarket,
    resolve_market_via_gateway,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer
from tests.unit.connectors.gmx_v2.market_fixtures import (
    market_address,
    market_record,
    prime_catalog,
)

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
        long_token_decimals=8,
        short_token=USDC.lower(),
        short_token_symbol="USDC",
        short_token_decimals=6,
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
    assert record.long_token_decimals == 8
    assert record.short_token == USDC.lower()
    assert record.short_token_decimals == 6
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
async def test_missing_collateral_token_metadata_is_retryable_unavailability() -> None:
    registry = GmxV2MarketRegistry()
    tokens_without_long = {"tokens": [token for token in TOKENS["tokens"] if token["address"] != WBTC]}
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[MARKETS, tokens_without_long])):
        with pytest.raises(PerpMarketCatalogueUnavailable, match="long-token metadata"):
            await registry.resolve(
                chain="arbitrum",
                market="HYPE/USD",
                eth_call=AsyncMock(return_value=_reader_result()),
            )


@pytest.mark.parametrize(
    ("token_address", "bad_decimals", "missing_metadata"),
    [
        pytest.param(WBTC, True, "long-token metadata", id="long-true"),
        pytest.param(USDC, False, "short-token metadata", id="short-false"),
    ],
)
@pytest.mark.asyncio
async def test_boolean_collateral_decimals_are_rejected(
    token_address: str,
    bad_decimals: bool,
    missing_metadata: str,
) -> None:
    tokens = {
        "tokens": [
            {**token, "decimals": bad_decimals} if token["address"] == token_address else token
            for token in TOKENS["tokens"]
        ]
    }
    registry = GmxV2MarketRegistry()

    with patch.object(registry, "_get_json", AsyncMock(side_effect=[MARKETS, tokens])):
        with pytest.raises(PerpMarketCatalogueUnavailable, match=missing_metadata):
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


def test_stale_verified_record_returns_none_for_empty_history() -> None:
    registry = GmxV2MarketRegistry()

    assert registry._stale_verified_record("arbitrum", HYPE_MARKET, True, RuntimeError("down")) is None


def test_stale_verified_record_refuses_listing_sensitive_callers() -> None:
    registry = GmxV2MarketRegistry()
    registry._verified_history[("arbitrum", HYPE_MARKET.lower())] = _record()

    assert registry._stale_verified_record("arbitrum", HYPE_MARKET, False, RuntimeError("down")) is None


def test_stale_verified_record_serves_address_query_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    registry = GmxV2MarketRegistry()
    record = _record()
    registry._verified_history[("arbitrum", HYPE_MARKET.lower())] = record

    with caplog.at_level("WARNING", logger="almanak.connectors.gmx_v2.gateway.market_registry"):
        stale = registry._stale_verified_record("arbitrum", HYPE_MARKET, True, RuntimeError("down"))

    assert stale is record
    assert any("serving last verified record" in message for message in caplog.messages)


def test_stale_verified_record_ignores_label_queries() -> None:
    """The no-TTL history is keyed by market-token address only."""
    registry = GmxV2MarketRegistry()
    registry._verified_history[("arbitrum", HYPE_MARKET.lower())] = _record()

    assert registry._stale_verified_record("arbitrum", "HYPE/USD", True, RuntimeError("down")) is None


@pytest.mark.asyncio
async def test_resolve_serves_stale_verified_address_through_catalogue_outage() -> None:
    """End-to-end: the except/return-stale path in resolve() itself."""
    registry = GmxV2MarketRegistry()
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[MARKETS, TOKENS])):
        first = await registry.resolve(
            chain="arbitrum",
            market=HYPE_MARKET,
            eth_call=AsyncMock(return_value=_reader_result()),
        )
    assert first is not None
    registry._cache.clear()
    registry._catalog_cache.clear()

    with patch.object(registry, "_get_json", AsyncMock(side_effect=RuntimeError("catalogue down"))):
        stale = await registry.resolve(chain="arbitrum", market=HYPE_MARKET, eth_call=AsyncMock())
        with pytest.raises(RuntimeError, match="catalogue down"):
            await registry.resolve(chain="arbitrum", market="HYPE/USD", eth_call=AsyncMock())

    assert stale is first


def test_require_single_index_identity_accepts_single_record() -> None:
    GmxV2MarketRegistry._require_single_index_identity("HYPE/USD", [_record()])


def test_require_single_index_identity_accepts_shared_identity() -> None:
    from dataclasses import replace

    second = replace(_record(), market_token=("0x" + "7" * 40))

    GmxV2MarketRegistry._require_single_index_identity("HYPE/USD", [_record(), second])


def test_require_single_index_identity_normalises_symbol_and_address_case() -> None:
    """Case-only differences are the SAME identity — the check normalises with
    ``index_symbol.upper()`` / ``index_token.lower()`` and must accept, not
    refuse (review pin: the normalisation itself was previously untested)."""
    from dataclasses import replace

    base = _record()
    case_variant = replace(
        base,
        market_token=("0x" + "7" * 40),
        index_symbol=base.index_symbol.lower(),
        index_token=base.index_token.upper().replace("0X", "0x"),
    )

    GmxV2MarketRegistry._require_single_index_identity("HYPE/USD", [base, case_variant])


@pytest.mark.parametrize(
    "variant",
    [
        pytest.param({"index_token": "0x" + "6" * 40}, id="index-token"),
        pytest.param({"index_symbol": "ALT"}, id="index-symbol"),
        pytest.param({"index_token_decimals": 18}, id="index-decimals"),
    ],
)
def test_require_single_index_identity_refuses_distinct_identities(variant: dict[str, object]) -> None:
    from dataclasses import replace

    second = replace(_record(), market_token=("0x" + "7" * 40), **variant)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="ambiguous across distinct index-price identities"):
        GmxV2MarketRegistry._require_single_index_identity("HYPE/USD", [_record(), second])


def test_remember_verified_round_trips_all_three_aliases() -> None:
    registry = GmxV2MarketRegistry()
    record = _record()
    selected = registry._matching_markets(MARKETS, "HYPE/USD")[0]

    registry._remember_verified("arbitrum", "1:0", "HYPE/USD", selected, record)

    for alias in ("HYPE/USD", selected.name, selected.market_token):
        assert registry._cached_record(("arbitrum", f"1:0:{alias.lower()}")) is record
    assert registry._verified_history[("arbitrum", record.market_token.lower())] is record


def test_cached_record_misses_expired_entries_and_foreign_scopes() -> None:
    import time

    registry = GmxV2MarketRegistry()
    record = _record()
    selected = registry._matching_markets(MARKETS, "HYPE/USD")[0]
    registry._remember_verified("arbitrum", "1:0", "HYPE/USD", selected, record)

    assert registry._cached_record(("arbitrum", "0:0:hype/usd")) is None
    registry._cache[("arbitrum", "1:0:hype/usd")] = (time.monotonic() - 1.0, record)
    assert registry._cached_record(("arbitrum", "1:0:hype/usd")) is None


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
        require_listed=False,
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
            PerpMarketVerificationError("API/on-chain market mismatch"),
            grpc.StatusCode.FAILED_PRECONDITION,
            "API/on-chain market mismatch",
        ),
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
            long_token_decimals=8,
            short_token=USDC,
            short_token_symbol="USDC",
            short_token_decimals=6,
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


@pytest.mark.parametrize(
    ("market_input", "expected_query"),
    [
        pytest.param("ETH/USD", "ETH/USD", id="label"),
        pytest.param("eth-usd", "ETH/USD", id="label-alias"),
        pytest.param(
            market_address("arbitrum", "ETH/USD"),
            market_address("arbitrum", "ETH/USD"),
            id="address",
        ),
    ],
)
def test_compiler_forwards_market_input_verbatim_to_dynamic_resolution(market_input: str, expected_query: str) -> None:
    """Successor of the curated-label disambiguation test (address-first).

    The compiler no longer translates anything through a curated table before
    querying the gateway: the canonicalised strategy input IS the query, for
    labels and addresses alike. Disambiguation of ambiguous labels belongs to
    the dynamic registry (exact API ``name`` pinning), so the compiler must not
    editorialise the input on the way in — and a successful resolution must be
    remembered in the process catalog for later gateway-less compiles.
    """
    requested_markets: list[str] = []
    record = market_record("arbitrum", "ETH/USD")

    def resolve_dynamic(request: object, timeout: float) -> gateway_pb2.PerpMarketResponse:
        del timeout
        requested_markets.append(request.market)  # type: ignore[attr-defined]
        return gateway_pb2.PerpMarketResponse(
            success=True,
            market=gateway_pb2.PerpMarket(
                protocol="gmx_v2",
                chain="arbitrum",
                label=record.label,
                market_token=record.market_token,
                index_token=record.index_token,
                index_symbol=record.index_symbol,
                index_token_decimals=record.index_token_decimals,
                long_token=record.long_token,
                long_token_symbol=record.long_token_symbol,
                long_token_decimals=record.long_token_decimals,
                short_token=record.short_token,
                short_token_symbol=record.short_token_symbol,
                short_token_decimals=record.short_token_decimals,
                verified=True,
            ),
        )

    gateway = SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=resolve_dynamic),
    )
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    resolved = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), market_input, "intent-1")

    assert resolved == record.market_token
    assert requested_markets == [expected_query]
    assert market_catalog.by_address("arbitrum", record.market_token) is not None


def test_open_requires_listed_and_close_does_not() -> None:
    """Listing policy (review P1): the OPEN leg demands CURRENT listing.

    ``GetPerpMarket`` now carries ``require_listed``; a risk-increasing
    resolution must set it (a delisted market's createOrder succeeds on-chain
    but the keeper cancels and keeps the fee), while the risk-reducing close
    keeps the historic delisted/stale grace.
    """
    seen_flags: list[bool] = []
    record = market_record("arbitrum", "ETH/USD")

    def resolve_dynamic(request: object, timeout: float) -> gateway_pb2.PerpMarketResponse:
        del timeout
        seen_flags.append(bool(request.require_listed))  # type: ignore[attr-defined]
        return gateway_pb2.PerpMarketResponse(
            success=True,
            market=gateway_pb2.PerpMarket(
                protocol="gmx_v2",
                chain="arbitrum",
                label=record.label,
                market_token=record.market_token,
                index_token=record.index_token,
                index_symbol=record.index_symbol,
                index_token_decimals=record.index_token_decimals,
                long_token=record.long_token,
                long_token_symbol=record.long_token_symbol,
                long_token_decimals=record.long_token_decimals,
                short_token=record.short_token,
                short_token_symbol=record.short_token_symbol,
                short_token_decimals=record.short_token_decimals,
                verified=True,
            ),
        )

    gateway = SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=resolve_dynamic),
    )
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)
    compiler = GMXV2Compiler()

    compiler._resolve_market(ctx, SimpleNamespace(), record.market_token, "intent-1", require_listed=True)
    compiler._resolve_market(ctx, SimpleNamespace(), record.market_token, "intent-2")

    assert seen_flags == [True, False]


def test_catalog_fallback_is_close_only() -> None:
    """A remembered verification cannot prove CURRENT listing (review P1).

    With the dynamic surface unavailable, the process-catalog fallback serves
    only risk-reducing resolutions: the CLOSE resolves from the remembered
    verification, while the OPEN fails closed (transiently — an API blip or
    old gateway heals on retry) rather than compiling an increase whose
    keeper fee a delisted market would burn.
    """
    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
    gateway = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=5), market=SimpleNamespace())
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)
    compiler = GMXV2Compiler()
    address = market_address("arbitrum", "ETH/USD")

    close_resolved = compiler._resolve_market(ctx, SimpleNamespace(), address, "intent-close")
    assert close_resolved == address

    open_result = compiler._resolve_market(ctx, SimpleNamespace(), address, "intent-open", require_listed=True)
    assert isinstance(open_result, CompilationResult)
    assert open_result.status == CompilationStatus.FAILED
    assert open_result.is_transient is True


def test_connected_old_gateway_resolves_verified_address_from_catalog() -> None:
    """Successor of the old-gateway curated-fallback test (address-first).

    A connected gateway without ``GetPerpMarket`` makes dynamic resolution
    UNAVAILABLE, and there is no curated table behind it any more. The only
    market that may still resolve is an ADDRESS this process has already
    verified (open → close, retry, teardown): the address names an immutable
    on-chain tuple, so the remembered verification is still good.
    """
    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
    gateway = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=5), market=SimpleNamespace())
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    resolved = GMXV2Compiler()._resolve_market(
        ctx, SimpleNamespace(), market_address("arbitrum", "ETH/USD"), "intent-1"
    )

    assert resolved == market_address("arbitrum", "ETH/USD")


def test_connected_old_gateway_unverified_address_fails_closed_as_transient() -> None:
    """Catalog miss under an old gateway must FAIL closed — transiently.

    The other half of the old-gateway successor contract: with no curated
    fallback, an address this process never verified cannot be traded. The
    refusal is transient because unavailability of the dynamic surface (an old
    gateway binary, an API blip) heals on retry/redeploy — unlike a market the
    venue authoritatively does not know.
    """
    gateway = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=5), market=SimpleNamespace())
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=gateway, permission_discovery=False)

    result = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), market_address("arbitrum", "ETH/USD"), "intent-1")

    assert isinstance(result, CompilationResult)
    assert result.status == CompilationStatus.FAILED
    assert result.is_transient is True
    assert "not verified" in (result.error or "")


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
            long_token_decimals=8,
            short_token=USDC,
            short_token_symbol="USDC",
            short_token_decimals=6,
            verified=True,
        ),
    )
    stub = Mock(return_value=response)
    gateway = SimpleNamespace(
        config=SimpleNamespace(timeout=configured_timeout), market=SimpleNamespace(GetPerpMarket=stub)
    )

    resolve_market_via_gateway(gateway, chain="arbitrum", market="HYPE/USD")

    assert stub.call_args.kwargs["timeout"] == 30.0


@pytest.mark.parametrize(
    "missing_field",
    ["index_token_decimals", "long_token_decimals", "short_token_decimals"],
)
def test_old_gateway_without_token_decimals_fails_closed(missing_field: str) -> None:
    market_fields = {
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "label": "HYPE/USD",
        "market_token": HYPE_MARKET,
        "index_token": HYPE_INDEX,
        "index_symbol": "HYPE",
        "index_token_decimals": 8,
        "long_token": WBTC,
        "long_token_symbol": "WBTC.b",
        "long_token_decimals": 8,
        "short_token": USDC,
        "short_token_symbol": "USDC",
        "short_token_decimals": 6,
        "verified": True,
    }
    del market_fields[missing_field]
    response = gateway_pb2.PerpMarketResponse(
        success=True,
        market=gateway_pb2.PerpMarket(**market_fields),
    )
    gateway = SimpleNamespace(
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=lambda request, timeout: response),
    )

    with pytest.raises(GmxMarketDiscoveryUnavailable, match=missing_field):
        resolve_market_via_gateway(gateway, chain="arbitrum", market="HYPE/USD")


def _not_found_gateway() -> SimpleNamespace:
    """A connected gateway whose venue catalogue has no row for any query."""

    class NotFound(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.NOT_FOUND

    def missing_market(request: object, timeout: float) -> object:
        del request, timeout
        raise NotFound()

    return SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=missing_market),
    )


def test_gateway_not_found_verified_address_resolves_from_catalog() -> None:
    """Successor of the NOT_FOUND curated-fallback test, address leg.

    NOT_FOUND now means the venue API dropped the row (delisting) — but a
    market-token address names an immutable on-chain tuple, so an address this
    process already verified still resolves from the catalog. This is what
    keeps the close/teardown path off the venue API's uptime.
    """
    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=_not_found_gateway(), permission_discovery=False)

    resolved = GMXV2Compiler()._resolve_market(
        ctx, SimpleNamespace(), market_address("arbitrum", "ETH/USD"), "intent-1"
    )

    assert resolved == market_address("arbitrum", "ETH/USD")


def test_gateway_not_found_unverified_address_fails_closed_non_transient() -> None:
    """NOT_FOUND + unverified address must FAIL closed — and NOT transiently.

    NOT_FOUND is an authoritative venue answer, not an outage: retrying the
    same never-verified address keeps failing here, loudly, on every attempt,
    so classifying it transient would spin the retry ladder for nothing.
    """
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=_not_found_gateway(), permission_discovery=False)

    result = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), market_address("arbitrum", "ETH/USD"), "intent-1")

    assert isinstance(result, CompilationResult)
    assert result.status == CompilationStatus.FAILED
    assert result.is_transient is False
    assert "not verified" in (result.error or "")


def test_gateway_not_found_core_label_has_no_static_runtime_alias() -> None:
    """Even a former core label fails closed after a venue NOT_FOUND."""
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=_not_found_gateway(), permission_discovery=False)

    result = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), "ETH/USD", "intent-1")

    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.is_transient is False
    assert "No static runtime market aliases" in (result.error or "")


def test_gateway_not_found_non_core_label_fails_with_address_first_guidance() -> None:
    """A non-core label after a dynamic miss is refused with address-first wording.

    SOL/USD used to resolve from the curated table; now the error must teach
    the caller the successor contract — supply the market-token address — and
    a venue NOT_FOUND makes the refusal non-transient (see the address leg).
    """
    ctx = SimpleNamespace(chain="arbitrum", gateway_client=_not_found_gateway(), permission_discovery=False)

    result = GMXV2Compiler()._resolve_market(ctx, SimpleNamespace(), "SOL/USD", "intent-1")

    assert isinstance(result, CompilationResult)
    assert result.status == CompilationStatus.FAILED
    assert result.is_transient is False
    assert "address-first" in (result.error or "")


def test_verified_market_collateral_precedes_generic_symbol_resolution() -> None:
    record = market_record("arbitrum", "ETH/USD")
    prime_catalog(record, chain="arbitrum")
    wrong_usdc = SimpleNamespace(address="0x" + "1" * 40)
    ctx = SimpleNamespace(
        chain="arbitrum",
        permission_discovery=False,
        token_resolver=object(),
        services=SimpleNamespace(resolve_token=lambda token: wrong_usdc),
    )

    resolved = GMXV2Compiler()._resolve_collateral(
        ctx,
        "USDC",
        "intent-1",
        market_address=record.market_token,
    )

    assert resolved == record.short_token


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
        long_token_decimals=8,
        short_token=USDC,
        short_token_symbol="USDC",
        short_token_decimals=6,
    )
    with (
        patch.object(executor, "resolve_market_via_gateway", return_value=metadata),
        patch.object(executor, "_gateway_usd_price", return_value=57),
    ):
        minimum, maximum = executor._index_price_bounds(SimpleNamespace(), "arbitrum", "HYPE/USD")

    assert minimum == maximum == 57 * 10**22


def test_managed_anvil_keeper_uses_catalog_fallback_when_dynamic_market_is_not_found() -> None:
    """Successor to the static-table fallback (address-first): the keeper's
    oracle seeding falls back to the process's venue-verified catalog when
    dynamic resolution has no row — a market this process compiled is always
    seedable, table or no table."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as executor

    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
    with (
        patch.object(executor, "resolve_market_via_gateway", side_effect=GmxMarketNotFound("missing")),
        patch.object(executor, "_gateway_usd_price", return_value=3_000),
    ):
        minimum, maximum = executor._index_price_bounds(
            SimpleNamespace(), "arbitrum", market_address("arbitrum", "ETH/USD")
        )

    assert minimum == maximum == 3_000 * 10**12


def test_managed_anvil_keeper_fails_loud_when_market_is_unverified() -> None:
    """No catalog row + no dynamic row = a loud seeding error, never a guess."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as executor
    from almanak.connectors.gmx_v2.anvil_order_executor import GmxAnvilOrderExecutionError

    with (
        patch.object(executor, "resolve_market_via_gateway", side_effect=GmxMarketNotFound("missing")),
        patch.object(executor, "_gateway_usd_price", return_value=3_000),
        pytest.raises(GmxAnvilOrderExecutionError, match="no verified metadata"),
    ):
        executor._index_price_bounds(SimpleNamespace(), "arbitrum", market_address("arbitrum", "ETH/USD"))
