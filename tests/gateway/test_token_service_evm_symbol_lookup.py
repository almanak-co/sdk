"""Behavioral contract for the EVM symbol-resolution ladder."""

from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

import almanak.gateway.services.token_service as token_service_module
from almanak.framework.data.tokens import TokenNotFoundError
from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dexscreener_lookup import DexScreenerError, DexScreenerResult
from almanak.gateway.services.onchain_lookup import TokenMetadata
from almanak.gateway.services.token_service import COINGECKO_PLATFORM_IDS, TokenServiceServicer

_PROTOCOL_PREDICATES = (
    "_looks_like_pendle_symbol",
    "_looks_like_aave_symbol",
    "_looks_like_compound_symbol",
    "_looks_like_beefy_symbol",
    "_looks_like_yearn_symbol",
    "_looks_like_fluid_symbol",
)

_LOOKUP_METHODS = (
    "_try_pendle_symbol_lookup",
    "_try_aave_symbol_lookup",
    "_try_compound_symbol_lookup",
    "_try_beefy_symbol_lookup",
    "_try_yearn_symbol_lookup",
    "_try_fluid_symbol_lookup",
    "_try_morpho_symbol_lookup",
    "_try_coingecko_symbol_lookup",
    "_try_dexscreener_symbol_lookup",
)

_ADDRESS = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def service() -> TokenServiceServicer:
    instance = TokenServiceServicer(GatewaySettings())
    instance._resolver = MagicMock()
    return instance


def _response(source: str = "test") -> gateway_pb2.TokenMetadataResponse:
    return gateway_pb2.TokenMetadataResponse(
        success=True,
        symbol="TOKEN",
        address=_ADDRESS,
        decimals=18,
        name="Token",
        source=source,
    )


def _metadata(symbol: str = "TOKEN", name: str | None = "Token") -> TokenMetadata:
    return TokenMetadata(
        symbol=symbol,
        name=name,
        decimals=18,
        address=_ADDRESS,
        is_native=False,
    )


@pytest.mark.parametrize("hit_index", range(len(_LOOKUP_METHODS)))
@pytest.mark.asyncio
async def test_tier_table_preserves_exact_first_match_order(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
    hit_index: int,
) -> None:
    calls: list[tuple[str, str, str]] = []
    expected = _response()

    for predicate_name in _PROTOCOL_PREDICATES:
        monkeypatch.setattr(token_service_module, predicate_name, lambda _symbol: True)

    for index, method_name in enumerate(_LOOKUP_METHODS):
        result = expected if index == hit_index else None

        async def lookup(
            symbol: str,
            chain: str,
            *,
            _method_name: str = method_name,
            _result: gateway_pb2.TokenMetadataResponse | None = result,
        ) -> gateway_pb2.TokenMetadataResponse | None:
            calls.append((_method_name, symbol, chain))
            return _result

        monkeypatch.setattr(service, method_name, lookup)

    result = await service._try_evm_symbol_lookup("TOKEN", "ArBiTrUm")

    assert result is expected
    assert calls == [(name, "TOKEN", "ArBiTrUm") for name in _LOOKUP_METHODS[: hit_index + 1]]


@pytest.mark.asyncio
async def test_non_protocol_symbol_skips_gated_lookups_then_exhausts_fallbacks(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for predicate_name in _PROTOCOL_PREDICATES:
        monkeypatch.setattr(token_service_module, predicate_name, lambda _symbol: False)

    for method_name in _LOOKUP_METHODS:
        monkeypatch.setattr(service, method_name, AsyncMock(return_value=None))

    assert await service._try_evm_symbol_lookup("TOKEN", "arbitrum") is None
    for method_name in _LOOKUP_METHODS[:6]:
        getattr(service, method_name).assert_not_awaited()
    for method_name in _LOOKUP_METHODS[6:]:
        getattr(service, method_name).assert_awaited_once_with("TOKEN", "arbitrum")


@pytest.mark.asyncio
async def test_public_resolve_token_returns_dynamic_result_unchanged(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _response("dexscreener_dynamic")
    service._resolver.resolve.side_effect = TokenNotFoundError(
        token="TOKEN",
        chain="arbitrum",
        reason="not found",
    )
    monkeypatch.setattr(service, "_try_evm_symbol_lookup", AsyncMock(return_value=expected))
    context = MagicMock()

    result = await service.ResolveToken(
        gateway_pb2.ResolveTokenRequest(token="TOKEN", chain="arbitrum"),
        context,
    )

    assert result is expected
    service._try_evm_symbol_lookup.assert_awaited_once_with("TOKEN", "arbitrum")
    context.set_code.assert_not_called()
    context.set_details.assert_not_called()


@pytest.mark.asyncio
async def test_coingecko_success_preserves_address_chain_and_normalized_metadata(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = _metadata("MKR", "Maker")
    find_address = AsyncMock(return_value=_ADDRESS)
    confirm_address = AsyncMock(return_value=metadata)
    cache = MagicMock()
    monkeypatch.setattr(service, "_coingecko_find_address", find_address)
    monkeypatch.setattr(service, "_confirm_address_on_chain", confirm_address)
    monkeypatch.setattr(service, "_cache_discovered_token", cache)

    result = await service._try_coingecko_symbol_lookup("MKR", "ArBiTrUm")

    assert result == gateway_pb2.TokenMetadataResponse(
        success=True,
        symbol="MKR",
        address=_ADDRESS,
        decimals=18,
        name="Maker",
        source="coingecko_dynamic",
    )
    find_address.assert_awaited_once_with("MKR", COINGECKO_PLATFORM_IDS["arbitrum"])
    confirm_address.assert_awaited_once_with(_ADDRESS, "ArBiTrUm", expected_symbol="MKR")
    cache.assert_called_once_with(metadata, "ArBiTrUm", source="coingecko_dynamic")


@pytest.mark.parametrize(
    "outcome",
    [
        "missing_address",
        "missing_metadata",
        "address_lookup_error",
        "confirmation_timeout",
        "cache_error",
        "response_error",
    ],
)
@pytest.mark.asyncio
async def test_coingecko_malformed_and_failure_outcomes_are_fail_soft(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    find_address = AsyncMock(return_value=_ADDRESS)
    confirm_address = AsyncMock(return_value=_metadata())
    cache = MagicMock()
    to_response = MagicMock(return_value=_response("coingecko_dynamic"))
    monkeypatch.setattr(service, "_coingecko_find_address", find_address)
    monkeypatch.setattr(service, "_confirm_address_on_chain", confirm_address)
    monkeypatch.setattr(service, "_cache_discovered_token", cache)
    monkeypatch.setattr(service, "_metadata_to_response", to_response)

    if outcome == "missing_address":
        find_address.return_value = None
    elif outcome == "missing_metadata":
        confirm_address.return_value = None
    elif outcome == "address_lookup_error":
        find_address.side_effect = RuntimeError("malformed provider response")
    elif outcome == "confirmation_timeout":
        confirm_address.side_effect = TimeoutError("RPC timeout")
    elif outcome == "cache_error":
        cache.side_effect = RuntimeError("cache unavailable")
    else:
        to_response.side_effect = TypeError("malformed metadata")

    assert await service._try_coingecko_symbol_lookup("TOKEN", "arbitrum") is None


@pytest.mark.asyncio
async def test_coingecko_unsupported_chain_skips_external_calls(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    find_address = AsyncMock()
    monkeypatch.setattr(service, "_coingecko_find_address", find_address)

    assert await service._try_coingecko_symbol_lookup("TOKEN", "not-listed") is None
    find_address.assert_not_awaited()


@pytest.mark.parametrize(
    ("onchain_symbol", "expected_symbol", "accepted"),
    [
        ("TOKEN", "TOKEN", True),
        (" token ", "ToKeN", True),
        ("OTHER", "TOKEN", False),
        ("", "TOKEN", False),
        (None, "TOKEN", False),
    ],
)
@pytest.mark.asyncio
async def test_onchain_confirmation_normalizes_or_rejects_rpc_symbols(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
    onchain_symbol: str | None,
    expected_symbol: str,
    accepted: bool,
) -> None:
    metadata = _metadata()
    metadata.symbol = onchain_symbol  # type: ignore[assignment]
    lookup = MagicMock()
    lookup.lookup = AsyncMock(return_value=metadata)
    get_lookup = AsyncMock(return_value=lookup)
    service._rate_limiter = MagicMock()
    service._rate_limiter.wait_and_acquire = AsyncMock(return_value=True)
    monkeypatch.setattr(service, "_get_onchain_lookup", get_lookup)

    result = await service._confirm_address_on_chain(
        _ADDRESS,
        "ArBiTrUm",
        expected_symbol=expected_symbol,
    )

    assert (result is metadata) is accepted
    service._rate_limiter.wait_and_acquire.assert_awaited_once_with(timeout=2.0)
    get_lookup.assert_awaited_once_with("ArBiTrUm")
    lookup.lookup.assert_awaited_once_with("ArBiTrUm", _ADDRESS)


@pytest.mark.parametrize("rpc_outcome", [None, TimeoutError("timeout"), RuntimeError("malformed ABI")])
@pytest.mark.asyncio
async def test_onchain_confirmation_rpc_failures_return_none(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
    rpc_outcome: Exception | None,
) -> None:
    lookup = MagicMock()
    if rpc_outcome is None:
        lookup.lookup = AsyncMock(return_value=None)
    else:
        lookup.lookup = AsyncMock(side_effect=rpc_outcome)
    service._rate_limiter = MagicMock()
    service._rate_limiter.wait_and_acquire = AsyncMock(return_value=True)
    monkeypatch.setattr(service, "_get_onchain_lookup", AsyncMock(return_value=lookup))

    assert await service._confirm_address_on_chain(_ADDRESS, "arbitrum", expected_symbol="TOKEN") is None


@pytest.mark.asyncio
async def test_dexscreener_success_preserves_gate_address_chain_and_metadata(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dex_result = DexScreenerResult(
        address=_ADDRESS,
        chain="arbitrum",
        symbol="TOKEN",
        liquidity_usd=50_000.0,
        volume_24h_usd=5_000.0,
        pair_url=None,
    )
    find_address = AsyncMock(return_value=dex_result)
    metadata = _metadata()
    confirm_address = AsyncMock(return_value=metadata)
    cache = MagicMock()
    monkeypatch.setattr(token_service_module, "dexscreener_find_token_address", find_address)
    monkeypatch.setattr(service, "_confirm_address_on_chain", confirm_address)
    monkeypatch.setattr(service, "_cache_discovered_token", cache)

    result = await service._try_dexscreener_symbol_lookup("TOKEN", "arbitrum")

    assert result == gateway_pb2.TokenMetadataResponse(
        success=True,
        symbol="TOKEN",
        address=_ADDRESS,
        decimals=18,
        name="Token",
        source="dexscreener_dynamic",
    )
    find_address.assert_awaited_once_with(
        "TOKEN",
        "arbitrum",
        gate_config=service._dexscreener_gate_config,
    )
    confirm_address.assert_awaited_once_with(_ADDRESS, "arbitrum", expected_symbol="TOKEN")
    cache.assert_called_once_with(metadata, "arbitrum", source="dexscreener_dynamic")


@pytest.mark.parametrize(
    ("side_effect", "should_raise"),
    [
        (DexScreenerError("malformed response"), False),
        (RuntimeError("transport failure"), False),
        (
            AmbiguousTokenError(
                token="TOKEN",
                chain="arbitrum",
                reason="ambiguous",
                matching_addresses=[_ADDRESS],
            ),
            True,
        ),
    ],
)
@pytest.mark.asyncio
async def test_dexscreener_error_contract(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
    side_effect: Exception,
    should_raise: bool,
) -> None:
    monkeypatch.setattr(
        token_service_module,
        "dexscreener_find_token_address",
        AsyncMock(side_effect=side_effect),
    )

    if should_raise:
        with pytest.raises(AmbiguousTokenError) as exc_info:
            await service._try_dexscreener_symbol_lookup("TOKEN", "arbitrum")
        assert exc_info.value is side_effect
    else:
        assert await service._try_dexscreener_symbol_lookup("TOKEN", "arbitrum") is None


@pytest.mark.parametrize("outcome", ["missing_result", "missing_metadata"])
@pytest.mark.asyncio
async def test_dexscreener_malformed_outcomes_return_none_without_caching(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    dex_result = DexScreenerResult(
        address=_ADDRESS,
        chain="arbitrum",
        symbol="TOKEN",
        liquidity_usd=50_000.0,
        volume_24h_usd=5_000.0,
    )
    find_address = AsyncMock(return_value=None if outcome == "missing_result" else dex_result)
    confirm_address = AsyncMock(return_value=None)
    cache = MagicMock()
    monkeypatch.setattr(token_service_module, "dexscreener_find_token_address", find_address)
    monkeypatch.setattr(service, "_confirm_address_on_chain", confirm_address)
    monkeypatch.setattr(service, "_cache_discovered_token", cache)

    assert await service._try_dexscreener_symbol_lookup("TOKEN", "arbitrum") is None
    cache.assert_not_called()
    if outcome == "missing_result":
        confirm_address.assert_not_awaited()
    else:
        confirm_address.assert_awaited_once_with(_ADDRESS, "arbitrum", expected_symbol="TOKEN")


@pytest.mark.asyncio
async def test_dexscreener_unsupported_chain_skips_external_calls(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    find_address = AsyncMock()
    monkeypatch.setattr(token_service_module, "dexscreener_find_token_address", find_address)

    assert await service._try_dexscreener_symbol_lookup("TOKEN", "not-listed") is None
    find_address.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_ambiguity_status_and_payload_are_unchanged(
    service: TokenServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = AmbiguousTokenError(
        token="TOKEN",
        chain="arbitrum",
        reason="ambiguous",
        matching_addresses=[_ADDRESS],
    )
    service._resolver.resolve.side_effect = TokenNotFoundError(
        token="TOKEN",
        chain="arbitrum",
        reason="not found",
    )
    monkeypatch.setattr(service, "_try_evm_symbol_lookup", AsyncMock(side_effect=error))
    context = MagicMock()

    result = await service.ResolveToken(
        gateway_pb2.ResolveTokenRequest(token="TOKEN", chain="arbitrum"),
        context,
    )

    context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)
    details = context.set_details.call_args.args[0]
    assert details.startswith(f"AMBIGUOUS_SYMBOL|addresses={_ADDRESS}|")
    assert result.success is False
    assert result.error == details
