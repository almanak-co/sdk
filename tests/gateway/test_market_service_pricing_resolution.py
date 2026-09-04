"""Branch-complete characterization tests for pricing token resolution."""

from __future__ import annotations

import builtins
import logging
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.data.tokens import ResolvedToken
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.framework.data.tokens.models import CHAIN_ID_MAP
from almanak.gateway.services import market_service as market_service_module
from almanak.gateway.services.market_service import MarketServiceServicer, MultiChainAmbiguousPriceRequest
from almanak.gateway.services.onchain_lookup import TokenMetadata
from almanak.gateway.validation import ValidationError, validate_chain

BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
UNKNOWN_EVM = "0xEB4C2781e4ebA804CE9a9803C67d0893436bB27D"
SOLANA_JUP = "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
SOLANA_UNKNOWN = "7dHbWXmci3dT8UFYWYZweBLReY7pSp6tJzK54Y1L7m"


def _servicer(chains: list[str]) -> MarketServiceServicer:
    return MarketServiceServicer(SimpleNamespace(chains=chains, network="mainnet"))


def _price_result(price: str = "1.00") -> SimpleNamespace:
    return SimpleNamespace(
        price=Decimal(price),
        source="test",
        confidence=1.0,
        stale=False,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _aggregator(price: str = "1.00") -> MagicMock:
    aggregator = MagicMock()
    aggregator.get_aggregated_price = AsyncMock(return_value=_price_result(price))
    aggregator.get_last_details = MagicMock(return_value=None)
    return aggregator


@pytest.mark.parametrize(
    ("token", "chain", "expected"),
    [
        pytest.param(BASE_USDC, "base", True, id="evm-checksummed"),
        pytest.param(BASE_USDC.upper(), "base", True, id="evm-uppercase-prefix"),
        pytest.param(SOLANA_JUP, "solana", True, id="solana-case-sensitive-mint"),
        pytest.param(SOLANA_JUP, "base", False, id="solana-mint-on-evm-chain"),
        pytest.param("SOL", "solana", False, id="solana-symbol"),
        pytest.param("not-an-address", "base", False, id="invalid"),
    ],
)
def test_pricing_address_classification_table(token: str, chain: str, expected: bool) -> None:
    assert market_service_module._is_pricing_address(token, chain) is expected


@pytest.mark.parametrize(
    ("configured", "primary", "requested", "expected"),
    [
        pytest.param([], market_service_module._NO_CHAIN_KEY, "base", "base", id="explicit-on-demand"),
        pytest.param(["BASE"], market_service_module._NO_CHAIN_KEY, "", "base", id="single-configured"),
        pytest.param(["bsc"], market_service_module._NO_CHAIN_KEY, "BNB", "bsc", id="explicit-alias"),
        pytest.param(["base", "arbitrum"], "base", "", "base", id="chainless-symbol-primary"),
        pytest.param([], market_service_module._NO_CHAIN_KEY, "", None, id="chainless-unconfigured"),
        pytest.param(
            ["base", "arbitrum"],
            market_service_module._NO_CHAIN_KEY,
            "",
            None,
            id="chainless-symbol-before-primary-selection",
        ),
    ],
)
def test_pricing_chain_resolution_table(
    configured: list[str], primary: str, requested: str, expected: str | None
) -> None:
    servicer = _servicer(configured)
    servicer._primary_chain = primary

    assert servicer._resolve_pricing_chain("ETH", requested, is_evm_address=False) == expected


def test_pricing_chain_rejects_chainless_multichain_address_with_exact_error() -> None:
    servicer = _servicer(["base", "arbitrum"])

    with pytest.raises(MultiChainAmbiguousPriceRequest) as exc_info:
        servicer._resolve_pricing_chain(UNKNOWN_EVM, "", is_evm_address=True)

    assert str(exc_info.value) == (
        "Multi-chain gateway requires PriceRequest.chain for address-based lookups "
        "(token=0xEB4C...B27D, configured_chains=['base', 'arbitrum']). "
        "Set PriceRequest.chain to one of the configured chains."
    )


def test_pricing_chain_invalid_input_keeps_exact_log(caplog: pytest.LogCaptureFixture) -> None:
    servicer = _servicer(["base"])
    with pytest.raises(ValidationError) as exc_info:
        validate_chain("bsae")

    with caplog.at_level(logging.INFO, logger=market_service_module.__name__):
        assert servicer._resolve_pricing_chain("ETH", "bsae", is_evm_address=False) is None

    assert caplog.messages == [
        f"Token price identity lookup for ETH skipped: chain 'bsae' not allowed ({exc_info.value})"
    ]


def test_pricing_chain_unconfigured_input_keeps_exact_log(caplog: pytest.LogCaptureFixture) -> None:
    servicer = _servicer(["arbitrum"])

    with caplog.at_level(logging.INFO, logger=market_service_module.__name__):
        assert servicer._resolve_pricing_chain("ETH", "base", is_evm_address=False) is None

    assert caplog.messages == [
        "Token price identity lookup for ETH on base skipped: Chain 'base' is not configured on this gateway"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token", "chain", "expected_symbol", "expected_address"),
    [
        pytest.param(BASE_USDC, "base", "USDC", BASE_USDC, id="known-evm-address"),
        pytest.param(BASE_USDC.upper(), "base", "USDC", BASE_USDC, id="known-uppercase-evm-address"),
        pytest.param(NATIVE_SENTINEL, "base", "ETH", NATIVE_SENTINEL, id="native-sentinel"),
        pytest.param("USDC", "base", "USDC", BASE_USDC, id="pegged-symbol"),
        pytest.param("ETH", "base", None, None, id="native-symbol-measured-path"),
        pytest.param("WETH", "base", None, None, id="ordinary-symbol-measured-path"),
        pytest.param(SOLANA_JUP, "solana", "JUP", SOLANA_JUP, id="non-pegged-solana-address"),
    ],
)
async def test_static_pricing_resolution_precedence_table(
    token: str,
    chain: str,
    expected_symbol: str | None,
    expected_address: str | None,
) -> None:
    servicer = _servicer([chain])
    servicer._get_onchain_lookup = AsyncMock(side_effect=AssertionError("static resolution must win"))

    resolved = await servicer._resolve_token_for_pricing(token, chain)

    if expected_symbol is None:
        assert resolved is None
    else:
        assert isinstance(resolved, ResolvedToken)
        assert resolved.symbol == expected_symbol
        if chain == "solana":
            assert resolved.address == expected_address
        else:
            assert resolved.address.lower() == expected_address.lower()
    servicer._get_onchain_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_static_symbol_ambiguity_remains_a_best_effort_miss(caplog: pytest.LogCaptureFixture) -> None:
    servicer = _servicer(["base"])
    resolver = MagicMock()
    error = AmbiguousTokenError(token="DUP", chain="base", matching_addresses=[BASE_USDC, UNKNOWN_EVM])
    resolver.resolve.side_effect = error

    with (
        patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver),
        caplog.at_level(logging.DEBUG, logger=market_service_module.__name__),
    ):
        resolved = await servicer._resolve_token_for_pricing("DUP", "base")

    assert resolved is None
    assert caplog.messages == [f"Static token price identity resolution failed for DUP on base: {error}"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token",
    [
        pytest.param(UNKNOWN_EVM, id="checksummed-prefix"),
        pytest.param(UNKNOWN_EVM.replace("0x", "0X"), id="uppercase-prefix"),
    ],
)
async def test_unknown_evm_address_falls_back_without_changing_casing(token: str) -> None:
    servicer = _servicer(["base"])
    metadata = TokenMetadata(
        symbol="renBTC",
        name="renBTC",
        decimals=8,
        address=token,
        is_native=False,
    )
    lookup = MagicMock()
    lookup.lookup = AsyncMock(return_value=metadata)
    servicer._get_onchain_lookup = AsyncMock(return_value=lookup)

    resolved = await servicer._resolve_token_for_pricing(token, "base")

    assert isinstance(resolved, ResolvedToken)
    assert resolved.address == token
    assert resolved.chain == "base"
    assert resolved.chain_id == 8453
    assert resolved.source == "on_chain"
    assert resolved.is_verified is False
    lookup.lookup.assert_awaited_once_with("base", token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token", "chain"),
    [
        pytest.param("UNKNOWN", "base", id="unknown-symbol"),
        pytest.param(SOLANA_UNKNOWN, "solana", id="unknown-solana-mint"),
        pytest.param(UNKNOWN_EVM, "solana", id="evm-address-on-solana"),
    ],
)
async def test_non_evm_fallback_cases_never_start_an_evm_lookup(token: str, chain: str) -> None:
    servicer = _servicer([chain])
    servicer._get_onchain_lookup = AsyncMock(side_effect=AssertionError("EVM fallback must stay closed"))

    assert await servicer._resolve_token_for_pricing(token, chain) is None
    servicer._get_onchain_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lookup_result", "lookup_error", "expected_log"),
    [
        pytest.param(None, None, None, id="metadata-miss"),
        pytest.param(None, RuntimeError("rpc down"), "On-chain metadata lookup failed", id="lookup-failure"),
        pytest.param(
            TokenMetadata(symbol="", name="broken", decimals=18, address=UNKNOWN_EVM, is_native=False),
            None,
            "Failed to build ResolvedToken from on-chain metadata",
            id="invalid-metadata",
        ),
    ],
)
async def test_evm_fallback_failure_table(
    lookup_result: TokenMetadata | None,
    lookup_error: Exception | None,
    expected_log: str | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    servicer = _servicer(["base"])
    lookup = MagicMock()
    lookup.lookup = AsyncMock(side_effect=lookup_error) if lookup_error else AsyncMock(return_value=lookup_result)
    servicer._get_onchain_lookup = AsyncMock(return_value=lookup)

    with caplog.at_level(logging.INFO, logger=market_service_module.__name__):
        assert await servicer._resolve_token_for_pricing(UNKNOWN_EVM, "base") is None

    fallback_logs = [
        record.getMessage()
        for record in caplog.records
        if record.name == market_service_module.__name__
        and record.getMessage().startswith(("On-chain metadata lookup failed", "Failed to build ResolvedToken"))
    ]
    if expected_log is None:
        assert fallback_logs == []
    else:
        assert len(fallback_logs) == 1
        assert fallback_logs[0].startswith(expected_log)


@pytest.mark.asyncio
async def test_evm_fallback_import_failure_keeps_exact_log(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    servicer = _servicer(["base"])
    real_import = builtins.__import__

    def fail_token_models_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "almanak.framework.data.tokens.models":
            raise ImportError("models unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_token_models_import)
    with caplog.at_level(logging.DEBUG, logger=market_service_module.__name__):
        resolved = await servicer._resolve_evm_address_for_pricing(UNKNOWN_EVM, "base")

    assert resolved is None
    assert caplog.messages == ["Cannot import token models for address resolution: models unavailable"]


@pytest.mark.asyncio
async def test_evm_fallback_unregistered_descriptor_keeps_exact_log(caplog: pytest.LogCaptureFixture) -> None:
    servicer = _servicer(["base"])

    with (
        patch.object(ChainRegistry, "try_resolve", return_value=None),
        caplog.at_level(logging.DEBUG, logger=market_service_module.__name__),
    ):
        resolved = await servicer._resolve_evm_address_for_pricing(UNKNOWN_EVM, "base")

    assert resolved is None
    assert caplog.messages == ["Cannot resolve base to a registered chain for address resolution"]


def test_fallback_builder_uses_descriptor_name_and_chain_id_map() -> None:
    descriptor = ChainRegistry.resolve("bnb")
    metadata = TokenMetadata(
        symbol="TOKEN",
        name="Token",
        decimals=18,
        address=UNKNOWN_EVM,
        is_native=False,
    )

    resolved = market_service_module._build_onchain_pricing_token(
        UNKNOWN_EVM,
        metadata,
        descriptor,
        ResolvedToken,
        CHAIN_ID_MAP,
    )

    assert isinstance(resolved, ResolvedToken)
    assert resolved.chain == "bsc"
    assert resolved.chain_id == 56


@pytest.mark.asyncio
async def test_getprice_chainless_pegged_symbol_preserves_primary_chain_behavior() -> None:
    servicer = _servicer(["base", "arbitrum"])
    servicer._ensure_initialized = AsyncMock()
    servicer._primary_chain = "base"
    base_aggregator = _aggregator()
    arbitrum_aggregator = _aggregator()
    servicer._price_aggregators = {"base": base_aggregator, "arbitrum": arbitrum_aggregator}
    context = MagicMock()

    response = await servicer.GetPrice(
        market_service_module.gateway_pb2.PriceRequest(token="USDC", quote="USD", chain=""),
        context,
    )

    assert response.price == "1.00"
    context.set_code.assert_not_called()
    base_aggregator.get_aggregated_price.assert_awaited_once()
    resolved = base_aggregator.get_aggregated_price.await_args.kwargs["resolved_token"]
    assert resolved.chain == "base"
    assert resolved.address.lower() == BASE_USDC.lower()
    arbitrum_aggregator.get_aggregated_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_getprice_forwards_registered_solana_mint_with_exact_casing() -> None:
    servicer = _servicer(["solana"])
    servicer._ensure_initialized = AsyncMock()
    servicer._primary_chain = "solana"
    aggregator = _aggregator("0.42")
    servicer._price_aggregators = {"solana": aggregator}
    servicer._get_onchain_lookup = AsyncMock(side_effect=AssertionError("Solana has no EVM metadata fallback"))
    context = MagicMock()

    response = await servicer.GetPrice(
        market_service_module.gateway_pb2.PriceRequest(token=SOLANA_JUP, quote="USD", chain="solana"),
        context,
    )

    assert response.price == "0.42"
    context.set_code.assert_not_called()
    resolved = aggregator.get_aggregated_price.await_args.kwargs["resolved_token"]
    assert resolved.symbol == "JUP"
    assert resolved.address == SOLANA_JUP
    assert resolved.chain == "solana"
    servicer._get_onchain_lookup.assert_not_awaited()
