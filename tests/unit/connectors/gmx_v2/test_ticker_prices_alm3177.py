"""Unit contracts for the GMX V2 venue ticker reader (ALM-3177).

The reader descales the venue's signed 30-decimal oracle bounds to USD mids,
serves ONLY synthetic index symbols, skips malformed rows without poisoning
the page, and caches both catalogue endpoints so one compile's symbol lookups
cost one fetch.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.connectors._base.gateway_capabilities import (
    PerpMarketCatalogueUnavailable,
    VenueTickerPrice,
)
from almanak.connectors.gmx_v2.gateway.ticker_prices import GmxV2TickerPriceReader

# XMR/USD on Arbitrum — the ALM-3177 market. 12-decimal synthetic index:
# raw bounds are USD * 10**(30 - 12).
_XMR_ADDRESS = "0x13674172E6E44D31d4bE489d5184f3457c40153A"
_WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

_TOKENS_PAYLOAD = {
    "tokens": [
        {"symbol": "XMR", "address": _XMR_ADDRESS, "decimals": 12, "synthetic": True},
        {"symbol": "WETH", "address": _WETH_ADDRESS, "decimals": 18},
        {"symbol": "", "address": "0x" + "11" * 20, "decimals": 8, "synthetic": True},
        {"symbol": "BADDEC", "address": "0x" + "22" * 20, "decimals": 31, "synthetic": True},
        # bool("false") is True — a string flag must be rejected as malformed,
        # not truthiness-coerced into "synthetic".
        {"symbol": "STRFLAG", "address": "0x" + "88" * 20, "decimals": 12, "synthetic": "false"},
    ]
}

_TICKERS_PAYLOAD = [
    {
        "tokenAddress": _XMR_ADDRESS,
        "tokenSymbol": "XMR",
        "minPrice": str(368 * 10**18),
        "maxPrice": str(370 * 10**18),
        "timestamp": 1_786_100_760,
    },
    {
        "tokenAddress": _WETH_ADDRESS,
        "tokenSymbol": "WETH",
        "minPrice": str(3000 * 10**12),
        "maxPrice": str(3001 * 10**12),
        "timestamp": 1_786_100_760,
    },
    {
        "tokenAddress": "0x" + "88" * 20,
        "tokenSymbol": "STRFLAG",
        "minPrice": str(1 * 10**18),
        "maxPrice": str(1 * 10**18),
        "timestamp": 1_786_100_760,
    },
]


def _reader_with(payloads: dict[str, object]) -> tuple[GmxV2TickerPriceReader, AsyncMock]:
    reader = GmxV2TickerPriceReader()
    fetch = AsyncMock(side_effect=lambda chain, path: payloads[path])
    return reader, fetch


@pytest.mark.asyncio
async def test_descales_synthetic_mid_and_excludes_deployed_tokens() -> None:
    reader, fetch = _reader_with({"/tokens": _TOKENS_PAYLOAD, "/prices/tickers": _TICKERS_PAYLOAD})
    with patch.object(reader, "_get_json", fetch):
        page = await reader.fetch(chain="arbitrum")

    # WETH has a deployed contract -> excluded; STRFLAG's string "synthetic"
    # flag is malformed, not truthy -> excluded.
    assert set(page) == {"XMR"}
    entry = page["XMR"]
    assert isinstance(entry, VenueTickerPrice)
    assert entry.price_usd == Decimal("369")  # mid of 368/370 descaled by 10**(30-12)
    assert entry.updated_at == 1_786_100_760


@pytest.mark.asyncio
async def test_malformed_rows_are_skipped_without_poisoning_the_page() -> None:
    tickers = [
        dict(_TICKERS_PAYLOAD[0]),
        # Inverted bounds.
        {**_TICKERS_PAYLOAD[0], "tokenAddress": "0x" + "33" * 20, "minPrice": "10", "maxPrice": "5"},
        # Zero price.
        {**_TICKERS_PAYLOAD[0], "tokenAddress": "0x" + "44" * 20, "minPrice": "0", "maxPrice": "0"},
        # Unparseable price.
        {**_TICKERS_PAYLOAD[0], "tokenAddress": "0x" + "55" * 20, "minPrice": "not-a-number"},
        # Missing venue timestamp.
        {**_TICKERS_PAYLOAD[0], "tokenAddress": "0x" + "66" * 20, "timestamp": 0},
        # Address absent from the token catalogue.
        {**_TICKERS_PAYLOAD[0], "tokenAddress": "0x" + "77" * 20},
        "not-a-dict",
    ]
    tokens = {
        "tokens": _TOKENS_PAYLOAD["tokens"]
        + [
            {"symbol": f"S{i}", "address": "0x" + f"{i}{i}" * 20, "decimals": 12, "synthetic": True}
            for i in (3, 4, 5, 6)
        ]
    }
    reader, fetch = _reader_with({"/tokens": tokens, "/prices/tickers": tickers})
    with patch.object(reader, "_get_json", fetch):
        page = await reader.fetch(chain="arbitrum")

    assert set(page) == {"XMR"}


@pytest.mark.asyncio
async def test_page_is_cached_within_ttl() -> None:
    reader, fetch = _reader_with({"/tokens": _TOKENS_PAYLOAD, "/prices/tickers": _TICKERS_PAYLOAD})
    with patch.object(reader, "_get_json", fetch):
        first = await reader.fetch(chain="arbitrum")
        second = await reader.fetch(chain="arbitrum")

    assert first is second
    # One /tokens + one /prices/tickers for BOTH fetches.
    assert fetch.await_count == 2


@pytest.mark.asyncio
async def test_unsupported_chain_is_a_loud_error() -> None:
    reader = GmxV2TickerPriceReader()
    with pytest.raises(ValueError, match="unsupported on chain"):
        await reader.fetch(chain="base")


@pytest.mark.asyncio
async def test_non_list_tickers_payload_is_catalogue_unavailable() -> None:
    reader, fetch = _reader_with({"/tokens": _TOKENS_PAYLOAD, "/prices/tickers": {"unexpected": "shape"}})
    with patch.object(reader, "_get_json", fetch):
        with pytest.raises(PerpMarketCatalogueUnavailable, match="not a list"):
            await reader.fetch(chain="arbitrum")


@pytest.mark.asyncio
async def test_missing_tokens_list_is_catalogue_unavailable() -> None:
    reader, fetch = _reader_with({"/tokens": {"nope": []}, "/prices/tickers": _TICKERS_PAYLOAD})
    with patch.object(reader, "_get_json", fetch):
        with pytest.raises(PerpMarketCatalogueUnavailable, match="tokens list"):
            await reader.fetch(chain="arbitrum")
