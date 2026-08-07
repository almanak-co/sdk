"""GMX V2 gateway funding fetch: audited-address sourcing + signed-rate extraction.

The pre-consolidation gateway servicer carried its own hand-copied GMX
reader/DataStore/market dicts (5 Arbitrum markets, an old reader address)
and a ``getMarketInfo`` ABI declaring a 9-word ``MarketInfo`` that no
deployed reader returns — every live fetch failed strict eth-abi decode
inside a broad ``except Exception`` and silently served default rates.

These tests pin the consolidated implementation in
``almanak.connectors.gmx_v2.gateway.provider.fetch_funding_rate``:

* Reader/DataStore addresses come from the connector's audited ``GMX_V2``
  table and the market token from the dynamic market registry
  (``GmxV2MarketRegistry``, stubbed here), so reintroducing a local copy or
  the dead reader address fails here;
* the signed hourly rate derives from ``nextFunding.longsPayShorts`` +
  ``nextFunding.fundingFactorPerSecond`` (positive = longs pay shorts);
* every miss (unlisted market, ambiguous pair label, unsupported chain,
  no RPC, resolution failure, decode error) falls back to the connector
  default with ``is_live_data=False`` — funding factors are per market
  token, so an ambiguous pair label is refused, never guessed at.

The ABI's layout against the LIVE readers is pinned separately by
``tests/audit/test_gmx_v2_funding_reader_abi.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from web3 import Web3

from almanak.connectors.gmx_v2.addresses import GMX_V2
from almanak.connectors.gmx_v2.gateway.provider import (
    GMX_V2_READER_GET_MARKET_INFO_ABI,
    GmxV2GatewayConnector,
)

_FACTOR = 4_364_000_000_000_000_000  # ~4.36e-12/sec at 1e30 precision

# Literal audited market tokens (pinned on-chain by
# ``tests/audit/test_gmx_v2_market_identity.py`` before the static table was
# retired in favour of dynamic venue-catalogue resolution).
_ETH_USD_ARBITRUM = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
_BTC_USD_ARBITRUM = "0x47c031236e19d024b42f8AE6780E44A573170703"
_AVAX_USD_AVALANCHE = "0x913C1F46b48b3eD35E7dc3Cf754d4ae8499F31CF"


class _StubMarketRegistry:
    """Catalogue stub for ``GmxV2GatewayConnector._market_registry``.

    Mirrors the real registry's contract at this seam: a query matching one
    market returns a verified-shaped record carrying a lowercase
    ``market_token``; unknown markets return ``None``; a query matching
    several markets raises the registry's ambiguity ``ValueError`` (funding
    resolution must never pass ``allow_index_equivalent``, so the stub also
    rejects any call that tries to).
    """

    def __init__(self, catalogue: dict[tuple[str, str], list[str]]) -> None:
        self.calls: list[dict[str, Any]] = []
        self._catalogue = catalogue

    async def resolve(self, *, chain: str, market: str, eth_call: Any, **kwargs: Any) -> Any:
        self.calls.append({"chain": chain, "market": market, "eth_call": eth_call, **kwargs})
        assert not kwargs.get("allow_index_equivalent"), (
            "funding must never guess among collateral variants of one pair label"
        )
        addresses = self._catalogue.get((chain, market))
        if not addresses:
            return None
        if len(addresses) > 1:
            choices = ", ".join(addresses)
            raise ValueError(f"GMX market {market!r} is ambiguous; pass the exact full name or address: {choices}")
        return SimpleNamespace(market_token=addresses[0].lower())


def _connector(
    catalogue: dict[tuple[str, str], list[str]] | None = None,
) -> GmxV2GatewayConnector:
    connector = GmxV2GatewayConnector()
    connector._market_registry = _StubMarketRegistry(
        catalogue
        if catalogue is not None
        else {
            ("arbitrum", "ETH/USD"): [_ETH_USD_ARBITRUM],
            ("arbitrum", "BTC/USD"): [_BTC_USD_ARBITRUM],
            ("avalanche", "AVAX/USD"): [_AVAX_USD_AVALANCHE],
        }
    )
    return connector


def _market_info(*, longs_pay_shorts: bool, factor: int) -> tuple:
    """A decoded 29-word ``MarketInfo`` tuple as web3 returns it."""
    collateral = (0, 0)
    position = (collateral, collateral)
    return (
        (
            "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",  # marketToken
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # indexToken
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # longToken
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # shortToken
        ),
        0,  # borrowingFactorPerSecondForLongs
        0,  # borrowingFactorPerSecondForShorts
        (position, position),  # baseFunding
        (longs_pay_shorts, factor, factor, position, position),  # nextFunding
        (0, 0, 0),  # virtualInventory
        False,  # isDisabled
    )


class _FakeBoundCall:
    def __init__(self, result: tuple | Exception, calls: list) -> None:
        self._result = result
        self._calls = calls

    def __call__(self, *args: Any) -> _FakeBoundCall:
        self._calls.append(args)
        return self

    async def call(self) -> tuple:
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class _FakeWeb3:
    def __init__(self, result: tuple | Exception) -> None:
        self.contract_requests: list[dict[str, Any]] = []
        self.get_market_info_calls: list[tuple] = []
        self._result = result
        outer = self

        class _Eth:
            def contract(self, *, address: str, abi: list) -> Any:
                outer.contract_requests.append({"address": address, "abi": abi})
                bound = _FakeBoundCall(outer._result, outer.get_market_info_calls)
                functions = type("_Functions", (), {"getMarketInfo": bound})()
                return type("_Contract", (), {"functions": functions})()

        self.eth = _Eth()

    @staticmethod
    def to_checksum_address(value: str) -> str:
        return Web3.to_checksum_address(value)


class _FakeServicer:
    def __init__(self, web3: _FakeWeb3 | None) -> None:
        self._get_web3 = AsyncMock(return_value=web3)
        self.settings = SimpleNamespace(network="mainnet")

    def _get_default_mark_price(self, market: str) -> Decimal:
        return Decimal("3000")


@pytest.mark.asyncio
async def test_live_rate_resolved_through_registry_and_positive_when_longs_pay_shorts() -> None:
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR))
    servicer = _FakeServicer(web3)
    connector = _connector()

    # Slash-form input must canonicalize and resolve through the registry.
    data = await connector.fetch_funding_rate(servicer, "ETH/USD", "arbitrum")

    assert data.venue == "gmx_v2"
    assert data.market == "ETH-USD"
    assert data.is_live_data is True
    assert data.rate_hourly == Decimal(_FACTOR) / Decimal(10) ** 30 * Decimal("3600")

    # The market token must come from the dynamic registry, queried with the
    # pair form and a gateway eth_call for on-chain verification — and
    # WITHOUT index-equivalence: funding is per market token, so the pair
    # query must stay strict (an ambiguous label fails, it is never guessed).
    (resolve_call,) = connector._market_registry.calls
    assert resolve_call["chain"] == "arbitrum"
    assert resolve_call["market"] == "ETH/USD"
    assert resolve_call["eth_call"] is not None
    assert resolve_call.get("allow_index_equivalent", False) is False

    # The reader contract and the call operands must be the connector's
    # audited addresses — not a service-local copy.
    (request,) = web3.contract_requests
    assert request["address"] == GMX_V2["arbitrum"]["reader"]
    assert request["abi"] is GMX_V2_READER_GET_MARKET_INFO_ABI
    (call_args,) = web3.get_market_info_calls
    data_store, _prices, market_token = call_args
    assert data_store == GMX_V2["arbitrum"]["data_store"]
    assert market_token == _ETH_USD_ARBITRUM


@pytest.mark.asyncio
async def test_live_rate_negative_when_shorts_pay_longs() -> None:
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=False, factor=_FACTOR))
    servicer = _FakeServicer(web3)

    data = await _connector().fetch_funding_rate(servicer, "ETH-USD", "arbitrum")

    assert data.is_live_data is True
    assert data.rate_hourly == -(Decimal(_FACTOR) / Decimal(10) ** 30 * Decimal("3600"))


@pytest.mark.asyncio
async def test_registry_markets_beyond_the_old_five_are_live_fetchable() -> None:
    """The old servicer copy stopped at 5 Arbitrum markets; the dynamic

    registry serves every venue-listed (chain, market) — including Avalanche.
    """
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR))
    servicer = _FakeServicer(web3)

    data = await _connector().fetch_funding_rate(servicer, "AVAX-USD", "avalanche")

    assert data.is_live_data is True
    (request,) = web3.contract_requests
    assert request["address"] == GMX_V2["avalanche"]["reader"]
    (call_args,) = web3.get_market_info_calls
    assert call_args[0] == GMX_V2["avalanche"]["data_store"]
    assert call_args[2] == _AVAX_USD_AVALANCHE


@pytest.mark.asyncio
async def test_unlisted_market_falls_back_without_calling_the_reader() -> None:
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR))
    servicer = _FakeServicer(web3)

    data = await _connector().fetch_funding_rate(servicer, "XMR-USD", "arbitrum")

    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.00001")  # unknown-market default
    assert web3.contract_requests == []  # unresolved market never reaches the reader
    assert web3.get_market_info_calls == []


@pytest.mark.asyncio
async def test_unsupported_chain_falls_back_without_touching_rpc() -> None:
    servicer = _FakeServicer(_FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR)))
    connector = _connector()

    data = await connector.fetch_funding_rate(servicer, "ETH-USD", "base")

    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.000012")  # ETH-USD connector default
    servicer._get_web3.assert_not_awaited()
    assert connector._market_registry.calls == []  # no web3 -> no resolution


@pytest.mark.asyncio
async def test_registry_resolution_failure_falls_back_to_default() -> None:
    """A catalogue outage or verification error degrades exactly like an RPC

    failure: default rate, ``is_live_data=False``, never a raised exception.
    """
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR))
    servicer = _FakeServicer(web3)
    connector = _connector()
    connector._market_registry.resolve = AsyncMock(side_effect=RuntimeError("GMX metadata request failed"))

    data = await connector.fetch_funding_rate(servicer, "ETH-USD", "arbitrum")

    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.000012")
    assert web3.contract_requests == []


@pytest.mark.asyncio
async def test_ambiguous_pair_falls_back_but_exact_address_resolves_live() -> None:
    """Two collateral variants share one pair label: the pair query must

    refuse to guess (funding factors are per market token) and serve the
    default rate, while the exact market address still resolves to that
    market's live rate — the address-first funding contract.
    """
    # A second venue-listed ETH/USD collateral variant (the single-sided
    # WETH market); any second address would do — the stub only needs two
    # records under one label to trigger the registry's ambiguity error.
    eth_usd_single_sided = "0x450bb6774Dd8a756274E0ab4107953259d2ac541"
    catalogue = {
        ("arbitrum", "ETH/USD"): [_ETH_USD_ARBITRUM, eth_usd_single_sided],
        ("arbitrum", _ETH_USD_ARBITRUM): [_ETH_USD_ARBITRUM],
    }

    # Pair label → ambiguity error inside resolution → default rate, and the
    # reader is never called with a guessed variant.
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR))
    servicer = _FakeServicer(web3)
    data = await _connector(catalogue).fetch_funding_rate(servicer, "ETH-USD", "arbitrum")
    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.000012")  # ETH-USD connector default
    assert web3.contract_requests == []
    assert web3.get_market_info_calls == []

    # Exact market address → precise resolution → that market's live rate.
    web3 = _FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR))
    servicer = _FakeServicer(web3)
    data = await _connector(catalogue).fetch_funding_rate(servicer, _ETH_USD_ARBITRUM, "arbitrum")
    assert data.is_live_data is True
    assert data.rate_hourly == Decimal(_FACTOR) / Decimal(10) ** 30 * Decimal("3600")
    (call_args,) = web3.get_market_info_calls
    assert call_args[2] == _ETH_USD_ARBITRUM


@pytest.mark.asyncio
async def test_no_web3_falls_back_to_default() -> None:
    servicer = _FakeServicer(None)

    data = await _connector().fetch_funding_rate(servicer, "BTC-USD", "arbitrum")

    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.000010")


@pytest.mark.asyncio
async def test_decode_failure_falls_back_to_default() -> None:
    """The regression that motivated this module: a decode error must degrade

    to the default rate (is_live_data=False), never propagate — and the
    audit test exists precisely because this fallback hides ABI rot.
    """
    servicer = _FakeServicer(_FakeWeb3(ValueError("BadFunctionCallOutput")))

    data = await _connector().fetch_funding_rate(servicer, "ETH-USD", "arbitrum")

    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.000012")


@pytest.mark.asyncio
async def test_timeout_falls_back_to_default() -> None:
    """The reader call has a dedicated ``TimeoutError`` branch — same fallback."""
    servicer = _FakeServicer(_FakeWeb3(TimeoutError()))

    data = await _connector().fetch_funding_rate(servicer, "ETH-USD", "arbitrum")

    assert data.is_live_data is False
    assert data.rate_hourly == Decimal("0.000012")


@pytest.mark.asyncio
async def test_mark_price_comes_from_servicer_and_next_funding_is_the_hour_boundary() -> None:
    """Mark/index price are the servicer's defaults; GMX settles funding hourly,

    so ``next_funding_time`` must be the next exact hour boundary, in the future.
    """
    servicer = _FakeServicer(_FakeWeb3(_market_info(longs_pay_shorts=True, factor=_FACTOR)))

    data = await _connector().fetch_funding_rate(servicer, "ETH-USD", "arbitrum")

    assert data.mark_price == Decimal("3000")
    assert data.index_price == data.mark_price
    assert (data.next_funding_time.minute, data.next_funding_time.second) == (0, 0)
    assert data.next_funding_time > datetime.now(UTC)
