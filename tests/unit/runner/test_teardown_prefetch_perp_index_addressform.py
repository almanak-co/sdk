"""ALM-3217: address-shaped perp markets must warm their index price in teardown.

Every GMX demo and the perps scaffold build perp intents with
``market=self.market_address`` — an ``0x`` ADDRESS, not a symbol. The symbol
parser (:func:`extract_warmable_token_chains`) correctly rejects addresses, so
without gateway resolution the index price is never warmed, the GMX compiler
fails closed on the missing price, and the close is never compiled — stranding
an open leveraged position. VIB-6254's fix and its regression test used
symbol-form markets only, which is exactly why it shipped inert on the shape
production emits. These tests pin the ADDRESS form.
"""

from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.intents.vocabulary import PerpCloseIntent
from almanak.framework.runner.runner_teardown import prefetch_teardown_prices
from almanak.framework.teardown.oracle_warmup import resolve_perp_index_chains_via_gateway

XMR_MARKET_ADDRESS = "0x13674172e6e44d31d4be489d5184f3457c40153a"


class _FakePerpMarketService:
    def __init__(self, *, index_symbol: str = "XMR", verified: bool = True, success: bool = True) -> None:
        self._index_symbol = index_symbol
        self._verified = verified
        self._success = success
        self.requests: list = []

    def GetPerpMarket(self, request, timeout=None):  # noqa: N802 - gRPC surface
        self.requests.append(request)
        return SimpleNamespace(
            success=self._success,
            market=SimpleNamespace(index_symbol=self._index_symbol, verified=self._verified),
        )


class _FakeGatewayClient:
    def __init__(self, service) -> None:
        self.market = service


class _RecordingMarket:
    def __init__(self, gateway_client=None, chain: str = "arbitrum") -> None:
        self._chain = chain
        self._gateway_client = gateway_client
        self.calls: list[tuple[str, str | None]] = []

    def price(self, token: str, chain: str | None = None) -> Decimal:
        self.calls.append((token, chain))
        return Decimal("391.531")


def _address_close(*, chain: str = "arbitrum") -> PerpCloseIntent:
    return PerpCloseIntent(
        protocol="gmx_v2",
        market=XMR_MARKET_ADDRESS,
        collateral_token="USDC",
        is_long=True,
        chain=chain,
    )


def test_address_form_market_warms_index_symbol_via_gateway() -> None:
    """The production shape: 0x market address resolves to XMR and gets priced."""
    service = _FakePerpMarketService(index_symbol="XMR")
    market = _RecordingMarket(gateway_client=_FakeGatewayClient(service))

    prefetch_teardown_prices(market, [_address_close()])

    assert ("XMR", "arbitrum") in market.calls
    assert len(service.requests) == 1
    assert service.requests[0].market == XMR_MARKET_ADDRESS
    assert service.requests[0].protocol == "gmx_v2"


def test_address_form_without_gateway_still_warms_collateral_without_crashing() -> None:
    """No gateway client (symbol-shaped legacy path) — degrade, never raise."""
    market = _RecordingMarket(gateway_client=None)

    prefetch_teardown_prices(market, [_address_close()])

    assert ("USDC", None) in market.calls
    assert not any(symbol == "XMR" for symbol, _chain in market.calls)


def test_unverified_market_record_is_not_warmed() -> None:
    """Verified-only: an unverified record must not inject a guessed symbol."""
    service = _FakePerpMarketService(index_symbol="XMR", verified=False)
    market = _RecordingMarket(gateway_client=_FakeGatewayClient(service))

    resolved = resolve_perp_index_chains_via_gateway(market, [_address_close()], "arbitrum")

    assert resolved == {}


def test_gateway_failure_is_swallowed_and_warm_continues() -> None:
    """A warm miss must never block the unwind."""

    class _ExplodingService:
        def GetPerpMarket(self, request, timeout=None):  # noqa: N802
            raise RuntimeError("gateway unavailable")

    market = _RecordingMarket(gateway_client=_FakeGatewayClient(_ExplodingService()))

    resolved = resolve_perp_index_chains_via_gateway(market, [_address_close()], "arbitrum")

    assert resolved == {}


def test_symbol_form_market_still_warms_without_gateway() -> None:
    """The VIB-6254 symbol lane keeps working unchanged."""
    market = _RecordingMarket(gateway_client=None)

    prefetch_teardown_prices(
        market,
        [
            PerpCloseIntent(
                protocol="gmx_v2",
                market="XMR/USD",
                collateral_token="USDC",
                is_long=True,
                chain="arbitrum",
            )
        ],
    )

    assert ("XMR", "arbitrum") in market.calls


def test_intent_chain_beats_snapshot_fallback_chain() -> None:
    """Each address-form market warms on its own intent's chain."""
    service = _FakePerpMarketService(index_symbol="XMR")
    market = _RecordingMarket(gateway_client=_FakeGatewayClient(service), chain="avalanche")

    resolved = resolve_perp_index_chains_via_gateway(market, [_address_close(chain="arbitrum")], "avalanche")

    assert resolved == {"XMR": "arbitrum"}


def test_warm_and_validate_oracle_warms_address_form_index() -> None:
    """The manager-lane pre-flight (`warm_and_validate_oracle`) must warm the
    gateway-resolved index too — not only the runner-lane prefetch."""
    from almanak.framework.teardown.oracle_warmup import warm_and_validate_oracle

    service = _FakePerpMarketService(index_symbol="XMR")

    class _OracleMarket(_RecordingMarket):
        def get_price_oracle_dict(self) -> dict[str, Decimal]:
            return {symbol: Decimal("391.531") for symbol, _chain in self.calls}

    market = _OracleMarket(gateway_client=_FakeGatewayClient(service))

    oracle = warm_and_validate_oracle(market, [_address_close()], "arbitrum", raise_on_missing=False)

    assert ("XMR", "arbitrum") in market.calls
    assert oracle is not None and "XMR" in oracle


def test_cli_lane_without_intent_or_fallback_chain_uses_snapshot_chain() -> None:
    """TeardownManager derives its chain from intents, which GMX teardown
    intents commonly omit — the resolver must fall back to the snapshot's
    own chain instead of sending an empty chain to GetPerpMarket."""
    service = _FakePerpMarketService(index_symbol="XMR")
    market = _RecordingMarket(gateway_client=_FakeGatewayClient(service), chain="arbitrum")
    intent = PerpCloseIntent(
        protocol="gmx_v2",
        market=XMR_MARKET_ADDRESS,
        collateral_token="USDC",
        is_long=True,
        chain=None,
    )

    resolved = resolve_perp_index_chains_via_gateway(market, [intent], None)

    assert resolved == {"XMR": "arbitrum"}
    assert service.requests[0].chain == "arbitrum"
