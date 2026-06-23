"""Unit tests for ``MarketSnapshot.pt_price`` (VIB-5311).

The surface is a pure pass-through of the gateway PT/YT-USD price contract
(``GetPtPrice``, VIB-5309/5310). These tests mock the gateway market stub and
assert the band + ``stale`` → ``ValueConfidence`` mapping, plus the Empty ≠ Zero
invariant (unmeasured → ``price=None`` + ``UNAVAILABLE``, never ``Decimal("0")``).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.market.errors import PriceUnavailableError
from almanak.framework.market.snapshot import MarketSnapshot
from almanak.framework.portfolio.models import ValueConfidence
from almanak.gateway.proto import gateway_pb2


def _make_response(
    *,
    availability: int,
    price: str = "",
    confidence_band: int = gateway_pb2.PT_PRICE_CONFIDENCE_BAND_UNSPECIFIED,
    confidence: float = 0.0,
    stale: bool = False,
    underlying_price: str = "",
    pt_to_asset_rate: str = "",
    source: str = "",
    timestamp: int = 0,
    maturity_ts: int = 0,
    days_to_maturity: int = 0,
    symbol: str = "PT-sUSDe-26JUN2025",
    chain: str = "ethereum",
) -> gateway_pb2.PtPriceResponse:
    return gateway_pb2.PtPriceResponse(
        symbol=symbol,
        chain=chain,
        quote="USD",
        price=price,
        availability=availability,
        confidence=confidence,
        confidence_band=confidence_band,
        underlying_price=underlying_price,
        pt_to_asset_rate=pt_to_asset_rate,
        source=source,
        timestamp=timestamp,
        stale=stale,
        maturity_ts=maturity_ts,
        days_to_maturity=days_to_maturity,
    )


def _snapshot_with_response(response: gateway_pb2.PtPriceResponse) -> tuple[MarketSnapshot, MagicMock]:
    """Build a single-chain snapshot whose gateway stub returns ``response``."""
    client = MagicMock()
    client.is_connected = True
    client.market.GetPtPrice.return_value = response
    snap = MarketSnapshot(chain="ethereum", gateway_client=client)
    return snap, client


# ---------------------------------------------------------------------------
# AVAILABLE — band + stale → ValueConfidence
# ---------------------------------------------------------------------------


def test_available_high_fresh_maps_high():
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.97",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        confidence=0.99,
        stale=False,
        underlying_price="1.001",
        pt_to_asset_rate="0.969",
        source="composition:getPtToAssetRate×aggregator",
        timestamp=1_750_000_000,
        maturity_ts=1_760_000_000,
        days_to_maturity=30,
    )
    snap, client = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price == Decimal("0.97")
    assert result.confidence is ValueConfidence.HIGH
    assert result.underlying_price == Decimal("1.001")
    assert result.pt_to_asset_rate == Decimal("0.969")
    assert result.days_to_maturity == 30
    assert result.maturity_ts == 1_760_000_000
    assert result.source == "composition:getPtToAssetRate×aggregator"
    assert result.stale is False
    assert result.raw_confidence == pytest.approx(0.99)
    assert result.is_available is True

    # Pure pass-through: request carries symbol/chain/quote and a 0 maturity hint.
    sent = client.market.GetPtPrice.call_args.args[0]
    assert sent.symbol == "PT-sUSDe-26JUN2025"
    assert sent.chain == "ethereum"
    assert sent.quote == "USD"
    assert sent.maturity_ts == 0


def test_days_to_maturity_passthrough_when_maturity_ts_zero():
    """Regression (VIB-5311): a valid ``days_to_maturity`` must survive a 0 ``maturity_ts``.

    The gateway resolves days-to-maturity on-chain but echoes ``maturity_ts``
    from the (zero) request — the real-world path for any caller that does not
    pass a maturity hint. The old ``days_to_maturity if response.maturity_ts``
    guard nulled the valid value, leaving the PT implied-APY signal inert for
    every such caller. ``days_to_maturity`` must pass through on its own.
    """
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="32.0",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        confidence=0.99,
        underlying_price="32.005",
        pt_to_asset_rate="0.999858",
        source="composition:getPtToAssetRate×aggregator",
        timestamp=1_750_000_000,
        maturity_ts=0,  # gateway did NOT populate it (request echo)
        days_to_maturity=2,  # but it DID read days on-chain
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-wstETH-25JUN2026", chain="ethereum")

    assert result.days_to_maturity == 2  # not None
    assert result.maturity_ts is None  # still unpopulated (gateway follow-up)
    assert result.pt_to_asset_rate == Decimal("0.999858")


def test_days_to_maturity_zero_maps_to_none_empty_not_zero():
    """A 0/unset ``days_to_maturity`` is unmeasured (None), never 0 (Empty≠Zero)."""
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="1.0",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        confidence=0.99,
        underlying_price="1.0",
        pt_to_asset_rate="1.0",
        days_to_maturity=0,
        maturity_ts=0,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.days_to_maturity is None


def test_available_estimated_maps_estimated():
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.95",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_ESTIMATED,
        confidence=0.6,
        stale=False,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price == Decimal("0.95")
    assert result.confidence is ValueConfidence.ESTIMATED
    assert result.is_available is True


def test_available_but_stale_maps_stale_regardless_of_band():
    # stale wins over the band — a stale HIGH price must NOT render as HIGH.
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.96",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        confidence=0.99,
        stale=True,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price == Decimal("0.96")
    assert result.confidence is ValueConfidence.STALE
    assert result.stale is True
    # STALE still counts as an available number (just degraded).
    assert result.is_available is True
    # VIB-5312: a strategy can gate on the stale mark via a first-class property.
    assert result.is_stale is True


def test_fresh_high_mark_is_not_stale():
    # VIB-5312: a fresh HIGH mark is available AND not stale — the
    # ``is_available and not is_stale`` gate a strategy uses for a trustable mark.
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.97",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        confidence=0.99,
        stale=False,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.is_available is True
    assert result.is_stale is False
    assert result.confidence is ValueConfidence.HIGH


def test_is_stale_true_when_confidence_stale_even_without_flag():
    # Defense-in-depth: if the combined confidence is STALE, ``is_stale`` is True
    # even should the raw flag disagree — the two signals are OR-ed (fail-safe).
    from almanak.framework.market.models import PtPriceData

    data = PtPriceData(
        symbol="PT-x",
        chain="ethereum",
        price=Decimal("0.9"),
        confidence=ValueConfidence.STALE,
        stale=False,
    )
    assert data.is_stale is True


def test_available_band_unspecified_fails_closed_to_unavailable():
    # An AVAILABLE response with no band (old/garbled) → fail-closed confidence,
    # but the measured price is still returned (Empty ≠ Zero is about None vs 0,
    # and here the price IS measured).
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.94",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_UNSPECIFIED,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price == Decimal("0.94")
    assert result.confidence is ValueConfidence.UNAVAILABLE


# ---------------------------------------------------------------------------
# Unmeasured availabilities → Empty ≠ Zero (None, never Decimal("0"))
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "availability",
    [
        gateway_pb2.PT_PRICE_AVAILABILITY_UNMEASURED,
        gateway_pb2.PT_PRICE_AVAILABILITY_ERRORED,
        gateway_pb2.PT_PRICE_AVAILABILITY_UNSPECIFIED,
    ],
)
def test_unmeasured_availability_returns_none_and_unavailable(availability):
    response = _make_response(availability=availability, price="")
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price is None
    assert result.price != Decimal("0")  # Empty ≠ Zero
    assert result.confidence is ValueConfidence.UNAVAILABLE
    assert result.is_available is False


@pytest.mark.parametrize(
    "bad_price",
    [
        "",
        "not-a-number",
        "abc",
        # Non-finite Decimals parse WITHOUT raising InvalidOperation — they must
        # still fail closed, else NaN/Inf poisons NAV downstream.
        "NaN",
        "Infinity",
        "-Infinity",
        "inf",
    ],
)
def test_available_with_malformed_price_fails_closed_no_crash(bad_price):
    # A version-skewed/buggy gateway returning AVAILABLE with an empty,
    # non-numeric, or non-finite price must NOT crash the decide cycle nor leak
    # a non-finite Decimal — fail closed to UNAVAILABLE+None instead.
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price=bad_price,
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")  # must not raise

    assert result.price is None
    assert result.confidence is ValueConfidence.UNAVAILABLE
    assert result.is_available is False


@pytest.mark.parametrize("bad_leg", ["garbage", "NaN", "Infinity"])
def test_malformed_transparency_leg_drops_to_none_but_keeps_price(bad_leg):
    # A malformed/non-finite composition leg is display-only — it must degrade
    # to None, never crash nor poison the measured price.
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.97",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        underlying_price=bad_leg,
        pt_to_asset_rate="0.969",
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price == Decimal("0.97")
    assert result.confidence is ValueConfidence.HIGH
    assert result.underlying_price is None  # malformed/non-finite → dropped
    assert result.pt_to_asset_rate == Decimal("0.969")  # valid leg preserved


def test_out_of_range_timestamp_degrades_to_none_no_crash():
    # A malformed / out-of-range epoch must NOT crash a read whose price is
    # otherwise valid — degrade ``timestamp`` to None.
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.97",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        timestamp=10**18,  # far beyond datetime range → fromtimestamp raises
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price == Decimal("0.97")
    assert result.confidence is ValueConfidence.HIGH
    assert result.timestamp is None


def test_rpc_call_carries_an_explicit_deadline():
    # A hung gateway must not hang the decide cycle: the GetPtPrice call must
    # always pass an explicit timeout (the interceptors only propagate one).
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.9",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
    )
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.5
    client.market.GetPtPrice.return_value = response
    snap = MarketSnapshot(chain="ethereum", gateway_client=client)

    snap.pt_price("PT-x")

    timeout = client.market.GetPtPrice.call_args.kwargs.get("timeout")
    assert timeout == 12.5


def test_wiring_error_carries_pt_symbol_token():
    # PriceUnavailableError is structured (token, reason) — the wiring-error
    # raise must carry the PT symbol so handlers don't lose it.
    snap = MarketSnapshot(chain="ethereum", gateway_client=None)
    with pytest.raises(PriceUnavailableError) as exc_info:
        snap.pt_price("PT-sUSDe-26JUN2025")
    assert "PT-sUSDe-26JUN2025" in str(exc_info.value)


def test_unmeasured_does_not_raise():
    # Expected-unpriceable is normal: never raise, return the UNAVAILABLE object.
    response = _make_response(availability=gateway_pb2.PT_PRICE_AVAILABILITY_UNMEASURED)
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-unpriceable")
    assert result.price is None


# ---------------------------------------------------------------------------
# Transport / wiring errors
# ---------------------------------------------------------------------------


def test_rpc_error_fails_closed_to_unavailable():
    client = MagicMock()
    client.is_connected = True
    client.market.GetPtPrice.side_effect = RuntimeError("gateway UNAVAILABLE")
    snap = MarketSnapshot(chain="ethereum", gateway_client=client)

    result = snap.pt_price("PT-sUSDe-26JUN2025")

    assert result.price is None
    assert result.confidence is ValueConfidence.UNAVAILABLE


def test_missing_gateway_client_raises_price_unavailable():
    snap = MarketSnapshot(chain="ethereum", gateway_client=None)
    with pytest.raises(PriceUnavailableError):
        snap.pt_price("PT-sUSDe-26JUN2025")


def test_disconnected_gateway_client_raises_price_unavailable():
    client = MagicMock()
    client.is_connected = False
    snap = MarketSnapshot(chain="ethereum", gateway_client=client)
    with pytest.raises(PriceUnavailableError):
        snap.pt_price("PT-sUSDe-26JUN2025")


# ---------------------------------------------------------------------------
# Passthrough details: maturity hint, chain resolution
# ---------------------------------------------------------------------------


def test_maturity_hint_is_forwarded():
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.9",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
    )
    snap, client = _snapshot_with_response(response)

    snap.pt_price("PT-x", maturity=1_760_000_000)

    sent = client.market.GetPtPrice.call_args.args[0]
    assert sent.maturity_ts == 1_760_000_000


def test_explicit_chain_forwarded_when_configured():
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.9",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        chain="ethereum",
    )
    client = MagicMock()
    client.is_connected = True
    client.market.GetPtPrice.return_value = response
    snap = MarketSnapshot(chains=("ethereum", "arbitrum"), gateway_client=client)

    snap.pt_price("PT-x", chain="arbitrum")
    sent = client.market.GetPtPrice.call_args.args[0]
    assert sent.chain == "arbitrum"


def test_ambiguous_chain_raises_on_multichain_without_chain():
    from almanak.framework.market.errors import AmbiguousChainError

    client = MagicMock()
    client.is_connected = True
    snap = MarketSnapshot(chains=("ethereum", "arbitrum"), gateway_client=client)
    with pytest.raises(AmbiguousChainError):
        snap.pt_price("PT-x")


def test_available_with_zero_maturity_omits_days_to_maturity():
    # Both legs unset: days_to_maturity=0 is unmeasured → None (Empty≠Zero), and
    # maturity_ts=0 → None. NOTE: days is None here because *days itself* is 0,
    # NOT because maturity_ts is 0 — a positive days survives a 0 maturity_ts
    # (see test_days_to_maturity_passthrough_when_maturity_ts_zero, VIB-5311).
    response = _make_response(
        availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
        price="0.9",
        confidence_band=gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH,
        maturity_ts=0,
        days_to_maturity=0,
    )
    snap, _ = _snapshot_with_response(response)

    result = snap.pt_price("PT-x")
    assert result.days_to_maturity is None
    assert result.maturity_ts is None
