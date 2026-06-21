"""Message-level contract tests for the Pendle PT/YT USD price proto.

VIB-5309 (epic VIB-5299, M1). This is the wire contract ONLY — the
composition/sourcing provider is VIB-5310 and the servicer leaves
``GetPtPrice`` UNIMPLEMENTED here.

The cardinal invariant under test is the wire-level **Empty ≠ Zero**: an
unmeasured PT price is carried as ``availability != AVAILABLE`` with an EMPTY
``price`` string — never the literal ``"0"`` (which would be a *measured* zero
and silently fabricate value). These tests round-trip serialize/deserialize the
three availability states and assert the unmeasured / errored cases can never be
mistaken for a measured zero.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.gateway.proto import gateway_pb2 as pb


def _roundtrip(msg: pb.PtPriceResponse) -> pb.PtPriceResponse:
    """Serialize to bytes and parse back — proves the field survives the wire."""
    return pb.PtPriceResponse.FromString(msg.SerializeToString())


class TestPtPriceRequestContract:
    def test_request_identity_is_symbol_keyed(self) -> None:
        """Symbol is the canonical identity/join key (spine §3) — not an address."""
        req = pb.PtPriceRequest(
            symbol="PT-USDe-26DEC2024",
            chain="ethereum",
            quote="USD",
            maturity_ts=1_735_171_200,
        )
        back = pb.PtPriceRequest.FromString(req.SerializeToString())
        assert back.symbol == "PT-USDe-26DEC2024"
        assert back.chain == "ethereum"
        assert back.quote == "USD"
        assert back.maturity_ts == 1_735_171_200

    def test_request_maturity_is_optional_zero_default(self) -> None:
        """Config is maturity-less: maturity_ts=0 means 'gateway resolves it'."""
        req = pb.PtPriceRequest(symbol="PT-wstETH", chain="ethereum")
        assert req.maturity_ts == 0


class TestPtPriceAvailableResponse:
    def test_available_carries_measured_price(self) -> None:
        resp = pb.PtPriceResponse(
            symbol="PT-USDe-26DEC2024",
            chain="ethereum",
            quote="USD",
            price="0.9821",
            availability=pb.PT_PRICE_AVAILABILITY_AVAILABLE,
            confidence=0.97,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_HIGH,
            underlying_price="1.0003",
            pt_to_asset_rate="0.9818",
            source="composition:getPtToAssetRate×coingecko",
            timestamp=1_700_000_000,
            stale=False,
            maturity_ts=1_735_171_200,
            days_to_maturity=37,
        )
        back = _roundtrip(resp)
        assert back.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        assert back.price == "0.9821"
        # A measured price parses to a real Decimal.
        assert Decimal(back.price) == Decimal("0.9821")
        assert back.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_HIGH
        # Composition transparency survives the wire for re-derivation.
        assert Decimal(back.underlying_price) * Decimal(back.pt_to_asset_rate) == Decimal(
            "1.0003"
        ) * Decimal("0.9818")

    def test_estimated_band_when_rate_defaulted(self) -> None:
        """pt_to_asset_rate defaulted at-par → AVAILABLE but ESTIMATED band."""
        resp = pb.PtPriceResponse(
            symbol="PT-foo",
            chain="ethereum",
            price="1.0001",
            availability=pb.PT_PRICE_AVAILABILITY_AVAILABLE,
            confidence=0.6,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_ESTIMATED,
            underlying_price="1.0001",
            pt_to_asset_rate="1.0",
            source="composition:default-at-par×coingecko",
        )
        back = _roundtrip(resp)
        assert back.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        assert back.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_ESTIMATED
        assert back.price != ""


class TestPtPriceUnmeasuredIsNotZero:
    """The core Empty ≠ Zero invariant — unmeasured must never read as 0."""

    def test_unmeasured_has_no_numeric_price(self) -> None:
        resp = pb.PtPriceResponse(
            symbol="PT-unpriceable",
            chain="ethereum",
            quote="USD",
            # NO price set — underlying could not be priced.
            availability=pb.PT_PRICE_AVAILABILITY_UNMEASURED,
            confidence=0.0,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_UNAVAILABLE,
            source="composition:underlying-unpriceable",
        )
        back = _roundtrip(resp)
        assert back.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        # Empty ≠ Zero: the price string is EMPTY, not "0".
        assert back.price == ""
        assert back.price != "0"
        # And it must NOT be coercible to a measured Decimal zero by a naive reader.
        # A correct consumer gates on availability and never calls Decimal("").
        assert not _looks_like_measured_zero(back)

    def test_errored_has_no_numeric_price(self) -> None:
        resp = pb.PtPriceResponse(
            symbol="PT-broken-read",
            chain="ethereum",
            availability=pb.PT_PRICE_AVAILABILITY_ERRORED,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_UNAVAILABLE,
            source="composition:getPtToAssetRate-raised",
        )
        back = _roundtrip(resp)
        assert back.availability == pb.PT_PRICE_AVAILABILITY_ERRORED
        assert back.price == ""
        assert not _looks_like_measured_zero(back)

    def test_old_gateway_default_decodes_as_unmeasured(self) -> None:
        """A response built without availability decodes UNSPECIFIED → fail closed."""
        resp = pb.PtPriceResponse(symbol="PT-foo", chain="ethereum")
        back = _roundtrip(resp)
        # Zero-value enum is the safe default (mirrors AccountingBackendStatus).
        assert back.availability == pb.PT_PRICE_AVAILABILITY_UNSPECIFIED
        assert back.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_UNSPECIFIED
        assert back.price == ""
        assert not _looks_like_measured_zero(back)

    def test_measured_zero_is_distinguishable_from_unmeasured(self) -> None:
        """A genuine measured zero (AVAILABLE + price='0') ≠ unmeasured (empty)."""
        measured_zero = pb.PtPriceResponse(
            symbol="PT-worthless",
            chain="ethereum",
            price="0",
            availability=pb.PT_PRICE_AVAILABILITY_AVAILABLE,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_HIGH,
        )
        unmeasured = pb.PtPriceResponse(
            symbol="PT-worthless",
            chain="ethereum",
            availability=pb.PT_PRICE_AVAILABILITY_UNMEASURED,
        )
        mz = _roundtrip(measured_zero)
        um = _roundtrip(unmeasured)
        # Same symbol, but the contract keeps them unambiguously distinct.
        assert mz.availability != um.availability
        assert mz.price == "0" and Decimal(mz.price) == Decimal("0")
        assert um.price == ""
        assert _looks_like_measured_zero(mz)
        assert not _looks_like_measured_zero(um)


def _looks_like_measured_zero(resp: pb.PtPriceResponse) -> bool:
    """Mirror the contract a correct client must implement.

    A value is a MEASURED zero iff availability is AVAILABLE *and* the price
    string is a present, parseable ``0``. An empty price string — regardless of
    availability — is unmeasured and must never be coerced to Decimal(0).
    """
    if resp.availability != pb.PT_PRICE_AVAILABILITY_AVAILABLE:
        return False
    if resp.price == "":
        return False
    return Decimal(resp.price) == Decimal("0")


class TestMarketServiceRpcSurface:
    def test_get_pt_price_rpc_is_declared(self) -> None:
        from almanak.gateway.proto import gateway_pb2_grpc as grpc_mod

        # Stub exposes the unary-unary callable; servicer declares the handler.
        assert hasattr(grpc_mod.MarketServiceStub, "__init__")
        assert hasattr(grpc_mod.MarketServiceServicer, "GetPtPrice")
