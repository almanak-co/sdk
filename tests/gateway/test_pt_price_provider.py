"""Provider-level tests for ``MarketService.GetPtPrice`` (VIB-5310, epic VIB-5299).

The wire contract is covered by ``test_pt_price_proto.py`` (VIB-5309). This file
covers the COMPOSITION + honest-availability logic the provider adds:

* both legs measured       → AVAILABLE + HIGH, ``pt_usd = underlying × rate``
                             (rate = PT→SY market rate, VIB-5407 — the discounted
                             mark, NOT the accounting-asset rate)
* rate unavailable         → UNMEASURED, NO price (the at-par 1.0 default is
                             FORBIDDEN), ``pt_to_asset_rate`` left EMPTY (Empty≠Zero)
* underlying unpriceable    → UNMEASURED, NO price (never "0")
* a read raised unexpectedly → ERRORED, NO price
* YT in M1                  → UNMEASURED (held-YT deferred to VIB-5322/M3)
* unknown symbol           → UNMEASURED
* the structural guard: a response can never be AVAILABLE with an empty price.

Helpers (``_resolve_principal_token_ref`` / ``_price_underlying_usd`` /
``_read_pt_market``) are patched so the composition logic is tested in isolation
— no chain access, no aggregator network, mirroring ``test_market_service.py``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import web3

from almanak.connectors._base.gateway_capabilities import PrincipalTokenMarketRef
from almanak.framework.data.interfaces import AllDataSourcesFailed, PriceResult
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2 as pb
from almanak.gateway.services.market_service import (
    MarketServiceServicer,
    _build_pt_price_response,
    _resolve_maturity_ts,
    _UnpriceableUnderlying,
)


@pytest.fixture
def market_service() -> MarketServiceServicer:
    svc = MarketServiceServicer(GatewaySettings())
    svc._initialized = True  # skip lazy init / source construction
    return svc


@pytest.fixture
def mock_context() -> MagicMock:
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


# A representative on-chain ``expiry()`` timestamp (PT-wstETH-25JUN2026, unix s).
# Used to prove the response stamps the on-chain expiry, not the static echo.
_ONCHAIN_EXPIRY = 1_782_777_600


def _underlying(price: str = "1.0003", confidence: float = 0.97, stale: bool = False) -> PriceResult:
    return PriceResult(
        price=Decimal(price),
        source="coingecko",
        timestamp=datetime.now(UTC),
        confidence=confidence,
        stale=stale,
    )


def _pt_ref(family: str = "PT", maturity_ts: int = 1_754_956_800) -> PrincipalTokenMarketRef:
    return PrincipalTokenMarketRef(
        protocol="pendle",
        market_address="0x177768caf9d0e036725a51d3f60d7e20f2d4d194",
        underlying_token="0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
        family=family,
        maturity_ts=maturity_ts,
    )


def _request(symbol: str = "PT-sUSDe-13AUG2026", chain: str = "ethereum") -> pb.PtPriceRequest:
    return pb.PtPriceRequest(symbol=symbol, chain=chain, quote="USD")


class TestGetPtPriceAvailableHigh:
    @pytest.mark.asyncio
    async def test_both_legs_measured_is_available_high(self, market_service, mock_context):
        """Underlying priced + on-chain rate read → AVAILABLE + HIGH, composed.

        The on-chain ``expiry()`` (VIB-5384) is the authoritative maturity — the
        response stamps the on-chain value (here ``_ONCHAIN_EXPIRY``), NOT the
        resolver's static ``ref.maturity_ts`` echo (a different value below), so
        the dashboard/valuer see the real timestamp even when the request carries
        no maturity hint (the normal call path, where ``ref.maturity_ts == 0``).
        """
        # Static ref maturity differs from the on-chain read to prove the on-chain
        # value wins. days_to_maturity is derived from the SAME on-chain expiry.
        static_ref = _pt_ref(maturity_ts=1_700_000_000)
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=static_ref),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.95"), 120, _ONCHAIN_EXPIRY, "")),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        assert resp.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_HIGH
        # pt_usd = 1.00 × 0.95
        assert Decimal(resp.price) == Decimal("0.95")
        assert resp.underlying_price == "1.00"
        assert resp.pt_to_asset_rate == "0.95"
        assert resp.confidence == pytest.approx(0.9)  # HIGH carries the underlying confidence
        assert resp.days_to_maturity == 120
        # Authoritative on-chain expiry stamped (not the static 1_700_000_000 echo).
        assert resp.maturity_ts == _ONCHAIN_EXPIRY
        assert "getPtToSyRate" in resp.source
        mock_context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_mark_uses_pt_to_sy_rate_not_asset_rate_vib5407(self, market_service, mock_context):
        """VIB-5407: the composed PT/USD mark is driven by the PT→SY rate (the
        discounted market price), NOT getPtToAssetRate (the accounting-asset rate
        that accretes to ~par for a wrapped-staking SY and over-marks the held PT).

        Real Arbitrum PT-wstETH-25JUN2026 reads (live, captured): the asset rate
        is ~0.99993 (≈ par) while the PT→SY rate is ~0.80787 (matching the AMM
        swap). The reader exposes BOTH; ``_read_pt_market`` must select the SY rate
        so the mark is ~underlying × 0.808, not ~underlying × 1.0.
        """
        wsteth_usd = "2065.1422549467247"
        sy_rate = Decimal("0.807869123467764824")
        asset_rate = Decimal("0.999928097005235031")

        fake_reader = MagicMock()
        fake_reader.get_pt_to_sy_rate.return_value = sy_rate
        fake_reader.get_pt_to_asset_rate.return_value = asset_rate
        fake_reader.get_market_expiry_ts.return_value = _ONCHAIN_EXPIRY

        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(
                market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying(wsteth_usd, 0.97))
            ),
            patch.object(market_service, "_build_pt_reader", return_value=fake_reader),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        # The mark is underlying × SY-rate (discounted), not underlying × asset-rate.
        assert Decimal(resp.price) == Decimal(wsteth_usd) * sy_rate
        assert Decimal(resp.price) != Decimal(wsteth_usd) * asset_rate
        # The echoed rate field carries the SY (mark) rate.
        assert Decimal(resp.pt_to_asset_rate) == sy_rate
        # The over-mark would have been ~24% higher — assert the gap is removed.
        over_mark = Decimal(wsteth_usd) * asset_rate
        assert Decimal(resp.price) < over_mark
        # The money path read the SY rate; it never used the asset rate for the mark.
        fake_reader.get_pt_to_sy_rate.assert_called_once()
        fake_reader.get_pt_to_asset_rate.assert_not_called()
        assert "getPtToSyRate" in resp.source

    @pytest.mark.asyncio
    async def test_available_falls_back_to_static_maturity_when_onchain_unread(self, market_service, mock_context):
        """If the on-chain expiry read failed (expiry_ts=None) but the connector
        statically knew the maturity, the response stamps the static value — never
        fabricates one (Empty≠Zero)."""
        with (
            patch.object(
                market_service, "_resolve_principal_token_ref", return_value=_pt_ref(maturity_ts=1_754_956_800)
            ),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.95"), None, None, "")),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        assert resp.maturity_ts == 1_754_956_800  # static fallback
        assert resp.days_to_maturity == 0  # unread days → 0 (not fabricated)

    @pytest.mark.asyncio
    async def test_available_maturity_unset_when_neither_onchain_nor_static_known(self, market_service, mock_context):
        """Neither the on-chain expiry nor a static maturity known → maturity_ts
        stays unset (0). Empty≠Zero: an unmeasured maturity is never fabricated."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref(maturity_ts=0)),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.95"), None, None, "")),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        assert resp.maturity_ts == 0  # unset — never fabricated

    @pytest.mark.asyncio
    async def test_fresh_both_legs_measured_pt_to_asset_rate_emitted(self, market_service, mock_context):
        """HIGH path emits the measured pt_to_asset_rate (composition transparency)."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.95"), 30, _ONCHAIN_EXPIRY, "")),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)
        assert resp.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_HIGH
        assert resp.stale is False
        assert resp.pt_to_asset_rate == "0.95"


class TestGetPtPriceStaleIsEstimated:
    @pytest.mark.asyncio
    async def test_stale_underlying_is_available_estimated_not_high(self, market_service, mock_context):
        """Ratified AC: HIGH only when FRESH. A measured-but-stale underlying →
        AVAILABLE + ESTIMATED + stale flag (NOT HIGH). ESTIMATED is reserved for
        measured-but-degraded, never a fabricated input."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(
                market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.7, stale=True))
            ),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.9"), 30, _ONCHAIN_EXPIRY, "")),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        assert resp.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_ESTIMATED
        assert resp.stale is True
        # still a real composed price from two MEASURED legs (rate × underlying)
        assert Decimal(resp.price) == Decimal("0.9")
        assert resp.pt_to_asset_rate == "0.9"
        assert resp.confidence <= 0.5  # degraded, capped


class TestGetPtPriceMissingRate:
    @pytest.mark.asyncio
    async def test_missing_pt_rate_is_unmeasured_never_at_par(self, market_service, mock_context):
        """Ratified AC: a missing pt_to_asset_rate → UNMEASURED with NO price.
        The at-par (rate=1.0) default is FORBIDDEN — it overvalues the PT."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.05", 0.9))),
            patch.object(
                market_service,
                "_read_pt_market",
                return_value=(None, 60, _ONCHAIN_EXPIRY, "pt_to_sy_rate-read-failed"),
            ),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        assert resp.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_UNAVAILABLE
        assert resp.price == ""  # NO price — never the at-par 1.05
        assert resp.pt_to_asset_rate == ""  # rate not measured
        assert resp.confidence == 0.0
        # underlying WAS measured → echoed for transparency
        assert resp.underlying_price == "1.05"
        assert "pt-rate-unavailable" in resp.source
        # Maturity is still authoritative from the on-chain expiry even when the
        # rate read failed — the dashboard maturity column does not depend on rate.
        assert resp.maturity_ts == _ONCHAIN_EXPIRY
        assert resp.days_to_maturity == 60


class TestGetPtPriceUnmeasured:
    @pytest.mark.asyncio
    async def test_underlying_unpriceable_is_unmeasured_no_price(self, market_service, mock_context):
        """Underlying has no price source → UNMEASURED with NO numeric price."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(
                market_service,
                "_price_underlying_usd",
                AsyncMock(side_effect=_UnpriceableUnderlying("all sources failed")),
            ),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        assert resp.price == ""  # never "0"
        assert resp.confidence == 0.0
        assert resp.confidence_band == pb.PT_PRICE_CONFIDENCE_BAND_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_unknown_symbol_is_unmeasured(self, market_service, mock_context):
        """Symbol not resolvable → UNMEASURED, no price (expected-no-data)."""
        with patch.object(market_service, "_resolve_principal_token_ref", return_value=None):
            resp = await market_service.GetPtPrice(_request("PT-NOPE"), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        assert resp.price == ""

    @pytest.mark.asyncio
    async def test_yt_is_unmeasured_in_m1(self, market_service, mock_context):
        """Held-YT valuation is deferred to VIB-5322/M3 → UNMEASURED, never a guess."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref(family="YT")),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying())) as priced,
        ):
            resp = await market_service.GetPtPrice(_request("YT-sUSDe-13AUG2026"), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        assert resp.price == ""
        assert "yt-valuation-deferred" in resp.source
        priced.assert_not_called()  # never even prices the underlying for YT

    @pytest.mark.asyncio
    async def test_unknown_family_is_unmeasured_never_priced_as_pt(self, market_service, mock_context):
        """A non-PT/YT family (e.g. a future "LP") must NOT silently price as PT.

        ``PrincipalTokenMarketRef.family`` is a free str documented as PT/YT/LP;
        an unrecognized family must fail closed to UNMEASURED with a clear reason,
        never fall through to PT composition (which would misprice it).
        """
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref(family="LP")),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying())) as priced,
        ):
            resp = await market_service.GetPtPrice(_request("LP-sUSDe-13AUG2026"), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        assert resp.price == ""
        assert "unrecognized-family" in resp.source
        assert "LP" in resp.source
        priced.assert_not_called()  # never even prices the underlying for an unknown family


class TestGetPtPriceErrored:
    @pytest.mark.asyncio
    async def test_unexpected_read_error_is_errored_no_price(self, market_service, mock_context):
        """An unexpected exception (not 'unpriceable') → ERRORED with NO price."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(side_effect=RuntimeError("rpc exploded"))),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_ERRORED
        assert resp.price == ""
        assert resp.confidence == 0.0


class TestGetPtPriceValidation:
    @pytest.mark.asyncio
    async def test_empty_symbol_is_invalid_argument(self, market_service, mock_context):
        resp = await market_service.GetPtPrice(pb.PtPriceRequest(symbol="", chain="ethereum"), mock_context)
        import grpc

        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNSPECIFIED

    @pytest.mark.asyncio
    async def test_bad_chain_is_invalid_argument(self, market_service, mock_context):
        resp = await market_service.GetPtPrice(pb.PtPriceRequest(symbol="PT-x", chain="not-a-chain"), mock_context)
        import grpc

        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNSPECIFIED

    @pytest.mark.asyncio
    async def test_non_usd_quote_is_rejected(self, market_service, mock_context):
        """M1 is USD-only: a EUR quote is rejected, never composed as USD-labelled-EUR."""
        import grpc

        with patch.object(market_service, "_resolve_principal_token_ref") as resolve:
            resp = await market_service.GetPtPrice(
                pb.PtPriceRequest(symbol="PT-sUSDe-13AUG2026", chain="ethereum", quote="EUR"),
                mock_context,
            )
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNSPECIFIED
        resolve.assert_not_called()  # rejected before any sourcing

    @pytest.mark.asyncio
    async def test_empty_quote_defaults_to_usd(self, market_service, mock_context):
        """Empty quote == default USD — accepted, not rejected."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.95"), 30, _ONCHAIN_EXPIRY, "")),
        ):
            resp = await market_service.GetPtPrice(
                pb.PtPriceRequest(symbol="PT-sUSDe-13AUG2026", chain="ethereum"), mock_context
            )
        mock_context.set_code.assert_not_called()
        assert resp.quote == "USD"
        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE


class TestPerimeterLiveness:
    """Audit follow-ups: the blocking on-chain read must not stall the event loop,
    and the direct-mode reader must carry a bounded RPC timeout."""

    @pytest.mark.asyncio
    async def test_rate_read_dispatched_off_event_loop(self, market_service, mock_context):
        """The blocking ``_read_pt_market`` runs via ``asyncio.to_thread`` (off-loop)."""
        with (
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(
                market_service, "_read_pt_market", return_value=(Decimal("0.95"), 30, _ONCHAIN_EXPIRY, "")
            ) as read,
            patch(
                "almanak.gateway.services.market_service.asyncio.to_thread", wraps=asyncio.to_thread
            ) as to_thread_spy,
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        to_thread_spy.assert_called_once()
        # the FUNCTION dispatched off-loop is exactly the blocking rate read
        assert to_thread_spy.call_args.args[0] == read

    def test_gateway_reader_built_in_gateway_mode_no_httpprovider(self):
        """VIB-5348: the gateway builds the reader in GATEWAY mode.

        The connector forwards the injected gateway-native rpc client into the
        reader's ``gateway_client`` seam — NOT an ``rpc_url`` — so the reader runs
        with ``web3 is None`` and no raw ``HTTPProvider`` is instantiated on the
        hosted perimeter.
        """
        from almanak.connectors.pendle.gateway.provider import PendleGatewayConnector
        from almanak.connectors.pendle.on_chain_reader import PendleOnChainReader

        sentinel_client = object()
        with patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader") as MockReader:
            PendleGatewayConnector().build_principal_token_market_reader(
                chain="ethereum", rpc_client=sentinel_client
            )
        MockReader.assert_called_once_with(chain="ethereum", gateway_client=sentinel_client)

        # And a real reader built that way carries NO web3 provider.
        reader = PendleGatewayConnector().build_principal_token_market_reader(
            chain="ethereum", rpc_client=sentinel_client
        )
        assert isinstance(reader, PendleOnChainReader)
        assert reader.web3 is None

    def test_direct_mode_reader_wires_timeout_into_web3(self):
        """The timeout is NOT inert — it reaches the web3 HTTPProvider request_kwargs."""
        from almanak.connectors.pendle.on_chain_reader import PendleOnChainReader

        with patch.object(web3.Web3, "HTTPProvider", wraps=web3.Web3.HTTPProvider) as provider_spy:
            PendleOnChainReader(chain="ethereum", rpc_url="http://localhost:8545", request_timeout_seconds=12.0)

        provider_spy.assert_called_once()
        assert provider_spy.call_args.kwargs["request_kwargs"] == {"timeout": 12.0}


class TestAvailableNeverEmptyGuard:
    """The structural invariant: AVAILABLE implies a non-empty price."""

    def test_builder_rejects_available_with_empty_price(self):
        with pytest.raises(ValueError, match="availability=AVAILABLE requires a non-empty price"):
            _build_pt_price_response(
                symbol="PT-x",
                chain="ethereum",
                quote="USD",
                availability=pb.PT_PRICE_AVAILABILITY_AVAILABLE,
                confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_HIGH,
                price="",
            )

    def test_builder_allows_available_with_measured_zero(self):
        """Empty≠Zero: a measured zero string IS allowed for AVAILABLE."""
        resp = _build_pt_price_response(
            symbol="PT-x",
            chain="ethereum",
            quote="USD",
            availability=pb.PT_PRICE_AVAILABILITY_AVAILABLE,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_HIGH,
            price="0",
        )
        assert resp.price == "0"

    def test_builder_allows_unmeasured_with_empty_price(self):
        resp = _build_pt_price_response(
            symbol="PT-x",
            chain="ethereum",
            quote="USD",
            availability=pb.PT_PRICE_AVAILABILITY_UNMEASURED,
            confidence_band=pb.PT_PRICE_CONFIDENCE_BAND_UNAVAILABLE,
        )
        assert resp.price == ""


class TestGetPtPriceTelemetry:
    """VIB-5312 — the price path emits structured telemetry for exactly the four
    observable fields (price source, confidence, staleness, unavailable reason)
    for BOTH a measured and an unmeasured/stale mark. Every GetPtPrice outcome
    funnels through ``_build_pt_price_response`` so a single record is emitted.
    """

    def _telemetry_record(self, caplog):
        """The single ``pt_telemetry`` log record emitted for the call (or None)."""
        records = [r for r in caplog.records if getattr(r, "pt_telemetry", False)]
        assert len(records) == 1, f"expected exactly one PT telemetry record, got {len(records)}"
        return records[0]

    @pytest.mark.asyncio
    async def test_measured_high_emits_observable_fields(self, market_service, mock_context, caplog):
        """A fresh AVAILABLE+HIGH mark logs all four observable fields, no
        unavailable reason (Empty≠Zero: AVAILABLE → reason is None), no secrets."""
        with (
            caplog.at_level("DEBUG", logger="almanak.gateway.services.market_service"),
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.9))),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.95"), 30, _ONCHAIN_EXPIRY, "")),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_AVAILABLE
        rec = self._telemetry_record(caplog)
        assert rec.availability == "AVAILABLE"
        assert rec.confidence_band == "HIGH"
        assert rec.confidence == pytest.approx(0.9)
        assert rec.stale is False
        # Empty≠Zero in observability: an AVAILABLE mark has NO unavailable reason.
        assert rec.unavailable_reason is None
        assert "getPtToSyRate" in rec.price_source
        # No secrets leaked — only public market identity + the four signals.
        assert rec.symbol == "PT-sUSDe-13AUG2026"
        assert rec.chain == "ethereum"

    @pytest.mark.asyncio
    async def test_stale_mark_emits_stale_signal(self, market_service, mock_context, caplog):
        """A measured-but-stale mark carries ``stale=True`` in telemetry even though
        the price is AVAILABLE (the freshness signal rides separately from the band)."""
        with (
            caplog.at_level("DEBUG", logger="almanak.gateway.services.market_service"),
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(
                market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.00", 0.7, stale=True))
            ),
            patch.object(market_service, "_read_pt_market", return_value=(Decimal("0.9"), 30, _ONCHAIN_EXPIRY, "")),
        ):
            await market_service.GetPtPrice(_request(), mock_context)

        rec = self._telemetry_record(caplog)
        assert rec.availability == "AVAILABLE"
        assert rec.confidence_band == "ESTIMATED"
        assert rec.stale is True
        assert rec.unavailable_reason is None  # still a measured price

    @pytest.mark.asyncio
    async def test_unmeasured_emits_unavailable_reason(self, market_service, mock_context, caplog):
        """An UNMEASURED mark (missing rate) carries the stamped reason and a
        UNAVAILABLE band — Empty≠Zero: confidence 0.0, a real reason string."""
        with (
            caplog.at_level("INFO", logger="almanak.gateway.services.market_service"),
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(return_value=_underlying("1.05", 0.9))),
            patch.object(
                market_service,
                "_read_pt_market",
                return_value=(None, 60, _ONCHAIN_EXPIRY, "pt_to_sy_rate-read-failed"),
            ),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_UNMEASURED
        rec = self._telemetry_record(caplog)
        assert rec.availability == "UNMEASURED"
        assert rec.confidence_band == "UNAVAILABLE"
        assert rec.confidence == 0.0
        assert rec.stale is False
        # The unavailable reason reuses the provider's stamped source (no parallel
        # reason vocabulary — VIB-5312 scope guard).
        assert rec.unavailable_reason is not None
        assert "pt-rate-unavailable" in rec.unavailable_reason

    @pytest.mark.asyncio
    async def test_errored_emits_warning_with_reason(self, market_service, mock_context, caplog):
        """An ERRORED read logs at WARNING with the errored reason."""
        with (
            caplog.at_level("WARNING", logger="almanak.gateway.services.market_service"),
            patch.object(market_service, "_resolve_principal_token_ref", return_value=_pt_ref()),
            patch.object(market_service, "_price_underlying_usd", AsyncMock(side_effect=RuntimeError("rpc exploded"))),
        ):
            resp = await market_service.GetPtPrice(_request(), mock_context)

        assert resp.availability == pb.PT_PRICE_AVAILABILITY_ERRORED
        rec = self._telemetry_record(caplog)
        assert rec.availability == "ERRORED"
        assert rec.levelname == "WARNING"
        assert rec.unavailable_reason is not None

    def test_unavailable_reason_is_none_for_available(self):
        """``_pt_unavailable_reason`` — AVAILABLE → None (Empty≠Zero)."""
        from almanak.gateway.services.market_service import _pt_unavailable_reason

        assert _pt_unavailable_reason(pb.PT_PRICE_AVAILABILITY_AVAILABLE, "composition:x") is None

    def test_unavailable_reason_uses_source_for_unmeasured(self):
        """Non-AVAILABLE reuses the stamped source string as the reason."""
        from almanak.gateway.services.market_service import _pt_unavailable_reason

        reason = _pt_unavailable_reason(pb.PT_PRICE_AVAILABILITY_UNMEASURED, "unmeasured:underlying-unpriceable:0xabc")
        assert reason == "unmeasured:underlying-unpriceable:0xabc"

    def test_unavailable_reason_falls_back_to_label_on_empty_source(self):
        """A provider-bug empty source still yields a non-blank reason for the operator."""
        from almanak.gateway.services.market_service import _pt_unavailable_reason

        assert _pt_unavailable_reason(pb.PT_PRICE_AVAILABILITY_ERRORED, "") == "errored"


class TestResolveMaturityTs:
    """``_resolve_maturity_ts`` chooses the authoritative PT maturity (VIB-5384)."""

    def test_onchain_expiry_wins_over_static(self):
        # On-chain expiry is authoritative; the static echo is the fallback only.
        assert _resolve_maturity_ts(_ONCHAIN_EXPIRY, 1_700_000_000) == _ONCHAIN_EXPIRY

    def test_static_fallback_when_onchain_unread(self):
        assert _resolve_maturity_ts(None, 1_754_956_800) == 1_754_956_800

    def test_zero_when_neither_known(self):
        # Empty≠Zero: an unmeasured maturity is left unset, never fabricated.
        assert _resolve_maturity_ts(None, 0) == 0

    def test_non_positive_onchain_falls_back(self):
        # A non-positive on-chain read is not a real maturity → use the static one.
        assert _resolve_maturity_ts(0, 1_754_956_800) == 1_754_956_800


class TestPendlePrincipalTokenResolution:
    """Direct tests of the connector's symbol→market+underlying resolution.

    Exercises the (decomposed) provider helpers so the no-egress mapping is
    covered without going through the gateway servicer.
    """

    @pytest.fixture
    def connector(self):
        from almanak.connectors.pendle.gateway.provider import PendleGatewayConnector

        return PendleGatewayConnector()

    def test_resolve_pt_symbol(self, connector):
        ref = connector.resolve_principal_token_ref(symbol="PT-sUSDe-13AUG2026", chain="ethereum")
        assert ref is not None
        assert ref.protocol == "pendle"
        assert ref.family == "PT"
        assert ref.market_address.lower() == "0x177768caf9d0e036725a51d3f60d7e20f2d4d194"
        assert ref.underlying_token  # non-empty SY-mint token

    def test_resolve_yt_symbol_family(self, connector):
        ref = connector.resolve_principal_token_ref(symbol="YT-sUSDe-13AUG2026", chain="ethereum")
        assert ref is not None
        assert ref.family == "YT"

    def test_resolve_is_case_insensitive(self, connector):
        ref = connector.resolve_principal_token_ref(symbol="pt-wsteth-25jun2026", chain="arbitrum")
        assert ref is not None
        assert ref.family == "PT"

    def test_resolve_passes_through_maturity(self, connector):
        ref = connector.resolve_principal_token_ref(
            symbol="PT-sUSDe-13AUG2026", chain="ethereum", maturity_ts=1_754_956_800
        )
        assert ref is not None
        assert ref.maturity_ts == 1_754_956_800

    def test_resolve_empty_symbol_is_none(self, connector):
        assert connector.resolve_principal_token_ref(symbol="   ", chain="ethereum") is None

    def test_resolve_unknown_symbol_is_none(self, connector):
        assert connector.resolve_principal_token_ref(symbol="PT-NOPE-1JAN2099", chain="ethereum") is None

    def test_resolve_unknown_chain_is_none(self, connector):
        assert connector.resolve_principal_token_ref(symbol="PT-sUSDe-13AUG2026", chain="solana") is None

    def test_resolve_market_without_underlying_is_none(self, connector):
        """A resolvable market with no static underlying → None (never a 0)."""
        with patch("almanak.connectors.pendle.gateway.provider._lookup_underlying_token", return_value=None):
            assert connector.resolve_principal_token_ref(symbol="PT-sUSDe-13AUG2026", chain="ethereum") is None

    def test_chains_advertised(self, connector):
        chains = connector.principal_token_price_chains()
        assert "ethereum" in chains
        assert "arbitrum" in chains

    def test_build_reader_supported_and_unsupported_chain(self, connector):
        from almanak.connectors.pendle.on_chain_reader import PendleOnChainReader

        sentinel_client = object()
        reader = connector.build_principal_token_market_reader(chain="ethereum", rpc_client=sentinel_client)
        assert isinstance(reader, PendleOnChainReader)
        assert reader.web3 is None  # gateway mode — no raw HTTPProvider
        # No PT-oracle mapping for base → None (gateway emits UNMEASURED, never at-par).
        assert connector.build_principal_token_market_reader(chain="base", rpc_client=sentinel_client) is None


class TestPendleProviderHelpers:
    """Unit coverage of the decomposed module-level resolution helpers."""

    def test_pt_family(self):
        from almanak.connectors.pendle.gateway.provider import _pt_family

        assert _pt_family("YT-sUSDe-13AUG2026") == "YT"
        assert _pt_family("yt-wsteth") == "YT"
        assert _pt_family("PT-sUSDe-13AUG2026") == "PT"

    def test_lookup_market_address_exact_and_case_insensitive_and_miss(self):
        from almanak.connectors.pendle.gateway.provider import _lookup_market_address

        exact = _lookup_market_address("PT-sUSDe-13AUG2026", "ethereum", "PT")
        assert exact is not None
        ci = _lookup_market_address("pt-susde-13aug2026", "ethereum", "PT")
        assert ci == exact
        assert _lookup_market_address("PT-NOPE", "ethereum", "PT") is None

    def test_lookup_underlying_token_hit_and_miss(self):
        from almanak.connectors.pendle.gateway.provider import _lookup_underlying_token

        hit = _lookup_underlying_token("0x177768caf9d0e036725a51d3f60d7e20f2d4d194", "ethereum")
        assert hit  # sUSDe SY-mint token
        assert _lookup_underlying_token("0xdeadbeef", "ethereum") is None


class TestServicerResolutionAndReader:
    """Cover the servicer's capability-dispatch + reader-build helpers."""

    def test_resolve_principal_token_ref_dispatches_to_pendle(self, market_service):
        ref = market_service._resolve_principal_token_ref("PT-sUSDe-13AUG2026", "ethereum", 0)
        assert ref is not None
        assert ref.protocol == "pendle"

    def test_resolve_principal_token_ref_unknown_is_none(self, market_service):
        assert market_service._resolve_principal_token_ref("PT-NOPE", "ethereum", 0) is None

    def test_build_pt_reader_known_protocol(self, market_service):
        from almanak.connectors.pendle.on_chain_reader import PendleOnChainReader

        reader = market_service._build_pt_reader("pendle", "ethereum")
        assert isinstance(reader, PendleOnChainReader)
        # VIB-5348: gateway-native transport — the reader holds no web3 provider.
        assert reader.web3 is None

    def test_build_pt_reader_unknown_protocol_is_none(self, market_service):
        assert market_service._build_pt_reader("not-a-protocol", "ethereum") is None

    def test_read_pt_market_no_reader_returns_estimated_signal(self, market_service):
        ref = _pt_ref()
        with patch.object(market_service, "_build_pt_reader", return_value=None):
            rate, days, expiry_ts, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate is None
        assert days is None
        assert expiry_ts is None
        assert reason == "rate-reader-unavailable"

    def test_read_pt_market_reads_rate_and_expiry(self, market_service):
        """Rate + on-chain expiry are read; days_to_maturity is DERIVED from the
        SAME expiry timestamp so the two can never disagree (VIB-5384)."""
        import math
        import time as _time

        ref = _pt_ref()
        # Expiry ~90 days out → days_to_maturity ceil((expiry-now)/86400).
        expiry = int(_time.time()) + 90 * 86400
        fake_reader = MagicMock()
        # VIB-5407: the money mark is the PT→SY rate, not the asset rate.
        fake_reader.get_pt_to_sy_rate.return_value = Decimal("0.97")
        fake_reader.get_market_expiry_ts.return_value = expiry
        with patch.object(market_service, "_build_pt_reader", return_value=fake_reader):
            rate, days, expiry_ts, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate == Decimal("0.97")
        assert expiry_ts == expiry
        # days derived from the on-chain expiry (NOT a separate get_days call)
        assert days == max(0, math.ceil((expiry - int(_time.time())) / 86400))
        assert reason == ""
        # The provider derives days from expiry itself, never the legacy helper.
        fake_reader.get_days_to_maturity.assert_not_called()

    def test_read_pt_market_expiry_unread_leaves_maturity_unmeasured(self, market_service):
        """A failed on-chain expiry read → expiry_ts and days both None (Empty≠Zero),
        never a fabricated 0."""
        ref = _pt_ref()
        fake_reader = MagicMock()
        fake_reader.get_pt_to_sy_rate.return_value = Decimal("0.97")
        fake_reader.get_market_expiry_ts.return_value = None
        with patch.object(market_service, "_build_pt_reader", return_value=fake_reader):
            rate, days, expiry_ts, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate == Decimal("0.97")
        assert expiry_ts is None
        assert days is None
        assert reason == ""

    def test_read_pt_market_non_positive_expiry_leaves_maturity_unmeasured(self, market_service):
        """A reader that returns a non-positive expiry must NOT stamp days = 0 while
        maturity_ts falls back: _read_pt_market normalizes <= 0 to unread so days
        and maturity_ts stay consistent (VIB-5384, Empty≠Zero). The canonical
        reader already maps <= 0 → None, but the gateway must not trust every
        PrincipalTokenMarketReader implementation to do so."""
        ref = _pt_ref()
        fake_reader = MagicMock()
        fake_reader.get_pt_to_sy_rate.return_value = Decimal("0.97")
        fake_reader.get_market_expiry_ts.return_value = 0
        with patch.object(market_service, "_build_pt_reader", return_value=fake_reader):
            rate, days, expiry_ts, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate == Decimal("0.97")
        assert expiry_ts is None
        assert days is None
        assert reason == ""

    def test_read_pt_market_non_positive_rate_is_none(self, market_service):
        ref = _pt_ref()
        fake_reader = MagicMock()
        fake_reader.get_pt_to_sy_rate.return_value = Decimal("0")
        fake_reader.get_market_expiry_ts.return_value = None
        with patch.object(market_service, "_build_pt_reader", return_value=fake_reader):
            rate, _days, _expiry, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate is None
        assert reason == "pt_to_sy_rate-non-positive"

    def test_read_pt_market_rate_read_raises_is_graceful(self, market_service):
        ref = _pt_ref()
        fake_reader = MagicMock()
        fake_reader.get_pt_to_sy_rate.side_effect = RuntimeError("rpc down")
        fake_reader.get_market_expiry_ts.return_value = None
        with patch.object(market_service, "_build_pt_reader", return_value=fake_reader):
            rate, days, expiry_ts, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate is None
        assert days is None
        assert expiry_ts is None
        assert reason == "pt_to_sy_rate-read-failed"

    def test_read_pt_market_expiry_read_raises_is_graceful(self, market_service):
        """An exception from the expiry read is swallowed → expiry_ts/days None,
        while the rate read still succeeds independently."""
        ref = _pt_ref()
        fake_reader = MagicMock()
        fake_reader.get_pt_to_sy_rate.return_value = Decimal("0.97")
        fake_reader.get_market_expiry_ts.side_effect = RuntimeError("rpc down")
        with patch.object(market_service, "_build_pt_reader", return_value=fake_reader):
            rate, days, expiry_ts, reason = market_service._read_pt_market(ref, "ethereum")
        assert rate == Decimal("0.97")
        assert expiry_ts is None
        assert days is None
        assert reason == ""


class TestUnpriceableClassification:
    """``_price_underlying_usd`` maps 'all sources failed' → _UnpriceableUnderlying."""

    @pytest.mark.asyncio
    async def test_all_sources_failed_becomes_unpriceable(self, market_service):
        agg = MagicMock()
        agg.get_aggregated_price = AsyncMock(side_effect=AllDataSourcesFailed({"coingecko": "nope"}))
        market_service._price_aggregator = agg
        with patch.object(market_service, "_resolve_token_for_pricing", AsyncMock(return_value=None)):
            with pytest.raises(_UnpriceableUnderlying):
                await market_service._price_underlying_usd("WSTETH", "ethereum")

    @pytest.mark.asyncio
    async def test_unexpected_error_propagates(self, market_service):
        agg = MagicMock()
        agg.get_aggregated_price = AsyncMock(side_effect=RuntimeError("boom"))
        market_service._price_aggregator = agg
        with patch.object(market_service, "_resolve_token_for_pricing", AsyncMock(return_value=None)):
            with pytest.raises(RuntimeError):
                await market_service._price_underlying_usd("WSTETH", "ethereum")
