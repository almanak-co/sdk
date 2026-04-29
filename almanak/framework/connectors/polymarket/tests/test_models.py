"""V2-specific tests for Polymarket models.

V2 introduced:
- New constants: CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2, PUSD, COLLATERAL_ONRAMP,
  COLLATERAL_OFFRAMP, USDCE_POLYGON, USDC_NATIVE_POLYGON, BYTES32_ZERO.
- New helper: build_ctf_exchange_domain(exchange_address) — per-order EIP-712
  domain since the verifyingContract differs by market (regular vs NegRisk).
- New ORDER_TYPES schema: 11 fields (drops V1's taker/expiration/nonce/
  feeRateBps; adds timestamp/metadata/builder).
- New OrderStatus values DELAYED and UNMATCHED.
- New LimitOrderParams shape (no fee_rate_bps; expiration is API-only).

Pin all of these so a regression on the wire shape or the constants surfaces
in unit tests rather than as a 400/401 from the CLOB.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.connectors.polymarket.models import (
    BYTES32_ZERO,
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
    COLLATERAL_OFFRAMP,
    COLLATERAL_ONRAMP,
    CTF_EXCHANGE_V2,
    CTF_EXCHANGE_V2_DOMAIN_NAME,
    CTF_EXCHANGE_V2_DOMAIN_VERSION,
    NEG_RISK_EXCHANGE_V2,
    ORDER_TYPES,
    POLYGON_CHAIN_ID,
    PUSD,
    USDC_NATIVE_POLYGON,
    USDCE_POLYGON,
    LimitOrderParams,
    MarketOrderParams,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    PriceLevel,
    SignatureType,
    SignedOrder,
    UnsignedOrder,
    build_ctf_exchange_domain,
)

# =============================================================================
# V2 contract constants
# =============================================================================


class TestV2Constants:
    """Pin the V2 contract addresses — a typo here would silently sign orders
    against the wrong contract and the CLOB would reject them with an
    invalid-signature error that's painful to trace."""

    def test_pusd_address_pinned(self) -> None:
        """Pin the canonical V2 pUSD address (Polygon mainnet). A drift here
        signs orders against the wrong contract — assert the exact value, not
        just shape, so a one-character regression fails immediately."""
        assert PUSD == "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

    def test_collateral_onramp_address_pinned(self) -> None:
        """Pin the canonical CollateralOnramp address (the wrap entry point)."""
        assert COLLATERAL_ONRAMP == "0x93070a847efEf7F70739046A929D47a521F5B8ee"

    def test_collateral_offramp_address_pinned(self) -> None:
        """Pin the canonical CollateralOfframp address (the unwrap entry point)."""
        assert COLLATERAL_OFFRAMP == "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"

    def test_ctf_exchange_v2_address_pinned(self) -> None:
        assert CTF_EXCHANGE_V2 == "0xE111180000d2663C0091e4f400237545B87B996B"

    def test_neg_risk_exchange_v2_address_pinned(self) -> None:
        assert NEG_RISK_EXCHANGE_V2 == "0xe2222d279d744050d28e00520010520000310F59"

    def test_v2_exchange_addresses_distinct(self) -> None:
        """CTF and NegRisk V2 exchanges must be different contracts."""
        assert CTF_EXCHANGE_V2 != NEG_RISK_EXCHANGE_V2

    def test_pusd_distinct_from_source_assets(self) -> None:
        """pUSD is the V2 trading collateral; USDC.e and native USDC are
        source-of-funds tokens that get wrapped into pUSD via the Onramp.
        Confusing the two would route trades to the wrong contract."""
        assert PUSD != USDCE_POLYGON
        assert PUSD != USDC_NATIVE_POLYGON

    def test_source_assets_distinct(self) -> None:
        """USDC.e (bridged) and native USDC have different addresses."""
        assert USDCE_POLYGON != USDC_NATIVE_POLYGON

    def test_polygon_chain_id(self) -> None:
        """All V2 contracts live on Polygon (137)."""
        assert POLYGON_CHAIN_ID == 137

    def test_bytes32_zero_format(self) -> None:
        """BYTES32_ZERO is the EIP-712-friendly default for empty bytes32."""
        assert BYTES32_ZERO == "0x" + "00" * 32
        assert len(BYTES32_ZERO) == 66


# =============================================================================
# build_ctf_exchange_domain — per-order EIP-712 domain
# =============================================================================


class TestBuildCtfExchangeDomain:
    """V2 builds the EIP-712 domain per-order because the verifyingContract
    is market-specific (regular CTF V2 vs NegRisk V2)."""

    def test_domain_has_canonical_keys(self) -> None:
        d = build_ctf_exchange_domain(CTF_EXCHANGE_V2)
        assert set(d.keys()) == {"name", "version", "chainId", "verifyingContract"}

    def test_domain_name_is_polymarket_ctf_exchange(self) -> None:
        d = build_ctf_exchange_domain(CTF_EXCHANGE_V2)
        assert d["name"] == "Polymarket CTF Exchange"
        assert d["name"] == CTF_EXCHANGE_V2_DOMAIN_NAME

    def test_domain_version_is_2(self) -> None:
        """V2's domain version is "2" — V1 used "1"."""
        d = build_ctf_exchange_domain(CTF_EXCHANGE_V2)
        assert d["version"] == "2"
        assert d["version"] == CTF_EXCHANGE_V2_DOMAIN_VERSION

    def test_domain_chain_id_is_polygon(self) -> None:
        d = build_ctf_exchange_domain(CTF_EXCHANGE_V2)
        assert d["chainId"] == POLYGON_CHAIN_ID

    def test_domain_routes_neg_risk_address(self) -> None:
        """Domain mirrors the input address — the helper does not validate
        it's one of the two known V2 exchanges, so callers can use it for
        future contracts (e.g. testnet)."""
        d = build_ctf_exchange_domain(NEG_RISK_EXCHANGE_V2)
        assert d["verifyingContract"] == NEG_RISK_EXCHANGE_V2

    def test_domains_differ_when_address_differs(self) -> None:
        d_ctf = build_ctf_exchange_domain(CTF_EXCHANGE_V2)
        d_neg = build_ctf_exchange_domain(NEG_RISK_EXCHANGE_V2)
        assert d_ctf["verifyingContract"] != d_neg["verifyingContract"]
        # Other fields are identical.
        assert d_ctf["name"] == d_neg["name"]
        assert d_ctf["version"] == d_neg["version"]
        assert d_ctf["chainId"] == d_neg["chainId"]


# =============================================================================
# V2 ORDER_TYPES schema
# =============================================================================


class TestV2OrderTypesSchema:
    """ORDER_TYPES is the EIP-712 schema for the signed Order struct.
    V2 has 11 fields; V1 had 12. A drift here breaks signature recovery."""

    def test_order_field_count(self) -> None:
        """V2 has exactly 11 signed fields."""
        assert len(ORDER_TYPES["Order"]) == 11

    def test_order_field_names_match_v2(self) -> None:
        """Pin every field name and type — anyone reordering or renaming
        these would silently produce signatures for an entirely different
        struct (the EIP-712 hash changes)."""
        names_types = [(f["name"], f["type"]) for f in ORDER_TYPES["Order"]]
        assert names_types == [
            ("salt", "uint256"),
            ("maker", "address"),
            ("signer", "address"),
            ("tokenId", "uint256"),
            ("makerAmount", "uint256"),
            ("takerAmount", "uint256"),
            ("side", "uint8"),
            ("signatureType", "uint8"),
            ("timestamp", "uint256"),
            ("metadata", "bytes32"),
            ("builder", "bytes32"),
        ]

    def test_order_v1_fields_absent(self) -> None:
        """V1's `taker`, `expiration`, `nonce`, and `feeRateBps` must not
        appear in V2's schema."""
        names = {f["name"] for f in ORDER_TYPES["Order"]}
        assert "taker" not in names
        assert "expiration" not in names
        assert "nonce" not in names
        assert "feeRateBps" not in names

    def test_order_v2_fields_present(self) -> None:
        """V2 additions: timestamp, metadata, builder."""
        names = {f["name"] for f in ORDER_TYPES["Order"]}
        assert "timestamp" in names
        assert "metadata" in names
        assert "builder" in names


# =============================================================================
# CLOB_AUTH_DOMAIN (L1 EIP-712 message)
# =============================================================================


class TestClobAuthDomain:
    """L1 EIP-712 auth — pinning the domain prevents drift that would
    cause derive-api-key 401s."""

    def test_domain_keys(self) -> None:
        assert "name" in CLOB_AUTH_DOMAIN
        assert "version" in CLOB_AUTH_DOMAIN
        assert "chainId" in CLOB_AUTH_DOMAIN

    def test_domain_chain_id_is_polygon(self) -> None:
        assert CLOB_AUTH_DOMAIN["chainId"] == POLYGON_CHAIN_ID

    def test_auth_message_constant(self) -> None:
        assert "control" in CLOB_AUTH_MESSAGE.lower()
        assert "wallet" in CLOB_AUTH_MESSAGE.lower()

    def test_auth_types_has_clobauth(self) -> None:
        """L1 auth uses ClobAuth as the primary type."""
        assert "ClobAuth" in CLOB_AUTH_TYPES


# =============================================================================
# Enums — extended in V2
# =============================================================================


class TestOrderStatusEnumV2:
    """V2 added DELAYED and UNMATCHED to the order-status set so that
    OrderResponse.from_api_response doesn't silently coerce them to LIVE."""

    def test_v1_statuses_still_present(self) -> None:
        assert OrderStatus.LIVE.value == "LIVE"
        assert OrderStatus.MATCHED.value == "MATCHED"
        assert OrderStatus.CANCELLED.value == "CANCELLED"
        assert OrderStatus.EXPIRED.value == "EXPIRED"
        assert OrderStatus.FAILED.value == "FAILED"
        assert OrderStatus.REJECTED.value == "REJECTED"

    def test_v2_added_delayed(self) -> None:
        """DELAYED = matching engine still processing the order."""
        assert OrderStatus.DELAYED.value == "DELAYED"

    def test_v2_added_unmatched(self) -> None:
        """UNMATCHED = IOC/FOK couldn't match any liquidity."""
        assert OrderStatus.UNMATCHED.value == "UNMATCHED"


class TestOrderTypeEnum:
    """OrderType (TIF) values."""

    def test_v1_types_present(self) -> None:
        assert OrderType.GTC.value == "GTC"
        assert OrderType.IOC.value == "IOC"
        assert OrderType.FOK.value == "FOK"

    def test_v2_added_gtd(self) -> None:
        """GTD is the V2-only TIF (V1 used the on-chain expiration field)."""
        assert OrderType.GTD.value == "GTD"


class TestSignatureTypeEnum:
    """V2 still uses the same EIP-1271 magic for Safe; pin the integer values
    since they're encoded into the signed Order struct (signatureType field)."""

    def test_eoa_is_zero(self) -> None:
        assert SignatureType.EOA.value == 0

    def test_poly_proxy_is_one(self) -> None:
        assert SignatureType.POLY_PROXY.value == 1

    def test_poly_gnosis_safe_is_two(self) -> None:
        assert SignatureType.POLY_GNOSIS_SAFE.value == 2


class TestOrderSideEnum:
    """OrderSide is encoded as uint8 in the signed Order struct — pinning
    the integer values prevents accidental BUY/SELL inversion."""

    def test_buy_is_zero(self) -> None:
        assert OrderSide.BUY.value == 0

    def test_sell_is_one(self) -> None:
        assert OrderSide.SELL.value == 1


# =============================================================================
# LimitOrderParams / MarketOrderParams shape
# =============================================================================


class TestV2LimitOrderParams:
    """V2 dropped fee_rate_bps; expiration is wire-only (API-level GTD)."""

    def test_required_fields(self) -> None:
        params = LimitOrderParams(token_id="1", side="BUY", price=Decimal("0.5"), size=Decimal("10"))
        assert params.token_id == "1"
        assert params.side == "BUY"
        assert params.price == Decimal("0.5")
        assert params.size == Decimal("10")
        # Default expiration is 0 (no GTD).
        assert params.expiration == 0

    def test_no_fee_rate_bps_field(self) -> None:
        """fee_rate_bps was a V1 field (operator-set in V2)."""
        params = LimitOrderParams(token_id="1", side="BUY", price=Decimal("0.5"), size=Decimal("10"))
        assert not hasattr(params, "fee_rate_bps")

    def test_explicit_expiration_routes_to_api_gtd(self) -> None:
        """expiration on params is wire-only; the builder routes it to
        UnsignedOrder.api_expiration, NOT into the signed struct."""
        params = LimitOrderParams(
            token_id="1", side="BUY", price=Decimal("0.5"), size=Decimal("10"), expiration=1700000000
        )
        assert params.expiration == 1700000000


class TestV2UnsignedOrder:
    """The 11-field signed struct + 2 wire-only routing fields
    (exchange_address, api_expiration)."""

    @staticmethod
    def _make() -> UnsignedOrder:
        return UnsignedOrder(
            salt=1,
            maker="0x" + "ab" * 20,
            signer="0x" + "ab" * 20,
            token_id=42,
            maker_amount=1_000_000,
            taker_amount=2_000_000,
            side=0,
            signature_type=0,
            timestamp=1700000000_000,
            metadata=BYTES32_ZERO,
            builder=BYTES32_ZERO,
            exchange_address=CTF_EXCHANGE_V2,
            api_expiration=0,
        )

    def test_to_struct_excludes_exchange_address(self) -> None:
        """exchange_address is a routing hint, not part of the signed struct."""
        struct = self._make().to_struct()
        assert "exchange_address" not in struct
        assert "exchangeAddress" not in struct

    def test_to_struct_excludes_api_expiration(self) -> None:
        """api_expiration is wire-only (envelope), not signed."""
        struct = self._make().to_struct()
        assert "api_expiration" not in struct
        assert "apiExpiration" not in struct
        assert "expiration" not in struct

    def test_to_struct_field_count_matches_order_types(self) -> None:
        """The struct's keys must be exactly the names in ORDER_TYPES['Order'].
        Anyone changing one without the other breaks signature recovery."""
        struct = self._make().to_struct()
        order_type_names = {f["name"] for f in ORDER_TYPES["Order"]}
        assert set(struct.keys()) == order_type_names


class TestV2SignedOrderApiPayload:
    """to_api_payload() — wire shape.

    Wraps the signed Order in the V2 envelope:
      { order: {...full order + signature + expiration}, owner, orderType }
    """

    @staticmethod
    def _signed() -> SignedOrder:
        order = TestV2UnsignedOrder._make()
        return SignedOrder(order=order, signature="0x" + "ab" * 65)

    def test_envelope_keys(self) -> None:
        payload = self._signed().to_api_payload(owner="api-key", order_type="GTC")
        assert set(payload.keys()) == {"order", "owner", "orderType"}

    def test_owner_field_propagated(self) -> None:
        payload = self._signed().to_api_payload(owner="my-uuid")
        assert payload["owner"] == "my-uuid"

    def test_order_type_default_is_gtc(self) -> None:
        payload = self._signed().to_api_payload(owner="x")
        assert payload["orderType"] == "GTC"

    def test_order_type_override(self) -> None:
        for ot in ("GTC", "GTD", "IOC", "FOK"):
            payload = self._signed().to_api_payload(owner="x", order_type=ot)
            assert payload["orderType"] == ot

    def test_signature_inside_order(self) -> None:
        """Wire shape: signature lives inside the `order` object, not at the
        top level (V1 had it at the top level)."""
        payload = self._signed().to_api_payload(owner="x")
        assert "signature" in payload["order"]
        assert "signature" not in payload

    def test_side_string_encoded_for_buy(self) -> None:
        order = TestV2UnsignedOrder._make()
        order.side = OrderSide.BUY.value
        payload = SignedOrder(order=order, signature="0x" + "ab" * 65).to_api_payload(owner="x")
        assert payload["order"]["side"] == "BUY"

    def test_side_string_encoded_for_sell(self) -> None:
        order = TestV2UnsignedOrder._make()
        order.side = OrderSide.SELL.value
        payload = SignedOrder(order=order, signature="0x" + "ab" * 65).to_api_payload(owner="x")
        assert payload["order"]["side"] == "SELL"

    def test_string_encoded_numeric_fields(self) -> None:
        """The CLOB API expects every numeric field as a JSON string."""
        payload = self._signed().to_api_payload(owner="x")
        order = payload["order"]
        for f in ("tokenId", "makerAmount", "takerAmount", "timestamp", "expiration"):
            assert isinstance(order[f], str), f"{f} must be a string on the wire"


class TestV2MarketOrderParamsShape:
    """MarketOrderParams basics."""

    def test_required_fields(self) -> None:
        params = MarketOrderParams(token_id="1", side="BUY", amount=Decimal("100"))
        assert params.token_id == "1"
        assert params.side == "BUY"
        assert params.amount == Decimal("100")
        assert params.worst_price is None  # default

    def test_optional_worst_price(self) -> None:
        params = MarketOrderParams(token_id="1", side="SELL", amount=Decimal("100"), worst_price=Decimal("0.5"))
        assert params.worst_price == Decimal("0.5")


# =============================================================================
# OrderBook depth-walk ordering — best_bid / best_ask / spread
#
# Polymarket's CLOB returns the orderbook in depth-walk order: bids ascend
# (worst price first, best last) and asks descend (worst first, best last).
# Pre-fix the model used bids[0] / asks[0] which corresponded to the WORST
# price on each side — silently wrong for every market with ≥ 2 levels.
# Cross-checked against live ``GET /price?side=BUY|SELL`` matching
# ``bids[-1].price`` and ``asks[-1].price``.
# =============================================================================


class TestOrderBookDepthWalkOrdering:
    """Pin the depth-walk semantics so a future "helpful" sort can't
    re-introduce the old off-by-N-levels bug."""

    @staticmethod
    def _book(bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> OrderBook:
        return OrderBook(
            market="t",
            asset_id="t",
            bids=[PriceLevel(price=Decimal(p), size=Decimal(s)) for p, s in bids],
            asks=[PriceLevel(price=Decimal(p), size=Decimal(s)) for p, s in asks],
        )

    def test_best_bid_picks_last_entry(self) -> None:
        """bids ascend; best bid is ``bids[-1]``, NOT ``bids[0]``."""
        book = self._book(
            bids=[("0.50", "1"), ("0.60", "1"), ("0.65", "1")],
            asks=[("0.85", "1"), ("0.75", "1"), ("0.70", "1")],
        )
        assert book.best_bid == Decimal("0.65")  # last bid, not first

    def test_best_ask_picks_last_entry(self) -> None:
        """asks descend; best ask is ``asks[-1]``, NOT ``asks[0]``."""
        book = self._book(
            bids=[("0.50", "1"), ("0.60", "1"), ("0.65", "1")],
            asks=[("0.85", "1"), ("0.75", "1"), ("0.70", "1")],
        )
        assert book.best_ask == Decimal("0.70")  # last ask, not first

    def test_spread_is_best_ask_minus_best_bid(self) -> None:
        """spread uses the corrected end-of-list values (0.70 - 0.65 = 0.05),
        not a worst-of-each-side computation that would yield 0.35."""
        book = self._book(
            bids=[("0.50", "1"), ("0.60", "1"), ("0.65", "1")],
            asks=[("0.85", "1"), ("0.75", "1"), ("0.70", "1")],
        )
        assert book.spread == Decimal("0.05")

    def test_single_level_book_unaffected(self) -> None:
        """One level on each side: ``bids[0] == bids[-1]`` so the historical
        bug was invisible. This case must still work post-fix."""
        book = self._book(bids=[("0.40", "1")], asks=[("0.60", "1")])
        assert book.best_bid == Decimal("0.40")
        assert book.best_ask == Decimal("0.60")
        assert book.spread == Decimal("0.20")

    def test_empty_sides_return_none(self) -> None:
        """Empty bid or ask side → None for the missing best price; spread
        is None when either side is empty (avoids confusing zero spreads)."""
        only_bids = self._book(bids=[("0.40", "1")], asks=[])
        only_asks = self._book(bids=[], asks=[("0.60", "1")])
        empty = self._book(bids=[], asks=[])

        assert only_bids.best_bid == Decimal("0.40")
        assert only_bids.best_ask is None
        assert only_bids.spread is None

        assert only_asks.best_bid is None
        assert only_asks.best_ask == Decimal("0.60")
        assert only_asks.spread is None

        assert empty.best_bid is None
        assert empty.best_ask is None
        assert empty.spread is None

    def test_from_api_response_preserves_order(self) -> None:
        """from_api_response must preserve the API's depth-walk ordering —
        no implicit sort. Otherwise the post-fix property reads return
        whatever the sort orientation happened to be, not the true best."""
        response = {
            "market": "t",
            "asset_id": "t",
            "bids": [
                {"price": "0.10", "size": "1"},  # worst bid first
                {"price": "0.20", "size": "1"},
                {"price": "0.30", "size": "1"},  # best bid last
            ],
            "asks": [
                {"price": "0.90", "size": "1"},  # worst ask first
                {"price": "0.80", "size": "1"},
                {"price": "0.40", "size": "1"},  # best ask last
            ],
        }
        book = OrderBook.from_api_response(response)
        # Verify ordering is preserved at the index level.
        assert [lvl.price for lvl in book.bids] == [Decimal("0.10"), Decimal("0.20"), Decimal("0.30")]
        assert [lvl.price for lvl in book.asks] == [Decimal("0.90"), Decimal("0.80"), Decimal("0.40")]
        # And the property derivations are correct.
        assert book.best_bid == Decimal("0.30")
        assert book.best_ask == Decimal("0.40")
        assert book.spread == Decimal("0.10")
