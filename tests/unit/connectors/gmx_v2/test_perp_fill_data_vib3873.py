"""VIB-3873 / VIB-3872 WI-1 — keyed EventUtils decode for GMX fill economics.

These tests encode the keeper events with the REAL EventUtils ABI encoder (the
same dynamic keyed struct production emits) rather than hand-crafted flat words.
The flat-word shape is exactly the fixture blindspot that masked the VIB-3873
misread class (ALM-2993): a positional decode reads dynamic-struct ABI offsets
as field values. Building the keyed struct and decoding it BY NAME is what these
tests lock in.
"""

from decimal import Decimal

import pytest
from eth_abi import encode as abi_encode

from almanak.connectors.gmx_v2.receipt_parser import (
    _EVENT_LOG_DATA_ABI_TYPE,
    EVENT_TOPICS,
    GMX_MAX_ORDER_TYPE,
    GMXOrderTypeError,
    GMXv2ReceiptParser,
    PerpFillData,
)
from tests.unit.connectors.gmx_v2.market_fixtures import market_address, prime_catalog


# VIB-6110 price scaling now reads the process's venue-verified catalog
# (address-first) — prime it with the audited fixture snapshot, standing in
# for the dynamic verification a live compile performs before any fill.
@pytest.fixture(autouse=True)
def _verified_markets():
    prime_catalog()


_EVENT_LOG1_TOPIC = "0x" + "11" * 32
_TX_HASH = "0x" + "22" * 32
# A LISTED market (arbitrum ETH/USD, index-token decimals 18) so VIB-6110 price scaling
# resolves; lowercase to match the parser's decoded address form. With execution_price_raw
# = 3000 * 10**12 this scales to exactly $3000 USD-per-token (3000e12 * 10**18 / 10**30).
_MARKET = market_address("arbitrum", "ETH/USD").lower()
_COLLATERAL = "0x" + "44" * 20  # treated as USDC-scaled below
_ACCOUNT = "0x" + "55" * 20
_ORDER_KEY = "0x" + "ab" * 32
_POSITION_KEY = "0x" + "cd" * 32

# A SECOND listed market with DIFFERENT index-token decimals (arbitrum BTC/USD, 8) so a
# mis-correlated fill is detectable by market, direction AND price at once. With
# execution_price_raw = 65000 * 10**22 this scales to exactly $65,000 (65000e22 * 10**8 / 10**30).
_MARKET_BTC = market_address("arbitrum", "BTC/USD").lower()
_ORDER_KEY_B = "0x" + "be" * 32
_POSITION_KEY_B = "0x" + "ef" * 32

# The canonical Arbitrum GMX EventEmitter. Correlation is only trustworthy for logs
# emitted by this contract (VIB-6110) — see TestForgedEmitterIsNotCorrelated.
_EVENT_EMITTER = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"
_ATTACKER = "0x" + "66" * 20

# USDC-style collateral: 6 decimals => collateralTokenPrice raw = 1e(30-6) = 1e24.
_USDC_PRICE_RAW = 10**24


def _group(scalars: list[tuple], arrays: list[tuple] | None = None) -> tuple:
    return (scalars, arrays or [])


def _encode_event(event_name: str, *, addresses, uints, ints, bools, bytes32) -> str:
    payload = abi_encode(
        ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
        [
            "0x" + "77" * 20,
            event_name,
            (
                _group(addresses),
                _group(uints),
                _group(ints),
                _group(bools),
                _group(bytes32),
                _group([]),  # bytes items (unused)
                _group([]),  # string items (unused)
            ),
        ],
    )
    return "0x" + payload.hex()


def _position_increase_data(
    *,
    order_type: int = 2,
    execution_price_raw: int = 3000 * 10**12,  # ETH ~ $3000, native GMX price
    size_delta_usd_raw: int = 6000 * 10**30,
    size_delta_in_tokens_raw: int = 2 * 10**18,
    collateral_delta_raw: int = 1500 * 10**6,
    price_impact_raw: int = -2 * 10**28,  # -0.02 USD, SIGNED
    is_long: bool = True,
    scramble: bool = False,
    market: str = _MARKET,
    order_key: str = _ORDER_KEY,
    position_key: str = _POSITION_KEY,
) -> str:
    uints = [
        ("sizeInUsd", size_delta_usd_raw),
        ("collateralAmount", collateral_delta_raw),
        ("executionPrice", execution_price_raw),
        ("sizeDeltaUsd", size_delta_usd_raw),
        ("sizeDeltaInTokens", size_delta_in_tokens_raw),
        ("orderType", order_type),
    ]
    if scramble:
        # Keyed decode must be order-independent — reverse the item order and the
        # values must still map to the right keys. A positional decode would break.
        uints = list(reversed(uints))
    return _encode_event(
        "PositionIncrease",
        addresses=[("account", _ACCOUNT), ("market", market), ("collateralToken", _COLLATERAL)],
        uints=uints,
        ints=[("collateralDeltaAmount", collateral_delta_raw), ("pendingPriceImpactUsd", price_impact_raw)],
        bools=[("isLong", is_long)],
        bytes32=[("orderKey", bytes.fromhex(order_key[2:])), ("positionKey", bytes.fromhex(position_key[2:]))],
    )


def _position_decrease_data(
    *,
    order_type: int = 4,
    execution_price_raw: int = 3100 * 10**12,
    size_delta_usd_raw: int = 6000 * 10**30,
    collateral_delta_raw: int = 1500 * 10**6,
    base_pnl_raw: int = -25 * 10**30,  # SIGNED loss
    price_impact_raw: int = -3 * 10**28,
    is_long: bool = True,
    market: str = _MARKET,
    order_key: str = _ORDER_KEY,
    position_key: str = _POSITION_KEY,
) -> str:
    return _encode_event(
        "PositionDecrease",
        addresses=[("account", _ACCOUNT), ("market", market), ("collateralToken", _COLLATERAL)],
        uints=[
            ("executionPrice", execution_price_raw),
            ("sizeDeltaUsd", size_delta_usd_raw),
            ("sizeDeltaInTokens", 2 * 10**18),
            ("collateralAmount", 0),
            ("collateralDeltaAmount", collateral_delta_raw),
            ("orderType", order_type),
        ],
        ints=[("priceImpactUsd", price_impact_raw), ("basePnlUsd", base_pnl_raw)],
        bools=[("isLong", is_long)],
        bytes32=[("orderKey", bytes.fromhex(order_key[2:])), ("positionKey", bytes.fromhex(position_key[2:]))],
    )


def _position_fees_data(
    *,
    funding_fee_amount: int = 500_000,  # 0.5 USDC
    position_fee_amount: int = 1_200_000,  # 1.2 USDC
    borrowing_fee_amount: int = 300_000,  # 0.3 USDC
    price_raw: int = _USDC_PRICE_RAW,
    market: str = _MARKET,
    order_key: str = _ORDER_KEY,
    position_key: str = _POSITION_KEY,
) -> str:
    return _encode_event(
        "PositionFeesCollected",
        addresses=[("market", market), ("collateralToken", _COLLATERAL)],
        uints=[
            ("collateralTokenPrice.min", price_raw),
            ("collateralTokenPrice.max", price_raw),
            ("fundingFeeAmount", funding_fee_amount),
            ("positionFeeAmount", position_fee_amount),
            ("borrowingFeeAmount", borrowing_fee_amount),
        ],
        ints=[],
        bools=[("isIncrease", False)],
        bytes32=[("orderKey", bytes.fromhex(order_key[2:])), ("positionKey", bytes.fromhex(position_key[2:]))],
    )


def _log(event_name: str, data: str, *, log_index: int = 1, address: str = _EVENT_EMITTER) -> dict:
    return {
        "address": address,
        "topics": [_EVENT_LOG1_TOPIC, EVENT_TOPICS[event_name], "0x" + _POSITION_KEY[2:]],
        "data": data,
        "logIndex": log_index,
    }


def _receipt(logs: list[dict]) -> dict:
    return {
        "transactionHash": _TX_HASH,
        "blockNumber": 987654,
        "status": 1,
        "logs": logs,
        "gasUsed": 300_000,
    }


class TestPerpFillOpen:
    def test_open_fill_measures_entry_and_identity(self) -> None:
        receipt = _receipt([_log("PositionIncrease", _position_increase_data())])
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipt)

        assert isinstance(fill, PerpFillData)
        assert fill.is_open is True
        assert fill.is_long is True
        assert fill.market == _MARKET
        assert fill.collateral_token == _COLLATERAL
        assert fill.position_key == _POSITION_KEY
        assert fill.order_key == _ORDER_KEY
        # VIB-6110: entry_price is USD-per-token (executionPrice * 10**18 / 1e30), not the
        # 3e-15 raw ratio the field shipped before the fix.
        assert fill.entry_price == Decimal("3000")
        assert fill.exit_price is None  # opens have no exit
        assert fill.size_delta_usd == Decimal(6000)
        assert fill.collateral_delta_amount == Decimal(1500 * 10**6)
        assert fill.price_impact_usd == Decimal(-2 * 10**28) / Decimal(10**30)  # signed
        assert fill.realized_pnl_usd is None  # opens have no realized pnl
        assert fill.keeper_tx_hash == _TX_HASH
        assert fill.block_number == 987654

    def test_keyed_decode_is_order_independent(self) -> None:
        """Proves the decode is BY KEY, not positional: shuffling item order must
        not change any decoded value (a positional decode would corrupt them)."""
        ordered = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(
            _receipt([_log("PositionIncrease", _position_increase_data())])
        )
        shuffled = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(
            _receipt([_log("PositionIncrease", _position_increase_data(scramble=True))])
        )
        assert ordered == shuffled
        assert shuffled.entry_price == Decimal("3000")  # scaled USD-per-token (VIB-6110)
        assert shuffled.size_delta_usd == Decimal(6000)


class TestPriceScalingVib6110:
    """entry/exit price scaling fails closed (Empty≠Zero) — NEVER the raw GMX ratio."""

    def test_no_chain_leaves_price_unmeasured(self) -> None:
        # Without a chain the parser cannot resolve index-token decimals → entry/exit
        # price is UNMEASURED (None), never the 3e-15 raw ratio. Every other field stays
        # measured (the receipt decode is chain-agnostic).
        receipt = _receipt([_log("PositionIncrease", _position_increase_data())])
        fill = GMXv2ReceiptParser().extract_perp_fill(receipt)
        assert fill.entry_price is None
        assert fill.size_delta_usd == Decimal(6000)  # non-price fields unaffected

    def test_unknown_market_leaves_price_unmeasured(self) -> None:
        # The arbitrum ETH market address is not listed under avalanche → decimals
        # unresolved → price None (fail-closed), other fields still measured.
        receipt = _receipt([_log("PositionIncrease", _position_increase_data())])
        fill = GMXv2ReceiptParser(chain="avalanche").extract_perp_fill(receipt)
        assert fill.entry_price is None
        assert fill.size_delta_usd == Decimal(6000)


class TestPerpFillClose:
    def test_close_fill_measures_exit_pnl_and_fees(self) -> None:
        receipt = _receipt(
            [
                _log("PositionDecrease", _position_decrease_data(), log_index=1),
                _log("PositionFeesCollected", _position_fees_data(), log_index=2),
            ]
        )
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipt)

        assert fill.is_open is False
        # VIB-6110: exit_price is USD-per-token (3100 * 10**18 / 1e30), not the raw ratio.
        assert fill.exit_price == Decimal("3100")
        assert fill.entry_price is None
        assert fill.realized_pnl_usd == Decimal(-25)  # signed loss, measured
        assert fill.price_impact_usd == Decimal(-3 * 10**28) / Decimal(10**30)
        assert fill.collateral_delta_amount == Decimal(1500 * 10**6)
        # Fees: amount * collateralTokenPrice / 1e30 — decimals-free USD.
        assert fill.funding_fee_usd == Decimal("0.5")
        assert fill.position_fee_usd == Decimal("1.2")
        assert fill.borrowing_fee_usd == Decimal("0.3")

    def test_close_without_fees_event_leaves_fees_none(self) -> None:
        """Empty != Zero: no PositionFeesCollected => fee fields unmeasured (None)."""
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        fill = GMXv2ReceiptParser().extract_perp_fill(receipt)

        assert fill.is_open is False
        assert fill.realized_pnl_usd == Decimal(-25)
        assert fill.funding_fee_usd is None
        assert fill.position_fee_usd is None
        assert fill.borrowing_fee_usd is None


class TestBatchedKeeperTxOrderCorrelation:
    """VIB-6110: a keeper tx executing SEVERAL orders must settle the WATCHED one.

    Every other fixture in this file carries exactly one fill, which makes
    "take the first position event" and "take the event whose orderKey matches"
    observationally identical — that is precisely why the uncorrelated read
    survived green CI. These fixtures put two orders in ONE keeper transaction
    and make them differ in every field that matters (market, index-token
    decimals, direction, open/close, execution price, fees), so an
    uncorrelated read cannot coincidentally produce the right answer.
    """

    @staticmethod
    def _batched_receipt() -> dict:
        """One keeper tx: order A opens LONG ETH @ $3,000; order B closes SHORT BTC @ $65,000.

        A is emitted FIRST so a first-wins reader picks A. A is an increase and
        B is a decrease, so a reader that also prefers increases picks A too.
        """
        return _receipt(
            [
                # --- order A: unrelated LONG ETH open, emitted first ---
                _log(
                    "PositionIncrease",
                    _position_increase_data(
                        market=_MARKET,
                        order_key=_ORDER_KEY,
                        position_key=_POSITION_KEY,
                        execution_price_raw=3000 * 10**12,  # ETH, 18 decimals -> $3,000
                        is_long=True,
                    ),
                    log_index=1,
                ),
                _log(
                    "PositionFeesCollected",
                    _position_fees_data(
                        market=_MARKET,
                        order_key=_ORDER_KEY,
                        position_key=_POSITION_KEY,
                    ),
                    log_index=2,
                ),
                # --- order B: the WATCHED SHORT BTC close, emitted second ---
                _log(
                    "PositionDecrease",
                    _position_decrease_data(
                        market=_MARKET_BTC,
                        order_key=_ORDER_KEY_B,
                        position_key=_POSITION_KEY_B,
                        execution_price_raw=65000 * 10**22,  # BTC, 8 decimals -> $65,000
                        is_long=False,
                        base_pnl_raw=40 * 10**30,  # SIGNED gain, distinct from A
                    ),
                    log_index=3,
                ),
                _log(
                    "PositionFeesCollected",
                    _position_fees_data(
                        market=_MARKET_BTC,
                        order_key=_ORDER_KEY_B,
                        position_key=_POSITION_KEY_B,
                        funding_fee_amount=700_000,  # 0.7 USDC, distinct from A's 0.5
                        position_fee_amount=2_400_000,  # 2.4 USDC, distinct from A's 1.2
                        borrowing_fee_amount=900_000,  # 0.9 USDC, distinct from A's 0.3
                    ),
                    log_index=4,
                ),
            ]
        )

    def test_watched_close_is_not_replaced_by_a_sibling_open(self) -> None:
        """The money bug: settling B must NOT return A's open.

        Pre-fix this returned is_open=True, market=ETH, entry_price=$3,000 and
        exit_price=None — B's $65,000 exit price silently lost, the close
        booked as an open, the short booked as a long. Every one of those
        values is individually plausible, so nothing downstream could catch it.
        """
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(self._batched_receipt(), order_key=_ORDER_KEY_B)

        assert fill is not None
        # Identity: B's order, B's position, B's market — never A's.
        assert fill.order_key == _ORDER_KEY_B
        assert fill.position_key == _POSITION_KEY_B
        assert fill.market == _MARKET_BTC
        # Direction and lifecycle: B is a SHORT CLOSE, A is a LONG OPEN.
        assert fill.is_open is False
        assert fill.is_long is False
        # Price: B's $65,000 exit, scaled by BTC's 8 index-token decimals.
        # A's $3,000 would be the pre-fix answer.
        assert fill.exit_price == Decimal("65000")
        assert fill.entry_price is None
        assert fill.realized_pnl_usd == Decimal(40)

    def test_watched_open_is_not_replaced_by_a_sibling_close(self) -> None:
        """Symmetry: settling A must return A's open, not B's close."""
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(self._batched_receipt(), order_key=_ORDER_KEY)

        assert fill is not None
        assert fill.order_key == _ORDER_KEY
        assert fill.market == _MARKET
        assert fill.is_open is True
        assert fill.is_long is True
        assert fill.entry_price == Decimal("3000")
        assert fill.exit_price is None

    def test_fees_are_correlated_to_the_watched_order(self) -> None:
        """Fees must come from the WATCHED order's fee event, not the first one.

        A's fee event is emitted first; pre-fix, B's settlement was charged
        A's 0.5 / 1.2 / 0.3 USDC.
        """
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(self._batched_receipt(), order_key=_ORDER_KEY_B)

        assert fill.funding_fee_usd == Decimal("0.7")
        assert fill.position_fee_usd == Decimal("2.4")
        assert fill.borrowing_fee_usd == Decimal("0.9")

    def test_unmatched_order_key_is_unmeasured_not_a_sibling_fill(self) -> None:
        """Fail-closed: an order absent from the receipt yields None, never a neighbour's fill."""
        absent = "0x" + "07" * 32
        assert GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(self._batched_receipt(), order_key=absent) is None

    def test_order_key_match_is_case_insensitive(self) -> None:
        """Watched keys arrive lowercased; decoded keys must still match if cased."""
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(
            self._batched_receipt(), order_key=_ORDER_KEY_B.upper().replace("0X", "0x")
        )
        assert fill is not None
        assert fill.order_key == _ORDER_KEY_B

    def test_result_wrapper_threads_the_order_key(self) -> None:
        """The fail-closed ExtractResult variant must correlate too, not just the raw call."""
        parser = GMXv2ReceiptParser(chain="arbitrum")
        ok = parser.extract_perp_fill_result(self._batched_receipt(), order_key=_ORDER_KEY_B)
        assert ok.value.market == _MARKET_BTC
        assert ok.value.exit_price == Decimal("65000")

        missing = parser.extract_perp_fill_result(self._batched_receipt(), order_key="0x" + "07" * 32)
        assert getattr(missing, "value", None) is None

    def test_uncorrelated_call_keeps_legacy_first_wins(self) -> None:
        """order_key=None is unchanged behaviour for callers with no watched order."""
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(self._batched_receipt())
        assert fill is not None
        assert fill.order_key == _ORDER_KEY  # first position event wins, as before


class TestForgedEmitterIsNotCorrelated:
    """VIB-6110: correlation only trusts logs from the canonical GMX EventEmitter.

    ``_parse_log`` recognises GMX events by topic hash and never checks
    ``log["address"]``. A GMX order carries an owner-chosen ``callbackContract``
    that executes inside the same keeper transaction, so a co-batched adversary
    can compute our order key from the ``DataStore`` nonce and emit a forged
    ``PositionDecrease`` carrying it. Matching on an ``orderKey`` decoded from an
    unauthenticated log would just move the attack from "be first in the log
    list" to "write the right 32 bytes".
    """

    def test_forged_log_carrying_our_order_key_is_ignored(self) -> None:
        """An attacker contract emitting our order key must not become our fill."""
        receipt = _receipt(
            [
                # Forged: attacker address, OUR order key, wrong market, wrong price.
                _log(
                    "PositionDecrease",
                    _position_decrease_data(
                        market=_MARKET,
                        order_key=_ORDER_KEY_B,
                        position_key=_POSITION_KEY_B,
                        execution_price_raw=1 * 10**12,  # $1 — an absurd exit
                        is_long=True,
                    ),
                    log_index=1,
                    address=_ATTACKER,
                ),
                # Genuine: canonical emitter, same order key, real market and price.
                _log(
                    "PositionDecrease",
                    _position_decrease_data(
                        market=_MARKET_BTC,
                        order_key=_ORDER_KEY_B,
                        position_key=_POSITION_KEY_B,
                        execution_price_raw=65000 * 10**22,
                        is_long=False,
                    ),
                    log_index=2,
                ),
            ]
        )
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipt, order_key=_ORDER_KEY_B)

        assert fill is not None
        assert fill.market == _MARKET_BTC  # genuine, not the forged ETH market
        assert fill.exit_price == Decimal("65000")  # never the forged $1
        assert fill.is_long is False

    def test_forged_only_receipt_is_unmeasured(self) -> None:
        """With no genuine emitter log, a forged fill must yield None, not a fill."""
        receipt = _receipt(
            [
                _log(
                    "PositionDecrease",
                    _position_decrease_data(order_key=_ORDER_KEY_B, position_key=_POSITION_KEY_B),
                    address=_ATTACKER,
                )
            ]
        )
        assert GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipt, order_key=_ORDER_KEY_B) is None

    def test_warm_parse_cache_cannot_launder_a_forged_log(self) -> None:
        """The emitter check must survive ``ResultEnricher``'s parse cache.

        The cache is keyed on ``transactionHash``. An earlier design filtered
        the receipt's logs and handed the copy to ``parse_receipt`` — but the
        copy kept the same hash, so a cache warmed by an UNFILTERED call
        returned the unfiltered parse and the forged fill sailed through the
        filter. ``_merge_receipt_logs`` stamps a synthetic hash to dodge exactly
        this trap. Authenticating each parsed event's ``contract_address``
        instead is cache-immune; this test is the regression pin.
        """
        from almanak.framework.execution.result_enricher import ResultEnricher

        forged = _receipt(
            [
                _log(
                    "PositionDecrease",
                    _position_decrease_data(
                        market=_MARKET,
                        order_key=_ORDER_KEY_B,
                        position_key=_POSITION_KEY_B,
                        execution_price_raw=1 * 10**12,  # $1 — the forged exit
                    ),
                    address=_ATTACKER,
                )
            ]
        )
        parser = GMXv2ReceiptParser(chain="arbitrum")

        # Warm the cache with an UNCORRELATED call on the very same receipt
        # (same transactionHash), exactly as an enrichment pass would.
        ResultEnricher._install_parse_cache(parser)
        assert getattr(parser.parse_receipt, "_is_cached_wrapper", False), "cache not installed"
        parser.extract_perp_fill(forged)

        # The correlated call must still refuse the forged log.
        assert parser.extract_perp_fill(forged, order_key=_ORDER_KEY_B) is None

    def test_correlated_call_without_resolvable_emitter_fails_closed(self) -> None:
        """No chain => no known emitter => a correlated call cannot authenticate => None.

        Unmeasured is recoverable; a forged settlement is not.
        """
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        assert GMXv2ReceiptParser().extract_perp_fill(receipt, order_key=_ORDER_KEY) is None
        assert GMXv2ReceiptParser(chain="ethereum").extract_perp_fill(receipt, order_key=_ORDER_KEY) is None
        # ...while the same receipt still decodes on the uncorrelated legacy path.
        assert GMXv2ReceiptParser().extract_perp_fill(receipt) is not None


class TestExtractFundingFeeUsd:
    def test_measured_funding_fee(self) -> None:
        receipt = _receipt([_log("PositionFeesCollected", _position_fees_data())])
        assert GMXv2ReceiptParser().extract_funding_fee_usd(receipt) == Decimal("0.5")

    def test_measured_zero_funding_is_zero_not_none(self) -> None:
        receipt = _receipt([_log("PositionFeesCollected", _position_fees_data(funding_fee_amount=0))])
        result = GMXv2ReceiptParser().extract_funding_fee_usd(receipt)
        assert result == Decimal("0")
        assert result is not None

    def test_no_fees_event_is_none(self) -> None:
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        assert GMXv2ReceiptParser().extract_funding_fee_usd(receipt) is None


class TestOrderTypeBoundCheck:
    def test_bound_constant_is_seven(self) -> None:
        assert GMX_MAX_ORDER_TYPE == 7

    def test_increase_order_type_above_bound_raises(self) -> None:
        parser = GMXv2ReceiptParser()
        # A flat-word misread yields garbage like 32 (an ABI offset). Direct
        # decoder call proves the tripwire in isolation (mutation-resistant).
        with pytest.raises(GMXOrderTypeError):
            parser._decode_event_utils_position_increase(_position_increase_data(order_type=32).removeprefix("0x"))

    def test_decrease_order_type_above_bound_raises(self) -> None:
        parser = GMXv2ReceiptParser()
        with pytest.raises(GMXOrderTypeError):
            parser._decode_event_utils_position_decrease(_position_decrease_data(order_type=160).removeprefix("0x"))

    def test_parse_receipt_fails_closed_on_bad_order_type(self) -> None:
        receipt = _receipt([_log("PositionIncrease", _position_increase_data(order_type=32))])
        result = GMXv2ReceiptParser().parse_receipt(receipt)
        assert result.success is False
        assert "order_type" in (result.error or "")

    @pytest.mark.parametrize("order_type", [0, 2, 4, 7])
    def test_in_range_order_type_ok(self, order_type: int) -> None:
        decoded = GMXv2ReceiptParser()._decode_event_utils_position_increase(
            _position_increase_data(order_type=order_type).removeprefix("0x")
        )
        assert decoded is not None
        assert decoded["order_type"] == order_type


class TestEmptyNotZero:
    def test_no_position_event_returns_none(self) -> None:
        receipt = _receipt([_log("PositionFeesCollected", _position_fees_data())])
        assert GMXv2ReceiptParser().extract_perp_fill(receipt) is None

    def test_empty_receipt_returns_none(self) -> None:
        assert GMXv2ReceiptParser().extract_perp_fill(_receipt([])) is None

    def test_keyed_increase_does_not_fabricate_typed_position_increase(self) -> None:
        """A production keyed PositionIncrease must NOT populate positionally
        decoded typed data (the VIB-3873 garbage). It flows via PerpFillData."""
        receipt = _receipt([_log("PositionIncrease", _position_increase_data())])
        parsed = GMXv2ReceiptParser().parse_receipt(receipt)
        assert parsed.success is True
        assert parsed.position_increases == []  # no garbage typed row


class TestPerpFillToDict:
    def test_to_dict_preserves_empty_not_zero(self) -> None:
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        payload = GMXv2ReceiptParser().extract_perp_fill(receipt).to_dict()
        assert payload["funding_fee_usd"] is None  # unmeasured stays None
        assert payload["realized_pnl_usd"] == "-25"
        assert payload["is_open"] is False
