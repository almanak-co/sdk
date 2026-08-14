"""GMX V2 ``acceptablePrice`` must be a real bound, proven on DECODED CALLDATA (VIB-6219).

Before this suite the connector shipped a degenerate sentinel on every perp leg::

    acceptable_price = Decimal(10**30) if intent.is_long else Decimal("0")   # open
    acceptable_price = Decimal("0") if intent.is_long else Decimal(10**30)   # close

"accept literally any execution price". The user's ``max_slippage`` was converted
to basis points, handed to the adapter as ``default_slippage_bps``, and never
read. Every GMX position this SDK opened or closed was unbounded against price
movement between order submission and keeper execution.

Why these tests decode calldata
-------------------------------
There was previously NO test in this repo that decoded an order's calldata to
assert a non-degenerate bound. Asserting on a Python variable three layers above
the ABI encoder is exactly what let the sentinel ship: the compiler's local
``acceptable_price`` was "correct" in the sense that it matched what the code
intended to send. So every assertion below reads the number back out of the
encoded ``createOrder`` call inside the real ``multicall`` payload, built by the
real :class:`GMXV2SDK`.

Direction semantics (``PositionUtils.validateAcceptablePrice`` in gmx-synthetics)
---------------------------------------------------------------------------------
=================  =====================  =================
leg                economic direction     acceptablePrice
=================  =====================  =================
open long          buying the index       maximum
open short         selling the index      minimum
close long         selling the index      minimum
close short        buying the index       maximum
=================  =====================  =================

All four are covered here. Getting a sign backwards is worse than the original
bug: the order would *look* protected while being unbounded in one direction.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.acceptable_price import (
    bound_is_maximum,
    derive_acceptable_price_30dec,
    price_30dec_to_usd,
)
from almanak.connectors.gmx_v2.sdk import GMXV2SDK, OrderType
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.min_out_guard import UnprotectedTradeError
from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent
from tests.unit.connectors.gmx_v2.market_fixtures import (
    FIXTURE_MARKETS,
    fake_dynamic_gateway,
    market_address,
    market_record,
    prime_catalog,
)

# --------------------------------------------------------------------------
# Scenario constants. ETH/USD on Arbitrum has an 18-decimal index token, so
# GMX's raw price scale is 10 ** (30 - 18) == 10**12.
# --------------------------------------------------------------------------
ETH_PRICE_USD = Decimal("3000")
#: $3000/ETH in GMX's native convention: 3000 * 10**12.
ETH_PRICE_30DEC = 3_000 * 10**12
#: 1% — the ``max_slippage`` default on both perp intents.
SLIPPAGE = Decimal("0.01")
#: 3000 * 1.01 == 3030, scaled.
ETH_UPPER_BOUND_30DEC = 3_030 * 10**12
#: 3000 * 0.99 == 2970, scaled.
ETH_LOWER_BOUND_30DEC = 2_970 * 10**12

#: The two "accept anything" values this ticket deletes.
SENTINEL_MAX = 10**30
SENTINEL_MIN = 0

USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
EXECUTION_FEE = 100_000_000_000_000

#: Address-first market inputs (the audited fixture snapshot, not a live table).
ETH_MARKET_ARB = market_address("arbitrum", "ETH/USD")
SOL_MARKET_ARB = market_address("arbitrum", "SOL/USD")
BTC_MARKET_ARB = market_address("arbitrum", "BTC/USD")


@pytest.fixture(autouse=True)
def _verified_markets() -> None:
    """Prime the process catalog with the audited fixture markets.

    Address-first: the compiler reads index symbol/decimals ONLY from the
    catalog of venue-verified markets. CLOSE-path compiles here run with no
    gateway — priming from the fixture snapshot reproduces the state a live
    process reaches after dynamic resolution verified the market (the
    catalog fallback is close-only by design). Without it every close compile
    here would fail closed at the price bound, which is its own (separately
    pinned) behaviour, not what these tests are about. OPEN-path compiles
    additionally need the fake dynamic gateway — see ``_make_open_compiler``.
    """
    prime_catalog()


def _make_compiler(chain: str = "arbitrum", prices: dict[str, Decimal] | None = None) -> IntentCompiler:
    """A compile-path-complete IntentCompiler with no network and a real price oracle."""
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = chain
    compiler.wallet_address = "0x" + "1" * 40
    # Never dialled: GMXV2SDK only needs a provider object to ABI-encode, and
    # get_execution_fee (the one networked call) is patched out below.
    compiler.rpc_url = "http://127.0.0.1:1"
    compiler._approve_cache = {}
    compiler._allowance_cache = {}
    compiler._gateway_client = None
    compiler._config = IntentCompilerConfig(allow_placeholder_prices=False)
    compiler._using_placeholders = False
    compiler._placeholder_warning_logged = False
    compiler._stablecoin_fallback_logged = set()
    compiler.price_oracle = {"ETH": ETH_PRICE_USD, "USDC": Decimal("1")} if prices is None else prices
    compiler.default_deadline_seconds = 600
    compiler.default_protocol = "gmx_v2"
    compiler._token_resolver = None
    compiler._build_approve_tx = lambda token_address, spender, amount: []
    compiler._get_chain_rpc_url = lambda: "http://127.0.0.1:1"
    return compiler


def _make_open_compiler(chain: str = "arbitrum", prices: dict[str, Decimal] | None = None) -> IntentCompiler:
    """``_make_compiler`` plus the fake dynamic gateway an OPEN compile requires.

    A risk-increasing PERP_OPEN resolution demands CURRENT venue listing
    (``require_listed=True``); the primed process catalog serves only the
    risk-reducing close, so an open with no usable dynamic gateway fails
    closed (transiently) before ever reaching the price bound under test.
    Production opens always run with a gateway — this fake models that while
    keeping the compile path offline.
    """
    compiler = _make_compiler(chain, prices)
    compiler._gateway_client = fake_dynamic_gateway(chain)
    return compiler


def _open_intent(*, is_long: bool, market: str = ETH_MARKET_ARB, **kwargs: Any) -> PerpOpenIntent:
    params: dict[str, Any] = {
        "market": market,
        "collateral_token": "USDC",
        "collateral_amount": Decimal("100"),
        "size_usd": Decimal("1000"),
        "is_long": is_long,
        "leverage": Decimal("10"),
        "protocol": "gmx_v2",
    }
    params.update(kwargs)
    return PerpOpenIntent(**params)


def _close_intent(*, is_long: bool, market: str = ETH_MARKET_ARB, **kwargs: Any) -> PerpCloseIntent:
    params: dict[str, Any] = {
        "market": market,
        "collateral_token": "USDC",
        "is_long": is_long,
        # Explicit so the close path does not try to read position size on-chain.
        "size_usd": Decimal("1000"),
        "protocol": "gmx_v2",
    }
    params.update(kwargs)
    return PerpCloseIntent(**params)


def _compile(compiler: IntentCompiler, intent: Any) -> Any:
    """Run the REAL compile path: real GMXV2SDK, real ABI encoding.

    Only ``get_execution_fee`` is patched — it is the single call that would
    reach the network, and the keeper fee is irrelevant to the price bound.
    """
    with patch.object(GMXV2SDK, "get_execution_fee", return_value=EXECUTION_FEE):
        return compiler.compile(intent)


def _decode_create_order_params(result: Any) -> Any:
    """Pull the order params back out of the ENCODED createOrder calldata.

    Walks the real payload the wallet would sign: ``multicall(bytes[])`` →
    the ``createOrder`` element → ``params.numbers.acceptablePrice``. Decoding
    (rather than reading a compiler local) is the whole point of this suite.
    """
    perp_txs = [tx for tx in result.transactions if tx.tx_type in ("perp_open", "perp_close")]
    assert len(perp_txs) == 1, f"expected exactly one order tx, got {[t.tx_type for t in perp_txs]}"

    sdk = GMXV2SDK(rpc_url="http://127.0.0.1:1", chain="arbitrum")
    outer_fn, outer_args = sdk.exchange_router.decode_function_input(perp_txs[0].data)
    assert outer_fn.fn_name == "multicall"

    create_order_calls = []
    for call in outer_args["data"]:
        inner_fn, inner_args = sdk.exchange_router.decode_function_input(call)
        if inner_fn.fn_name == "createOrder":
            create_order_calls.append(inner_args["params"])
    assert len(create_order_calls) == 1, "expected exactly one createOrder in the multicall"
    return create_order_calls[0]


def _decode_acceptable_price(result: Any) -> int:
    return int(_decode_create_order_params(result)["numbers"]["acceptablePrice"])


# =============================================================================
# The headline requirement: all four direction/side combos, on decoded calldata
# =============================================================================


class TestEncodedAcceptablePriceAllFourCombos:
    """Every perp leg encodes a real, correctly-signed bound into createOrder."""

    def test_open_long_encodes_an_upper_bound(self) -> None:
        """Opening a long BUYS the index → acceptablePrice caps the price paid."""
        result = _compile(_make_open_compiler(), _open_intent(is_long=True))
        assert result.status == CompilationStatus.SUCCESS, result.error

        encoded = _decode_acceptable_price(result)
        assert encoded == ETH_UPPER_BOUND_30DEC
        # The bound must be ABOVE spot (we tolerate paying up to +1%)...
        assert encoded > ETH_PRICE_30DEC
        # ...and must NOT be the "accept anything" sentinel this ticket deletes.
        assert encoded != SENTINEL_MAX
        assert price_30dec_to_usd(encoded, 18) == Decimal("3030")

    def test_open_short_encodes_a_lower_bound(self) -> None:
        """Opening a short SELLS the index → acceptablePrice floors the price received."""
        result = _compile(_make_open_compiler(), _open_intent(is_long=False))
        assert result.status == CompilationStatus.SUCCESS, result.error

        encoded = _decode_acceptable_price(result)
        assert encoded == ETH_LOWER_BOUND_30DEC
        assert encoded < ETH_PRICE_30DEC
        # A zero floor is the sentinel: it accepts any price down to nothing.
        assert encoded != SENTINEL_MIN
        assert encoded > 0
        assert price_30dec_to_usd(encoded, 18) == Decimal("2970")

    def test_close_long_encodes_a_lower_bound(self) -> None:
        """Closing a long SELLS the index → acceptablePrice floors the price received.

        This is the inverse of the open-long case, and the leg teardown uses.
        """
        result = _compile(_make_compiler(), _close_intent(is_long=True))
        assert result.status == CompilationStatus.SUCCESS, result.error

        encoded = _decode_acceptable_price(result)
        assert encoded == ETH_LOWER_BOUND_30DEC
        assert encoded < ETH_PRICE_30DEC
        assert encoded != SENTINEL_MIN
        assert encoded > 0

    def test_close_short_encodes_an_upper_bound(self) -> None:
        """Closing a short BUYS the index back → acceptablePrice caps the price paid."""
        result = _compile(_make_compiler(), _close_intent(is_long=False))
        assert result.status == CompilationStatus.SUCCESS, result.error

        encoded = _decode_acceptable_price(result)
        assert encoded == ETH_UPPER_BOUND_30DEC
        assert encoded > ETH_PRICE_30DEC
        assert encoded != SENTINEL_MAX

    @pytest.mark.parametrize(
        ("is_long", "is_open"),
        [(True, True), (False, True), (True, False), (False, False)],
    )
    def test_no_leg_ever_encodes_a_sentinel(self, is_long: bool, is_open: bool) -> None:
        """Cross-cutting guard: neither sentinel may appear on ANY leg.

        Kept separate from the exact-value tests so that a future change to the
        derivation formula cannot quietly reintroduce an unbounded order while
        the per-leg expectations are "updated to match".
        """
        intent = _open_intent(is_long=is_long) if is_open else _close_intent(is_long=is_long)
        result = _compile(_make_open_compiler() if is_open else _make_compiler(), intent)
        assert result.status == CompilationStatus.SUCCESS, result.error

        encoded = _decode_acceptable_price(result)
        assert encoded not in (SENTINEL_MIN, SENTINEL_MAX)
        # A real bound sits within an order of magnitude of spot; both sentinels
        # are >14 orders of magnitude away.
        assert ETH_PRICE_30DEC // 10 < encoded < ETH_PRICE_30DEC * 10

    def test_encoded_bound_tracks_the_users_max_slippage(self) -> None:
        """The tolerance must be load-bearing, not decorative.

        The pre-VIB-6219 code computed ``slippage_bps`` and threw it away, so a
        user tightening ``max_slippage`` changed nothing about the calldata.
        """
        tight = _decode_acceptable_price(
            _compile(_make_open_compiler(), _open_intent(is_long=True, max_slippage=Decimal("0.001")))
        )
        loose = _decode_acceptable_price(
            _compile(_make_open_compiler(), _open_intent(is_long=True, max_slippage=Decimal("0.05")))
        )
        assert tight == 3_003 * 10**12  # 3000 * 1.001
        assert loose == 3_150 * 10**12  # 3000 * 1.05
        assert tight < loose, "a tighter tolerance must encode a tighter cap"

    @pytest.mark.parametrize(
        ("is_long", "is_open", "expected_30dec"),
        [
            # 3000 * 1.05 == 3150 (maximum legs); 3000 * 0.95 == 2850 (minimum legs).
            (True, True, 3_150 * 10**12),  # open long   -> buying  -> maximum
            (False, True, 2_850 * 10**12),  # open short  -> selling -> minimum
            (True, False, 2_850 * 10**12),  # close long  -> selling -> minimum
            (False, False, 3_150 * 10**12),  # close short -> buying  -> maximum
        ],
    )
    def test_every_leg_honours_a_non_default_max_slippage(
        self, is_long: bool, is_open: bool, expected_30dec: int
    ) -> None:
        """All FOUR legs must read the user's tolerance — not just the one we spot-checked.

        The open and close paths call ``_derive_acceptable_price`` from two
        separate call sites, each doing its own ``int(intent.max_slippage *
        10000)``. An implementation that wired the tolerance on the open path and
        hardcoded the default on the close path would satisfy every other test in
        this file: the four-combo tests all use the 1% default, so a hardcoded 1%
        is observationally identical to a correctly-read 1%. This is the test
        that separates them, so it deliberately uses a NON-default 5%.

        (Raised by the UAT-GATE Phase 0b spec critic as a permitted silent error
        in an earlier draft of the card — the hole was real; this closes it.)
        """
        intent = (
            _open_intent(is_long=is_long, max_slippage=Decimal("0.05"))
            if is_open
            else _close_intent(is_long=is_long, max_slippage=Decimal("0.05"))
        )
        result = _compile(_make_open_compiler() if is_open else _make_compiler(), intent)
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert _decode_acceptable_price(result) == expected_30dec

    def test_encoded_bound_uses_the_markets_index_decimals(self) -> None:
        """The price scale is ``30 - index_decimals``; a fixed scale would be wrong.

        SOL/USD's index token has 9 decimals, not ETH's 18 — encoding a
        3000-style 10**12 scale for SOL would be off by 10**9.
        """
        compiler = _make_open_compiler(prices={"SOL": Decimal("150"), "USDC": Decimal("1")})
        result = _compile(compiler, _open_intent(is_long=True, market=SOL_MARKET_ARB))
        assert result.status == CompilationStatus.SUCCESS, result.error

        assert market_record("arbitrum", "SOL/USD").index_token_decimals == 9
        # 150 * 1.01 == 151.5, at scale 10 ** (30 - 9)
        assert _decode_acceptable_price(result) == int(Decimal("151.5") * 10**21)

    def test_bound_is_also_reported_in_bundle_metadata(self) -> None:
        """The encoded bound is observable off-chain, not just inside calldata."""
        result = _compile(_make_open_compiler(), _open_intent(is_long=True))
        metadata = result.action_bundle.metadata
        assert metadata["acceptable_price_30dec"] == str(ETH_UPPER_BOUND_30DEC)
        assert metadata["acceptable_price_usd"] == "3030"
        assert int(metadata["acceptable_price_30dec"]) == _decode_acceptable_price(result)

    def test_trigger_open_encodes_limit_order_with_trigger_anchored_bound(self) -> None:
        """A strategy-authored resting open must remain cancellable on mainnet."""
        result = _compile(
            _make_open_compiler(prices={"ETH": Decimal("3000"), "USDC": Decimal("1")}),
            _open_intent(
                is_long=True,
                trigger_price=Decimal("1500"),
                max_slippage=Decimal("0.02"),
            ),
        )
        assert result.status == CompilationStatus.SUCCESS, result.error

        params = _decode_create_order_params(result)
        assert int(params["orderType"]) == int(OrderType.LIMIT_INCREASE)
        assert int(params["numbers"]["triggerPrice"]) == 1_500 * 10**12
        assert int(params["numbers"]["acceptablePrice"]) == 1_530 * 10**12
        assert result.action_bundle.metadata["trigger_price_usd"] == "1500"

    @pytest.mark.parametrize(
        ("is_long", "trigger_price"),
        [(True, Decimal("3000")), (True, Decimal("3500")), (False, Decimal("3000")), (False, Decimal("2500"))],
    )
    def test_marketable_trigger_is_a_safety_refusal(self, is_long: bool, trigger_price: Decimal) -> None:
        result = _compile(
            _make_open_compiler(prices={"ETH": Decimal("3000"), "USDC": Decimal("1")}),
            _open_intent(is_long=is_long, trigger_price=trigger_price),
        )

        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.transactions == []
        assert "already marketable" in (result.error or "")


# =============================================================================
# Fail closed — and classify the failure correctly
# =============================================================================


class TestFailsClosedWhenItCannotBoundThePrice:
    """No price, no order. The old code shipped an unbounded order instead."""

    def test_missing_index_price_is_a_SAFETY_REFUSAL(self) -> None:
        """A fixed-oracle miss cannot be repaired by the slippage ladder."""
        compiler = _make_open_compiler(prices={"USDC": Decimal("1")})  # no ETH price
        result = _compile(compiler, _open_intent(is_long=True))

        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is not True
        assert result.is_safety_refusal is True
        assert result.transactions == []
        assert "ETH" in (result.error or "")

    def test_missing_index_price_on_CLOSE_is_also_a_safety_refusal(self) -> None:
        """Same classification on the teardown-reachable leg."""
        compiler = _make_compiler(prices={"USDC": Decimal("1")})
        result = _compile(compiler, _close_intent(is_long=True))

        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is not True
        assert result.is_safety_refusal is True
        assert result.transactions == []

    def test_hundred_percent_slippage_is_rejected_at_construction_since_VIB_6217(self) -> None:
        """``max_slippage=1`` is no longer constructible — VIB-6217 moved that bound upstream.

        This test used to build the intent directly and assert the GMX guard refused
        it. Since VIB-6217 (#3496) made ``max_slippage`` ``[0, 1)`` exclusive at all
        seven intent validators, construction itself raises, so the old form could
        never reach the compiler. Pinned here rather than deleted, because losing it
        would drop the record of *where* the bound is enforced.
        """
        with pytest.raises(ValidationError, match=r"max_slippage must be in \[0, 1\)"):
            _open_intent(is_long=False, max_slippage=Decimal("1"))

    def test_hundred_percent_slippage_injected_past_the_validator_is_a_SAFETY_REFUSAL(self) -> None:
        """The GMX guard must still refuse 100% when it arrives by MUTATION, not construction.

        This is the reachable path, and it is not hypothetical: ``model_copy`` does
        **not** re-run pydantic validators, and
        ``teardown_manager.py:1811`` escalates slippage with exactly
        ``intent.model_copy(update={"max_slippage": slippage})`` — so the teardown
        ladder can hand the compiler a value the validator would have rejected
        (**VIB-6238**). VIB-6217's bound is therefore necessary but not sufficient,
        and this connector's own refusal is the remaining line of defence.

        A tolerance of 1 derives a zero lower bound — the very sentinel this ticket
        deletes — so it must be refused, and refused as GUARD_REFUSED (VIB-5746)
        rather than a fault, since zero calldata was built.
        """
        legal = _open_intent(is_long=False, max_slippage=Decimal("0.99"))
        injected = legal.model_copy(update={"max_slippage": Decimal("1")})
        assert injected.max_slippage == Decimal("1"), (
            "model_copy is expected to bypass the validator here — if this assert "
            "fails, VIB-6238 has been fixed and this test should assert the "
            "mutation is rejected instead"
        )

        result = _compile(_make_open_compiler(), injected)

        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.is_transient is False
        assert result.transactions == []

    def test_zero_price_is_a_SAFETY_REFUSAL_not_a_zero_bound(self) -> None:
        """Empty != Zero: a zero price must never become a zero bound."""
        compiler = _make_open_compiler(prices={"ETH": Decimal("0"), "USDC": Decimal("1")})
        result = _compile(compiler, _open_intent(is_long=True))

        assert result.status == CompilationStatus.FAILED
        assert result.transactions == []
        # A zero oracle entry is indistinguishable from a missing one at the
        # read boundary, so either classification is defensible — what is NOT
        # defensible is compiling an order.
        assert result.is_transient or result.is_safety_refusal

    def test_unverified_market_has_no_index_decimals_and_is_a_SAFETY_REFUSAL(self) -> None:
        """No verified decimals ⇒ no trustworthy price scale ⇒ refuse rather than misscale.

        Successor of the pre-address-first "market without index decimals" case
        (which patched the curated table to drop the decimals row). The curated
        table is gone: index decimals now come ONLY from the process catalog of
        venue-verified markets. The way a market still reaches price derivation
        without them is a core-alias LABEL (``sdk.get_market_address``) whose
        tuple this process never dynamically verified — so the catalog is
        emptied here rather than primed.
        """
        market_catalog.clear()  # this market is deliberately UNVERIFIED
        compiler = _make_compiler()
        result = _compile(compiler, _open_intent(is_long=True, market="ETH/USD"))

        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.transactions == []
        assert "no verified index-token decimals" in (result.error or "")


# =============================================================================
# The derivation itself
# =============================================================================


class TestDeriveAcceptablePrice:
    """Unit-level checks of the formula, scale and rounding direction."""

    @pytest.mark.parametrize(
        ("is_long", "is_increase", "expect_max"),
        [
            (True, True, True),  # open long   -> buying  -> maximum
            (False, True, False),  # open short  -> selling -> minimum
            (True, False, False),  # close long  -> selling -> minimum
            (False, False, True),  # close short -> buying  -> maximum
        ],
    )
    def test_direction_table(self, is_long: bool, is_increase: bool, expect_max: bool) -> None:
        assert bound_is_maximum(is_long=is_long, is_increase=is_increase) is expect_max

        derived = derive_acceptable_price_30dec(
            index_price_usd=ETH_PRICE_USD,
            index_token_decimals=18,
            slippage_bps=100,
            is_long=is_long,
            is_increase=is_increase,
            context="test",
        )
        if expect_max:
            assert derived == ETH_UPPER_BOUND_30DEC > ETH_PRICE_30DEC
        else:
            assert derived == ETH_LOWER_BOUND_30DEC < ETH_PRICE_30DEC

    def test_scale_matches_gmx_convention_across_decimals(self) -> None:
        """``raw = usd * 10 ** (30 - decimals)`` — the convention the receipt parser reads back."""
        for decimals, price, expected in [
            (18, Decimal("3000"), 3000 * 10**12),  # ETH
            (8, Decimal("65000"), 65000 * 10**22),  # BTC
            (9, Decimal("150"), 150 * 10**21),  # SOL
            (6, Decimal("0.5"), 5 * 10**23),  # XRP-like
        ]:
            derived = derive_acceptable_price_30dec(
                index_price_usd=price,
                index_token_decimals=decimals,
                slippage_bps=0,
                is_long=True,
                is_increase=True,
                context="test",
            )
            assert derived == expected, f"decimals={decimals}"

    def test_zero_slippage_pins_the_bound_to_spot(self) -> None:
        for is_long in (True, False):
            assert (
                derive_acceptable_price_30dec(
                    index_price_usd=ETH_PRICE_USD,
                    index_token_decimals=18,
                    slippage_bps=0,
                    is_long=is_long,
                    is_increase=True,
                    context="test",
                )
                == ETH_PRICE_30DEC
            )

    def test_rounding_always_tightens_the_bound(self) -> None:
        """Sub-unit rounding must never loosen protection.

        A maximum rounds DOWN and a minimum rounds UP, matching
        ``anvil_order_executor._read_price_bounds``'s FLOOR/CEILING pair.
        """
        price = Decimal("1234.5678901234567")
        exact = price * Decimal(10) ** 12

        upper = derive_acceptable_price_30dec(
            index_price_usd=price,
            index_token_decimals=18,
            slippage_bps=0,
            is_long=True,
            is_increase=True,
            context="test",
        )
        lower = derive_acceptable_price_30dec(
            index_price_usd=price,
            index_token_decimals=18,
            slippage_bps=0,
            is_long=False,
            is_increase=True,
            context="test",
        )
        assert upper <= exact, "a maximum must never round up"
        assert lower >= exact, "a minimum must never round down"
        assert upper >= exact - 1 and lower <= exact + 1, "rounding must be sub-unit"

    @pytest.mark.parametrize("slippage_bps", [-1, 10_000, 10_001])
    def test_out_of_range_slippage_is_refused(self, slippage_bps: int) -> None:
        with pytest.raises(UnprotectedTradeError):
            derive_acceptable_price_30dec(
                index_price_usd=ETH_PRICE_USD,
                index_token_decimals=18,
                slippage_bps=slippage_bps,
                is_long=True,
                is_increase=True,
                context="test",
            )

    @pytest.mark.parametrize("price", [Decimal("0"), Decimal("-1")])
    def test_non_positive_price_is_refused(self, price: Decimal) -> None:
        with pytest.raises(UnprotectedTradeError):
            derive_acceptable_price_30dec(
                index_price_usd=price,
                index_token_decimals=18,
                slippage_bps=100,
                is_long=True,
                is_increase=True,
                context="test",
            )

    @pytest.mark.parametrize("decimals", [-1, 31])
    def test_out_of_range_decimals_are_refused(self, decimals: int) -> None:
        """A bad scale silently moves the bound by orders of magnitude — refuse instead."""
        with pytest.raises(UnprotectedTradeError):
            derive_acceptable_price_30dec(
                index_price_usd=ETH_PRICE_USD,
                index_token_decimals=decimals,
                slippage_bps=100,
                is_long=True,
                is_increase=True,
                context="test",
            )

    def test_a_lower_bound_can_never_be_returned_as_zero(self) -> None:
        """A lower bound stays strictly positive even at the truncation limit.

        This is the failure mode ``min_out_guard`` was written for: under FLOOR
        arithmetic a small enough value haircuts to ``0``, which is exactly the
        sentinel. Rounding a *minimum* UP makes that structurally impossible
        here — worth pinning, because switching this branch to FLOOR "for
        symmetry" would silently reintroduce the sentinel for low-priced,
        high-decimal markets.

        ``derive_acceptable_price_30dec`` still routes the result through
        ``require_protective_min``; with ROUND_CEILING that chokepoint is
        defence-in-depth rather than a reachable branch.
        """
        # 30 decimals => scale 10**0, so this would floor to 0.
        derived = derive_acceptable_price_30dec(
            index_price_usd=Decimal("0.0001"),
            index_token_decimals=30,
            slippage_bps=100,
            is_long=False,
            is_increase=True,
            context="test",
        )
        assert derived == 1, "a sub-unit minimum must round UP to 1, never down to the sentinel"

    def test_usd_round_trip(self) -> None:
        assert price_30dec_to_usd(ETH_PRICE_30DEC, 18) == ETH_PRICE_USD
        assert price_30dec_to_usd(65000 * 10**22, 8) == Decimal("65000")


# =============================================================================
# Reachability — is the price the compiler now needs actually THERE in production?
# =============================================================================


class TestPriceIsReachableOnTheProductionPath:
    """This repo repeatedly ships slippage machinery that never reaches the encoder.

    The inverse risk applies to a fail-closed guard: if the price it demands is
    not populated on the real run path, the guard doesn't protect trades — it
    blocks them. These tests pin the wiring the derivation depends on.
    """

    def test_compiler_asks_for_the_same_symbol_production_valuation_already_uses(self) -> None:
        """The index symbol is resolved identically to ``perps_read._gmx_market_metadata``.

        That helper feeds ``portfolio_valuer``'s live
        ``market.price(meta.index_token_symbol)`` call for every open GMX
        position, so the symbol is already proven-resolvable on the production
        price path. Requiring the *same* symbol at compile time inherits that
        proof instead of inventing a new lookup that might not be populated.
        The compile side reads the verified catalog (primed from the audited
        fixture snapshot here), so this also pins fixture/valuation agreement.
        """
        from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
        from almanak.connectors.gmx_v2.perps_read import _gmx_market_metadata

        compiler = GMXV2Compiler()
        checked = 0
        for chain, record in FIXTURE_MARKETS:
            meta = _gmx_market_metadata(record.market_token, chain)
            assert meta is not None, f"{chain}/{record.label} has no valuation metadata"
            assert compiler._index_symbol_for_market(chain, record.market_token) == meta.index_token_symbol
            # The decimals used for price scaling must agree too — a
            # mismatch would bound the order at the wrong magnitude.
            assert compiler._index_token_decimals(chain, record.market_token) == meta.index_token_decimals
            checked += 1
        assert checked >= 15, "expected the full fixture catalogue, not a patched stub"

    def test_symbol_lookup_is_alias_independent(self) -> None:
        """Every input alias for one market yields ONE symbol, keyed off the address.

        "ETH", "WETH" and "ETH/USD" all resolve to the same market address, so
        keying the symbol lookup off the resolved ADDRESS (not the user-typed
        ``intent.market``) is what makes the symbol unique; keying off the raw
        string would ask the oracle for a price of "WETH/USD".
        """
        from almanak.connectors.gmx_v2.compiler import GMXV2Compiler

        assert GMXV2Compiler()._index_symbol_for_market("arbitrum", ETH_MARKET_ARB) == "ETH"
        # Case-insensitive on the address, as every other GMX address lookup is.
        assert GMXV2Compiler()._index_symbol_for_market("arbitrum", ETH_MARKET_ARB.lower()) == "ETH"

    def test_every_catalogued_market_can_produce_a_bound(self) -> None:
        """No verified market is structurally un-compilable.

        Every fixture row is an audited snapshot of a venue-verified record; a
        row whose decimals could not scale a bound would fail closed forever on
        a market the venue really serves — pin the consequence for THIS guard
        across the full 6..24-decimal range the catalogue spans.
        """
        for chain, record in FIXTURE_MARKETS:
            bound = derive_acceptable_price_30dec(
                index_price_usd=Decimal("100"),
                index_token_decimals=record.index_token_decimals,
                slippage_bps=100,
                is_long=True,
                is_increase=True,
                context=f"{chain}/{record.label}",
            )
            assert bound > 0

    @pytest.mark.parametrize(
        ("chain", "market"),
        [pytest.param(chain, record.label, id=f"{chain}-{record.label}") for chain, record in FIXTURE_MARKETS],
    )
    def test_every_catalogued_market_compiles_the_CLOSE_leg(self, chain: str, market: str) -> None:
        """Guard the teardown-reachable compile path across the whole catalogue."""
        record = market_record(chain, market)
        compiler = _make_compiler(
            chain,
            prices={record.index_symbol: Decimal("100"), "USDC": Decimal("1")},
        )

        result = _compile(compiler, _close_intent(is_long=True, market=record.market_token))

        assert result.status == CompilationStatus.SUCCESS, f"{chain}/{market}: {result.error}"
        assert _decode_acceptable_price(result) > 0


# =============================================================================
# Fixes applied in response to the multi-auditor audit
# =============================================================================


class TestArithmeticIsExactSoRoundingOnlyTightens:
    """The FLOOR/CEILING ratchet is only one-way if the arithmetic is exact.

    Found by CodeRabbit. Under Python's ambient 28-digit Decimal context each
    intermediate multiplication pre-rounds ROUND_HALF_EVEN — outward as often as
    inward — so the explicit rounding then applied to an already-loosened value.
    The module's advertised "truncation can only tighten" invariant silently
    failed for prices carrying more than 28 significant digits.

    Those are not exotic inputs: ``Decimal(some_float)`` expands to the float's
    exact binary value (``Decimal(3000.1)`` has 43 significant digits), and this
    module prices whatever the oracle produced.
    """

    @staticmethod
    def _exact(price: Decimal, decimals: int, slippage_bps: int, upper: bool) -> int:
        """Reference value computed at 300 digits — the answer with no pre-rounding."""
        from decimal import ROUND_CEILING, ROUND_FLOOR, localcontext

        with localcontext() as ctx:
            ctx.prec = 300
            raw = price * (Decimal(10) ** (30 - decimals))
            frac = Decimal(slippage_bps) / Decimal(10_000)
            value = raw * (Decimal(1) + frac) if upper else raw * (Decimal(1) - frac)
            return int(value.to_integral_value(rounding=ROUND_FLOOR if upper else ROUND_CEILING))

    @pytest.mark.parametrize(
        ("price", "decimals", "slippage_bps"),
        [
            # The exact case measured to loosen before the fix: a 1-unit-too-low minimum.
            (Decimal("1.000000000000000000000000000001"), 18, 1),
            (Decimal("99999999999999999999999999999999.9"), 18, 9999),
            (Decimal("3000.123456789012345678901234567890"), 18, 100),
            (Decimal("65432.98765432109876543210987654321"), 8, 1),
            # Float-sourced price: 43 significant digits, entirely reachable.
            (Decimal(3000.1), 18, 100),
            (Decimal(0.1), 18, 9999),
        ],
    )
    @pytest.mark.parametrize("upper", [True, False])
    def test_bound_is_never_looser_than_the_exact_value(
        self, price: Decimal, decimals: int, slippage_bps: int, upper: bool
    ) -> None:
        got = derive_acceptable_price_30dec(
            index_price_usd=price,
            index_token_decimals=decimals,
            slippage_bps=slippage_bps,
            is_long=upper,
            is_increase=True,
            context="precision",
        )
        want = self._exact(price, decimals, slippage_bps, upper)
        if upper:
            assert got <= want, f"maximum LOOSENED: {got} > exact {want}"
        else:
            assert got >= want, f"minimum LOOSENED: {got} < exact {want}"

    @pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity")])
    def test_non_finite_prices_raise_the_DOCUMENTED_error(self, bad: Decimal) -> None:
        """NaN/Infinity must refuse as UnprotectedTradeError, not leak another type.

        Before the fix ``NaN`` raised ``InvalidOperation`` (``Decimal("NaN") <= 0``
        raises rather than returning False) and ``Infinity`` raised
        ``OverflowError`` from ``int()``. Both are swallowed by the compiler's
        outer ``except Exception`` into a generic FAILED — fail-closed, never a
        money leak — but a guard that raises something other than what it
        documents cannot be relied on by a caller catching the documented type.
        """
        with pytest.raises(UnprotectedTradeError):
            derive_acceptable_price_30dec(
                index_price_usd=bad,
                index_token_decimals=18,
                slippage_bps=100,
                is_long=True,
                is_increase=True,
                context="non-finite",
            )


class TestSubBasisPointToleranceIsRefusedNotTruncated:
    """`int(max_slippage * 10000)` truncates; a silent truncation to 0 bps is refused.

    Found by CodeRabbit. A tolerance finer than one basis point is not a safety
    problem — 0 bps is the tightest bound there is — but it silently converts an
    explicit user request into "pin to spot exactly", producing an order that
    almost certainly cannot fill while still burning the keeper execution fee.
    """

    def test_sub_bp_tolerance_is_a_SAFETY_REFUSAL(self) -> None:
        result = _compile(_make_open_compiler(), _open_intent(is_long=True, max_slippage=Decimal("0.00005")))
        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.transactions == []
        assert "basis-point" in result.error or "basis point" in result.error

    def test_sub_bp_tolerance_is_also_refused_on_CLOSE(self) -> None:
        result = _compile(_make_compiler(), _close_intent(is_long=True, max_slippage=Decimal("0.00009")))
        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.transactions == []

    def test_exactly_zero_is_STILL_ALLOWED(self) -> None:
        """Pinning to spot on purpose is a legitimate request, not a truncation."""
        result = _compile(_make_open_compiler(), _open_intent(is_long=True, max_slippage=Decimal("0")))
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert _decode_acceptable_price(result) == ETH_PRICE_30DEC

    def test_exactly_one_bp_is_allowed(self) -> None:
        """The boundary the error message tells the user to use must actually work."""
        result = _compile(_make_open_compiler(), _open_intent(is_long=True, max_slippage=Decimal("0.0001")))
        assert result.status == CompilationStatus.SUCCESS, result.error
        # 3000 * 1.0001 == 3000.3
        assert _decode_acceptable_price(result) == int(Decimal("3000.3") * 10**12)

    def test_non_decimal_tolerance_is_permanent_safety_refusal(self) -> None:
        legal = _open_intent(is_long=True)
        malformed = legal.model_copy(update={"max_slippage": "0.005"})

        result = _compile(_make_open_compiler(), malformed)

        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.is_transient is False
        assert result.transactions == []


class TestPlaceholderPricesCannotProduceABound:
    """A placeholder `$1` price must REFUSE, never derive a bound (audit Blocker 1).

    The reachable fail-open this suite otherwise missed. When the compiler has no
    price oracle at all (`_using_placeholders`, which the runner enters whenever
    its price pre-fetch yields nothing and which the gateway's compile path
    permits unconditionally), `require_token_price` returns a fake
    `Decimal("1")` for any symbol. Deriving from that:

      short open on BTC/USD -> minimum ~= 0.99 * 10**22
      real execution price  ~= 10**27

    GMX requires `executionPrice >= acceptablePrice` for that leg, so the bound
    is trivially satisfied — the order broadcasts with NO protection, while the
    log line and bundle metadata report a real-looking number. That is precisely
    the defect VIB-6219 deletes, re-entered through a transient price outage
    rather than a hardcoded sentinel.

    No guard inside `derive_acceptable_price_30dec` can catch it: `$1` is finite
    and strictly positive. It has to be caught at the read.

    These tests use the REAL `IntentCompiler.__init__` rather than the
    `__new__`-plus-hand-set-attributes harness the rest of this file uses,
    because the placeholder wiring lives in `__init__` — a harness that bypasses
    it cannot observe this bug at all (audit finding 15).
    """

    @staticmethod
    def _placeholder_mode_compiler() -> IntentCompiler:
        """A compiler with NO price oracle — exactly the runner's degraded state."""
        return IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rpc_url="http://127.0.0.1:1",
            price_oracle=None,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    def test_the_harness_really_is_in_placeholder_mode(self) -> None:
        """Anti-vacuity: prove the fake $1 is actually on offer before asserting refusal."""
        compiler = self._placeholder_mode_compiler()
        assert compiler._using_placeholders is True
        assert compiler._require_token_price("BTC") == Decimal("1"), (
            "if this stops returning a fake $1 the bug is gone and these tests are vacuous"
        )

    @pytest.mark.parametrize("is_long", [True, False])
    def test_open_refuses_instead_of_bounding_against_one_dollar(self, is_long: bool) -> None:
        compiler = self._placeholder_mode_compiler()
        # The OPEN leg demands CURRENT listing, so it must get past dynamic
        # resolution to reach the placeholder guard this test pins.
        compiler._gateway_client = fake_dynamic_gateway("arbitrum")
        result = _compile(compiler, _open_intent(is_long=is_long, market=BTC_MARKET_ARB))
        assert result.status == CompilationStatus.FAILED, (
            f"placeholder mode must refuse; got {result.status} with "
            f"acceptablePrice={result.action_bundle.metadata.get('acceptable_price_30dec') if result.action_bundle else None}"
        )
        assert result.transactions == [], "no calldata may be built from a placeholder price"
        assert result.is_transient is not True
        assert result.is_safety_refusal is True

    @pytest.mark.parametrize("is_long", [True, False])
    def test_close_refuses_too(self, is_long: bool) -> None:
        """The teardown-reachable leg. A fix applied only to open would pass the test above."""
        result = _compile(self._placeholder_mode_compiler(), _close_intent(is_long=is_long, market=BTC_MARKET_ARB))
        assert result.status == CompilationStatus.FAILED
        assert result.transactions == []
        assert result.is_transient is not True
        assert result.is_safety_refusal is True

    def test_a_real_oracle_still_compiles(self) -> None:
        """Negative control: the guard must not reject a genuine price.

        Without this, "refuse in placeholder mode" and "refuse always" are
        indistinguishable, and a guard that blocks every GMX perp would pass
        every other test in this class.
        """
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rpc_url="http://127.0.0.1:1",
            price_oracle={"BTC": Decimal("65000"), "USDC": Decimal("1")},
        )
        assert compiler._using_placeholders is False
        # Same open-leg listing requirement as above: dynamic resolution must
        # succeed for the genuine price to reach the encoder.
        compiler._gateway_client = fake_dynamic_gateway("arbitrum")
        result = _compile(compiler, _open_intent(is_long=True, market=BTC_MARKET_ARB))
        assert result.status == CompilationStatus.SUCCESS, result.error
        # BTC index token is 8 decimals -> scale 10**22. 65000 * 1.01 == 65650.
        assert _decode_acceptable_price(result) == int(Decimal("65650") * 10**22)

    def test_permission_discovery_is_EXEMPT_from_the_placeholder_guard(self) -> None:
        """The guard must not brick the Safe/Zodiac manifest.

        Permission discovery compiles synthetic perp intents offline, with no
        price oracle by construction, purely to enumerate the (target, selector)
        pairs a Safe must authorise. That calldata is never signed. Refusing
        there yields ZERO permissions — and an empty Zodiac Roles manifest means
        every Safe GMX call reverts at `execTransactionWithRole`, which
        AGENTS.md §Connector additions calls out by name.

        Found because `tests/unit/permissions/test_protocol_compatibility.py`
        went red when the placeholder guard was first added unscoped. Pinned
        here too so the exemption is visible from the guard's own suite, not
        only from a permissions test three directories away.
        """
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rpc_url="http://127.0.0.1:1",
            price_oracle=None,
            config=IntentCompilerConfig(allow_placeholder_prices=True, permission_discovery=True),
        )
        assert compiler._using_placeholders is True
        result = _compile(compiler, _open_intent(is_long=True, market=BTC_MARKET_ARB))
        assert result.status == CompilationStatus.SUCCESS, (
            f"permission discovery must still compile; got {result.error}"
        )
        assert result.transactions, "discovery needs calldata to enumerate selectors from"
