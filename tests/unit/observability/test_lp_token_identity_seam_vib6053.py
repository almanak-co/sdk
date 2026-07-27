"""Golden seam-contract regression for the LP token-identity class (VIB-6053).

Every case below is reconstructed from the REAL archived evidence of the
2026-07-26 platform LP matrix (`tests/platform/results/20260726-lp-anvil-{eoa,safe}/
evidence/<cell>/ledger.json`), using the `extracted_data_json` payload the parser
actually emitted and the `transaction_ledger` row the seam actually wrote. The
"before" numbers in each docstring are the persisted values, not illustrations.

The class: a ledger row is a set of (token, amount) pairs, and it is correct only
when BOTH halves come from the same layer's leg ordering. The receipt layer orders
by the venue's canonical slot (V3 pool `token0()`/`token1()`, Curve `coins(i)`);
the intent layer orders by the user's pool label. The seam took amounts from the
first and symbols from the second, transposing the row AND scaling each leg by the
other token's decimals.

Three prior fixes (VIB-5851, VIB-5983, VIB-5988) compensated at consumers and the
class survived each. These tests pin the producer-side contract instead: identity
travels WITH the amount, so there is nothing left to infer.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.observability.ledger import (
    _extract_from_lp_close,
    _extract_from_lp_open,
)

# Canonical mainnet addresses — the pools' real token0/token1.
ETH_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ETH_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
BASE_WETH = "0x4200000000000000000000000000000000000006"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _result(**extracted):
    return SimpleNamespace(extracted_data=extracted, swap_amounts=None)


def _lp_open(**kw):
    fields = {
        "amount0": None,
        "amount1": None,
        "currency0": None,
        "currency1": None,
        "coin_symbols": None,
        "additional_amounts": None,
    }
    return SimpleNamespace(**{**fields, **kw})


def _lp_close(**kw):
    fields = {
        "amount0_collected": None,
        "amount1_collected": None,
        "currency0": None,
        "currency1": None,
        "coin_symbols": None,
        "additional_amounts": None,
    }
    return SimpleNamespace(**{**fields, **kw})


class TestUniswapV3EthereumTransposition:
    """VIB-6053 — `uniswap_v3__ethereum`, both lanes, OPEN and CLOSE.

    On ethereum the WETH/USDC pool's `token0()` is USDC (0xa0b8… < 0xc02a…), so the
    receipt's `amount0=49644145` is USDC-6dp and `amount1=26524078519924184` is
    WETH-18dp. The strategy's pool label is "WETH/USDC/3000", the opposite order.

    Persisted pre-fix (real row):
        token_in=WETH  amount_in=4.9644145E-11        <- USDC raw / 10**18
        token_out=USDC amount_out=26524078519.924184  <- WETH raw / 10**6

    A $100 position booked as a $26.5bn row, self-consistently across open and
    close so no single-row sanity check could catch it.
    """

    INTENT = SimpleNamespace(pool="WETH/USDC/3000", token0="WETH", token1="USDC", amount0=None, amount1=None)

    def test_open_pairs_each_amount_with_its_own_token(self):
        result = _result(
            lp_open_data=_lp_open(
                amount0=49644145,
                amount1=26524078519924184,
                currency0=ETH_USDC,
                currency1=ETH_WETH,
            )
        )
        token_in, token_out, amount_in, amount_out, _, _ = _extract_from_lp_open(self.INTENT, result, "ethereum")
        assert (token_in, token_out) == ("USDC", "WETH")
        assert Decimal(amount_in) == Decimal("49.644145")
        assert Decimal(amount_out) == Decimal("0.026524078519924184")

    def test_close_pairs_each_amount_with_its_own_token(self):
        result = _result(
            lp_close_data=_lp_close(
                amount0_collected=49644144,
                amount1_collected=26524078519924183,
                currency0=ETH_USDC,
                currency1=ETH_WETH,
            )
        )
        token_in, token_out, amount_in, amount_out, _, _ = _extract_from_lp_close(self.INTENT, result, "ethereum")
        assert (token_in, token_out) == ("USDC", "WETH")
        assert Decimal(amount_in) == Decimal("49.644144")
        assert Decimal(amount_out) == Decimal("0.026524078519924183")

    def test_label_order_no_longer_influences_the_row(self):
        """The same receipt under the OPPOSITE label must produce the same row.

        This is the property the class violated: pre-fix the row tracked the
        user's label, not the chain.
        """
        flipped = SimpleNamespace(pool="USDC/WETH/3000", token0="USDC", token1="WETH", amount0=None, amount1=None)
        payload = dict(
            amount0=49644145,
            amount1=26524078519924184,
            currency0=ETH_USDC,
            currency1=ETH_WETH,
        )
        a = _extract_from_lp_open(self.INTENT, _result(lp_open_data=_lp_open(**payload)), "ethereum")
        b = _extract_from_lp_open(flipped, _result(lp_open_data=_lp_open(**payload)), "ethereum")
        assert a == b


class TestCurveAllEmptyClose:
    """VIB-6051 — `curve__{ethereum,arbitrum,optimism,polygon,base}`, both lanes.

    Curve pool ids are registry nicknames ("3pool"), so the "WETH/USDC" pool-string
    split yields nothing and the intent carries no token0/token1. Both symbols
    resolved to "", `_lp_amount_to_human` returned None for every non-zero raw, and
    the row persisted with ALL FOUR money columns empty on a SUCCESSFUL close.

    Persisted pre-fix (real row): token_in="" amount_in="" token_out="" amount_out=""
    — while the same row's `extracted_data_json` carried the full measurement and
    `position_events` carried the correct amounts. The data was never missing; the
    seam simply never read `coin_symbols` (stamped since VIB-5429).
    """

    INTENT = SimpleNamespace(pool="3pool", amount0=None, amount1=None)

    def test_close_resolves_legs_from_the_pool_coin_registry(self):
        result = _result(
            lp_close_data=_lp_close(
                amount0_collected=16476418910488126183,
                amount1_collected=15812206,
                coin_symbols=["DAI", "USDC", "USDT"],
                additional_amounts={2: 67760105},
            )
        )
        token_in, token_out, amount_in, amount_out, _, _ = _extract_from_lp_close(self.INTENT, result, "ethereum")
        assert (token_in, token_out) == ("DAI", "USDC")
        assert Decimal(amount_in) == Decimal("16.476418910488126183")
        assert Decimal(amount_out) == Decimal("15.812206")

    def test_coin_symbols_are_index_aligned_not_alphabetical(self):
        """Slot i is pool coin i. A different registry order must move the amounts."""
        result = _result(
            lp_close_data=_lp_close(
                amount0_collected=15812206,
                amount1_collected=16476418910488126183,
                coin_symbols=["USDC", "DAI", "USDT"],
            )
        )
        token_in, token_out, amount_in, amount_out, _, _ = _extract_from_lp_close(self.INTENT, result, "ethereum")
        assert (token_in, token_out) == ("USDC", "DAI")
        assert Decimal(amount_in) == Decimal("15.812206")
        assert Decimal(amount_out) == Decimal("16.476418910488126183")

    def test_missing_registry_symbols_stay_unmeasured_never_fabricated(self):
        """Empty != Zero: no identity means no number, not a zero."""
        result = _result(lp_close_data=_lp_close(amount0_collected=16476418910488126183, coin_symbols=None))
        extracted = _extract_from_lp_close(self.INTENT, result, "ethereum")
        if extracted is not None:
            _, _, amount_in, amount_out, _, _ = extracted
            assert amount_in == ""
            assert amount_out == ""


class TestAerodromeMagnitudeSort:
    """VIB-6045 — `aerodrome__{base,optimism}`, both lanes.

    The Path-B transfer fallback ordered legs by RAW INTEGER magnitude across
    tokens with different decimals, then discarded the token addresses entirely,
    so no downstream consumer could repair the pairing.

    Persisted pre-fix (real row):
        token_in=USDC  amount_in=29331153505356442 / 10**6  = 29331153505.356442
        token_out=WETH amount_out=55056478 / 10**18         = 5.5056478E-11

    A $110 close booked as a $29.3bn row — and that number also sizes the teardown
    swap-back (ALM-2766 clamp), so it stranded real funds.
    """

    INTENT = SimpleNamespace(pool="USDC/WETH/volatile", token0="USDC", token1="WETH", amount0=None, amount1=None)

    def test_close_binds_amounts_to_their_own_tokens(self):
        result = _result(
            lp_close_data=_lp_close(
                amount0_collected=29331153505356442,
                amount1_collected=55056478,
                currency0=BASE_WETH,
                currency1=BASE_USDC,
            )
        )
        token_in, token_out, amount_in, amount_out, _, _ = _extract_from_lp_close(self.INTENT, result, "base")
        assert (token_in, token_out) == ("WETH", "USDC")
        assert Decimal(amount_in) == Decimal("0.029331153505356442")
        assert Decimal(amount_out) == Decimal("55.056478")
        # The pre-fix row's headline number must be impossible now.
        assert Decimal(amount_in) < Decimal("1")
        assert Decimal(amount_out) < Decimal("1000")


class TestFallbackIsAdditiveNotReplacing:
    """PR A is strictly additive: a parser that emits no identity is untouched.

    This is what lets the producers land before the fail-closed step. If these
    regress, a connector whose parser change was missed would silently lose its
    (previously correct) row rather than keep it.
    """

    def test_no_identity_still_uses_the_intent_label_order(self):
        intent = SimpleNamespace(pool="WETH/USDC/3000", token0="WETH", token1="USDC", amount0=None, amount1=None)
        result = _result(lp_open_data=_lp_open(amount0=1000, amount1=2000))
        token_in, token_out, _, _, _, _ = _extract_from_lp_open(intent, result, "ethereum")
        assert (token_in, token_out) == ("WETH", "USDC")

    def test_pool_string_fallback_survives(self):
        intent = SimpleNamespace(pool="WETH/USDC/3000", amount0=None, amount1=None)
        result = _result(lp_open_data=_lp_open(amount0=1000, amount1=2000))
        token_in, token_out, _, _, _, _ = _extract_from_lp_open(intent, result, "ethereum")
        assert (token_in, token_out) == ("WETH", "USDC")

    @pytest.mark.parametrize("measured_zero", [0])
    def test_measured_zero_stays_measured(self, measured_zero):
        """Empty != Zero in the other direction: a measured 0 is not unmeasured."""
        intent = SimpleNamespace(pool="WETH/USDC/3000", token0="WETH", token1="USDC", amount0=None, amount1=None)
        result = _result(
            lp_close_data=_lp_close(
                amount0_collected=measured_zero,
                amount1_collected=55056478,
                currency0=BASE_WETH,
                currency1=BASE_USDC,
            )
        )
        _, _, amount_in, amount_out, _, _ = _extract_from_lp_close(intent, result, "base")
        assert amount_in == "0"
        assert Decimal(amount_out) == Decimal("55.056478")


class TestParserCurrencyPairing:
    """VIB-6053 — `_pair_tokens_from_parser_currencies`, the Layer-3 observation branch.

    Extracted from `_realign_event_lp_pair_if_needed` so the CRAP gate stays green and
    so the branch is directly testable. Its three-way return is load-bearing and each
    arm means something different:

    * `None`  — parser emitted nothing; fall through to the inference branches (this
      is what keeps the change additive for unmigrated connectors);
    * `(a,b)` — parser observed identity; adopt it;
    * `("","")` — parser observed identity but it did not resolve. Deliberately NOT
      `None`, because falling through would let an address-sort *inference* override
      a parser *observation* on a money path.
    """

    from types import SimpleNamespace as _NS

    ETH_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    ETH_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    @staticmethod
    def _call(lp_data, chain="ethereum"):
        from almanak.framework.observability.position_events import (
            _pair_tokens_from_parser_currencies,
        )

        return _pair_tokens_from_parser_currencies(lp_data, chain)

    def test_absent_currencies_return_none_so_caller_falls_through(self):
        assert self._call(_lp_close()) is None
        assert self._call(None) is None

    def test_partial_currencies_return_none(self):
        """One currency is not identity — never pair off half an observation."""
        assert self._call(_lp_close(currency0=self.ETH_USDC)) is None
        assert self._call(_lp_close(currency1=self.ETH_WETH)) is None

    def test_both_currencies_resolve_to_symbols_in_slot_order(self):
        got = self._call(_lp_close(currency0=self.ETH_USDC, currency1=self.ETH_WETH))
        assert got == ("USDC", "WETH")

    def test_slot_order_is_preserved_not_normalised(self):
        """Reversing the parser's slots reverses the result — no re-sorting."""
        got = self._call(_lp_close(currency0=self.ETH_WETH, currency1=self.ETH_USDC))
        assert got == ("WETH", "USDC")

    def test_unresolvable_currencies_return_empty_pair_not_none(self):
        """The distinction that stops an inference overriding an observation."""
        got = self._call(
            _lp_close(currency0="0x" + "de" * 20, currency1="0x" + "ad" * 20),
            chain="ethereum",
        )
        assert got == ("", "")
        assert got is not None

    def test_works_on_dict_shaped_lp_data(self):
        """Payloads round-trip through JSON, so dict access must work too."""
        got = self._call({"currency0": self.ETH_USDC, "currency1": self.ETH_WETH})
        assert got == ("USDC", "WETH")

    def test_open_side_carrier_is_read_identically(self):
        got = self._call(_lp_open(currency0=self.ETH_USDC, currency1=self.ETH_WETH))
        assert got == ("USDC", "WETH")
