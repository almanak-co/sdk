"""VIB-6053 — parser-emitted currencies drive the `position_events` LP pair.

Parent-level companion to the unit tests on `_pair_tokens_from_parser_currencies`:
these drive the real entry point (`build_position_event_from_intent`) so the branch
is proven **reachable and terminal**, not merely correct in isolation.

The property under test is the one the whole seam fix rests on:

> `currency0`/`currency1` are a DIRECT OBSERVATION by the parser of which token holds
> which amount slot. Every other branch *infers* that pairing. An inference must never
> override an observation.

Deliberately mirrors the VIB-5988 suite's fixtures (same magnitudes, same address
book, same fake resolver) so the two are directly comparable: there, the label order
is inverted relative to chain order and the pair is recovered by *derivation*; here it
is recovered by *observation*, and the observation must win.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.position_events import build_position_event_from_intent
from tests.support.token_resolver import FakeToken, FakeTokenResolver

USDC_RAW = 2_185_779  # 6 dec  -> 2.185779 USDC
WETH_RAW = 1_032_114_889_479_681  # 18 dec -> ~0.001032 WETH

# A correctly-paired position is ~$4. A transposed one is astronomically off, because
# each leg gets the other token's decimals.
SANE_LOW = Decimal("3.0")
SANE_HIGH = Decimal("6.0")
PHANTOM_FLOOR = Decimal("1_000_000")


def _addr(byte: str) -> str:
    return "0x" + byte * 20


# Ethereum-like: USDC address < WETH address, so chain order is USDC-first while the
# human pool label is "WETH/USDC".
_ADDR_BOOK = {"USDC": _addr("11"), "WETH": _addr("cc")}
_DECIMALS = {"USDC": 6, "WETH": 18}
_CHAIN = "ethereum"


@pytest.fixture
def _resolver(monkeypatch):
    # VIB-6100: use the shared double — full ResolvedToken shape (incl. chain),
    # TokenNotFoundError on miss (not None), chain positional-or-keyword.
    # The prior SimpleNamespace double omitted ``chain`` and returned None, so
    # the seam reported defects and every "resolved" assertion was a false green
    # on the fallback branch.
    resolver = FakeTokenResolver(
        {
            "USDC": FakeToken(
                symbol="USDC",
                address=_ADDR_BOOK["USDC"],
                decimals=_DECIMALS["USDC"],
                chain=_CHAIN,
            ),
            "WETH": FakeToken(
                symbol="WETH",
                address=_ADDR_BOOK["WETH"],
                decimals=_DECIMALS["WETH"],
                chain=_CHAIN,
            ),
        }
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: resolver,
    )
    return resolver


class _Intent:
    def __init__(self, intent_type: str, pool: str = "WETH/USDC/25", **kw):
        self.intent_type = type("IT", (), {"value": intent_type})()
        self.protocol = kw.get("protocol", "uniswap_v3")
        self.pool = pool
        self.position_id = "lp_0"
        # `_pair_tokens_from_intent` reads token0/token1 first, then
        # from_token/to_token, and only parses `pool` when a slot is still empty.
        # These — not the pool label — set the label order under test; getting it
        # wrong makes a realign test vacuous (VIB-5988 note).
        self.from_token = kw.get("from_token", "WETH")
        self.to_token = kw.get("to_token", "USDC")
        self.token0 = None
        self.token1 = None


class _Result:
    def __init__(self, extracted: dict, tx_hash: str = "0xopen"):
        self.position_id = "lp_0"
        self.transaction_results = [SimpleNamespace(tx_hash=tx_hash, gas_used=200_000, success=True)]
        self.gas_cost_usd = "1.00"
        self.extracted_data = extracted


def _prices() -> dict:
    return {"WETH": "1917.0", "USDC": "1.0"}


def _build(extracted: dict, intent_type: str = "LP_OPEN", **intent_kw):
    return build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent(intent_type, **intent_kw),
        result=_Result(extracted),
        chain="ethereum",
        price_oracle=_prices(),
    )


def _open_data(**kw) -> LPOpenData:
    return LPOpenData(
        position_id=0,
        tick_lower=-60_000,
        tick_upper=60_000,
        liquidity=500_000,
        amount0=USDC_RAW,  # chain slot 0
        amount1=WETH_RAW,  # chain slot 1
        pool_address=_addr("ab"),
        **kw,
    )


class TestParserCurrenciesDriveThePair:
    def test_open_pair_adopts_parser_currencies_over_label_order(self, _resolver):
        """The label says WETH/USDC; the parser says slot0=USDC. The parser wins."""
        event = _build(
            {"lp_open_data": _open_data(currency0=_ADDR_BOOK["USDC"], currency1=_ADDR_BOOK["WETH"])}
        )
        assert (event.token0, event.token1) == ("USDC", "WETH")
        assert SANE_LOW < Decimal(str(event.value_usd)) < SANE_HIGH
        assert Decimal(str(event.value_usd)) < PHANTOM_FLOOR

    def test_close_pair_adopts_parser_currencies(self, _resolver):
        close = LPCloseData(
            amount0_collected=USDC_RAW,
            amount1_collected=WETH_RAW,
            currency0=_ADDR_BOOK["USDC"],
            currency1=_ADDR_BOOK["WETH"],
            pool_address=_addr("ab"),
        )
        event = _build({"lp_close_data": close}, intent_type="LP_CLOSE")
        assert (event.token0, event.token1) == ("USDC", "WETH")
        assert SANE_LOW < Decimal(str(event.value_usd)) < SANE_HIGH

    def test_parser_order_is_honoured_even_when_it_is_NOT_address_sorted(self, _resolver):
        """The anti-regression that separates observation from derivation.

        Here the parser declares slot0=WETH (the HIGHER address), which an address
        sort would "correct" to USDC-first. It must not: TraderJoe's tokenX/tokenY is
        legitimately not address-ordered, and re-sorting an observed pair is exactly
        the class this fix retires.
        """
        event = _build(
            {
                "lp_open_data": _open_data(
                    currency0=_ADDR_BOOK["WETH"],  # deliberately the higher address
                    currency1=_ADDR_BOOK["USDC"],
                )
            }
        )
        assert (event.token0, event.token1) == ("WETH", "USDC")

    def test_no_currencies_leaves_prior_behaviour_untouched(self, _resolver):
        """PR A is additive: an unmigrated parser must behave exactly as before."""
        event = _build({"lp_open_data": _open_data()})
        assert event.token0 and event.token1
        # Pin the ORDER, not just the set: set-equality would pass under a transposition —
        # the exact regression class this PR fixes. This pins the pre-existing derivation's
        # order for the no-currency (unmigrated) path.
        assert (event.token0, event.token1) == ("USDC", "WETH")

    def test_unresolvable_currencies_do_not_fall_through_to_derivation(self, _resolver):
        """Currencies emitted but unresolvable ⇒ keep label order, do NOT address-sort.

        The parser observed identity we cannot read. Letting the weaker address-sort
        inference take over would be a silent downgrade on a money path.
        """
        event = _build(
            {
                "lp_open_data": _open_data(
                    currency0="0x" + "de" * 20,
                    currency1="0x" + "ad" * 20,
                )
            }
        )
        assert (event.token0, event.token1) == ("WETH", "USDC")  # intent label order
