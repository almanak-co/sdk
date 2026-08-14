"""VIB-5988 — position_events realign gate must key on amount-order provenance,
not on bare ``primitive_money_legs`` key presence.

VIB-5983 (#3422) shipped the realign but skipped it whenever declared money legs
were present, on the premise that "legs are already aligned to amounts". That
premise does not hold on the OPEN path: tokens there come from the intent pool
label (``_pair_tokens_from_intent``) while amounts come from typed
``lp_open_data.amount0``/``amount1``. The legs were never consulted, because
``_pair_tokens_from_declared_legs`` read only ``output_legs`` and an LP_OPEN
declares INPUT legs. Result on a PML-stamping connector (TraderJoe V2):
label-order symbols paired with connector-order amounts → the full VIB-5983
phantom (~$1bn on a ~$4 LP).

The fix adopts the role-appropriate declared pair (INPUT on OPEN, OUTPUT on
CLOSE) and only address-sorts when NO legs were declared.

Address-sorting a PML-stamped pair would be actively wrong for the motivating
connector: TraderJoe V2's ``amount0``/``amount1`` are tokenX/tokenY in emission
order and are explicitly NOT address-sorted (``traderjoe_v2/receipt_parser.py``
— "do NOT sort by token address ... would swap legs whenever tokenX's address >
tokenY's, mis-scaling amounts"). ``test_tj_style_non_address_order_pair_is_not_sorted``
pins that.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.position_events import (
    _pair_tokens_from_declared_legs,
    build_position_event_from_intent,
)

# Same magnitudes as the VIB-5983 suite so the phantom is directly comparable.
USDC_RAW = 2_185_779  # 6 dec  -> 2.185779 USDC
WETH_RAW = 1_032_114_889_479_681  # 18 dec -> ~0.001032 WETH

SANE_LOW = Decimal("3.0")
SANE_HIGH = Decimal("6.0")
PHANTOM_FLOOR = Decimal("1_000_000")


def _addr(byte: str) -> str:
    return "0x" + byte * 20


# Ethereum-like: USDC address < WETH address, so V3 chain order is USDC-first
# while the human pool label is "WETH/USDC".
_ADDR_BOOK = {"USDC": _addr("11"), "WETH": _addr("cc")}
_DECIMALS = {"USDC": 6, "WETH": 18}


@pytest.fixture
def _resolver(monkeypatch):
    class _FakeResolver:
        # VIB-6628: accepts kwargs loosely rather than mirroring production's exact
        # acceptance surface. Conformance (does it accept every legal call?) is
        # enforced by test_resolver_double_conformance_vib6100.py; strictness (does
        # it reject illegal ones?) is tracked there. Tightening needs the surface
        # MEASURED first — a double stricter than production is a false-green
        # generator too, as the chain-alias case in #3472 showed.
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002 — chain positional-or-keyword, as production
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError

            up = str(token).upper()
            if up in _ADDR_BOOK:
                return SimpleNamespace(symbol=up, address=_ADDR_BOOK[up], decimals=_DECIMALS[up], chain=chain)
            for sym, addr in _ADDR_BOOK.items():
                if addr.lower() == str(token).lower():
                    return SimpleNamespace(symbol=sym, address=addr, decimals=_DECIMALS[sym], chain=chain)
            # Production raises on miss; returning None is a VIB-6100 defect
            # (CodeRabbit #3694).
            raise TokenNotFoundError(token=str(token), chain=str(chain), reason="not in fixture address book")

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _FakeResolver(),
    )


class _Intent:
    """LP intent whose pool label is in the human (inverted) order."""

    def __init__(
        self,
        intent_type: str,
        pool: str = "WETH/USDC/25",
        protocol: str = "traderjoe_v2",
        from_token: str = "WETH",
        to_token: str = "USDC",
    ):
        self.intent_type = type("IT", (), {"value": intent_type})()
        self.protocol = protocol
        self.pool = pool
        self.position_id = "traderjoe_lp_0"
        # NOTE: _pair_tokens_from_intent reads token0/token1 then
        # from_token/to_token, and only falls back to parsing `pool` when a slot
        # is still empty. So these — not the pool label — set the label order a
        # test exercises. Getting this wrong makes a realign test vacuous.
        self.from_token = from_token
        self.to_token = to_token
        self.token0 = None
        self.token1 = None


class _Result:
    def __init__(self, extracted: dict, tx_hash: str = "0xopen"):
        self.position_id = "traderjoe_lp_0"
        self.transaction_results = [SimpleNamespace(tx_hash=tx_hash, gas_used=200_000, success=True)]
        self.gas_cost_usd = "1.00"
        self.extracted_data = extracted


def _prices() -> dict:
    return {"WETH": "1917.0", "USDC": "1.0"}


def _legs(*pairs, role: str):
    """Build PrimitiveMoneyLegs of ``role`` from ``(symbol, human_amount)`` pairs."""
    from almanak.connectors._strategy_base.primitive_money_leg import (
        PrimitiveMoneyLeg,
        PrimitiveMoneyLegs,
    )
    from almanak.framework.accounting.measured import MeasuredMoney

    build = PrimitiveMoneyLeg.input if role == "input" else PrimitiveMoneyLeg.output
    return PrimitiveMoneyLegs.of(*(build(sym, MeasuredMoney.measured(Decimal(amt))) for sym, amt in pairs))


def _tj_open_legs():
    """TraderJoe V2 LP_OPEN shape: two INPUT legs in tokenX/tokenY emission order."""
    return _legs(("USDC", "2.185779"), ("WETH", "0.001032"), role="input")


def _open_event(extracted_extra: dict | None = None):
    lp_open = LPOpenData(
        position_id=0,
        tick_lower=-60_000,
        tick_upper=60_000,
        liquidity=500_000,
        amount0=USDC_RAW,  # connector order (tokenX)
        amount1=WETH_RAW,  # connector order (tokenY)
        pool_address=_addr("ab"),
    )
    extracted = {"lp_open_data": lp_open, **(extracted_extra or {})}
    return build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_OPEN"),
        result=_Result(extracted),
        chain="ethereum",
        price_oracle=_prices(),
    )


# ── the role-aware declared-pair reader ──────────────────────────────────


def test_declared_pair_reads_input_legs_when_opening():
    assert _pair_tokens_from_declared_legs({"primitive_money_legs": _tj_open_legs()}, opening=True) == (
        "USDC",
        "WETH",
    )


def test_declared_pair_ignores_input_legs_when_closing():
    """The pre-fix blind spot: OUTPUT-only read returns nothing on an OPEN."""
    assert _pair_tokens_from_declared_legs({"primitive_money_legs": _tj_open_legs()}, opening=False) == (
        None,
        None,
    )


def test_declared_pair_default_is_closing_role():
    """Default must stay OUTPUT so the existing LP_CLOSE call site is unchanged."""
    close_legs = _legs(("USDC", "2.185779"), ("WETH", "0.001032"), role="output")
    assert _pair_tokens_from_declared_legs({"primitive_money_legs": close_legs}) == ("USDC", "WETH")


def test_declared_pair_none_on_single_leg():
    single = _legs(("USDC", "2.185779"), role="input")
    assert _pair_tokens_from_declared_legs({"primitive_money_legs": single}, opening=True) == ("USDC", None)


# ── OPEN path: the reported defect ───────────────────────────────────────


def test_open_with_declared_legs_adopts_leg_order(_resolver):
    """VIB-5988 core: PML-stamped OPEN pairs tokens from the legs, not the label."""
    event = _open_event({"primitive_money_legs": _tj_open_legs()})

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("USDC", "WETH")
    value = Decimal(event.value_usd)
    assert SANE_LOW < value < SANE_HIGH, f"expected ~$4, got {value}"
    assert value < PHANTOM_FLOOR


def test_open_without_declared_legs_still_address_sorts(_resolver):
    """VIB-5983 regression guard — the no-legs path is untouched."""
    event = _open_event()

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("USDC", "WETH")
    assert Decimal(event.value_usd) < PHANTOM_FLOOR


def test_open_with_unusable_legs_keeps_label_order(_resolver, caplog):
    """Legs present but no readable pair (Pendle-style PT input+output, or a
    single-sided add): neither provenance is established, so label order is kept
    and the gap is logged rather than guessed at."""
    odd = _legs(("USDC", "2.185779"), role="input")
    with caplog.at_level("WARNING"):
        event = _open_event({"primitive_money_legs": odd})

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC")
    assert any("VIB-5988" in r.message for r in caplog.records), "unusable-legs gap must be logged"


def test_tj_style_non_address_order_pair_is_not_sorted(_resolver):
    """The reason address-sorting a PML pair is wrong.

    Declared legs in WETH-then-USDC order (a TraderJoe LBPair whose tokenX has
    the HIGHER address) must survive verbatim. Address-sorting would swap them
    away from the amount order and mis-scale exactly as VIB-5983 did.
    """
    legs = _legs(("WETH", "0.001032"), ("USDC", "2.185779"), role="input")
    lp_open = LPOpenData(
        position_id=0,
        tick_lower=-60_000,
        tick_upper=60_000,
        liquidity=500_000,
        amount0=WETH_RAW,  # tokenX = WETH here
        amount1=USDC_RAW,  # tokenY = USDC
        pool_address=_addr("ab"),
    )
    event = build_position_event_from_intent(
        deployment_id="d1",
        # Label order (USDC, WETH) is the INVERSE of the declared leg order, so
        # this discriminates leg-adoption as well as the no-address-sort claim.
        # Without the inversion the _Intent default (WETH, USDC) already equals
        # the expected result and the test passes on origin/main (pr-auditor #8).
        intent=_Intent("LP_OPEN", pool="USDC/WETH/25", from_token="USDC", to_token="WETH"),
        result=_Result({"lp_open_data": lp_open, "primitive_money_legs": legs}),
        chain="ethereum",
        price_oracle=_prices(),
    )

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC")
    value = Decimal(event.value_usd)
    assert SANE_LOW < value < SANE_HIGH, f"expected ~$4, got {value}"


# ── CLOSE path ───────────────────────────────────────────────────────────


def test_close_with_declared_legs_beats_a_prefix_labelorder_cache(_resolver):
    """CLOSE branch of the new gate — the ONLY scenario where it does work.

    pr-auditor Blocker #1: the original version of this test was vacuous. It
    passed on ``origin/main``, because the pre-existing VIB-5221 path already
    fills EMPTY token slots from ``output_legs`` before the realign runs — so
    the new gate was a no-op and the assertion proved nothing.

    The gate only bites when the slots are already NON-empty and wrong: the
    VIB-4086 carry-forward copies ``token0``/``token1`` from a cached OPEN, and
    a pre-fix (or legacy-DB) OPEN cached them in LABEL order. That is the
    in-flight / resumed-strategy case. Seed exactly that state.
    """
    lp_close = LPCloseData(
        position_id=0,
        liquidity_removed=500_000,
        amount0_collected=USDC_RAW,  # chain order: USDC first
        amount1_collected=WETH_RAW,
        pool_address=_addr("ab"),
    )
    legs = _legs(("USDC", "2.185779"), ("WETH", "0.001032"), role="output")
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_CLOSE"),
        result=_Result({"lp_close_data": lp_close, "primitive_money_legs": legs}, tx_hash="0xclose"),
        chain="ethereum",
        price_oracle=_prices(),
        # A pre-fix OPEN row: label order, the inverse of the declared legs.
        recent_open_events={
            ("traderjoe_lp_0", "LP"): {"token0": "WETH", "token1": "USDC"},
        },
    )

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("USDC", "WETH"), (
        "declared OUTPUT legs must override a poisoned label-order cache"
    )
    value = Decimal(event.value_usd)
    assert SANE_LOW < value < SANE_HIGH, f"expected ~$4, got {value}"
    assert value < PHANTOM_FLOOR


# ── real-connector integration (UAT-GATE Phase 0b: SPEC_INSUFFICIENT fix) ──
#
# Every test above hand-builds the PrimitiveMoneyLegs, so a broken TraderJoe leg
# producer — or broken connector -> position_events wiring — would still pass
# them. The load-bearing assumption of this whole PR is "TJ V2 declares two
# INPUT legs on OPEN, in the same tokenX/tokenY order it writes amount0/amount1".
# This test drives the REAL parser to produce those legs and feeds them through
# the REAL producer, in the configuration where an address sort would be WRONG
# (tokenX has the higher address). Receipt-shaping helpers mirror
# tests/unit/connectors/traderjoe_v2/test_traderjoe_v2_receipt_parser_extras.py.

# tokenX address > tokenY address, so address order DISAGREES with LB order.
LB_WALLET = "0x" + "11" * 20
LB_POOL = "0x" + "22" * 20
LB_TOKEN_X = "0x" + "cc" * 20  # WETH — higher address, but LBPair tokenX
LB_TOKEN_Y = "0x" + "dd" * 20  # ... paired against a LOWER-address tokenY below


def _topic_addr(addr: str) -> str:
    return "0x" + "00" * 12 + addr[2:].lower()


def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


def _bins_data(bin_ids: list[int]) -> str:
    ids_offset_hex = _uint256_hex(0x40)
    amounts_offset_hex = _uint256_hex(0x40 + 32 + len(bin_ids) * 32)
    ids_len_hex = _uint256_hex(len(bin_ids))
    ids_elements = "".join(_uint256_hex(b) for b in bin_ids)
    return "0x" + ids_offset_hex + amounts_offset_hex + ids_len_hex + ids_elements + _uint256_hex(0)


def _make_log(topic0: str, contract: str, topics: list[str] | None = None, data: str = "0x") -> dict:
    return {"topics": [topic0, *(topics or [])], "address": contract, "data": data, "logIndex": 0}


def _real_tj_deposit_receipt(token_x: str, token_y: str, x_raw: int, y_raw: int) -> dict:
    """A DepositedToBins LP_OPEN receipt: wallet -> LBPair Transfers, tokenX first."""
    from almanak.connectors.traderjoe_v2.receipt_parser import EVENT_TOPICS

    return {
        "status": 1,
        "transactionHash": "0x" + "6a" * 32,
        "blockNumber": 30,
        "gasUsed": 500,
        "logs": [
            _make_log(
                EVENT_TOPICS["DepositedToBins"],
                LB_POOL,
                topics=[_topic_addr(LB_WALLET), _topic_addr(LB_WALLET)],
                data=_bins_data([8388608]),
            ),
            _make_log(
                EVENT_TOPICS["Transfer"],
                token_x,
                topics=[_topic_addr(LB_WALLET), _topic_addr(LB_POOL)],
                data="0x" + _uint256_hex(x_raw),
            ),
            _make_log(
                EVENT_TOPICS["Transfer"],
                token_y,
                topics=[_topic_addr(LB_WALLET), _topic_addr(LB_POOL)],
                data="0x" + _uint256_hex(y_raw),
            ),
        ],
    }


def _address_resolver(by_address: dict[str, tuple[str, int]]):
    from unittest.mock import MagicMock

    mock = MagicMock()

    def _resolve(addr, _chain=None, **_kw):
        sym, dec = by_address[str(addr).lower()]
        return SimpleNamespace(symbol=sym, decimals=dec)

    mock.resolve.side_effect = _resolve
    return mock


def test_real_traderjoe_legs_drive_the_position_event_pair(_resolver):
    """End-to-end: REAL TraderJoe parser -> REAL position_events producer.

    The LBPair here has tokenX=WETH (0xcc..) and tokenY=USDC (0x11..), i.e.
    tokenX has the HIGHER address, so an address sort would swap the pair away
    from the amount order and mis-scale. The declared legs must win.
    """
    from unittest.mock import patch

    from almanak.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser
    from almanak.framework.execution.extracted_data import LPOpenData

    weth_addr = "0x" + "cc" * 20  # tokenX
    usdc_addr = "0x" + "11" * 20  # tokenY (lower address)

    receipt = _real_tj_deposit_receipt(weth_addr, usdc_addr, WETH_RAW, USDC_RAW)
    resolver = _address_resolver({weth_addr.lower(): ("WETH", 18), usdc_addr.lower(): ("USDC", 6)})

    with patch(
        "almanak.connectors.traderjoe_v2.receipt_parser.get_token_resolver",
        return_value=resolver,
    ):
        # chain= is required: the leg builder only resolves token identity when
        # self._chain is set (otherwise identity is "" — Empty != Zero).
        legs = TraderJoeV2ReceiptParser(chain="ethereum").extract_primitive_money_legs(receipt)

    # Pin the assumption this PR rests on, against the real parser.
    assert legs is not None, "TJ V2 must declare legs for a two-sided deposit"
    assert [leg.token for leg in legs.input_legs] == ["WETH", "USDC"], (
        "TJ V2 must declare INPUT legs in tokenX-then-tokenY emission order"
    )
    assert legs.output_legs == (), "an OPEN must declare no OUTPUT legs"

    # Feed the REAL legs through the REAL producer, with amounts in the same
    # tokenX/tokenY order and an intent pool label in the OPPOSITE order.
    # (the _resolver fixture supplies the framework-side address book: the same
    # USDC=0x11.. / WETH=0xcc.. mapping used for the parser above)
    lp_open = LPOpenData(
        position_id=0,
        tick_lower=-60_000,
        tick_upper=60_000,
        liquidity=500_000,
        amount0=WETH_RAW,  # tokenX
        amount1=USDC_RAW,  # tokenY
        pool_address=LB_POOL,
    )
    event = build_position_event_from_intent(
        deployment_id="d1",
        # Label order (USDC, WETH) is the INVERSE of the declared leg order
        # (WETH, USDC), so this only passes if the legs actually win.
        intent=_Intent("LP_OPEN", pool="USDC/WETH/25", from_token="USDC", to_token="WETH"),
        result=_Result({"lp_open_data": lp_open, "primitive_money_legs": legs}),
        chain="ethereum",
        price_oracle=_prices(),
    )

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC")
    value = Decimal(event.value_usd)
    assert SANE_LOW < value < SANE_HIGH, f"expected ~$4, got {value}"
    assert value < PHANTOM_FLOOR


def test_production_enricher_wiring_lands_legs_for_traderjoe_lp_open():
    """UAT-GATE Phase 0b rev-2 (SPEC_PERMITS_SILENT_ERROR) fix.

    The test above still hand-assembles ``extracted_data``. If the production
    connector -> position_events wiring dropped or renamed the legs, it would
    keep passing. This drives the REAL ``ResultEnricher`` so the legs land in
    ``extracted_data`` by production code, then feeds THAT enriched result to
    the producer.

    Chain of custody under test: TJ receipt -> ResultEnricher overlay
    (``"LP_OPEN": [..., "primitive_money_legs"]``) -> extracted_data ->
    position_events pair selection -> value_usd.
    """
    from unittest.mock import patch

    from almanak.framework.execution.orchestrator import (
        ExecutionContext,
        ExecutionPhase,
        ExecutionResult,
        TransactionResult,
    )
    from almanak.framework.execution.result_enricher import ResultEnricher

    weth_addr = "0x" + "cc" * 20  # tokenX (higher address)
    usdc_addr = "0x" + "11" * 20  # tokenY

    # Pin the overlay itself — the one line that routes legs onto an LP_OPEN.
    spec = ResultEnricher._merge_spec_with_overlay("LP_OPEN", "traderjoe_v2")
    assert "primitive_money_legs" in spec, (
        "traderjoe_v2 LP_OPEN must declare primitive_money_legs in the enricher overlay"
    )

    receipt = _real_tj_deposit_receipt(weth_addr, usdc_addr, WETH_RAW, USDC_RAW)
    result = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        transaction_results=[TransactionResult(tx_hash="0x" + "6a" * 32, success=True, receipt=receipt, gas_used=500)],
        total_gas_used=500,
    )
    intent = SimpleNamespace(intent_type="LP_OPEN", protocol="traderjoe_v2", intent_id="i-1")
    context = ExecutionContext(deployment_id="d1", chain="ethereum", wallet_address=LB_WALLET)

    resolver = _address_resolver({weth_addr.lower(): ("WETH", 18), usdc_addr.lower(): ("USDC", 6)})
    with patch(
        "almanak.connectors.traderjoe_v2.receipt_parser.get_token_resolver",
        return_value=resolver,
    ):
        enriched = ResultEnricher(live_mode=False).enrich(result, intent, context)

    legs = enriched.extracted_data.get("primitive_money_legs")
    assert legs is not None, "production enricher must land primitive_money_legs on an LP_OPEN"
    assert [leg.token for leg in legs.input_legs] == ["WETH", "USDC"]

    # ── the final seam: carry the ENRICHED extracted_data into the producer ──
    # Everything below consumes what production code produced above — the
    # legs AND the amounts (lp_open_data) both come from the real parser via
    # the real enricher. Nothing is hand-assembled.
    lp_open = enriched.extracted_data.get("lp_open_data")
    assert lp_open is not None, "enricher must also land lp_open_data"
    assert (lp_open.amount0, lp_open.amount1) == (WETH_RAW, USDC_RAW), (
        "amounts must be in the same tokenX/tokenY order as the declared legs"
    )

    event = build_position_event_from_intent(
        deployment_id="d1",
        # Label order (USDC, WETH) is the INVERSE of the on-chain leg order.
        intent=_Intent("LP_OPEN", pool="USDC/WETH/25", from_token="USDC", to_token="WETH"),
        result=enriched,
        chain="ethereum",
        price_oracle=_prices(),
    )

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC")
    value = Decimal(event.value_usd)
    assert SANE_LOW < value < SANE_HIGH, f"expected ~$4, got {value}"
    assert value < PHANTOM_FLOOR


# ── N-coin guard ordering ────────────────────────────────────────────────


def test_ncoin_guard_wins_over_declared_legs(_resolver):
    """A Curve-style N-coin open stamps legs too; the coin universe owns the
    ordering, so the declared-legs branch must not fire."""
    lp_open = LPOpenData(
        position_id=0,
        tick_lower=None,
        tick_upper=None,
        liquidity=500_000,
        amount0=USDC_RAW,
        amount1=WETH_RAW,
        pool_address=_addr("ab"),
        coin_symbols=["WETH", "USDC", "DAI"],
    )
    legs = _legs(("USDC", "2.185779"), ("WETH", "0.001032"), role="input")
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_OPEN"),
        result=_Result({"lp_open_data": lp_open, "primitive_money_legs": legs}),
        chain="ethereum",
        price_oracle=_prices(),
    )

    assert event is not None
    # Pool-index order preserved from the label — no leg adoption, no sort.
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC")


def test_ncoin_additional_amounts_guard_blocks_leg_adoption(_resolver):
    """pr-auditor Important #3 — a second, drift-proof N-coin signal.

    Curve declares ONE INPUT leg per FUNDED coin, while `amount0`/`amount1` are
    per POOL INDEX. For a 3-coin pool with coins 0 and 2 funded, the legs are
    [coin0, coin2] but the amounts are [coin0_amt, coin1_amt=0] — so leg index
    does NOT align with amount slot, and adopting the pair would be the exact
    VIB-5983 mis-scaling class.

    `coin_symbols` normally blocks that, but it is resolved from
    `CURVE_TEST_POOLS[...]["coins"]` while the legs come from `coin_addresses`. A
    future pool entry carrying one and not the other silently re-opens a
    nine-figure mis-valuation. `additional_amounts` comes off the receipt
    itself, so it cannot drift from the registry. This pins that second signal
    alone, with `coin_symbols` deliberately ABSENT.
    """
    lp_open = LPOpenData(
        position_id=0,
        tick_lower=None,
        tick_upper=None,
        liquidity=500_000,
        amount0=WETH_RAW,
        amount1=USDC_RAW,
        pool_address=_addr("ab"),
        # 3-coin pool: coin index 2 funded. NOTE coin_symbols is NOT set —
        # this test must pass on the additional_amounts signal alone.
        additional_amounts={2: 1_000_000},
    )
    legs = _legs(("USDC", "2.185779"), ("WETH", "0.001032"), role="input")
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_OPEN", from_token="WETH", to_token="USDC"),
        result=_Result({"lp_open_data": lp_open, "primitive_money_legs": legs}),
        chain="ethereum",
        price_oracle=_prices(),
    )

    assert event is not None
    # Legs say (USDC, WETH); an address sort would also say (USDC, WETH).
    # The guard must block BOTH, leaving the pool-index label order intact.
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC"), (
        "additional_amounts must block declared-leg adoption for N-coin pools"
    )


# ── VIB-6383: the precedence arm between VIB-6053 and VIB-5988 ───────────────
#
# These tests need a resolver double the CURRENCY branch can actually use, which
# the module-level ``_resolver`` fixture is not. ``_resolve_lp_close_symbol``
# calls ``resolver.resolve(cur, chain, log_errors=..., skip_gateway=...)`` with
# ``chain`` POSITIONAL, while ``_resolver``'s double takes a single positional
# arg. The mismatch raises ``TypeError`` inside the helper's broad fail-open
# ``except``, which swallows it and reports ``""`` — i.e. "unresolved".
#
# That is VIB-6100 ("fail-open except in token resolvers turns a test double's
# signature mismatch into a fake 'unresolved'"), and it bites here in the worst
# way: with ``_resolver``, EVERY address reads as unresolvable, so a
# fall-through test would pass no matter what address it was handed and would be
# controlling for nothing. The double below takes the chain positionally so a
# known address genuinely resolves and an unknown one genuinely does not — which
# is what makes the resolved/unresolved pair below discriminating.


@pytest.fixture
def _currency_resolver(monkeypatch):
    def _make():
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        class _Double:
            def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002 — chain is positional
                k = str(token).lower()
                for sym, addr in _ADDR_BOOK.items():
                    if addr.lower() == k or sym == str(token).upper():
                        return SimpleNamespace(symbol=sym, address=addr, decimals=_DECIMALS[sym], chain=chain)
                # Business miss: production raises, never returns None (None is a defect).
                raise TokenNotFoundError(token=str(token), chain=str(chain), reason="unknown")

        return _Double()

    for target in (
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        "almanak.framework.observability.ledger.get_token_resolver",
    ):
        try:
            monkeypatch.setattr(target, _make)
        except AttributeError:
            pass


def _open_event_with_currencies(currency0: str, currency1: str, extracted_extra: dict | None = None):
    """An LP_OPEN whose parser stamped currencies, in tokenX/tokenY slot order."""
    lp_open = LPOpenData(
        position_id=0,
        tick_lower=-60_000,
        tick_upper=60_000,
        liquidity=500_000,
        amount0=USDC_RAW,  # connector order (tokenX)
        amount1=WETH_RAW,  # connector order (tokenY)
        pool_address=_addr("ab"),
        currency0=currency0,
        currency1=currency1,
    )
    extracted = {"lp_open_data": lp_open, **(extracted_extra or {})}
    return build_position_event_from_intent(
        deployment_id="d1",
        # Label order is the INVERSE of the connector's amount order, so any
        # branch that silently keeps the label wins/loses visibly.
        intent=_Intent("LP_OPEN", pool="WETH/USDC/25", from_token="WETH", to_token="USDC"),
        result=_Result(extracted),
        chain="ethereum",
        price_oracle=_prices(),
    )


def test_unresolvable_currencies_fall_through_to_declared_legs(_currency_resolver):
    """VIB-6383 — THE NEGATIVE CONTROL for the currencies/declared-legs precedence.

    This is the test that decides the whole design question, so it is worth being
    explicit about what it is controlling for.

    VIB-6053 made the parser-currency branch outrank everything and be *terminal*:
    on ``("", "")`` — currencies observed but unresolvable — it kept **label**
    order rather than falling through. That was written when currencies existed
    only for Uniswap V4, which declares no money legs, so no connector carried
    both carriers. TraderJoe Liquidity Book is the first that does (VIB-6383
    stamps ``currency0``/``currency1`` on the LB parser), and the collision made
    this arm reachable for the first time.

    The fix falls through to declared legs — and ONLY to declared legs. What is
    controlled for here is that the fall-through actually reaches them rather
    than the label: the intent's pool label is deliberately the INVERSE of the
    connector's amount order, so keeping label order books the ~$1bn phantom and
    adopting the legs books ~$4.

    Regressing the caller back to a terminal ``return`` turns this red. Nothing
    else in the suite covers it — and note that the helper's own three-way
    contract is untouched, so
    ``test_unresolvable_currencies_return_empty_pair_not_none`` in
    ``tests/unit/observability/test_lp_token_identity_seam_vib6053.py`` stays
    green alongside this. Both must hold.
    """
    # Addresses the fake resolver knows nothing about -> `("", "")`.
    event = _open_event_with_currencies(
        _addr("de"),
        _addr("ad"),
        {"primitive_money_legs": _tj_open_legs()},
    )

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("USDC", "WETH"), (
        "unresolvable currencies must fall through to the declared legs "
        f"(tokenX=USDC, tokenY=WETH), got {event.token0}/{event.token1}"
    )
    value = Decimal(event.value_usd)
    assert SANE_LOW < value < SANE_HIGH, f"expected ~$4, got {value}"
    assert value < PHANTOM_FLOOR


def test_unresolvable_currencies_without_legs_keep_label_order_never_address_sort(
    _currency_resolver, caplog
):
    """VIB-6383 — the guard: an address sort is not a valid stand-in for a failed
    observation.

    With no declared legs to fall through to, the caller must keep label order and
    must NOT reach ``realign_token_pair_by_address``. The label here is
    ``WETH/USDC`` while the address sort would produce ``USDC/WETH`` (USDC
    ``0x11…`` < WETH ``0xcc…``), so the two outcomes are distinguishable and this
    cannot pass by coincidence.
    """
    import logging

    with caplog.at_level(logging.WARNING):
        event = _open_event_with_currencies(_addr("de"), _addr("ad"))

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("WETH", "USDC"), (
        "must keep label order, not derive one from an address sort; got "
        f"{event.token0}/{event.token1}"
    )
    assert any("VIB-6383" in r.message for r in caplog.records), (
        "the degraded path must be observable, not silent"
    )


def test_resolvable_currencies_still_outrank_declared_legs(_currency_resolver):
    """MUST-NOT-CHANGE — VIB-6053's actual rule is untouched.

    When the currencies DO resolve they still win outright, even with declared
    legs present. Pairing this with the fall-through test above is what makes the
    change falsifiable: a "fix" that simply demoted currencies beneath declared
    legs would pass the first test and fail this one.

    The legs here are deliberately built in the WRONG order (WETH, USDC) so that
    adopting them instead of the currencies is visible.
    """
    event = _open_event_with_currencies(
        _ADDR_BOOK["USDC"],
        _ADDR_BOOK["WETH"],
        {"primitive_money_legs": _legs(("WETH", "0.001032"), ("USDC", "2.185779"), role="input")},
    )

    assert event is not None
    assert (event.token0.upper(), event.token1.upper()) == ("USDC", "WETH"), (
        "resolved parser currencies must outrank declared legs (VIB-6053)"
    )
