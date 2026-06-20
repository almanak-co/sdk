"""Pendle PT position-events lifecycle (VIB-52xx, G-PT3).

PT buy/sell arrive as ``SWAP`` and redeem as ``WITHDRAW`` — neither flows
through the static ``INTENT_TO_EVENT_TYPE`` map correctly. These tests pin the
protocol-aware ``_seed_event`` interception: a PT buy seeds a ``PENDLE_PT``
OPEN, a sell/redeem seeds a CLOSE on the *same* position_id, and every
non-Pendle-PT intent is left to the generic path unchanged.

The position_id is derived from the **normalized PT symbol** — NOT a
pool/market address. A Pendle ``SwapIntent`` carries no pool, and the resolved
market address is never persisted on the ledger row, so the symbol (present on
both the intent here and the ledger row the accounting treatment reads) is the
only identifier that lets position_events ↔ accounting_events join.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.primitive_money_leg import (
    PrimitiveMoneyLeg,
    PrimitiveMoneyLegs,
)
from almanak.framework.accounting.measured import MeasuredMoney
from almanak.framework.observability import position_events as pe

_PT = "PT-wstETH-25JUN2026"
_WALLET = "0xWALLETabcDEF0000000000000000000000000000"


@dataclass
class _StubSwapIntent:
    intent_type: str = "SWAP"
    protocol: str = "pendle"
    from_token: str = ""
    to_token: str = ""
    pool: str = ""  # Pendle SwapIntents carry no pool — identity must not need it
    position_id: str = ""

    @property
    def value(self) -> str:  # parity with IntentType.value access
        return self.intent_type


def _redeem_legs(pt_symbol: str, out_token: str = "WSTETH") -> PrimitiveMoneyLegs:
    """A redeem's connector-declared money legs: INPUT=PT, OUTPUT=underlying.

    Mirrors what ``PendleReceiptParser.extract_primitive_money_legs`` lands on
    ``result.extracted_data["primitive_money_legs"]`` for a PT redeem (G-PT /
    VIB-4988 part 2) — the only place the canonical PT symbol exists on a redeem
    (the WithdrawIntent carries the underlying out token + YT address, not the PT).
    """
    return PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input(pt_symbol, MeasuredMoney.measured(Decimal("1"))),
        PrimitiveMoneyLeg.output(out_token, MeasuredMoney.measured(Decimal("1"))),
    )


def _seed(intent: Any, *, extracted: dict[str, Any] | None = None) -> Any:
    ctx = pe.IntentEventContext(
        intent=intent,
        result=None,
        extracted=extracted or {},
        deployment_id="test",
        chain="arbitrum",
        ledger_entry_id="le-1",
        wallet_address=_WALLET,
    )
    return pe._seed_event(ctx)


def _expected_pid() -> str:
    # pendle_pt:<chain>:<wallet>:<normalized-pt-symbol>
    return f"pendle_pt:arbitrum:{_WALLET.lower()}:{_PT.lower()}"


def test_pt_buy_seeds_open() -> None:
    ev = _seed(_StubSwapIntent(to_token=_PT))
    assert ev is not None
    assert ev.event_type == pe.PositionEventType.OPEN.value
    assert ev.position_type == pe.PositionType.PENDLE_PT.value
    assert ev.position_id == _expected_pid()


def test_pt_sell_seeds_close_same_position_id() -> None:
    ev = _seed(_StubSwapIntent(from_token=_PT, to_token="WSTETH"))
    assert ev is not None
    assert ev.event_type == pe.PositionEventType.CLOSE.value
    assert ev.position_type == pe.PositionType.PENDLE_PT.value
    assert ev.position_id == _expected_pid()


def test_pt_redeem_withdraw_seeds_close_not_lending() -> None:
    """A real Pendle redeem arrives as WITHDRAW carrying the underlying out token
    (and YT address) — NOT the PT — on the intent. The canonical PT symbol comes
    from the connector-declared INPUT money leg, and the event must seed a CLOSE
    on the symbol-derived id (NOT mis-resolve to a lending leg, the bug the
    interception closes; NOT land an empty position_id, the bug part 2 closes)."""
    intent = _StubSwapIntent(intent_type="WITHDRAW", from_token="WSTETH", to_token="")
    ev = _seed(intent, extracted={"primitive_money_legs": _redeem_legs(_PT)})
    assert ev is not None
    assert ev.event_type == pe.PositionEventType.CLOSE.value
    assert ev.position_type == pe.PositionType.PENDLE_PT.value
    assert ev.position_id == _expected_pid()


def test_buy_and_redeem_share_position_id() -> None:
    """OPEN (buy) and CLOSE (redeem) collapse onto one renderable position — the
    redeem's symbol sourced from the declared INPUT leg, not the intent."""
    buy = _seed(_StubSwapIntent(to_token=_PT))
    redeem = _seed(
        _StubSwapIntent(intent_type="WITHDRAW", from_token="WSTETH"),
        extracted={"primitive_money_legs": _redeem_legs(_PT)},
    )
    assert buy.position_id == redeem.position_id == _expected_pid()


def test_non_pt_pendle_withdraw_not_a_pt_event() -> None:
    """A non-PT Pendle withdraw (e.g. a YT redeem) has no PT- INPUT leg, so the
    PT interception must DECLINE it (``_pendle_pt_event`` → None) — NOT seed a
    bogus ``pendle_pt:...:<underlying>`` id (the part-2 guard). The withdraw then
    falls to the generic path, which is NOT a PENDLE_PT event."""
    # The interception helper declines: no declared legs → no resolvable PT.
    assert (
        pe._pendle_pt_event(
            _StubSwapIntent(intent_type="WITHDRAW", from_token="USDC"),
            "WITHDRAW",
            "arbitrum",
            _WALLET,
            redeem_pt_symbol="",
        )
        is None
    )
    # Declared legs whose INPUT is not a PT- token → still declined.
    yt_legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input("YT-wstETH-25JUN2026", MeasuredMoney.measured(Decimal("1"))),
        PrimitiveMoneyLeg.output("WSTETH", MeasuredMoney.measured(Decimal("1"))),
    )
    assert pe._redeem_pt_symbol_from_legs({"primitive_money_legs": yt_legs}) == ""
    # End-to-end through _seed_event: a non-PT Pendle withdraw is not booked as a
    # PENDLE_PT position (it falls to the generic lending-collateral path).
    ev = _seed(_StubSwapIntent(intent_type="WITHDRAW", from_token="USDC"))
    assert ev is None or ev.position_type != pe.PositionType.PENDLE_PT.value


def test_buy_and_sell_share_position_id() -> None:
    """OPEN (buy) and CLOSE (sell) collapse onto one renderable position — the
    round-trip the realized-yield FIFO match depends on."""
    buy = _seed(_StubSwapIntent(to_token=_PT))
    sell = _seed(_StubSwapIntent(from_token=_PT, to_token="WSTETH"))
    assert buy.position_id == sell.position_id == _expected_pid()


def test_position_id_independent_of_pool() -> None:
    """Identity is symbol-derived: a garbage/absent pool must NOT change it.

    This pins the structural fix — the old design read ``intent.pool``, which is
    always empty on a Pendle SwapIntent, so the position_id was always empty.
    """
    no_pool = _seed(_StubSwapIntent(to_token=_PT, pool=""))
    junk_pool = _seed(_StubSwapIntent(to_token=_PT, pool="TOKEN/0xdeadbeef"))
    assert no_pool.position_id == junk_pool.position_id == _expected_pid()


def test_symbol_key_is_case_insensitive() -> None:
    """Mixed-case PT symbols collapse to the same key (lowercased)."""
    upper = _seed(_StubSwapIntent(to_token=_PT.upper()))
    lower = _seed(_StubSwapIntent(to_token=_PT.lower()))
    assert upper.position_id == lower.position_id == _expected_pid()


def test_non_pt_pendle_swap_returns_none() -> None:
    """A YT / SY ↔ underlying Pendle swap is not a PT position action."""
    assert _seed(_StubSwapIntent(from_token="USDC", to_token="SY-wstETH")) is None


def test_non_pendle_swap_unchanged_returns_none() -> None:
    """A generic (non-Pendle) swap still produces no position event."""
    assert _seed(_StubSwapIntent(protocol="uniswap_v3", to_token="PT-lookalike")) is None


def test_pendle_pt_event_helper_empty_symbol_degrades() -> None:
    """No PT symbol on either leg → empty position_id (degrade, do not
    fabricate). A Pendle SWAP with neither leg a PT is simply not a PT action,
    so the helper returns None."""
    result = pe._pendle_pt_event(
        _StubSwapIntent(from_token="WSTETH", to_token="USDC"), "SWAP", "arbitrum", _WALLET
    )
    assert result is None
