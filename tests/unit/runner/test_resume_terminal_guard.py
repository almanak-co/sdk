"""Unit tests for the resume-into-terminal-state boot guard (VIB-5887).

Covers the three contract cases from the ticket:

(1) resume into a TERMINAL lifecycle state + fresh (non-trivial) wallet capital
    -> guard FIRES (loud WARNING + RESUMED-TERMINAL structured sentinel).
(2) normal mid-lifecycle resume (state restored but NOT terminal)
    -> guard does NOT fire (crash-recovery resume must stay silent). The balance
    is not even read.
(3) fresh (non-resume) boot -> guard does NOT fire (gated on is_resume).

Plus the adversarial-review hardening:
  * the native GAS RESERVE is excluded but native capital ABOVE the reserve still
    counts (gas-only wallet -> no fire; native >> reserve -> fires);
  * the check is explicitly gated on ``is_resume``;
  * ``_coerce_decimal`` rejects non-finite (NaN / Infinity) and str()-raising inputs.

Both the terminal signal (``is_lifecycle_complete``) and the balance
(``get_portfolio_snapshot().wallet_balances``, native gas excluded via the chain
registry) are faked here so the test is pure (no chain, no DB).
"""

from __future__ import annotations

import logging
from decimal import Decimal

from almanak.framework.runner.resume_terminal_guard import (
    _MATERIAL_IDLE_USD,
    ResumeTerminalReport,
    _coerce_decimal,
    detect_resume_into_terminal,
    warn_on_resume_into_terminal,
)

# On Avalanche the native gas token is AVAX (ChainRegistry). A wallet balance with
# this symbol is priced against the gas reserve; only value above it counts.
_NATIVE = "AVAX"


class _Bal:
    def __init__(self, symbol, value_usd, *, balance=None, price_usd=None):
        self.symbol = symbol
        self.value_usd = value_usd
        self.balance = balance
        self.price_usd = price_usd


class _Snap:
    def __init__(self, wallet_balances):
        self.wallet_balances = wallet_balances


class _FakeStrategy:
    """Minimal strategy exposing the terminal hook + a portfolio snapshot."""

    def __init__(
        self,
        *,
        terminal=None,
        raise_hook=False,
        chain="avalanche",
        wallet_balances=None,
        raise_snapshot=False,
        no_snapshot=False,
        gas_reserve=None,
    ):
        # ``terminal=None`` models a strategy that does NOT implement the hook.
        self._terminal = terminal
        self._raise_hook = raise_hook
        self.chain = chain
        self._wallet_balances = wallet_balances if wallet_balances is not None else []
        self._raise_snapshot = raise_snapshot
        self.snapshot_calls = 0
        if gas_reserve is not None:
            self.gas_reserve = gas_reserve
        if terminal is not None or raise_hook:
            self.is_lifecycle_complete = self._hook  # type: ignore[assignment]
        if not no_snapshot:
            self.get_portfolio_snapshot = self._snapshot  # type: ignore[assignment]

    def _hook(self) -> bool:
        if self._raise_hook:
            raise RuntimeError("boom")
        return bool(self._terminal)

    def _snapshot(self):
        self.snapshot_calls += 1
        if self._raise_snapshot:
            raise RuntimeError("valuation exploded")
        return _Snap(self._wallet_balances)


# ``runner`` is threaded through for signature symmetry with the boot-strand guard
# but is not consulted by the balance read; a bare object suffices.
_RUNNER = object()


# ---------------------------------------------------------------------------
# (1) resume-into-terminal + fresh capital -> guard FIRES
# ---------------------------------------------------------------------------


def test_resume_into_terminal_with_fresh_capital_fires(caplog):
    strategy = _FakeStrategy(
        terminal=True,
        gas_reserve=Decimal("0.05"),
        wallet_balances=[
            _Bal("USDC", Decimal("2.5")),
            _Bal(_NATIVE, Decimal("1.5"), balance=Decimal("0.05"), price_usd=Decimal("30")),
        ],
    )

    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report == ResumeTerminalReport(
        is_terminal=True,
        idle_capital_usd=Decimal("2.5"),  # AVAX == gas reserve -> excluded
        threshold_usd=_MATERIAL_IDLE_USD,
        fired=True,
    )

    with caplog.at_level(logging.WARNING):
        enforced = warn_on_resume_into_terminal(_RUNNER, strategy, "deployment:abc123", is_resume=True)

    assert enforced.fired is True
    text = caplog.text
    assert "RESUMED-TERMINAL" in text
    assert "2.5" in text  # idle capital named
    assert "ALMANAK_RESUME_TERMINAL deployment_id=deployment:abc123" in text
    assert "idle_capital_usd=2.5" in text


# ---------------------------------------------------------------------------
# (2) normal mid-lifecycle resume -> guard does NOT fire (no regression)
# ---------------------------------------------------------------------------


def test_mid_lifecycle_resume_does_not_fire_and_skips_balance_read(caplog):
    # Plenty of idle cash, but the strategy is NOT terminal (e.g. state="borrowed").
    strategy = _FakeStrategy(terminal=False, wallet_balances=[_Bal("USDC", Decimal("1000"))])

    with caplog.at_level(logging.WARNING):
        report = warn_on_resume_into_terminal(_RUNNER, strategy, "deployment:mid", is_resume=True)

    assert report.fired is False
    assert report.is_terminal is False
    # Balance is only read when terminal — a mid-lifecycle resume short-circuits.
    assert strategy.snapshot_calls == 0
    assert "RESUMED-TERMINAL" not in caplog.text


# ---------------------------------------------------------------------------
# (3) fresh (non-resume) boot -> gated out
# ---------------------------------------------------------------------------


def test_fresh_boot_is_gated_out_even_if_terminal(caplog):
    # is_resume=False: even a (spuriously) terminal strategy with capital must not
    # fire on a fresh, first-ever boot. Balance is not read.
    strategy = _FakeStrategy(terminal=True, wallet_balances=[_Bal("USDC", Decimal("500"))])

    with caplog.at_level(logging.WARNING):
        report = warn_on_resume_into_terminal(_RUNNER, strategy, "deployment:fresh", is_resume=False)

    assert report.fired is False
    assert report.is_terminal is False
    assert strategy.snapshot_calls == 0
    assert "RESUMED-TERMINAL" not in caplog.text


def test_fresh_boot_without_hook_does_not_fire():
    strategy = _FakeStrategy(terminal=None, wallet_balances=[_Bal("USDC", Decimal("500"))])
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.fired is False
    assert report.is_terminal is False
    assert strategy.snapshot_calls == 0


# ---------------------------------------------------------------------------
# Native gas reserve: exclude the reserve, count native ABOVE it
# ---------------------------------------------------------------------------


def test_declared_gas_reserve_only_does_not_fire(caplog):
    # ONLY balance is exactly the declared gas reserve (0.05 AVAX @ $30 = $1.5).
    strategy = _FakeStrategy(
        terminal=True,
        gas_reserve=Decimal("0.05"),
        wallet_balances=[_Bal(_NATIVE, Decimal("1.5"), balance=Decimal("0.05"), price_usd=Decimal("30"))],
    )

    with caplog.at_level(logging.WARNING):
        report = warn_on_resume_into_terminal(_RUNNER, strategy, "deployment:gasonly", is_resume=True)

    assert report.is_terminal is True
    assert report.idle_capital_usd == Decimal("0")  # reserve fully excluded
    assert report.fired is False
    assert strategy.snapshot_calls == 1  # balance WAS read (resumed + terminal)
    assert "RESUMED-TERMINAL" not in caplog.text


def test_native_capital_above_declared_reserve_fires():
    # 1.0 AVAX @ $30 = $30 wallet, reserve 0.05 AVAX = $1.5 -> $28.5 idle counts.
    strategy = _FakeStrategy(
        terminal=True,
        gas_reserve=Decimal("0.05"),
        wallet_balances=[_Bal(_NATIVE, Decimal("30"), balance=Decimal("1.0"), price_usd=Decimal("30"))],
    )
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.idle_capital_usd == Decimal("28.5")
    assert report.fired is True


def test_native_reserve_priced_from_value_when_no_explicit_price():
    # No price_usd on the row: reserve priced from value/balance (0.05/1.0 * $30 = $1.5).
    strategy = _FakeStrategy(
        terminal=True,
        gas_reserve=Decimal("0.05"),
        wallet_balances=[_Bal(_NATIVE, Decimal("30"), balance=Decimal("1.0"))],
    )
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.idle_capital_usd == Decimal("28.5")
    assert report.fired is True


def test_undeclared_reserve_uses_default_buffer():
    # No declared gas_reserve: a small native balance below the default buffer is
    # treated as gas and does not fire; a large one fires above the buffer.
    gas_only = _FakeStrategy(terminal=True, wallet_balances=[_Bal(_NATIVE, Decimal("1.75"))])
    assert detect_resume_into_terminal(_RUNNER, gas_only, is_resume=True).fired is False

    real = _FakeStrategy(terminal=True, wallet_balances=[_Bal(_NATIVE, Decimal("30"))])
    got = detect_resume_into_terminal(_RUNNER, real, is_resume=True)
    assert got.idle_capital_usd == Decimal("25")  # 30 - default $5 buffer
    assert got.fired is True


# ---------------------------------------------------------------------------
# Edge cases — conservative "no false alarm" behaviour
# ---------------------------------------------------------------------------


def test_terminal_but_trivial_capital_does_not_fire():
    strategy = _FakeStrategy(terminal=True, wallet_balances=[_Bal("USDC", Decimal("0.10"))])
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.is_terminal is True
    assert report.fired is False


def test_capital_exactly_at_threshold_fires():
    strategy = _FakeStrategy(terminal=True, wallet_balances=[_Bal("USDC", _MATERIAL_IDLE_USD)])
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.fired is True


def test_raising_hook_treated_as_not_terminal():
    strategy = _FakeStrategy(raise_hook=True, wallet_balances=[_Bal("USDC", Decimal("1000"))])
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.is_terminal is False
    assert report.fired is False
    assert strategy.snapshot_calls == 0


def test_unreadable_balance_degrades_to_no_warning():
    strategy = _FakeStrategy(terminal=True, raise_snapshot=True)
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.is_terminal is True
    assert report.idle_capital_usd == Decimal("0")
    assert report.fired is False


def test_nan_value_usd_does_not_poison_detection():
    # A NaN wallet value must not skip detection (NaN >= threshold is False) — coerced
    # to 0, alongside a real $2.5 balance that still fires.
    strategy = _FakeStrategy(
        terminal=True,
        wallet_balances=[_Bal("WEIRD", Decimal("NaN")), _Bal("USDC", Decimal("2.5"))],
    )
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.idle_capital_usd == Decimal("2.5")
    assert report.fired is True


def test_missing_snapshot_method_degrades_to_zero():
    strategy = _FakeStrategy(terminal=True, no_snapshot=True)
    report = detect_resume_into_terminal(_RUNNER, strategy, is_resume=True)
    assert report.idle_capital_usd == Decimal("0")
    assert report.fired is False


# ---------------------------------------------------------------------------
# _coerce_decimal — VIB-4062 no-bifurcation + finite-safety
# ---------------------------------------------------------------------------


def test_coerce_decimal_handles_decimal_str_float_none_and_junk():
    assert _coerce_decimal(Decimal("2.5")) == Decimal("2.5")
    assert _coerce_decimal("2.5") == Decimal("2.5")
    assert _coerce_decimal(2.5) == Decimal("2.5")
    assert _coerce_decimal(None) == Decimal("0")
    assert _coerce_decimal("not-a-number") == Decimal("0")


def test_coerce_decimal_rejects_non_finite():
    assert _coerce_decimal(Decimal("NaN")) == Decimal("0")
    assert _coerce_decimal(Decimal("Infinity")) == Decimal("0")
    assert _coerce_decimal(Decimal("-Infinity")) == Decimal("0")
    assert _coerce_decimal("nan") == Decimal("0")
    assert _coerce_decimal("inf") == Decimal("0")
    assert _coerce_decimal(float("nan")) == Decimal("0")
    assert _coerce_decimal(float("inf")) == Decimal("0")


def test_coerce_decimal_survives_str_that_raises():
    class _Boom:
        def __str__(self):
            raise RuntimeError("no str for you")

    assert _coerce_decimal(_Boom()) == Decimal("0")


# ---------------------------------------------------------------------------
# Strategy hook: default + subclass override flow through the detector
# ---------------------------------------------------------------------------


def test_intent_strategy_default_hook_is_false():
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    # Default implementation is conservative: never terminal.
    assert IntentStrategy.is_lifecycle_complete(object()) is False  # type: ignore[arg-type]


def test_benqi_demo_hooks_report_complete_state():
    # The two demos named in VIB-5887 override the hook off their private state.
    from almanak.demo_strategies.benqi_lending_lifecycle.strategy import (
        BenqiLendingLifecycleStrategy,
    )
    from almanak.demo_strategies.benqi_looping.strategy import BenqiLoopingStrategy

    lending = BenqiLendingLifecycleStrategy.__new__(BenqiLendingLifecycleStrategy)
    lending._loop_state = "complete"
    assert lending.is_lifecycle_complete() is True
    lending._loop_state = "borrowed"
    assert lending.is_lifecycle_complete() is False

    looping = BenqiLoopingStrategy.__new__(BenqiLoopingStrategy)
    looping._state = "complete"
    assert looping.is_lifecycle_complete() is True
    looping._state = "levered"  # holding a built position is NOT terminal
    assert looping.is_lifecycle_complete() is False
