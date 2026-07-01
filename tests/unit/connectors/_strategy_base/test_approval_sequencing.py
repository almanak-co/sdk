"""Unit tests for the shared ERC-20 approval-sequencing primitive (VIB-5492).

``build_approval_sequence`` is the single source of truth for the money-critical
approve ordering shared by the framework compiler and connector adapters. These
tests pin every branch of the pure decision: skip-if-confirmed-sufficient,
non-USDT direct approve, USDT-class reset-first, and the fail-safe on an
UNCONFIRMED (``None``) allowance.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.base.approval_sequencing import build_approval_sequence

MAX = 2**256 - 1


def _reset() -> tuple[str, int]:
    return ("reset", 0)


def _approve(value: int) -> tuple[str, int]:
    return ("approve", value)


def _seq(**kwargs) -> list[tuple[str, int]]:
    return build_approval_sequence(
        build_reset_tx=_reset,
        build_approve_tx=_approve,
        **kwargs,
    )


class TestSkip:
    def test_confirmed_sufficient_allowance_skips(self) -> None:
        assert _seq(amount=1000, current_allowance=1000, reset_before_change=True, approval_amount=MAX) == []

    def test_confirmed_greater_allowance_skips(self) -> None:
        assert _seq(amount=1000, current_allowance=5000, reset_before_change=False, approval_amount=MAX) == []

    def test_unconfirmed_allowance_never_skips(self) -> None:
        """None (unconfirmed) allowance must NOT short-circuit the approve, even if
        a cache might have suggested it was covered — never skip on unknown alone."""
        txs = _seq(amount=1000, current_allowance=None, reset_before_change=False, approval_amount=MAX)
        assert txs == [_approve(MAX)]


class TestNonPositiveAmount:
    """A non-positive spend needs no approval — never emit a reset or approve,
    even on the reset-requiring + unconfirmed-allowance path where the
    sufficiency check cannot short-circuit (``current_allowance is None``)."""

    def test_zero_amount_emits_nothing_even_reset_token_unknown_allowance(self) -> None:
        assert _seq(amount=0, current_allowance=None, reset_before_change=True, approval_amount=MAX) == []

    def test_zero_amount_emits_nothing_nonzero_allowance(self) -> None:
        assert _seq(amount=0, current_allowance=500, reset_before_change=True, approval_amount=MAX) == []

    def test_negative_amount_emits_nothing(self) -> None:
        assert _seq(amount=-1, current_allowance=None, reset_before_change=True, approval_amount=MAX) == []


class TestNonResetToken:
    def test_zero_allowance_single_approve(self) -> None:
        txs = _seq(amount=1000, current_allowance=0, reset_before_change=False, approval_amount=MAX)
        assert txs == [_approve(MAX)]

    def test_nonzero_insufficient_no_reset_when_token_tolerates(self) -> None:
        """A non-reset token with an existing non-zero allowance is re-approved
        directly (no reset) — it does not revert on non-zero -> non-zero."""
        txs = _seq(amount=1000, current_allowance=500, reset_before_change=False, approval_amount=MAX)
        assert txs == [_approve(MAX)]

    def test_unknown_allowance_no_reset_when_token_tolerates(self) -> None:
        txs = _seq(amount=1000, current_allowance=None, reset_before_change=False, approval_amount=MAX)
        assert txs == [_approve(MAX)]


class TestResetToken:
    def test_confirmed_zero_is_single_approve(self) -> None:
        """A POSITIVELY confirmed zero allowance needs no reset even on a USDT-class
        token — the common fresh-token path, and the zero-vs-unknown distinction
        the fail-safe hinges on."""
        txs = _seq(amount=1000, current_allowance=0, reset_before_change=True, approval_amount=MAX)
        assert txs == [_approve(MAX)]

    def test_nonzero_resets_first(self) -> None:
        txs = _seq(amount=1000, current_allowance=500, reset_before_change=True, approval_amount=MAX)
        assert txs == [_reset(), _approve(MAX)]

    def test_unknown_allowance_fails_safe_to_reset(self) -> None:
        """An UNCONFIRMED allowance on a reset-requiring token fails toward
        reset+approve — approve(0) never reverts, a lone approve(MAX) might."""
        txs = _seq(amount=1000, current_allowance=None, reset_before_change=True, approval_amount=MAX)
        assert txs == [_reset(), _approve(MAX)]

    def test_reset_is_ordered_first(self) -> None:
        """Reset precedes approve so a mid-bundle failure strands allowance=0
        (re-approvable), never a stale non-zero value."""
        txs = _seq(amount=1000, current_allowance=1, reset_before_change=True, approval_amount=MAX)
        assert [t[0] for t in txs] == ["reset", "approve"]


class TestApprovalAmountIsCallerPosture:
    def test_buffered_amount_passed_through(self) -> None:
        """The primitive approves exactly the caller-supplied value — MAX or a
        buffered amount — it does not impose a posture."""
        txs = _seq(amount=1000, current_allowance=0, reset_before_change=False, approval_amount=1100)
        assert txs == [_approve(1100)]
