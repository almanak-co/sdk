"""VIB-6483: the mined-revert diagnostic must not silently discard operands.

``_ReplayProbe.describe()`` bounded the revert payload at ``raw[:74]`` — ``0x`` +
a 4-byte selector + exactly ONE 32-byte word. Every custom error carrying more
than one argument was cut to its first operand, with no ellipsis and no length,
so a truncated payload was indistinguishable from a complete one.

That is not a cosmetic cut. ``decode_revert_data`` renders a custom error as its
selector alone (``custom error 0xd3dacaac``), so ``raw`` is the ONLY channel
through which an operand ever reaches the operator. On this diagnostic's first
real-fork use (R12, ``docs/internal/gmx-readiness/r12-20260803/``) it surfaced
``InsufficientGasForCancellation(uint256,uint256)`` carrying the measured gas and
NOT the threshold it was compared against — the operand that explains the
failure behind the Urgent VIB-6437.

NEGATIVE CONTROL for the whole file: restore
``f"{detail} (raw={self.raw[:74]})"`` in ``_ReplayProbe.describe()``.
``test_a_two_argument_custom_error_keeps_both_operands`` and
``test_the_r12_payload_survives_end_to_end_through_the_raised_error`` fail on the
missing second operand; ``test_an_oversized_payload_is_cut_visibly_not_silently``
fails on the absent truncation marker.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from almanak.connectors.gmx_v2.anvil_order_executor import (
    _RAW_HEX_CAP,
    _replay_once,
    _replay_revert_reason,
    _ReplayProbe,
)

# ``InsufficientGasForCancellation(uint256,uint256)`` — the exact selector R12
# captured on a real Arbitrum fork, with the exact ``arg0`` it carried.
_SELECTOR = "d3dacaac"
_ARG0 = 845_611
_ARG1 = 1_000_000


def _custom_error(selector: str, *args: int) -> str:
    """A real ABI-encoded custom-error payload: selector + one word per operand."""
    return "0x" + selector + "".join(format(arg, "064x") for arg in args)


def _revert_response(payload: str) -> dict:
    """The JSON-RPC error shape an executed revert with data produces."""
    return {"error": {"code": 3, "message": "execution reverted", "data": payload}}


def test_a_two_argument_custom_error_keeps_both_operands() -> None:
    """The defect, at the unit it lives in.

    Both operands are asserted by their FULL 64-hex-character words, not by a
    substring: ``format(845611, "064x")`` and ``format(1000000, "064x")`` share a
    long run of zeros, so a lazy ``"ce72b" in message`` check would pass on a
    payload whose second word had been cut.
    """
    payload = _custom_error(_SELECTOR, _ARG0, _ARG1)
    message = _ReplayProbe(outcome="revert", reason=f"custom error 0x{_SELECTOR}", raw=payload).describe()

    assert payload in message, "the complete payload must reach the operator"
    assert format(_ARG0, "064x") in message
    assert format(_ARG1, "064x") in message, "arg1 is the operand VIB-6483 discarded"
    # Nothing was cut, so nothing may claim a cut.
    assert "truncated" not in message
    assert "…" not in message


def test_a_single_argument_custom_error_is_unchanged() -> None:
    """The one-operand case the old bound happened to fit must not move.

    ``0x`` + 8 selector characters + one 64-character word is exactly 74 — the
    old slice boundary — so this is the payload for which old and new behaviour
    must be byte-identical.
    """
    payload = _custom_error("ee07a123", _ARG0)
    assert len(payload) == 74

    message = _ReplayProbe(outcome="revert", reason="custom error 0xee07a123", raw=payload).describe()

    assert message == f"custom error 0xee07a123 (raw={payload})"
    assert message == f"custom error 0xee07a123 (raw={payload[:74]})", "identical to the pre-fix rendering"


def test_an_oversized_payload_is_cut_visibly_not_silently() -> None:
    """A cap may stay, but a cut must announce itself and its true size.

    The visibility is the actual lesson of VIB-6483: a silent cut is worse than a
    loud one because the output LOOKS complete.
    """
    words = (_RAW_HEX_CAP - 8) // 64 + 4  # four words past the cap
    payload = _custom_error(_SELECTOR, *range(1, words + 1))
    payload_bytes = (len(payload) - 2) // 2

    message = _ReplayProbe(outcome="revert", reason=f"custom error 0x{_SELECTOR}", raw=payload).describe()

    assert "…" in message, "a cut payload must be marked as cut"
    assert f"{payload_bytes} bytes total" in message, "and must state how much really existed"
    assert payload not in message, "the fixture must actually exceed the cap or this proves nothing"
    # The cut lands on a 32-byte-word boundary: what is shown stays decodable.
    #
    # Known limitation (VIB-6486): this pins the cut's SHAPE, not that ``shown``
    # is the payload's own prefix — a formatter emitting unrelated ABI words of
    # the same length would pass here. Nothing else backstops it: this is the
    # only test that reaches the TRUNCATING branch, because the operand tests
    # use under-cap payloads and exercise the early ``return raw``. So a slicing
    # defect confined to that branch passes the whole suite. One line closes it:
    # ``assert payload.startswith("0x" + shown)``.
    shown = message.split("raw=0x", 1)[1].split("…", 1)[0]
    assert len(shown) == _RAW_HEX_CAP
    assert (len(shown) - 8) % 64 == 0


def test_the_r12_payload_survives_end_to_end_through_the_raised_error() -> None:
    """Reachability: the operands must survive the real production path.

    A unit assertion on ``describe()`` alone would not prove the operator ever
    sees them — ``describe()`` has exactly one caller, and this is it.
    """
    payload = _custom_error(_SELECTOR, _ARG0, _ARG1)
    provider = MagicMock()
    provider.make_request.return_value = _revert_response(payload)

    # The probe classifies it as a measured revert carrying the whole payload...
    probe = _replay_once(provider, {"from": "0x" + "11" * 20}, "0x1")
    assert probe.outcome == "revert"
    assert probe.raw == payload

    # ...and the assembled diagnosis carries both operands out to the caller.
    message = _replay_revert_reason(provider, {"from": "0x" + "11" * 20, "gas": "0x3e17de"}, 4_242)

    assert f"custom error 0x{_SELECTOR}" in message
    assert format(_ARG0, "064x") in message
    assert format(_ARG1, "064x") in message
