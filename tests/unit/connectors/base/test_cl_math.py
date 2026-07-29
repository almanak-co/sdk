"""Unit tests for the shared concentrated-liquidity math helpers.

Focus: ``compute_lp_slippage_mins`` slippage-source precedence. The regression
guarded here is the ``Decimal("0")`` falsy trap — an intent that explicitly
requests zero LP slippage (fail-closed, min == desired) must NOT silently fall
back to the wider connector default.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.base.cl_math import compute_lp_slippage_mins
from almanak.framework.intents.min_out_guard import UnprotectedTradeError

_DESIRED0 = 1_000_000
_DESIRED1 = 2_000_000
_DEFAULT = Decimal("0.05")


def _mins(intent: SimpleNamespace) -> tuple[int, int]:
    return compute_lp_slippage_mins(
        intent=intent,
        amount0_desired=_DESIRED0,
        amount1_desired=_DESIRED1,
        default_lp_slippage=_DEFAULT,
    )


class TestComputeLpSlippageMins:
    def test_explicit_zero_max_slippage_is_preserved(self) -> None:
        """max_slippage=0 means fail-closed (min == desired), not 'use default'."""
        intent = SimpleNamespace(protocol_params=None, max_slippage=Decimal("0"))
        assert _mins(intent) == (_DESIRED0, _DESIRED1)

    def test_none_max_slippage_falls_back_to_default(self) -> None:
        intent = SimpleNamespace(protocol_params=None, max_slippage=None)
        assert _mins(intent) == (950_000, 1_900_000)

    def test_max_slippage_used_when_set(self) -> None:
        intent = SimpleNamespace(protocol_params=None, max_slippage=Decimal("0.01"))
        assert _mins(intent) == (990_000, 1_980_000)

    def test_protocol_params_lp_slippage_takes_precedence(self) -> None:
        intent = SimpleNamespace(
            protocol_params={"lp_slippage": 0.02}, max_slippage=Decimal("0.01")
        )
        assert _mins(intent) == (980_000, 1_960_000)

    def test_protocol_params_zero_lp_slippage_preserved(self) -> None:
        intent = SimpleNamespace(
            protocol_params={"lp_slippage": 0}, max_slippage=Decimal("0.05")
        )
        assert _mins(intent) == (_DESIRED0, _DESIRED1)

    def test_protocol_params_lp_slippage_out_of_range_fails_closed(self) -> None:
        """VIB-6217: an out-of-range lp_slippage RAISES; it is no longer clamped.

        This test previously asserted the clamp: ``lp_slippage: 5`` became exactly
        1 and produced ``(0, 0)`` — i.e. a fat-fingered tolerance silently became
        ``amount0Min = amount1Min = 0``, a mint that accepts any outcome. That is
        maximum harm from a typo with no signal, so the assertion was protecting
        the defect. The 5 case is the important one: it proves the failure is
        driven by the RANGE CHECK and not merely by the value being 1.
        """
        too_high = SimpleNamespace(protocol_params={"lp_slippage": 5}, max_slippage=None)
        with pytest.raises(UnprotectedTradeError, match=r"\[0, 1\)"):
            _mins(too_high)

        exactly_one = SimpleNamespace(protocol_params={"lp_slippage": 1.0}, max_slippage=None)
        with pytest.raises(UnprotectedTradeError, match=r"\[0, 1\)"):
            _mins(exactly_one)

        negative = SimpleNamespace(protocol_params={"lp_slippage": -1}, max_slippage=None)
        with pytest.raises(UnprotectedTradeError, match=r"\[0, 1\)"):
            _mins(negative)

    def test_protocol_params_lp_slippage_just_below_one_is_allowed(self) -> None:
        """The refusal is a range check, not a ban on large tolerances."""
        wide = SimpleNamespace(protocol_params={"lp_slippage": 0.99}, max_slippage=None)
        assert _mins(wide) == (10_000, 20_000)

    def test_out_of_range_failure_does_not_depend_on_max_slippage_fallback(self) -> None:
        """An explicit lp_slippage is never quietly replaced by the intent's own.

        Guards the branch shape: the range check must fire on the protocol_params
        value even when a perfectly valid ``max_slippage`` is sitting right there
        to fall back to. Falling back would "repair" the typo just as silently as
        the old clamp did.
        """
        both = SimpleNamespace(
            protocol_params={"lp_slippage": 5}, max_slippage=Decimal("0.01")
        )
        with pytest.raises(UnprotectedTradeError):
            _mins(both)
