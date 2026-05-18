"""Unit tests for the shared concentrated-liquidity math helpers.

Focus: ``compute_lp_slippage_mins`` slippage-source precedence. The regression
guarded here is the ``Decimal("0")`` falsy trap — an intent that explicitly
requests zero LP slippage (fail-closed, min == desired) must NOT silently fall
back to the wider connector default.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.connectors.base.cl_math import compute_lp_slippage_mins

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

    def test_protocol_params_lp_slippage_clamped_to_unit_interval(self) -> None:
        too_high = SimpleNamespace(protocol_params={"lp_slippage": 5}, max_slippage=None)
        assert _mins(too_high) == (0, 0)

        negative = SimpleNamespace(protocol_params={"lp_slippage": -1}, max_slippage=None)
        assert _mins(negative) == (_DESIRED0, _DESIRED1)
