"""Unit tests for the pool-agnostic peg-divergence helper (VIB-5426).

These exercise :func:`check_peg_divergence` with no gateway / oracle at all — it
is a pure value function over already-priced coins, reusable by any pegged-pool
reader (Curve today; Balancer stable / StableSwap-NG forks tomorrow).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.valuation.peg_divergence import (
    DEFAULT_DEPEG_THRESHOLD_BPS,
    check_peg_divergence,
)


def _d(x: str) -> Decimal:
    return Decimal(x)


def test_healthy_usd_pool_ok() -> None:
    out = check_peg_divergence([_d("1.0"), _d("1.0"), _d("1.0")], expected_peg_usd=_d("1"))
    assert out.ok is True
    assert out.reason is None
    assert out.peg_usd == _d("1.0")
    assert out.max_divergence_bps == 0


def test_minor_noise_within_band_ok() -> None:
    # 30 bps spread — normal StableSwap micro-deviation, must NOT flip UNAVAILABLE.
    out = check_peg_divergence([_d("1.000"), _d("0.997"), _d("1.001")], expected_peg_usd=_d("1"))
    assert out.ok is True
    assert out.max_divergence_bps < DEFAULT_DEPEG_THRESHOLD_BPS


def test_single_coin_depeg_fires() -> None:
    out = check_peg_divergence([_d("1.0"), _d("1.0"), _d("0.90")], expected_peg_usd=_d("1"))
    assert out.ok is False
    assert out.reason == "depeg_divergence"
    assert out.max_divergence_bps == 1000  # 0.10 / 1.0 (median) = 1000 bps
    assert out.peg_usd == _d("1.0")


def test_systemic_depeg_fires_only_with_expected_peg() -> None:
    prices = [_d("0.90"), _d("0.90"), _d("0.90")]
    # Without the expected peg, the inter-coin spread is 0 → looks healthy.
    blind = check_peg_divergence(prices)
    assert blind.ok is True
    assert blind.max_divergence_bps == 0
    # With the expected $1 numeraire, the peg-LEVEL drift is caught.
    seen = check_peg_divergence(prices, expected_peg_usd=_d("1"))
    assert seen.ok is False
    assert seen.reason == "depeg_divergence"
    assert seen.max_divergence_bps == 1000


def test_non_usd_peg_holds_generically() -> None:
    # A stETH/ETH-style pool priced in USD ≈ $3000 each: equal → healthy, with the
    # discovered (median) peg = $3000. No $1 special-casing anywhere.
    out = check_peg_divergence([_d("3000"), _d("2997")], expected_peg_usd=_d("3000"))
    assert out.ok is True
    assert out.peg_usd == _d("2998.5")
    # A stETH depeg to 0.94·ETH ≈ $2820 fires.
    bad = check_peg_divergence([_d("3000"), _d("2820")], expected_peg_usd=_d("3000"))
    assert bad.ok is False
    assert bad.reason == "depeg_divergence"


@pytest.mark.parametrize("prices", [[], [_d("1.0"), None], [_d("1.0"), _d("0")], [_d("1.0"), _d("-1")]])
def test_unmeasured_or_nonpositive_is_oracle_miss(prices: list[Decimal | None]) -> None:
    out = check_peg_divergence(prices, expected_peg_usd=_d("1"))
    assert out.ok is False
    assert out.reason == "oracle_unmeasured"  # distinct from a real depeg
    assert out.peg_usd is None


def test_threshold_is_configurable() -> None:
    prices = [_d("1.0"), _d("0.985")]  # 150 bps
    assert check_peg_divergence(prices, threshold_bps=100, expected_peg_usd=_d("1")).ok is False
    assert check_peg_divergence(prices, threshold_bps=200, expected_peg_usd=_d("1")).ok is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
