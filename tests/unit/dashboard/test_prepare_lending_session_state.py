"""Unit tests for ``prepare_lending_session_state``.

Covers the hydration contract:

* raw strategy state merges into session state without clobbering caller fields,
* alternate amount keys (``supplied_token_amount`` / ``_supplied_token_amount``
  / ``borrowed_token_amount`` / ``_borrowed_token_amount``) are recognized,
* missing USD values are computed via ``api_client.get_price``,
* risk metrics (LTV, leverage, health factor, available-to-borrow) are derived
  from the hydrated values,
* the strategy's own ``health_factor`` is NOT overwritten by the static-config
  approximation,
* stale ``health_factor`` is cleared when the position becomes debt-free.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.framework.dashboard.templates.lending_dashboard import (
    LendingDashboardConfig,
    prepare_lending_session_state,
)


class _StubClient:
    """Minimal stand-in for ``DashboardAPIClient``.

    Stores the prices we want to return per (token, chain) and the state to
    return from ``get_state``. Set ``state_error`` to make ``get_state`` raise.
    """

    def __init__(
        self,
        *,
        state: dict[str, Any] | None = None,
        prices: dict[str, float | str] | None = None,
        state_error: Exception | None = None,
    ) -> None:
        self._state = state or {}
        self._prices = prices or {}
        self._state_error = state_error

    def get_state(self) -> dict[str, Any]:
        if self._state_error is not None:
            raise self._state_error
        return self._state

    def get_price(self, token: str, *, chain: str) -> float | str | None:
        return self._prices.get(token)


def test_merges_raw_state_without_clobbering_session_state() -> None:
    """Caller-provided keys win; missing/empty keys backfill from raw state."""
    client = _StubClient(
        state={
            "collateral_amount": "999",  # should NOT overwrite caller's value
            "borrowed_token_amount": "50",  # should backfill (key absent)
            "extra_marker": "from_raw",
        }
    )
    out = prepare_lending_session_state(
        client,
        session_state={"collateral_amount": "100", "borrowed_token_amount": None},
        config=LendingDashboardConfig(collateral_token="USDC", borrow_token="USDT"),
        strategy_config={"chain": "arbitrum"},
    )
    assert out["collateral_amount"] == "100"  # caller's value preserved
    assert out["borrowed_token_amount"] == "50"  # backfilled
    assert out["extra_marker"] == "from_raw"


def test_get_state_exception_does_not_raise() -> None:
    """A broken gateway must not crash the dashboard."""
    client = _StubClient(state_error=RuntimeError("gateway down"))
    out = prepare_lending_session_state(
        client,
        session_state={"collateral_amount": "100", "collateral_value_usd": "100"},
        config=LendingDashboardConfig(),
        strategy_config={},
    )
    # No exception; we still get the caller-provided values back.
    assert out["collateral_amount"] == "100"


def test_alternate_supplied_borrowed_keys_are_recognized() -> None:
    """``_supplied_token_amount`` / ``_borrowed_token_amount`` underscore
    variants used by some strategies must hydrate the canonical fields.
    """
    client = _StubClient(prices={"WETH": "2000", "USDC": "1"})
    out = prepare_lending_session_state(
        client,
        session_state={
            "_supplied_token_amount": "0.5",
            "_borrowed_token_amount": "300",
        },
        config=LendingDashboardConfig(
            collateral_token="WETH", borrow_token="USDC", liquidation_ltv=0.85, max_ltv=0.80
        ),
        strategy_config={"chain": "arbitrum"},
    )
    # 0.5 WETH * $2000 = $1000 collateral; 300 USDC * $1 = $300 debt.
    assert out["collateral_amount"] == "0.5"
    assert out["borrowed_amount"] == "300"
    assert Decimal(out["collateral_value_usd"]) == Decimal("1000")
    assert Decimal(out["borrowed_value_usd"]) == Decimal("300")


def test_risk_metrics_derived_from_values() -> None:
    """LTV, leverage, available_to_borrow, health_factor all computed."""
    client = _StubClient(prices={"WETH": "2000", "USDC": "1"})
    out = prepare_lending_session_state(
        client,
        session_state={"collateral_amount": "1", "borrowed_amount": "1000"},
        config=LendingDashboardConfig(
            collateral_token="WETH",
            borrow_token="USDC",
            liquidation_ltv=0.85,
            max_ltv=0.80,
        ),
        strategy_config={"chain": "arbitrum"},
    )
    # collateral $2000 / debt $1000 -> LTV 0.5, leverage 2.0,
    # available = 2000*0.8 - 1000 = 600, HF = 2000*0.85/1000 = 1.70
    assert Decimal(out["ltv"]) == Decimal("0.5")
    assert Decimal(out["leverage"]) == Decimal("2")
    assert Decimal(out["available_to_borrow_usd"]) == Decimal("600.0")
    assert Decimal(out["health_factor"]) == Decimal("1.7")


def test_strategy_supplied_health_factor_is_not_overwritten() -> None:
    """A strategy that stamps the on-chain ``healthFactor`` must win over the
    static-config approximation.

    Regression guard for the audit finding: ``liquidation_ltv`` from
    ``LendingDashboardConfig`` is a default, not the on-chain per-asset
    ``liquidationThreshold``. Silently replacing a real ``1.04`` with a stale
    ``1.25`` would mask imminent liquidation.
    """
    client = _StubClient(
        state={"health_factor": "1.04"},  # the strategy's authoritative reading
        prices={"WETH": "2000", "USDC": "1"},
    )
    out = prepare_lending_session_state(
        client,
        session_state={"collateral_amount": "1", "borrowed_amount": "1000"},
        config=LendingDashboardConfig(
            collateral_token="WETH",
            borrow_token="USDC",
            liquidation_ltv=0.85,
        ),
        strategy_config={"chain": "arbitrum"},
    )
    assert Decimal(out["health_factor"]) == Decimal("1.04")


def test_debt_free_position_clears_stale_health_factor() -> None:
    """When debt is zero, any stale ``health_factor`` is dropped.

    The render path defaults to a safe value when the key is absent; leaving
    a stale numeric there would misrepresent risk on a debt-free position.
    """
    client = _StubClient(
        state={"health_factor": "1.04"},  # leftover from previous iteration
        prices={"WETH": "2000"},
    )
    out = prepare_lending_session_state(
        client,
        session_state={"collateral_amount": "1", "borrowed_amount": "0"},
        config=LendingDashboardConfig(collateral_token="WETH", borrow_token="USDC"),
        strategy_config={"chain": "arbitrum"},
    )
    assert "health_factor" not in out


def test_debt_free_skips_health_factor_entirely() -> None:
    """With no prior ``health_factor`` and no debt, the key stays absent."""
    client = _StubClient(prices={"WETH": "2000"})
    out = prepare_lending_session_state(
        client,
        session_state={"collateral_amount": "1"},
        config=LendingDashboardConfig(collateral_token="WETH", borrow_token="USDC"),
        strategy_config={"chain": "arbitrum"},
    )
    assert "health_factor" not in out
