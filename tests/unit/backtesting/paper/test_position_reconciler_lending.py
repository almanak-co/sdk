"""Unit tests for ``PositionReconciler._reconcile_lending_positions``.

The W5 Sub-A audit flagged this method as ``GENUINELY UNCOVERED`` (CC=24, ~8%
body coverage, zero direct tests). These tests drive the post-Sub-D extracted
helpers (``_check_supply_drift`` / ``_check_borrow_drift`` /
``_collect_untracked_*``) and the orchestrator end-to-end with mocked Aave
queries — no real RPC.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.paper.position_reconciler import (
    DiscrepancyType,
    PositionReconciler,
    PositionType,
    TrackedPosition,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_supply(
    *,
    asset: str = "USDC",
    asset_address: str = "0xUSDC",
    atoken_balance: int = 1_000_000,
    position_id: str | None = None,
) -> TrackedPosition:
    return TrackedPosition(
        position_id=position_id or f"aave_v3_{asset_address.lower()}_supply",
        position_type=PositionType.SUPPLY,
        protocol="aave_v3",
        asset=asset,
        asset_address=asset_address,
        atoken_balance=atoken_balance,
    )


def _make_borrow(
    *,
    asset: str = "WETH",
    asset_address: str = "0xWETH",
    debt_balance: int = 500_000,
    position_id: str | None = None,
) -> TrackedPosition:
    return TrackedPosition(
        position_id=position_id or f"aave_v3_{asset_address.lower()}_borrow",
        position_type=PositionType.BORROW,
        protocol="aave_v3",
        asset=asset,
        asset_address=asset_address,
        debt_balance=debt_balance,
    )


def _on_chain_aave(
    *,
    asset_address: str,
    asset: str,
    has_supply: bool = False,
    has_debt: bool = False,
    current_atoken_balance: int = 0,
    total_debt: int = 0,
) -> SimpleNamespace:
    """Mock the AaveV3PositionData duck-typed shape that ``query_aave_positions`` returns."""
    return SimpleNamespace(
        asset=asset,
        asset_address=asset_address,
        has_supply=has_supply,
        has_debt=has_debt,
        current_atoken_balance=current_atoken_balance,
        total_debt=total_debt,
    )


def _patch_query(
    monkeypatch: pytest.MonkeyPatch,
    return_value: list[Any] | None = None,
    raise_exc: Exception | None = None,
) -> None:
    """Patch ``query_aave_positions`` in the position_queries module the reconciler imports."""

    async def _fake(*, wallet: str, web3: Any, chain: str) -> list[Any]:
        if raise_exc is not None:
            raise raise_exc
        return list(return_value or [])

    import almanak.framework.backtesting.paper.position_queries as queries_mod

    monkeypatch.setattr(queries_mod, "query_aave_positions", _fake)


def _run(coro: Any) -> Any:
    return asyncio.new_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Orchestrator-level cases
# ---------------------------------------------------------------------------


class TestReconcileLendingPositions:
    """Drives ``_reconcile_lending_positions`` end-to-end with mocked Aave queries."""

    def test_no_diff_when_balances_match(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tracker and on-chain agree → no discrepancies."""
        reconciler = PositionReconciler(chain="arbitrum")
        # The "untracked" check synthesises ids of the form ``aave_v3_<addr>_supply``,
        # so the dict keys must match that convention.
        supply = _make_supply(atoken_balance=1_000_000)
        borrow = _make_borrow(debt_balance=500_000)
        reconciler.positions[supply.position_id] = supply
        reconciler.positions[borrow.position_id] = borrow

        on_chain = [
            _on_chain_aave(
                asset_address="0xUSDC",
                asset="USDC",
                has_supply=True,
                current_atoken_balance=1_000_000,
            ),
            _on_chain_aave(
                asset_address="0xWETH",
                asset="WETH",
                has_debt=True,
                total_debt=500_000,
            ),
        ]
        _patch_query(monkeypatch, return_value=on_chain)

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_query_failure_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failing query is logged and reported as zero discrepancies (do not crash)."""
        reconciler = PositionReconciler(chain="arbitrum")
        reconciler.positions["s"] = _make_supply()
        _patch_query(monkeypatch, raise_exc=RuntimeError("rpc down"))

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_discovers_untracked_supply_and_borrow(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On-chain rows absent from the tracker emit MISSING_IN_TRACKER per kind."""
        reconciler = PositionReconciler(chain="arbitrum")
        # Tracker is empty
        on_chain = [
            _on_chain_aave(
                asset_address="0xUSDC",
                asset="USDC",
                has_supply=True,
                current_atoken_balance=2_000_000,
            ),
            _on_chain_aave(
                asset_address="0xWETH",
                asset="WETH",
                has_debt=True,
                total_debt=750_000,
            ),
        ]
        _patch_query(monkeypatch, return_value=on_chain)

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        kinds = {(d.discrepancy_type, d.position_type) for d in diffs}
        assert (DiscrepancyType.MISSING_IN_TRACKER, PositionType.SUPPLY) in kinds
        assert (DiscrepancyType.MISSING_IN_TRACKER, PositionType.BORROW) in kinds
        assert len(diffs) == 2

    def test_tracked_position_missing_on_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A tracked supply with no on-chain row emits MISSING_ON_CHAIN."""
        reconciler = PositionReconciler(chain="arbitrum")
        supply = _make_supply(asset="USDC", asset_address="0xUSDC")
        reconciler.positions[supply.position_id] = supply
        _patch_query(monkeypatch, return_value=[])  # empty on-chain

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.MISSING_ON_CHAIN
        assert d.position_type == PositionType.SUPPLY
        assert d.actual is None

    def test_borrow_amount_mismatch_above_tolerance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Borrow drift greater than tolerance emits AMOUNT_MISMATCH."""
        reconciler = PositionReconciler(chain="arbitrum")
        borrow = _make_borrow(debt_balance=500_000)
        reconciler.positions[borrow.position_id] = borrow
        on_chain = [
            _on_chain_aave(
                asset_address="0xWETH",
                asset="WETH",
                has_debt=True,
                total_debt=550_000,  # 10% drift
            ),
        ]
        _patch_query(monkeypatch, return_value=on_chain)

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),  # 1% tolerance
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH
        assert d.position_type == PositionType.BORROW
        assert d.expected == 500_000
        assert d.actual == 550_000


class TestUntrackedAddressNormalisation:
    """Tracker may key positions by checksum address; on-chain dicts are lowercase.

    Regression against the prior shape where ``_collect_untracked_supply`` /
    ``_collect_untracked_borrow`` synthesised a lowercase position id and looked
    it up by membership against ``tracked_supply`` / ``tracked_borrow`` — a
    tracker entry keyed off a checksum address would falsely surface as
    ``MISSING_IN_TRACKER`` on every reconciliation pass.
    """

    # Real USDC checksum on Ethereum mainnet — exercises mixed case explicitly.
    _USDC_CHECKSUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    _WETH_CHECKSUM = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def test_supply_matches_checksum_tracker_against_lowercase_on_chain(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        reconciler = PositionReconciler(chain="ethereum")
        supply = TrackedPosition(
            # Tracker entry keyed off the checksum address (mixed case).
            position_id=f"aave_v3_{self._USDC_CHECKSUM}_supply",
            position_type=PositionType.SUPPLY,
            protocol="aave_v3",
            asset="USDC",
            asset_address=self._USDC_CHECKSUM,
            atoken_balance=1_000_000,
        )
        reconciler.positions[supply.position_id] = supply

        on_chain = [
            _on_chain_aave(
                asset_address=self._USDC_CHECKSUM.lower(),
                asset="USDC",
                has_supply=True,
                current_atoken_balance=1_000_000,
            ),
        ]
        _patch_query(monkeypatch, return_value=on_chain)

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        # No MISSING_IN_TRACKER false positive, no MISSING_ON_CHAIN either —
        # the position must be matched by normalised address comparison.
        assert all(d.discrepancy_type != DiscrepancyType.MISSING_IN_TRACKER for d in diffs)
        assert all(d.discrepancy_type != DiscrepancyType.MISSING_ON_CHAIN for d in diffs)
        assert diffs == []

    def test_borrow_matches_checksum_tracker_against_lowercase_on_chain(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        reconciler = PositionReconciler(chain="ethereum")
        borrow = TrackedPosition(
            position_id=f"aave_v3_{self._WETH_CHECKSUM}_borrow",
            position_type=PositionType.BORROW,
            protocol="aave_v3",
            asset="WETH",
            asset_address=self._WETH_CHECKSUM,
            debt_balance=500_000,
        )
        reconciler.positions[borrow.position_id] = borrow

        on_chain = [
            _on_chain_aave(
                asset_address=self._WETH_CHECKSUM.lower(),
                asset="WETH",
                has_debt=True,
                total_debt=500_000,
            ),
        ]
        _patch_query(monkeypatch, return_value=on_chain)

        diffs = _run(
            reconciler._reconcile_lending_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert all(d.discrepancy_type != DiscrepancyType.MISSING_IN_TRACKER for d in diffs)
        assert all(d.discrepancy_type != DiscrepancyType.MISSING_ON_CHAIN for d in diffs)
        assert diffs == []


