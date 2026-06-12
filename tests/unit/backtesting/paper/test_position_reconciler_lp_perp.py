"""Unit tests for PositionReconciler._reconcile_lp_positions and
PositionReconciler._reconcile_perp_positions.

These are the first tests for the LP and perp reconciler lanes. They also pin
the new coverage-frozenset filter semantics introduced in the
refactor/paper-engine-protocol-dispatch PR, which replaced protocol-literal
string comparisons with ``pos.protocol in LP_RECONCILER_PROTOCOLS`` /
``pos.protocol in PERP_RECONCILER_PROTOCOLS`` (imported from position_queries).

No real RPC. All on-chain queries are monkeypatched, matching the structural
pattern established by test_position_reconciler_lending.py.
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
# Factories
# ---------------------------------------------------------------------------


def _make_lp(
    *,
    position_id: str = "12345",
    protocol: str = "uniswap_v3",
    liquidity: int = 1_000_000,
    tick_lower: int = -887_220,
    tick_upper: int = 887_220,
) -> TrackedPosition:
    return TrackedPosition(
        position_id=position_id,
        position_type=PositionType.LP,
        protocol=protocol,
        token0="0xWETH",
        token1="0xUSDC",
        liquidity=liquidity,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        fee_tier=3000,
    )


def _on_chain_lp(
    *,
    token_id: int = 12345,
    liquidity: int = 1_000_000,
    tick_lower: int = -887_220,
    tick_upper: int = 887_220,
) -> SimpleNamespace:
    """Duck-typed UniswapV3Position as returned by query_uniswap_v3_positions."""
    return SimpleNamespace(
        token_id=token_id,
        liquidity=liquidity,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        is_active=liquidity > 0,
    )


def _make_perp_long(
    *,
    position_id: str = "0xperpkey",
    protocol: str = "gmx_v2",
    size_in_usd: int = 1_000 * 10**30,
) -> TrackedPosition:
    return TrackedPosition(
        position_id=position_id,
        position_type=PositionType.PERP_LONG,
        protocol=protocol,
        market="0xETH_USD_MARKET",
        collateral_token="0xUSDC",
        size_in_usd=size_in_usd,
        is_long=True,
    )


def _make_perp_short(
    *,
    position_id: str = "0xshortkey",
    protocol: str = "gmx_v2",
    size_in_usd: int = 500 * 10**30,
) -> TrackedPosition:
    return TrackedPosition(
        position_id=position_id,
        position_type=PositionType.PERP_SHORT,
        protocol=protocol,
        market="0xETH_USD_MARKET",
        collateral_token="0xUSDC",
        size_in_usd=size_in_usd,
        is_long=False,
    )


def _on_chain_gmx(
    *,
    position_key: str = "0xperpkey",
    size_in_usd: int = 1_000 * 10**30,
    is_long: bool = True,
) -> SimpleNamespace:
    """Duck-typed GMXv2Position as returned by query_gmx_positions."""
    return SimpleNamespace(
        position_key=position_key,
        size_in_usd=size_in_usd,
        is_long=is_long,
        is_active=size_in_usd > 0,
    )


def _patch_uniswap_query(
    monkeypatch: pytest.MonkeyPatch,
    return_value: list[Any] | None = None,
    raise_exc: Exception | None = None,
) -> None:
    """Patch query_uniswap_v3_positions in the position_queries module."""

    async def _fake(*, wallet: str, web3: Any, chain: str) -> list[Any]:
        if raise_exc is not None:
            raise raise_exc
        return list(return_value or [])

    import almanak.framework.backtesting.paper.position_queries as queries_mod

    monkeypatch.setattr(queries_mod, "query_uniswap_v3_positions", _fake)


def _patch_gmx_query(
    monkeypatch: pytest.MonkeyPatch,
    return_value: list[Any] | None = None,
    raise_exc: Exception | None = None,
) -> None:
    """Patch query_gmx_positions in the position_queries module."""

    async def _fake(*, wallet: str, web3: Any, chain: str) -> list[Any]:
        if raise_exc is not None:
            raise raise_exc
        return list(return_value or [])

    import almanak.framework.backtesting.paper.position_queries as queries_mod

    monkeypatch.setattr(queries_mod, "query_gmx_positions", _fake)


def _run(coro: Any) -> Any:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# LP reconciler lane
# ---------------------------------------------------------------------------


class TestReconcileLpPositions:
    """Drives _reconcile_lp_positions end-to-end with mocked Uniswap queries."""

    def test_no_discrepancy_when_matching(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tracked LP matches on-chain exactly -- no discrepancies emitted."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_lp(position_id="12345", liquidity=1_000_000, tick_lower=-100, tick_upper=100)
        reconciler.positions[pos.position_id] = pos

        _patch_uniswap_query(
            monkeypatch,
            return_value=[_on_chain_lp(token_id=12345, liquidity=1_000_000, tick_lower=-100, tick_upper=100)],
        )

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_tracked_position_missing_on_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tracked LP not found on-chain emits MISSING_ON_CHAIN."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_lp(position_id="99999")
        reconciler.positions[pos.position_id] = pos

        _patch_uniswap_query(monkeypatch, return_value=[])  # nothing on-chain

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.MISSING_ON_CHAIN
        assert d.position_type == PositionType.LP
        assert d.position_id == "99999"
        assert d.actual is None

    def test_liquidity_drift_beyond_tolerance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """LP liquidity differs by more than tolerance -- LIQUIDITY_MISMATCH emitted."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_lp(position_id="12345", liquidity=1_000_000)
        reconciler.positions[pos.position_id] = pos

        # 20% difference -- well outside 1% tolerance
        _patch_uniswap_query(
            monkeypatch,
            return_value=[_on_chain_lp(token_id=12345, liquidity=1_200_000)],
        )

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        types = {d.discrepancy_type for d in diffs}
        assert DiscrepancyType.LIQUIDITY_MISMATCH in types

    def test_tick_range_mismatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """LP tick range differs -- TICK_RANGE_MISMATCH emitted."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_lp(position_id="12345", tick_lower=-100, tick_upper=100)
        reconciler.positions[pos.position_id] = pos

        _patch_uniswap_query(
            monkeypatch,
            return_value=[_on_chain_lp(token_id=12345, tick_lower=-200, tick_upper=200)],
        )

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        types = {d.discrepancy_type for d in diffs}
        assert DiscrepancyType.TICK_RANGE_MISMATCH in types

    def test_on_chain_query_raises_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If the on-chain query raises, the method returns empty without crashing."""
        reconciler = PositionReconciler(chain="arbitrum")
        reconciler.positions["12345"] = _make_lp(position_id="12345")

        _patch_uniswap_query(monkeypatch, raise_exc=RuntimeError("rpc down"))

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_untracked_on_chain_active_position_emits_missing_in_tracker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An active on-chain LP not in the tracker emits MISSING_IN_TRACKER."""
        reconciler = PositionReconciler(chain="arbitrum")
        # Tracker is empty

        _patch_uniswap_query(
            monkeypatch,
            return_value=[_on_chain_lp(token_id=77777, liquidity=500_000)],
        )

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.MISSING_IN_TRACKER
        assert d.position_type == PositionType.LP
        assert d.position_id == "77777"

    def test_untracked_inactive_on_chain_position_not_flagged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An inactive (zero-liquidity) on-chain LP does NOT trigger MISSING_IN_TRACKER."""
        reconciler = PositionReconciler(chain="arbitrum")

        _patch_uniswap_query(
            monkeypatch,
            return_value=[_on_chain_lp(token_id=88888, liquidity=0)],  # inactive
        )

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_non_covered_protocol_lp_position_is_ignored(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """LP position from a protocol outside LP_RECONCILER_PROTOCOLS is silently ignored.

        This pins the PR's filter semantics: an aerodrome LP position must NOT
        be flagged as MISSING_ON_CHAIN even when the on-chain query returns nothing.
        """
        reconciler = PositionReconciler(chain="base")
        # aerodrome is NOT in LP_RECONCILER_PROTOCOLS (which is {"uniswap_v3"})
        aerodrome_pos = _make_lp(position_id="555", protocol="aerodrome")
        reconciler.positions[aerodrome_pos.position_id] = aerodrome_pos

        _patch_uniswap_query(monkeypatch, return_value=[])

        diffs = _run(
            reconciler._reconcile_lp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        # The aerodrome position must NOT appear in any discrepancy
        assert all(d.position_id != "555" for d in diffs)
        assert diffs == []


# ---------------------------------------------------------------------------
# Perp reconciler lane
# ---------------------------------------------------------------------------


class TestReconcilePerpPositions:
    """Drives _reconcile_perp_positions end-to-end with mocked GMX queries."""

    def test_non_arbitrum_chain_returns_empty_immediately(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On any chain other than arbitrum, the method returns empty without querying."""
        reconciler = PositionReconciler(chain="ethereum")
        reconciler.positions["0xperpkey"] = _make_perp_long()

        # The GMX query must never be called -- if it were called and raised, we'd see it
        _patch_gmx_query(monkeypatch, raise_exc=AssertionError("must not be called on non-arbitrum"))

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_no_discrepancy_when_matching(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tracked perp matches on-chain exactly -- no discrepancies emitted."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_perp_long(position_id="0xperpkey", size_in_usd=1_000 * 10**30)
        reconciler.positions[pos.position_id] = pos

        _patch_gmx_query(
            monkeypatch,
            return_value=[_on_chain_gmx(position_key="0xperpkey", size_in_usd=1_000 * 10**30)],
        )

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_tracked_perp_missing_on_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tracked perp not found on-chain emits MISSING_ON_CHAIN."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_perp_long(position_id="0xmissingkey")
        reconciler.positions[pos.position_id] = pos

        _patch_gmx_query(monkeypatch, return_value=[])  # nothing on-chain

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.MISSING_ON_CHAIN
        assert d.position_type == PositionType.PERP_LONG
        assert d.position_id == "0xmissingkey"
        assert d.actual is None

    def test_perp_short_missing_on_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A short perp missing on-chain reports correct position_type."""
        reconciler = PositionReconciler(chain="arbitrum")
        pos = _make_perp_short(position_id="0xshortmissing")
        reconciler.positions[pos.position_id] = pos

        _patch_gmx_query(monkeypatch, return_value=[])

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        assert diffs[0].position_type == PositionType.PERP_SHORT

    def test_size_drift_beyond_tolerance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Perp size differs beyond tolerance -- SIZE_MISMATCH emitted."""
        reconciler = PositionReconciler(chain="arbitrum")
        tracked_size = 1_000 * 10**30
        on_chain_size = 1_200 * 10**30  # 20% larger
        pos = _make_perp_long(position_id="0xperpkey", size_in_usd=tracked_size)
        reconciler.positions[pos.position_id] = pos

        _patch_gmx_query(
            monkeypatch,
            return_value=[_on_chain_gmx(position_key="0xperpkey", size_in_usd=on_chain_size)],
        )

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),  # 1% tolerance
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.SIZE_MISMATCH
        assert d.expected == tracked_size
        assert d.actual == on_chain_size

    def test_on_chain_query_raises_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If the on-chain GMX query raises, the method returns empty without crashing."""
        reconciler = PositionReconciler(chain="arbitrum")
        reconciler.positions["0xperpkey"] = _make_perp_long()

        _patch_gmx_query(monkeypatch, raise_exc=RuntimeError("gmx rpc down"))

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []

    def test_untracked_active_on_chain_perp_emits_missing_in_tracker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Active on-chain perp not in the tracker emits MISSING_IN_TRACKER."""
        reconciler = PositionReconciler(chain="arbitrum")
        # Tracker is empty

        _patch_gmx_query(
            monkeypatch,
            return_value=[_on_chain_gmx(position_key="0xunknown", size_in_usd=500 * 10**30, is_long=True)],
        )

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        d = diffs[0]
        assert d.discrepancy_type == DiscrepancyType.MISSING_IN_TRACKER
        assert d.position_type == PositionType.PERP_LONG
        assert d.position_id == "0xunknown"

    def test_untracked_short_on_chain_emits_perp_short_type(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Untracked on-chain short uses PERP_SHORT position_type."""
        reconciler = PositionReconciler(chain="arbitrum")

        _patch_gmx_query(
            monkeypatch,
            return_value=[_on_chain_gmx(position_key="0xshort", size_in_usd=200 * 10**30, is_long=False)],
        )

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert len(diffs) == 1
        assert diffs[0].position_type == PositionType.PERP_SHORT

    def test_non_covered_protocol_perp_position_is_ignored(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Perp position from a protocol outside PERP_RECONCILER_PROTOCOLS is silently ignored.

        This pins the PR's filter semantics: an aster_perps position must NOT be
        flagged as MISSING_ON_CHAIN even when the on-chain GMX query returns nothing.
        """
        reconciler = PositionReconciler(chain="arbitrum")
        # aster_perps is NOT in PERP_RECONCILER_PROTOCOLS (which is {"gmx_v2"})
        aster_pos = _make_perp_long(position_id="0xastershort", protocol="aster_perps")
        reconciler.positions[aster_pos.position_id] = aster_pos

        _patch_gmx_query(monkeypatch, return_value=[])

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert all(d.position_id != "0xastershort" for d in diffs)
        assert diffs == []

    def test_size_within_tolerance_no_discrepancy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Perp size difference within tolerance does not trigger SIZE_MISMATCH."""
        reconciler = PositionReconciler(chain="arbitrum")
        tracked_size = 1_000 * 10**30
        # 0.5% difference -- within 1% tolerance
        on_chain_size = int(tracked_size * 1.005)
        pos = _make_perp_long(position_id="0xperpkey", size_in_usd=tracked_size)
        reconciler.positions[pos.position_id] = pos

        _patch_gmx_query(
            monkeypatch,
            return_value=[_on_chain_gmx(position_key="0xperpkey", size_in_usd=on_chain_size)],
        )

        diffs = _run(
            reconciler._reconcile_perp_positions(
                web3=object(),
                wallet_address="0xwallet",
                tolerance_percent=Decimal("0.01"),
            )
        )

        assert diffs == []


# ---------------------------------------------------------------------------
# Coverage-set membership tests (pin the PR's frozenset semantics)
# ---------------------------------------------------------------------------


class TestReconcilerProtocolSets:
    """Verify the protocol coverage frozensets have the expected membership."""

    def test_lp_reconciler_protocols_contains_uniswap_v3(self) -> None:
        from almanak.framework.backtesting.paper.position_queries import LP_RECONCILER_PROTOCOLS

        assert "uniswap_v3" in LP_RECONCILER_PROTOCOLS

    def test_lp_reconciler_protocols_excludes_aerodrome(self) -> None:
        from almanak.framework.backtesting.paper.position_queries import LP_RECONCILER_PROTOCOLS

        assert "aerodrome" not in LP_RECONCILER_PROTOCOLS

    def test_lp_reconciler_protocols_is_frozenset(self) -> None:
        from almanak.framework.backtesting.paper.position_queries import LP_RECONCILER_PROTOCOLS

        assert isinstance(LP_RECONCILER_PROTOCOLS, frozenset)

    def test_perp_reconciler_protocols_contains_gmx_v2(self) -> None:
        from almanak.framework.backtesting.paper.position_queries import PERP_RECONCILER_PROTOCOLS

        assert "gmx_v2" in PERP_RECONCILER_PROTOCOLS

    def test_perp_reconciler_protocols_excludes_aster_perps(self) -> None:
        from almanak.framework.backtesting.paper.position_queries import PERP_RECONCILER_PROTOCOLS

        assert "aster_perps" not in PERP_RECONCILER_PROTOCOLS

    def test_perp_reconciler_protocols_is_frozenset(self) -> None:
        from almanak.framework.backtesting.paper.position_queries import PERP_RECONCILER_PROTOCOLS

        assert isinstance(PERP_RECONCILER_PROTOCOLS, frozenset)
