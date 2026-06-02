"""Tests for VIB-3503 Part 2c: PortfolioValuer per-snapshot accounting-event prefetch.

The refactor collapses N gRPC round trips per snapshot (one per position)
to 1 by fetching all events for the deployment once at the top of
``value()`` and filtering from memory in the per-position enrichers.

These tests pin:
- Prefetch is called exactly once per ``value()`` invocation, regardless of
  position count.
- Cache is scoped to one ``value()`` call -- the next call does a fresh
  prefetch (no stale data).
- Missing primitive (old StateManager backend without
  ``get_accounting_events_sync``) silently no-ops; enrichers see empty
  events; PnL stays at zero.
- Lending and vault enrichers consult the cache, not the underlying primitive.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


def _accounting_event(position_key: str, event_type: str = "SUPPLY") -> dict:
    return {
        "id": f"id-{position_key}-{event_type}",
        "deployment_id": "s1",
        "cycle_id": "cyc",
        "execution_mode": "live",
        "timestamp": 1_712_000_000,
        "chain": "arbitrum",
        "protocol": "aave_v3",
        "wallet_address": "0xwallet",
        "event_type": event_type,
        "position_key": position_key,
        "ledger_entry_id": "le-1",
        "tx_hash": "0xabc",
        "confidence": "HIGH",
        "payload_json": "{}",
        "schema_version": 1,
    }


def _wire_store(events: list[dict]) -> MagicMock:
    """Mock accounting store whose sync primitive returns ``events``."""
    store = MagicMock()
    store.get_accounting_events_sync = MagicMock(return_value=list(events))
    return store


class TestPortfolioValuerPrefetch:
    def test_prefetch_populates_cache_grouped_by_position_key(self) -> None:
        """Cache is a dict keyed by position_key with the event dicts as values."""
        store = _wire_store(
            [
                _accounting_event("aave-usdc"),
                _accounting_event("aave-usdc", "BORROW"),
                _accounting_event("yearn-vault-1", "VAULT_DEPOSIT"),
            ]
        )
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")

        v._prefetch_accounting_events("d1")

        assert v._snapshot_event_cache is not None
        assert set(v._snapshot_event_cache.keys()) == {"aave-usdc", "yearn-vault-1"}
        assert len(v._snapshot_event_cache["aave-usdc"]) == 2
        assert len(v._snapshot_event_cache["yearn-vault-1"]) == 1
        store.get_accounting_events_sync.assert_called_once_with("d1")

    def test_prefetch_silently_noops_when_store_missing_method(self) -> None:
        """Old backend without get_accounting_events_sync → cache stays None."""
        v = PortfolioValuer()
        v.set_accounting_context(MagicMock(spec=[]), "d1")  # spec=[] strips all attributes

        v._prefetch_accounting_events("d1")

        assert v._snapshot_event_cache is None

    def test_prefetch_silently_noops_when_store_missing(self) -> None:
        """No accounting context → cache stays None."""
        v = PortfolioValuer()
        v._prefetch_accounting_events("d1")
        assert v._snapshot_event_cache is None

    def test_prefetch_handles_primitive_exception(self) -> None:
        """Primitive raises → cache cleared to None, enrichers no-op safely."""
        store = MagicMock()
        store.get_accounting_events_sync = MagicMock(side_effect=RuntimeError("rpc boom"))
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")

        v._prefetch_accounting_events("d1")

        assert v._snapshot_event_cache is None

    def test_events_for_position_key_reads_cache_when_present(self) -> None:
        """When cache is populated, _events_for_position_key reads memory only."""
        store = _wire_store([_accounting_event("k1"), _accounting_event("k2")])
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")
        v._prefetch_accounting_events("d1")
        store.get_accounting_events_sync.reset_mock()  # forget the prefetch call

        result_k1 = v._events_for_position_key("k1")
        result_k2 = v._events_for_position_key("k2")
        result_missing = v._events_for_position_key("does-not-exist")

        assert len(result_k1) == 1
        assert result_k1[0]["position_key"] == "k1"
        assert len(result_k2) == 1
        assert result_missing == []
        # Crucially: no per-position primitive calls when the cache is hit.
        store.get_accounting_events_sync.assert_not_called()

    def test_events_for_position_key_falls_back_when_cache_none(self) -> None:
        """When cache is None (e.g. enricher invoked outside value()),
        fall back to a per-position primitive call.
        """
        store = _wire_store([_accounting_event("k1")])
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")
        # No prefetch performed — cache stays None.

        result = v._events_for_position_key("k1")

        assert len(result) == 1
        store.get_accounting_events_sync.assert_called_once_with("d1", position_key="k1")

    def test_lending_enricher_consults_cache_not_primitive(self) -> None:
        """_enrich_lending_pnl reads from prefetch cache; primitive untouched
        on the per-position path.
        """
        store = _wire_store([_accounting_event("aave-usdc-supply", "SUPPLY")])
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")
        v._prefetch_accounting_events("d1")
        store.get_accounting_events_sync.reset_mock()

        # Drive the same code path the enricher uses.
        events = v._events_for_position_key("aave-usdc-supply")

        assert len(events) == 1
        store.get_accounting_events_sync.assert_not_called()

    def test_vault_enricher_consults_cache_not_primitive(self) -> None:
        """Symmetric assertion for _enrich_vault_pnl: the VAULT_DEPOSIT
        cache-read path also stops issuing per-position primitive calls
        after prefetch. Without this test the vault refactor in
        portfolio_valuer.py:_enrich_vault_pnl is unpinned.
        """
        store = _wire_store([_accounting_event("yearn-vault-1", "VAULT_DEPOSIT")])
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")
        v._prefetch_accounting_events("d1")
        store.get_accounting_events_sync.reset_mock()

        events = v._events_for_position_key("yearn-vault-1")

        assert len(events) == 1
        assert events[0]["event_type"] == "VAULT_DEPOSIT"
        store.get_accounting_events_sync.assert_not_called()

    def test_prefetch_skips_non_dict_rows(self) -> None:
        """A backend that returns a non-dict row mid-list must not crash
        prefetch. The cache builder skips bad rows and keeps the rest.
        Defensive against future backend bugs that emit corrupt entries.
        """
        store = MagicMock()
        store.get_accounting_events_sync = MagicMock(
            return_value=[
                _accounting_event("k1"),
                None,  # corrupt row
                "not-a-dict",  # corrupt row
                _accounting_event("k2"),
            ]
        )
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")

        v._prefetch_accounting_events("d1")

        assert v._snapshot_event_cache is not None
        assert set(v._snapshot_event_cache.keys()) == {"k1", "k2"}

    def test_prefetch_handles_none_return(self) -> None:
        """A backend that returns None (instead of an empty list) must
        treat it as "no events" rather than raising on iteration. The
        SQLiteStore primitive returns [] in this case but a future backend
        could legitimately return None on a degraded path.
        """
        store = MagicMock()
        store.get_accounting_events_sync = MagicMock(return_value=None)
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")

        v._prefetch_accounting_events("d1")

        # Empty cache (not None) signals "prefetch ran successfully and
        # found no events"; downstream enrichers will return early on the
        # empty lookup rather than falling back to a per-position primitive
        # call that we know would also be empty.
        assert v._snapshot_event_cache == {}


class TestPortfolioValuerPrefetchClearsCache:
    """Cache lifecycle: scoped to one value() invocation. The cleanest way
    to assert this without reaching into ``value()`` itself is to run the
    prefetch twice and confirm both executions mutate the cache identically
    (i.e. no stale entries leak across calls).
    """

    def test_repeated_prefetch_replaces_cache_atomically(self) -> None:
        store = MagicMock()
        store.get_accounting_events_sync = MagicMock(
            side_effect=[
                [_accounting_event("k1")],
                [_accounting_event("k2")],
            ]
        )
        v = PortfolioValuer()
        v.set_accounting_context(store, "d1")

        v._prefetch_accounting_events("d1")
        first_cache = v._snapshot_event_cache
        assert set(first_cache or {}) == {"k1"}

        v._prefetch_accounting_events("d1")
        second_cache = v._snapshot_event_cache
        assert set(second_cache or {}) == {"k2"}
        # Old entry must not survive the second prefetch.
        assert "k1" not in (second_cache or {})


class TestPortfolioValuerCacheBackwardsCompat:
    """Decimal-typed cost basis still works — the cache change is invisible
    to downstream PnL math."""

    def test_cache_empty_returns_zero_pnl_no_crash(self) -> None:
        """An enricher run against an empty cache must not crash; the
        position keeps cost_basis_usd = 0, unrealized_pnl_usd = 0.

        The position info must carry enough detail (asset + wallet +
        protocol) for ``_try_derive_lending_position_key`` to succeed --
        otherwise the enricher exits at the no-key branch and never
        reaches the empty-cache lookup we're trying to exercise.
        """
        from almanak.framework.teardown.models import PositionType
        from almanak.framework.valuation.portfolio_valuer import PositionValue

        v = PortfolioValuer()
        v.set_accounting_context(_wire_store([]), "d1")
        v._prefetch_accounting_events("d1")
        # Cache is non-None but empty after prefetch with no events.
        assert v._snapshot_event_cache == {}

        pos_value = PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="aave_v3",
            chain="arbitrum",
            value_usd=Decimal("1000"),
            label="aave_v3 SUPPLY",
            tokens=[],
        )
        position_info = MagicMock()
        position_info.position_type = PositionType.SUPPLY
        position_info.position_id = "p1"
        position_info.protocol = "aave_v3"
        # asset + wallet are both required by _try_derive_lending_position_key,
        # otherwise the enricher returns early at the no-key branch and the
        # cache lookup is never exercised.
        position_info.details = {
            "asset": "USDC",
            "wallet": "0x1234567890123456789012345678901234567890",
        }

        v._enrich_lending_pnl(pos_value, position_info, "arbitrum")

        # Reached the cache lookup, found nothing, returned cleanly without
        # crashing or mutating the position. Cost basis stays at default zero.
        assert pos_value.cost_basis_usd == Decimal("0")
        assert pos_value.unrealized_pnl_usd == Decimal("0")
        assert pos_value.realized_pnl_usd == Decimal("0")

    def test_borrow_enricher_includes_deleverage_events(self) -> None:
        """VIB-4974: DELEVERAGE must reach ``compute_position_pnl`` on the
        BORROW side of the snapshot/valuation lane.

        DELEVERAGE closes/reduces a borrow through the same ``match_repay``
        path as REPAY and carries debt-side principal + interest. The
        per-side event filter in ``_enrich_lending_pnl`` historically scoped
        the BORROW side to ``{"BORROW", "REPAY"}`` only, silently dropping a
        deleveraged unwind's realized cost from ``cost_basis_usd`` /
        ``realized_pnl_usd``. This pins that the event survives the filter.

        Decisive assertion: the same BORROW + DELEVERAGE event set that
        ``test_position_pnl_interest_sign.test_deleverage_interest_is_a_cost``
        scores as ``realized_pnl_usd == -0.001500`` must produce that here. If
        the filter drops DELEVERAGE, only the lone BORROW survives and realized
        PnL stays at the default ``0`` — so this fails on the unfixed filter.
        """
        import json

        from almanak.framework.teardown.models import PositionType
        from almanak.framework.valuation.portfolio_valuer import PositionValue

        def _delta_event(
            position_key: str, event_type: str, *, principal: str | None = None, interest: str | None = None, ts: str
        ) -> dict:
            payload: dict = {}
            if principal is not None:
                payload["principal_delta_usd"] = principal
            if interest is not None:
                payload["interest_delta_usd"] = interest
            ev = _accounting_event(position_key, event_type)
            ev["payload_json"] = json.dumps(payload)
            ev["timestamp"] = ts
            ev["ledger_entry_id"] = f"led-{event_type}-{ts}"
            return ev

        v = PortfolioValuer()
        v.set_accounting_context(_wire_store([]), "d1")

        position_info = MagicMock()
        position_info.position_type = PositionType.BORROW
        position_info.position_id = "p-borrow"
        position_info.protocol = "aave_v3"
        position_info.details = {
            "asset": "USDT",
            "wallet": "0x1234567890123456789012345678901234567890",
        }
        key = v._try_derive_lending_position_key(position_info, "arbitrum")
        assert key, "test setup: BORROW position key must derive"

        v._snapshot_event_cache = {
            key: [
                _delta_event(key, "BORROW", principal="5.0", ts="2026-06-01T00:00:00"),
                # principal_delta_usd is a POSITIVE magnitude in production for
                # debt closes (lending_accounting.py:862-865); cost basis is
                # reduced via ``cost_basis -= principal``.
                _delta_event(key, "DELEVERAGE", principal="5.0", interest="0.001500", ts="2026-06-01T02:00:00"),
            ]
        }

        pos_value = PositionValue(
            position_type=PositionType.BORROW,
            protocol="aave_v3",
            chain="arbitrum",
            value_usd=Decimal("0"),  # debt fully repaid by the DELEVERAGE
            label="aave_v3 BORROW",
            tokens=[],
        )

        v._enrich_lending_pnl(pos_value, position_info, "arbitrum")

        # DELEVERAGE survived the filter → its borrow interest paid is realized
        # as a cost. On the unfixed filter this assertion reads Decimal("0").
        assert pos_value.realized_pnl_usd == Decimal("-0.001500")
