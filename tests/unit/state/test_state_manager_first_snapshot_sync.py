"""StateManager delegation contract for the VIB-4394 first-snapshot sync readers.

Mirrors the ``get_ledger_quant_stats`` passthrough pattern
(``test_state_manager_ledger_quant_stats.py``): no WARM backend, an unsupported
backend, or a failed read all degrade to ``None`` — the documented "structurally
absent" signal that makes the boot OPENING_BALANCE seed a no-op rather than
fabricating empty inventory (Empty ≠ Zero). The end-to-end boot-seed flow is
covered in ``tests/unit/framework/accounting/test_opening_balance_boot_seed.py``;
this file isolates the two StateManager methods themselves.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from almanak.framework.state.state_manager import StateManager


def _make_manager(*, warm: object | None = None) -> StateManager:
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._warm = warm
    sm._record_metrics = MagicMock()
    # __init__ owns this in production; the degrade paths warn through
    # _unimplemented_warn, which reads it.
    sm._unimplemented_logged = set()
    return sm


class _NoReader:
    """Warm backend stand-in that does NOT expose get_first_snapshot_sync."""


# =============================================================================
# has_first_snapshot_backend
# =============================================================================


def test_has_backend_false_when_warm_is_none() -> None:
    sm = _make_manager(warm=None)
    assert sm.has_first_snapshot_backend() is False


def test_has_backend_false_when_backend_lacks_reader() -> None:
    sm = _make_manager(warm=_NoReader())
    assert sm.has_first_snapshot_backend() is False


def test_has_backend_true_when_backend_exposes_reader() -> None:
    warm = MagicMock()
    warm.get_first_snapshot_sync = MagicMock(return_value=None)
    sm = _make_manager(warm=warm)
    assert sm.has_first_snapshot_backend() is True


# =============================================================================
# get_first_snapshot_sync
# =============================================================================


def test_get_first_snapshot_no_warm_backend_returns_none_and_warns() -> None:
    sm = _make_manager(warm=None)
    assert sm.get_first_snapshot_sync("dep-X") is None
    # One-shot warning recorded (structurally absent → UNMEASURED, Empty ≠ Zero).
    assert ("get_first_snapshot_sync", "dep-X") in sm._unimplemented_logged


def test_get_first_snapshot_unsupported_backend_returns_none_and_warns() -> None:
    sm = _make_manager(warm=_NoReader())
    assert sm.get_first_snapshot_sync("dep-X") is None
    assert ("get_first_snapshot_sync", "dep-X") in sm._unimplemented_logged


def test_get_first_snapshot_backend_error_returns_none() -> None:
    warm = MagicMock()
    warm.get_first_snapshot_sync = MagicMock(side_effect=RuntimeError("db down"))
    sm = _make_manager(warm=warm)
    # A read failure degrades to None (UNMEASURED) — never raises at boot.
    assert sm.get_first_snapshot_sync("dep-X") is None


def test_get_first_snapshot_happy_path_with_snapshot() -> None:
    snapshot = MagicMock(name="PortfolioSnapshot")
    warm = MagicMock()
    warm.get_first_snapshot_sync = MagicMock(return_value=snapshot)
    sm = _make_manager(warm=warm)

    assert sm.get_first_snapshot_sync("dep-X") is snapshot
    warm.get_first_snapshot_sync.assert_called_once_with("dep-X")
    # Backend present and reader succeeded — no unimplemented warning emitted.
    assert ("get_first_snapshot_sync", "dep-X") not in sm._unimplemented_logged


def test_get_first_snapshot_happy_path_without_snapshot() -> None:
    """A wired backend that simply has no snapshot yet returns None WITHOUT warning.

    This is "measured: no snapshot" — distinct from the structurally-absent
    backend (which warns). The seed no-ops either way, but the warning fires only
    for the structural gap.
    """
    warm = MagicMock()
    warm.get_first_snapshot_sync = MagicMock(return_value=None)
    sm = _make_manager(warm=warm)

    assert sm.get_first_snapshot_sync("dep-X") is None
    warm.get_first_snapshot_sync.assert_called_once_with("dep-X")
    assert ("get_first_snapshot_sync", "dep-X") not in sm._unimplemented_logged


def test_get_first_snapshot_warning_is_one_shot() -> None:
    sm = _make_manager(warm=None)
    sm._unimplemented_warn = MagicMock(wraps=sm._unimplemented_warn)

    sm.get_first_snapshot_sync("dep-X")
    sm.get_first_snapshot_sync("dep-X")

    # Both calls route through _unimplemented_warn, but the (method, identity)
    # key de-dups inside it — the key is recorded exactly once.
    assert sm._unimplemented_warn.call_count == 2
    assert ("get_first_snapshot_sync", "dep-X") in sm._unimplemented_logged
