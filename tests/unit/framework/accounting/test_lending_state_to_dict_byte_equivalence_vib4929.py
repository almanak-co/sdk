"""Byte-equivalence pin for ``lending_state_to_dict`` (VIB-4929 PR-3a).

This is the most important regression guard the generic-reader rework ships. The
serialized dict lands verbatim in ``transaction_ledger.pre_state_json`` /
``post_state_json`` — a live-money column read back by
``category_handlers/lending_handler.py``. A byte-level drift in the persisted
dict (a dropped key, a different stringification, a missing derived bps) silently
corrupts the accounting books.

PR-3a unified the per-protocol ``AaveAccountState`` / ``MorphoBlueAccountState``
into the connector-owned :class:`LendingAccountState`. This suite freezes the
exact dict literal ``lending_state_to_dict`` MUST still produce per protocol so
the unification stays byte-identical to the pre-PR shape, with explicit coverage
of the two traps the rework had to preserve:

* **Aave-family trap**: ``e_mode_category`` and ``interest_rate_mode`` are emitted
  as JSON ``null`` **even when their value is None** — the pre-PR
  ``isinstance(AaveAccountState)`` branch did this unconditionally. They are gated
  on the structural ``family == "aave"`` discriminator, NOT value-presence.
  Morpho and Compound do NOT emit these keys at all.
* **Morpho ROUND_HALF_UP bps**: ``liquidation_threshold_bps`` is derived from
  ``lltv`` via ``round(lltv * 10000)`` with ROUND_HALF_UP — pinned to the half-up
  boundary so the rounding mode can't silently change.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
from almanak.framework.accounting.lending_accounting import lending_state_to_dict


def _aave(
    *,
    collateral: str,
    debt: str,
    hf: str,
    lt_bps: int,
    e_mode: int | None,
    irm: str | None,
) -> LendingAccountState:
    """Build the Aave-family unified state (family stamped by the reducer)."""
    return LendingAccountState(
        collateral_usd=Decimal(collateral),
        debt_usd=Decimal(debt),
        health_factor=Decimal(hf),
        liquidation_threshold_bps=lt_bps,
        e_mode_category=e_mode,
        lltv=None,
        interest_rate_mode=irm,
        family="aave",
    )


def _morpho(*, collateral: str, debt: str, hf: str, lltv: str) -> LendingAccountState:
    """Build the Morpho-family unified state (no Aave discriminator; lltv set)."""
    return LendingAccountState(
        collateral_usd=Decimal(collateral),
        debt_usd=Decimal(debt),
        health_factor=Decimal(hf),
        liquidation_threshold_bps=None,
        e_mode_category=None,
        lltv=Decimal(lltv),
        interest_rate_mode=None,
        family=None,
    )


# ---------------------------------------------------------------------------
# Frozen pre-PR dict literals — the exact persisted shape, per protocol.
# ---------------------------------------------------------------------------

_AAVE_FULL_CASE = (
    "aave_v3",
    _aave(collateral="15420.50", debt="8200.00", hf="1.882", lt_bps=8500, e_mode=0, irm="variable"),
    {
        "protocol": "aave_v3",
        "collateral_usd": "15420.50",
        "debt_usd": "8200.00",
        "health_factor": "1.882",
        "liquidation_threshold_bps": 8500,
        "e_mode_category": 0,
        "interest_rate_mode": "variable",
    },
)

# THE AAVE-FAMILY TRAP: e_mode_category=None / interest_rate_mode=None STILL emit
# those keys (as JSON null), gated on family=="aave", not on value-presence.
_AAVE_NULL_OPTIONALS_CASE = (
    "aave_v3",
    _aave(collateral="100.0", debt="50.0", hf="2.0", lt_bps=8000, e_mode=None, irm=None),
    {
        "protocol": "aave_v3",
        "collateral_usd": "100.0",
        "debt_usd": "50.0",
        "health_factor": "2.0",
        "liquidation_threshold_bps": 8000,
        "e_mode_category": None,
        "interest_rate_mode": None,
    },
)

# Aave alias normalises protocol to lowercase but keeps the family keys.
_AAVE_ALIAS_CASE = (
    "AAVE_V3",
    _aave(collateral="1.0", debt="0", hf="999999", lt_bps=8500, e_mode=2, irm=None),
    {
        "protocol": "aave_v3",
        "collateral_usd": "1.0",
        "debt_usd": "0",
        "health_factor": "999999",
        "liquidation_threshold_bps": 8500,
        "e_mode_category": 2,
        "interest_rate_mode": None,
    },
)

# Morpho: emits lltv + a derived ROUND_HALF_UP bps; NEVER the Aave-only keys.
_MORPHO_CASE = (
    "morpho_blue",
    _morpho(collateral="9000.0", debt="4000.0", hf="1.935", lltv="0.86"),
    {
        "protocol": "morpho_blue",
        "collateral_usd": "9000.0",
        "debt_usd": "4000.0",
        "health_factor": "1.935",
        "lltv": "0.86",
        "liquidation_threshold_bps": 8600,  # 0.86 * 10000
    },
)

# Compound V3 (VIB-4929 PR-3b — now the unified LendingAccountState with
# family=None and lltv=None): only the common three + protocol; no lltv, no
# Aave-only keys, no derived bps.
_COMPOUND_CASE = (
    "compound_v3",
    LendingAccountState(
        collateral_usd=Decimal("500.0"),
        debt_usd=Decimal("250.0"),
        health_factor=Decimal("1.7"),
        liquidation_threshold_bps=None,
        e_mode_category=None,
        lltv=None,
        interest_rate_mode=None,
        family=None,
    ),
    {
        "protocol": "compound_v3",
        "collateral_usd": "500.0",
        "debt_usd": "250.0",
        "health_factor": "1.7",
    },
)


@pytest.mark.parametrize(
    ("protocol", "state", "expected"),
    [
        _AAVE_FULL_CASE,
        _AAVE_NULL_OPTIONALS_CASE,
        _AAVE_ALIAS_CASE,
        _MORPHO_CASE,
        _COMPOUND_CASE,
    ],
    ids=["aave_full", "aave_null_optionals_trap", "aave_alias", "morpho", "compound_transitional"],
)
def test_lending_state_to_dict_byte_equal_to_frozen_pre_pr_literal(protocol, state, expected) -> None:
    out = lending_state_to_dict(state, protocol=protocol)
    # Exact dict equality (keys AND values AND key set) — this is the persisted
    # JSON, so a different key set or stringification is a regression.
    assert out == expected


def test_aave_family_emits_null_optional_keys_not_dropped() -> None:
    """Explicit assertion on THE TRAP: the keys are present with value None."""
    out = lending_state_to_dict(
        _aave(collateral="1", debt="1", hf="1", lt_bps=8500, e_mode=None, irm=None),
        protocol="aave_v3",
    )
    assert out is not None
    assert "e_mode_category" in out and out["e_mode_category"] is None
    assert "interest_rate_mode" in out and out["interest_rate_mode"] is None


def test_morpho_does_not_emit_aave_only_keys() -> None:
    out = lending_state_to_dict(_morpho(collateral="9000", debt="4000", hf="1.9", lltv="0.86"), protocol="morpho_blue")
    assert out is not None
    assert "e_mode_category" not in out
    assert "interest_rate_mode" not in out


def test_compound_emits_only_common_three() -> None:
    out = lending_state_to_dict(_COMPOUND_CASE[1], protocol="compound_v3")
    assert out is not None
    assert set(out) == {"protocol", "collateral_usd", "debt_usd", "health_factor"}


@pytest.mark.parametrize(
    ("lltv", "expected_bps"),
    [
        ("0.86", 8600),
        ("0.945", 9450),
        # ROUND_HALF_UP boundary: 0.85005 * 10000 = 8500.5 → 8501 (half-up), not 8500.
        ("0.85005", 8501),
        # Just below the half boundary rounds down.
        ("0.850049", 8500),
    ],
)
def test_morpho_derived_bps_round_half_up(lltv: str, expected_bps: int) -> None:
    out = lending_state_to_dict(_morpho(collateral="1", debt="1", hf="1", lltv=lltv), protocol="morpho_blue")
    assert out is not None
    assert out["liquidation_threshold_bps"] == expected_bps
    assert out["lltv"] == lltv


def test_none_state_returns_none() -> None:
    """Honest absence over fabricated zeros — unchanged hard rule."""
    assert lending_state_to_dict(None, protocol="aave_v3") is None
