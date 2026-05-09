"""Tests for the AccountingProcessor intent classifier (VIB-3467).

Covers all 25+ intent types across all 8 AccountingCategory values.
"""

import pytest

from almanak.framework.accounting.classifier import AccountingCategory, classify


# ──────────────────────────────────────────────────────────────────────────────
# LENDING
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type",
    ["SUPPLY", "supply", "Supply", "BORROW", "REPAY", "DELEVERAGE", "WITHDRAW"],
)
def test_lending_category(intent_type: str) -> None:
    assert classify(intent_type) == AccountingCategory.LENDING
    assert classify(intent_type, protocol="aave_v3") == AccountingCategory.LENDING
    assert classify(intent_type, protocol="morpho_blue") == AccountingCategory.LENDING
    assert classify(intent_type, protocol="radiant_v2") == AccountingCategory.LENDING


# ──────────────────────────────────────────────────────────────────────────────
# PENDLE LP
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
def test_pendle_lp_category(intent_type: str) -> None:
    assert classify(intent_type, protocol="pendle") == AccountingCategory.PENDLE_LP
    assert classify(intent_type, protocol="Pendle") == AccountingCategory.PENDLE_LP
    assert classify(intent_type, protocol="pendle_v2") == AccountingCategory.PENDLE_LP


# ──────────────────────────────────────────────────────────────────────────────
# LP (non-Pendle)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
def test_lp_non_pendle(intent_type: str) -> None:
    assert classify(intent_type) == AccountingCategory.LP
    assert classify(intent_type, protocol="uniswap_v3") == AccountingCategory.LP
    assert classify(intent_type, protocol="aerodrome") == AccountingCategory.LP


# ──────────────────────────────────────────────────────────────────────────────
# PENDLE PT (SWAP + pendle + PT- token)
# ──────────────────────────────────────────────────────────────────────────────


def test_pendle_pt_buy() -> None:
    assert classify("SWAP", protocol="pendle", token_out="PT-wstETH-25JUN2026") == AccountingCategory.PENDLE_PT


def test_pendle_pt_requires_pt_prefix() -> None:
    # SWAP on pendle without PT- prefix → generic SWAP
    assert classify("SWAP", protocol="pendle", token_out="SY-wstETH") == AccountingCategory.SWAP
    assert classify("SWAP", protocol="pendle", token_out="YT-wstETH-25JUN2026") == AccountingCategory.SWAP


def test_pendle_pt_prefix_case_insensitive() -> None:
    assert classify("SWAP", protocol="pendle", token_out="pt-wstETH-25JUN2026") == AccountingCategory.PENDLE_PT


# ──────────────────────────────────────────────────────────────────────────────
# PERP
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type",
    ["PERP_OPEN", "PERP_CLOSE", "PERP_INCREASE", "PERP_DECREASE", "PERP_LIQUIDATE"],
)
def test_perp_category(intent_type: str) -> None:
    assert classify(intent_type) == AccountingCategory.PERP
    assert classify(intent_type, protocol="gmx_v2") == AccountingCategory.PERP
    assert classify(intent_type, protocol="drift") == AccountingCategory.PERP


# ──────────────────────────────────────────────────────────────────────────────
# VAULT
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type",
    ["VAULT_DEPOSIT", "VAULT_WITHDRAW", "VAULT_REDEEM", "VAULT_HARVEST", "VAULT_REALLOCATE"],
)
def test_vault_category(intent_type: str) -> None:
    assert classify(intent_type) == AccountingCategory.VAULT
    assert classify(intent_type, protocol="beefy") == AccountingCategory.VAULT
    assert classify(intent_type, protocol="erc4626") == AccountingCategory.VAULT


# ──────────────────────────────────────────────────────────────────────────────
# SWAP (non-Pendle)
# ──────────────────────────────────────────────────────────────────────────────


def test_swap_generic() -> None:
    assert classify("SWAP") == AccountingCategory.SWAP
    assert classify("SWAP", protocol="uniswap_v3") == AccountingCategory.SWAP
    assert classify("SWAP", protocol="enso") == AccountingCategory.SWAP
    assert classify("SWAP", protocol="1inch") == AccountingCategory.SWAP


# ──────────────────────────────────────────────────────────────────────────────
# NO_ACCOUNTING
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type",
    # VIB-4164 (T4) reclassified BRIDGE → AccountingCategory.TRANSFER. The
    # remaining utility / placeholder intents stay on NO_ACCOUNTING.
    ["HOLD", "WRAP_NATIVE", "UNWRAP_NATIVE", "ENSURE_BALANCE", "FLASH_LOAN"],
)
def test_no_accounting_types(intent_type: str) -> None:
    assert classify(intent_type) == AccountingCategory.NO_ACCOUNTING
    assert classify(intent_type, protocol="any") == AccountingCategory.NO_ACCOUNTING


def test_bridge_classifies_to_transfer() -> None:
    """VIB-4164 (T4) — BRIDGE was NO_ACCOUNTING pre-T4, now routes to TRANSFER."""
    assert classify("BRIDGE") == AccountingCategory.TRANSFER
    # Protocol-aware special cases in classify() never apply to BRIDGE
    # (the row's primitive is BRIDGE, not LP / SWAP).
    for protocol in ("across", "stargate", "lifi", "pendle_v2", "any"):
        assert classify("BRIDGE", protocol=protocol) == AccountingCategory.TRANSFER


def test_unknown_intent_type() -> None:
    assert classify("UNKNOWN_FUTURE_INTENT") == AccountingCategory.NO_ACCOUNTING
    assert classify("") == AccountingCategory.NO_ACCOUNTING
    assert classify("APPROVE") == AccountingCategory.NO_ACCOUNTING


# ──────────────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────────────


def test_case_normalisation() -> None:
    assert classify("supply") == AccountingCategory.LENDING
    assert classify("lp_open", protocol="uniswap") == AccountingCategory.LP
    assert classify("lp_open", protocol="PENDLE") == AccountingCategory.PENDLE_LP
    assert classify("swap", protocol="PENDLE", token_out="PT-stETH-26DEC2025") == AccountingCategory.PENDLE_PT


def test_token_out_empty_pendle_swap() -> None:
    # Pendle SWAP with empty token_out → not a PT buy → generic SWAP
    assert classify("SWAP", protocol="pendle", token_out="") == AccountingCategory.SWAP
