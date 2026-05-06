"""Chain-resolution rules for VIB-4062 MarketSnapshot.

PRD §4.2 — single-chain mismatch raises ChainNotConfiguredError; multi-chain
chain=None raises AmbiguousChainError; both error types are distinct.
"""

from __future__ import annotations

import pytest

from almanak.framework.market import (
    AmbiguousChainError,
    ChainNotConfiguredError,
    MarketSnapshot,
    MarketSnapshotBuilder,
    MarketSnapshotError,
    MultiChainMarketSnapshot,
)


def _wallet() -> str:
    return "0x" + "0" * 40


# =============================================================================
# Single-chain rules
# =============================================================================


def test_single_chain_default_returns_only_chain():
    market = MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address=_wallet())
    assert market.chains == ("arbitrum",)
    assert market._resolve_chain(None) == "arbitrum"


def test_single_chain_explicit_match_returns_chain():
    market = MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address=_wallet())
    assert market._resolve_chain("arbitrum") == "arbitrum"


def test_single_chain_explicit_mismatch_raises_chain_not_configured():
    market = MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address=_wallet())
    with pytest.raises(ChainNotConfiguredError) as exc_info:
        market._resolve_chain("ethereum")
    err = exc_info.value
    assert err.chain == "ethereum"
    assert err.chains == ("arbitrum",)
    # No silent ignore: if a caller passed a wrong chain, we tell them.
    assert "not in configured chains" in str(err)


# =============================================================================
# Multi-chain rules
# =============================================================================


@pytest.fixture
def multi_chain_market() -> MarketSnapshot:
    snap = MarketSnapshot(
        chain="arbitrum",
        wallet_address=_wallet(),
        chains=("arbitrum", "ethereum", "optimism"),
    )
    return snap


def test_multi_chain_chains_property_lists_all(multi_chain_market):
    assert set(multi_chain_market.chains) == {"arbitrum", "ethereum", "optimism"}


def test_multi_chain_default_raises_ambiguous(multi_chain_market):
    with pytest.raises(AmbiguousChainError) as exc_info:
        multi_chain_market._resolve_chain(None)
    err = exc_info.value
    # Multi-chain default-to-primary is explicitly rejected (PRD §4.2 R2).
    assert "multi-chain" in str(err).lower()
    assert set(err.chains) == {"arbitrum", "ethereum", "optimism"}


def test_multi_chain_explicit_known_returns_chain(multi_chain_market):
    assert multi_chain_market._resolve_chain("ethereum") == "ethereum"


def test_multi_chain_explicit_unknown_raises_chain_not_configured(multi_chain_market):
    with pytest.raises(ChainNotConfiguredError) as exc_info:
        multi_chain_market._resolve_chain("polygon")
    assert exc_info.value.chain == "polygon"


# =============================================================================
# Distinct error types (PRD §4.2 R5)
# =============================================================================


def test_error_types_are_distinct():
    assert AmbiguousChainError is not ChainNotConfiguredError


def test_both_inherit_from_marketsnapshoterror():
    assert issubclass(AmbiguousChainError, MarketSnapshotError)
    assert issubclass(ChainNotConfiguredError, MarketSnapshotError)


def test_runner_can_branch_on_error_type():
    """Runner-side error handlers branch on type — both are subclasses of
    ``MarketSnapshotError`` but each carries distinct policy semantics.
    """
    multi = MarketSnapshot(
        chain="arbitrum",
        wallet_address=_wallet(),
        chains=("arbitrum", "ethereum"),
    )
    # Capture both error types in a parametric scenario:
    failures: list[tuple[type, str]] = []
    try:
        multi._resolve_chain(None)
    except MarketSnapshotError as exc:
        failures.append((type(exc), exc.severity))

    try:
        multi._resolve_chain("polygon")
    except MarketSnapshotError as exc:
        failures.append((type(exc), exc.severity))

    assert (AmbiguousChainError, "critical") in failures
    assert (ChainNotConfiguredError, "critical") in failures


# =============================================================================
# MultiChainMarketSnapshot type alias
# =============================================================================


def test_multichain_alias_is_marketsnapshot():
    """No separate runtime class — it's a TypeAlias only (PRD §4.2)."""
    assert MultiChainMarketSnapshot is MarketSnapshot


def test_multichain_alias_constructs_canonical_class():
    obj = MultiChainMarketSnapshot(
        chain="arbitrum",
        wallet_address=_wallet(),
        chains=("arbitrum", "ethereum"),
    )
    assert type(obj) is MarketSnapshot
    assert isinstance(obj, MarketSnapshot)
    assert obj.chains == ("arbitrum", "ethereum")
