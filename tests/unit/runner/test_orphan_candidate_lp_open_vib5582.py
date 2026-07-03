"""VIB-5582 — orphan-candidate recovery net for a failed LP_OPEN registry commit.

``save_ledger_and_registry_atomic`` is a single transaction (SQLite) / single
RPC (gateway): a raise means NOTHING landed — no ledger row, no registry row,
no ``position_events`` row — even though the on-chain mint already succeeded.
For a Uniswap V4 pool this orphan is *also* unrecoverable by Plan-B
``--discover`` (the V4 PositionManager has no ``tokenOfOwnerByIndex`` /
``ERC721Enumerable`` — no on-chain wallet-wide enumeration exists at all).

``StrategyRunner._save_ledger_and_registry_lp_with_orphan_guard`` closes that
gap: on an ``AccountingPersistenceError`` from the atomic write it (1) emits
a structured ``orphan_candidate_lp_open`` ERROR log with every identity
anchor an operator needs to recover manually, and (2) falls back to the
plain (already gateway-routed, already-production) ``save_ledger_entry`` RPC
as a best-effort durability net — then re-raises the ORIGINAL exception
unchanged so the existing VIB-3157 fail-closed/live-halt contract is
untouched.

These tests exercise the wrapper directly (bound via ``functools.partial``
onto a minimal stub, mirroring the established pattern in
``test_registry_dispatch_helpers.py``), not the full runner.
"""

from __future__ import annotations

import functools
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.registry_errors import RegistryAutoCollisionError


def _runner(*, state_manager) -> SimpleNamespace:
    runner = SimpleNamespace()
    runner.state_manager = state_manager
    runner._save_ledger_and_registry_lp_with_orphan_guard = functools.partial(
        StrategyRunner._save_ledger_and_registry_lp_with_orphan_guard, runner
    )
    runner._log_orphan_candidate_lp_open = functools.partial(StrategyRunner._log_orphan_candidate_lp_open, runner)
    runner._best_effort_ledger_fallback = functools.partial(StrategyRunner._best_effort_ledger_fallback, runner)
    return runner


def _entry(tx_hash: str = "0xledger") -> SimpleNamespace:
    return SimpleNamespace(id="ledger-uuid-1", tx_hash=tx_hash)


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id="deployment:abc123", wallet_address="0xWallet")


def _v4_payload() -> dict:
    return {
        "token_id": "42",
        "pool_id": "0x" + "ab" * 32,
        "position_manager": "0xPositionManager",
    }


@pytest.mark.asyncio
async def test_happy_path_no_orphan_log_no_fallback(monkeypatch, caplog):
    """The common case: the atomic write succeeds — no marker, no fallback."""
    save_mock = AsyncMock(return_value=None)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    state_manager = SimpleNamespace(save_ledger_entry=AsyncMock())
    runner = _runner(state_manager=state_manager)
    registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih")

    with caplog.at_level(logging.ERROR):
        await runner._save_ledger_and_registry_lp_with_orphan_guard(
            entry=_entry(),
            registry_row=registry_row,
            strategy=_strategy(),
            intent_type_str="LP_OPEN",
            chain="arbitrum",
            protocol="uniswap_v4",
            payload=_v4_payload(),
        )

    save_mock.assert_awaited_once()
    state_manager.save_ledger_entry.assert_not_awaited()
    assert "orphan_candidate_lp_open" not in caplog.text


@pytest.mark.asyncio
async def test_lp_open_failure_logs_orphan_marker_with_identity(monkeypatch, caplog):
    """The VIB-5360 shape: mint succeeded, atomic commit raises. The marker
    must carry chain/protocol/wallet/tx_hash/token_id/pool_id/position_manager
    /physical_identity_hash so an operator can recover WITHOUT --wallet-wide."""
    exc = AccountingPersistenceError("ledger", deployment_id="deployment:abc123", message="boom")
    save_mock = AsyncMock(side_effect=exc)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    state_manager = SimpleNamespace(save_ledger_entry=AsyncMock(return_value=None))
    runner = _runner(state_manager=state_manager)
    registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih-v4")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(AccountingPersistenceError):
            await runner._save_ledger_and_registry_lp_with_orphan_guard(
                entry=_entry(tx_hash="0xminted"),
                registry_row=registry_row,
                strategy=_strategy(),
                intent_type_str="LP_OPEN",
                chain="arbitrum",
                protocol="uniswap_v4",
                payload=_v4_payload(),
            )

    assert "orphan_candidate_lp_open" in caplog.text
    assert "deployment:abc123" in caplog.text
    assert "arbitrum" in caplog.text
    assert "uniswap_v4" in caplog.text
    assert "0xminted" in caplog.text
    assert "42" in caplog.text  # token_id
    assert "0x" + "ab" * 32 in caplog.text  # pool_id
    assert "0xPositionManager" in caplog.text
    assert "0xpih-v4" in caplog.text
    # Best-effort durability net still ran.
    state_manager.save_ledger_entry.assert_awaited_once()


@pytest.mark.asyncio
async def test_registry_auto_collision_error_also_triggers_orphan_marker(monkeypatch, caplog):
    """The EXACT VIB-4614/VIB-5360 collision shape: the preflight missed a
    same-pool auto-mode reopen and the commit-path unique-index backstop
    raises ``RegistryAutoCollisionError`` — NOT ``AccountingPersistenceError``
    (registry_errors.py: deliberately not a subclass, so it "propagates
    UNCHANGED" per VIB-5409's own contract). The orphan-candidate marker
    must fire for THIS exception type too, and the ORIGINAL typed exception
    (not a laundered AccountingPersistenceError) must be what propagates —
    a second, generic `except Exception` re-wrap would be exactly the
    VIB-5360 defect-2 regression the outer `_write_ledger_entry` handler
    was written to prevent."""
    collision_exc = RegistryAutoCollisionError(
        semantic_grouping_key="arbitrum:" + "ab" * 32,
        existing_physical_identity_hash="0xpih-existing",
        opened_tx="0xexistingtx",
        accounting_category="lp",
    )
    save_mock = AsyncMock(side_effect=collision_exc)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    state_manager = SimpleNamespace(save_ledger_entry=AsyncMock(return_value=None))
    runner = _runner(state_manager=state_manager)
    registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih-new-orphan")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RegistryAutoCollisionError) as excinfo:
            await runner._save_ledger_and_registry_lp_with_orphan_guard(
                entry=_entry(tx_hash="0xorphanmint"),
                registry_row=registry_row,
                strategy=_strategy(),
                intent_type_str="LP_OPEN",
                chain="arbitrum",
                protocol="uniswap_v4",
                payload=_v4_payload(),
            )

    # The EXACT typed exception propagates, verbatim — never laundered.
    assert excinfo.value is collision_exc
    assert "orphan_candidate_lp_open" in caplog.text
    assert "0xorphanmint" in caplog.text
    assert "0xpih-new-orphan" in caplog.text
    state_manager.save_ledger_entry.assert_awaited_once()


@pytest.mark.asyncio
async def test_lp_close_failure_does_not_log_orphan_marker(monkeypatch, caplog):
    """A failed LP_CLOSE commit is a different, already-covered shape
    (VIB-5409's close-row fallback / typed-error propagation) — the
    orphan-candidate marker is LP_OPEN-only. The ledger fallback still runs
    (pure durability upside for either direction)."""
    exc = AccountingPersistenceError("ledger", deployment_id="deployment:abc123", message="boom")
    save_mock = AsyncMock(side_effect=exc)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    state_manager = SimpleNamespace(save_ledger_entry=AsyncMock(return_value=None))
    runner = _runner(state_manager=state_manager)
    registry_row = SimpleNamespace(status="closed", physical_identity_hash="0xpih-close")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(AccountingPersistenceError):
            await runner._save_ledger_and_registry_lp_with_orphan_guard(
                entry=_entry(),
                registry_row=registry_row,
                strategy=_strategy(),
                intent_type_str="LP_CLOSE",
                chain="arbitrum",
                protocol="uniswap_v4",
                payload=_v4_payload(),
            )

    assert "orphan_candidate_lp_open" not in caplog.text
    state_manager.save_ledger_entry.assert_awaited_once()


@pytest.mark.asyncio
async def test_ledger_fallback_second_failure_does_not_mask_original(monkeypatch, caplog):
    """A SECOND failure (the plain ledger write also fails) must be logged
    and swallowed — the ORIGINAL AccountingPersistenceError is what
    propagates, never the fallback's own exception."""
    original_exc = AccountingPersistenceError("ledger", deployment_id="deployment:abc123", message="original")
    save_mock = AsyncMock(side_effect=original_exc)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    state_manager = SimpleNamespace(save_ledger_entry=AsyncMock(side_effect=RuntimeError("fallback also broken")))
    runner = _runner(state_manager=state_manager)
    registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(AccountingPersistenceError) as excinfo:
            await runner._save_ledger_and_registry_lp_with_orphan_guard(
                entry=_entry(),
                registry_row=registry_row,
                strategy=_strategy(),
                intent_type_str="LP_OPEN",
                chain="arbitrum",
                protocol="uniswap_v4",
                payload=_v4_payload(),
            )

    assert excinfo.value is original_exc
    assert "orphan_candidate_ledger_fallback_failed" in caplog.text


@pytest.mark.asyncio
async def test_no_state_manager_skips_fallback_without_raising(monkeypatch, caplog):
    """A missing/incapable state_manager must not itself raise — the
    fallback is best-effort, never load-bearing for the re-raise."""
    exc = AccountingPersistenceError("ledger", deployment_id="deployment:abc123", message="boom")
    save_mock = AsyncMock(side_effect=exc)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    runner = _runner(state_manager=None)
    registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(AccountingPersistenceError):
            await runner._save_ledger_and_registry_lp_with_orphan_guard(
                entry=_entry(),
                registry_row=registry_row,
                strategy=_strategy(),
                intent_type_str="LP_OPEN",
                chain="arbitrum",
                protocol="uniswap_v4",
                payload=_v4_payload(),
            )

    assert "orphan_candidate_lp_open" in caplog.text


@pytest.mark.asyncio
async def test_v3_payload_shape_logged_too(monkeypatch, caplog):
    """The marker must also work for the V3 payload shape
    (`pool_address` / `nft_manager_addr` instead of `pool_id` /
    `position_manager`) — this is a shared V3+V4 safety net, not V4-only."""
    exc = AccountingPersistenceError("ledger", deployment_id="deployment:v3", message="boom")
    save_mock = AsyncMock(side_effect=exc)
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_mock)

    state_manager = SimpleNamespace(save_ledger_entry=AsyncMock(return_value=None))
    runner = _runner(state_manager=state_manager)
    registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih-v3")
    v3_payload = {
        "token_id": "7",
        "pool_address": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        "nft_manager_addr": "0xNftManager",
    }

    with caplog.at_level(logging.ERROR):
        with pytest.raises(AccountingPersistenceError):
            await runner._save_ledger_and_registry_lp_with_orphan_guard(
                entry=_entry(tx_hash="0xv3mint"),
                registry_row=registry_row,
                strategy=_strategy(),
                intent_type_str="LP_OPEN",
                chain="arbitrum",
                protocol="uniswap_v3",
                payload=v3_payload,
            )

    assert "orphan_candidate_lp_open" in caplog.text
    assert "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443" in caplog.text
    assert "0xNftManager" in caplog.text
