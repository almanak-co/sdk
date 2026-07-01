"""VIB-5572: a FAILED teardown must latch the entry gate (no re-entry).

Root cause reproduced by the 20260630 overnight batch: a ``metamorpho`` teardown
redeemed on-chain, was marked FAILED, and in **local mode**
``_request_teardown_failure_shutdown`` is a deliberate no-op (the runner stays
alive for debugging). Because a FAILED request is no longer ``is_active``,
``should_teardown()`` returns False on the next iteration, nothing gates
``decide()``, and the normal loop **re-opened** the position the teardown had
just closed (on-chain: redeem at nonce 26 → re-deposit at nonce 29).

The fix is a failed-teardown entry latch: once a teardown has FAILED for a
deployment, ``_step_teardown_and_cb_gate`` HOLDs the iteration instead of
proceeding to ``decide()`` — in ALL modes, and across a restart. The persisted
request is the single source of truth: the healthy path reads it once then
short-circuits; a blocked runner re-reads each iteration so clearing/superseding
the FAILED request releases a live runner. Scope is deliberately narrow: a
COMPLETED teardown already calls ``request_shutdown()`` on every path, so it is
NOT gated (a re-launched, cleanly torn-down strategy keeps its current
behaviour). This is a runner-level gate, so it is primitive-, protocol-, chain-
and token-agnostic: metamorpho merely surfaced it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownRequest,
    TeardownStatus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _make_strategy(deployment_id: str = "deployment:abc123") -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = "base"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    return strategy


def _make_state(strategy: MagicMock) -> RunIterationState:
    return RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
    )


def _stub_manager(request: TeardownRequest | None) -> MagicMock:
    manager = MagicMock()
    manager.get_request.return_value = request
    return manager


@pytest.fixture()
def _local(monkeypatch, tmp_path):
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))


# ---------------------------------------------------------------------------
# 1. The load-bearing repro: local teardown FAILURE latches the entry gate
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_local")
class TestLocalFailureLatchesEntry:
    def test_failure_shutdown_latches_entry_but_keeps_runner_alive(self):
        """Local failure keeps the runner alive (debugging) AND latches entry."""
        runner = _make_runner()

        runner._request_teardown_failure_shutdown("redeem reverted")

        # Local mode preserves the debugging affordance (runner stays alive)...
        assert runner._shutdown_requested is False
        # ...but the entry gate is now latched so the loop cannot re-enter.
        assert runner._teardown_entry_blocked is True
        assert "redeem reverted" in (runner._teardown_entry_blocked_reason or "")

    @pytest.mark.asyncio
    async def test_gate_holds_instead_of_deciding_after_local_failure(self, monkeypatch):
        """After a failed local teardown, the teardown/cb step HOLDs.

        Returning a non-None result from ``_step_teardown_and_cb_gate`` makes
        ``run_iteration`` return early (it never reaches ``_step_decide``), so a
        HOLD here IS the proof that ``decide()`` — and therefore any re-entry
        intent — is skipped. In reality ``mark_failed`` persists FAILED before
        ``_request_teardown_failure_shutdown`` runs, so the gate's re-read
        confirms the block; the stub mirrors that.
        """
        import almanak.framework.teardown as teardown_pkg

        runner = _make_runner()
        strategy = _make_strategy()
        request = TeardownRequest(
            deployment_id=strategy.deployment_id, mode=TeardownMode.SOFT, status=TeardownStatus.FAILED
        )
        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=_stub_manager(request)),
        )

        # Simulate the terminal failure that occurred this process.
        runner._request_teardown_failure_shutdown("redeem reverted")

        # No teardown is pending now (FAILED is not active → should_teardown False).
        with patch.object(runner, "_check_teardown_requested", return_value=None):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is not None
        assert result.status == IterationStatus.HOLD
        assert isinstance(result.intent, HoldIntent)
        assert "VIB-5572" in result.intent.reason or "teardown" in result.intent.reason.lower()

    @pytest.mark.asyncio
    async def test_decide_not_reached_when_entry_latched(self, monkeypatch):
        """Belt-and-suspenders: with a FAILED request, a spy on _step_decide is never awaited."""
        import almanak.framework.teardown as teardown_pkg

        runner = _make_runner()
        strategy = _make_strategy()
        request = TeardownRequest(
            deployment_id=strategy.deployment_id, mode=TeardownMode.SOFT, status=TeardownStatus.FAILED
        )
        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=_stub_manager(request)),
        )
        runner._teardown_entry_blocked = True
        runner._teardown_entry_blocked_reason = "teardown FAILED (test)"

        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_step_decide") as spy_decide,
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is not None
        assert result.status == IterationStatus.HOLD
        spy_decide.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Restart survival: a persisted terminal request blocks a fresh runner
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_local")
class TestPersistedTerminalBlocksFreshRunner:
    def test_failed_status_blocks(self, monkeypatch):
        import almanak.framework.teardown as teardown_pkg

        request = TeardownRequest(
            deployment_id="deployment:abc123", mode=TeardownMode.SOFT, status=TeardownStatus.FAILED
        )
        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=_stub_manager(request)),
        )

        runner = _make_runner()  # fresh: no in-memory latch
        strategy = _make_strategy()

        blocked, reason = runner._entry_blocked_by_failed_teardown(strategy)

        assert blocked is True
        assert "FAILED" in (reason or "")
        # Reading a FAILED status latches the runner for the rest of the process.
        assert runner._teardown_entry_blocked is True

    @pytest.mark.parametrize(
        "status",
        [
            TeardownStatus.CANCELLED,
            TeardownStatus.PENDING,
            TeardownStatus.EXECUTING,
            # COMPLETED does NOT block: a successful teardown already calls
            # request_shutdown() on every path, so same-process re-entry is
            # impossible; blocking it would only add a NEW restriction on a
            # deliberately re-launched, cleanly torn-down strategy (VIB-5572
            # scope note). Current restart behaviour is preserved.
            TeardownStatus.COMPLETED,
        ],
    )
    def test_non_failed_status_does_not_block(self, monkeypatch, status):
        """FAILED is the only status this gate blocks. CANCELLED = operator
        resume; active statuses are handled by the teardown_mode intercept;
        COMPLETED already shut the process down — none block entry here."""
        import almanak.framework.teardown as teardown_pkg

        request = TeardownRequest(deployment_id="deployment:abc123", mode=TeardownMode.SOFT, status=status)
        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=_stub_manager(request)),
        )

        runner = _make_runner()
        strategy = _make_strategy()

        blocked, reason = runner._entry_blocked_by_failed_teardown(strategy)

        assert blocked is False
        assert reason is None
        assert runner._teardown_entry_blocked is False

    def test_absent_request_does_not_block(self, monkeypatch):
        import almanak.framework.teardown as teardown_pkg

        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=_stub_manager(None)),
        )

        runner = _make_runner()
        blocked, reason = runner._entry_blocked_by_failed_teardown(_make_strategy())

        assert blocked is False
        assert reason is None

    def test_persisted_read_is_one_time_when_not_blocked(self, monkeypatch):
        """The hot path must not read the teardown channel every iteration."""
        import almanak.framework.teardown as teardown_pkg

        manager = _stub_manager(None)
        factory = MagicMock(return_value=manager)
        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager_for_runtime", factory)

        runner = _make_runner()
        strategy = _make_strategy()

        for _ in range(5):
            assert runner._entry_blocked_by_failed_teardown(strategy) == (False, None)

        # Read happened exactly once; subsequent iterations short-circuit.
        assert manager.get_request.call_count == 1


# ---------------------------------------------------------------------------
# 2b. Release: clearing / superseding a FAILED request unblocks a live runner
#     (the persisted request is the single source of truth — CodeRabbit Major /
#     Codex P2 / pr-auditor #3).
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_local")
class TestClearingReleasesBlock:
    def _blocked_runner(self):
        runner = _make_runner()
        # Simulate a live runner already blocked by a FAILED teardown.
        runner._teardown_entry_blocked = True
        runner._teardown_entry_blocked_reason = "prior teardown FAILED (test)"
        runner._teardown_status_checked = True
        return runner

    @pytest.mark.parametrize("cleared", [None, TeardownStatus.CANCELLED, TeardownStatus.COMPLETED])
    def test_cleared_or_superseded_request_releases_block(self, monkeypatch, cleared):
        """A blocked runner re-reads every iteration; a non-FAILED status releases."""
        import almanak.framework.teardown as teardown_pkg

        request = (
            None
            if cleared is None
            else TeardownRequest(deployment_id="deployment:abc123", mode=TeardownMode.SOFT, status=cleared)
        )
        manager = _stub_manager(request)
        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager_for_runtime", MagicMock(return_value=manager))

        runner = self._blocked_runner()
        strategy = _make_strategy()

        blocked, reason = runner._entry_blocked_by_failed_teardown(strategy)

        assert blocked is False
        assert reason is None
        assert runner._teardown_entry_blocked is False
        # It genuinely re-read while blocked (did not short-circuit).
        assert manager.get_request.call_count == 1

    def test_still_failed_keeps_block_on_reread(self, monkeypatch):
        import almanak.framework.teardown as teardown_pkg

        request = TeardownRequest(
            deployment_id="deployment:abc123", mode=TeardownMode.SOFT, status=TeardownStatus.FAILED
        )
        monkeypatch.setattr(
            teardown_pkg, "get_teardown_state_manager_for_runtime", MagicMock(return_value=_stub_manager(request))
        )
        runner = self._blocked_runner()
        blocked, _ = runner._entry_blocked_by_failed_teardown(_make_strategy())
        assert blocked is True
        assert runner._teardown_entry_blocked is True


# ---------------------------------------------------------------------------
# 3. Read-error fail direction: hosted fails loud; local degrades open on a
#    first check but a BLOCKED runner keeps the block (fail-safe).
# ---------------------------------------------------------------------------


class TestHostedReadErrorFailsLoud:
    def test_hosted_read_error_raises(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-test")
        import almanak.framework.teardown as teardown_pkg

        boom = MagicMock()
        boom.get_request.side_effect = RuntimeError("gateway down")
        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=boom),
        )

        runner = _make_runner()
        with pytest.raises(RuntimeError, match="gateway down"):
            runner._entry_blocked_by_failed_teardown(_make_strategy())

    def test_local_read_error_degrades_open(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
        import almanak.framework.teardown as teardown_pkg

        boom = MagicMock()
        boom.get_request.side_effect = RuntimeError("sqlite locked")
        monkeypatch.setattr(
            teardown_pkg,
            "get_teardown_state_manager_for_runtime",
            MagicMock(return_value=boom),
        )

        runner = _make_runner()
        # First check (not yet blocked): local degrades open, consistent with
        # should_teardown()'s local degradation; the same-process failure case is
        # covered by the in-memory latch (set in _request_teardown_failure_shutdown).
        assert runner._entry_blocked_by_failed_teardown(_make_strategy()) == (False, None)

    def test_local_read_error_while_blocked_keeps_block(self, monkeypatch, tmp_path):
        """Fail-safe: a degraded read must NEVER release an existing block."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
        import almanak.framework.teardown as teardown_pkg

        boom = MagicMock()
        boom.get_request.side_effect = RuntimeError("sqlite locked")
        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager_for_runtime", MagicMock(return_value=boom))

        runner = _make_runner()
        runner._teardown_entry_blocked = True
        runner._teardown_entry_blocked_reason = "prior teardown FAILED (test)"
        runner._teardown_status_checked = True

        blocked, reason = runner._entry_blocked_by_failed_teardown(_make_strategy())
        assert blocked is True
        assert "FAILED" in (reason or "")

    def test_hosted_localpatherror_raises(self, monkeypatch):
        """A path-helper error in hosted fails loud like any hosted read error."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-test")
        import almanak.framework.teardown as teardown_pkg
        from almanak.framework.local_paths import LocalPathError

        boom = MagicMock()
        boom.get_request.side_effect = LocalPathError("no strategy DB")
        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager_for_runtime", MagicMock(return_value=boom))

        runner = _make_runner()
        with pytest.raises(LocalPathError):
            runner._entry_blocked_by_failed_teardown(_make_strategy())

    def test_local_localpatherror_degrades_open_on_first_check(self, monkeypatch, tmp_path):
        """In local, LocalPathError degrades open on a first check.

        The loud LocalPathError surface is ``_check_teardown_requested`` /
        ``should_teardown`` (runs first each iteration); this gate degrades so a
        bare env without a resolvable strategy DB — where no FAILED could have
        been persisted anyway — is not wedged. A blocked runner still keeps its
        block (covered by ``test_local_read_error_while_blocked_keeps_block``).
        """
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
        import almanak.framework.teardown as teardown_pkg
        from almanak.framework.local_paths import LocalPathError

        boom = MagicMock()
        boom.get_request.side_effect = LocalPathError("no strategy DB")
        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager_for_runtime", MagicMock(return_value=boom))

        runner = _make_runner()
        assert runner._entry_blocked_by_failed_teardown(_make_strategy()) == (False, None)
