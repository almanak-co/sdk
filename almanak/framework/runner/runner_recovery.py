"""Session recovery and execution progress methods for StrategyRunner.

Extracted from strategy_runner.py for maintainability. Each function takes
``runner`` (a StrategyRunner instance) as its first argument and is called
via a thin delegation stub in StrategyRunner.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any, cast

from ..execution.orchestrator import ExecutionOrchestrator
from ..execution.session import (
    ExecutionPhase as SessionPhase,
)
from ..execution.session import (
    ExecutionSession,
    TransactionStatus,
)
from ..state.state_manager import StateData, StateNotFoundError
from .runner_models import ExecutionProgress

# Use the original strategy_runner logger so existing log-capture tests and
# log-filtering rules continue to work after the extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# -------------------------------------------------------------------------
# Startup recovery
# -------------------------------------------------------------------------


async def recover_incomplete_sessions(runner: Any) -> int:
    """Recover incomplete execution sessions on startup.

    Scans for sessions that were interrupted (e.g., due to crash) and
    attempts to recover them based on their phase:

    - SUBMITTED phase: Poll for receipt - the transaction may have been
      mined. If confirmed, update state; if failed/not found, mark complete.
    - SIGNING/PREPARING phase: Safe to abandon - no on-chain state change
      occurred. Mark as failed so they can be retried from scratch.
    - CONFIRMING phase: Poll for receipt like SUBMITTED.

    Duplicate transaction prevention:
    - Track recovered tx_hashes and nonces to prevent re-execution
    - If a transaction was already submitted, we skip re-submission

    Returns:
        Number of sessions recovered
    """
    if runner._session_store is None:
        logger.debug("Session store not configured, skipping recovery")
        return 0

    incomplete_sessions = runner._session_store.get_incomplete_sessions()

    if not incomplete_sessions:
        logger.info("No incomplete sessions found for recovery")
        return 0

    logger.info(f"Found {len(incomplete_sessions)} incomplete sessions for recovery")

    recovered_count = 0

    for session in incomplete_sessions:
        try:
            recovered = await recover_session(runner, session)
            if recovered:
                recovered_count += 1
        except Exception as e:
            logger.error(
                f"Recovery failed for session {session.session_id}: {e}",
                extra={"session_id": session.session_id},
            )
            # Mark session as failed to prevent infinite recovery attempts
            session.set_error(f"Recovery failed: {e}")
            session.mark_complete(success=False)
            runner._session_store.save(session)

    logger.info(f"Recovered {recovered_count}/{len(incomplete_sessions)} sessions")
    return recovered_count


async def recover_session(runner: Any, session: ExecutionSession) -> bool:
    """Recover a single incomplete execution session.

    Args:
        runner: StrategyRunner instance
        session: The session to recover

    Returns:
        True if session was successfully recovered
    """
    logger.info(
        f"Recovering session {session.session_id} "
        f"(strategy={session.strategy_id}, phase={session.phase.value}, "
        f"attempt={session.attempt_number})"
    )

    # Track nonces from this session for duplicate prevention
    strategy_id = session.strategy_id
    if strategy_id not in runner._recovered_nonces:
        runner._recovered_nonces[strategy_id] = set()

    for tx_state in session.transactions:
        if tx_state.tx_hash:
            runner._recovered_tx_hashes.add(tx_state.tx_hash)
        if tx_state.nonce > 0:
            runner._recovered_nonces[strategy_id].add(tx_state.nonce)

    # Handle based on session phase
    if session.phase in (SessionPhase.SUBMITTED, SessionPhase.CONFIRMING):
        # Transaction was submitted - poll for receipt
        return await recover_submitted_session(runner, session)
    elif session.phase in (SessionPhase.PREPARING, SessionPhase.SIGNING):
        # No on-chain activity yet - safe to abandon
        return await recover_early_phase_session(runner, session)
    else:
        logger.warning(f"Unknown phase {session.phase.value} for session {session.session_id}")
        return False


async def recover_submitted_session(runner: Any, session: ExecutionSession) -> bool:
    """Recover a session that was in SUBMITTED or CONFIRMING phase.

    For submitted transactions, we poll for receipts to determine
    the final outcome. The transaction may have:
    - Succeeded (CONFIRMED)
    - Failed/reverted (FAILED)
    - Been dropped from mempool (not found)

    Args:
        runner: StrategyRunner instance
        session: Session with submitted transactions

    Returns:
        True if recovery completed successfully
    """
    if runner._session_store is None:
        return False

    # Get tx_hashes to poll
    tx_hashes = [tx.tx_hash for tx in session.transactions if tx.tx_hash]

    if not tx_hashes:
        logger.warning(
            f"Session {session.session_id} in {session.phase.value} but no tx_hashes found - marking as failed"
        )
        session.set_error("No transaction hashes found for submitted session")
        session.mark_complete(success=False)
        runner._session_store.save(session)
        return True

    logger.info(f"Polling {len(tx_hashes)} transactions for session {session.session_id}")

    # Poll for receipts via the submitter
    # Note: Session recovery currently only supports single-chain mode
    # Multi-chain recovery would require additional chain tracking in sessions
    if runner._is_multi_chain:
        logger.warning(
            f"Session recovery not yet supported in multi-chain mode. Marking session {session.session_id} as failed."
        )
        session.set_error("Session recovery not supported in multi-chain mode")
        session.mark_complete(success=False)
        runner._session_store.save(session)
        return True

    # Single-chain mode - get submitter from orchestrator
    single_chain_orch = cast(ExecutionOrchestrator, runner.execution_orchestrator)
    submitter = single_chain_orch.submitter

    try:
        # Poll with a shorter timeout for recovery (30s instead of 120s)
        receipts = await submitter.get_receipts(tx_hashes, timeout=30.0)

        # Update session with receipt results
        all_confirmed = True
        any_failed = False

        for receipt in receipts:
            tx_status = TransactionStatus.CONFIRMED if receipt.success else TransactionStatus.FAILED

            session.update_transaction(
                tx_hash=receipt.tx_hash,
                status=tx_status,
                gas_used=receipt.gas_used,
                block_number=receipt.block_number,
            )

            if receipt.success:
                logger.info(
                    f"Recovered tx {receipt.tx_hash}: CONFIRMED in block {receipt.block_number}",
                    extra={"session_id": session.session_id, "tx_hash": receipt.tx_hash},
                )
            else:
                logger.warning(
                    f"Recovered tx {receipt.tx_hash}: FAILED/REVERTED",
                    extra={"session_id": session.session_id, "tx_hash": receipt.tx_hash},
                )
                all_confirmed = False
                any_failed = True

        # Mark session complete based on results
        success = all_confirmed and not any_failed
        session.mark_complete(success=success)
        runner._session_store.save(session)

        logger.info(
            f"Session {session.session_id} recovery complete: success={success}",
            extra={"session_id": session.session_id},
        )

        # Update strategy state if recovery was successful
        if success:
            await update_recovered_state(runner, session)

        return True

    except TimeoutError:
        # Transaction not found in time - may have been dropped
        logger.warning(
            f"Timeout polling receipts for session {session.session_id} - transactions may have been dropped",
            extra={"session_id": session.session_id},
        )
        session.set_error("Timeout waiting for transaction receipts during recovery")
        session.mark_complete(success=False)
        runner._session_store.save(session)
        return True

    except Exception as e:
        logger.error(
            f"Error polling receipts for session {session.session_id}: {e}",
            extra={"session_id": session.session_id},
        )
        raise


async def recover_early_phase_session(runner: Any, session: ExecutionSession) -> bool:
    """Recover a session that was in PREPARING or SIGNING phase.

    These sessions haven't submitted any transactions on-chain,
    so it's safe to simply mark them as failed and let the
    strategy retry from scratch on the next iteration.

    Args:
        runner: StrategyRunner instance
        session: Session in early phase

    Returns:
        True if recovery completed
    """
    if runner._session_store is None:
        return False

    logger.info(
        f"Session {session.session_id} was in {session.phase.value} phase - "
        f"no on-chain activity, marking as failed for retry",
        extra={"session_id": session.session_id},
    )

    session.set_error(f"Session interrupted in {session.phase.value} phase - no on-chain activity, safe to retry")
    session.mark_complete(success=False)
    runner._session_store.save(session)

    return True


async def update_recovered_state(runner: Any, session: ExecutionSession) -> None:
    """Update strategy state after successful session recovery.

    This ensures the strategy's state reflects the recovered execution,
    preventing the strategy from retrying already-completed actions.

    Args:
        runner: StrategyRunner instance
        session: Successfully recovered session
    """
    try:
        state = await runner.state_manager.load_state(session.strategy_id)
        # GatewayStateManager returns None instead of raising StateNotFoundError
        if state is None:
            logger.debug(f"No state found for {session.strategy_id} during recovery marking")
            return

        # Record the recovered session in state
        recovered_sessions = state.state.get("recovered_sessions", [])
        recovered_sessions.append(
            {
                "session_id": session.session_id,
                "intent_id": session.intent_id,
                "recovered_at": datetime.now(UTC).isoformat(),
                "transactions": [{"tx_hash": tx.tx_hash, "status": tx.status.value} for tx in session.transactions],
            }
        )
        state.state["recovered_sessions"] = recovered_sessions

        await runner.state_manager.save_state(state, expected_version=state.version)

        logger.debug(f"Updated state for strategy {session.strategy_id} with recovered session {session.session_id}")

    except Exception as e:
        logger.error(
            f"Failed to update state after session recovery: {e}",
            extra={"session_id": session.session_id},
        )


def is_duplicate_transaction(
    runner: Any,
    tx_hash: str | None = None,
    nonce: int | None = None,
    strategy_id: str | None = None,
) -> bool:
    """Check if a transaction would be a duplicate of a recovered session.

    This is used to prevent re-submitting transactions that were
    already submitted before a crash.

    Args:
        runner: StrategyRunner instance
        tx_hash: Transaction hash to check
        nonce: Transaction nonce to check
        strategy_id: Strategy ID for nonce check

    Returns:
        True if transaction would be a duplicate
    """
    if tx_hash and tx_hash in runner._recovered_tx_hashes:
        logger.warning(f"Transaction {tx_hash} was already recovered - skipping to prevent duplicate")
        return True

    if nonce is not None and strategy_id:
        recovered_nonces = runner._recovered_nonces.get(strategy_id, set())
        if nonce in recovered_nonces:
            logger.warning(
                f"Nonce {nonce} for strategy {strategy_id} was already used "
                f"in a recovered session - skipping to prevent duplicate"
            )
            return True

    return False


# -------------------------------------------------------------------------
# Execution progress management
# -------------------------------------------------------------------------


def compute_intents_hash(runner: Any, intents: list) -> str:
    """Compute a hash of intents to detect if they changed.

    Args:
        runner: StrategyRunner instance (unused but kept for delegation pattern consistency)
        intents: List of intents to hash

    Returns:
        SHA256 hash of serialized intents
    """
    # Serialize intents to JSON-like string
    serialized = []
    for intent in intents:
        serialized.append(intent.serialize() if hasattr(intent, "serialize") else str(intent))
    intent_str = json.dumps(serialized, sort_keys=True, default=str)
    return hashlib.sha256(intent_str.encode()).hexdigest()[:16]


async def load_execution_progress(runner: Any, strategy_id: str) -> ExecutionProgress | None:
    """Load execution progress from persisted state.

    Args:
        runner: StrategyRunner instance
        strategy_id: Strategy identifier

    Returns:
        ExecutionProgress if found, None otherwise
    """
    try:
        state = await runner.state_manager.load_state(strategy_id)
        # GatewayStateManager returns None instead of raising StateNotFoundError
        if state is None:
            return None
        progress_data = state.state.get("execution_progress")
        if progress_data:
            return ExecutionProgress.from_dict(progress_data)
    except Exception as e:
        logger.debug(f"No execution progress found for {strategy_id}: {e}")
    return None


async def save_execution_progress(runner: Any, strategy_id: str, progress: ExecutionProgress) -> None:
    """Save execution progress to persisted state.

    Args:
        runner: StrategyRunner instance
        strategy_id: Strategy identifier
        progress: Execution progress to save
    """
    try:
        # Try to load existing state, create if it doesn't exist
        try:
            state = await runner.state_manager.load_state(strategy_id)
            # GatewayStateManager returns None instead of raising StateNotFoundError
            if state is None:
                raise StateNotFoundError(strategy_id)
            expected_version = state.version
        except StateNotFoundError:
            # Create initial state for this strategy
            state = StateData(
                strategy_id=strategy_id,
                version=1,
                state={},
            )
            expected_version = None  # No version check for new state
            logger.debug(f"Creating initial state for {strategy_id}")

        progress.last_updated = datetime.now(UTC)
        state.state["execution_progress"] = progress.to_dict()
        await runner.state_manager.save_state(state, expected_version=expected_version)
        logger.debug(
            f"Saved execution progress for {strategy_id}: "
            f"step {progress.completed_step_index + 1}/{progress.total_steps}"
        )
    except Exception as e:
        logger.error(f"Failed to save execution progress: {e}")


async def clear_execution_progress(runner: Any, strategy_id: str) -> None:
    """Clear execution progress from state (after completion or abort).

    Args:
        runner: StrategyRunner instance
        strategy_id: Strategy identifier
    """
    try:
        state = await runner.state_manager.load_state(strategy_id)
        # GatewayStateManager returns None instead of raising StateNotFoundError
        if state is None:
            return
        if "execution_progress" in state.state:
            del state.state["execution_progress"]
            await runner.state_manager.save_state(state, expected_version=state.version)
            logger.debug(f"Cleared execution progress for {strategy_id}")
    except Exception as e:
        logger.debug(f"Could not clear execution progress: {e}")
