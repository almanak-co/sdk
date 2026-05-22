"""Phase helpers for :meth:`IntentExecutionService.execute_intent` (Phase 6A.3).

This module contains phase-level helpers extracted from the retry loop body
of ``IntentExecutionService.execute_intent`` to reduce cyclomatic complexity
and isolate responsibilities. Every helper preserves the EXACT original
behavior captured by the unit tests in
``tests/unit/runner/test_inner_runner.py``.

Design notes
------------
* Helpers are module-level functions (not methods) that take the service
  instance explicitly. This keeps them free of ``self.`` noise inside
  ``execute_intent`` while still respecting the service's private state.
* Every sadflow callback call, log level (warning vs. debug), sleep, and
  the "Retrying in %.1fs..." debug message are reproduced byte-for-byte
  from the pre-extraction body.
* ``_inner_runner_helpers`` does NOT import :class:`IntentExecutionService`
  at module load time — it uses ``TYPE_CHECKING`` to avoid a circular
  import while still offering typed signatures.
* Each phase helper returns a :class:`PhaseOutcome` discriminated record
  that tells the caller what to do next: continue the retry loop, break
  out of it, return immediately with a built result, or proceed with the
  next phase of the current attempt.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from .inner_runner import EnrichedExecutionResult, IntentExecutionService

logger = logging.getLogger("almanak.framework.runner.inner_runner")


# =============================================================================
# Phase-outcome discriminator
# =============================================================================


@dataclass
class PhaseOutcome:
    """Control-flow signal returned by a phase helper.

    ``kind`` values:
      * ``"proceed"`` — phase succeeded; ``payload`` carries the response for
        the caller to feed into the next phase.
      * ``"continue"`` — phase failed retryably; caller should ``continue`` the
        retry loop (the helper has already scheduled the retry sleep).
      * ``"break"`` — phase failed non-retryably; caller should ``break`` out
        of the retry loop (last_error is set on the service-local variable
        by the caller via ``last_error``).
      * ``"return"`` — helper produced a terminal :class:`EnrichedExecutionResult`
        that the caller must return verbatim (e.g., the broadcast-but-failed
        guard in the execute-failure path).
    """

    kind: Literal["proceed", "continue", "break", "return"]
    payload: Any = None  # compile_resp / exec_resp / EnrichedExecutionResult
    last_error: str | None = None


# =============================================================================
# Shared retry-failure helper
# =============================================================================


async def _handle_retryable_rpc_failure(
    service: IntentExecutionService,
    *,
    intent_type: str,
    intent_params: dict[str, Any],
    chain: str,
    tool_name: str,
    error_message: str,
    raw_error_text: str,
    attempt: int,
    max_retries: int,
    log_prefix: str,
    retry_debug_log: bool,
) -> PhaseOutcome:
    """Shared handler for an RPC-level failure (compile or execute).

    Mirrors the symmetric ``except Exception`` blocks of the original
    ``execute_intent`` body:

    * Log at warning level when the attempt is final, debug otherwise.
    * If the error is non-retryable, fire sadflow with ``is_final=True``
      and signal ``break``.
    * Otherwise fire sadflow with ``is_final=is_last``, sleep if there
      are retries remaining, and signal ``continue``.

    ``retry_debug_log`` toggles the ``"Retrying in %.1fs..."`` debug line:
    the original code emitted it for the compile-RPC exception and the
    execute-response-failure paths, but not for the compile-response and
    execute-RPC paths. We preserve that asymmetry exactly.
    """
    attempts = attempt + 1
    is_last = attempt == max_retries or not _is_retryable_local(raw_error_text)
    log_fn = logger.warning if is_last else logger.debug
    log_fn(
        "%s (attempt %d/%d): %s",
        log_prefix,
        attempts,
        max_retries + 1,
        error_message,
    )
    if not _is_retryable_local(raw_error_text):
        service._fire_sadflow(intent_type, intent_params, error_message, attempt, max_retries, True, chain, tool_name)
        return PhaseOutcome(kind="break", last_error=error_message)

    service._fire_sadflow(
        intent_type,
        intent_params,
        error_message,
        attempt,
        max_retries,
        is_last,
        chain,
        tool_name,
    )
    if attempt < max_retries:
        delay = service._retry_policy.delay_for_attempt(attempt)
        if retry_debug_log:
            logger.debug("Retrying in %.1fs...", delay)
        await asyncio.sleep(delay)
    return PhaseOutcome(kind="continue", last_error=error_message)


async def _handle_compile_response_failure(
    service: IntentExecutionService,
    *,
    intent_type: str,
    intent_params: dict[str, Any],
    chain: str,
    tool_name: str,
    compile_error_text: str,
    attempt: int,
    max_retries: int,
) -> PhaseOutcome:
    """Handle ``compile_resp.success == False`` — matches the original's
    subtle second ``is_last`` reassignment pre-sadflow.

    The original code computes ``is_last`` twice — once that mixes in
    non-retryability for the log decision, and a second time that is
    *only* ``attempt == max_retries`` for the sadflow ``is_final`` flag
    on the retryable branch. We reproduce that ordering faithfully.
    """
    attempts = attempt + 1
    last_error = f"Compilation failed: {compile_error_text}"
    is_last = attempt == max_retries or not _is_retryable_local(compile_error_text)
    log_fn = logger.warning if is_last else logger.debug
    log_fn(
        "Intent compilation failed (attempt %d/%d): %s",
        attempts,
        max_retries + 1,
        last_error,
    )
    if not _is_retryable_local(compile_error_text):
        service._fire_sadflow(intent_type, intent_params, last_error, attempt, max_retries, True, chain, tool_name)
        return PhaseOutcome(kind="break", last_error=last_error)

    is_last = attempt == max_retries
    service._fire_sadflow(intent_type, intent_params, last_error, attempt, max_retries, is_last, chain, tool_name)
    if attempt < max_retries:
        delay = service._retry_policy.delay_for_attempt(attempt)
        await asyncio.sleep(delay)
    return PhaseOutcome(kind="continue", last_error=last_error)


# =============================================================================
# Phase helpers: compile / execute / success / execute-failure
# =============================================================================


async def compile_intent_phase(
    service: IntentExecutionService,
    *,
    intent_type: str,
    intent_params: dict[str, Any],
    chain: str,
    wallet: str,
    price_map: dict[str, str],
    tool_name: str,
    attempt: int,
    max_retries: int,
) -> PhaseOutcome:
    """Phase 1 — call ``execution.CompileIntent`` and classify the outcome.

    Success: ``PhaseOutcome(kind="proceed", payload=compile_resp)``.
    Failure: delegates to the shared retry-failure helper and returns its
    ``continue`` / ``break`` signal.
    """
    from almanak.gateway.proto import gateway_pb2

    try:
        compile_resp = service._client.execution.CompileIntent(
            gateway_pb2.CompileIntentRequest(
                intent_type=intent_type,
                intent_data=json.dumps(intent_params).encode(),
                chain=chain,
                wallet_address=wallet,
                price_map=price_map,
            )
        )
    except Exception as e:
        return await _handle_retryable_rpc_failure(
            service,
            intent_type=intent_type,
            intent_params=intent_params,
            chain=chain,
            tool_name=tool_name,
            error_message=f"Compilation RPC error: {e}",
            raw_error_text=str(e),
            attempt=attempt,
            max_retries=max_retries,
            log_prefix="Intent compilation failed",
            retry_debug_log=True,
        )

    if not compile_resp.success:
        return await _handle_compile_response_failure(
            service,
            intent_type=intent_type,
            intent_params=intent_params,
            chain=chain,
            tool_name=tool_name,
            compile_error_text=compile_resp.error or "",
            attempt=attempt,
            max_retries=max_retries,
        )

    return PhaseOutcome(kind="proceed", payload=compile_resp)


async def execute_bundle_phase(
    service: IntentExecutionService,
    *,
    compile_resp: Any,
    intent_type: str,
    intent_params: dict[str, Any],
    chain: str,
    wallet: str,
    dry_run: bool,
    simulate: bool,
    tool_name: str,
    attempt: int,
    max_retries: int,
) -> PhaseOutcome:
    """Phase 2 — call ``execution.Execute`` and classify the RPC outcome.

    Only the RPC-level exception path is handled here — the response-level
    (``exec_resp.success == False``) path is handled by
    :func:`handle_execution_failure` from the caller so that the success
    branch stays linear and readable.
    """
    from almanak.gateway.proto import gateway_pb2

    try:
        exec_resp = service._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=compile_resp.action_bundle,
                dry_run=dry_run,
                simulation_enabled=simulate,
                deployment_id=service._deployment_id,
                chain=chain,
                wallet_address=wallet,
            )
        )
    except Exception as e:
        return await _handle_retryable_rpc_failure(
            service,
            intent_type=intent_type,
            intent_params=intent_params,
            chain=chain,
            tool_name=tool_name,
            error_message=f"Execution RPC error: {e}",
            raw_error_text=str(e),
            attempt=attempt,
            max_retries=max_retries,
            log_prefix="Intent execution failed",
            retry_debug_log=False,
        )

    return PhaseOutcome(kind="proceed", payload=exec_resp)


def build_success_result(
    service: IntentExecutionService,
    *,
    exec_resp: Any,
    compile_resp: Any,
    intent_type: str,
    intent_params: dict[str, Any],
    chain: str,
    wallet: str,
    dry_run: bool,
    protocol: str | None,
    tool_name: str,
    attempts: int,
) -> EnrichedExecutionResult:
    """Phase 3 — build the :class:`EnrichedExecutionResult` on the success
    branch (``exec_resp.success or dry_run``) and enrich it.

    Mirrors the original success block byte-for-byte:
      * Build the base result (including raw_receipts passthrough).
      * On a real success, call ``_enrich_result``.
      * On a dry-run swap, call ``_enrich_dry_run_swap``.
      * Emit the "succeeded after N attempts" info log when ``attempts > 1``.
    """
    from .inner_runner import EnrichedExecutionResult

    tx_hashes = list(exec_resp.tx_hashes) if exec_resp.tx_hashes else []
    result = EnrichedExecutionResult(
        success=exec_resp.success,
        tx_hashes=tx_hashes,
        error=None if exec_resp.success else (exec_resp.error or "Unknown execution error"),
        attempts=attempts,
        dry_run=dry_run,
        raw_receipts=getattr(exec_resp, "receipts", None),
    )

    if exec_resp.success and not dry_run:
        service._enrich_result(
            result,
            intent_type,
            intent_params,
            chain,
            wallet,
            protocol,
            compile_resp=compile_resp,
        )
    elif dry_run and intent_type.lower() == "swap":
        service._enrich_dry_run_swap(result, compile_resp, intent_params)

    if attempts > 1:
        logger.info(
            "Intent %s succeeded after %d attempts",
            tool_name or intent_type,
            attempts,
        )

    return result


async def handle_execution_failure(
    service: IntentExecutionService,
    *,
    exec_resp: Any,
    intent_type: str,
    intent_params: dict[str, Any],
    chain: str,
    tool_name: str,
    attempt: int,
    max_retries: int,
    attempts: int,
) -> PhaseOutcome:
    """Phase 4 — classify and route an ``exec_resp.success == False`` outcome.

    Handles:
      * The broadcast-but-failed short-circuit (``exec_resp.tx_hashes`` present):
        fires sadflow with ``is_final=True`` and returns a terminal
        :class:`EnrichedExecutionResult` via ``kind="return"``. This is
        critical — retrying a broadcast tx could duplicate on-chain actions.
      * Non-retryable error: fires sadflow with ``is_final=True`` and
        signals ``break``.
      * Retryable error on a non-final attempt: fires sadflow with
        ``is_final=(attempt == max_retries)``, sleeps, and signals
        ``continue``. The "Retrying in %.1fs..." debug log is preserved.
    """
    from .inner_runner import EnrichedExecutionResult

    last_error = exec_resp.error or "Unknown execution error"
    is_final_attempt = attempt == max_retries or not _is_retryable_local(last_error) or bool(exec_resp.tx_hashes)
    log_fn = logger.warning if is_final_attempt else logger.debug
    log_fn(
        "Intent execution failed (attempt %d/%d): %s",
        attempts,
        max_retries + 1,
        last_error,
    )

    # Never retry if the transaction was already broadcast (tx_hashes present).
    # Retrying could duplicate on-chain actions (e.g., double swap).
    if exec_resp.tx_hashes:
        logger.warning(
            "Transaction was broadcast (tx_hashes=%s) but execution reported failure. "
            "Skipping retry to avoid duplicate on-chain actions.",
            list(exec_resp.tx_hashes),
        )
        service._fire_sadflow(intent_type, intent_params, last_error, attempt, max_retries, True, chain, tool_name)
        return PhaseOutcome(
            kind="return",
            payload=EnrichedExecutionResult(
                success=False,
                tx_hashes=list(exec_resp.tx_hashes),
                error=last_error,
                attempts=attempts,
            ),
            last_error=last_error,
        )

    if not _is_retryable_local(last_error):
        service._fire_sadflow(intent_type, intent_params, last_error, attempt, max_retries, True, chain, tool_name)
        return PhaseOutcome(kind="break", last_error=last_error)

    service._fire_sadflow(
        intent_type,
        intent_params,
        last_error,
        attempt,
        max_retries,
        attempt == max_retries,
        chain,
        tool_name,
    )
    if attempt < max_retries:
        delay = service._retry_policy.delay_for_attempt(attempt)
        logger.debug("Retrying in %.1fs...", delay)
        await asyncio.sleep(delay)
    return PhaseOutcome(kind="continue", last_error=last_error)


# =============================================================================
# Private re-export of _is_retryable to avoid a circular-import cycle
# =============================================================================


def _is_retryable_local(error_msg: str) -> bool:
    """Thin indirection so we can keep the import lazy.

    We import :func:`_is_retryable` from :mod:`inner_runner` at call-time
    rather than at module load to stay clear of a circular import (the
    helper module is loaded by ``inner_runner`` itself).
    """
    from .inner_runner import _is_retryable

    return _is_retryable(error_msg)
