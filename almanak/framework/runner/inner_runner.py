"""Shared Intent Execution Service for production-hardened DeFi execution.

This module extracts the core compile-execute-enrich-retry pipeline into a
reusable service that both StrategyRunner and ToolExecutor can use.

The IntentExecutionService wraps gateway gRPC calls with:
- Retry logic with exponential backoff (RetryConfig)
- Result enrichment via ResultEnricher (position_id, swap_amounts, etc.)
- Sadflow hooks for failure handling
- Timeline event emission for audit trail

StrategyRunner already has these features built into _execute_single_chain().
ToolExecutor previously bypassed all of them. This service closes that gap.

Example:
    service = IntentExecutionService(
        gateway_client=client,
        chain="arbitrum",
        wallet_address="0x...",
        strategy_id="my-strategy",
    )

    result = await service.execute_intent(
        intent_type="swap",
        intent_params={"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
    )
    # result.enriched_data contains position_id, swap_amounts, etc.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class RetryPolicy:
    """Retry configuration for intent execution.

    Mirrors the RetryConfig from IntentStateMachine but works at the
    gateway gRPC level rather than the local orchestrator level.
    """

    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate delay for a retry attempt using exponential backoff."""
        delay = self.initial_delay_seconds * (self.backoff_multiplier**attempt)
        return min(delay, self.max_delay_seconds)


@dataclass
class SadflowEvent:
    """Information about an execution failure for sadflow hooks.

    Provides context to the on_sadflow callback so callers can
    implement custom failure handling (alerts, logging, state cleanup).
    """

    intent_type: str
    intent_params: dict[str, Any]
    error: str
    attempt: int
    max_retries: int
    is_final: bool  # True when all retries exhausted
    chain: str = ""
    tool_name: str = ""


@dataclass
class EnrichedExecutionResult:
    """Result of intent execution through the shared service.

    Contains both the raw gateway response data and enriched fields
    extracted by ResultEnricher.
    """

    success: bool
    tx_hashes: list[str] = field(default_factory=list)
    error: str | None = None
    attempts: int = 1
    dry_run: bool = False

    # Raw gateway response data
    raw_receipts: bytes | str | None = None

    # Enriched data (populated by ResultEnricher)
    position_id: int | str | None = None
    swap_amounts: Any = None  # SwapAmounts dataclass
    lp_close_data: Any = None  # LPCloseData dataclass
    extracted_data: dict[str, Any] = field(default_factory=dict)
    extraction_warnings: list[str] = field(default_factory=list)

    @property
    def tx_hash(self) -> str | None:
        """First transaction hash for convenience."""
        return self.tx_hashes[0] if self.tx_hashes else None


# Non-retryable error patterns: retrying these will never succeed
_NON_RETRYABLE_PATTERNS = frozenset(
    {
        "insufficient funds",
        "insufficient balance",
        "exceeds allowance",
        "nonce too low",
        "already known",
        "replacement underpriced",
        "invalid opcode",
        "out of gas",
        "execution reverted",
        "invalid selector",
        # Auth errors are never transient — retrying wastes time
        "unauthenticated",
        "no authentication token",
        "permission denied",
        "permission_denied",
    }
)


def _parse_int(val: Any) -> int:
    """Parse a value that may be hex string, decimal string, or int to int."""
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        val = val.strip()
        if val.startswith("0x") or val.startswith("0X"):
            return int(val, 16)
        if val:
            return int(val)
        return 0
    return 0


def _is_retryable(error_msg: str) -> bool:
    """Check if an error is retryable.

    Non-retryable errors are those that will never succeed no matter how
    many times we retry (e.g., insufficient funds, invalid opcodes).
    """
    lower = error_msg.lower()
    return not any(pattern in lower for pattern in _NON_RETRYABLE_PATTERNS)


# =============================================================================
# Intent Execution Service
# =============================================================================


class IntentExecutionService:
    """Shared service for production-hardened intent execution.

    Wraps gateway gRPC compile+execute calls with:
    - Retry logic with exponential backoff
    - Result enrichment (position_id, swap_amounts, etc.)
    - Sadflow hooks for failure handling
    - Non-retryable error detection

    Both StrategyRunner (via gateway path) and ToolExecutor delegate
    their intent execution to this service.

    Args:
        gateway_client: Connected GatewayClient instance.
        chain: Target blockchain (e.g., "arbitrum").
        wallet_address: Wallet address for execution.
        strategy_id: Strategy identifier for audit trail.
        retry_policy: Retry configuration. Uses safe defaults if not provided.
        on_sadflow: Optional callback invoked on execution failures.
            Receives a SadflowEvent with failure context.
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        *,
        chain: str = "arbitrum",
        wallet_address: str = "",
        strategy_id: str = "",
        retry_policy: RetryPolicy | None = None,
        on_sadflow: Any | None = None,
    ) -> None:
        self._client = gateway_client
        self._chain = chain
        self._wallet_address = wallet_address
        self._strategy_id = strategy_id
        self._retry_policy = retry_policy or RetryPolicy()
        self._on_sadflow = on_sadflow

    def _fetch_prices_for_intent(self, intent_type: str, intent_params: dict[str, Any]) -> dict[str, str]:
        """Fetch token prices from the gateway for intent compilation.

        The execution service requires real prices on mainnet (VIB-523) to
        compute accurate slippage amounts. This extracts token symbols from
        the intent params and queries the gateway MarketService.
        """
        # Extract token symbols from intent params using the shared utility
        # (handles all token fields + callback_intents recursion)
        from almanak.framework.runner.token_extraction import extract_token_symbols
        from almanak.gateway.proto import gateway_pb2

        symbols = set(extract_token_symbols(intent_params))

        price_map: dict[str, str] = {}
        for symbol in symbols:
            try:
                resp = self._client.market.GetPrice(gateway_pb2.PriceRequest(token=symbol, quote="USD"))
                price_val = float(resp.price)
                if price_val > 0:
                    price_map[symbol] = str(resp.price)
            except Exception as exc:
                logger.debug("Could not fetch price for %s: %s", symbol, exc)

        if price_map:
            logger.debug(
                "Fetched %d prices for %s compilation: %s", len(price_map), intent_type, list(price_map.keys())
            )
        return price_map

    async def execute_intent(
        self,
        intent_type: str,
        intent_params: dict[str, Any],
        *,
        chain: str | None = None,
        wallet_address: str | None = None,
        dry_run: bool = False,
        simulate: bool = True,
        tool_name: str = "",
        protocol: str | None = None,
    ) -> EnrichedExecutionResult:
        """Execute an intent through the full production pipeline.

        Compile -> Execute -> Enrich -> Retry on failure.

        Args:
            intent_type: Intent type string (e.g., "swap", "lp_open").
            intent_params: Intent parameters dict.
            chain: Override chain (defaults to service chain).
            wallet_address: Override wallet (defaults to service wallet).
            dry_run: If True, compile and simulate but don't execute on-chain.
            simulate: Whether to simulate before execution.
            tool_name: Tool name for logging and sadflow events.
            protocol: Protocol name for result enrichment (e.g., "uniswap_v3").

        Returns:
            EnrichedExecutionResult with enriched data from ResultEnricher.
        """
        from almanak.gateway.proto import gateway_pb2

        effective_chain = chain or self._chain
        effective_wallet = wallet_address or self._wallet_address

        # Fetch real prices for compilation (required on mainnet to avoid
        # placeholder-price rejections -- VIB-523).
        price_map = self._fetch_prices_for_intent(intent_type, intent_params)

        last_error: str | None = None
        max_retries = self._retry_policy.max_retries
        attempts = 0

        for attempt in range(max_retries + 1):
            attempts = attempt + 1

            # Step 1: Compile intent
            try:
                compile_resp = self._client.execution.CompileIntent(
                    gateway_pb2.CompileIntentRequest(
                        intent_type=intent_type,
                        intent_data=json.dumps(intent_params).encode(),
                        chain=effective_chain,
                        wallet_address=effective_wallet,
                        price_map=price_map,
                    )
                )
            except Exception as e:
                last_error = f"Compilation RPC error: {e}"
                is_last_rpc = attempt == max_retries or not _is_retryable(str(e))
                log_fn = logger.warning if is_last_rpc else logger.debug
                log_fn(
                    "Intent compilation failed (attempt %d/%d): %s",
                    attempts,
                    max_retries + 1,
                    last_error,
                )
                if not _is_retryable(str(e)):
                    self._fire_sadflow(
                        intent_type, intent_params, last_error, attempt, max_retries, True, effective_chain, tool_name
                    )
                    break
                self._fire_sadflow(
                    intent_type,
                    intent_params,
                    last_error,
                    attempt,
                    max_retries,
                    is_last_rpc,
                    effective_chain,
                    tool_name,
                )
                if attempt < max_retries:
                    delay = self._retry_policy.delay_for_attempt(attempt)
                    logger.debug("Retrying in %.1fs...", delay)
                    await asyncio.sleep(delay)
                continue

            if not compile_resp.success:
                last_error = f"Compilation failed: {compile_resp.error}"
                is_last = attempt == max_retries or not _is_retryable(compile_resp.error or "")
                log_fn = logger.warning if is_last else logger.debug
                log_fn(
                    "Intent compilation failed (attempt %d/%d): %s",
                    attempts,
                    max_retries + 1,
                    last_error,
                )
                if not _is_retryable(compile_resp.error or ""):
                    self._fire_sadflow(
                        intent_type, intent_params, last_error, attempt, max_retries, True, effective_chain, tool_name
                    )
                    break
                is_last = attempt == max_retries
                self._fire_sadflow(
                    intent_type, intent_params, last_error, attempt, max_retries, is_last, effective_chain, tool_name
                )
                if attempt < max_retries:
                    delay = self._retry_policy.delay_for_attempt(attempt)
                    await asyncio.sleep(delay)
                continue

            # Step 2: Execute
            try:
                exec_resp = self._client.execution.Execute(
                    gateway_pb2.ExecuteRequest(
                        action_bundle=compile_resp.action_bundle,
                        dry_run=dry_run,
                        simulation_enabled=simulate,
                        strategy_id=self._strategy_id,
                        chain=effective_chain,
                        wallet_address=effective_wallet,
                    )
                )
            except Exception as e:
                last_error = f"Execution RPC error: {e}"
                is_last_exec = attempt == max_retries or not _is_retryable(str(e))
                log_fn = logger.warning if is_last_exec else logger.debug
                log_fn(
                    "Intent execution failed (attempt %d/%d): %s",
                    attempts,
                    max_retries + 1,
                    last_error,
                )
                if not _is_retryable(str(e)):
                    self._fire_sadflow(
                        intent_type, intent_params, last_error, attempt, max_retries, True, effective_chain, tool_name
                    )
                    break
                self._fire_sadflow(
                    intent_type,
                    intent_params,
                    last_error,
                    attempt,
                    max_retries,
                    is_last_exec,
                    effective_chain,
                    tool_name,
                )
                if attempt < max_retries:
                    delay = self._retry_policy.delay_for_attempt(attempt)
                    await asyncio.sleep(delay)
                continue

            if exec_resp.success or dry_run:
                # Success! Build result and enrich.
                tx_hashes = list(exec_resp.tx_hashes) if exec_resp.tx_hashes else []
                result = EnrichedExecutionResult(
                    success=exec_resp.success,
                    tx_hashes=tx_hashes,
                    error=None if exec_resp.success else (exec_resp.error or "Unknown execution error"),
                    attempts=attempts,
                    dry_run=dry_run,
                    raw_receipts=getattr(exec_resp, "receipts", None),
                )

                # Step 3: Enrich result
                if exec_resp.success and not dry_run:
                    self._enrich_result(result, intent_type, intent_params, effective_chain, effective_wallet, protocol)
                elif dry_run and intent_type.lower() == "swap":
                    self._enrich_dry_run_swap(result, compile_resp, intent_params)

                if attempts > 1:
                    logger.info(
                        "Intent %s succeeded after %d attempts",
                        tool_name or intent_type,
                        attempts,
                    )

                return result

            # Execution failed
            last_error = exec_resp.error or "Unknown execution error"
            is_final_attempt = attempt == max_retries or not _is_retryable(last_error) or bool(exec_resp.tx_hashes)
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
                self._fire_sadflow(
                    intent_type, intent_params, last_error, attempt, max_retries, True, effective_chain, tool_name
                )
                return EnrichedExecutionResult(
                    success=False,
                    tx_hashes=list(exec_resp.tx_hashes),
                    error=last_error,
                    attempts=attempts,
                )

            if not _is_retryable(last_error):
                self._fire_sadflow(
                    intent_type, intent_params, last_error, attempt, max_retries, True, effective_chain, tool_name
                )
                break

            self._fire_sadflow(
                intent_type,
                intent_params,
                last_error,
                attempt,
                max_retries,
                attempt == max_retries,
                effective_chain,
                tool_name,
            )
            if attempt < max_retries:
                delay = self._retry_policy.delay_for_attempt(attempt)
                logger.debug("Retrying in %.1fs...", delay)
                await asyncio.sleep(delay)

        # All retries exhausted
        return EnrichedExecutionResult(
            success=False,
            error=last_error or "Unknown error after retries exhausted",
            attempts=attempts,
        )

    def _fire_sadflow(
        self,
        intent_type: str,
        intent_params: dict[str, Any],
        error: str,
        attempt: int,
        max_retries: int,
        is_final: bool,
        chain: str,
        tool_name: str,
    ) -> None:
        """Fire sadflow callback if configured."""
        if self._on_sadflow is None:
            return
        try:
            event = SadflowEvent(
                intent_type=intent_type,
                intent_params=intent_params,
                error=error,
                attempt=attempt,
                max_retries=max_retries,
                is_final=is_final,
                chain=chain,
                tool_name=tool_name,
            )
            self._on_sadflow(event)
        except Exception as e:
            logger.debug("Sadflow callback failed (non-fatal): %s", e)

    def _enrich_result(
        self,
        result: EnrichedExecutionResult,
        intent_type: str,
        intent_params: dict[str, Any],
        chain: str,
        wallet_address: str,
        protocol: str | None,
    ) -> None:
        """Run ResultEnricher on the execution result.

        Parses receipts from the gateway response and extracts intent-specific
        data (position_id, swap_amounts, lp_close_data, etc.).

        This is the same enrichment that StrategyRunner applies in
        _execute_single_chain(), now available to ToolExecutor.
        """
        try:
            from almanak.framework.execution.orchestrator import (
                ExecutionContext,
                ExecutionPhase,
                ExecutionResult,
                TransactionResult,
            )
            from almanak.framework.execution.result_enricher import ResultEnricher

            # Build a minimal ExecutionResult that ResultEnricher can work with
            receipts = self._parse_gateway_receipts(result.raw_receipts)
            if not receipts:
                logger.debug("No receipts to enrich for %s", intent_type)
                return

            # Build TransactionResult objects from receipts
            tx_results = []
            for i, receipt_dict in enumerate(receipts):
                # Create a mock TransactionReceipt-like object that ResultEnricher can consume
                tx_hash = result.tx_hashes[i] if i < len(result.tx_hashes) else ""
                raw_gas = receipt_dict.get("gasUsed", receipt_dict.get("gas_used", 0))
                gas_used = _parse_int(raw_gas)
                tx_result = TransactionResult(
                    tx_hash=tx_hash,
                    success=True,
                    receipt=_DictReceipt(receipt_dict),  # type: ignore[arg-type]  # adapter satisfies duck-typing
                    gas_used=gas_used,
                )
                tx_results.append(tx_result)

            # Build ExecutionResult for the enricher
            exec_result = ExecutionResult(
                success=True,
                phase=ExecutionPhase.COMPLETE,
                transaction_results=tx_results,
                total_gas_used=sum(tr.gas_used for tr in tx_results),
            )

            # Build ExecutionContext
            effective_protocol = protocol or self._infer_protocol(intent_type, intent_params)
            context = ExecutionContext(
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=wallet_address,
                protocol=effective_protocol,
            )

            # Build a minimal intent-like object for the enricher
            intent_obj = _MinimalIntent(intent_type, intent_params)

            # Run enrichment
            enricher = ResultEnricher()
            enriched = enricher.enrich(exec_result, intent_obj, context)

            # Transfer enriched data to our result
            result.position_id = enriched.position_id
            result.swap_amounts = enriched.swap_amounts
            result.lp_close_data = enriched.lp_close_data
            result.extracted_data = enriched.extracted_data
            result.extraction_warnings = enriched.extraction_warnings

            if enriched.position_id or enriched.swap_amounts or enriched.lp_close_data:
                logger.info(
                    "Enriched %s result: position_id=%s, swap_amounts=%s, lp_close_data=%s",
                    intent_type,
                    enriched.position_id is not None,
                    enriched.swap_amounts is not None,
                    enriched.lp_close_data is not None,
                )

        except Exception as e:
            logger.warning("Result enrichment failed for %s (non-fatal): %s", intent_type, e)
            result.extraction_warnings.append(f"Enrichment failed: {e}")

    def _enrich_dry_run_swap(
        self,
        result: EnrichedExecutionResult,
        compile_resp: Any,
        intent_params: dict[str, Any],
    ) -> None:
        """Populate swap_amounts from compilation metadata for dry-run responses.

        In dry-run mode, no real execution occurs so there are no receipts to parse.
        However, the compiler stores quote data in the ActionBundle metadata
        (amount_in, min_amount_out, from_token, to_token). We use this to give
        users a useful preview of the expected swap output.
        """
        try:
            from decimal import Decimal

            from almanak.framework.execution.extracted_data import SwapAmounts

            bundle_bytes = getattr(compile_resp, "action_bundle", None)
            if not bundle_bytes:
                return

            if isinstance(bundle_bytes, bytes):
                bundle_data = json.loads(bundle_bytes.decode("utf-8"))
            elif isinstance(bundle_bytes, str):
                bundle_data = json.loads(bundle_bytes)
            else:
                return

            metadata = bundle_data.get("metadata", {})
            amount_in_str = metadata.get("amount_in")
            min_amount_out_str = metadata.get("min_amount_out")
            if not amount_in_str or not min_amount_out_str:
                return

            amount_in = int(amount_in_str)
            min_amount_out = int(min_amount_out_str)
            if amount_in <= 0 or min_amount_out <= 0:
                return

            # Extract token decimals from metadata
            from_token_info = metadata.get("from_token", {})
            to_token_info = metadata.get("to_token", {})
            from_decimals = from_token_info.get("decimals")
            to_decimals = to_token_info.get("decimals")
            if from_decimals is None or to_decimals is None:
                logger.debug("Dry-run swap enrichment skipped: missing token decimals in metadata")
                return
            from_symbol = from_token_info.get("symbol", intent_params.get("from_token", ""))
            to_symbol = to_token_info.get("symbol", intent_params.get("to_token", ""))

            # Compute human-readable amounts
            amount_in_decimal = Decimal(amount_in) / Decimal(10**from_decimals)
            # min_amount_out is amount_out * (1 - slippage), so the expected amount_out
            # is approximately min_amount_out / (1 - slippage). For the quoted amount,
            # check if we have quoted_candidates with the best amount_out.
            best_amount_out = min_amount_out  # fallback to min
            # If quoting was used, extract the actual quoted amount from candidates
            # The compiler stores quoted_candidates in fee_tier_candidates when quoting
            # was the source, but actually the data is lost through gRPC serialization.
            # Use the slippage to back-calculate the expected amount_out.
            slippage_str = metadata.get("slippage")
            if slippage_str:
                slippage = Decimal(slippage_str)
                if slippage < 1:  # e.g., 0.005 = 0.5%
                    # min_amount_out = expected * (1 - slippage)
                    # expected = min_amount_out / (1 - slippage)
                    best_amount_out = int(Decimal(min_amount_out) / (1 - slippage))

            amount_out_decimal = Decimal(best_amount_out) / Decimal(10**to_decimals)

            # Compute effective price
            effective_price = None
            if amount_in_decimal > 0:
                effective_price = amount_out_decimal / amount_in_decimal

            result.swap_amounts = SwapAmounts(
                amount_in=amount_in,
                amount_out=best_amount_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=None,  # Not known for dry-run estimates
                token_in=from_symbol,
                token_out=to_symbol,
            )
            logger.debug(
                "Dry-run swap quote: %s %s -> %s %s (price: %s)",
                amount_in_decimal,
                from_symbol,
                amount_out_decimal,
                to_symbol,
                effective_price,
            )
        except Exception as e:
            logger.debug("Dry-run swap enrichment failed (non-fatal): %s", e)

    def _parse_gateway_receipts(self, raw_receipts: bytes | str | None) -> list[dict[str, Any]]:
        """Parse receipts from gateway Execute response.

        The gateway returns receipts as a JSON-serialized bytes field.
        """
        if not raw_receipts:
            return []
        try:
            if isinstance(raw_receipts, bytes):
                data = json.loads(raw_receipts.decode("utf-8"))
            elif isinstance(raw_receipts, str):
                data = json.loads(raw_receipts)
            else:
                return []

            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.debug("Failed to parse gateway receipts: %s", e)
            return []

    def _infer_protocol(self, intent_type: str, intent_params: dict[str, Any]) -> str | None:
        """Infer protocol from intent params if not explicitly provided."""
        protocol = intent_params.get("protocol")
        if protocol:
            return protocol
        # Default protocol by intent type
        _DEFAULT_PROTOCOLS = {
            "swap": "enso",
            "lp_open": "uniswap_v3",
            "lp_close": "uniswap_v3",
            "borrow": "aave_v3",
            "repay": "aave_v3",
            "supply": "aave_v3",
            "withdraw": "aave_v3",
        }
        return _DEFAULT_PROTOCOLS.get(intent_type.lower())


# =============================================================================
# Helper classes for ResultEnricher compatibility
# =============================================================================


class _DictReceipt:
    """Wrapper that makes a dict look like a TransactionReceipt for ResultEnricher.

    ResultEnricher calls receipt.to_dict() or accesses receipt.logs.
    This wrapper supports both patterns.
    """

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def to_dict(self) -> dict[str, Any]:
        return self._data

    @property
    def logs(self) -> list:
        return self._data.get("logs", [])

    @property
    def success(self) -> bool:
        status = self._data.get("status")
        if isinstance(status, str):
            return status == "0x1" or status == "1"
        if isinstance(status, int):
            return status == 1
        return True  # Assume success if status not present

    @property
    def tx_hash(self) -> str:
        return self._data.get("transactionHash", self._data.get("tx_hash", ""))

    @property
    def gas_used(self) -> int:
        val = self._data.get("gasUsed", self._data.get("gas_used", 0))
        if isinstance(val, str):
            return int(val, 16) if val.startswith("0x") else int(val)
        return int(val)

    @property
    def gas_cost_wei(self) -> int:
        return 0  # Gateway doesn't always provide this


class _MinimalIntent:
    """Minimal intent-like object for ResultEnricher.

    ResultEnricher accesses intent.intent_type and intent.protocol.
    This provides both from raw strings.
    """

    def __init__(self, intent_type: str, params: dict[str, Any]) -> None:
        from almanak.framework.intents.vocabulary import IntentType

        # Map string intent types to IntentType enum
        _TYPE_MAP = {
            "swap": IntentType.SWAP,
            "lp_open": IntentType.LP_OPEN,
            "lp_close": IntentType.LP_CLOSE,
            "borrow": IntentType.BORROW,
            "repay": IntentType.REPAY,
            "supply": IntentType.SUPPLY,
            "withdraw": IntentType.WITHDRAW,
            "hold": IntentType.HOLD,
            "bridge": IntentType.BRIDGE,
            "perp_open": IntentType.PERP_OPEN,
            "perp_close": IntentType.PERP_CLOSE,
            "stake": IntentType.STAKE,
            "unstake": IntentType.UNSTAKE,
        }

        mapped = _TYPE_MAP.get(intent_type.lower())
        if mapped is None:
            logger.warning("Unknown intent type '%s' for enrichment; defaulting to SWAP", intent_type)
            mapped = IntentType.SWAP
        self.intent_type = mapped
        self.protocol = params.get("protocol")
        self.intent_id = ""


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "EnrichedExecutionResult",
    "IntentExecutionService",
    "RetryPolicy",
    "SadflowEvent",
]
