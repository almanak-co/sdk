"""Tool executor -- maps tool calls to gateway gRPC service calls.

The executor is the deterministic bridge between the agent-facing tool
interface and the gateway. It validates inputs, enforces policy, dispatches
to the appropriate gateway service, and wraps results in standard response
envelopes.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from decimal import Decimal, localcontext
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import ValidationError

from almanak.framework.agent_tools.catalog import (
    RiskTier,
    ToolCatalog,
    ToolCategory,
    ToolDefinition,
    get_default_catalog,
)
from almanak.framework.agent_tools.errors import (
    AgentErrorCode,
    ExecutionFailedError,
    RiskBlockedError,
    SimulationFailedError,
    ToolError,
    ToolValidationError,
    get_error_category,
)
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyEngine
from almanak.framework.agent_tools.schemas import ToolResponse
from almanak.framework.agent_tools.tracing import DecisionTracer, sanitize_args

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


def _error_dict(code: AgentErrorCode, message: str, *, recoverable: bool = False) -> dict:
    """Build a standardized error dict for ToolResponse envelopes.

    Includes ``error_code``, ``message``, ``recoverable``, and ``error_category``
    so LLM agents can reliably pattern-match on error types and decide
    retry vs abort vs escalate.
    """
    return {
        "error_code": code.value,
        "message": message,
        "recoverable": recoverable,
        "error_category": get_error_category(code).value,
    }


class ToolExecutor:
    """Executes agent tool calls through the Almanak gateway.

    Lifecycle of a tool call:
        1. Validate input against Pydantic schema.
        2. Check policy (allowed tool, chain, token, spend limits, rate limits).
        3. Dispatch to the appropriate gateway RPC.
        4. Wrap the result in a ``ToolResponse`` envelope.

    Args:
        gateway_client: Connected ``GatewayClient`` instance.
        policy: Agent policy constraints. Uses safe defaults if not provided.
        catalog: Tool catalog. Uses built-in catalog if not provided.
        wallet_address: Strategy wallet address (needed for balance/execution calls).
        strategy_id: Strategy identifier (needed for state operations).
        safe_addresses: Optional allowlist of known Safe addresses for ``execution_wallet``
            validation. ``None`` = not configured (warn only); empty set = deny all overrides.
            Values are normalized to lowercase for case-insensitive matching.
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        *,
        policy: AgentPolicy | None = None,
        catalog: ToolCatalog | None = None,
        wallet_address: str = "",
        strategy_id: str = "",
        default_chain: str = "arbitrum",
        alert_manager: Any | None = None,
        safe_addresses: set[str] | None = None,
        tracer: DecisionTracer | None = None,
        max_retries: int = 3,
        initial_retry_delay: float = 1.0,
        max_retry_delay: float = 60.0,
    ) -> None:
        self._client = gateway_client
        self._catalog = catalog or get_default_catalog()
        self._wallet_address = wallet_address
        self._policy_engine = PolicyEngine(
            policy or AgentPolicy(),
            price_lookup=self._lookup_token_price,
            default_wallet=wallet_address,
        )
        self._strategy_id = strategy_id
        self._default_chain = default_chain
        self._alert_manager = alert_manager
        self._tracer = tracer or DecisionTracer()
        # Allowlist of known Safe addresses for execution_wallet validation.
        # None = not configured (warning only); empty set = deny all overrides.
        self._safe_addresses: set[str] | None = (
            {a.lower() for a in safe_addresses} if safe_addresses is not None else None
        )

        # Retry policy for intent execution (shared with IntentExecutionService)
        from almanak.framework.runner.inner_runner import RetryPolicy

        self._retry_policy = RetryPolicy(
            max_retries=max_retries,
            initial_delay_seconds=initial_retry_delay,
            max_delay_seconds=max_retry_delay,
        )

        # Bundle cache for compile -> execute flow
        # Each entry stores (chain, bundle_bytes, original_args) to prevent cross-chain
        # execution and enable spend tracking on compiled bundle execution.
        # Capped at 100 entries to prevent unbounded memory growth.
        self._compiled_bundles: dict[str, tuple[str, bytes, dict]] = {}
        self._max_compiled_bundles = 100

        # State version tracking for optimistic locking
        self._state_versions: dict[str, int] = {}

        # Vault settlement crash-recovery state (A2)
        # Mirrors SettlementPhase from vault/config.py but tracked independently
        # for the agent executor path. Persisted in agent state.
        self._settlement_phase: str = "idle"  # idle|proposing|proposed|settling|settled
        self._settlement_proposed_assets: int = 0
        self._settlement_nonce: int = 0
        self._vault_epoch_counter: int = 0

    def _lookup_token_price(self, token: str) -> Decimal | None:
        """Synchronous price lookup for spend-limit pre-checks.

        Returns the USD price of a token, or None if unavailable.
        Used by PolicyEngine to convert raw token amounts to USD estimates.
        """
        try:
            from almanak.gateway.proto import gateway_pb2

            resp = self._client.market.GetPrice(gateway_pb2.PriceRequest(token=token, quote="USD"))
            price = Decimal(str(resp.price))
            return price if price > 0 else None
        except Exception:  # noqa: BLE001
            return None

    def _record_tool_event(self, tool_name: str, tool_def: Any, result: Any) -> None:
        """Fire-and-forget timeline event for audit trail."""
        try:
            from almanak.gateway.proto import gateway_pb2

            self._client.observe.RecordTimelineEvent(
                gateway_pb2.RecordTimelineEventRequest(
                    strategy_id=self._strategy_id,
                    event_type="tool_execution",
                    details_json=json.dumps(
                        {
                            "tool_name": tool_name,
                            "category": tool_def.category.value,
                            "risk_tier": tool_def.risk_tier.value,
                            "status": result.status,
                        }
                    ),
                )
            )
        except Exception:  # noqa: BLE001
            logger.debug("Failed to record tool execution event (non-fatal)")

    def _fire_alert(self, message: str, *, severity: str = "warning") -> None:
        """Fire-and-forget alert via alert manager."""
        if not self._alert_manager:
            return
        try:
            import asyncio

            coro = self._alert_manager.send_alert(message=message, severity=severity)
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)
            except RuntimeError:
                # No running event loop -- close the coroutine to avoid leak
                coro.close()
        except Exception:  # noqa: BLE001
            logger.debug("Failed to send alert (non-fatal)")

    def _resolve_simulation_flag(self, wallet: str, *, tool_name: str, is_safe_wallet: bool | None = None) -> bool:
        """Determine whether simulation should be enabled for an execution call.

        Respects the policy's require_simulation_before_execution flag but skips
        simulation when the execution wallet is a Safe (eth_estimateGas requires
        the actual signer for the `from` address, which isn't available locally).

        Args:
            wallet: The wallet address used for execution.
            tool_name: Name of the tool (for logging).
            is_safe_wallet: Explicitly mark as Safe wallet. If None, inferred
                from whether ``wallet`` differs from the strategy EOA.
        """
        policy_requires = self._policy_engine.policy.require_simulation_before_execution
        if is_safe_wallet is None:
            is_safe_wallet = wallet != self._wallet_address
        if policy_requires and is_safe_wallet:
            logger.info(
                "%s: simulation required by policy but skipped (wallet %s is a Safe; "
                "eth_estimateGas unavailable for non-EOA signers)",
                tool_name,
                wallet[:10],
            )
            return False
        return policy_requires

    # -- Public API ---------------------------------------------------------

    @property
    def tracer(self) -> DecisionTracer:
        """Access the decision tracer for this executor."""
        return self._tracer

    async def execute(self, tool_name: str, arguments: dict) -> ToolResponse:
        """Execute a tool call end-to-end.

        Returns a ``ToolResponse`` envelope. Errors are caught and returned
        as structured error payloads (never raised to the agent).

        Every call is automatically traced via the ``DecisionTracer``,
        capturing arguments (sanitized), policy result, execution outcome,
        and timing.
        """
        start = time.monotonic()
        policy_result_dict: dict | None = None
        execution_result_dict: dict | None = None
        error_str: str | None = None
        result: ToolResponse | None = None

        try:
            result, policy_result_dict = await self._execute_inner(tool_name, arguments)
            execution_result_dict = {"status": result.status}
            if result.data:
                execution_result_dict["data_keys"] = list(result.data.keys())
            return result
        except ToolError as e:
            logger.warning("Tool %s failed: %s", tool_name, e)
            error_str = f"[{e.code}] {e.message}"
            # Capture policy denial info in the trace
            if isinstance(e, RiskBlockedError):
                policy_result_dict = (
                    e.policy_result
                    if hasattr(e, "policy_result")
                    else {
                        "allowed": False,
                        "violations": [e.message],
                    }
                )
                if self._alert_manager:
                    self._fire_alert(f"Policy denied {tool_name}: {e.message}", severity="warning")
            result = ToolResponse(
                status="error",
                error=e.to_dict(),
                explanation=e.message,
            )
            execution_result_dict = {"status": result.status, "error_code": e.code}
            return result
        except Exception as e:
            logger.exception("Unexpected error in tool %s", tool_name)
            error_str = f"internal_error: {e}"
            result = ToolResponse(
                status="error",
                error=_error_dict(AgentErrorCode.INTERNAL_ERROR, str(e), recoverable=False),
                explanation=f"Unexpected error: {e}",
            )
            execution_result_dict = {"status": result.status, "error_code": "internal_error"}
            return result
        finally:
            try:
                duration_ms = (time.monotonic() - start) * 1000
                safe_args = sanitize_args(arguments) if isinstance(arguments, dict) else {"_raw": str(arguments)}
                self._tracer.trace_tool_call(
                    tool_name=tool_name,
                    args=safe_args,
                    policy_result=policy_result_dict,
                    execution_result=execution_result_dict,
                    error=error_str,
                    duration_ms=duration_ms,
                )
            except Exception:  # noqa: BLE001
                logger.debug("Tracing failed (non-fatal, execution result preserved)")

    # -- Internal dispatch --------------------------------------------------

    async def _execute_inner(self, tool_name: str, arguments: dict) -> tuple[ToolResponse, dict | None]:
        """Execute the tool and return (response, policy_result_dict).

        Returns a tuple to avoid storing policy result on the instance
        (which would create a data race under concurrent async calls).
        """
        # 1. Lookup tool
        tool_def = self._catalog.get(tool_name)
        if tool_def is None:
            raise ToolValidationError(f"Unknown tool: {tool_name}", tool_name=tool_name)

        # 1b. Inject default chain if the schema has a 'chain' field and it wasn't provided
        if "chain" not in arguments and hasattr(tool_def.request_schema, "model_fields"):
            if "chain" in tool_def.request_schema.model_fields:
                arguments = {**arguments, "chain": self._default_chain}

        # 2. Validate input schema
        try:
            request = tool_def.request_schema(**arguments)
        except ValidationError as e:
            raise ToolValidationError(
                f"Invalid arguments for {tool_name}: {e}",
                suggestion="Check argument types and required fields.",
                tool_name=tool_name,
            ) from e

        # 3. Policy check
        # validate_risk is a read-only pre-trade check: its request args contain
        # the *hypothetical* trade's chain/token/protocol, not the tool's own
        # execution context. Skip the tool-level policy check entirely -- the
        # real trade-level checks happen inside _execute_validate_risk. This
        # allows agents to query risk status even when the circuit breaker is
        # tripped (they need to know *why* trading is blocked).
        policy_result_dict: dict | None = None
        if tool_name != "validate_risk":
            self._policy_engine.record_tool_call()
            decision = self._policy_engine.check(tool_def, request.model_dump())
            policy_result_dict = {
                "allowed": decision.allowed,
                "violations": decision.violations,
                "suggestions": decision.suggestions,
            }
            if not decision.allowed:
                suggestion = "; ".join(decision.suggestions) if decision.suggestions else None
                err = RiskBlockedError(
                    f"Policy denied '{tool_name}': {'; '.join(decision.violations)}",
                    suggestion=suggestion,
                    tool_name=tool_name,
                )
                err.policy_result = policy_result_dict  # type: ignore[attr-defined]
                raise err

        # 4. Dispatch by category
        request_dict = request.model_dump()

        if tool_def.category == ToolCategory.DATA:
            result = await self._dispatch_data(tool_name, request_dict)
        elif tool_def.category == ToolCategory.PLANNING:
            result = await self._dispatch_planning(tool_name, request_dict)
        elif tool_def.category == ToolCategory.ACTION:
            result = await self._dispatch_action(tool_name, request_dict)
        elif tool_def.category == ToolCategory.STATE:
            result = await self._dispatch_state(tool_name, request_dict)
        else:
            raise ToolValidationError(f"Unsupported category: {tool_def.category}", tool_name=tool_name)

        # Fire-and-forget audit trail for every tool execution
        self._record_tool_event(tool_name, tool_def, result)

        return result, policy_result_dict

    # ── DATA TOOLS ──────────────────────────────────────────────────────

    async def _dispatch_data(self, tool_name: str, args: dict) -> ToolResponse:
        from almanak.gateway.proto import gateway_pb2

        if tool_name == "get_price":
            try:
                resp = self._client.market.GetPrice(gateway_pb2.PriceRequest(token=args["token"], quote="USD"))
                return ToolResponse(
                    status="success",
                    data={
                        "token": args["token"],
                        "price_usd": float(resp.price),
                        "source": resp.source,
                        "timestamp": str(resp.timestamp),
                    },
                )
            except Exception as e:
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.GATEWAY_ERROR,
                        f"Price unavailable for {args['token']}: {e}",
                        recoverable=True,
                    ),
                )

        if tool_name == "get_balance":
            wallet = args.get("wallet_address") or self._wallet_address
            resp = self._client.market.GetBalance(
                gateway_pb2.BalanceRequest(
                    token=args["token"],
                    chain=args.get("chain", self._default_chain),
                    wallet_address=wallet,
                )
            )
            return ToolResponse(
                status="success",
                data={
                    "token": args["token"],
                    "balance": resp.balance,
                    "balance_usd": resp.balance_usd,
                },
            )

        if tool_name == "batch_get_balances":
            chain = args.get("chain", self._default_chain)
            tokens = args.get("tokens")

            if not tokens:
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.VALIDATION_ERROR,
                        "tokens list is required; pass explicit token symbols to query.",
                        recoverable=True,
                    ),
                    explanation="Cannot enumerate all tokens automatically. "
                    "Provide a list of token symbols (e.g. ['ETH', 'USDC']).",
                )

            wallet = args.get("wallet_address") or self._wallet_address
            requests = [gateway_pb2.BalanceRequest(token=t, chain=chain, wallet_address=wallet) for t in tokens]
            resp = self._client.market.BatchGetBalances(gateway_pb2.BatchBalanceRequest(requests=requests))
            balances = [
                {"token": tokens[i], "balance": r.balance, "balance_usd": r.balance_usd}
                for i, r in enumerate(resp.responses)
            ]
            total_usd = sum(
                Decimal(b["balance_usd"]) for b in balances if b.get("balance_usd") and b["balance_usd"] != ""
            )

            return ToolResponse(
                status="success",
                data={"balances": balances, "total_usd": str(total_usd)},
            )

        if tool_name == "get_indicator":
            resp = self._client.market.GetIndicator(
                gateway_pb2.IndicatorRequest(
                    indicator_type=args["indicator"].upper(),
                    token=args["token"],
                    quote="USD",
                    params={"period": str(args.get("period", 14))},
                )
            )
            return ToolResponse(
                status="success",
                data={
                    "indicator": args["indicator"],
                    "value": float(resp.value),
                    "signal": resp.metadata.get("signal"),
                    "extra": dict(resp.metadata) if resp.metadata else None,
                },
            )

        if tool_name == "get_pool_state":
            return await self._execute_get_pool_state(args)

        if tool_name == "get_lp_position":
            return await self._execute_get_lp_position(args)

        if tool_name == "resolve_token":
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(args["token"], args.get("chain", self._default_chain))
            return ToolResponse(
                status="success",
                data={
                    "symbol": token.symbol,
                    "address": token.address,
                    "decimals": token.decimals,
                    "chain": token.chain,
                    "source": token.source,
                },
            )

        if tool_name == "get_risk_metrics":
            return await self._execute_get_risk_metrics(args)

        if tool_name == "get_vault_state":
            return await self._execute_get_vault_state(args)

        raise ToolValidationError(f"Unknown data tool: {tool_name}", tool_name=tool_name)

    # ── PLANNING / SAFETY TOOLS ─────────────────────────────────────────

    async def _dispatch_planning(self, tool_name: str, args: dict) -> ToolResponse:
        from almanak.gateway.proto import gateway_pb2

        if tool_name == "compile_intent":
            chain = args.get("chain", self._default_chain)
            intent_type = args["intent_type"]
            params = args.get("params", {})

            resp = self._client.execution.CompileIntent(
                gateway_pb2.CompileIntentRequest(
                    intent_type=intent_type,
                    intent_data=json.dumps(params).encode(),
                    chain=chain,
                    wallet_address=self._wallet_address,
                )
            )

            if not resp.success:
                raise SimulationFailedError(
                    f"Compilation failed: {resp.error}",
                    tool_name=tool_name,
                )

            # Cache the compiled bundle with chain metadata and original args for
            # later execution and spend tracking
            bundle_id = str(uuid.uuid4())
            # Evict oldest entries if cache is full
            if len(self._compiled_bundles) >= self._max_compiled_bundles:
                oldest_key = next(iter(self._compiled_bundles))
                del self._compiled_bundles[oldest_key]
            self._compiled_bundles[bundle_id] = (chain, resp.action_bundle, args)

            # Parse actions for the response
            try:
                bundle_data = json.loads(resp.action_bundle)
                actions = bundle_data.get("actions", [])
            except (json.JSONDecodeError, AttributeError):
                actions = []

            return ToolResponse(
                status="success",
                data={
                    "bundle_id": bundle_id,
                    "actions": actions,
                    "gas_estimate_usd": "",
                    "warnings": [],
                },
            )

        if tool_name == "simulate_intent":
            sim_bundle_id = args.get("bundle_id")
            cached_chain = None
            if sim_bundle_id and sim_bundle_id in self._compiled_bundles:
                cached_chain, bundle_bytes, _original_args = self._compiled_bundles[sim_bundle_id]
            elif args.get("intent_type"):
                # Compile on the fly for simulation
                chain = args.get("chain", self._default_chain)
                params = args.get("params", {})
                compile_resp = self._client.execution.CompileIntent(
                    gateway_pb2.CompileIntentRequest(
                        intent_type=args["intent_type"],
                        intent_data=json.dumps(params).encode(),
                        chain=chain,
                        wallet_address=self._wallet_address,
                    )
                )
                if not compile_resp.success:
                    return ToolResponse(
                        status="error",
                        error=_error_dict(AgentErrorCode.SIMULATION_FAILED, compile_resp.error, recoverable=True),
                    )
                bundle_bytes = compile_resp.action_bundle
            else:
                raise ToolValidationError(
                    "Must provide either bundle_id or intent_type+params for simulation.",
                    tool_name=tool_name,
                )

            # Use the cached chain from the bundle if available, otherwise fall back to args
            chain = cached_chain or args.get("chain", self._default_chain)
            exec_resp = self._client.execution.Execute(
                gateway_pb2.ExecuteRequest(
                    action_bundle=bundle_bytes,
                    dry_run=True,
                    simulation_enabled=True,
                    strategy_id=self._strategy_id,
                    chain=chain,
                    wallet_address=self._wallet_address,
                )
            )

            return ToolResponse(
                status="simulated" if exec_resp.success else "error",
                data={
                    "success": exec_resp.success,
                    "estimated_output": {},
                    "price_impact": "",
                    "gas_estimate_usd": "",
                    "revert_reason": exec_resp.error if not exec_resp.success else None,
                },
            )

        if tool_name == "validate_risk":
            return self._execute_validate_risk(args)

        if tool_name == "compute_rebalance_candidate":
            result = await self._execute_compute_rebalance_candidate(args)
            # Set rebalance gate based on viability
            if result.status == "success" and result.data and result.data.get("viable"):
                self._policy_engine.set_rebalance_approved(True)
            return result

        if tool_name == "estimate_gas":
            return self._execute_estimate_gas(args)

        raise ToolValidationError(f"Unknown planning tool: {tool_name}", tool_name=tool_name)

    # ── ACTION TOOLS ────────────────────────────────────────────────────

    async def _dispatch_action(self, tool_name: str, args: dict) -> ToolResponse:
        # Handle execute_compiled_bundle separately -- it already has a compiled bundle
        if tool_name == "execute_compiled_bundle":
            return await self._execute_compiled_bundle(args)

        # Vault tools -- handled separately from intent-based action tools
        if tool_name == "deploy_vault":
            return await self._execute_deploy_vault(args)
        if tool_name == "settle_vault":
            return await self._execute_settle_vault(args)
        if tool_name == "approve_vault_underlying":
            return await self._execute_approve_vault_underlying(args)
        if tool_name == "deposit_vault":
            return await self._execute_deposit_vault(args)
        if tool_name == "teardown_vault":
            return await self._execute_teardown_vault(args)

        # Build intent params from the tool-specific arguments
        intent_type, intent_params = self._action_to_intent(tool_name, args)
        dry_run = args.get("dry_run", False)
        chain = args.get("chain", self._default_chain)

        # Use execution_wallet override if provided (e.g. Safe address for vault fund management)
        wallet = args.get("execution_wallet") or self._wallet_address

        # Validate execution_wallet against known Safe addresses when configured
        if wallet.lower() != self._wallet_address.lower():
            if self._safe_addresses is not None:
                if wallet.lower() not in self._safe_addresses:
                    raise ToolValidationError(
                        f"execution_wallet '{wallet}' is not in the configured Safe address allowlist. "
                        "Only known Safe addresses may be used as execution_wallet overrides.",
                        tool_name=tool_name,
                    )
            else:
                logger.warning(
                    "%s: execution_wallet '%s' differs from strategy EOA but no safe_addresses "
                    "allowlist is configured -- cannot validate. Consider passing safe_addresses "
                    "to ToolExecutor for production use.",
                    tool_name,
                    wallet,
                )

        simulate = self._resolve_simulation_flag(wallet, tool_name=tool_name)

        # Infer protocol for result enrichment
        protocol = intent_params.get("protocol")

        # Execute through the shared IntentExecutionService (retry + enrichment)
        from almanak.framework.runner.inner_runner import IntentExecutionService

        service = IntentExecutionService(
            self._client,
            chain=chain,
            wallet_address=wallet,
            strategy_id=self._strategy_id,
            retry_policy=self._retry_policy,
            on_sadflow=self._on_sadflow_event,
        )

        enriched = await service.execute_intent(
            intent_type=intent_type,
            intent_params=intent_params,
            chain=chain,
            wallet_address=wallet,
            dry_run=dry_run,
            simulate=simulate,
            tool_name=tool_name,
            protocol=protocol,
        )

        # Track spend (and failures) before raising so circuit breaker works
        if not dry_run:
            usd_amount = await self._estimate_usd_spend(args)
            self._policy_engine.record_trade(usd_amount, success=enriched.success, tool_name=tool_name)
            # Update portfolio value for stop-loss tracking after successful trades
            if enriched.success:
                portfolio_usd = await self._fetch_portfolio_value()
                if portfolio_usd > 0:
                    self._policy_engine.update_portfolio_value(portfolio_usd)
            # Alert on circuit breaker threshold
            elif self._policy_engine.is_circuit_breaker_tripped:
                self._fire_alert(
                    f"Circuit breaker tripped: {self._policy_engine.consecutive_failures} "
                    f"consecutive failures (tool: {tool_name})",
                    severity="critical",
                )

        if not enriched.success and not dry_run:
            raise ExecutionFailedError(
                f"Execution failed for {tool_name}: {enriched.error}",
                tool_name=tool_name,
            )

        if dry_run:
            status = "simulated" if enriched.success else "error"
        else:
            status = "success" if enriched.success else "error"
        data = self._build_action_response_from_enriched(tool_name, enriched, args)

        # Reset rebalance gate after LP open/close execution
        if tool_name in ("open_lp_position", "close_lp_position"):
            self._policy_engine.set_rebalance_approved(False)

        return ToolResponse(status=status, data=data)

    async def _execute_compiled_bundle(self, args: dict) -> ToolResponse:
        """Execute a previously compiled ActionBundle from the bundle cache."""
        from almanak.gateway.proto import gateway_pb2

        bundle_id = args["bundle_id"]
        dry_run = args.get("dry_run", False)
        chain = args.get("chain", self._default_chain)

        cached = self._compiled_bundles.get(bundle_id)
        if cached is None:
            raise ToolValidationError(
                f"Bundle '{bundle_id}' not found. It may have expired or was never compiled. "
                "Use compile_intent first to create a bundle.",
                tool_name="execute_compiled_bundle",
            )

        compiled_chain, bundle_bytes, original_args = cached

        # Enforce chain match to prevent wrong-network execution
        if chain.lower() != compiled_chain.lower():
            raise ToolValidationError(
                f"Bundle was compiled for chain '{compiled_chain}' but execution requested on '{chain}'. "
                "Recompile the bundle for the target chain.",
                tool_name="execute_compiled_bundle",
            )

        # Pre-execution spend gate: check the original compile_intent params against
        # spend limits BEFORE executing. This prevents a compiled bundle from bypassing
        # the spend limit pre-check that action tools receive at compile time.
        intent_params = original_args.get("params", {})
        if intent_params and not dry_run:
            violations: list[str] = []
            suggestions: list[str] = []
            self._policy_engine._check_spend_limits(intent_params, violations, suggestions)
            if violations:
                from almanak.framework.agent_tools.errors import RiskBlockedError

                raise RiskBlockedError(
                    f"Compiled bundle blocked by spend limits: {'; '.join(violations)}",
                    tool_name="execute_compiled_bundle",
                    suggestion="; ".join(suggestions),
                )

        # Simulation flag: policy requirement cannot be bypassed by agent
        simulation_enabled = self._policy_engine.policy.require_simulation_before_execution or args.get(
            "require_simulation", True
        )

        exec_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=bundle_bytes,
                dry_run=dry_run,
                simulation_enabled=simulation_enabled,
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=self._wallet_address,
            )
        )

        # Track trade result using the original compile_intent args for USD estimation
        if not dry_run:
            usd_amount = await self._estimate_usd_spend(original_args.get("params", {}))
            self._policy_engine.record_trade(usd_amount, success=exec_resp.success, tool_name="execute_compiled_bundle")

        if not exec_resp.success and not dry_run:
            raise ExecutionFailedError(
                f"Execution of compiled bundle failed: {exec_resp.error}",
                tool_name="execute_compiled_bundle",
            )

        # Remove from cache after execution (one-shot)
        self._compiled_bundles.pop(bundle_id, None)

        tx_hashes = list(exec_resp.tx_hashes) if exec_resp.tx_hashes else []
        status = "simulated" if dry_run else ("success" if exec_resp.success else "error")

        return ToolResponse(
            status=status,
            data={
                "tx_hashes": tx_hashes,
                "success": exec_resp.success,
                "gas_used_usd": "",
                "receipts": [],
            },
        )

    def _action_to_intent(self, tool_name: str, args: dict) -> tuple[str, dict]:
        """Map action tool arguments to intent type + params."""
        if tool_name == "swap_tokens":
            return "swap", {
                "from_token": args["token_in"],
                "to_token": args["token_out"],
                "amount": args["amount"],
                "max_slippage": args.get("slippage_bps", 50) / 10000,
                "protocol": args.get("protocol"),
            }

        if tool_name == "open_lp_position":
            # Build pool identifier: "TOKEN_A/TOKEN_B/FEE"
            # The compiler sorts tokens by resolved address (token0 < token1), so we
            # must sort the same way to ensure amount0 pairs with token0.
            token_a = args["token_a"]
            token_b = args["token_b"]
            amount_a = args["amount_a"]
            amount_b = args["amount_b"]

            # Sort by resolved address to match compiler's _parse_pool_info sort order.
            # Use resolve_for_swap to auto-wrap native tokens (ETH->WETH, etc.)
            try:
                from almanak.framework.data.tokens import get_token_resolver

                resolver = get_token_resolver()
                chain = args.get("chain", self._default_chain)
                addr_a = resolver.resolve_for_swap(token_a, chain).address
                addr_b = resolver.resolve_for_swap(token_b, chain).address
            except Exception:
                addr_a, addr_b = token_a, token_b

            price_lower = args["price_lower"]
            price_upper = args["price_upper"]

            if addr_a.lower() > addr_b.lower():
                token_a, token_b = token_b, token_a
                amount_a, amount_b = amount_b, amount_a
                # Prices are NOT inverted. The LLM computes price bounds from
                # get_pool_state's current_price, which is always in "token1 per
                # token0" direction (Uniswap V3 convention). Sorting the token
                # variables doesn't change the price direction -- the compiler
                # also expects "token1 per token0" prices.

            pool = f"{token_a}/{token_b}"
            fee_tier = args.get("fee_tier")
            if fee_tier:
                pool = f"{pool}/{fee_tier}"
            return "lp_open", {
                "pool": pool,
                "amount0": amount_a,
                "amount1": amount_b,
                "protocol": args.get("protocol", "uniswap_v3"),
                "range_lower": price_lower,
                "range_upper": price_upper,
            }

        if tool_name == "close_lp_position":
            # LPCloseIntent only supports full close -- reject partial amounts
            amount = args.get("amount", "all")
            if str(amount).lower() != "all":
                raise ToolValidationError(
                    f"Partial LP close (amount='{amount}') is not supported. "
                    "Only amount='all' is allowed. Use the full close to exit the position.",
                    tool_name=tool_name,
                )
            # LPCloseIntent fields: position_id, pool, collect_fees, protocol, chain
            return "lp_close", {
                "position_id": args["position_id"],
                "collect_fees": args.get("collect_fees", True),
                "protocol": args.get("protocol", "uniswap_v3"),
            }

        if tool_name == "supply_lending":
            return "supply", {
                "token": args["token"],
                "amount": args["amount"],
                "protocol": args.get("protocol", "aave_v3"),
                "use_as_collateral": args.get("use_as_collateral", True),
            }

        if tool_name == "borrow_lending":
            return "borrow", {
                "borrow_token": args["token"],
                "borrow_amount": args["amount"],
                "collateral_token": args["collateral_token"],
                "collateral_amount": args["collateral_amount"],
                "protocol": args.get("protocol", "aave_v3"),
            }

        if tool_name == "repay_lending":
            return "repay", {
                "token": args["token"],
                "amount": args["amount"],
                "protocol": args.get("protocol", "aave_v3"),
            }

        raise ToolValidationError(f"Unknown action tool: {tool_name}", tool_name=tool_name)

    def _build_action_response(self, tool_name: str, exec_resp: Any, args: dict) -> dict:
        """Build tool-specific response data from gateway ExecutionResult."""
        tx_hashes = list(exec_resp.tx_hashes) if exec_resp.tx_hashes else []
        tx_hash = tx_hashes[0] if tx_hashes else None

        base = {"tx_hash": tx_hash, "gas_usd": ""}

        if tool_name == "swap_tokens":
            return {
                **base,
                "amount_in": args.get("amount", ""),
                "amount_out": "",
                "effective_price": "",
                "price_impact": "",
            }

        if tool_name == "open_lp_position":
            position_id = self._extract_position_id(exec_resp, args)
            return {**base, "position_id": position_id, "liquidity": "", "tick_lower": 0, "tick_upper": 0}

        if tool_name == "close_lp_position":
            return {
                **base,
                "token_a_received": "",
                "token_b_received": "",
                "fees_collected_a": "",
                "fees_collected_b": "",
            }

        if tool_name == "supply_lending":
            return {**base, "amount_supplied": args.get("amount", "")}
        if tool_name == "borrow_lending":
            return {**base, "amount_borrowed": args.get("amount", "")}
        if tool_name == "repay_lending":
            return {**base, "amount_repaid": args.get("amount", "")}

        return base

    def _build_action_response_from_enriched(self, tool_name: str, enriched: Any, args: dict) -> dict:
        """Build tool-specific response data from EnrichedExecutionResult.

        This is the enrichment-aware replacement for _build_action_response.
        It uses data extracted by ResultEnricher (via IntentExecutionService)
        to populate response fields that were previously empty strings.
        """
        tx_hash = enriched.tx_hash
        base = {"tx_hash": tx_hash, "gas_usd": ""}

        if tool_name == "swap_tokens":
            swap = enriched.swap_amounts
            return {
                **base,
                "amount_in": str(swap.amount_in_decimal)
                if swap and swap.amount_in_decimal is not None
                else args.get("amount", ""),
                "amount_out": str(swap.amount_out_decimal) if swap and swap.amount_out_decimal is not None else "",
                "effective_price": str(swap.effective_price) if swap and swap.effective_price is not None else "",
                "price_impact": "",
                "slippage_bps": swap.slippage_bps if swap and swap.slippage_bps is not None else None,
                "token_in": swap.token_in if swap else args.get("token_in", ""),
                "token_out": swap.token_out if swap else args.get("token_out", ""),
            }

        if tool_name == "open_lp_position":
            position_id = enriched.position_id
            liquidity = enriched.extracted_data.get("liquidity", "")
            tick_lower = enriched.extracted_data.get("tick_lower", 0)
            tick_upper = enriched.extracted_data.get("tick_upper", 0)
            return {
                **base,
                "position_id": position_id,
                "liquidity": liquidity,
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
            }

        if tool_name == "close_lp_position":
            lp_data = enriched.lp_close_data
            return {
                **base,
                "token_a_received": str(lp_data.amount0_collected)
                if lp_data and lp_data.amount0_collected is not None
                else "",
                "token_b_received": str(lp_data.amount1_collected)
                if lp_data and lp_data.amount1_collected is not None
                else "",
                "fees_collected_a": str(lp_data.fees0) if lp_data and lp_data.fees0 is not None else "",
                "fees_collected_b": str(lp_data.fees1) if lp_data and lp_data.fees1 is not None else "",
            }

        if tool_name == "supply_lending":
            return {**base, "amount_supplied": args.get("amount", "")}
        if tool_name == "borrow_lending":
            return {**base, "amount_borrowed": args.get("amount", "")}
        if tool_name == "repay_lending":
            return {**base, "amount_repaid": args.get("amount", "")}

        return base

    def _on_sadflow_event(self, event: Any) -> None:
        """Handle sadflow events from IntentExecutionService.

        Fires alerts and logs failure context. This bridges the
        IntentExecutionService's sadflow hooks to the ToolExecutor's
        existing alert infrastructure.
        """
        if event.is_final:
            severity = "critical"
            message = (
                f"Intent execution permanently failed after {event.max_retries + 1} attempts: "
                f"{event.tool_name or event.intent_type} on {event.chain}: {event.error}"
            )
        else:
            severity = "warning"
            message = (
                f"Intent execution failed (attempt {event.attempt + 1}/{event.max_retries + 1}): "
                f"{event.tool_name or event.intent_type} on {event.chain}: {event.error}"
            )

        logger.warning(message)
        self._fire_alert(message, severity=severity)

    def _extract_position_id(self, exec_resp: Any, args: dict) -> int | None:
        """Extract LP position NFT tokenId from execution receipts."""
        try:
            if not exec_resp.receipts:
                return None
            import json as _json

            receipts = _json.loads(exec_resp.receipts)
            if not isinstance(receipts, list):
                receipts = [receipts]
            # IncreaseLiquidity event topic (Uniswap V3 NonfungiblePositionManager)
            increase_liq_topic = "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f"
            for receipt in receipts:
                for log in receipt.get("logs", []):
                    topics = log.get("topics", [])
                    if topics and topics[0] == increase_liq_topic and len(topics) >= 2:
                        return int(topics[1], 16)
        except Exception as e:
            logger.debug("Could not extract position_id from receipts: %s", e)
        return None

    async def _estimate_usd_spend(self, args: dict) -> Decimal:
        """Best-effort USD estimation for spend tracking.

        Attempts to look up token prices via the gateway to convert raw
        token amounts to USD. For LP opens, tracks both token_a and token_b.
        Falls back to the raw amount or zero if price lookup fails.
        """
        from almanak.framework.data.tokens import get_token_resolver
        from almanak.gateway.proto import gateway_pb2

        spend_items: list[tuple[str, str]] = []

        # Primary token (swap, supply, borrow, repay, vault deposit, or LP token_a)
        # Also checks from_token (intent vocabulary used by compile_intent) and
        # underlying_token (vault deposit).
        primary_token = (
            args.get("token_in")
            or args.get("token")
            or args.get("from_token")
            or args.get("underlying_token")
            or args.get("token_a")
        )
        primary_amount = args.get("amount") or args.get("amount_a")
        if primary_token and primary_amount is not None:
            spend_items.append((primary_token, str(primary_amount)))

        # Secondary token (LP token_b)
        if args.get("token_b") and args.get("amount_b") is not None:
            spend_items.append((args["token_b"], str(args["amount_b"])))

        total = Decimal("0")
        chain = args.get("chain", self._default_chain)
        for token, amount_str in spend_items:
            if amount_str.lower() == "all":
                logger.warning("Cannot track spend accurately for 'all' amount on %s", token)
                continue
            try:
                raw_amount = Decimal(amount_str)
            except (ArithmeticError, TypeError, ValueError):
                continue

            # Normalize by token decimals when amount looks like raw units.
            # Uses the same heuristic as PolicyEngine._estimate_usd_value:
            # if amount > 10^(decimals-1), it's likely raw (e.g. 10_000_000
            # for 10 USDC at 6 decimals). This avoids the fragile fixed
            # threshold and correctly handles 18-decimal tokens.
            try:
                resolved = get_token_resolver().resolve(token, chain)
                if resolved and resolved.decimals > 0:
                    threshold = Decimal(10 ** max(resolved.decimals - 1, 0))
                    if raw_amount > threshold:
                        raw_amount = raw_amount / Decimal(10**resolved.decimals)
            except Exception:  # noqa: BLE001
                pass

            try:
                resp = self._client.market.GetPrice(gateway_pb2.PriceRequest(token=token, quote="USD"))
                price = Decimal(str(resp.price))
                total += raw_amount * price if price > 0 else raw_amount
            except Exception:  # noqa: BLE001 - gateway may raise any gRPC error
                logger.debug("Could not look up price for %s; using raw amount for spend tracking", token)
                total += raw_amount

        return total

    async def _fetch_portfolio_value(self) -> Decimal:
        """Best-effort portfolio value for stop-loss tracking.

        Queries balances across all chains in the agent policy's allowed_chains
        (not just the default chain) so that multi-chain portfolios are tracked.
        """
        try:
            from almanak.gateway.proto import gateway_pb2

            tokens = list(self._policy_engine.policy.allowed_tokens or self._RISK_METRIC_TOKENS)
            chains = list(self._policy_engine.policy.allowed_chains or {self._default_chain})
            requests = [
                gateway_pb2.BalanceRequest(token=t, chain=c, wallet_address=self._wallet_address)
                for c in chains
                for t in tokens
            ]
            resp = self._client.market.BatchGetBalances(gateway_pb2.BatchBalanceRequest(requests=requests))
            total = Decimal("0")
            for r in resp.responses:
                if r.balance_usd:
                    try:
                        total += Decimal(r.balance_usd)
                    except (ArithmeticError, ValueError):
                        continue
            return total
        except Exception:  # noqa: BLE001
            return Decimal("0")

    # ── VAULT TOOLS ──────────────────────────────────────────────────────

    async def _execute_deploy_vault(self, args: dict) -> ToolResponse:
        """Deploy a new Lagoon vault via factory contract.

        Includes idempotency check: if agent state already has a vault_address,
        verifies it on-chain and returns it rather than deploying a duplicate.
        """
        from almanak.framework.connectors.lagoon.deployer import (
            LagoonVaultDeployer,
            VaultDeployParams,
        )
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)
        dry_run = args.get("dry_run", False)

        # Idempotency: check if vault already deployed in a previous attempt
        try:
            state_resp = self._client.state.LoadState(gateway_pb2.LoadStateRequest(strategy_id=self._strategy_id))
            saved_state = json.loads(state_resp.data) if state_resp.data else {}
            existing_vault = saved_state.get("vault_address")
            if existing_vault:
                # Verify it exists on-chain
                from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK

                sdk = LagoonVaultSDK(self._client, chain=chain)
                try:
                    sdk.get_total_assets(existing_vault)
                    logger.info(
                        "deploy_vault: vault already exists at %s (idempotency check), returning existing",
                        existing_vault,
                    )
                    return ToolResponse(
                        status="success",
                        data={
                            "status": "success",
                            "vault_address": existing_vault,
                            "tx_hash": None,
                            "message": f"Vault already deployed at {existing_vault}",
                        },
                    )
                except Exception as e:
                    # On EVM, calling a non-existent address returns 0x (success),
                    # so an exception here means a transient RPC/gateway error,
                    # not "vault doesn't exist". Fail closed to prevent duplicates.
                    logger.warning("deploy_vault: cannot verify saved vault %s on-chain: %s", existing_vault[:10], e)
                    return ToolResponse(
                        status="error",
                        error=_error_dict(
                            AgentErrorCode.VAULT_VERIFICATION_FAILED,
                            f"deploy_vault aborted: saved vault {existing_vault} exists in state "
                            f"but on-chain verification failed: {e}",
                            recoverable=True,
                        ),
                    )
        except Exception as e:
            import grpc

            if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.NOT_FOUND:
                logger.debug("deploy_vault: no saved state found; proceeding with deployment")
            else:
                logger.warning("deploy_vault: failed to load state for idempotency check: %s", e)
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.STATE_LOAD_FAILED,
                        f"deploy_vault aborted: unable to verify existing vault due to state load failure: {e}",
                        recoverable=True,
                    ),
                )

        params = VaultDeployParams(
            chain=chain,
            underlying_token_address=args["underlying_token_address"],
            name=args["name"],
            symbol=args["symbol"],
            safe_address=args["safe_address"],
            admin_address=args["admin_address"],
            fee_receiver_address=args["fee_receiver_address"],
            deployer_address=args["deployer_address"],
            valuation_manager_address=args.get("valuation_manager_address"),
        )

        deployer = LagoonVaultDeployer(gateway_client=self._client)
        bundle = deployer.build_deploy_vault_bundle(params)
        bundle_bytes = json.dumps(bundle.to_dict()).encode()

        simulate = self._policy_engine.policy.require_simulation_before_execution

        exec_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=bundle_bytes,
                dry_run=dry_run,
                simulation_enabled=simulate,
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=self._wallet_address,
            )
        )

        if not exec_resp.success and not dry_run:
            raise ExecutionFailedError(
                f"Vault deployment failed: {exec_resp.error}",
                tool_name="deploy_vault",
            )

        # Parse vault address from receipt
        vault_address = None
        tx_hash = None
        if exec_resp.tx_hashes:
            tx_hash = exec_resp.tx_hashes[0]
        if exec_resp.receipts:
            try:
                # receipts is a bytes field containing JSON-serialized list of receipts
                raw = exec_resp.receipts
                if isinstance(raw, bytes):
                    receipts_list = json.loads(raw.decode("utf-8"))
                elif isinstance(raw, str):
                    receipts_list = json.loads(raw)
                else:
                    receipts_list = []
                if receipts_list and isinstance(receipts_list, list) and len(receipts_list) > 0:
                    receipt = receipts_list[0]
                    if isinstance(receipt, dict):
                        deploy_result = LagoonVaultDeployer.parse_deploy_receipt(receipt)
                        vault_address = deploy_result.vault_address
            except Exception as e:
                logger.warning("Failed to parse deploy receipt: %s", e)

        # Reset settlement state for the new vault (clear stale data from prior vaults)
        if not dry_run and vault_address:
            self._settlement_phase = "idle"
            self._settlement_nonce = 0
            self._settlement_proposed_assets = 0
            self._vault_epoch_counter = 0
            self._save_settlement_state("idle")
            logger.info("deploy_vault: reset settlement state for new vault %s", vault_address[:10])

        status = "simulated" if dry_run else "success"
        return ToolResponse(
            status=status,
            data={
                "status": status,
                "vault_address": vault_address,
                "tx_hash": tx_hash,
                "message": f"Vault deployed at {vault_address}" if vault_address else "Vault deployment submitted",
            },
        )

    # ── POOL / POSITION READ TOOLS ─────────────────────────────────────

    async def _execute_get_pool_state(self, args: dict) -> ToolResponse:
        """Read Uniswap V3 pool state via slot0() and liquidity() RPC calls."""
        from almanak.framework.connectors.uniswap_v3.sdk import FACTORY_ADDRESSES, compute_pool_address
        from almanak.framework.data.tokens import get_token_resolver
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)
        token_a_sym = args["token_a"]
        token_b_sym = args["token_b"]
        fee_tier = args.get("fee_tier", 3000)

        # Resolve token addresses
        resolver = get_token_resolver()
        token_a = resolver.resolve_for_swap(token_a_sym, chain)
        token_b = resolver.resolve_for_swap(token_b_sym, chain)

        # Use explicit pool_address if provided, otherwise compute from factory
        pool_address = args.get("pool_address")
        if not pool_address:
            if chain not in FACTORY_ADDRESSES:
                return ToolResponse(
                    status="error",
                    error=_error_dict(AgentErrorCode.UNSUPPORTED_CHAIN, f"No Uniswap V3 factory on {chain}"),
                )
            pool_address = compute_pool_address(FACTORY_ADDRESSES[chain], token_a.address, token_b.address, fee_tier)

        # Read slot0: sqrtPriceX96 (uint160), tick (int24), ...
        slot0_selector = "0x3850c7bd"
        slot0_resp = self._client.rpc.Call(
            gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_call",
                params=json.dumps([{"to": pool_address, "data": slot0_selector}, "latest"]),
                id="pool_slot0",
            ),
            timeout=30.0,
        )
        if not slot0_resp.success:
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.RPC_FAILED,
                    f"slot0() failed: {slot0_resp.error}",
                    recoverable=True,
                ),
            )

        slot0_hex = json.loads(slot0_resp.result).removeprefix("0x")
        if len(slot0_hex) < 128:
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.EMPTY_POOL,
                    "Pool may not exist or is uninitialized",
                ),
            )

        sqrt_price_x96 = int(slot0_hex[0:64], 16)
        raw_tick = int(slot0_hex[64:128], 16)
        tick = raw_tick if raw_tick < 2**23 else raw_tick - 2**24

        # Read liquidity
        liq_selector = "0x1a686502"
        liq_resp = self._client.rpc.Call(
            gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_call",
                params=json.dumps([{"to": pool_address, "data": liq_selector}, "latest"]),
                id="pool_liquidity",
            ),
            timeout=30.0,
        )
        liquidity = 0
        if liq_resp.success:
            liq_hex = json.loads(liq_resp.result).removeprefix("0x")
            if liq_hex:
                liquidity = int(liq_hex, 16)

        # Compute human price from sqrtPriceX96
        # Raw price = (sqrtPriceX96 / 2^96)^2 gives token1/token0 in raw units.
        # Adjust for decimals: human_price = raw_price * 10^(decimals0 - decimals1)
        raw_price = (sqrt_price_x96 / 2**96) ** 2 if sqrt_price_x96 > 0 else 0

        # Determine token0/token1 by address ordering (same as Uniswap)
        if token_a.address.lower() < token_b.address.lower():
            token0, token1 = token_a, token_b
        else:
            token0, token1 = token_b, token_a

        decimal_adjustment = 10 ** (token0.decimals - token1.decimals)
        adjusted_price = raw_price * decimal_adjustment

        return ToolResponse(
            status="success",
            data={
                "pool_address": pool_address,
                "current_price": str(adjusted_price),
                "current_price_raw": str(raw_price),
                "tick": tick,
                "liquidity": str(liquidity),
                "sqrt_price_x96": str(sqrt_price_x96),
                "fee_tier": fee_tier,
                "token0": token0.symbol if hasattr(token0, "symbol") else str(token0.address),
                "token1": token1.symbol if hasattr(token1, "symbol") else str(token1.address),
                "token0_decimals": token0.decimals,
                "token1_decimals": token1.decimals,
                "volume_24h_usd": "",
                "fee_apr": "",
                "tvl_usd": "",
            },
        )

    async def _execute_get_lp_position(self, args: dict) -> ToolResponse:
        """Read Uniswap V3 LP position via NonfungiblePositionManager.positions()."""
        from almanak.framework.connectors.uniswap_v3.receipt_parser import POSITION_MANAGER_ADDRESSES
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)
        position_id = int(args["position_id"])

        nft_manager = POSITION_MANAGER_ADDRESSES.get(chain)
        if not nft_manager:
            return ToolResponse(
                status="error",
                error=_error_dict(AgentErrorCode.UNSUPPORTED_CHAIN, f"No position manager on {chain}"),
            )

        # positions(uint256) selector = 0x99fbab88
        calldata = "0x99fbab88" + hex(position_id)[2:].zfill(64)
        resp = self._client.rpc.Call(
            gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_call",
                params=json.dumps([{"to": nft_manager, "data": calldata}, "latest"]),
                id="lp_position",
            ),
            timeout=30.0,
        )
        if not resp.success:
            return ToolResponse(
                status="error",
                error=_error_dict(AgentErrorCode.RPC_FAILED, f"positions() failed: {resp.error}", recoverable=True),
            )

        raw = json.loads(resp.result).removeprefix("0x")
        if len(raw) < 768:  # 12 words * 64 hex chars
            return ToolResponse(
                status="error",
                error=_error_dict(AgentErrorCode.INVALID_POSITION, f"Position {position_id} not found or burned"),
            )

        words = [raw[i * 64 : (i + 1) * 64] for i in range(12)]
        token0 = "0x" + words[2][-40:]
        token1 = "0x" + words[3][-40:]
        fee = int(words[4], 16)
        tick_lower_raw = int(words[5], 16)
        tick_lower = tick_lower_raw if tick_lower_raw < 2**23 else tick_lower_raw - 2**24
        tick_upper_raw = int(words[6], 16)
        tick_upper = tick_upper_raw if tick_upper_raw < 2**23 else tick_upper_raw - 2**24
        liquidity = int(words[7], 16)
        tokens_owed_0 = int(words[10], 16)
        tokens_owed_1 = int(words[11], 16)

        # Read current tick to determine if in range
        # We need the pool address -- compute from token0/token1/fee
        from almanak.framework.connectors.uniswap_v3.sdk import FACTORY_ADDRESSES, compute_pool_address

        in_range = True
        pool_current_tick = None
        if chain in FACTORY_ADDRESSES:
            pool_addr = compute_pool_address(FACTORY_ADDRESSES[chain], token0, token1, fee)
            slot0_resp = self._client.rpc.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_call",
                    params=json.dumps([{"to": pool_addr, "data": "0x3850c7bd"}, "latest"]),
                    id="lp_pool_slot0",
                ),
                timeout=30.0,
            )
            if slot0_resp.success:
                s0 = json.loads(slot0_resp.result).removeprefix("0x")
                if len(s0) >= 128:
                    current_tick_raw = int(s0[64:128], 16)
                    pool_current_tick = current_tick_raw if current_tick_raw < 2**23 else current_tick_raw - 2**24
                    in_range = tick_lower <= pool_current_tick < tick_upper

        data: dict = {
            "position_id": str(position_id),
            "token_a": token0,
            "token_b": token1,
            "fee_tier": fee,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "liquidity": str(liquidity),
            "tokens_owed_a": str(tokens_owed_0),
            "tokens_owed_b": str(tokens_owed_1),
            "in_range": in_range,
        }
        if pool_current_tick is not None:
            data["current_tick"] = pool_current_tick

        return ToolResponse(status="success", data=data)

    async def _execute_compute_rebalance_candidate(self, args: dict) -> ToolResponse:
        """Deterministic economic viability check for LP rebalancing.

        Computes estimated gas cost vs expected daily fee revenue to determine
        if a rebalance is worth executing.
        """
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)

        # Read current gas price via eth_gasPrice
        gas_resp = self._client.rpc.Call(
            gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_gasPrice",
                params="[]",
                id="gas_price",
            ),
            timeout=15.0,
        )
        gas_price_wei = 0
        if gas_resp.success:
            gas_hex = json.loads(gas_resp.result).removeprefix("0x")
            gas_price_wei = int(gas_hex, 16) if gas_hex else 0
        gas_price_gwei = gas_price_wei / 1e9

        # Estimate gas cost (close LP + optional swap + open LP)
        gas_units = 800_000  # conservative for close + swap + open on Base
        gas_cost_eth = gas_units * gas_price_wei / 1e18

        # Get ETH price for USD conversion
        try:
            eth_price_resp = self._client.market.GetPrice(gateway_pb2.PriceRequest(token="ETH", quote="USD"))
            eth_price = float(eth_price_resp.price)
        except Exception:  # noqa: BLE001 - gateway may raise any gRPC error
            eth_price = 2500.0  # fallback

        gas_cost_usd = gas_cost_eth * eth_price

        # Estimate daily fee revenue (conservative)
        # For Base L2, gas is very cheap (~$0.01-0.05 per tx), so most rebalances are viable
        fee_tier = args.get("fee_tier", 3000)
        fee_pct = fee_tier / 1_000_000
        # Conservative estimate: $5k daily volume, 10% liquidity share
        estimated_daily_volume = float(args.get("estimated_daily_volume", 5000))
        our_share = float(args.get("our_liquidity_share", 0.1))
        expected_daily_fees = estimated_daily_volume * fee_pct * our_share

        net_ev = expected_daily_fees - gas_cost_usd
        viable = net_ev > 0

        reason = (
            f"Net EV ${net_ev:.4f}/day (fees ${expected_daily_fees:.4f} - gas ${gas_cost_usd:.4f})"
            if viable
            else f"Negative EV: gas ${gas_cost_usd:.4f} > daily fees ${expected_daily_fees:.4f}"
        )

        return ToolResponse(
            status="success",
            data={
                "viable": viable,
                "reason": reason,
                "breakdown": {
                    "estimated_gas_cost_usd": f"{gas_cost_usd:.6f}",
                    "expected_daily_fees_usd": f"{expected_daily_fees:.6f}",
                    "net_ev_usd": f"{net_ev:.6f}",
                    "gas_price_gwei": f"{gas_price_gwei:.4f}",
                    "eth_price_usd": f"{eth_price:.2f}",
                },
            },
        )

    # ── VAULT TOOLS ──────────────────────────────────────────────────────

    # Common tokens for portfolio value estimation in get_risk_metrics
    _RISK_METRIC_TOKENS: ClassVar[list[str]] = ["USDC", "USDT", "WETH", "WBTC", "DAI", "ETH"]

    # Intent-type to gas operation mapping for estimate_gas tool
    _INTENT_GAS_OPS: ClassVar[dict[str, list[str]]] = {
        "SWAP": ["approve", "swap_simple"],
        "LP_OPEN": ["approve", "lp_mint"],
        "LP_CLOSE": ["lp_decrease_liquidity", "lp_collect", "lp_burn"],
        "BORROW": ["lending_borrow"],
        "SUPPLY": ["approve", "lending_supply"],
        "REPAY": ["approve", "lending_repay"],
        "WITHDRAW": ["lending_withdraw"],
        "BRIDGE": ["approve", "bridge_deposit"],
        "FLASH_LOAN": ["flash_loan"],
        "STAKE": ["approve", "swap_simple"],
        "UNSTAKE": ["swap_simple"],
    }

    # Intent types that map to MEDIUM or HIGH risk actions for validation purposes.
    _HIGH_RISK_INTENT_TYPES: ClassVar[frozenset[str]] = frozenset({"lp_open", "borrow", "flash_loan"})

    # Map intent types to canonical action tool names for accurate policy checks.
    _INTENT_TO_TOOL_NAME: ClassVar[dict[str, str]] = {
        "swap": "swap_tokens",
        "lp_open": "open_lp_position",
        "lp_close": "close_lp_position",
        "supply": "supply_lending",
        "borrow": "borrow_lending",
        "repay": "repay_lending",
        "flash_loan": "flash_loan",
    }

    def _execute_validate_risk(self, args: dict) -> ToolResponse:
        """Run pre-trade risk validation without executing.

        Evaluates the proposed trade against all PolicyEngine checks (token,
        protocol, chain, spend limits, cooldown, rate limits, circuit breaker)
        and returns a structured report of violations, warnings, and a risk
        summary including remaining daily spend capacity.

        This is a read-only operation -- no state is mutated.
        """
        intent_type = args.get("intent_type", "")
        params = args.get("params", {})
        chain = args.get("chain", self._default_chain)

        # Build a synthetic action-args dict from intent params + chain so
        # that PolicyEngine checks can inspect the same fields they see on
        # real action tool calls.
        synthetic_args = self._build_synthetic_args(intent_type, params, chain)

        # Determine the appropriate risk tier for this intent type.
        intent_lower = intent_type.lower()
        if intent_lower in self._HIGH_RISK_INTENT_TYPES:
            risk_tier = RiskTier.HIGH
        elif intent_lower == "hold":
            risk_tier = RiskTier.NONE
        else:
            risk_tier = RiskTier.MEDIUM

        # Create a synthetic tool definition to represent this intent for
        # policy checking. Use the canonical action tool name so that
        # allowed_tools, rebalance gate, and approval threshold checks
        # match the same names used during real execution.
        mapped_tool_name = self._INTENT_TO_TOOL_NAME.get(intent_lower, "validate_risk")
        base_tool = self._catalog.get("validate_risk")
        if base_tool is None:
            raise ToolValidationError(
                "validate_risk tool is not registered in the catalog",
                tool_name="validate_risk",
            )
        synthetic_tool = ToolDefinition(
            name=mapped_tool_name,
            description="Synthetic tool for risk validation",
            category=ToolCategory.PLANNING,
            risk_tier=risk_tier,
            request_schema=base_tool.request_schema,
            response_schema=base_tool.response_schema,
        )

        # Run policy checks (read-only: does not record trade or tool call).
        decision = self._policy_engine.check(synthetic_tool, synthetic_args)

        # Collect violations with structured detail.
        violations = []
        for v in decision.violations:
            # Infer a check name from the violation text for structured output.
            check_name = self._infer_check_name(v)
            violations.append(
                {
                    "check": check_name,
                    "message": v,
                    "severity": "blocking",
                }
            )

        # Generate warnings for near-limit scenarios.
        warnings = self._generate_risk_warnings(synthetic_args)

        # Build risk summary.
        estimated_value_usd = self._policy_engine._estimate_usd_value(synthetic_args)
        daily_spend_used = self._policy_engine._daily_spend_usd
        daily_limit = self._policy_engine.policy.max_daily_spend_usd
        daily_remaining = max(daily_limit - daily_spend_used, Decimal("0"))

        risk_summary = {
            "estimated_value_usd": str(estimated_value_usd),
            "daily_spend_remaining_usd": str(daily_remaining),
            "daily_spend_used_usd": str(daily_spend_used),
            "daily_spend_limit_usd": str(daily_limit),
            "single_trade_limit_usd": str(self._policy_engine.policy.max_single_trade_usd),
        }

        is_valid = len(violations) == 0

        return ToolResponse(
            status="success",
            data={
                "valid": is_valid,
                "violations": violations,
                "warnings": warnings,
                "risk_summary": risk_summary,
            },
            explanation=(
                "All policy checks passed. Trade is within risk limits."
                if is_valid
                else f"Trade blocked by {len(violations)} policy violation(s). Review violations for details."
            ),
        )

    @staticmethod
    def _build_synthetic_args(intent_type: str, params: dict, chain: str) -> dict:
        """Build a synthetic args dict suitable for PolicyEngine checks.

        Maps intent parameter names to the canonical field names that
        policy checks look for (token_in, token_out, protocol, chain, etc.).
        """
        synthetic: dict = {"chain": chain}
        intent_lower = intent_type.lower()

        if intent_lower == "swap":
            synthetic["token_in"] = params.get("from_token") or params.get("token_in", "")
            synthetic["token_out"] = params.get("to_token") or params.get("token_out", "")
            synthetic["amount"] = params.get("amount") or params.get("amount_usd", "")
            synthetic["protocol"] = params.get("protocol", "")
        elif intent_lower in ("lp_open", "lp_close"):
            pool = params.get("pool", "")
            if "/" in pool:
                parts = pool.split("/")
                synthetic["token_a"] = parts[0]
                synthetic["token_b"] = parts[1] if len(parts) > 1 else ""
            else:
                synthetic["token_a"] = params.get("token_a", "")
                synthetic["token_b"] = params.get("token_b", "")
            synthetic["amount_a"] = params.get("amount0") or params.get("amount_a", "")
            synthetic["amount_b"] = params.get("amount1") or params.get("amount_b", "")
            synthetic["protocol"] = params.get("protocol", "")
            if "position_id" in params:
                synthetic["position_id"] = params["position_id"]
        elif intent_lower in ("supply", "repay"):
            synthetic["token"] = params.get("token", "")
            synthetic["amount"] = params.get("amount", "")
            synthetic["protocol"] = params.get("protocol", "")
        elif intent_lower == "borrow":
            synthetic["token"] = params.get("borrow_token") or params.get("token", "")
            synthetic["amount"] = params.get("borrow_amount") or params.get("amount", "")
            synthetic["collateral_token"] = params.get("collateral_token", "")
            synthetic["collateral_amount"] = params.get("collateral_amount", "")
            synthetic["protocol"] = params.get("protocol", "")
        else:
            # Fallback: pass through all params so policy checks can inspect them.
            synthetic.update(params)

        # Carry through intent_type for intent-type-allowed check.
        synthetic["intent_type"] = intent_type

        return synthetic

    def _generate_risk_warnings(self, synthetic_args: dict) -> list[dict]:
        """Generate warnings for near-limit scenarios (non-blocking advisories)."""
        warnings: list[dict] = []
        policy = self._policy_engine.policy

        estimated_usd = self._policy_engine._estimate_usd_value(synthetic_args)

        # Warn if trade would use >80% of single-trade limit
        if estimated_usd > 0 and policy.max_single_trade_usd > 0:
            pct_of_single = estimated_usd / policy.max_single_trade_usd * 100
            if pct_of_single > Decimal("80") and estimated_usd <= policy.max_single_trade_usd:
                warnings.append(
                    {
                        "check": "single_trade_near_limit",
                        "message": f"Trade value ${estimated_usd:.2f} is {pct_of_single:.0f}% of "
                        f"single-trade limit ${policy.max_single_trade_usd}.",
                        "severity": "warning",
                    }
                )

        # Warn if projected daily spend would exceed 80% of daily limit
        projected_daily = self._policy_engine._daily_spend_usd + estimated_usd
        if projected_daily > 0 and policy.max_daily_spend_usd > 0:
            pct_of_daily = projected_daily / policy.max_daily_spend_usd * 100
            if pct_of_daily > Decimal("80") and projected_daily <= policy.max_daily_spend_usd:
                warnings.append(
                    {
                        "check": "daily_spend_near_limit",
                        "message": f"Projected daily spend ${projected_daily:.2f} is {pct_of_daily:.0f}% of "
                        f"daily limit ${policy.max_daily_spend_usd}.",
                        "severity": "warning",
                    }
                )

        # Warn about cooldown if it's partially elapsed
        if self._policy_engine._last_trade_timestamp > 0 and policy.cooldown_seconds > 0:
            elapsed = time.time() - self._policy_engine._last_trade_timestamp
            if elapsed >= policy.cooldown_seconds:
                # Cooldown fully elapsed -- no warning needed
                pass
            elif elapsed >= policy.cooldown_seconds * 0.5:
                remaining = int(policy.cooldown_seconds - elapsed)
                warnings.append(
                    {
                        "check": "cooldown_partial",
                        "message": f"Cooldown has {remaining}s remaining (will be blocking at execution time).",
                        "severity": "warning",
                    }
                )

        # Warn about trade rate approaching limit
        now = time.time()
        recent_trades = [t for t in self._policy_engine._trades_this_hour if now - t < 3600]
        if len(recent_trades) >= policy.max_trades_per_hour - 1 and len(recent_trades) < policy.max_trades_per_hour:
            warnings.append(
                {
                    "check": "trade_rate_near_limit",
                    "message": f"Trade count {len(recent_trades)}/{policy.max_trades_per_hour} per hour. "
                    "Next trade will hit the rate limit.",
                    "severity": "warning",
                }
            )

        # Warn about consecutive failures approaching circuit breaker
        failures = self._policy_engine._consecutive_failures
        if (
            failures > 0
            and failures >= policy.max_consecutive_failures - 1
            and not self._policy_engine.is_circuit_breaker_tripped
        ):
            warnings.append(
                {
                    "check": "circuit_breaker_near",
                    "message": f"Consecutive failures: {failures}/{policy.max_consecutive_failures}. "
                    "One more failure will trip the circuit breaker.",
                    "severity": "warning",
                }
            )

        return warnings

    @staticmethod
    def _infer_check_name(violation_text: str) -> str:
        """Infer a machine-readable check name from a policy violation message."""
        text_lower = violation_text.lower()
        if "tool" in text_lower and "not in the allowed set" in text_lower:
            return "tool_not_allowed"
        if "chain" in text_lower and "not allowed" in text_lower:
            return "chain_not_allowed"
        if "protocol" in text_lower and "not allowed" in text_lower:
            return "protocol_not_allowed"
        if "token" in text_lower and "not in the allowed set" in text_lower:
            return "token_not_allowed"
        if "intent type" in text_lower and "not allowed" in text_lower:
            return "intent_type_not_allowed"
        if "single-trade limit" in text_lower:
            return "single_trade_limit"
        if "daily" in text_lower and ("spend" in text_lower or "limit" in text_lower):
            return "daily_spend_limit"
        if "rate limit" in text_lower and "tool call" in text_lower:
            return "tool_rate_limit"
        if "rate limit" in text_lower and "trade" in text_lower:
            return "trade_rate_limit"
        if "circuit breaker" in text_lower:
            return "circuit_breaker"
        if "cooldown" in text_lower:
            return "cooldown"
        if "stop-loss" in text_lower:
            return "stop_loss"
        if "position" in text_lower and "size" in text_lower:
            return "position_size_limit"
        if "approval threshold" in text_lower:
            return "approval_gate"
        if "rebalance" in text_lower:
            return "rebalance_gate"
        if "wallet" in text_lower and "not in the allowed set" in text_lower:
            return "execution_wallet_not_allowed"
        return "policy_violation"

    def _execute_estimate_gas(self, args: dict) -> ToolResponse:
        """Estimate gas for an intent using static gas tables with chain overrides."""
        from almanak.framework.intents.compiler import get_gas_estimate

        chain = args.get("chain", self._default_chain)
        intent_type = args.get("intent_type", "SWAP").upper()
        ops = self._INTENT_GAS_OPS.get(intent_type)
        if ops is None:
            valid = ", ".join(sorted(self._INTENT_GAS_OPS))
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.INVALID_INTENT_TYPE,
                    f"Unknown intent_type '{intent_type}'. Valid types: {valid}",
                    recoverable=True,
                ),
            )
        total_gas = sum(get_gas_estimate(chain, op) for op in ops)

        return ToolResponse(
            status="success",
            data={
                "gas_units": total_gas,
                "gas_price_gwei": "",
                "cost_usd": "",
                "cost_native": "",
            },
            explanation=(
                f"Static estimate for {intent_type} on {chain}: {total_gas} gas units "
                f"(ops: {', '.join(ops)}). Gas price and USD cost require live RPC data."
            ),
        )

    async def _execute_get_risk_metrics(self, args: dict) -> ToolResponse:
        """Get portfolio risk metrics from current wallet balances.

        Fetches live portfolio value from gateway balances, records a snapshot
        in the PolicyEngine, and computes rolling risk metrics (max drawdown,
        volatility, Sharpe ratio, 95% VaR) from the snapshot history.
        """
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)

        # Query balances for common tokens to compute portfolio value
        common_tokens = self._RISK_METRIC_TOKENS
        total_value_usd = Decimal("0")
        any_success = False

        try:
            requests = [
                gateway_pb2.BalanceRequest(
                    token=token,
                    chain=chain,
                    wallet_address=self._wallet_address,
                )
                for token in common_tokens
            ]
            batch_resp = self._client.market.BatchGetBalances(gateway_pb2.BatchBalanceRequest(requests=requests))

            for resp in batch_resp.responses:
                if resp.error:
                    logger.debug("Balance query error for risk metrics: %s", resp.error)
                    continue
                if resp.balance_usd:
                    try:
                        total_value_usd += Decimal(resp.balance_usd)
                        any_success = True
                    except (ArithmeticError, ValueError) as exc:
                        logger.debug("Invalid balance_usd in risk metrics: %s", exc)
                        continue
                else:
                    any_success = True  # zero balance is still a valid response
        except Exception as e:  # noqa: BLE001 - gateway may raise any gRPC error
            logger.warning("Failed to fetch balances for risk metrics: %s", e)
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.GATEWAY_ERROR,
                    f"Failed to fetch balances for risk metrics: {e}",
                    recoverable=True,
                ),
            )

        if not any_success:
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.ALL_QUERIES_FAILED,
                    "All balance queries returned errors; portfolio value is unknown.",
                    recoverable=True,
                ),
            )

        # Record this observation for rolling risk calculations
        self._policy_engine.update_portfolio_value(total_value_usd)

        # Compute risk metrics from snapshot history
        metrics = self._policy_engine.get_risk_metrics()

        n = metrics["data_points"]
        warnings = metrics["warnings"]

        # Build explanation based on data availability
        if n < 3:
            explanation = (
                f"Portfolio value derived from on-chain balances ({n} snapshot(s) recorded). "
                "Need at least 3 snapshots to compute volatility/Sharpe and 10 for VaR."
            )
        elif n < 10:
            explanation = (
                f"Portfolio value and partial risk metrics from {n} snapshots. "
                "VaR requires at least 10 data points for a reliable estimate."
            )
        else:
            explanation = f"Full risk metrics computed from {n} portfolio snapshots."

        return ToolResponse(
            status="success",
            data={
                "portfolio_value_usd": str(total_value_usd),
                "var_95": metrics["var_95_pct"],
                "sharpe_ratio": metrics["sharpe_ratio"],
                "volatility_annualized": metrics["volatility_annualized"],
                "max_drawdown_pct": metrics["max_drawdown_pct"],
                "data_points": n,
                "data_sufficient": metrics["data_sufficient"],
                "warnings": warnings,
            },
            explanation=explanation,
        )

    async def _execute_get_vault_state(self, args: dict) -> ToolResponse:
        """Read current state of a Lagoon vault."""
        from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK

        chain = args.get("chain", self._default_chain)
        vault_address = args["vault_address"]
        sdk = LagoonVaultSDK(self._client, chain=chain)

        try:
            total_assets = sdk.get_total_assets(vault_address)
            pending_deposits = sdk.get_pending_deposits(vault_address)
            pending_redeems = sdk.get_pending_redemptions(vault_address)
            share_price = sdk.get_share_price(vault_address)
        except Exception as e:
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.VAULT_READ_FAILED,
                    f"Failed to read vault state: {e}",
                    recoverable=True,
                ),
            )

        return ToolResponse(
            status="success",
            data={
                "status": "active",
                "total_assets": str(total_assets),
                "pending_deposits": str(pending_deposits),
                "pending_redeems": str(pending_redeems),
                "share_price": str(share_price),
            },
        )

    async def _compute_vault_nav(self, vault_address: str, safe_address: str, chain: str) -> int:
        """Compute deterministic NAV for a vault by summing Safe's assets.

        Sums:
        1. Safe's underlying token balance
        2. Silo contract's underlying token balance (pending deposits)
        3. USD value of LP position tokens (if lp_position_id in agent state)

        Returns total in raw underlying token units.
        """
        from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
        from almanak.gateway.proto import gateway_pb2

        sdk = LagoonVaultSDK(self._client, chain=chain)

        # Get underlying token address from vault
        try:
            underlying_token = sdk.get_underlying_token_address(vault_address)
        except Exception as e:
            logger.warning("Failed to read vault underlying token: %s", e)
            # Fallback to current total assets
            return sdk.get_total_assets(vault_address)

        # 1. Read Safe's underlying token balance
        balance_calldata = "0x70a08231" + safe_address.lower().removeprefix("0x").zfill(64)

        balance_resp = self._client.rpc.Call(
            gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_call",
                params=json.dumps([{"to": underlying_token, "data": balance_calldata}, "latest"]),
                id="nav_underlying_balance",
            ),
            timeout=30.0,
        )

        underlying_balance = 0
        if balance_resp.success:
            raw = json.loads(balance_resp.result)
            underlying_balance = int(raw, 16) if raw and raw != "0x" else 0

        # 2. Read silo balance (pending deposits held by vault's silo contract)
        silo_balance = 0
        try:
            silo_address = sdk.get_silo_address(vault_address)
            if silo_address and silo_address != "0x" + "0" * 40:
                silo_resp = self._client.rpc.Call(
                    gateway_pb2.RpcRequest(
                        chain=chain,
                        method="eth_call",
                        params=json.dumps(
                            [
                                {
                                    "to": underlying_token,
                                    "data": "0x70a08231" + silo_address.lower().removeprefix("0x").zfill(64),
                                },
                                "latest",
                            ]
                        ),
                        id="nav_silo_balance",
                    ),
                    timeout=30.0,
                )
                if silo_resp.success:
                    raw = json.loads(silo_resp.result)
                    silo_balance = int(raw, 16) if raw and raw != "0x" else 0
        except Exception:
            logger.warning("Could not read silo balance; NAV may understate vault value")

        total_nav = underlying_balance + silo_balance

        # 3. If there's an LP position, add its token values
        try:
            strategy_id = self._strategy_id
            state_resp = self._client.state.LoadState(gateway_pb2.LoadStateRequest(strategy_id=strategy_id))
            agent_state = json.loads(state_resp.data) if state_resp.data else {}
            lp_position_id = agent_state.get("lp_position_id")

            if lp_position_id:
                lp_result = await self._execute_get_lp_position(
                    {
                        "position_id": str(lp_position_id),
                        "chain": chain,
                    }
                )
                if lp_result.status == "success" and lp_result.data:
                    lp_data = lp_result.data
                    # Get token prices and compute USD value
                    token_a = lp_data.get("token_a", "")
                    token_b = lp_data.get("token_b", "")

                    # Compute principal token amounts from liquidity + tick range
                    lp_usd_value = Decimal("0")
                    liquidity_raw = int(lp_data.get("liquidity", "0"))
                    tick_lower = int(lp_data.get("tick_lower", 0))
                    tick_upper = int(lp_data.get("tick_upper", 0))

                    # current_tick is required for accurate LP valuation --
                    # a midpoint fallback would produce silently wrong NAV.
                    current_tick = lp_data.get("current_tick")
                    if current_tick is not None:
                        current_tick = int(current_tick)
                    else:
                        logger.warning(
                            "current_tick unavailable for LP position %s; skipping LP value in NAV (conservative)",
                            lp_position_id,
                        )

                    # Compute Uni V3 principal amounts using Decimal to avoid
                    # float precision loss at extreme ticks.
                    amount0_raw = 0
                    amount1_raw = 0
                    if liquidity_raw > 0 and tick_upper > tick_lower and current_tick is not None:
                        try:
                            with localcontext() as ctx:
                                ctx.prec = 40
                                base = Decimal("1.0001")
                                liq = Decimal(liquidity_raw)
                                sqrt_lower = (base**tick_lower).sqrt()
                                sqrt_upper = (base**tick_upper).sqrt()
                                sqrt_current = (base**current_tick).sqrt()

                                if current_tick < tick_lower:
                                    amount0_raw = int(liq * (1 / sqrt_lower - 1 / sqrt_upper))
                                elif current_tick >= tick_upper:
                                    amount1_raw = int(liq * (sqrt_upper - sqrt_lower))
                                else:
                                    amount0_raw = int(liq * (1 / sqrt_current - 1 / sqrt_upper))
                                    amount1_raw = int(liq * (sqrt_current - sqrt_lower))
                        except ArithmeticError as e:
                            logger.warning("Could not compute LP principal amounts from tick math: %s", e)

                    # Add uncollected fees to principal amounts
                    amount0_raw += int(lp_data.get("tokens_owed_a", "0"))
                    amount1_raw += int(lp_data.get("tokens_owed_b", "0"))

                    # Price both token amounts
                    for token_addr, amount_raw in [(token_a, amount0_raw), (token_b, amount1_raw)]:
                        if not token_addr or amount_raw <= 0:
                            continue
                        try:
                            price_resp = self._client.market.GetPrice(
                                gateway_pb2.PriceRequest(token=token_addr, quote="USD")
                            )
                            token_price = Decimal(str(price_resp.price))
                            if token_price > 0:
                                from almanak.framework.data.tokens import get_token_resolver

                                resolved = get_token_resolver().resolve(token_addr, chain)
                                amount_human = Decimal(amount_raw) / Decimal(10**resolved.decimals)
                                lp_usd_value += amount_human * token_price
                        except Exception:
                            logger.warning("Could not price LP token %s for NAV", token_addr[:10])

                    # Convert LP USD value to underlying units
                    if lp_usd_value > 0:
                        try:
                            underlying_price_resp = self._client.market.GetPrice(
                                gateway_pb2.PriceRequest(token=underlying_token, quote="USD")
                            )
                            underlying_price = Decimal(str(underlying_price_resp.price))
                            if underlying_price > 0:
                                from almanak.framework.data.tokens import get_token_resolver

                                underlying_resolved = get_token_resolver().resolve(underlying_token, chain)
                                lp_in_underlying = int(
                                    lp_usd_value / underlying_price * Decimal(10**underlying_resolved.decimals)
                                )
                                total_nav += lp_in_underlying
                        except Exception:
                            logger.warning("Could not convert LP value to underlying units; LP excluded from NAV")

        except Exception:
            logger.warning("Could not load agent state for LP position; LP excluded from NAV")

        logger.info(
            "Computed vault NAV: underlying_balance=%d, silo=%d, total=%d",
            underlying_balance,
            silo_balance,
            total_nav,
        )
        return total_nav

    async def _check_settlement_liquidity(
        self, vault_address: str, safe_address: str, chain: str
    ) -> tuple[bool, int, int]:
        """Check if the Safe has enough liquid underlying to cover pending redemptions.

        Returns:
            Tuple of (sufficient, liquid_balance, needed_amount).
        """
        from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
        from almanak.gateway.proto import gateway_pb2

        sdk = LagoonVaultSDK(self._client, chain=chain)

        try:
            pending_redeems = sdk.get_pending_redemptions(vault_address)
        except Exception:
            logger.warning("Cannot verify pending redemptions; failing closed for safety")
            return False, 0, 0

        if pending_redeems == 0:
            return True, 0, 0

        # Convert redeem shares to underlying via on-chain convertToAssets.
        # Uses the ERC-4626 function directly to avoid precision loss from
        # intermediate share_price division (shares are 18 decimals but
        # underlying may be 6, e.g. USDC).
        try:
            needed = sdk.convert_to_assets(vault_address, pending_redeems)
        except (RuntimeError, ValueError, TypeError) as exc:
            logger.warning("Cannot convert pending redemptions to assets; failing closed: %s", exc)
            return False, 0, 0

        # Read Safe's underlying balance
        try:
            underlying_token = sdk.get_underlying_token_address(vault_address)
            balance_calldata = "0x70a08231" + safe_address.lower().removeprefix("0x").zfill(64)
            balance_resp = self._client.rpc.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_call",
                    params=json.dumps([{"to": underlying_token, "data": balance_calldata}, "latest"]),
                    id="liquidity_check_balance",
                ),
                timeout=30.0,
            )
            liquid = 0
            if balance_resp.success:
                raw = json.loads(balance_resp.result)
                liquid = int(raw, 16) if raw and raw != "0x" else 0
        except Exception:
            logger.warning("Cannot read Safe balance for liquidity check; failing closed")
            return False, 0, needed

        return liquid >= needed, liquid, needed

    def _load_settlement_state(self) -> None:
        """Load settlement crash-recovery state from persisted agent state."""
        from almanak.gateway.proto import gateway_pb2

        try:
            resp = self._client.state.LoadState(gateway_pb2.LoadStateRequest(strategy_id=self._strategy_id))
            agent_state = json.loads(resp.data) if resp.data else {}
            settle_state = agent_state.get("_vault_settlement", {})
            self._settlement_phase = settle_state.get("phase", "idle")
            self._settlement_proposed_assets = settle_state.get("proposed_assets", 0)
            self._settlement_nonce = settle_state.get("nonce", 0)
            self._vault_epoch_counter = settle_state.get("epoch_counter", 0)
        except Exception:
            logger.debug("Could not load settlement state (using defaults)")

    def _save_settlement_state(self, phase: str, proposed_assets: int | None = None) -> None:
        """Persist settlement crash-recovery state into agent state.

        Raises on failure for pre-propose phases (proposing) since we must not
        submit irreversible on-chain transactions without durable state.
        Post-propose failures are logged but non-fatal.
        """
        from almanak.gateway.proto import gateway_pb2

        self._settlement_phase = phase
        if proposed_assets is not None:
            self._settlement_proposed_assets = proposed_assets

        try:
            # Load current state, merge settlement state, save back
            try:
                resp = self._client.state.LoadState(gateway_pb2.LoadStateRequest(strategy_id=self._strategy_id))
                agent_state = json.loads(resp.data) if resp.data else {}
            except Exception as load_err:
                import grpc

                if isinstance(load_err, grpc.RpcError) and load_err.code() == grpc.StatusCode.NOT_FOUND:
                    agent_state = {}
                elif "NOT_FOUND" in str(load_err):
                    agent_state = {}
                else:
                    raise
            agent_state["_vault_settlement"] = {
                "phase": self._settlement_phase,
                "proposed_assets": self._settlement_proposed_assets,
                "nonce": self._settlement_nonce,
                "epoch_counter": self._vault_epoch_counter,
            }
            self._client.state.SaveState(
                gateway_pb2.SaveStateRequest(
                    strategy_id=self._strategy_id,
                    data=json.dumps(agent_state).encode(),
                    schema_version=1,
                )
            )
        except Exception:
            if phase in ("proposing", "settling"):
                # Pre-irreversible-action: must not proceed without durable state
                raise
            logger.warning("Failed to persist settlement state (phase=%s, non-fatal)", phase)

    def _vault_preflight_checks(
        self, sdk: Any, vault_address: str, safe_address: str, valuator_address: str
    ) -> str | None:
        """Run on-chain preflight checks before settlement.

        Verifies that the on-chain valuation manager and curator match the
        provided addresses. This mirrors the lifecycle manager's preflight
        checks to prevent settlement with misconfigured vault parameters.

        Returns:
            None if all checks pass, or an error message string.
        """
        try:
            on_chain_valuator = sdk.get_valuation_manager(vault_address)
            if on_chain_valuator.lower() != valuator_address.lower():
                return (
                    f"Preflight failed: on-chain valuation manager {on_chain_valuator} "
                    f"!= provided valuator {valuator_address}"
                )
        except Exception as e:
            return f"Preflight failed: could not verify valuation manager: {e}"

        try:
            on_chain_curator = sdk.get_curator(vault_address)
            if on_chain_curator.lower() != safe_address.lower():
                return f"Preflight failed: on-chain curator {on_chain_curator} != provided safe {safe_address}"
        except Exception as e:
            return f"Preflight failed: could not verify curator: {e}"

        return None

    async def _execute_settle_vault(self, args: dict) -> ToolResponse:
        """Run a vault settlement cycle with crash-recovery state machine.

        Phases: IDLE -> PROPOSING -> PROPOSED -> SETTLING -> SETTLED -> IDLE

        On entry, loads persisted settlement phase and resumes from the
        interrupted point. This prevents orphaned vault state if the process
        crashes between propose and settle.
        """
        from almanak.core.models.params import UpdateTotalAssetsParams
        from almanak.framework.connectors.lagoon.adapter import LagoonVaultAdapter
        from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)
        vault_address = args["vault_address"]
        safe_address = args["safe_address"]
        valuator_address = args["valuator_address"]
        dry_run = args.get("dry_run", False)

        sdk = LagoonVaultSDK(self._client, chain=chain)
        adapter = LagoonVaultAdapter(sdk)

        # Preflight: verify on-chain vault config matches provided args
        preflight_error = self._vault_preflight_checks(sdk, vault_address, safe_address, valuator_address)
        if preflight_error:
            logger.error("settle_vault preflight failed: %s", preflight_error)
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.PREFLIGHT_FAILED,
                    preflight_error,
                ),
            )

        # Load crash-recovery state
        self._load_settlement_state()
        phase = self._settlement_phase

        logger.info(
            "settle_vault entry: phase=%s, nonce=%d, epoch=%d", phase, self._settlement_nonce, self._vault_epoch_counter
        )

        # --- Resume from SETTLED: just finalize ---
        if phase == "settled":
            logger.info("Resuming from SETTLED phase, finalizing")
            return self._finalize_executor_settlement(dry_run)

        # --- Resume from SETTLING: check if settle already succeeded on-chain ---
        if phase == "settling":
            proposed = self._settlement_proposed_assets
            on_chain_total = sdk.get_total_assets(vault_address)
            if on_chain_total == proposed and self._settlement_nonce > 0:
                logger.info(
                    "Settle already confirmed on-chain (total_assets=%d, nonce=%d)",
                    on_chain_total,
                    self._settlement_nonce,
                )
                self._save_settlement_state("settled")
                return self._finalize_executor_settlement(dry_run)
            # Otherwise retry settle
            logger.info("Retrying settle (on-chain=%d, proposed=%d)", on_chain_total, proposed)
            return await self._do_settle_deposit_and_redeem(
                args,
                sdk,
                adapter,
                vault_address,
                safe_address,
                valuator_address,
                proposed,
                chain,
                dry_run,
            )

        # --- Resume from PROPOSED: skip propose, go to settle ---
        if phase == "proposed":
            proposed = self._settlement_proposed_assets
            logger.info("Resuming from PROPOSED phase, proceeding to settle (proposed=%d)", proposed)
            return await self._do_settle_deposit_and_redeem(
                args,
                sdk,
                adapter,
                vault_address,
                safe_address,
                valuator_address,
                proposed,
                chain,
                dry_run,
            )

        # --- Resume from PROPOSING: check if propose already succeeded on-chain ---
        if phase == "proposing":
            proposed = self._settlement_proposed_assets
            on_chain_proposed = sdk.get_proposed_total_assets(vault_address)
            if on_chain_proposed == proposed and self._settlement_nonce > 0:
                logger.info(
                    "Propose already confirmed on-chain (proposed=%d, nonce=%d)",
                    on_chain_proposed,
                    self._settlement_nonce,
                )
                self._save_settlement_state("proposed")
                return await self._do_settle_deposit_and_redeem(
                    args,
                    sdk,
                    adapter,
                    vault_address,
                    safe_address,
                    valuator_address,
                    proposed,
                    chain,
                    dry_run,
                )
            # Otherwise retry from propose
            logger.info("Retrying propose (on-chain=%d, intended=%d)", on_chain_proposed, proposed)

        # --- Start fresh from IDLE (or retry from PROPOSING) ---

        # Only compute NAV for fresh starts (idle), not for proposing retries
        if phase in ("idle", "proposing"):
            new_total_assets = await self._determine_nav(args, sdk, vault_address, safe_address, chain)
        else:
            new_total_assets = self._settlement_proposed_assets

        # For freshly deployed vaults, pendingDepositRequest may revert before
        # the first settlement initializes the vault. Default to 0 in that case.
        try:
            pending_deposits = sdk.get_pending_deposits(vault_address)
        except Exception as e:
            logger.warning("get_pending_deposits failed (vault may be uninitialized): %s", e)
            pending_deposits = 0

        # Check settlement liquidity (can Safe cover pending redemptions?)
        sufficient, liquid, needed = await self._check_settlement_liquidity(vault_address, safe_address, chain)
        if not sufficient:
            logger.warning(
                "Insufficient liquidity for settlement: liquid=%d, needed=%d, shortfall=%d",
                liquid,
                needed,
                needed - liquid,
            )
            self._fire_alert(
                f"Vault settlement blocked: insufficient liquidity (have {liquid}, need {needed})",
                severity="critical",
            )
            return ToolResponse(
                status="error",
                error=_error_dict(
                    AgentErrorCode.INSUFFICIENT_LIQUIDITY,
                    f"Safe has {liquid} underlying but {needed} needed for pending redemptions",
                    recoverable=True,
                ),
                decision_hints={
                    "action_needed": "close_lp_position",
                    "shortfall": str(needed - liquid),
                    "liquid_balance": str(liquid),
                    "needed_amount": str(needed),
                },
            )

        # Phase: PROPOSING -- propose valuation (from valuator/EOA)
        self._settlement_nonce += 1
        self._save_settlement_state("proposing", proposed_assets=new_total_assets)

        propose_params = UpdateTotalAssetsParams(
            vault_address=vault_address,
            valuator_address=valuator_address,
            new_total_assets=new_total_assets,
            pending_deposits=pending_deposits,
        )
        propose_bundle = adapter.build_propose_valuation_bundle(propose_params)
        propose_bytes = json.dumps(propose_bundle.to_dict()).encode()

        logger.info(
            "settle_vault: propose (valuator=%s, total_assets=%s, pending=%s, nonce=%d)",
            valuator_address[:10],
            new_total_assets,
            pending_deposits,
            self._settlement_nonce,
        )

        # C6 fix: propose tx comes from valuator EOA, not Safe -- don't force is_safe_wallet
        propose_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=propose_bytes,
                dry_run=dry_run,
                simulation_enabled=self._resolve_simulation_flag(valuator_address, tool_name="settle_vault.propose"),
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=valuator_address,
            )
        )

        logger.info(
            "settle_vault propose result: success=%s, tx_hashes=%s, error=%s",
            propose_resp.success,
            list(propose_resp.tx_hashes) if propose_resp.tx_hashes else [],
            propose_resp.error[:100] if propose_resp.error else "none",
        )

        if not propose_resp.success and not dry_run:
            self._save_settlement_state("idle")
            self._fire_alert(
                f"Vault propose NAV failed: {propose_resp.error[:200] if propose_resp.error else 'unknown'}",
                severity="critical",
            )
            raise ExecutionFailedError(
                f"Vault propose failed: {propose_resp.error}",
                tool_name="settle_vault",
            )

        # Phase: PROPOSED
        self._save_settlement_state("proposed")

        return await self._do_settle_deposit_and_redeem(
            args,
            sdk,
            adapter,
            vault_address,
            safe_address,
            valuator_address,
            new_total_assets,
            chain,
            dry_run,
        )

    async def _determine_nav(self, args: dict, sdk: Any, vault_address: str, safe_address: str, chain: str) -> int:
        """Determine NAV for settlement: compute or validate LLM-provided value."""
        computed_nav = await self._compute_vault_nav(vault_address, safe_address, chain)

        if args.get("new_total_assets"):
            new_total_assets = int(args["new_total_assets"])

            # Initialization settlement: brand new vault with totalAssets=0 on-chain.
            # Accept new_total_assets=0 without NAV guard -- Safe may hold pre-existing
            # funds that aren't part of this vault.
            if new_total_assets == 0:
                on_chain_total = sdk.get_total_assets(vault_address)
                if on_chain_total == 0:
                    logger.info("settle_vault: initialization settlement (on-chain totalAssets=0), accepting NAV=0")
                    return 0

            if computed_nav > 0:
                delta_bps = abs(new_total_assets - computed_nav) * 10000 // computed_nav if computed_nav else 0
                logger.info(
                    "settle_vault NAV cross-check: llm=%d, computed=%d, delta=%d bps",
                    new_total_assets,
                    computed_nav,
                    delta_bps,
                )
                from almanak.framework.vault.lifecycle import validate_nav_change_bps

                ok, reason = validate_nav_change_bps(computed_nav, new_total_assets)
                if not ok:
                    raise RiskBlockedError(
                        f"NAV update rejected: LLM proposed {new_total_assets} but computed NAV is {computed_nav}. {reason}",
                        tool_name="settle_vault",
                        suggestion="Omit new_total_assets to use the deterministic computed value.",
                    )
            else:
                # computed_nav == 0: reject LLM override -- cannot validate without independent computation
                logger.warning(
                    "settle_vault: rejecting LLM-provided new_total_assets=%d because computed NAV is 0 "
                    "(cannot validate). Falling back to on-chain total assets.",
                    new_total_assets,
                )
                return sdk.get_total_assets(vault_address)
            return new_total_assets

        new_total_assets = computed_nav if computed_nav > 0 else sdk.get_total_assets(vault_address)
        logger.info("settle_vault using computed NAV: %d", new_total_assets)
        return new_total_assets

    async def _do_settle_deposit_and_redeem(
        self,
        _args: dict,
        sdk: Any,
        adapter: Any,
        vault_address: str,
        safe_address: str,
        _valuator_address: str,
        new_total_assets: int,
        chain: str,
        dry_run: bool,
    ) -> ToolResponse:
        """Execute settle-deposit (and optionally settle-redeem) transactions."""
        from almanak.core.models.params import SettleDepositParams, SettleRedeemParams
        from almanak.gateway.proto import gateway_pb2

        # Phase: SETTLING
        self._save_settlement_state("settling")

        settle_params = SettleDepositParams(
            vault_address=vault_address,
            safe_address=safe_address,
            total_assets=new_total_assets,
        )
        settle_bundle = adapter.build_settle_deposit_bundle(settle_params)
        settle_bytes = json.dumps(settle_bundle.to_dict()).encode()

        logger.info(
            "settle_vault: settle_deposit (safe=%s, total_assets=%s)",
            safe_address[:10],
            new_total_assets,
        )

        settle_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=settle_bytes,
                dry_run=dry_run,
                simulation_enabled=self._resolve_simulation_flag(
                    safe_address, tool_name="settle_vault.settle", is_safe_wallet=True
                ),
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=safe_address,
            )
        )

        logger.info(
            "settle_vault settle_deposit result: success=%s, tx_hashes=%s, error=%s",
            settle_resp.success,
            list(settle_resp.tx_hashes) if settle_resp.tx_hashes else [],
            settle_resp.error[:100] if settle_resp.error else "none",
        )

        if not settle_resp.success and not dry_run:
            # Revert to PROPOSED so next call retries settle (not propose)
            self._save_settlement_state("proposed")
            self._fire_alert(
                f"Vault settleDeposit failed: {settle_resp.error[:200] if settle_resp.error else 'unknown'}",
                severity="critical",
            )
            raise ExecutionFailedError(
                f"Vault settlement failed: {settle_resp.error}",
                tool_name="settle_vault",
            )

        tx_hashes = list(settle_resp.tx_hashes) if settle_resp.tx_hashes else []

        # C6 fix: settle redeems if pending
        try:
            pending_redeems = sdk.get_pending_redemptions(vault_address)
        except Exception as e:
            logger.warning("Could not read pending redemptions after settlement: %s (defaulting to 0)", e)
            pending_redeems = 0

        if pending_redeems > 0:
            logger.info("settle_vault: settling %d pending redemptions", pending_redeems)
            redeem_params = SettleRedeemParams(
                vault_address=vault_address,
                safe_address=safe_address,
                total_assets=new_total_assets,
            )
            redeem_bundle = adapter.build_settle_redeem_bundle(redeem_params)
            redeem_bytes = json.dumps(redeem_bundle.to_dict()).encode()

            redeem_resp = self._client.execution.Execute(
                gateway_pb2.ExecuteRequest(
                    action_bundle=redeem_bytes,
                    dry_run=dry_run,
                    simulation_enabled=self._resolve_simulation_flag(
                        safe_address, tool_name="settle_vault.redeem", is_safe_wallet=True
                    ),
                    strategy_id=self._strategy_id,
                    chain=chain,
                    wallet_address=safe_address,
                )
            )
            if redeem_resp.success:
                if redeem_resp.tx_hashes:
                    tx_hashes.extend(list(redeem_resp.tx_hashes))
                logger.info("settle_vault: settle_redeem succeeded")
            else:
                logger.warning(
                    "settle_vault: settle_redeem failed: %s",
                    redeem_resp.error[:100] if redeem_resp.error else "unknown",
                )

        # Phase: SETTLED -> finalize
        self._save_settlement_state("settled")
        return self._finalize_executor_settlement(dry_run, tx_hashes=tx_hashes)

    def _finalize_executor_settlement(self, dry_run: bool, tx_hashes: list[str] | None = None) -> ToolResponse:
        """Finalize settlement: increment epoch, reset phase to idle, return response."""
        self._vault_epoch_counter += 1
        new_total_assets = self._settlement_proposed_assets
        epoch_id = self._vault_epoch_counter

        # Reset state to idle
        self._settlement_nonce = 0
        self._save_settlement_state("idle")

        status = "simulated" if dry_run else "success"
        tx_hash = tx_hashes[0] if tx_hashes else None

        return ToolResponse(
            status=status,
            data={
                "status": status,
                "new_total_assets": str(new_total_assets),
                "epoch_id": epoch_id,
                "tx_hash": tx_hash,
                "tx_hashes": tx_hashes or [],
                "message": f"Settlement complete. Total assets: {new_total_assets}, epoch: {epoch_id}",
            },
        )

    async def _execute_teardown_vault(self, args: dict) -> ToolResponse:
        """Deterministic vault teardown with crash-recovery state machine.

        Phases: lp_closing -> swapping -> settling -> torn_down
        Progress is persisted after each phase so partial failures resume
        from the interrupted step instead of retrying from scratch.
        """
        from almanak.framework.agent_tools.policy import TEARDOWN_REQUIRED_TOOLS
        from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
        from almanak.gateway.proto import gateway_pb2

        # Pre-flight: verify all required sub-tools are permitted by policy.
        # Without this check, teardown would fail mid-execution with cryptic
        # "not in the allowed set" errors from individual sub-calls.
        allowed = self._policy_engine.policy.allowed_tools
        if allowed is not None:
            allowed_set = set(allowed)
            missing = TEARDOWN_REQUIRED_TOOLS - allowed_set
            if missing:
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.TEARDOWN_MISSING_SUB_TOOLS,
                        f"teardown_vault requires these sub-tools to be in allowed_tools: "
                        f"{sorted(missing)}. Add them to the policy's allowed_tools set.",
                    ),
                )

        chain = args.get("chain", self._default_chain)
        vault_address = args["vault_address"]
        safe_address = args["safe_address"]
        valuator_address = args["valuator_address"]
        dry_run = args.get("dry_run", False)

        tx_hashes: list[str] = []
        positions_closed = 0
        swaps_executed = 0

        # 1. Load agent state (includes teardown progress if resuming)
        strategy_id = self._strategy_id
        agent_state: dict = {}
        try:
            state_resp = self._client.state.LoadState(gateway_pb2.LoadStateRequest(strategy_id=strategy_id))
            agent_state = json.loads(state_resp.data) if state_resp.data else {}
        except Exception:
            logger.warning("No agent state found during teardown; starting from clean state")

        teardown_state = agent_state.get("_teardown", {})
        teardown_phase = teardown_state.get("phase", "start")

        # Already completed
        if agent_state.get("phase") == "torn_down" or teardown_phase == "torn_down":
            return ToolResponse(
                status="success",
                data={"status": "success", "message": "Vault already torn down"},
            )

        def _save_teardown_progress(phase: str, **extra: Any) -> None:
            """Persist teardown progress so partial failures can resume."""
            if dry_run:
                return
            teardown_state["phase"] = phase
            teardown_state.update(extra)
            agent_state["_teardown"] = teardown_state
            try:
                self._client.state.SaveState(
                    gateway_pb2.SaveStateRequest(
                        strategy_id=strategy_id,
                        data=json.dumps(agent_state).encode(),
                        schema_version=1,
                    )
                )
            except Exception:
                logger.warning("Failed to persist teardown progress (phase=%s)", phase)

        # 2. Close LP positions (skip if already done in a previous attempt)
        lp_close_failed = False
        lp_close_error = ""
        lp_position_id = agent_state.get("lp_position_id")
        if lp_position_id and teardown_phase in ("start", "lp_closing"):
            _save_teardown_progress("lp_closing")
            logger.info("teardown_vault: closing LP position %s", lp_position_id)
            try:
                close_result = await self.execute(
                    "close_lp_position",
                    {
                        "position_id": str(lp_position_id),
                        "chain": chain,
                        "execution_wallet": safe_address,
                        "dry_run": dry_run,
                    },
                )
                if close_result.status == "success":
                    positions_closed += 1
                    if close_result.data and close_result.data.get("tx_hash"):
                        tx_hashes.append(close_result.data["tx_hash"])
                    agent_state["lp_position_id"] = None
                    _save_teardown_progress("lp_closed")
                else:
                    lp_close_failed = True
                    lp_close_error = str(close_result.error)
                    logger.warning("teardown_vault: LP close failed: %s", close_result.error)
            except Exception as e:
                lp_close_failed = True
                lp_close_error = str(e)
                logger.warning("teardown_vault: LP close error: %s", e)

            if lp_close_failed:
                _save_teardown_progress("lp_closing", error=lp_close_error)
                self._fire_alert(
                    f"Vault teardown blocked: LP close failed for position {lp_position_id}: {lp_close_error[:200]}",
                    severity="critical",
                )
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.TEARDOWN_LP_CLOSE_FAILED,
                        f"LP position close failed: {lp_close_error}. Will retry on next teardown attempt.",
                        recoverable=True,
                    ),
                )
        elif teardown_phase in ("lp_closed", "swapping", "settling"):
            # LP already closed in a previous attempt
            positions_closed = teardown_state.get("positions_closed", 0)

        # 3. Swap non-underlying tokens to underlying
        sdk = LagoonVaultSDK(self._client, chain=chain)
        try:
            underlying_token = sdk.get_underlying_token_address(vault_address)
        except Exception:
            underlying_token = None

        if underlying_token and teardown_phase in ("start", "lp_closing", "lp_closed", "swapping"):
            _save_teardown_progress("swapping", positions_closed=positions_closed)

            # Build token list from LP position data and agent state
            tokens_to_swap: set[str] = set()

            # Get tokens from the LP position that was just closed
            if lp_position_id:
                try:
                    lp_info = await self._execute_get_lp_position({"position_id": str(lp_position_id), "chain": chain})
                    if lp_info.status == "success" and lp_info.data:
                        for key in ("token_a", "token_b"):
                            addr = lp_info.data.get(key, "")
                            if addr and addr.lower() != underlying_token.lower():
                                tokens_to_swap.add(addr)
                except Exception:
                    logger.warning("teardown_vault: could not read LP tokens for swap list")

            # Check agent state for any additional known token addresses
            for key in ("token_a", "token_b", "almanak_token"):
                addr = agent_state.get(key, "")
                if addr and addr.lower() != underlying_token.lower():
                    tokens_to_swap.add(addr)

            for token_addr in tokens_to_swap:
                try:
                    balance_result = await self.execute(
                        "get_balance",
                        {
                            "token": token_addr,
                            "chain": chain,
                            "wallet_address": safe_address,
                        },
                    )
                    if (
                        balance_result.status == "success"
                        and balance_result.data
                        and float(balance_result.data.get("balance", "0")) > 0
                    ):
                        swap_result = await self.execute(
                            "swap_tokens",
                            {
                                "token_in": token_addr,
                                "token_out": underlying_token,
                                "amount": balance_result.data["balance"],
                                "chain": chain,
                                "execution_wallet": safe_address,
                                "dry_run": dry_run,
                            },
                        )
                        if swap_result.status == "success":
                            swaps_executed += 1
                            if swap_result.data and swap_result.data.get("tx_hash"):
                                tx_hashes.append(swap_result.data["tx_hash"])
                except Exception as e:
                    logger.warning("teardown_vault: swap %s failed: %s (tokens may remain in Safe)", token_addr[:10], e)

            _save_teardown_progress("swapped", positions_closed=positions_closed, swaps_executed=swaps_executed)

        # 4. Final settlement
        settle_failed = False
        if teardown_phase not in ("settling_done", "torn_down"):
            _save_teardown_progress("settling")
            try:
                settle_result = await self.execute(
                    "settle_vault",
                    {
                        "vault_address": vault_address,
                        "safe_address": safe_address,
                        "valuator_address": valuator_address,
                        "chain": chain,
                        "dry_run": dry_run,
                    },
                )
                if settle_result.status == "success" and settle_result.data:
                    if settle_result.data.get("tx_hash"):
                        tx_hashes.append(settle_result.data["tx_hash"])
                elif settle_result.status != "success":
                    settle_failed = True
                    logger.warning("teardown_vault: final settlement failed: %s", settle_result.error)
            except Exception as e:
                settle_failed = True
                logger.warning("teardown_vault: final settlement error: %s", e)

        # 5. Compute final NAV
        final_nav = 0
        try:
            final_nav = sdk.get_total_assets(vault_address)
        except Exception as e:
            logger.warning("Failed to read final NAV during teardown: %s", e)

        # 6. Determine status and save final state
        if dry_run:
            status = "simulated"
        elif settle_failed:
            status = "partial_failure"
            _save_teardown_progress("settling", error="settlement failed")
            self._fire_alert(
                f"Vault teardown partial failure: final settlement failed for vault {vault_address[:10]}",
                severity="critical",
            )
        else:
            status = "success"

        # Save final teardown state (even on partial failure for progress tracking)
        if not dry_run and status == "success":
            agent_state["phase"] = "torn_down"
            agent_state["lp_position_id"] = None
            agent_state.pop("_teardown", None)
            try:
                self._client.state.SaveState(
                    gateway_pb2.SaveStateRequest(
                        strategy_id=strategy_id,
                        data=json.dumps(agent_state).encode(),
                        schema_version=1,
                    )
                )
            except Exception:
                logger.warning("Failed to save teardown state")

        return ToolResponse(
            status=status,
            data={
                "status": status,
                "positions_closed": positions_closed,
                "swaps_executed": swaps_executed,
                "final_nav": str(final_nav),
                "tx_hashes": tx_hashes,
                "message": f"Teardown {status}. Closed {positions_closed} positions, {swaps_executed} swaps. Final NAV: {final_nav}",
            },
        )

    async def _execute_approve_vault_underlying(self, args: dict) -> ToolResponse:
        """Approve the vault to pull underlying tokens from the Safe."""
        from almanak.framework.connectors.lagoon.deployer import LagoonVaultDeployer
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)
        vault_address = args["vault_address"]
        underlying_token = args["underlying_token"]
        safe_address = args["safe_address"]
        dry_run = args.get("dry_run", False)

        deployer = LagoonVaultDeployer()
        bundle = deployer.build_post_deploy_bundle(underlying_token, vault_address, safe_address)
        bundle_bytes = json.dumps(bundle.to_dict()).encode()

        exec_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=bundle_bytes,
                dry_run=dry_run,
                simulation_enabled=self._resolve_simulation_flag(
                    safe_address, tool_name="approve_vault_underlying", is_safe_wallet=True
                ),
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=safe_address,
            )
        )

        if not exec_resp.success and not dry_run:
            raise ExecutionFailedError(
                f"Approve vault underlying failed: {exec_resp.error}",
                tool_name="approve_vault_underlying",
            )

        tx_hash = exec_resp.tx_hashes[0] if exec_resp.tx_hashes else None
        status = "simulated" if dry_run else "success"
        return ToolResponse(
            status=status,
            data={
                "status": status,
                "tx_hash": tx_hash,
                "message": f"Safe approved vault {vault_address[:10]}... for underlying token",
            },
        )

    async def _execute_deposit_vault(self, args: dict) -> ToolResponse:
        """Deposit underlying tokens into a vault (approve + requestDeposit).

        Executes as two sequential steps:
        1. ERC20 approve (depositor approves vault to pull underlying)
        2. requestDeposit (vault pulls underlying from depositor)

        Split into two Execute calls because requestDeposit depends on
        the approve being committed first (simulation would fail otherwise).
        """
        from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
        from almanak.framework.models.reproduction_bundle import ActionBundle
        from almanak.gateway.proto import gateway_pb2

        chain = args.get("chain", self._default_chain)
        vault_address = args["vault_address"]
        amount = int(args["amount"])
        depositor = args.get("depositor_address") or self._wallet_address
        dry_run = args.get("dry_run", False)

        sdk = LagoonVaultSDK(self._client, chain=chain)
        underlying_token = args["underlying_token"]
        simulate = self._policy_engine.policy.require_simulation_before_execution

        # Step 1: Approve vault to spend underlying tokens
        approve_tx = sdk.build_approve_deposit_tx(underlying_token, vault_address, depositor, amount)
        approve_bundle = ActionBundle(
            intent_type="APPROVE_DEPOSIT",
            transactions=[approve_tx],
            metadata={"vault_address": vault_address, "depositor": depositor, "amount": str(amount)},
        )
        approve_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=json.dumps(approve_bundle.to_dict()).encode(),
                dry_run=dry_run,
                simulation_enabled=simulate,
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=depositor,
            )
        )
        if not approve_resp.success and not dry_run:
            raise ExecutionFailedError(
                f"Vault deposit approve failed: {approve_resp.error}",
                tool_name="deposit_vault",
            )

        # Step 2: Request deposit (depends on approve being committed).
        # Simulation is disabled here because eth_estimateGas may run against
        # a state where the approve tx from Step 1 has not yet been mined,
        # causing a spurious "transfer amount exceeds allowance/balance" revert.
        deposit_tx = sdk.build_request_deposit_tx(vault_address, depositor, amount)
        deposit_bundle = ActionBundle(
            intent_type="DEPOSIT_VAULT",
            transactions=[deposit_tx],
            metadata={"vault_address": vault_address, "depositor": depositor, "amount": str(amount)},
        )
        exec_resp = self._client.execution.Execute(
            gateway_pb2.ExecuteRequest(
                action_bundle=json.dumps(deposit_bundle.to_dict()).encode(),
                dry_run=dry_run,
                simulation_enabled=False,
                strategy_id=self._strategy_id,
                chain=chain,
                wallet_address=depositor,
            )
        )
        # Track deposit spend for daily limit accounting
        if not dry_run:
            usd_amount = await self._estimate_usd_spend(args)
            self._policy_engine.record_trade(usd_amount, success=exec_resp.success, tool_name="deposit_vault")

        if not exec_resp.success and not dry_run:
            raise ExecutionFailedError(
                f"Vault deposit failed: {exec_resp.error}",
                tool_name="deposit_vault",
            )

        tx_hash = exec_resp.tx_hashes[-1] if exec_resp.tx_hashes else None
        status = "simulated" if dry_run else "success"
        return ToolResponse(
            status=status,
            data={
                "status": status,
                "tx_hash": tx_hash,
                "approve_tx_hash": approve_resp.tx_hashes[-1] if approve_resp.tx_hashes else None,
                "amount_deposited": str(amount),
                "message": f"Deposited {amount} into vault {vault_address[:10]}...",
            },
        )

    # ── STATE TOOLS ─────────────────────────────────────────────────────

    async def _dispatch_state(self, tool_name: str, args: dict) -> ToolResponse:
        from almanak.gateway.proto import gateway_pb2

        strategy_id = args.get("strategy_id") or self._strategy_id

        if tool_name == "save_agent_state":
            state_bytes = json.dumps(args.get("state", {})).encode()
            # Use tracked version for optimistic locking
            expected_version = self._state_versions.get(strategy_id, 0)
            resp = self._client.state.SaveState(
                gateway_pb2.SaveStateRequest(
                    strategy_id=strategy_id,
                    expected_version=expected_version,
                    data=state_bytes,
                    schema_version=1,
                )
            )
            # Track the new version for subsequent saves
            if resp.success:
                self._state_versions[strategy_id] = resp.new_version
            return ToolResponse(
                status="success" if resp.success else "error",
                data={"version": resp.new_version, "checksum": resp.checksum},
            )

        if tool_name == "load_agent_state":
            try:
                resp = self._client.state.LoadState(gateway_pb2.LoadStateRequest(strategy_id=strategy_id))
                state = json.loads(resp.data) if resp.data else {}
                # Track version for subsequent saves
                self._state_versions[strategy_id] = resp.version
                return ToolResponse(
                    status="success",
                    data={"state": state, "version": resp.version},
                )
            except Exception as e:
                if "NOT_FOUND" in str(e):
                    return ToolResponse(
                        status="success",
                        data={"state": {}, "version": 0},
                        explanation="No previous state found.",
                    )
                # Real error -- propagate so the agent knows state loading failed
                logger.error("Failed to load agent state: %s", e)
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.STATE_LOAD_FAILED,
                        f"Failed to load state: {e}",
                        recoverable=True,
                    ),
                    explanation="State loading failed due to a gateway error. Retry or investigate.",
                )

        if tool_name == "record_agent_decision":
            # Record via ObserveService timeline event
            decision_id = str(uuid.uuid4())
            decision_payload = json.dumps(
                {
                    "decision_id": decision_id,
                    "summary": args.get("decision_summary", ""),
                    "tool_calls": args.get("tool_calls", []),
                    "intent_type": args.get("intent_type"),
                }
            )
            try:
                self._client.observe.RecordTimelineEvent(
                    gateway_pb2.RecordTimelineEventRequest(
                        strategy_id=strategy_id,
                        event_type="agent_decision",
                        details_json=decision_payload,
                    )
                )
            except Exception as e:
                logger.warning("Failed to record agent decision: %s", e)
                return ToolResponse(
                    status="error",
                    error=_error_dict(
                        AgentErrorCode.RECORD_FAILED,
                        f"Failed to record decision: {e}",
                        recoverable=True,
                    ),
                    data={"recorded": False, "decision_id": decision_id},
                )

            return ToolResponse(
                status="success",
                data={"recorded": True, "decision_id": decision_id},
            )

        raise ToolValidationError(f"Unknown state tool: {tool_name}", tool_name=tool_name)
