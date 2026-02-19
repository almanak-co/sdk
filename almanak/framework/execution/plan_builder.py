"""Plan Builder for Converting Intents to Execution Plans.

This module provides utilities for converting Intent objects and IntentSequences
into PlanBundle/PlanStep structures for execution via PlanExecutor.

Example:
    from almanak.framework.execution.plan_builder import PlanBuilder

    builder = PlanBuilder(strategy_id="my-strategy")

    # Build plan from intents
    plan = builder.build_plan_from_intents(intents)

    # Or from an IntentSequence
    plan = builder.build_plan_from_sequence(intent_sequence)
"""

import logging
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from almanak.framework.execution.plan import (
    PlanBundle,
    PlanStep,
    RemediationAction,
    StepArtifacts,
    StepStatus,
)
from almanak.framework.intents.vocabulary import (
    AnyIntent,
    IntentSequence,
    IntentType,
)

logger = logging.getLogger(__name__)


def is_cross_chain_intent(intent: AnyIntent) -> bool:
    """Check if an intent involves cross-chain bridging.

    Args:
        intent: Intent object to check

    Returns:
        True if intent has destination_chain different from source chain
    """
    intent_type = getattr(intent, "intent_type", None)
    if intent_type == IntentType.BRIDGE:
        from_chain = getattr(intent, "from_chain", None)
        to_chain = getattr(intent, "to_chain", None)
        if from_chain is not None and to_chain is not None:
            return from_chain != to_chain

    # Backward-compatible fallback for intents that expose destination_chain
    dest_chain = getattr(intent, "destination_chain", None)
    src_chain = getattr(intent, "chain", None)
    return dest_chain is not None and dest_chain != src_chain


def get_intent_chain(intent: AnyIntent, default_chain: str = "arbitrum") -> str:
    """Get the execution chain for an intent.

    Args:
        intent: Intent object
        default_chain: Default chain if not specified

    Returns:
        Chain name where intent executes
    """
    return getattr(intent, "chain", None) or default_chain


def get_remediation_action(intent: AnyIntent) -> RemediationAction:
    """Determine appropriate remediation action for an intent.

    Args:
        intent: Intent object

    Returns:
        RemediationAction based on intent type and characteristics
    """
    intent_type = intent.intent_type

    # Cross-chain swaps: bridge back on failure
    if is_cross_chain_intent(intent):
        return RemediationAction.BRIDGE_BACK

    # Lending operations: hold position on failure
    if intent_type in (IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY):
        return RemediationAction.HOLD

    # Perps: operator intervention for complex failures
    if intent_type == IntentType.PERP_OPEN:
        return RemediationAction.OPERATOR_INTERVENTION

    # Default: retry
    return RemediationAction.RETRY


def intent_to_dict(intent: AnyIntent) -> dict[str, Any]:
    """Serialize an intent to a dictionary.

    Args:
        intent: Intent object to serialize

    Returns:
        Dictionary representation of the intent
    """
    # Get all dataclass fields
    result: dict[str, Any] = {
        "intent_id": intent.intent_id,
        "intent_type": intent.intent_type.value,
    }

    # Add common fields if present
    for field_name in [
        "chain",
        "destination_chain",
        "protocol",
        "from_token",
        "to_token",
        "token",
        "amount",
        "amount_usd",
        "max_slippage",
        "collateral_token",
        "collateral_amount",
        "borrow_token",
        "borrow_amount",
        "interest_rate_mode",
        "market",
        "size_usd",
        "is_long",
        "leverage",
        "use_as_collateral",
        "reason",
    ]:
        value = getattr(intent, field_name, None)
        if value is not None:
            # Convert Decimal to string for serialization
            if isinstance(value, Decimal):
                result[field_name] = str(value)
            else:
                result[field_name] = value

    return result


def get_step_description(intent: AnyIntent) -> str:
    """Generate a human-readable description for a step.

    Args:
        intent: Intent object

    Returns:
        Description string
    """
    intent_type = intent.intent_type.value
    chain = get_intent_chain(intent)
    protocol = getattr(intent, "protocol", None) or ""

    if intent.intent_type == IntentType.SWAP:
        from_token = getattr(intent, "from_token", "?")
        to_token = getattr(intent, "to_token", "?")
        dest_chain = getattr(intent, "destination_chain", None)
        if dest_chain and dest_chain != chain:
            return f"Cross-chain swap {from_token} ({chain}) → {to_token} ({dest_chain}) via {protocol}"
        return f"Swap {from_token} → {to_token} on {chain} via {protocol}"

    if intent.intent_type == IntentType.SUPPLY:
        token = getattr(intent, "token", "?")
        return f"Supply {token} to {protocol} on {chain}"

    if intent.intent_type == IntentType.BORROW:
        token = getattr(intent, "borrow_token", "?")
        return f"Borrow {token} from {protocol} on {chain}"

    if intent.intent_type == IntentType.PERP_OPEN:
        market = getattr(intent, "market", "?")
        is_long = getattr(intent, "is_long", True)
        direction = "long" if is_long else "short"
        return f"Open {direction} {market} on {protocol} ({chain})"

    if intent.intent_type == IntentType.HOLD:
        reason = getattr(intent, "reason", "")
        return f"Hold: {reason}" if reason else "Hold position"

    return f"{intent_type} on {chain}"


class PlanBuilder:
    """Builder for creating execution plans from intents.

    Converts Intent objects and IntentSequences into PlanBundle structures
    that can be executed by PlanExecutor with proper state tracking.

    Example:
        builder = PlanBuilder(strategy_id="leverage-loop")
        plan = builder.build_plan_from_intents([
            Intent.swap(...),
            Intent.supply(...),
            Intent.borrow(...),
        ])
    """

    def __init__(
        self,
        strategy_id: str | None = None,
        default_chain: str = "arbitrum",
        max_retries: int = 3,
    ) -> None:
        """Initialize the plan builder.

        Args:
            strategy_id: Strategy identifier for the plan
            default_chain: Default chain for intents without explicit chain
            max_retries: Default max retries for each step
        """
        self._strategy_id = strategy_id
        self._default_chain = default_chain
        self._max_retries = max_retries

    def build_plan_from_intents(
        self,
        intents: Sequence[AnyIntent],
        description: str | None = None,
    ) -> PlanBundle:
        """Build an execution plan from a sequence of intents.

        Intents are assumed to be in dependency order (each step depends
        on the previous one completing successfully).

        Args:
            intents: Sequence of intents to execute
            description: Optional plan description

        Returns:
            PlanBundle ready for execution
        """
        if not intents:
            return PlanBundle(
                plan_id=self._generate_plan_id(),
                steps=[],
                strategy_id=self._strategy_id,
                description=description or "Empty plan",
            )

        steps: list[PlanStep] = []
        previous_step_id: str | None = None

        for i, intent in enumerate(intents):
            step = self._create_step_from_intent(
                intent=intent,
                step_index=i,
                depends_on=previous_step_id,
            )
            steps.append(step)
            previous_step_id = step.step_id

        plan = PlanBundle(
            plan_id=self._generate_plan_id(),
            steps=steps,
            strategy_id=self._strategy_id,
            description=description or self._generate_plan_description(intents),
        )

        logger.info(f"Built plan {plan.plan_id}: {len(steps)} steps, chains={list(plan.chains_involved)}")

        return plan

    def build_plan_from_sequence(
        self,
        sequence: IntentSequence,
    ) -> PlanBundle:
        """Build an execution plan from an IntentSequence.

        Args:
            sequence: IntentSequence containing intents and description

        Returns:
            PlanBundle ready for execution
        """
        intents = list(sequence)
        return self.build_plan_from_intents(
            intents=intents,
            description=sequence.description,
        )

    def _create_step_from_intent(
        self,
        intent: AnyIntent,
        step_index: int,
        depends_on: str | None = None,
    ) -> PlanStep:
        """Create a PlanStep from an intent.

        Args:
            intent: Intent to convert
            step_index: Index of this step in the sequence
            depends_on: Step ID this step depends on

        Returns:
            PlanStep ready for execution
        """
        step_id = f"step-{step_index + 1:03d}-{intent.intent_id[:8]}"
        chain = get_intent_chain(intent, self._default_chain)
        dependencies = [depends_on] if depends_on else []

        step = PlanStep(
            step_id=step_id,
            chain=chain,
            intent=intent_to_dict(intent),
            dependencies=dependencies,
            status=StepStatus.PENDING,
            artifacts=StepArtifacts(),
            remediation=get_remediation_action(intent),
            max_retries=self._max_retries,
            description=get_step_description(intent),
        )

        # Mark cross-chain steps for special handling
        if is_cross_chain_intent(intent):
            step.artifacts.bridge_deposit_id = None  # Will be set after execution

        return step

    def _generate_plan_id(self) -> str:
        """Generate a unique plan ID."""
        timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        prefix = self._strategy_id[:10] if self._strategy_id else "plan"
        return f"{prefix}-{timestamp}-{unique_id}"

    def _generate_plan_description(self, intents: Sequence[AnyIntent]) -> str:
        """Generate a description for the plan.

        Args:
            intents: Intents in the plan

        Returns:
            Human-readable description
        """
        if not intents:
            return "Empty plan"

        # Collect chains and intent types
        chains = set()
        intent_types = []
        for intent in intents:
            chains.add(get_intent_chain(intent, self._default_chain))
            dest = getattr(intent, "destination_chain", None)
            if dest:
                chains.add(dest)
            intent_types.append(intent.intent_type.value)

        chain_str = " → ".join(sorted(chains)) if len(chains) > 1 else list(chains)[0]
        type_str = " → ".join(intent_types)

        return f"{type_str} on {chain_str}"


def build_plan_from_decide_result(
    decide_result: AnyIntent | IntentSequence | list | None,
    strategy_id: str | None = None,
    default_chain: str = "arbitrum",
) -> PlanBundle | None:
    """Build a plan from a strategy's decide() result.

    Handles all possible return types from DecideResult:
    - Single intent
    - IntentSequence
    - List of intents/sequences
    - None (hold)

    Args:
        decide_result: Result from strategy.decide()
        strategy_id: Strategy identifier
        default_chain: Default chain for intents

    Returns:
        PlanBundle or None if result was None/Hold
    """
    if decide_result is None:
        return None

    builder = PlanBuilder(
        strategy_id=strategy_id,
        default_chain=default_chain,
    )

    # Handle IntentSequence
    if isinstance(decide_result, IntentSequence):
        return builder.build_plan_from_sequence(decide_result)

    # Handle single intent
    if hasattr(decide_result, "intent_type"):
        # Check for HOLD intent
        if decide_result.intent_type == IntentType.HOLD:
            return None
        return builder.build_plan_from_intents([cast(AnyIntent, decide_result)])

    # Handle list
    if isinstance(decide_result, list):
        # Flatten list of intents/sequences
        intents: list[AnyIntent] = []
        for item in decide_result:
            if isinstance(item, IntentSequence):
                intents.extend(list(item))
            elif hasattr(item, "intent_type"):
                if item.intent_type != IntentType.HOLD:
                    intents.append(item)

        if not intents:
            return None

        return builder.build_plan_from_intents(intents)

    logger.warning(f"Unknown decide_result type: {type(decide_result)}")
    return None


__all__ = [
    "PlanBuilder",
    "build_plan_from_decide_result",
    "is_cross_chain_intent",
    "get_intent_chain",
    "get_remediation_action",
    "intent_to_dict",
    "get_step_description",
]
