"""Deployment module for Almanak Strategy Framework.

This module provides deployment utilities including:
- Canary deployments for safe version testing
- A/B testing for strategy variants (future)
- Blue/green deployment patterns (future)

Usage:
    from almanak.framework.deployment import CanaryDeployment, CanaryConfig

    # Create a canary deployment
    canary = CanaryDeployment(
        strategy_id="my_strategy",
        stable_version_id="v_123",
        canary_version_id="v_456",
    )

    # Start canary deployment
    result = await canary.deploy(canary_percent=10, observation_period_minutes=60)
"""

from .canary import (
    CanaryComparison,
    CanaryConfig,
    CanaryDecision,
    CanaryDeployment,
    CanaryEventType,
    CanaryMetrics,
    CanaryResult,
    CanaryState,
    CanaryStatus,
    DeployCanaryResult,
    PromotionCriteria,
)

__all__ = [
    "CanaryDeployment",
    "CanaryConfig",
    "CanaryState",
    "CanaryStatus",
    "CanaryMetrics",
    "CanaryDecision",
    "CanaryResult",
    "DeployCanaryResult",
    "CanaryEventType",
    "CanaryComparison",
    "PromotionCriteria",
]
