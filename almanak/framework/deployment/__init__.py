"""Deployment module for Almanak Strategy Framework.

This module provides deployment utilities including:
- Canary deployments for safe version testing
- A/B testing for strategy variants (future)
- Blue/green deployment patterns (future)

Usage:
    from almanak.framework.deployment import CanaryDeployment, CanaryConfig

    # Create a canary deployment
    canary = CanaryDeployment(
        deployment_id="my_strategy",
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
from .mode import (
    DeploymentMode,
    FatalBootError,
    deployment_commit_sha,
    deployment_id,
    deployment_mode,
    deployment_sdk_version,
    deployment_strategy_name,
    deployment_strategy_version,
    is_hosted,
    is_local,
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
    # Deployment-mode helpers (VIB-4722 — ALMANAK_IS_HOSTED signal,
    # ALMANAK_DEPLOYMENT_ID value).
    "deployment_id",
    "deployment_mode",
    "is_hosted",
    "is_local",
    "DeploymentMode",
    "FatalBootError",
    # Deployment-identity helpers used by the deployment-start banner.
    "deployment_commit_sha",
    "deployment_sdk_version",
    "deployment_strategy_name",
    "deployment_strategy_version",
]
