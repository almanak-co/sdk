"""Almanak Agent Tools -- structured tool interface for AI agents.

This package exposes Almanak's DeFi capabilities (market data, execution,
state management) as validated, policy-enforced tools that any LLM agent
framework can consume.

Quick start::

    from almanak.framework.agent_tools import ToolExecutor, AgentPolicy, get_default_catalog

    catalog = get_default_catalog()
    executor = ToolExecutor(gateway_client, policy=AgentPolicy(), wallet_address="0x...")
    result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
"""

from almanak.framework.agent_tools.catalog import (  # noqa: F401
    LatencyClass,
    RiskTier,
    ToolCatalog,
    ToolCategory,
    ToolDefinition,
    get_default_catalog,
)
from almanak.framework.agent_tools.errors import (  # noqa: F401
    AgentErrorCode,
    ErrorCategory,
    ExecutionFailedError,
    PermissionDeniedError,
    RiskBlockedError,
    SimulationFailedError,
    ToolError,
    ToolTimeoutError,
    ToolValidationError,
    UpstreamUnavailableError,
    get_error_category,
)
from almanak.framework.agent_tools.executor import ToolExecutor  # noqa: F401
from almanak.framework.agent_tools.llm_client import (  # noqa: F401
    LLMClient,
    LLMClientProtocol,
    LLMConfig,
    LLMConfigError,
    validate_llm_config,
)
from almanak.framework.agent_tools.policy import (  # noqa: F401
    AgentPolicy,
    PolicyDecision,
    PolicyEngine,
    PolicyStateStore,
)
from almanak.framework.agent_tools.schemas import ToolResponse  # noqa: F401
from almanak.framework.agent_tools.testing import MockCall, MockGatewayClient, MockGatewayConfig  # noqa: F401
from almanak.framework.agent_tools.tracing import (  # noqa: F401
    CallbackTraceSink,
    DecisionTracer,
    FileTraceSink,
    InMemoryTraceSink,
    TraceEntry,
    TraceSink,
    sanitize_args,
)
