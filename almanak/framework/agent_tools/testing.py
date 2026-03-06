"""Mock gateway for testing agent tool execution without real infrastructure.

Provides a ``MockGatewayClient`` that replaces the real ``GatewayClient``
in ``ToolExecutor``, along with assertion helpers and pytest fixtures.
All gateway gRPC calls are intercepted and return configurable responses.

Usage::

    from almanak.framework.agent_tools.testing import MockGatewayClient

    mock = MockGatewayClient()
    mock.set_balance("USDC", "arbitrum", Decimal("10000"))
    mock.set_price("ETH", Decimal("2000"))

    executor = ToolExecutor(mock, policy=AgentPolicy(...), wallet_address="0x...")
    result = await executor.execute("get_price", {"token": "ETH"})

    mock.assert_called("get_price")
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import grpc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class MockCall:
    """Records a single gateway-level RPC call."""

    tool_name: str
    method: str
    args: dict
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    response: Any = None


@dataclass
class MockGatewayConfig:
    """Configuration for mock gateway defaults."""

    default_chain: str = "arbitrum"
    default_compile_success: bool = True
    default_execute_success: bool = True


# ---------------------------------------------------------------------------
# Stub service classes (mimic gRPC stub interfaces)
# ---------------------------------------------------------------------------


class _StubResponse:
    """Generic attribute-bag response object that mimics protobuf messages."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")


class _MockMarketService:
    """Mimics gateway_pb2_grpc.MarketServiceStub."""

    def __init__(self, parent: MockGatewayClient) -> None:
        self._parent = parent

    def GetPrice(self, request: Any, **kwargs: Any) -> Any:
        token: str = (
            getattr(request, "token", None) or (request.get("token") if isinstance(request, dict) else "") or ""
        )
        price = self._parent._prices.get(token, Decimal("0"))
        resp = _StubResponse(price=str(price), source="mock", timestamp="0")
        self._parent._record("GetPrice", {"token": token}, resp)
        return resp

    def GetBalance(self, request: Any, **kwargs: Any) -> Any:
        token: str = getattr(request, "token", "") or ""
        chain: str = getattr(request, "chain", "") or self._parent._config.default_chain
        key = (token, chain)
        self._parent._record("GetBalance", {"token": token, "chain": chain}, None)
        balance = self._parent._balances.get(key, Decimal("0"))
        price = self._parent._prices.get(token)
        if price is None:
            raise AssertionError(f"No mock price configured for token '{token}'. Call set_price() first.")
        balance_usd = balance * price
        resp = _StubResponse(balance=str(balance), balance_usd=str(balance_usd), error="")
        self._parent.call_log[-1].response = resp
        return resp

    def BatchGetBalances(self, request: Any, **kwargs: Any) -> Any:
        responses = []
        reqs = getattr(request, "requests", [])
        self._parent._record("BatchGetBalances", {"count": len(reqs)}, None)
        for r in reqs:
            token: str = getattr(r, "token", "") or ""
            chain: str = getattr(r, "chain", "") or self._parent._config.default_chain
            key = (token, chain)
            balance = self._parent._balances.get(key, Decimal("0"))
            price = self._parent._prices.get(token)
            if price is None:
                raise AssertionError(f"No mock price configured for token '{token}'. Call set_price() first.")
            balance_usd = balance * price
            responses.append(_StubResponse(balance=str(balance), balance_usd=str(balance_usd), error=""))
        resp = _StubResponse(responses=responses)
        self._parent.call_log[-1].response = resp
        return resp

    def GetIndicator(self, request: Any, **kwargs: Any) -> Any:
        resp = _StubResponse(value="50.0", metadata={"signal": "neutral"})
        self._parent._record("GetIndicator", {"indicator": getattr(request, "indicator_type", "")}, resp)
        return resp


class _MockExecutionService:
    """Mimics gateway_pb2_grpc.ExecutionServiceStub."""

    def __init__(self, parent: MockGatewayClient) -> None:
        self._parent = parent

    def CompileIntent(self, request: Any, **kwargs: Any) -> Any:
        intent_type = getattr(request, "intent_type", "")
        chain = getattr(request, "chain", "")

        # Check for custom compile result
        custom = self._parent._compile_results.get(intent_type)
        if custom is not None:
            success, error = custom
        else:
            success = self._parent._config.default_compile_success
            error = "" if success else "mock compilation failure"

        bundle = json.dumps({"actions": [{"type": intent_type, "mock": True}]}).encode() if success else b""
        resp = _StubResponse(success=success, error=error, action_bundle=bundle)
        self._parent._record("CompileIntent", {"intent_type": intent_type, "chain": chain}, resp)
        return resp

    def Execute(self, request: Any, **kwargs: Any) -> Any:
        dry_run = getattr(request, "dry_run", False)

        # Check for custom execute result
        custom = self._parent._execute_result
        if custom is not None:
            success, error = custom
        else:
            success = self._parent._config.default_execute_success
            error = "" if success else "mock execution failure"

        resp = _StubResponse(
            success=success,
            error=error,
            tx_hashes=["0xmock_tx_hash"] if success and not dry_run else [],
            receipts=b"[]",
        )
        self._parent._record("Execute", {"dry_run": dry_run}, resp)
        return resp


class _MockStateService:
    """Mimics gateway_pb2_grpc.StateServiceStub."""

    def __init__(self, parent: MockGatewayClient) -> None:
        self._parent = parent
        self._store: dict[str, tuple[bytes, int]] = {}  # strategy_id -> (data, version)

    def SaveState(self, request: Any, **kwargs: Any) -> Any:
        strategy_id = getattr(request, "strategy_id", "default")
        data = getattr(request, "data", b"{}")
        _, version = self._store.get(strategy_id, (b"{}", 0))
        new_version = version + 1
        self._store[strategy_id] = (data, new_version)
        resp = _StubResponse(success=True, new_version=new_version, checksum="mock_checksum")
        self._parent._record("SaveState", {"strategy_id": strategy_id}, resp)
        return resp

    def LoadState(self, request: Any, **kwargs: Any) -> Any:
        strategy_id = getattr(request, "strategy_id", "default")
        stored = self._store.get(strategy_id)
        if stored is None:
            raise _MockRpcError(grpc.StatusCode.NOT_FOUND, "state not found")
        data, version = stored
        resp = _StubResponse(data=data, version=version)
        self._parent._record("LoadState", {"strategy_id": strategy_id}, resp)
        return resp


class _MockObserveService:
    """Mimics gateway_pb2_grpc.ObserveServiceStub."""

    def __init__(self, parent: MockGatewayClient) -> None:
        self._parent = parent

    def RecordTimelineEvent(self, request: Any, **kwargs: Any) -> Any:
        resp = _StubResponse(success=True)
        self._parent._record(
            "RecordTimelineEvent",
            {"event_type": getattr(request, "event_type", "")},
            resp,
        )
        return resp


class _MockRpcError(grpc.RpcError):
    """Minimal mock of grpc.RpcError for state NOT_FOUND."""

    def __init__(self, code: Any, details: str = "") -> None:
        self._code = code
        self._details = details
        message = f"{details} (code={code.name if hasattr(code, 'name') else code})"
        super().__init__(message)

    def code(self) -> Any:
        return self._code

    def details(self) -> str:
        return self._details


# ---------------------------------------------------------------------------
# Main mock gateway client
# ---------------------------------------------------------------------------


class MockGatewayClient:
    """Mock gateway client for testing agent tool execution without infrastructure.

    Drop-in replacement for ``GatewayClient`` in ``ToolExecutor``. Provides
    configurable responses for prices, balances, compilation, and execution,
    and records all calls for assertion.

    Example::

        mock = MockGatewayClient()
        mock.set_price("ETH", Decimal("2000"))
        mock.set_balance("USDC", "arbitrum", Decimal("10000"))

        executor = ToolExecutor(mock, policy=AgentPolicy())
        result = await executor.execute("get_price", {"token": "ETH"})

        mock.assert_called("GetPrice", times=1)
    """

    def __init__(self, config: MockGatewayConfig | None = None) -> None:
        self._config = config or MockGatewayConfig()
        self.call_log: list[MockCall] = []

        # Configurable state
        self._prices: dict[str, Decimal] = {}
        self._balances: dict[tuple[str, str], Decimal] = {}  # (token, chain) -> amount
        self._compile_results: dict[str, tuple[bool, str]] = {}  # intent_type -> (success, error)
        self._execute_result: tuple[bool, str] | None = None

        # Service stubs
        self._market = _MockMarketService(self)
        self._execution = _MockExecutionService(self)
        self._state = _MockStateService(self)
        self._observe = _MockObserveService(self)

    # -- GatewayClient interface compatibility ---------------------------------

    @property
    def is_connected(self) -> bool:
        return True

    @property
    def market(self) -> _MockMarketService:
        return self._market

    @property
    def execution(self) -> _MockExecutionService:
        return self._execution

    @property
    def state(self) -> _MockStateService:
        return self._state

    @property
    def observe(self) -> _MockObserveService:
        return self._observe

    # -- Configuration methods -------------------------------------------------

    def set_price(self, token: str, price: Decimal) -> None:
        """Set the mock USD price for a token."""
        self._prices[token] = price

    def set_balance(self, token: str, chain: str, amount: Decimal) -> None:
        """Set the mock balance for a token on a chain."""
        self._balances[(token, chain)] = amount

    def set_compile_result(self, intent_type: str, *, success: bool, error: str = "") -> None:
        """Configure the compile result for a specific intent type."""
        self._compile_results[intent_type] = (success, error)

    def set_execute_result(self, *, success: bool, error: str = "") -> None:
        """Configure the default execution result."""
        self._execute_result = (success, error)

    # -- Call recording --------------------------------------------------------

    def _record(self, method: str, args: dict, response: Any) -> None:
        """Record a gateway call for later assertion."""
        # Derive tool_name from method for convenience
        tool_name = _METHOD_TO_TOOL.get(method, method)
        self.call_log.append(
            MockCall(
                tool_name=tool_name,
                method=method,
                args=args,
                response=response,
            )
        )

    # -- Assertion helpers -----------------------------------------------------

    def get_calls(self, method: str | None = None) -> list[MockCall]:
        """Get recorded calls, optionally filtered by method name."""
        if method is None:
            return list(self.call_log)
        return [c for c in self.call_log if c.method == method or c.tool_name == method]

    def assert_called(self, method: str, *, times: int | None = None) -> None:
        """Assert that a gateway method was called.

        Args:
            method: Gateway method name (e.g. "GetPrice") or tool name (e.g. "get_price").
            times: If specified, assert exact call count.

        Raises:
            AssertionError: If the assertion fails.
        """
        calls = self.get_calls(method)
        if times is not None:
            assert len(calls) == times, (
                f"Expected {method} to be called {times} time(s), got {len(calls)}. "
                f"All calls: {[c.method for c in self.call_log]}"
            )
        else:
            assert len(calls) > 0, (
                f"Expected {method} to be called at least once. All calls: {[c.method for c in self.call_log]}"
            )

    def assert_not_called(self, method: str) -> None:
        """Assert that a gateway method was never called."""
        calls = self.get_calls(method)
        assert len(calls) == 0, (
            f"Expected {method} to never be called, but it was called {len(calls)} time(s). "
            f"Args: {[c.args for c in calls]}"
        )

    def reset(self) -> None:
        """Clear all recorded calls (keeps configured responses)."""
        self.call_log.clear()

    def reset_all(self) -> None:
        """Clear everything: recorded calls, prices, balances, and results."""
        self.call_log.clear()
        self._prices.clear()
        self._balances.clear()
        self._compile_results.clear()
        self._execute_result = None
        self._state._store.clear()


# Mapping from gRPC method names to user-facing tool names for convenience
_METHOD_TO_TOOL: dict[str, str] = {
    "GetPrice": "get_price",
    "GetBalance": "get_balance",
    "BatchGetBalances": "batch_get_balances",
    "GetIndicator": "get_indicator",
    "CompileIntent": "compile_intent",
    "Execute": "execute",
    "SaveState": "save_agent_state",
    "LoadState": "load_agent_state",
    "RecordTimelineEvent": "record_agent_decision",
}
