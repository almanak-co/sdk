"""CLI commands for Intent debugging and inspection.

Usage:
    almanak intent inspect <strategy.py>
    almanak intent trace <strategy.py> --scenario <json>

Example:
    almanak intent inspect templates/dynamic_lp_intent/strategy.py
    almanak intent trace templates/dynamic_lp_intent/strategy.py --scenario scenario.json
"""

import ast
import importlib.util
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

from ..intents import (
    CompilationStatus,
    Intent,
    IntentCompiler,
    IntentType,
    generate_state_diagram,
)
from ..intents.vocabulary import (
    AnyIntent,
    BorrowIntent,
    HoldIntent,
    LPCloseIntent,
    LPOpenIntent,
    RepayIntent,
    SwapIntent,
)
from ..strategies.intent_strategy import (
    IntentStrategy,
    MarketSnapshot,
    RSIData,
    StrategyMetadata,
    TokenBalance,
)

# =============================================================================
# Constants
# =============================================================================

# Intent type to dataclass mapping
INTENT_TYPE_TO_CLASS: dict[IntentType, type[AnyIntent]] = {
    IntentType.SWAP: SwapIntent,
    IntentType.LP_OPEN: LPOpenIntent,
    IntentType.LP_CLOSE: LPCloseIntent,
    IntentType.BORROW: BorrowIntent,
    IntentType.REPAY: RepayIntent,
    IntentType.HOLD: HoldIntent,
}

# Default example intents for each type
DEFAULT_EXAMPLE_INTENTS: dict[str, dict[str, Any]] = {
    "SWAP": {
        "from_token": "USDC",
        "to_token": "ETH",
        "amount_usd": "1000",
    },
    "LP_OPEN": {
        "pool": "WETH/USDC/3000",
        "amount0": "1.0",
        "amount1": "2000.0",
        "range_lower": "1800.0",
        "range_upper": "2200.0",
    },
    "LP_CLOSE": {
        "position_id": "123456",
        "pool": "WETH/USDC/3000",
    },
    "BORROW": {
        "protocol": "aave_v3",
        "collateral_token": "ETH",
        "collateral_amount": "1.0",
        "borrow_token": "USDC",
        "borrow_amount": "1500.0",
    },
    "REPAY": {
        "protocol": "aave_v3",
        "token": "USDC",
        "amount": "1500.0",
    },
    "HOLD": {
        "reason": "No action needed",
    },
}


# =============================================================================
# Data Types
# =============================================================================


@dataclass
class IntentInspectionResult:
    """Result of inspecting a strategy's intents."""

    strategy_name: str
    strategy_path: str
    metadata: dict[str, Any] | None
    intent_types: list[str]
    intent_details: list[dict[str, Any]]
    state_diagrams: dict[str, str]
    action_bundles: dict[str, dict[str, Any]]
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "strategy_name": self.strategy_name,
            "strategy_path": self.strategy_path,
            "metadata": self.metadata,
            "intent_types": self.intent_types,
            "intent_details": self.intent_details,
            "state_diagrams": self.state_diagrams,
            "action_bundles": self.action_bundles,
            "errors": self.errors,
        }


@dataclass
class TraceStep:
    """A single step in an intent trace."""

    step_number: int
    description: str
    state: str
    intent: dict[str, Any] | None
    action_bundle: dict[str, Any] | None
    success: bool
    error: str | None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_number": self.step_number,
            "description": self.description,
            "state": self.state,
            "intent": self.intent,
            "action_bundle": self.action_bundle,
            "success": self.success,
            "error": self.error,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class TraceResult:
    """Result of tracing a strategy execution."""

    strategy_name: str
    scenario_file: str | None
    scenario: dict[str, Any]
    steps: list[TraceStep]
    final_intent: dict[str, Any] | None
    final_action_bundle: dict[str, Any] | None
    success: bool
    error: str | None
    execution_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "strategy_name": self.strategy_name,
            "scenario_file": self.scenario_file,
            "scenario": self.scenario,
            "steps": [s.to_dict() for s in self.steps],
            "final_intent": self.final_intent,
            "final_action_bundle": self.final_action_bundle,
            "success": self.success,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
        }


# =============================================================================
# Strategy Loading
# =============================================================================


def load_strategy_from_file(file_path: Path) -> tuple[type[IntentStrategy[Any]] | None, str | None]:
    """Load a strategy class from a Python file.

    Args:
        file_path: Path to the strategy Python file

    Returns:
        Tuple of (strategy_class, error_message)
    """
    if not file_path.exists():
        return None, f"File not found: {file_path}"

    if not file_path.suffix == ".py":
        return None, f"Expected .py file, got: {file_path}"

    try:
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location("strategy_module", file_path)
        if spec is None or spec.loader is None:
            return None, f"Could not load module spec from {file_path}"

        module = importlib.util.module_from_spec(spec)
        sys.modules["strategy_module"] = module
        spec.loader.exec_module(module)

        # Find concrete IntentStrategy subclasses (skip abstract base classes
        # like StatelessStrategy that may be imported but not instantiable).
        strategy_classes = []
        for name in dir(module):
            obj = getattr(module, name)
            if (
                isinstance(obj, type)
                and obj is not IntentStrategy
                and issubclass(obj, IntentStrategy)
                and not getattr(obj, "__abstractmethods__", frozenset())
            ):
                strategy_classes.append(obj)

        if not strategy_classes:
            return None, "No concrete IntentStrategy subclass found in file"

        # Prefer the most-derived class (defined in this file, not just imported)
        if len(strategy_classes) > 1:
            # Filter to classes defined in this module (not imported base classes)
            local_classes = [c for c in strategy_classes if c.__module__ == module.__name__]
            if local_classes:
                strategy_classes = local_classes

        return strategy_classes[0], None

    except Exception as e:
        return None, f"Error loading strategy: {str(e)}"


def analyze_strategy_source(file_path: Path) -> list[str]:
    """Analyze strategy source code to detect intent types.

    Uses AST parsing to find Intent.* calls in the decide() method.

    Args:
        file_path: Path to the strategy file

    Returns:
        List of detected intent type names
    """
    try:
        source = file_path.read_text()
        tree = ast.parse(source)

        intent_types: set[str] = set()

        class IntentVisitor(ast.NodeVisitor):
            """AST visitor to find Intent.* calls."""

            def visit_Call(self, node: ast.Call) -> None:
                # Look for Intent.swap, Intent.lp_open, etc.
                if isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == "Intent":
                        method_name = node.func.attr
                        # Map method names to IntentType values
                        method_to_type = {
                            "swap": "SWAP",
                            "lp_open": "LP_OPEN",
                            "lp_close": "LP_CLOSE",
                            "borrow": "BORROW",
                            "repay": "REPAY",
                            "hold": "HOLD",
                        }
                        if method_name in method_to_type:
                            intent_types.add(method_to_type[method_name])
                self.generic_visit(node)

        visitor = IntentVisitor()
        visitor.visit(tree)

        return sorted(intent_types)

    except Exception as e:
        click.echo(f"Warning: Could not parse source for intent detection: {e}", err=True)
        return []


def get_strategy_metadata(strategy_class: type[IntentStrategy[Any]]) -> dict[str, Any] | None:
    """Get metadata from a strategy class."""
    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    if metadata:
        if isinstance(metadata, StrategyMetadata):
            return metadata.to_dict()
        elif isinstance(metadata, dict):
            return metadata
    return None


# =============================================================================
# Intent Inspection
# =============================================================================


def create_example_intent(intent_type: str, params: dict[str, Any] | None = None) -> AnyIntent:
    """Create an example intent of the given type.

    Args:
        intent_type: Type of intent to create
        params: Optional parameters to use (uses defaults if not provided)

    Returns:
        Created intent instance
    """
    defaults = DEFAULT_EXAMPLE_INTENTS.get(intent_type, {})
    if params:
        defaults = {**defaults, **params}

    if intent_type == "SWAP":
        return Intent.swap(
            from_token=defaults.get("from_token", "USDC"),
            to_token=defaults.get("to_token", "ETH"),
            amount_usd=Decimal(defaults.get("amount_usd", "1000")),
        )
    elif intent_type == "LP_OPEN":
        return Intent.lp_open(
            pool=defaults.get("pool", "WETH/USDC/3000"),
            amount0=Decimal(defaults.get("amount0", "1.0")),
            amount1=Decimal(defaults.get("amount1", "2000.0")),
            range_lower=Decimal(defaults.get("range_lower", "1800.0")),
            range_upper=Decimal(defaults.get("range_upper", "2200.0")),
            protocol=defaults.get("protocol", "uniswap_v3"),
        )
    elif intent_type == "LP_CLOSE":
        return Intent.lp_close(
            position_id=defaults.get("position_id", "123456"),
            pool=defaults.get("pool", "WETH/USDC/3000"),
            protocol=defaults.get("protocol", "uniswap_v3"),
        )
    elif intent_type == "BORROW":
        return Intent.borrow(
            protocol=defaults.get("protocol", "aave_v3"),
            collateral_token=defaults.get("collateral_token", "ETH"),
            collateral_amount=Decimal(defaults.get("collateral_amount", "1.0")),
            borrow_token=defaults.get("borrow_token", "USDC"),
            borrow_amount=Decimal(defaults.get("borrow_amount", "1500.0")),
        )
    elif intent_type == "REPAY":
        return Intent.repay(
            protocol=defaults.get("protocol", "aave_v3"),
            token=defaults.get("token", "USDC"),
            amount=Decimal(defaults.get("amount", "1500.0")),
        )
    else:
        return Intent.hold(reason=defaults.get("reason", "No action needed"))


def compile_example_intent(
    intent: AnyIntent,
    chain: str = "arbitrum",
    wallet_address: str = "0x" + "1" * 40,
) -> dict[str, Any]:
    """Compile an intent and return the result as a dict.

    Args:
        intent: The intent to compile
        chain: Chain to compile for
        wallet_address: Wallet address for compilation

    Returns:
        Dictionary with compilation result
    """
    compiler = IntentCompiler(
        chain=chain,
        wallet_address=wallet_address,
    )

    try:
        result = compiler.compile(intent)
        return {
            "status": result.status.value,
            "action_bundle": result.action_bundle.to_dict() if result.action_bundle else None,
            "transactions": [
                {
                    "to": tx.to,
                    "value": str(tx.value),
                    "data": tx.data[:66] + "..." if len(tx.data) > 66 else tx.data,
                    "gas_estimate": tx.gas_estimate,
                    "description": tx.description,
                    "tx_type": tx.tx_type,
                }
                for tx in result.transactions
            ],
            "total_gas_estimate": result.total_gas_estimate,
            "error": result.error,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
        }


def inspect_strategy(
    file_path: Path,
    chain: str = "arbitrum",
) -> IntentInspectionResult:
    """Inspect a strategy file and extract intent information.

    Args:
        file_path: Path to the strategy file
        chain: Chain to use for ActionBundle compilation

    Returns:
        IntentInspectionResult with all inspection data
    """
    errors: list[str] = []

    # Load strategy class
    strategy_class, load_error = load_strategy_from_file(file_path)
    if load_error:
        return IntentInspectionResult(
            strategy_name="unknown",
            strategy_path=str(file_path),
            metadata=None,
            intent_types=[],
            intent_details=[],
            state_diagrams={},
            action_bundles={},
            errors=[load_error],
        )

    assert strategy_class is not None

    # Get metadata
    metadata = get_strategy_metadata(strategy_class)
    strategy_name = getattr(strategy_class, "STRATEGY_NAME", strategy_class.__name__)

    # Detect intent types from source
    source_intent_types = analyze_strategy_source(file_path)

    # Also check metadata for intent_types
    if metadata and metadata.get("intent_types"):
        for it in metadata["intent_types"]:
            if it not in source_intent_types:
                source_intent_types.append(it)
        source_intent_types = sorted(source_intent_types)

    # If no intents detected, use all possible types
    if not source_intent_types:
        source_intent_types = [t.value for t in IntentType]
        errors.append("Could not detect intent types from source, showing all possible types")

    # Generate state diagrams for each intent type
    state_diagrams = {}
    for intent_type_str in source_intent_types:
        try:
            intent_type = IntentType(intent_type_str)
            state_diagrams[intent_type_str] = generate_state_diagram(intent_type)
        except ValueError:
            errors.append(f"Unknown intent type: {intent_type_str}")

    # Generate example intents and compile them
    intent_details = []
    action_bundles = {}
    for intent_type_str in source_intent_types:
        try:
            example_intent = create_example_intent(intent_type_str)
            intent_details.append(
                {
                    "type": intent_type_str,
                    "example": example_intent.serialize(),
                }
            )

            # Compile example intent (skip HOLD as it has no transactions)
            if intent_type_str != "HOLD":
                compilation_result = compile_example_intent(example_intent, chain=chain)
                action_bundles[intent_type_str] = compilation_result
            else:
                action_bundles[intent_type_str] = {
                    "status": "SUCCESS",
                    "note": "HOLD intents require no transactions",
                }
        except Exception as e:
            errors.append(f"Error creating example for {intent_type_str}: {str(e)}")

    return IntentInspectionResult(
        strategy_name=strategy_name,
        strategy_path=str(file_path),
        metadata=metadata,
        intent_types=source_intent_types,
        intent_details=intent_details,
        state_diagrams=state_diagrams,
        action_bundles=action_bundles,
        errors=errors,
    )


# =============================================================================
# Intent Tracing
# =============================================================================


def create_market_snapshot_from_scenario(
    scenario: dict[str, Any],
    chain: str = "arbitrum",
    wallet_address: str = "0x" + "1" * 40,
) -> MarketSnapshot:
    """Create a MarketSnapshot from scenario data.

    Args:
        scenario: Scenario data with prices, balances, RSI values
        chain: Chain name
        wallet_address: Wallet address

    Returns:
        Populated MarketSnapshot
    """
    market = MarketSnapshot(
        chain=chain,
        wallet_address=wallet_address,
    )

    # Set prices from scenario
    prices = scenario.get("prices", {})
    for token, price in prices.items():
        market.set_price(token, Decimal(str(price)))

    # Set balances from scenario
    balances = scenario.get("balances", {})
    for token, balance_data in balances.items():
        if isinstance(balance_data, dict):
            balance = TokenBalance(
                symbol=token,
                balance=Decimal(str(balance_data.get("balance", 0))),
                balance_usd=Decimal(str(balance_data.get("balance_usd", 0))),
                address=balance_data.get("address", ""),
            )
        else:
            # Simple number format
            balance = TokenBalance(
                symbol=token,
                balance=Decimal(str(balance_data)),
                balance_usd=Decimal(str(balance_data)),
            )
        market.set_balance(token, balance)

    # Set RSI values from scenario
    rsi_values = scenario.get("rsi", {})
    for token, rsi_data in rsi_values.items():
        if isinstance(rsi_data, dict):
            rsi = RSIData(
                value=Decimal(str(rsi_data.get("value", 50))),
                period=rsi_data.get("period", 14),
            )
        else:
            rsi = RSIData(value=Decimal(str(rsi_data)))
        market.set_rsi(token, rsi)

    return market


class _TraceStepRecorder:
    """Helper that accumulates TraceStep entries with monotonic numbering."""

    def __init__(self) -> None:
        self.steps: list[TraceStep] = []
        self._counter = 0

    def add(
        self,
        description: str,
        state: str,
        intent: AnyIntent | None = None,
        action_bundle: dict[str, Any] | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Append a new TraceStep to the running trace."""
        self._counter += 1
        self.steps.append(
            TraceStep(
                step_number=self._counter,
                description=description,
                state=state,
                intent=intent.serialize() if intent else None,
                action_bundle=action_bundle,
                success=success,
                error=error,
            )
        )


def _build_trace_failure(
    *,
    strategy_name: str,
    scenario_file: str | None,
    scenario: dict[str, Any],
    steps: list[TraceStep],
    error: str,
    start_time: float,
) -> TraceResult:
    """Assemble a failed TraceResult with elapsed time."""
    import time

    return TraceResult(
        strategy_name=strategy_name,
        scenario_file=scenario_file,
        scenario=scenario,
        steps=steps,
        final_intent=None,
        final_action_bundle=None,
        success=False,
        error=error,
        execution_time_ms=(time.time() - start_time) * 1000,
    )


def _try_instantiate_config(config_class: type, config_defaults: dict[str, Any]) -> Any | None:
    """Attempt to construct a config via from_dict() then direct kwargs."""
    if hasattr(config_class, "from_dict"):
        try:
            return config_class.from_dict(config_defaults)
        except Exception:
            pass
    try:
        return config_class(**config_defaults)
    except Exception:
        return None


def _load_config_from_module(
    config_module_path: Path,
    config_defaults: dict[str, Any],
) -> Any:
    """Locate a *Config class in a sibling config.py and instantiate it.

    Returns None if the module spec/loader cannot be obtained — callers preserve
    the legacy behaviour of leaving config unset in that branch.
    """
    spec = importlib.util.spec_from_file_location("config_module", config_module_path)
    if not (spec and spec.loader):
        return None

    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)

    for name in dir(config_module):
        obj = getattr(config_module, name)
        if isinstance(obj, type) and name.endswith("Config"):
            instance = _try_instantiate_config(obj, config_defaults)
            if instance is not None:
                return instance
    raise ValueError("No config class found")


def _make_mock_config(chain: str, wallet_address: str) -> Any:
    """Build a minimal config dataclass for strategies without a config.py sibling.

    Class bodies don't see enclosing-function locals, so we declare the fields
    without defaults and pass the values via the constructor instead.
    """

    @dataclass
    class MockConfig:
        strategy_id: str
        chain: str
        wallet_address: str

        def to_dict(self) -> dict[str, Any]:
            return {
                "strategy_id": self.strategy_id,
                "chain": self.chain,
                "wallet_address": self.wallet_address,
            }

    return MockConfig(
        strategy_id="trace-test",
        chain=chain,
        wallet_address=wallet_address,
    )


def _resolve_trace_config(
    file_path: Path,
    scenario_config: dict[str, Any],
    chain: str,
    wallet_address: str,
) -> Any:
    """Return a config object from a sibling config.py or a generated mock.

    Returns None when config.py exists but its module spec/loader is unavailable —
    matches the legacy behaviour of leaving config unset on that branch.
    """
    config_module_path = file_path.parent / "config.py"
    if not config_module_path.exists():
        return _make_mock_config(chain, wallet_address)

    config_defaults = {
        "strategy_id": "trace-test",
        "chain": chain,
        "wallet_address": wallet_address,
        **scenario_config,
    }
    return _load_config_from_module(config_module_path, config_defaults)


def _resolve_intent_from_list(decide_result: list[Any]) -> AnyIntent | None:
    """Pick the head intent out of a parallel-execution list (possibly empty)."""
    from ..intents.vocabulary import IntentSequence

    if not decide_result:
        return Intent.hold(reason="Empty result list")
    first_item = decide_result[0]
    if isinstance(first_item, IntentSequence):
        return first_item.first
    return first_item


def _normalize_decide_result(
    decide_result: Any,
    recorder: _TraceStepRecorder,
) -> AnyIntent:
    """Reduce a decide() return (None, single intent, list, IntentSequence) to one intent."""
    from ..intents.vocabulary import IntentSequence

    intent: AnyIntent | None
    if decide_result is None:
        intent = Intent.hold(reason="decide() returned None")
    elif isinstance(decide_result, IntentSequence):
        intent = decide_result.first
        recorder.add(
            f"decide() returned IntentSequence with {len(decide_result)} intents",
            "DECIDE_MULTI",
        )
    elif isinstance(decide_result, list):
        intent = _resolve_intent_from_list(decide_result)
        recorder.add(
            f"decide() returned list with {len(decide_result)} items for parallel execution",
            "DECIDE_MULTI",
        )
    else:
        intent = decide_result

    if intent is None:
        intent = Intent.hold(reason="No intent resolved")
    return intent


def _build_action_bundle_dict(compilation_result: Any) -> dict[str, Any]:
    """Convert a successful CompilationResult into the trace dict shape."""
    return {
        "status": "SUCCESS",
        "transactions": [
            {
                "to": tx.to,
                "value": str(tx.value),
                "data": tx.data[:66] + "..." if len(tx.data) > 66 else tx.data,
                "gas_estimate": tx.gas_estimate,
                "description": tx.description,
            }
            for tx in compilation_result.transactions
        ],
        "total_gas_estimate": compilation_result.total_gas_estimate,
    }


def _compile_intent_for_trace(
    intent: AnyIntent,
    compiler: IntentCompiler,
    recorder: _TraceStepRecorder,
) -> tuple[dict[str, Any] | None, str | None]:
    """Compile the final intent (or skip for HOLD) and append trace steps.

    Returns ``(bundle, error)``. ``error`` is ``None`` on success or HOLD;
    a non-None ``error`` tells `trace_strategy` to surface the failure on the
    `TraceResult` (so the CLI exits non-zero on compile errors).
    """
    if intent.intent_type == IntentType.HOLD:
        recorder.add("HOLD intent - no compilation needed", "HOLD")
        return None, None

    recorder.add("Compiling intent to ActionBundle", "COMPILING")
    try:
        compilation_result = compiler.compile(intent)
    except Exception as e:
        recorder.add(f"Compilation error: {e}", "COMPILE_ERROR", success=False, error=str(e))
        return None, str(e)

    if compilation_result.status != CompilationStatus.SUCCESS:
        err = compilation_result.error or "Compilation failed"
        recorder.add(
            f"Compilation failed: {err}",
            "COMPILE_ERROR",
            success=False,
            error=err,
        )
        return None, err

    bundle = _build_action_bundle_dict(compilation_result)
    recorder.add(
        f"Compilation successful: {len(compilation_result.transactions)} transaction(s)",
        "COMPILED",
        action_bundle=bundle,
    )
    return bundle, None


def trace_strategy(
    file_path: Path,
    scenario: dict[str, Any],
    scenario_file: str | None = None,
    chain: str = "arbitrum",
) -> TraceResult:
    """Trace strategy execution with a given scenario.

    Args:
        file_path: Path to the strategy file
        scenario: Scenario data
        scenario_file: Optional path to scenario file (for reporting)
        chain: Chain to use

    Returns:
        TraceResult with full execution trace
    """
    import time

    start_time = time.time()
    recorder = _TraceStepRecorder()

    # --- 1. Load strategy class
    recorder.add("Loading strategy from file", "LOADING")
    strategy_class, load_error = load_strategy_from_file(file_path)
    if load_error:
        recorder.add("Failed to load strategy", "ERROR", success=False, error=load_error)
        return _build_trace_failure(
            strategy_name="unknown",
            scenario_file=scenario_file,
            scenario=scenario,
            steps=recorder.steps,
            error=load_error,
            start_time=start_time,
        )

    assert strategy_class is not None
    strategy_name = getattr(strategy_class, "STRATEGY_NAME", strategy_class.__name__)
    recorder.add(f"Loaded strategy: {strategy_name}", "LOADED")

    # --- 2. Resolve config + compiler
    config_data = scenario.get("config", {})
    wallet_address_str = config_data.get("wallet_address", "0x" + "1" * 40)

    recorder.add("Initializing strategy", "INITIALIZING")
    try:
        config = _resolve_trace_config(file_path, config_data, chain, wallet_address_str)
        compiler = IntentCompiler(
            chain=chain,
            wallet_address=getattr(config, "wallet_address", wallet_address_str),
        )
        recorder.add("Strategy initialized", "INITIALIZED")
    except Exception as e:
        recorder.add("Strategy initialization failed", "ERROR", success=False, error=str(e))
        return _build_trace_failure(
            strategy_name=strategy_name,
            scenario_file=scenario_file,
            scenario=scenario,
            steps=recorder.steps,
            error=str(e),
            start_time=start_time,
        )

    # --- 3. Build market snapshot
    recorder.add("Creating market snapshot", "CREATING_MARKET")
    market = create_market_snapshot_from_scenario(scenario, chain=chain, wallet_address=wallet_address_str)
    recorder.add("Market snapshot created", "MARKET_READY")

    # --- 4. Run decide()
    recorder.add("Executing decide()", "DECIDING")
    try:
        strategy = strategy_class(config=config, compiler=compiler)  # type: ignore[call-arg]
        decide_result = strategy.decide(market)
        intent = _normalize_decide_result(decide_result, recorder)
        recorder.add(
            f"decide() returned: {intent.intent_type.value}",
            "DECIDED",
            intent=intent,
        )
    except Exception as e:
        recorder.add(f"decide() failed: {e}", "ERROR", success=False, error=str(e))
        return _build_trace_failure(
            strategy_name=strategy_name,
            scenario_file=scenario_file,
            scenario=scenario,
            steps=recorder.steps,
            error=str(e),
            start_time=start_time,
        )

    # --- 5. Compile final intent (skipped for HOLD)
    final_action_bundle, compile_error = _compile_intent_for_trace(intent, compiler, recorder)
    if compile_error is not None:
        return _build_trace_failure(
            strategy_name=strategy_name,
            scenario_file=scenario_file,
            scenario=scenario,
            steps=recorder.steps,
            error=compile_error,
            start_time=start_time,
        )

    recorder.add("Trace complete", "COMPLETE")

    return TraceResult(
        strategy_name=strategy_name,
        scenario_file=scenario_file,
        scenario=scenario,
        steps=recorder.steps,
        final_intent=intent.serialize(),
        final_action_bundle=final_action_bundle,
        success=True,
        error=None,
        execution_time_ms=(time.time() - start_time) * 1000,
    )


# =============================================================================
# Output Formatting
# =============================================================================


def _print_metadata_section(metadata: dict[str, Any]) -> None:
    """Print the METADATA block with optional author/tags/chains/protocols rows."""
    click.echo("-" * 40)
    click.echo("METADATA")
    click.echo("-" * 40)
    click.echo(f"  Name: {metadata.get('name', 'N/A')}")
    click.echo(f"  Version: {metadata.get('version', 'N/A')}")
    click.echo(f"  Description: {metadata.get('description', 'N/A')}")
    optional_rows: tuple[tuple[str, str], ...] = (
        ("author", "Author"),
        ("tags", "Tags"),
        ("supported_chains", "Chains"),
        ("supported_protocols", "Protocols"),
    )
    for key, label in optional_rows:
        value = metadata.get(key)
        if not value:
            continue
        if key == "author":
            click.echo(f"  {label}: {value}")
        else:
            click.echo(f"  {label}: {', '.join(value)}")
    click.echo()


def _print_intent_types_section(intent_types: list[str]) -> None:
    """Print the DETECTED INTENT TYPES bullet list."""
    click.echo("-" * 40)
    click.echo("DETECTED INTENT TYPES")
    click.echo("-" * 40)
    for intent_type in intent_types:
        click.echo(f"  • {intent_type}")
    click.echo()


def _print_state_diagrams_section(state_diagrams: dict[str, str]) -> None:
    """Print the STATE MACHINE DIAGRAMS section, one diagram per intent type."""
    click.echo("-" * 40)
    click.echo("STATE MACHINE DIAGRAMS")
    click.echo("-" * 40)
    for intent_type, diagram in state_diagrams.items():
        click.echo(f"\n[{intent_type}]")
        click.echo(diagram)


def _print_successful_bundle(bundle: dict[str, Any], verbose: bool) -> None:
    """Render a SUCCESS-status action bundle with transactions + total gas."""
    click.echo(f"  Status: {click.style('SUCCESS', fg='green')}")
    if bundle.get("transactions"):
        click.echo(f"  Transactions: {len(bundle['transactions'])}")
        for i, tx in enumerate(bundle["transactions"], 1):
            click.echo(f"    {i}. {tx['description']} (gas: {tx['gas_estimate']:,})")
            if verbose:
                click.echo(f"       To: {tx['to']}")
                click.echo(f"       Data: {tx['data']}")
    if bundle.get("total_gas_estimate"):
        click.echo(f"  Total Gas: {bundle['total_gas_estimate']:,}")


def _print_action_bundle(bundle: dict[str, Any], verbose: bool) -> None:
    """Render a single action bundle (success / note-only / failed)."""
    status = bundle.get("status", "UNKNOWN")
    if status == "SUCCESS":
        _print_successful_bundle(bundle, verbose)
    elif bundle.get("note"):
        click.echo(f"  Note: {bundle['note']}")
    else:
        click.echo(f"  Status: {click.style('FAILED', fg='red')}")
        click.echo(f"  Error: {bundle.get('error', 'Unknown error')}")


def _print_action_bundles_section(action_bundles: dict[str, dict[str, Any]], verbose: bool) -> None:
    """Print the EXAMPLE ACTION BUNDLES section."""
    click.echo()
    click.echo("-" * 40)
    click.echo("EXAMPLE ACTION BUNDLES")
    click.echo("-" * 40)
    for intent_type, bundle in action_bundles.items():
        click.echo(f"\n[{intent_type}]")
        _print_action_bundle(bundle, verbose)


def _print_inspection_errors_section(errors: list[str]) -> None:
    """Print WARNINGS/ERRORS block when inspection produced any errors."""
    if not errors:
        return
    click.echo()
    click.echo("-" * 40)
    click.echo(click.style("WARNINGS/ERRORS", fg="yellow"))
    click.echo("-" * 40)
    for error in errors:
        click.echo(f"  ⚠ {error}")


def print_inspection_result(result: IntentInspectionResult, verbose: bool = False) -> None:
    """Print inspection result to console."""
    click.echo()
    click.echo("=" * 70)
    click.echo("INTENT INSPECTION RESULT")
    click.echo("=" * 70)
    click.echo()

    click.echo(f"Strategy: {result.strategy_name}")
    click.echo(f"File: {result.strategy_path}")
    click.echo()

    if result.metadata:
        _print_metadata_section(result.metadata)
    _print_intent_types_section(result.intent_types)
    _print_state_diagrams_section(result.state_diagrams)
    _print_action_bundles_section(result.action_bundles, verbose)
    _print_inspection_errors_section(result.errors)

    click.echo()
    click.echo("=" * 70)


def _print_trace_step(step: TraceStep, verbose: bool) -> None:
    """Render one TraceStep line, plus optional verbose-intent and error rows."""
    status_icon = "✓" if step.success else "✗"
    status_color = "green" if step.success else "red"
    click.echo(f"  {step.step_number:2}. [{click.style(status_icon, fg=status_color)}] {step.description}")
    if verbose and step.intent:
        click.echo(f"      Intent: {step.intent.get('type', 'N/A')}")
    if step.error:
        click.echo(f"      {click.style(f'Error: {step.error}', fg='red')}")


def _print_trace_steps_section(steps: list[TraceStep], verbose: bool) -> None:
    """Print the EXECUTION TRACE block."""
    click.echo("-" * 40)
    click.echo("EXECUTION TRACE")
    click.echo("-" * 40)
    for step in steps:
        _print_trace_step(step, verbose)
    click.echo()


def _print_final_intent(final_intent: dict[str, Any], verbose: bool) -> None:
    """Render the final-intent type (and verbose details) under FINAL RESULT."""
    click.echo(f"  Intent Type: {final_intent.get('type', 'N/A')}")
    if verbose:
        click.echo(f"  Intent ID: {final_intent.get('intent_id', 'N/A')}")
        click.echo(f"  Full Intent: {json.dumps(final_intent, indent=4, default=str)}")


def _print_final_action_bundle(bundle: dict[str, Any]) -> None:
    """Render the ActionBundle subsection of FINAL RESULT."""
    click.echo()
    click.echo("  ActionBundle:")
    click.echo(f"    Status: {bundle.get('status', 'UNKNOWN')}")
    if bundle.get("transactions"):
        click.echo(f"    Transactions: {len(bundle['transactions'])}")
        for i, tx in enumerate(bundle["transactions"], 1):
            click.echo(f"      {i}. {tx['description']} (gas: {tx['gas_estimate']:,})")
    if bundle.get("total_gas_estimate"):
        click.echo(f"    Total Gas: {bundle['total_gas_estimate']:,}")


def _print_trace_final_section(result: TraceResult, verbose: bool) -> None:
    """Print the FINAL RESULT block (final intent + action bundle + status)."""
    click.echo("-" * 40)
    click.echo("FINAL RESULT")
    click.echo("-" * 40)
    if result.final_intent:
        _print_final_intent(result.final_intent, verbose)
    if result.final_action_bundle:
        _print_final_action_bundle(result.final_action_bundle)

    click.echo()
    if result.success:
        click.echo(click.style("Trace completed successfully.", fg="green"))
    else:
        click.echo(click.style(f"Trace failed: {result.error}", fg="red"))


def print_trace_result(result: TraceResult, verbose: bool = False) -> None:
    """Print trace result to console."""
    click.echo()
    click.echo("=" * 70)
    click.echo("INTENT TRACE RESULT")
    click.echo("=" * 70)
    click.echo()

    click.echo(f"Strategy: {result.strategy_name}")
    if result.scenario_file:
        click.echo(f"Scenario: {result.scenario_file}")
    click.echo(f"Execution Time: {result.execution_time_ms:.2f}ms")
    click.echo()

    _print_trace_steps_section(result.steps, verbose)
    _print_trace_final_section(result, verbose)

    click.echo()
    click.echo("=" * 70)


# =============================================================================
# CLI Commands
# =============================================================================


@click.group("intent")
def intent_group() -> None:
    """Intent debugging and inspection tools.

    Use these commands to inspect Intent-based strategies, visualize state
    machines, and trace execution with test scenarios.
    """
    pass


@intent_group.command("inspect")
@click.argument("strategy_file", type=click.Path(exists=True))
@click.option(
    "--chain",
    "-c",
    type=click.Choice(["ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche"]),
    default="arbitrum",
    help="Chain for ActionBundle compilation (default: arbitrum)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed output including transaction data",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output results to JSON file",
)
@click.option(
    "--diagram-only",
    is_flag=True,
    default=False,
    help="Show only state machine diagrams",
)
def inspect(
    strategy_file: str,
    chain: str,
    verbose: bool,
    output: str | None,
    diagram_only: bool,
) -> None:
    """Inspect an Intent-based strategy.

    Shows all possible intents the strategy can return, generated state machine
    diagrams, and ActionBundles that would be generated for each intent type.

    Examples:

        # Basic inspection
        almanak intent inspect templates/dynamic_lp_intent/strategy.py

        # With verbose output
        almanak intent inspect -v templates/dynamic_lp_intent/strategy.py

        # Output to JSON
        almanak intent inspect -o result.json templates/dynamic_lp_intent/strategy.py

        # Show only state diagrams
        almanak intent inspect --diagram-only templates/dynamic_lp_intent/strategy.py
    """
    file_path = Path(strategy_file)

    # Run inspection
    result = inspect_strategy(file_path, chain=chain)

    if output:
        # Write JSON output
        output_path = Path(output)
        with open(output_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        click.echo(f"Results written to: {output_path}")
    else:
        if diagram_only:
            # Show only diagrams
            click.echo()
            click.echo("=" * 70)
            click.echo(f"STATE MACHINE DIAGRAMS - {result.strategy_name}")
            click.echo("=" * 70)
            for intent_type, diagram in result.state_diagrams.items():
                click.echo(f"\n[{intent_type}]")
                click.echo(diagram)
        else:
            print_inspection_result(result, verbose=verbose)


@intent_group.command("trace")
@click.argument("strategy_file", type=click.Path(exists=True))
@click.option(
    "--scenario",
    "-s",
    type=click.Path(exists=True),
    required=True,
    help="JSON file with scenario data (prices, balances, RSI)",
)
@click.option(
    "--chain",
    "-c",
    type=click.Choice(["ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche"]),
    default="arbitrum",
    help="Chain for execution (default: arbitrum)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed trace output",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output trace to JSON file",
)
def trace(
    strategy_file: str,
    scenario: str,
    chain: str,
    verbose: bool,
    output: str | None,
) -> None:
    """Trace strategy execution with a test scenario.

    Runs the strategy's decide() method with the provided market conditions
    and shows step-by-step execution trace, including the final intent and
    compiled ActionBundle.

    Scenario JSON format:
    {
        "prices": {"ETH": 2000, "USDC": 1},
        "balances": {"USDC": {"balance": 10000, "balance_usd": 10000}},
        "rsi": {"ETH": 45},
        "config": {"volatility_factor": 1.5}
    }

    Examples:

        # Trace with scenario file
        almanak intent trace templates/dynamic_lp_intent/strategy.py -s scenario.json

        # With verbose output
        almanak intent trace -v -s scenario.json templates/dynamic_lp_intent/strategy.py

        # Output to JSON
        almanak intent trace -s scenario.json -o trace.json templates/dynamic_lp_intent/strategy.py
    """
    file_path = Path(strategy_file)
    scenario_path = Path(scenario)

    # Load scenario
    try:
        with open(scenario_path) as f:
            scenario_data = json.load(f)
    except Exception as e:
        click.echo(f"Error loading scenario file: {e}", err=True)
        raise click.Abort() from e

    # Run trace
    result = trace_strategy(
        file_path,
        scenario_data,
        scenario_file=str(scenario_path),
        chain=chain,
    )

    if output:
        # Write JSON output
        output_path = Path(output)
        with open(output_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        click.echo(f"Trace written to: {output_path}")
    else:
        print_trace_result(result, verbose=verbose)

    # Exit with error code if trace failed
    if not result.success:
        sys.exit(1)


# Export the command group
__all__ = ["intent_group", "inspect", "trace"]
