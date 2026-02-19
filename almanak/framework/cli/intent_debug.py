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

        # Find IntentStrategy subclasses
        strategy_classes = []
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and obj is not IntentStrategy and issubclass(obj, IntentStrategy):
                strategy_classes.append(obj)

        if not strategy_classes:
            return None, "No IntentStrategy subclass found in file"

        # Return the first one (or could return all)
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
    steps: list[TraceStep] = []
    step_counter = 0

    def add_step(
        description: str,
        state: str,
        intent: AnyIntent | None = None,
        action_bundle: dict[str, Any] | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        nonlocal step_counter
        step_counter += 1
        steps.append(
            TraceStep(
                step_number=step_counter,
                description=description,
                state=state,
                intent=intent.serialize() if intent else None,
                action_bundle=action_bundle,
                success=success,
                error=error,
            )
        )

    # Load strategy class
    add_step("Loading strategy from file", "LOADING")
    strategy_class, load_error = load_strategy_from_file(file_path)
    if load_error:
        add_step("Failed to load strategy", "ERROR", success=False, error=load_error)
        return TraceResult(
            strategy_name="unknown",
            scenario_file=scenario_file,
            scenario=scenario,
            steps=steps,
            final_intent=None,
            final_action_bundle=None,
            success=False,
            error=load_error,
            execution_time_ms=(time.time() - start_time) * 1000,
        )

    assert strategy_class is not None
    strategy_name = getattr(strategy_class, "STRATEGY_NAME", strategy_class.__name__)
    add_step(f"Loaded strategy: {strategy_name}", "LOADED")

    # Get config from scenario or use defaults
    config_data = scenario.get("config", {})
    config: Any = None
    wallet_address_str = config_data.get("wallet_address", "0x" + "1" * 40)

    # Try to instantiate strategy
    add_step("Initializing strategy", "INITIALIZING")
    try:
        # Look for config class
        config_module_path = file_path.parent / "config.py"
        if config_module_path.exists():
            spec = importlib.util.spec_from_file_location("config_module", config_module_path)
            if spec and spec.loader:
                config_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(config_module)

                # Find config class
                for name in dir(config_module):
                    obj = getattr(config_module, name)
                    if isinstance(obj, type) and name.endswith("Config"):
                        # Create config with defaults
                        config_defaults = {
                            "strategy_id": "trace-test",
                            "chain": chain,
                            "wallet_address": wallet_address_str,
                            **config_data,
                        }
                        try:
                            # Try from_dict class method
                            if hasattr(obj, "from_dict"):
                                config = obj.from_dict(config_defaults)
                                break
                        except Exception:
                            pass
                        try:
                            # Try direct instantiation
                            config = obj(**config_defaults)
                            break
                        except Exception:
                            pass
                else:
                    raise ValueError("No config class found")
        else:
            # Create a simple mock config that has the required attributes
            @dataclass
            class MockConfig:
                strategy_id: str = "trace-test"
                chain: str = chain
                wallet_address: str = wallet_address_str

                def to_dict(self) -> dict[str, Any]:
                    return {
                        "strategy_id": self.strategy_id,
                        "chain": self.chain,
                        "wallet_address": self.wallet_address,
                    }

            config = MockConfig()

        # Create compiler
        compiler = IntentCompiler(
            chain=chain,
            wallet_address=getattr(config, "wallet_address", wallet_address_str),
        )

        # Create strategy instance (simplified - may need adaptation per strategy)
        # For now, we'll directly call decide() with mocked market data
        add_step("Strategy initialized", "INITIALIZED")

    except Exception as e:
        add_step("Strategy initialization failed", "ERROR", success=False, error=str(e))
        return TraceResult(
            strategy_name=strategy_name,
            scenario_file=scenario_file,
            scenario=scenario,
            steps=steps,
            final_intent=None,
            final_action_bundle=None,
            success=False,
            error=str(e),
            execution_time_ms=(time.time() - start_time) * 1000,
        )

    # Create market snapshot from scenario
    add_step("Creating market snapshot", "CREATING_MARKET")
    market = create_market_snapshot_from_scenario(scenario, chain=chain, wallet_address=wallet_address_str)
    add_step("Market snapshot created", "MARKET_READY")

    # Try to call decide() via strategy instance
    add_step("Executing decide()", "DECIDING")
    try:
        # Instantiate strategy - pass all required arguments
        strategy = strategy_class(
            config=config,
            compiler=compiler,
        )  # type: ignore[call-arg]
        decide_result = strategy.decide(market)

        # Normalize DecideResult to single intent for trace
        from ..intents.vocabulary import AnyIntent, IntentSequence

        intent: AnyIntent | None = None
        if decide_result is None:
            intent = Intent.hold(reason="decide() returned None")
        elif isinstance(decide_result, IntentSequence):
            intent = decide_result.first
            add_step(
                f"decide() returned IntentSequence with {len(decide_result)} intents",
                "DECIDE_MULTI",
            )
        elif isinstance(decide_result, list):
            if not decide_result:
                intent = Intent.hold(reason="Empty result list")
            else:
                first_item = decide_result[0]
                if isinstance(first_item, IntentSequence):
                    intent = first_item.first
                else:
                    intent = first_item
            add_step(
                f"decide() returned list with {len(decide_result)} items for parallel execution",
                "DECIDE_MULTI",
            )
        else:
            intent = decide_result

        if intent is None:
            intent = Intent.hold(reason="No intent resolved")

        add_step(
            f"decide() returned: {intent.intent_type.value}",
            "DECIDED",
            intent=intent,
        )

    except Exception as e:
        add_step(f"decide() failed: {e}", "ERROR", success=False, error=str(e))
        return TraceResult(
            strategy_name=strategy_name,
            scenario_file=scenario_file,
            scenario=scenario,
            steps=steps,
            final_intent=None,
            final_action_bundle=None,
            success=False,
            error=str(e),
            execution_time_ms=(time.time() - start_time) * 1000,
        )

    # Compile intent if not HOLD
    final_action_bundle = None
    if intent.intent_type != IntentType.HOLD:
        add_step("Compiling intent to ActionBundle", "COMPILING")
        try:
            compilation_result = compiler.compile(intent)
            if compilation_result.status == CompilationStatus.SUCCESS:
                final_action_bundle = {
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
                add_step(
                    f"Compilation successful: {len(compilation_result.transactions)} transaction(s)",
                    "COMPILED",
                    action_bundle=final_action_bundle,
                )
            else:
                add_step(
                    f"Compilation failed: {compilation_result.error}",
                    "COMPILE_ERROR",
                    success=False,
                    error=compilation_result.error,
                )
        except Exception as e:
            add_step(f"Compilation error: {e}", "COMPILE_ERROR", success=False, error=str(e))
    else:
        add_step("HOLD intent - no compilation needed", "HOLD")

    add_step("Trace complete", "COMPLETE")

    return TraceResult(
        strategy_name=strategy_name,
        scenario_file=scenario_file,
        scenario=scenario,
        steps=steps,
        final_intent=intent.serialize(),
        final_action_bundle=final_action_bundle,
        success=True,
        error=None,
        execution_time_ms=(time.time() - start_time) * 1000,
    )


# =============================================================================
# Output Formatting
# =============================================================================


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

    # Metadata
    if result.metadata:
        click.echo("-" * 40)
        click.echo("METADATA")
        click.echo("-" * 40)
        click.echo(f"  Name: {result.metadata.get('name', 'N/A')}")
        click.echo(f"  Version: {result.metadata.get('version', 'N/A')}")
        click.echo(f"  Description: {result.metadata.get('description', 'N/A')}")
        if result.metadata.get("author"):
            click.echo(f"  Author: {result.metadata['author']}")
        if result.metadata.get("tags"):
            click.echo(f"  Tags: {', '.join(result.metadata['tags'])}")
        if result.metadata.get("supported_chains"):
            click.echo(f"  Chains: {', '.join(result.metadata['supported_chains'])}")
        if result.metadata.get("supported_protocols"):
            click.echo(f"  Protocols: {', '.join(result.metadata['supported_protocols'])}")
        click.echo()

    # Detected intent types
    click.echo("-" * 40)
    click.echo("DETECTED INTENT TYPES")
    click.echo("-" * 40)
    for intent_type in result.intent_types:
        click.echo(f"  • {intent_type}")
    click.echo()

    # State diagrams
    click.echo("-" * 40)
    click.echo("STATE MACHINE DIAGRAMS")
    click.echo("-" * 40)
    for intent_type, diagram in result.state_diagrams.items():
        click.echo(f"\n[{intent_type}]")
        click.echo(diagram)

    # Action bundles
    click.echo()
    click.echo("-" * 40)
    click.echo("EXAMPLE ACTION BUNDLES")
    click.echo("-" * 40)
    for intent_type, bundle in result.action_bundles.items():
        click.echo(f"\n[{intent_type}]")
        status = bundle.get("status", "UNKNOWN")
        if status == "SUCCESS":
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
        elif bundle.get("note"):
            click.echo(f"  Note: {bundle['note']}")
        else:
            click.echo(f"  Status: {click.style('FAILED', fg='red')}")
            click.echo(f"  Error: {bundle.get('error', 'Unknown error')}")

    # Errors
    if result.errors:
        click.echo()
        click.echo("-" * 40)
        click.echo(click.style("WARNINGS/ERRORS", fg="yellow"))
        click.echo("-" * 40)
        for error in result.errors:
            click.echo(f"  ⚠ {error}")

    click.echo()
    click.echo("=" * 70)


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

    # Steps
    click.echo("-" * 40)
    click.echo("EXECUTION TRACE")
    click.echo("-" * 40)
    for step in result.steps:
        status_icon = "✓" if step.success else "✗"
        status_color = "green" if step.success else "red"
        click.echo(f"  {step.step_number:2}. [{click.style(status_icon, fg=status_color)}] {step.description}")
        if verbose and step.intent:
            click.echo(f"      Intent: {step.intent.get('type', 'N/A')}")
        if step.error:
            click.echo(f"      {click.style(f'Error: {step.error}', fg='red')}")

    click.echo()

    # Final intent
    click.echo("-" * 40)
    click.echo("FINAL RESULT")
    click.echo("-" * 40)
    if result.final_intent:
        click.echo(f"  Intent Type: {result.final_intent.get('type', 'N/A')}")
        if verbose:
            click.echo(f"  Intent ID: {result.final_intent.get('intent_id', 'N/A')}")
            click.echo(f"  Full Intent: {json.dumps(result.final_intent, indent=4, default=str)}")

    if result.final_action_bundle:
        click.echo()
        click.echo("  ActionBundle:")
        bundle = result.final_action_bundle
        click.echo(f"    Status: {bundle.get('status', 'UNKNOWN')}")
        if bundle.get("transactions"):
            click.echo(f"    Transactions: {len(bundle['transactions'])}")
            for i, tx in enumerate(bundle["transactions"], 1):
                click.echo(f"      {i}. {tx['description']} (gas: {tx['gas_estimate']:,})")
        if bundle.get("total_gas_estimate"):
            click.echo(f"    Total Gas: {bundle['total_gas_estimate']:,}")

    # Final status
    click.echo()
    if result.success:
        click.echo(click.style("Trace completed successfully.", fg="green"))
    else:
        click.echo(click.style(f"Trace failed: {result.error}", fg="red"))

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
