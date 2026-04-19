import ast
import subprocess
import tempfile
from pathlib import Path

import pytest

from almanak._version import __version__
from almanak.framework.cli.new_strategy import (
    StrategyTemplate,
    SupportedChain,
    generate_config_json,
    generate_pyproject_toml,
    generate_strategy_file,
)

ALL_TEMPLATES = list(StrategyTemplate)


def _get_strategy_class_def(tree: ast.AST) -> ast.ClassDef:
    """Return the emitted IntentStrategy subclass definition.

    Scaffolded files may contain both a ``<Template>State(StrEnum)`` and the
    strategy class. The strategy class name always ends in ``Strategy`` and is
    the class we want for method/teardown assertions.
    """
    class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    assert class_defs, "No class definition found in generated code"
    strategy_classes = [c for c in class_defs if c.name.endswith("Strategy")]
    assert strategy_classes, (
        f"No class ending in 'Strategy' found (got: {[c.name for c in class_defs]})"
    )
    return strategy_classes[0]


# ---------------------------------------------------------------------------
# pyproject.toml tests (existing)
# ---------------------------------------------------------------------------


def test_generate_pyproject_toml_uses_installed_version() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert 'name = "mean_reversion"' in pyproject
    assert f"almanak>={__version__}" in pyproject
    assert "interval = 60" in pyproject


def test_generate_pyproject_toml_has_no_build_system() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert "[build-system]" not in pyproject


def test_generate_pyproject_toml_has_no_framework_or_version_fields() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert 'framework = "v2"' not in pyproject
    assert f'version = "{__version__}"' not in pyproject
    # Only version that should appear is under [project] and in the dep spec
    assert 'version = "0.1.0"' in pyproject


def test_generate_pyproject_toml_has_run_section() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert "[tool.almanak.run]" in pyproject
    assert "interval = 60" in pyproject


# ---------------------------------------------------------------------------
# Smoke tests: every template generates valid, parseable, lintable Python
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_is_valid_python(template: StrategyTemplate) -> None:
    """Generated strategy.py must parse without syntax errors."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        # ast.parse will raise SyntaxError if code is invalid
        ast.parse(code)


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_passes_ruff(template: StrategyTemplate) -> None:
    """Generated strategy.py must pass ruff check (no lint errors)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        strategy_path = Path(tmpdir) / "strategy.py"
        strategy_path.write_text(code)

        result = subprocess.run(
            ["uv", "run", "ruff", "check", str(strategy_path), "--select", "E,W,F,I"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"ruff check failed for {template.value}:\n{result.stdout}\n{result.stderr}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_has_decide_method(template: StrategyTemplate) -> None:
    """Generated strategy must define a decide() method."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        tree = ast.parse(code)
        strategy_class = _get_strategy_class_def(tree)
        method_names = [
            n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)
        ]
        assert "decide" in method_names, f"decide() not found in {strategy_class.name}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_has_teardown_methods(template: StrategyTemplate) -> None:
    """Generated strategy must define teardown stubs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        tree = ast.parse(code)
        strategy_class = _get_strategy_class_def(tree)
        method_names = [
            n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)
        ]
        for required in ("get_open_positions", "generate_teardown_intents"):
            assert required in method_names, f"{required}() missing from {strategy_class.name}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_config_json_is_valid(template: StrategyTemplate) -> None:
    """Generated config.json must be valid JSON (no parse errors)."""
    import json

    config_str = generate_config_json(
        name="Smoke Test",
        template=template,
        chain=SupportedChain.ARBITRUM,
    )
    config = json.loads(config_str)
    assert isinstance(config, dict)


@pytest.mark.parametrize(
    "chain",
    [SupportedChain.ARBITRUM, SupportedChain.BASE, SupportedChain.MANTLE, SupportedChain.SOLANA],
    ids=lambda c: c.value,
)
def test_config_json_emits_chain_as_first_key(chain: SupportedChain) -> None:
    """Generated config.json must emit the chain as the first top-level key.

    sdk-planner and other tooling read chain from config.json rather than
    importing the strategy module. The framework reads this as an explicit
    override of the @almanak_strategy decorator's default_chain.
    """
    import json

    config_str = generate_config_json(
        name="Chain Field Test",
        template=StrategyTemplate.TA_SWAP,
        chain=chain,
    )
    config = json.loads(config_str)
    assert list(config.keys())[0] == "chain", "chain must be the first top-level key"
    assert config["chain"] == chain.value


# ---------------------------------------------------------------------------
# Mantle-specific anvil_funding config branch
# ---------------------------------------------------------------------------


def test_generate_config_json_mantle_includes_anvil_funding() -> None:
    """generate_config_json for Mantle includes MNT/WMNT/WETH anvil_funding entries."""
    import json

    config_str = generate_config_json(
        name="Mantle Test",
        template=StrategyTemplate.TA_SWAP,
        chain=SupportedChain.MANTLE,
    )
    config = json.loads(config_str)
    assert "anvil_funding" in config, "Mantle config must include anvil_funding"
    funding = config["anvil_funding"]
    assert funding.get("MNT") == 1000
    assert funding.get("WMNT") == 10
    assert funding.get("WETH") == 5
    assert funding.get("USDC") == 10000


def test_generate_config_json_all_chains_include_anvil_funding() -> None:
    """generate_config_json for all chains includes anvil_funding with chain-appropriate tokens."""
    import json

    for chain in (SupportedChain.ARBITRUM, SupportedChain.BASE, SupportedChain.OPTIMISM):
        config_str = generate_config_json(
            name="Anvil Funding Test",
            template=StrategyTemplate.TA_SWAP,
            chain=chain,
        )
        config = json.loads(config_str)
        assert "anvil_funding" in config, f"Chain {chain} must include anvil_funding"
        funding = config["anvil_funding"]
        assert funding.get("ETH") == 10, f"Chain {chain} must fund 10 ETH"
        assert funding.get("WETH") == 5, f"Chain {chain} must fund 5 WETH"
        assert funding.get("USDC") == 10000, f"Chain {chain} must fund 10000 USDC"


def test_generate_config_json_bsc_uses_native_tokens() -> None:
    """BSC anvil_funding uses BNB/WBNB instead of ETH/WETH."""
    import json

    config_str = generate_config_json(
        name="BSC Test",
        template=StrategyTemplate.BLANK,
        chain=SupportedChain.BSC,
    )
    config = json.loads(config_str)
    funding = config["anvil_funding"]
    assert funding.get("BNB") == 10
    assert funding.get("WBNB") == 5
    assert funding.get("WETH") == 5
    assert funding.get("USDC") == 10000


def test_generate_config_json_sonic_uses_native_tokens() -> None:
    """Sonic anvil_funding uses S instead of ETH."""
    import json

    config_str = generate_config_json(
        name="Sonic Test",
        template=StrategyTemplate.BLANK,
        chain=SupportedChain.SONIC,
    )
    config = json.loads(config_str)
    funding = config["anvil_funding"]
    assert funding.get("S") == 100
    assert funding.get("WETH") == 5
    assert funding.get("USDC") == 10000


def test_generate_config_json_avalanche_uses_native_tokens() -> None:
    """Avalanche anvil_funding uses AVAX/WAVAX instead of ETH/WETH."""
    import json

    config_str = generate_config_json(
        name="Avalanche Test",
        template=StrategyTemplate.BLANK,
        chain=SupportedChain.AVALANCHE,
    )
    config = json.loads(config_str)
    funding = config["anvil_funding"]
    assert funding.get("AVAX") == 100
    assert funding.get("WAVAX") == 10
    assert funding.get("WETH") == 5
    assert funding.get("USDC") == 10000


# ---------------------------------------------------------------------------
# Stateful templates must have on_intent_executed callback
# ---------------------------------------------------------------------------

STATEFUL_TEMPLATES = [
    StrategyTemplate.DYNAMIC_LP,
    StrategyTemplate.LENDING_LOOP,
    StrategyTemplate.BASIS_TRADE,
    StrategyTemplate.VAULT_YIELD,
    StrategyTemplate.PERPS,
    StrategyTemplate.MULTI_STEP,
    StrategyTemplate.STAKING,
]


@pytest.mark.parametrize("template", STATEFUL_TEMPLATES, ids=lambda t: t.value)
def test_stateful_templates_have_callbacks(template: StrategyTemplate) -> None:
    """Stateful templates must define on_intent_executed for state tracking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        tree = ast.parse(code)
        strategy_class = _get_strategy_class_def(tree)
        method_names = [
            n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)
        ]
        assert "on_intent_executed" in method_names, (
            f"on_intent_executed() missing from {template.value} template"
        )


# ---------------------------------------------------------------------------
# LENDING_LOOP: state machine transitions and persistence
# ---------------------------------------------------------------------------


def _make_mock_intent(intent_type_value: str):
    """Create a minimal mock intent with intent_type.value == intent_type_value."""

    class _IntentType:
        value = intent_type_value

    class _MockIntent:
        intent_type = _IntentType()

    return _MockIntent()


def _scaffold_lending_loop():
    """Scaffold a LENDING_LOOP strategy and return a concrete subclass for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Test Loop",
            template=StrategyTemplate.LENDING_LOOP,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    # Execute the scaffolded code; capture the class object.
    ns: dict = {}
    exec(compile(code, "<scaffold>", "exec"), ns)  # noqa: S102
    # Find the scaffolded strategy class: a direct subclass of IntentStrategy
    # that is not IntentStrategy itself (which also appears in ns after import).
    from almanak.framework.strategies import IntentStrategy as _Base

    base_cls = next(
        v
        for v in ns.values()
        if isinstance(v, type) and issubclass(v, _Base) and v is not _Base
    )

    # Create a concrete subclass satisfying IntentStrategy's abstract interface.
    # Only the callback/persistence methods are under test; the abstract methods
    # are already tested by other scaffold tests.
    class _Concrete(base_cls):
        def decide(self, market):
            return None  # Not under test

        def get_open_positions(self):
            return None  # Not under test

        def generate_teardown_intents(self, mode=None, market=None):
            return []  # Not under test

    return _Concrete


class _MockMarket:
    """Minimal market mock so __init__ doesn't fail on market access."""

    def price(self, token):
        return __import__("decimal").Decimal("2000")

    def balance(self, token):
        class B:
            balance = __import__("decimal").Decimal("1")
            balance_usd = __import__("decimal").Decimal("2000")

        return B()

    def funding_rate(self, protocol, market):
        class F:
            rate_hourly = __import__("decimal").Decimal("0.0001")

        return F()


def _make_strategy():
    """Instantiate the scaffolded LENDING_LOOP strategy with a stub config."""
    cls = _scaffold_lending_loop()

    class _Cfg:
        def get(self, key, default=None):
            defaults = {
                "supply_amount": "1",
                "borrow_amount": "500",
                "target_leverage": "2.0",
                "borrow_ratio": "0.7",
                "min_health_factor": "1.5",
                "min_collateral_usd": "100",
                "collateral_token": "WETH",
                "borrow_token": "USDC",
            }
            return defaults.get(key, default)

    strat = cls.__new__(cls)
    # Manually run the init params (avoid full IntentStrategy.__init__)
    from decimal import Decimal

    strat.config = _Cfg()
    strat.supply_amount = Decimal("1")
    strat.borrow_amount = Decimal("500")
    strat.target_leverage = Decimal("2.0")
    strat.borrow_ratio = Decimal("0.7")
    strat.min_health_factor = Decimal("1.5")
    strat.min_collateral_usd = Decimal("100")
    strat.collateral_token = "WETH"
    strat.borrow_token = "USDC"
    strat._loop_state = "idle"
    strat._loop_count = 0
    strat._current_leverage = Decimal("1.0")
    return strat


def test_lending_loop_supply_transition() -> None:
    """SUPPLY intent -> state advances to 'supplied'."""
    strat = _make_strategy()
    strat.on_intent_executed(_make_mock_intent("SUPPLY"), success=True, result=None)
    assert strat._loop_state == "supplied"
    assert strat._loop_count == 0  # loop_count increments only on SWAP


def test_lending_loop_borrow_transition() -> None:
    """BORROW intent -> state advances to 'borrowed'."""
    strat = _make_strategy()
    strat._loop_state = "supplied"
    strat.on_intent_executed(_make_mock_intent("BORROW"), success=True, result=None)
    assert strat._loop_state == "borrowed"


def test_lending_loop_swap_below_target_loops() -> None:
    """SWAP when leverage < target -> state returns to 'idle' for next loop."""
    strat = _make_strategy()
    strat._loop_state = "borrowed"
    strat._loop_count = 0
    # After 1 loop: leverage = 1 + 0.7 = 1.7 < 2.0 target
    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert strat._loop_state == "idle"
    assert strat._loop_count == 1


def test_lending_loop_swap_at_target_monitors() -> None:
    """SWAP when leverage >= target -> state advances to 'monitoring'."""
    from decimal import Decimal

    strat = _make_strategy()
    strat._loop_state = "borrowed"
    strat._loop_count = 1  # Will become 2 after SWAP
    # After 2 loops: 1 + 0.7 + 0.49 = 2.19 >= 2.0 target
    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert strat._loop_state == "monitoring"
    assert strat._loop_count == 2
    assert strat._current_leverage >= Decimal("2.0")


def test_lending_loop_no_transition_on_failure() -> None:
    """success=False -> no state change."""
    strat = _make_strategy()
    strat._loop_state = "borrowed"
    original_count = strat._loop_count
    strat.on_intent_executed(_make_mock_intent("SWAP"), success=False, result=None)
    assert strat._loop_state == "borrowed"
    assert strat._loop_count == original_count


def test_lending_loop_persistence_round_trip() -> None:
    """get_persistent_state() / load_persistent_state() round-trips all fields."""
    from decimal import Decimal

    strat = _make_strategy()
    strat._loop_state = "monitoring"
    strat._loop_count = 3
    strat._current_leverage = Decimal("2.19")

    saved = strat.get_persistent_state()
    assert saved["loop_state"] == "monitoring"
    assert saved["loop_count"] == 3
    assert saved["current_leverage"] == "2.19"

    # Restore into a fresh instance
    strat2 = _make_strategy()
    strat2.load_persistent_state(saved)
    assert strat2._loop_state == "monitoring"
    assert strat2._loop_count == 3
    assert strat2._current_leverage == Decimal("2.19")


def test_lending_loop_persistence_decimal_as_string() -> None:
    """load_persistent_state handles current_leverage stored as a string."""
    from decimal import Decimal

    strat = _make_strategy()
    strat.load_persistent_state({"loop_state": "idle", "loop_count": 0, "current_leverage": "1.7"})
    assert strat._current_leverage == Decimal("1.7")


def test_lending_loop_persistence_zero_loop_count() -> None:
    """load_persistent_state handles loop_count=0 (initial state)."""
    from decimal import Decimal

    strat = _make_strategy()
    strat.load_persistent_state({"loop_state": "idle", "loop_count": 0, "current_leverage": "1.0"})
    assert strat._loop_count == 0
    assert strat._loop_state == "idle"
    assert strat._current_leverage == Decimal("1.0")


# ---------------------------------------------------------------------------
# BASIS_TRADE: state machine transitions and persistence
# ---------------------------------------------------------------------------


def _scaffold_basis_trade():
    """Scaffold a BASIS_TRADE strategy and return a concrete subclass for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Test Basis",
            template=StrategyTemplate.BASIS_TRADE,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    ns: dict = {}
    exec(compile(code, "<scaffold>", "exec"), ns)  # noqa: S102
    from almanak.framework.strategies import IntentStrategy as _Base

    base_cls = next(
        v
        for v in ns.values()
        if isinstance(v, type) and issubclass(v, _Base) and v is not _Base
    )

    class _Concrete(base_cls):
        def decide(self, market):
            return None

        def get_open_positions(self):
            return None

        def generate_teardown_intents(self, mode=None, market=None):
            return []

    return _Concrete


def _make_basis_strategy():
    """Instantiate the scaffolded BASIS_TRADE strategy with a stub config."""
    from decimal import Decimal

    cls = _scaffold_basis_trade()

    class _Cfg:
        def get(self, key, default=None):
            defaults = {
                "spot_size_usd": "10000",
                "hedge_ratio": "1.0",
                "funding_entry_threshold": "0.0001",
                "funding_exit_threshold": "-0.00005",
                "base_token": "WETH",
                "quote_token": "USDC",
                "perp_market": "ETH/USD",
            }
            return defaults.get(key, default)

    strat = cls.__new__(cls)
    strat.config = _Cfg()
    strat.spot_size_usd = Decimal("10000")
    strat.hedge_ratio = Decimal("1.0")
    strat.funding_entry_threshold = Decimal("0.0001")
    strat.funding_exit_threshold = Decimal("-0.00005")
    strat.base_token = "WETH"
    strat.quote_token = "USDC"
    strat.perp_market = "ETH/USD"
    strat._trade_state = "idle"
    return strat


def test_basis_trade_swap_idle_to_spot_bought() -> None:
    """SWAP when idle -> state advances to 'spot_bought'."""
    strat = _make_basis_strategy()
    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert strat._trade_state == "spot_bought"


def test_basis_trade_perp_open_to_hedged() -> None:
    """PERP_OPEN -> state advances to 'hedged'."""
    strat = _make_basis_strategy()
    strat._trade_state = "spot_bought"
    strat.on_intent_executed(_make_mock_intent("PERP_OPEN"), success=True, result=None)
    assert strat._trade_state == "hedged"


def test_basis_trade_perp_close_to_unwinding() -> None:
    """PERP_CLOSE -> state advances to 'unwinding'."""
    strat = _make_basis_strategy()
    strat._trade_state = "hedged"
    strat.on_intent_executed(_make_mock_intent("PERP_CLOSE"), success=True, result=None)
    assert strat._trade_state == "unwinding"


def test_basis_trade_swap_unwinding_to_idle() -> None:
    """SWAP when unwinding -> state returns to 'idle' (unwind complete)."""
    strat = _make_basis_strategy()
    strat._trade_state = "unwinding"
    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert strat._trade_state == "idle"


def test_basis_trade_full_lifecycle() -> None:
    """Full lifecycle: idle -> spot_bought -> hedged -> unwinding -> idle."""
    strat = _make_basis_strategy()
    assert strat._trade_state == "idle"

    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert strat._trade_state == "spot_bought"

    strat.on_intent_executed(_make_mock_intent("PERP_OPEN"), success=True, result=None)
    assert strat._trade_state == "hedged"

    strat.on_intent_executed(_make_mock_intent("PERP_CLOSE"), success=True, result=None)
    assert strat._trade_state == "unwinding"

    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert strat._trade_state == "idle"


def test_basis_trade_no_transition_on_failure() -> None:
    """success=False -> no state change."""
    strat = _make_basis_strategy()
    strat._trade_state = "hedged"
    strat.on_intent_executed(_make_mock_intent("PERP_CLOSE"), success=False, result=None)
    assert strat._trade_state == "hedged"


def test_basis_trade_persistence_round_trip() -> None:
    """get_persistent_state() / load_persistent_state() round-trips trade_state."""
    strat = _make_basis_strategy()
    strat._trade_state = "unwinding"

    saved = strat.get_persistent_state()
    assert saved["trade_state"] == "unwinding"

    strat2 = _make_basis_strategy()
    strat2.load_persistent_state(saved)
    assert strat2._trade_state == "unwinding"


def test_basis_trade_persistence_all_states() -> None:
    """Persistence round-trips correctly for each trade_state value."""
    for state in ("idle", "spot_bought", "hedged", "unwinding"):
        strat = _make_basis_strategy()
        strat._trade_state = state
        saved = strat.get_persistent_state()
        strat2 = _make_basis_strategy()
        strat2.load_persistent_state(saved)
        assert strat2._trade_state == state, f"round-trip failed for state={state}"


# ---------------------------------------------------------------------------
# LP template guardrails: pool format, amounts, config keys
# ---------------------------------------------------------------------------


def test_dynamic_lp_config_uses_symbolic_pool() -> None:
    """dynamic_lp config.json must use symbolic pool format, not raw hex."""
    import json

    config_str = generate_config_json("Test LP", StrategyTemplate.DYNAMIC_LP, SupportedChain.ARBITRUM)
    config = json.loads(config_str)
    assert "pool" in config, "dynamic_lp config must have 'pool' key"
    assert "pool_address" not in config, "dynamic_lp config must NOT have 'pool_address'"
    assert config["pool"] == "WETH/USDC/3000", f"Expected 'WETH/USDC/3000', got '{config['pool']}'"


def test_multi_step_config_uses_symbolic_pool() -> None:
    """multi_step config.json must use symbolic pool format, not raw hex."""
    import json

    config_str = generate_config_json("Test MS", StrategyTemplate.MULTI_STEP, SupportedChain.ARBITRUM)
    config = json.loads(config_str)
    assert "pool" in config, "multi_step config must have 'pool' key"
    assert "pool_address" not in config, "multi_step config must NOT have 'pool_address'"
    assert config["pool"] == "WETH/USDC/3000", f"Expected 'WETH/USDC/3000', got '{config['pool']}'"


def test_multi_step_config_uses_rebalance_drift_pct() -> None:
    """multi_step config must use rebalance_drift_pct, not rebalance_threshold_pct."""
    import json

    config_str = generate_config_json("Test MS", StrategyTemplate.MULTI_STEP, SupportedChain.ARBITRUM)
    config = json.loads(config_str)
    assert "rebalance_drift_pct" in config, "multi_step config must have 'rebalance_drift_pct'"
    assert "rebalance_threshold_pct" not in config, "multi_step config must NOT have 'rebalance_threshold_pct'"
    assert config["rebalance_drift_pct"] == 3, "default drift should be 3 (%)"


def test_dynamic_lp_strategy_provides_both_lp_amounts() -> None:
    """dynamic_lp decide() must fetch both balances and pass both to lp_open."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            "Test LP", StrategyTemplate.DYNAMIC_LP, SupportedChain.ARBITRUM, output_dir=Path(tmpdir),
        )
    # Must NOT hardcode amount1=0 (single-sided)
    assert 'amount1=Decimal("0")' not in code, "LP_OPEN must not hardcode amount1=0"
    # Must fetch both base and quote balances before LP_OPEN
    assert "market.balance(self.base_token)" in code, "Must fetch base_token balance for LP"
    assert "market.balance(self.quote_token)" in code, "Must fetch quote_token balance for LP"
    # Must reference self.pool not self.pool_address
    assert "self.pool_address" not in code, "Must use self.pool, not self.pool_address"


def test_lp_templates_agents_md_has_footguns() -> None:
    """LP template AGENTS.md must include Common Mistakes section."""
    from almanak.framework.cli.strategy_agent_guide import StrategyGuideConfig, generate_strategy_agents_md

    for template in (StrategyTemplate.DYNAMIC_LP, StrategyTemplate.MULTI_STEP):
        guide_config = StrategyGuideConfig(
            strategy_name="test",
            template_name=template.value,
            chain="arbitrum",
            class_name="TestStrategy",
        )
        content = generate_strategy_agents_md(guide_config)
        assert "Common Mistakes" in content, f"{template.value} AGENTS.md must have Common Mistakes section"
        assert "symbolic format" in content.lower() or "raw hex" in content.lower(), (
            f"{template.value} AGENTS.md must warn about pool format"
        )


# ---------------------------------------------------------------------------
# VIB-2328: SDK root auto-detection for --output-dir default
# ---------------------------------------------------------------------------


def test_new_strategy_defaults_to_incubating_when_sdk_root(tmp_path: Path) -> None:
    """When strategies/incubating/ exists in cwd, default output is strategies/incubating/<name>."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    # Create SDK-root-like structure
    incubating_dir = tmp_path / "strategies" / "incubating"
    incubating_dir.mkdir(parents=True)

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        # Change into the SDK-root-like directory so Path.cwd() picks up strategies/incubating/
        # Unset CI so auto-detection is not suppressed (CI env var disables it in CI environments)
        os.chdir(tmp_path)
        result = runner.invoke(new_strategy, ["--name", "my_auto_strat", "--chain", "arbitrum"], env={"CI": ""})

    assert result.exit_code == 0, result.output
    assert (incubating_dir / "my_auto_strat").exists(), (
        "Expected strategy to be created in strategies/incubating/ when that directory exists"
    )


def test_new_strategy_falls_back_to_cwd_when_no_incubating(tmp_path: Path) -> None:
    """When strategies/incubating/ does NOT exist, default output is ./<name> in cwd."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        result = runner.invoke(new_strategy, ["--name", "my_fallback_strat", "--chain", "arbitrum"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "my_fallback_strat").exists(), (
        "Expected strategy to be created in cwd when strategies/incubating/ does not exist"
    )
    assert not (tmp_path / "strategies" / "incubating" / "my_fallback_strat").exists()


def test_new_strategy_output_dir_flag_overrides_auto_detection(tmp_path: Path) -> None:
    """Explicit --output-dir always wins over auto-detection."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    # Create SDK-root-like structure
    incubating_dir = tmp_path / "strategies" / "incubating"
    incubating_dir.mkdir(parents=True)
    explicit_dir = tmp_path / "explicit_output"
    explicit_dir.mkdir()

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        result = runner.invoke(
            new_strategy,
            ["--name", "my_explicit_strat", "--chain", "arbitrum", "--output-dir", str(explicit_dir / "my_explicit_strat")],
            env={"CI": ""},
        )

    assert result.exit_code == 0, result.output
    assert (explicit_dir / "my_explicit_strat").exists(), "Expected strategy in explicit output dir"
    assert not (incubating_dir / "my_explicit_strat").exists(), (
        "Strategy should NOT be in strategies/incubating/ when --output-dir is specified"
    )


# ---------------------------------------------------------------------------
# Directory validation: dotfile-only dirs, file paths, cleanup safety
# ---------------------------------------------------------------------------


def test_new_strategy_allows_dotfile_only_dir(tmp_path: Path) -> None:
    """Scaffolding into a directory with only dotfiles should succeed."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    target = tmp_path / "workspace"
    target.mkdir()
    (target / ".almanak").mkdir()
    (target / ".almanakdb").mkdir()
    (target / ".almanak" / "sdk.json").write_text("{}")

    runner = CliRunner()
    os.chdir(tmp_path)
    result = runner.invoke(
        new_strategy,
        ["--name", "my_strat", "--chain", "arbitrum", "--output-dir", str(target)],
    )

    assert result.exit_code == 0, result.output
    assert (target / "strategy.py").exists()
    assert (target / ".almanak" / "sdk.json").exists(), "Dotfiles should be preserved"


def test_new_strategy_rejects_dir_with_real_files(tmp_path: Path) -> None:
    """Scaffolding into a directory with non-dotfiles should fail."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    target = tmp_path / "workspace"
    target.mkdir()
    (target / "strategy.py").write_text("# existing")

    runner = CliRunner()
    os.chdir(tmp_path)
    result = runner.invoke(
        new_strategy,
        ["--name", "my_strat", "--chain", "arbitrum", "--output-dir", str(target)],
    )

    assert result.exit_code != 0
    assert "already contains files" in result.output


def test_new_strategy_rejects_file_path(tmp_path: Path) -> None:
    """Scaffolding with -o pointing to a file should fail gracefully."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    target = tmp_path / "not_a_dir"
    target.write_text("I am a file")

    runner = CliRunner()
    os.chdir(tmp_path)
    result = runner.invoke(
        new_strategy,
        ["--name", "my_strat", "--chain", "arbitrum", "--output-dir", str(target)],
    )

    assert result.exit_code != 0
    assert "not a directory" in result.output


def test_new_strategy_no_rmtree_on_existing_dir_failure(tmp_path: Path) -> None:
    """If scaffold fails in a pre-existing dir, the directory should NOT be deleted."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    target = tmp_path / "workspace"
    target.mkdir()
    (target / ".almanak").mkdir()
    (target / ".almanak" / "important.json").write_text("{}")

    # Make tests/ dir read-only to force a failure during scaffold
    tests_dir = target / "tests"
    tests_dir.mkdir()
    tests_dir.chmod(0o444)

    runner = CliRunner()
    os.chdir(tmp_path)
    result = runner.invoke(
        new_strategy,
        ["--name", "my_strat", "--chain", "arbitrum", "--output-dir", str(target)],
    )

    # Restore permissions for cleanup
    tests_dir.chmod(0o755)

    assert result.exit_code != 0, "Expected scaffold to fail due to read-only tests/ directory"
    # Directory should still exist (not rmtree'd)
    assert target.exists(), "Pre-existing directory should not be deleted on scaffold failure"
    assert (target / ".almanak" / "important.json").exists(), "Dotfiles should be preserved"


# ---------------------------------------------------------------------------
# StrEnum state machines: emitted code + runtime behavior + persistence
# ---------------------------------------------------------------------------

# Templates that emit a StrEnum state machine. Keep in sync with
# ``_TEMPLATE_STATE_ENUMS`` in ``almanak.framework.cli.new_strategy``.
STATEFUL_STRENUM_TEMPLATES = {
    StrategyTemplate.LENDING_LOOP: ("LendingLoopState", ("IDLE", "SUPPLIED", "BORROWED", "MONITORING")),
    StrategyTemplate.BASIS_TRADE: ("BasisTradeState", ("IDLE", "SPOT_BOUGHT", "HEDGED", "UNWINDING")),
    StrategyTemplate.VAULT_YIELD: ("VaultYieldState", ("IDLE", "DEPOSITED")),
    StrategyTemplate.PERPS: ("PerpsState", ("IDLE", "OPEN")),
    StrategyTemplate.STAKING: ("StakingState", ("IDLE", "STAKED")),
}


@pytest.mark.parametrize(
    "template,enum_name,members",
    [(t, name, members) for t, (name, members) in STATEFUL_STRENUM_TEMPLATES.items()],
    ids=lambda v: v.value if isinstance(v, StrategyTemplate) else str(v),
)
def test_stateful_templates_emit_strenum_class(
    template: StrategyTemplate, enum_name: str, members: tuple[str, ...]
) -> None:
    """Stateful scaffolds must emit a StrEnum subclass with the expected members."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="StrEnum Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    # Grep: the import must appear exactly once, at module level.
    assert "from enum import StrEnum" in code, (
        f"{template.value} must emit 'from enum import StrEnum' import"
    )
    # The class declaration must be present with ``StrEnum`` as the base.
    assert f"class {enum_name}(StrEnum):" in code, (
        f"{template.value} must emit 'class {enum_name}(StrEnum):' definition"
    )
    # Every declared member must appear. We match the form ``NAME = "value"``
    # since that's the canonical emitted form.
    for member in members:
        assert f"{member} = " in code, f"{enum_name} must define member {member}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_stateless_templates_do_not_emit_strenum_import(template: StrategyTemplate) -> None:
    """Stateless templates (e.g. BLANK, TA_SWAP, COPY_TRADER) must NOT import StrEnum.

    Adding an unused ``from enum import StrEnum`` would fail ruff F401 linting.
    """
    if template in STATEFUL_STRENUM_TEMPLATES:
        return
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Stateless Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    assert "from enum import StrEnum" not in code, (
        f"{template.value} must not import StrEnum when no state machine is emitted"
    )


@pytest.mark.parametrize("template", list(STATEFUL_STRENUM_TEMPLATES), ids=lambda t: t.value)
def test_stateful_templates_do_not_use_bare_state_strings(template: StrategyTemplate) -> None:
    """Stateful scaffolds must reference state values via the StrEnum, not bare strings.

    Guards against regression: it's easy to accidentally leave ``"idle"`` or
    ``"open"`` literals in the generator. The enum class name and ``.MEMBER``
    access pattern is the only source of truth.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="StrEnum Hygiene",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    enum_name, members = STATEFUL_STRENUM_TEMPLATES[template]
    state_values_in_enum = {
        # Recover the enum value from the ``MEMBER = "value"`` line in the
        # emitted code. We can just lowercase the MEMBER name because that's
        # the scheme the generator uses.
        m.lower() for m in members
    }
    # Find bare string state literals that should be EnumClass.MEMBER references.
    # Covers three usage patterns:
    #   1. Comparisons:  `state == "idle"`  and  `state != "open"`
    #   2. Membership:   `state in ("borrowed", "monitoring")`
    #   3. Assignments:  `self._loop_state = "supplied"`
    # Any literal match whose value is a declared enum value is a regression —
    # except when the match IS the enum member definition line itself
    # (``IDLE = "idle"``), which we whitelist below.
    import re

    bare_literal = re.compile(r'"([a-z_]+)"')
    for match in bare_literal.finditer(code):
        value = match.group(1)
        if value not in state_values_in_enum:
            continue
        # Extract the full source line containing this literal.
        line_start = code.rfind("\n", 0, match.start()) + 1
        line_end = code.find("\n", match.end())
        if line_end == -1:
            line_end = len(code)
        line = code[line_start:line_end]
        # Whitelist: the enum member definition itself — ``IDLE = "idle"``.
        if re.match(r'^\s*[A-Z_]+\s*=\s*"[a-z_]+"\s*$', line):
            continue
        # Whitelist: docstring/comment lines (conservative — if the value
        # appears inside a docstring as documentation it's harmless).
        stripped = line.lstrip()
        if stripped.startswith(("#", '"""', "'''", "*", ">>>")):
            continue
        raise AssertionError(
            f"{template.value}: bare string state value {value!r} used on line:\n"
            f"    {line.strip()}\n"
            f"Use {enum_name}.{value.upper()} instead."
        )


# ---------------------------------------------------------------------------
# Persistence round-trip: StrEnum <-> raw JSON <-> StrEnum
# ---------------------------------------------------------------------------


def test_lending_loop_state_json_round_trips_through_raw_strings() -> None:
    """A StrEnum state persisted to JSON must load back as an equal StrEnum member.

    This is the load-bearing compatibility property: old persisted state files
    contain plain strings (``"idle"``), and new files contain StrEnum members.
    Both MUST load cleanly, and a round-trip must preserve equality.
    """
    import json
    from enum import StrEnum

    strat = _make_strategy()
    # Drive the state machine to a non-default value so we actually test
    # something other than IDLE.
    strat.on_intent_executed(_make_mock_intent("SUPPLY"), success=True, result=None)
    # Now _loop_state should be the StrEnum member LendingLoopState.SUPPLIED.
    assert isinstance(strat._loop_state, StrEnum), (
        f"Expected StrEnum instance, got {type(strat._loop_state)}"
    )
    assert strat._loop_state == "supplied"  # StrEnum compares equal to its value

    # Serialize through JSON (the real persistence path).
    saved_json = json.dumps(strat.get_persistent_state())
    # Round-trip through JSON -> dict -> load_persistent_state.
    loaded = json.loads(saved_json)
    assert loaded["loop_state"] == "supplied", (
        "Raw JSON must contain the plain string value, not a repr of the enum"
    )
    assert isinstance(loaded["loop_state"], str), "JSON load must yield a plain str"

    # Load into a fresh strategy instance.
    strat2 = _make_strategy()
    strat2.load_persistent_state(loaded)
    assert strat2._loop_state == strat._loop_state, "Round-trip must preserve state"
    assert strat2._loop_state == "supplied"
    # After load_persistent_state, the value must be coerced back to a StrEnum.
    assert isinstance(strat2._loop_state, StrEnum), (
        "load_persistent_state must coerce plain strings back to StrEnum members"
    )


def test_lending_loop_state_loads_legacy_plain_strings() -> None:
    """Backward compat: pre-StrEnum state files (plain strings) MUST still load.

    This simulates loading a state file that was written before this migration
    was in place. The file contains ``"supplied"`` as a plain string; after
    ``load_persistent_state`` the in-memory value must be a StrEnum member.
    """
    from enum import StrEnum

    strat = _make_strategy()
    # Legacy: plain-string state file with no StrEnum awareness.
    legacy_state = {"loop_state": "borrowed", "loop_count": 2, "current_leverage": "1.7"}
    strat.load_persistent_state(legacy_state)
    assert isinstance(strat._loop_state, StrEnum)
    assert strat._loop_state == "borrowed"


def test_basis_trade_state_json_round_trips() -> None:
    """BasisTradeState round-trips through JSON like LendingLoopState."""
    import json
    from enum import StrEnum

    strat = _make_basis_strategy()
    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    assert isinstance(strat._trade_state, StrEnum)
    assert strat._trade_state == "spot_bought"

    saved_json = json.dumps(strat.get_persistent_state())
    loaded = json.loads(saved_json)
    assert loaded["trade_state"] == "spot_bought"
    assert isinstance(loaded["trade_state"], str)

    strat2 = _make_basis_strategy()
    strat2.load_persistent_state(loaded)
    assert strat2._trade_state == "spot_bought"
    assert isinstance(strat2._trade_state, StrEnum)


def test_basis_trade_state_loads_legacy_plain_strings() -> None:
    """BasisTradeState backward compat for pre-StrEnum state files."""
    from enum import StrEnum

    strat = _make_basis_strategy()
    strat.load_persistent_state({"trade_state": "unwinding"})
    assert isinstance(strat._trade_state, StrEnum)
    assert strat._trade_state == "unwinding"


def test_lending_loop_scaffolded_init_uses_strenum() -> None:
    """The scaffolded ``__init__`` (as emitted source) must init state as a StrEnum.

    We verify this at the generator-source level rather than by instantiating
    the strategy because ``_make_strategy`` manually bypasses ``__init__``.
    The AST-level check is the canonical contract: the emitted code must
    assign ``self._loop_state = LendingLoopState.IDLE``, not a bare string.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Init Check",
            template=StrategyTemplate.LENDING_LOOP,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    assert "self._loop_state = LendingLoopState.IDLE" in code, (
        "Scaffolded __init__ must assign StrEnum member, not a bare string"
    )
    # And the bare-string form must NOT appear.
    assert 'self._loop_state = "idle"' not in code


def test_basis_trade_scaffolded_init_uses_strenum() -> None:
    """BasisTrade ``__init__`` must init ``_trade_state`` via the StrEnum."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Init Check",
            template=StrategyTemplate.BASIS_TRADE,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
    assert "self._trade_state = BasisTradeState.IDLE" in code
    assert 'self._trade_state = "idle"' not in code
