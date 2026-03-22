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
        class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        assert len(class_defs) >= 1, "No class definition found"

        strategy_class = class_defs[0]
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
        class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        strategy_class = class_defs[0]
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
        class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        strategy_class = class_defs[0]
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
