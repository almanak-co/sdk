import ast
import json
import subprocess
import tempfile
from pathlib import Path

import pytest

from almanak._version import __version__
from almanak.framework.cli.new_strategy import (
    StrategyTemplate,
    SupportedChain,
    generate_config_json,
    generate_dashboard_metadata,
    generate_dashboard_ui,
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
    assert strategy_classes, f"No class ending in 'Strategy' found (got: {[c.name for c in class_defs]})"
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
# Dashboard scaffold tests — every new strategy ships with a starter
# dashboard/ui.py that is wired to the standard trade-tape section.
# ---------------------------------------------------------------------------


def test_generate_dashboard_ui_is_valid_python() -> None:
    """Scaffolded ``dashboard/ui.py`` must parse without syntax errors."""
    ast.parse(generate_dashboard_ui("Mean Reversion"))


def test_generate_dashboard_ui_passes_ruff() -> None:
    """Scaffolded ``dashboard/ui.py`` must pass ruff (E/W/F/I)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ui_path = Path(tmpdir) / "ui.py"
        ui_path.write_text(generate_dashboard_ui("Mean Reversion"))

        result = subprocess.run(
            ["uv", "run", "ruff", "check", str(ui_path), "--select", "E,W,F,I"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, f"ruff check failed for scaffolded ui.py:\n{result.stdout}\n{result.stderr}"


def test_generate_dashboard_ui_includes_three_section_helpers() -> None:
    """The scaffold must pre-wire all three section helpers (VIB-3969)
    so accounting is visually QA'able from day one — PnL eyeball at the
    top, audit detail at the bottom — both the imports and the calls."""
    code = generate_dashboard_ui("Mean Reversion")

    # Import line — multi-line `from … import (…)` form so ruff stays happy.
    assert "render_pnl_section" in code
    assert "render_cost_stack_section" in code
    assert "render_trade_tape_section" in code

    # All three helpers actually invoked inside render_custom_dashboard.
    assert "render_pnl_section(strategy_id)" in code
    assert "render_cost_stack_section(strategy_id" in code  # may have heading kwarg
    assert "render_trade_tape_section(strategy_id)" in code


def test_generate_dashboard_ui_section_order_is_pnl_then_audit() -> None:
    """The 3-section layout depends on PnL appearing BEFORE the audit
    block. Authors place primitive-specific UI between them, but PnL
    must lead the page so the operator sees money status first."""
    code = generate_dashboard_ui("Mean Reversion")

    pnl_idx = code.index("render_pnl_section(strategy_id)")
    cost_idx = code.index("render_cost_stack_section(strategy_id")
    tape_idx = code.index("render_trade_tape_section(strategy_id)")
    assert pnl_idx < cost_idx < tape_idx, "section order must be PnL → Cost Stack → Trade Tape"


def test_generate_dashboard_ui_defines_render_custom_dashboard() -> None:
    """The scaffold must define the function the platform's dashboard
    image looks for."""
    tree = ast.parse(generate_dashboard_ui("Mean Reversion"))
    func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    assert "render_custom_dashboard" in func_names


def test_generate_dashboard_metadata_is_valid_json() -> None:
    """Scaffolded ``dashboard/metadata.json`` must parse and carry the
    three fields the discoverer reads. ``icon`` defaults to an empty
    string per CLAUDE.md "no emojis unless asked" — strategy authors
    set their own."""
    payload = json.loads(generate_dashboard_metadata("Mean Reversion"))

    assert payload["display_name"] == "Mean Reversion"
    assert payload["description"]
    assert "icon" in payload
    assert payload["icon"] == ""


def test_generate_dashboard_ui_is_valid_python_for_names_with_quotes() -> None:
    """Strategy names containing ``"`` must produce valid Python — the
    template embeds names via ``json.dumps`` to escape them safely."""
    code = generate_dashboard_ui('My"Strategy')
    ast.parse(code)  # raises if scaffold output is malformed


def test_generate_dashboard_ui_is_valid_python_for_names_with_backslash() -> None:
    """Backslash in name shouldn't generate an escape-sequence trap."""
    code = generate_dashboard_ui("Foo\\Bar")
    ast.parse(code)


# ---------------------------------------------------------------------------
# Template-renderer scaffolds — DYNAMIC_LP / LENDING_LOOP / PERPS / TA_SWAP
# scaffold a ``render_*_dashboard()`` wrapper instead of the direct-sections
# starter. The framework renderer owns the title, the strategy header, and
# the three audit sections, so the scaffold must NOT call ``st.title(...)``
# or the section helpers itself — that double-renders.
# ---------------------------------------------------------------------------


def _ast_call_names(code: str) -> set[str]:
    """Return ``{'st.title', 'render_pnl_section', ...}`` — actual call
    targets in the AST, ignoring strings inside docstrings/comments."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(code)):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Name):
            names.add(f.id)
        elif isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
            names.add(f"{f.value.id}.{f.attr}")
    return names


_TEMPLATE_DASHBOARDS: tuple[tuple[StrategyTemplate, str, tuple[str, ...]], ...] = (
    (
        StrategyTemplate.DYNAMIC_LP,
        "render_lp_dashboard",
        ("LPDashboardConfig", "prepare_lp_session_state"),
    ),
    (
        StrategyTemplate.LENDING_LOOP,
        "render_lending_dashboard",
        ("get_aave_v3_config",),
    ),
    (
        StrategyTemplate.PERPS,
        "render_perp_dashboard",
        ("get_gmx_v2_config",),
    ),
    (
        StrategyTemplate.TA_SWAP,
        "render_ta_dashboard",
        ("get_rsi_config",),
    ),
)


@pytest.mark.parametrize(
    "template,renderer,additional",
    _TEMPLATE_DASHBOARDS,
    ids=lambda v: v.value if hasattr(v, "value") else str(v),
)
def test_template_dashboard_scaffold_calls_renderer(
    template: StrategyTemplate,
    renderer: str,
    additional: tuple[str, ...],
) -> None:
    """The 4 primitive-specific templates must scaffold the matching
    framework renderer (``render_lp_dashboard`` / ``render_lending_dashboard``
    / ``render_perp_dashboard`` / ``render_ta_dashboard``)."""
    calls = _ast_call_names(generate_dashboard_ui("Mean Reversion", template))
    assert renderer in calls
    for symbol in additional:
        assert symbol in calls, f"{template} scaffold missing {symbol!r} call"


@pytest.mark.parametrize(
    "template,renderer,additional",
    _TEMPLATE_DASHBOARDS,
    ids=lambda v: v.value if hasattr(v, "value") else str(v),
)
def test_template_dashboard_scaffold_does_not_double_render(
    template: StrategyTemplate,
    renderer: str,
    additional: tuple[str, ...],
) -> None:
    """The framework renderer owns the title and the three audit
    sections. Scaffolds wired to a renderer must NOT call them again."""
    calls = _ast_call_names(generate_dashboard_ui("Mean Reversion", template))
    leaks = calls & {
        "st.title",
        "render_pnl_section",
        "render_cost_stack_section",
        "render_trade_tape_section",
    }
    assert not leaks, f"{template} scaffold leaks {leaks!r} alongside {renderer}"


def test_lp_scaffold_passes_api_client_to_render_lp_dashboard() -> None:
    """The LP scaffold MUST emit ``render_lp_dashboard(..., api_client=api_client)``.

    Regression guard for the silent-empty failure mode observed on hosted in
    May 2026: when the LP scaffold drops the ``api_client`` kwarg, the
    gateway-backed Positions registry + Position Lifecycle sections (added
    in PR #2373) render empty, with no error or warning. AlmanakCode and
    other LLM-generated strategies pattern-match the scaffold output, so
    losing this kwarg in the scaffold silently regresses every new LP
    strategy. The UAT card at
    ``docs/internal/uat-cards/dashboard-lp-registry-lifecycle-sections.md``
    §D2 specifies this check; this test makes it a CI gate.
    """
    import re

    code = generate_dashboard_ui("Mean Reversion", StrategyTemplate.DYNAMIC_LP)
    calls = re.findall(r"render_lp_dashboard\([^)]*\)", code, flags=re.DOTALL)
    assert calls, "LP scaffold emitted no render_lp_dashboard(...) call"
    for call in calls:
        assert "api_client=api_client" in call, (
            "LP scaffold must emit render_lp_dashboard(..., api_client=api_client); "
            f"got: {call!r}"
        )


@pytest.mark.parametrize(
    "template,_renderer,_additional",
    _TEMPLATE_DASHBOARDS,
    ids=lambda v: v.value if hasattr(v, "value") else str(v),
)
def test_template_dashboard_scaffold_passes_ruff(
    template: StrategyTemplate,
    _renderer: str,
    _additional: tuple[str, ...],
) -> None:
    """Generated template-renderer scaffolds must be ruff-clean (E/W/F/I)
    so authors get a starter file they can run unchanged."""
    code = generate_dashboard_ui("Mean Reversion", template)
    with tempfile.TemporaryDirectory() as tmpdir:
        ui_path = Path(tmpdir) / "ui.py"
        ui_path.write_text(code)

        result = subprocess.run(
            ["uv", "run", "ruff", "check", str(ui_path), "--select", "E,W,F,I"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (
            f"ruff failed for {template.value} scaffold:\n{result.stdout}\n{result.stderr}"
        )


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
        method_names = [n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)]
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
        method_names = [n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)]
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
        method_names = [n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)]
        assert "on_intent_executed" in method_names, f"on_intent_executed() missing from {template.value} template"


# ---------------------------------------------------------------------------
# LENDING_LOOP: state machine transitions and persistence
# ---------------------------------------------------------------------------


def _make_mock_intent(intent_type_value: str, **attrs):
    """Create a minimal mock intent with intent_type.value == intent_type_value.

    Extra keyword attrs (e.g. ``repay_full=True``) are attached directly to
    the mock instance so intent-type-specific handlers can read them.
    """

    class _IntentType:
        value = intent_type_value

    class _MockIntent:
        intent_type = _IntentType()

    m = _MockIntent()
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


def _scaffold_lending_loop(preserve_decide: bool = False):
    """Scaffold a LENDING_LOOP strategy and return a concrete subclass for testing.

    Args:
        preserve_decide: If True, keep the scaffolded ``decide()`` (needed when
            testing the HF-guard / state-machine logic in ``decide``). Default
            False overrides ``decide`` to ``return None`` for callback tests.
    """
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

    base_cls = next(v for v in ns.values() if isinstance(v, type) and issubclass(v, _Base) and v is not _Base)

    if preserve_decide:

        class _Concrete(base_cls):
            def get_open_positions(self):
                return None

            def generate_teardown_intents(self, mode=None, market=None):
                return []

        return _Concrete

    # Default path: stub out decide() for callback-only tests.
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
                "emergency_threshold": "1.2",
                "min_collateral_usd": "100",
                "partial_repay_pct": "0.25",
                "lending_protocol": "aave_v3",
                "lending_market": "",
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
    strat.emergency_threshold = Decimal("1.2")
    strat.min_collateral_usd = Decimal("100")
    strat.partial_repay_pct = Decimal("0.25")
    strat.lending_protocol = "aave_v3"
    strat.lending_market = ""
    strat.collateral_token = "WETH"
    strat.borrow_token = "USDC"
    strat._loop_state = "idle"
    strat._loop_count = 0
    strat._current_leverage = Decimal("1.0")
    strat._total_borrowed = Decimal("0")
    strat._total_collateral = Decimal("0")
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


def test_lending_loop_repay_full_resets_leverage() -> None:
    """Successful repay_full -> leverage resets to 1.0 and state=MONITORING."""
    from decimal import Decimal

    strat = _make_strategy()
    strat._loop_state = "monitoring"
    strat._loop_count = 3
    strat._current_leverage = Decimal("2.5")

    strat.on_intent_executed(_make_mock_intent("REPAY", repay_full=True), success=True, result=None)
    assert strat._current_leverage == Decimal("1.0")
    assert strat._loop_state == "monitoring"


def test_lending_loop_partial_repay_shrinks_leverage() -> None:
    """Partial repay decrements leverage proportionally but never below 1.0."""
    from decimal import Decimal

    strat = _make_strategy()
    strat._loop_state = "monitoring"
    strat._current_leverage = Decimal("2.0")
    strat.partial_repay_pct = Decimal("0.25")

    strat.on_intent_executed(_make_mock_intent("REPAY", repay_full=False), success=True, result=None)
    # 2.0 * (1 - 0.25) = 1.5
    assert strat._current_leverage == Decimal("1.5")

    # Idempotent floor at 1.0.
    strat._current_leverage = Decimal("1.0")
    strat.on_intent_executed(_make_mock_intent("REPAY", repay_full=False), success=True, result=None)
    assert strat._current_leverage == Decimal("1.0")


def test_lending_loop_monitoring_swap_does_not_advance_loop() -> None:
    """An unwind SWAP during MONITORING (preceding a repay) must NOT increment loop_count."""
    strat = _make_strategy()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    original_leverage = strat._current_leverage

    strat.on_intent_executed(_make_mock_intent("SWAP"), success=True, result=None)
    # loop_count unchanged (otherwise unwind flow double-counts loops).
    assert strat._loop_count == 2
    assert strat._loop_state == "monitoring"
    assert strat._current_leverage == original_leverage


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
# LENDING_LOOP: unified health-factor guard (aave_v3 / morpho_blue / compound_v3)
# ---------------------------------------------------------------------------


class _HFMarket:
    """Market mock that returns a configurable health factor."""

    def __init__(self, hf, borrow_balance="500", debt_usd="400"):
        from decimal import Decimal

        self._hf = Decimal(str(hf))
        self._borrow_balance = Decimal(str(borrow_balance))
        self._debt_usd = Decimal(str(debt_usd))

    def position_health(self, protocol, market_id):
        debt_usd = self._debt_usd

        class _Health:
            def __init__(self, hf):
                self.health_factor = hf
                self.debt_value_usd = debt_usd

        return _Health(self._hf)

    def balance(self, token):
        from decimal import Decimal

        if token == "USDC":
            bal_amt = self._borrow_balance
            price_usd = Decimal("1")
        else:
            # Default WETH balance: 2 tokens @ $2000 = $4000 (above min_collateral_usd)
            bal_amt = Decimal("2")
            price_usd = Decimal("2000")

        bal = bal_amt
        bal_usd_val = bal_amt * price_usd

        class _B:
            balance = bal
            balance_usd = bal_usd_val

        return _B()

    def price(self, token):
        from decimal import Decimal

        return Decimal("2000") if token == "WETH" else Decimal("1")


def _make_strategy_live_decide():
    """Like _make_strategy but preserves the scaffolded decide() (for HF-guard tests)."""
    cls = _scaffold_lending_loop(preserve_decide=True)

    class _Cfg:
        def get(self, key, default=None):
            return default

    from decimal import Decimal

    strat = cls.__new__(cls)
    strat.config = _Cfg()
    strat.supply_amount = Decimal("1")
    strat.borrow_amount = Decimal("500")
    strat.target_leverage = Decimal("2.0")
    strat.borrow_ratio = Decimal("0.7")
    strat.min_health_factor = Decimal("1.5")
    strat.emergency_threshold = Decimal("1.2")
    strat.min_collateral_usd = Decimal("100")
    strat.partial_repay_pct = Decimal("0.25")
    strat.lending_protocol = "aave_v3"
    strat.lending_market = ""
    strat.collateral_token = "WETH"
    strat.borrow_token = "USDC"
    strat._loop_state = "idle"
    strat._loop_count = 0
    strat._current_leverage = Decimal("1.0")
    strat._total_borrowed = Decimal("0")
    strat._total_collateral = Decimal("0")
    return strat


def test_lending_loop_emits_partial_repay_when_hf_below_min() -> None:
    """HF dropped below min_health_factor but above emergency -> partial repay."""
    from decimal import Decimal

    strat = _make_strategy_live_decide()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    # HF = 1.35 is below min_health_factor (1.5) but above emergency_threshold (1.2)
    market = _HFMarket(hf="1.35", borrow_balance="400")

    intent = strat.decide(market)
    assert intent is not None
    # Must be a RepayIntent (not repay_full)
    assert intent.intent_type.value == "REPAY"
    assert getattr(intent, "repay_full", False) is False
    # Partial repay = 400 * 0.25 = 100 (quantized down to 0.01)
    assert intent.amount == Decimal("100.00")
    assert intent.token == "USDC"
    assert intent.protocol == "aave_v3"


def test_lending_loop_emits_full_deleverage_when_hf_below_emergency() -> None:
    """HF dropped below emergency_threshold -> full deleverage via repay_full."""
    strat = _make_strategy_live_decide()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    market = _HFMarket(hf="1.10", borrow_balance="400")  # below emergency 1.2

    intent = strat.decide(market)
    assert intent is not None
    assert intent.intent_type.value == "REPAY"
    assert intent.repay_full is True
    assert intent.token == "USDC"
    # State advances to monitoring so subsequent iterations won't continue looping
    assert strat._loop_state == "monitoring"


def test_lending_loop_no_repay_when_hf_healthy() -> None:
    """HF safely above min_health_factor -> continue loop / monitoring (no repay)."""
    strat = _make_strategy_live_decide()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    market = _HFMarket(hf="2.0", borrow_balance="400")

    intent = strat.decide(market)
    assert intent is not None
    # Not a repay -- HOLD (monitoring) when healthy.
    assert intent.intent_type.value == "HOLD"


def test_lending_loop_hf_provider_failure_continues_loop() -> None:
    """If HF call raises, strategy does not abort -- continues the loop defensively."""
    from decimal import Decimal

    class _BrokenMarket(_HFMarket):
        def position_health(self, protocol, market_id):
            raise RuntimeError("gateway unavailable")

        def balance(self, token):
            from decimal import Decimal

            class _B:
                balance = Decimal("2")
                balance_usd = Decimal("4000")

            return _B()

    strat = _make_strategy_live_decide()
    strat._loop_state = "idle"
    strat._loop_count = 1  # so HF-guard branch executes
    market = _BrokenMarket(hf="0")

    intent = strat.decide(market)
    # Should not raise -- degrades to the normal state machine.
    assert intent is not None
    # After loop_count>0 in 'idle' with healthy collateral, a SUPPLY intent is returned.
    assert intent.intent_type.value == "SUPPLY"
    # Leverage state unchanged
    assert strat._current_leverage == Decimal("1.0")


def test_lending_loop_hf_guard_dormant_when_no_borrows() -> None:
    """Before any borrow has occurred, HF guard is a no-op (no position to monitor)."""
    strat = _make_strategy_live_decide()
    strat._loop_state = "idle"
    strat._loop_count = 0
    # HF would trigger emergency deleverage if the guard fired.
    market = _HFMarket(hf="0.5", borrow_balance="0")

    intent = strat.decide(market)
    # Guard is dormant (loop_count==0 and state==idle) -- normal idle path runs.
    assert intent is not None
    assert intent.intent_type.value in {"SUPPLY", "HOLD"}


def test_lending_loop_hf_uses_configured_protocol() -> None:
    """HF guard passes self.lending_protocol / self.lending_market to market.position_health."""
    recorded = {}

    class _RecordingMarket(_HFMarket):
        def position_health(self_inner, protocol, market_id):
            recorded["protocol"] = protocol
            recorded["market_id"] = market_id
            return super().position_health(protocol, market_id)

    strat = _make_strategy_live_decide()
    strat.lending_protocol = "morpho_blue"
    strat.lending_market = "0xdeadbeef"
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    market = _RecordingMarket(hf="2.0")

    strat.decide(market)
    assert recorded["protocol"] == "morpho_blue"
    assert recorded["market_id"] == "0xdeadbeef"


def test_lending_loop_supply_uses_configured_protocol() -> None:
    """Supply intent must use self.lending_protocol, not hardcoded aave_v3.

    Morpho Blue requires market_id for isolated markets, so we also verify
    self.lending_market is threaded through.
    """
    strat = _make_strategy_live_decide()
    strat.lending_protocol = "morpho_blue"
    strat.lending_market = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
    strat._loop_state = "idle"
    strat._loop_count = 0
    market = _HFMarket(hf="2.0", borrow_balance="0")

    intent = strat.decide(market)
    assert intent is not None
    assert intent.intent_type.value == "SUPPLY"
    assert intent.protocol == "morpho_blue"  # not hardcoded aave_v3
    assert intent.market_id == strat.lending_market


def test_lending_loop_borrow_uses_configured_protocol() -> None:
    """Borrow intent must use self.lending_protocol, not hardcoded aave_v3."""
    strat = _make_strategy_live_decide()
    strat.lending_protocol = "compound_v3"
    strat.lending_market = "usdc"
    strat._loop_state = "supplied"
    strat._loop_count = 0
    market = _HFMarket(hf="2.0", borrow_balance="0")

    intent = strat.decide(market)
    assert intent is not None
    assert intent.intent_type.value == "BORROW"
    assert intent.protocol == "compound_v3"


def test_lending_loop_partial_repay_sized_from_debt_not_wallet() -> None:
    """Partial repay must size from ON-CHAIN DEBT, not wallet balance.

    After loops, wallet has 0 borrow_token (already swapped to collateral)
    but debt is $400. partial_repay_pct=0.25 -> target=100 USDC. Since
    wallet<target, strategy must swap collateral -> debt first.
    """
    from decimal import Decimal

    strat = _make_strategy_live_decide()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    # HF=1.35 triggers partial repay. Debt=$400 USDC. Wallet has 0 USDC.
    market = _HFMarket(hf="1.35", borrow_balance="0", debt_usd="400")

    intent = strat.decide(market)
    assert intent is not None
    # Since wallet<target (0<100), strategy must first SWAP collateral -> debt.
    assert intent.intent_type.value == "SWAP"
    assert intent.from_token == "WETH"
    assert intent.to_token == "USDC"
    # Unchanged debt amount stays
    assert Decimal(str(strat.partial_repay_pct)) == Decimal("0.25")


def test_lending_loop_emergency_swaps_collateral_when_wallet_empty() -> None:
    """When HF < emergency and wallet has no debt_token, strategy MUST first swap
    collateral -> debt token instead of emitting a repay_full that has no funds.
    """
    strat = _make_strategy_live_decide()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    market = _HFMarket(hf="1.10", borrow_balance="0", debt_usd="400")  # wallet empty

    intent = strat.decide(market)
    assert intent is not None
    # Must swap collateral -> debt first, not emit a dud repay_full.
    assert intent.intent_type.value == "SWAP"
    assert intent.from_token == "WETH"
    assert intent.to_token == "USDC"
    # State has advanced so the next iteration will emit repay_full.
    assert strat._loop_state == "monitoring"


def test_lending_loop_emergency_repay_full_when_wallet_funded() -> None:
    """When HF < emergency AND wallet has debt_token, strategy emits repay_full directly."""
    strat = _make_strategy_live_decide()
    strat._loop_state = "monitoring"
    strat._loop_count = 2
    market = _HFMarket(hf="1.10", borrow_balance="500", debt_usd="400")

    intent = strat.decide(market)
    assert intent is not None
    assert intent.intent_type.value == "REPAY"
    assert intent.repay_full is True


def test_lending_loop_emergency_from_borrowed_swaps_first_and_flags_monitoring() -> None:
    """HF guard triggered in BORROWED state:
    wallet empty -> strategy must swap collateral first AND flip to MONITORING
    so on_intent_executed does not mis-count the unwind as a loop iteration.
    """
    strat = _make_strategy_live_decide()
    strat._loop_state = "borrowed"
    strat._loop_count = 1
    market = _HFMarket(hf="1.10", borrow_balance="0", debt_usd="400")

    intent = strat.decide(market)
    assert intent is not None
    assert intent.intent_type.value == "SWAP"
    assert intent.from_token == "WETH"
    assert intent.to_token == "USDC"
    # State MUST flip to MONITORING to avoid double-counting in on_intent_executed.
    assert strat._loop_state == "monitoring"


def test_lending_loop_partial_repay_from_borrowed_transitions_to_monitoring() -> None:
    """Partial-repay deleverage (HF<min but HF>=emergency) from BORROWED must also
    transition to MONITORING so the strategy does not resume looping.
    """
    from decimal import Decimal

    strat = _make_strategy_live_decide()
    strat._loop_state = "borrowed"
    strat._loop_count = 1
    # HF=1.35 < min(1.5) but > emergency(1.2); wallet holds enough USDC for partial.
    market = _HFMarket(hf="1.35", borrow_balance="500", debt_usd="400")

    intent = strat.decide(market)
    assert intent is not None
    assert intent.intent_type.value == "REPAY"
    assert getattr(intent, "repay_full", False) is False
    assert intent.amount == Decimal("100.00")
    # State MUST flip to MONITORING to stop further loops.
    assert strat._loop_state == "monitoring"


def test_lending_loop_holds_when_non_stable_debt_has_no_oracle() -> None:
    """Non-stable debt with no oracle and no stablecoin fallback -> HOLD instead
    of silently over-repaying.
    """

    class _NoPriceMarket(_HFMarket):
        def price(self, token):
            raise RuntimeError("oracle unavailable")

    strat = _make_strategy_live_decide()
    strat.borrow_token = "ARB"  # not in STABLE_DEBT_TOKENS
    strat._loop_state = "monitoring"
    strat._loop_count = 1
    market = _NoPriceMarket(hf="1.35", borrow_balance="10", debt_usd="400")

    intent = strat.decide(market)
    assert intent is not None
    # Partial repay branch must HOLD (debt_tokens is None) rather than size a repay.
    assert intent.intent_type.value == "HOLD"


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

    base_cls = next(v for v in ns.values() if isinstance(v, type) and issubclass(v, _Base) and v is not _Base)

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
# PERPS: direction (LONG/SHORT) is config-driven
# ---------------------------------------------------------------------------


def _scaffold_perps_code() -> str:
    """Return the emitted strategy.py source for the PERPS template."""
    with tempfile.TemporaryDirectory() as tmpdir:
        return generate_strategy_file(
            name="Test Perps",
            template=StrategyTemplate.PERPS,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )


def _scaffold_perps_class():
    """Scaffold a PERPS strategy and return a concrete subclass for instantiation."""
    code = _scaffold_perps_code()
    ns: dict = {}
    exec(compile(code, "<scaffold>", "exec"), ns)  # noqa: S102
    from almanak.framework.strategies import IntentStrategy as _Base

    base_cls = next(v for v in ns.values() if isinstance(v, type) and issubclass(v, _Base) and v is not _Base)

    class _Concrete(base_cls):
        def decide(self, market):
            return None

        def get_open_positions(self):
            return None

        def generate_teardown_intents(self, mode=None, market=None):
            return []

    return _Concrete


def _make_perps_strategy(direction: str | None = "LONG"):
    """Instantiate a perps-like object by running the emitted init block.

    We do not call IntentStrategy.__init__ (requires full strategy context);
    instead we mirror the template's init-params code path against a stub
    config so we can assert the direction -> _is_long wiring the scaffold
    emits is correct. If ``direction`` is None, the config omits the key.
    """
    from decimal import Decimal

    cls = _scaffold_perps_class()

    defaults: dict = {
        "perp_market": "ETH/USD",
        "collateral_token": "USDC",
        "collateral_amount": "100",
        "position_size_usd": "1000",
        "leverage": "5",
        "take_profit_pct": "0.05",
        "stop_loss_pct": "0.03",
        "base_token": "ETH",
    }
    if direction is not None:
        defaults["direction"] = direction

    class _Cfg:
        def get(self, key, default=None):
            return defaults.get(key, default)

    strat = cls.__new__(cls)
    strat.config = _Cfg()

    # Mirror the emitted init-block (kept in lockstep with
    # _get_template_init_params(PERPS)) so drift surfaces loudly.
    def get_config(key, default):
        return defaults.get(key, default)

    strat.perp_market = get_config("perp_market", "ETH/USD")
    strat.collateral_token = get_config("collateral_token", "USDC")
    strat.collateral_amount = Decimal(str(get_config("collateral_amount", "100")))
    strat.position_size_usd = Decimal(str(get_config("position_size_usd", "1000")))
    strat.leverage = Decimal(str(get_config("leverage", "5")))
    strat.take_profit_pct = Decimal(str(get_config("take_profit_pct", "0.05")))
    strat.stop_loss_pct = Decimal(str(get_config("stop_loss_pct", "0.03")))
    strat.base_token = get_config("base_token", "ETH")

    _direction_raw = get_config("direction", None)
    if _direction_raw is None:
        _direction_raw = "LONG"
    strat.direction = str(_direction_raw).upper()
    if strat.direction not in ("LONG", "SHORT"):
        raise ValueError(f"Invalid direction {_direction_raw!r}: must be 'LONG' or 'SHORT'")
    strat._is_long = strat.direction == "LONG"

    # Match emitted init exactly: PerpsState is defined at module level in the
    # scaffolded strategy, so we reach into the class's module to get it.
    perps_state_enum = _scaffold_perps_perps_state_enum()
    strat._position_state = perps_state_enum.IDLE
    strat._entry_price = None
    # Direction pinning: set on PERP_OPEN, None otherwise (see callbacks)
    strat._position_is_long = None
    strat._position_direction = None
    return strat


def _scaffold_perps_perps_state_enum():
    """Return the PerpsState StrEnum from a freshly scaffolded perps strategy."""
    code = _scaffold_perps_code()
    ns: dict = {}
    exec(compile(code, "<scaffold>", "exec"), ns)  # noqa: S102
    return ns["PerpsState"]


def test_perps_scaffold_config_default_is_long() -> None:
    """config.json emitted by the PERPS template must default direction='LONG'."""
    import json

    config_str = generate_config_json("Test Perps", StrategyTemplate.PERPS, SupportedChain.ARBITRUM)
    config = json.loads(config_str)
    assert config.get("direction") == "LONG", "PERPS config.json must include direction='LONG' by default"


def test_perps_scaffold_reads_direction_from_config() -> None:
    """Emitted strategy.py reads the direction config field (not hardcoded)."""
    code = _scaffold_perps_code()
    # Must read direction from config
    assert 'get_config("direction"' in code, "PERPS must read 'direction' from config"
    # Must NOT contain hardcoded is_long=True anywhere in the emitted file
    assert "is_long=True" not in code, "PERPS scaffold must not hardcode is_long=True"
    # Must thread is_long=self._is_long instead
    assert "is_long=self._is_long" in code, "PERPS scaffold must wire is_long from self._is_long"


def test_perps_scaffold_long_config_sets_is_long_true() -> None:
    """Scaffolded strategy with direction='LONG' -> self._is_long == True."""
    strat = _make_perps_strategy(direction="LONG")
    assert strat.direction == "LONG"
    assert strat._is_long is True


def test_perps_scaffold_short_config_sets_is_long_false() -> None:
    """Scaffolded strategy with direction='SHORT' -> self._is_long == False."""
    strat = _make_perps_strategy(direction="SHORT")
    assert strat.direction == "SHORT"
    assert strat._is_long is False


def test_perps_scaffold_direction_is_case_insensitive() -> None:
    """Lowercase 'short' or 'long' should be normalized."""
    strat = _make_perps_strategy(direction="short")
    assert strat.direction == "SHORT"
    assert strat._is_long is False


def test_perps_scaffold_invalid_direction_raises() -> None:
    """Invalid direction values must raise ValueError."""
    with pytest.raises(ValueError, match="direction"):
        _make_perps_strategy(direction="sideways")


def test_perps_scaffold_missing_direction_defaults_to_long() -> None:
    """Omitting direction falls back to LONG (emitted __init__ also warns)."""
    strat = _make_perps_strategy(direction=None)
    assert strat.direction == "LONG"
    assert strat._is_long is True


def test_perps_scaffold_emits_warning_when_direction_omitted() -> None:
    """The emitted __init__ must logger.warning when direction is absent.

    Covers: 'Default to LONG if config omits it, but log a one-time warning
    on __init__ suggesting the user set it explicitly.'
    """
    from almanak.framework.cli.new_strategy import (
        TEMPLATE_CONFIGS,
        _get_template_init_params,
    )

    init_code = _get_template_init_params(StrategyTemplate.PERPS, TEMPLATE_CONFIGS[StrategyTemplate.PERPS])
    # The emitted init must contain a warning path for missing direction
    assert "logger.warning" in init_code
    assert "direction" in init_code
    assert "'LONG'" in init_code and "'SHORT'" in init_code


def test_perps_scaffold_teardown_uses_direction() -> None:
    """Teardown emits is_long=self._is_long, not hardcoded True."""
    code = _scaffold_perps_code()
    # perp_close in teardown must be direction-driven
    assert "is_long=self._is_long" in code
    # position_id must be direction-aware (not hardcoded _perp_long)
    assert '_perp_long"' not in code, "Teardown position_id must not hardcode '_perp_long' suffix"


def test_perps_scaffold_callbacks_persist_direction() -> None:
    """on_intent_executed / persistent state must pin the open position's direction.

    Guards against the footgun flagged by Gemini: if the user changes the
    config.json 'direction' while a position is open and restarts the strategy,
    the restored state must reflect the actually-opened side, not the newly
    configured one. Otherwise PnL math and teardown close the wrong side.
    """
    from almanak.framework.cli.new_strategy import _get_template_callbacks

    cb = _get_template_callbacks(StrategyTemplate.PERPS)
    # On PERP_OPEN we must pin is_long/direction to the live position
    assert "self._position_is_long = self._is_long" in cb, (
        "PERPS on_intent_executed must pin self._position_is_long on PERP_OPEN"
    )
    assert "self._position_direction = self.direction" in cb, (
        "PERPS on_intent_executed must pin self._position_direction on PERP_OPEN"
    )
    # get_persistent_state must include the persisted direction
    assert '"position_is_long": self._position_is_long' in cb, "get_persistent_state must persist position_is_long"
    assert '"position_direction": self._position_direction' in cb, (
        "get_persistent_state must persist position_direction"
    )
    # load_persistent_state must restore persisted direction for open positions
    assert 'state.get("position_is_long")' in cb, "load_persistent_state must read persisted position_is_long"
    # Persisted direction must override self._is_long when a position is open
    assert "self._is_long = persisted_is_long" in cb, (
        "load_persistent_state must override config direction with persisted one"
    )


def test_perps_scaffold_init_initializes_position_direction_attrs() -> None:
    """__init__ must initialize position_is_long / position_direction to None.

    These attributes are set on PERP_OPEN and cleared on PERP_CLOSE. They must
    exist at __init__ time so load_persistent_state / get_persistent_state
    have a clean slate when no position is open.
    """
    from almanak.framework.cli.new_strategy import (
        TEMPLATE_CONFIGS,
        _get_template_init_params,
    )

    init_code = _get_template_init_params(StrategyTemplate.PERPS, TEMPLATE_CONFIGS[StrategyTemplate.PERPS])
    assert "self._position_is_long = None" in init_code, "PERPS __init__ must initialize self._position_is_long = None"
    assert "self._position_direction = None" in init_code, (
        "PERPS __init__ must initialize self._position_direction = None"
    )


def test_perps_persisted_direction_overrides_config_mismatch() -> None:
    """Simulate the config-drift scenario end-to-end.

    Scenario: user opens a LONG position, changes config.json to SHORT,
    restarts. After load_persistent_state, _is_long must be True (the
    live position's side), not False (the new config).
    """
    from almanak.framework.cli.new_strategy import _get_template_callbacks

    cb_code = _get_template_callbacks(StrategyTemplate.PERPS)
    perps_state = _scaffold_perps_perps_state_enum()

    # Instantiate a SHORT-configured strategy (the new config after restart)
    strat = _make_perps_strategy(direction="SHORT")
    assert strat._is_long is False

    # Extract and exec the callback text into a namespace with PerpsState,
    # logger, and Decimal available (mirroring the emitted module scope).
    ns: dict = {}
    import logging
    import textwrap
    from decimal import Decimal as _D

    ns["logger"] = logging.getLogger("test")
    ns["Decimal"] = _D
    ns["PerpsState"] = perps_state

    fn_src = textwrap.dedent(cb_code)
    exec(compile(fn_src, "<perps_cb>", "exec"), ns)  # noqa: S102
    load = ns["load_persistent_state"]

    # Simulate restored state where the persisted position was LONG.
    # ``PerpsState(raw_state)`` coerces the persisted string to the enum, so
    # the emitted load method accepts plain strings as stored in JSON.
    persisted_state = {
        "position_state": "open",
        "entry_price": "2000.00",
        "position_is_long": True,
        "position_direction": "LONG",
    }

    load(strat, persisted_state)

    # After load, persisted direction must win
    assert strat._position_state == perps_state.OPEN
    assert strat._position_is_long is True, "Persisted position_is_long must override config-derived value"
    assert strat._position_direction == "LONG"
    assert strat._is_long is True, "self._is_long must be overridden to match the live position"
    assert strat.direction == "LONG", "self.direction must be overridden to match the live position"


def test_perps_idle_state_uses_config_direction() -> None:
    """When position_state is idle, load_persistent_state must NOT override config.

    No live position -> config is the source of truth for the next open.
    """
    from almanak.framework.cli.new_strategy import _get_template_callbacks

    cb_code = _get_template_callbacks(StrategyTemplate.PERPS)
    perps_state = _scaffold_perps_perps_state_enum()

    # Start with SHORT config; no live position.
    strat = _make_perps_strategy(direction="SHORT")

    ns: dict = {}
    import logging
    import textwrap
    from decimal import Decimal as _D

    ns["logger"] = logging.getLogger("test")
    ns["Decimal"] = _D
    ns["PerpsState"] = perps_state
    exec(compile(textwrap.dedent(cb_code), "<perps_cb>", "exec"), ns)  # noqa: S102
    load = ns["load_persistent_state"]

    load(strat, {"position_state": "idle", "entry_price": None})

    assert strat._position_state == perps_state.IDLE
    assert strat._is_long is False, "Idle + SHORT config -> _is_long stays False"
    assert strat.direction == "SHORT"
    assert strat._position_is_long is None
    assert strat._position_direction is None


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
            "Test LP",
            StrategyTemplate.DYNAMIC_LP,
            SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
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


def test_new_strategy_falls_back_to_cwd_when_ci_set(tmp_path: Path) -> None:
    """incubating/ exists + CI=1 → cwd/<name>, NOT strategies/incubating/<name>.

    Auto-detection of the SDK-root layout is suppressed in CI so a
    scaffold inside an SDK checkout (where ``strategies/incubating/``
    happens to exist) doesn't drop the new strategy into a directory the
    operator wasn't expecting (PR #2152 review)."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    incubating_dir = tmp_path / "strategies" / "incubating"
    incubating_dir.mkdir(parents=True)

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        result = runner.invoke(
            new_strategy,
            ["--name", "my_ci_strat", "--chain", "arbitrum"],
            env={"CI": "1"},
        )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "my_ci_strat").exists(), (
        "Expected strategy in cwd when CI=1 even though strategies/incubating/ exists"
    )
    assert not (incubating_dir / "my_ci_strat").exists(), "Strategy must NOT land in strategies/incubating/ when CI=1"


def test_new_strategy_degrades_when_runtime_config_invalid(tmp_path: Path) -> None:
    """Malformed unrelated env (e.g. ``ANVIL_*_PORT=abc``) must not abort scaffolding.

    ``cli_runtime_config_from_env()`` is consulted only to read ``is_ci``
    for the output-directory hint. A malformed runtime knob would
    otherwise raise ``ValueError`` and strand the scaffold before any
    file is written. On config-load failure we conservatively pick the
    cwd default — landing in cwd and asking the operator to ``mv`` is a
    smaller surprise than landing in ``strategies/incubating/`` while
    the user's env was broken (PR #2152 review)."""
    import os

    from click.testing import CliRunner

    from almanak.framework.cli.new_strategy import new_strategy

    incubating_dir = tmp_path / "strategies" / "incubating"
    incubating_dir.mkdir(parents=True)

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        result = runner.invoke(
            new_strategy,
            ["--name", "my_resilient_strat", "--chain", "arbitrum"],
            env={"CI": "", "ANVIL_ARBITRUM_PORT": "not-a-port"},
        )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "my_resilient_strat").exists(), (
        "Scaffolding should survive malformed unrelated env vars and "
        "fall back to cwd rather than auto-routing into strategies/incubating/"
    )
    assert not (incubating_dir / "my_resilient_strat").exists(), (
        "Strategy must NOT land in strategies/incubating/ when typed config refuses to load"
    )


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
            [
                "--name",
                "my_explicit_strat",
                "--chain",
                "arbitrum",
                "--output-dir",
                str(explicit_dir / "my_explicit_strat"),
            ],
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
    # Grep: StrEnum must be imported (alone or alongside Enum). VIB-3207 v2
    # emits ``_safe`` which needs ``Enum``, so stateful templates now merge
    # the two into ``from enum import Enum, StrEnum`` rather than running
    # two separate ``from enum`` lines (which would trigger ruff I001).
    assert "from enum import Enum, StrEnum" in code or "from enum import StrEnum" in code, (
        f"{template.value} must emit a StrEnum import"
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
    # Stateless templates still import ``Enum`` (used by ``_safe``) but must
    # not drag in ``StrEnum`` as an imported symbol (which would be unused
    # and fail F401). ``StrEnum`` may still appear in the base get_status
    # docstring — so we check the import line specifically.
    assert "from enum import Enum, StrEnum" not in code, (
        f"{template.value} must not combine-import StrEnum when no state machine is emitted"
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
        m.lower()
        for m in members
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
    assert isinstance(strat._loop_state, StrEnum), f"Expected StrEnum instance, got {type(strat._loop_state)}"
    assert strat._loop_state == "supplied"  # StrEnum compares equal to its value

    # Serialize through JSON (the real persistence path).
    saved_json = json.dumps(strat.get_persistent_state())
    # Round-trip through JSON -> dict -> load_persistent_state.
    loaded = json.loads(saved_json)
    assert loaded["loop_state"] == "supplied", "Raw JSON must contain the plain string value, not a repr of the enum"
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


# ---------------------------------------------------------------------------
# VIB-3207: enriched get_status() per template
# ---------------------------------------------------------------------------
#
# Every template's generated ``get_status()`` must:
#   1. Return a dict.
#   2. Always include the canonical ``{strategy, chain, wallet}`` trio.
#   3. Include template-specific fields when stateful.
#   4. Emit JSON-safe values (no raw Decimal / datetime / StrEnum objects
#      should survive into the returned dict — they must be str / isoformat
#      / .value respectively).
#   5. Never call into the gateway (covered implicitly by the fact that
#      the tests wire ``gateway_client`` to None on the strategy instance).
# ---------------------------------------------------------------------------


def _scaffold_and_get_class(template: StrategyTemplate, chain: SupportedChain = SupportedChain.ARBITRUM):
    """Scaffold a template and return a concrete subclass suitable for `get_status()` tests.

    Stubs out ``decide()`` / ``get_open_positions()`` / ``generate_teardown_intents()``
    so the class can be instantiated without wiring up a full strategy stack.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="StatusCheck",
            template=template,
            chain=chain,
            output_dir=Path(tmpdir),
        )
    ns: dict = {}
    exec(compile(code, "<scaffold>", "exec"), ns)  # noqa: S102
    from almanak.framework.strategies import IntentStrategy as _Base

    base_cls = next(v for v in ns.values() if isinstance(v, type) and issubclass(v, _Base) and v is not _Base)

    class _Concrete(base_cls):
        def decide(self, market):
            return None

        def get_open_positions(self):
            return None

        def generate_teardown_intents(self, mode=None, market=None):
            return []

    return _Concrete, ns


def _bare_instance(cls):
    """Create an instance without invoking ``IntentStrategy.__init__``.

    ``get_status()`` is a pure accessor; we only need the instance
    attributes it reads. Bypassing ``__init__`` avoids pulling in the
    full gateway/state scaffolding.

    Note: ``chain`` and ``wallet_address`` are ``@property`` on the base
    IntentStrategy, so we assign the underlying ``_chain`` / ``_wallet_address``
    slots directly.
    """
    inst = cls.__new__(cls)
    inst._chain = "arbitrum"
    inst._wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    inst.config = {}
    return inst


def _assert_base_status_shape(status: dict, *, expected_chain: str = "arbitrum") -> None:
    """Every template's get_status() must include the canonical trio."""
    assert isinstance(status, dict)
    assert status["strategy"] == "status_check"
    assert status["chain"] == expected_chain
    # wallet is truncated with "...".
    assert status["wallet"] is not None
    assert status["wallet"].startswith("0x") and status["wallet"].endswith("...")


def _assert_json_serializable(status: dict) -> None:
    """The full status dict must round-trip through json WITHOUT a custom encoder.

    CodeRabbit audit fix: the prior version passed ``default=lambda o: None``,
    which silently replaced any non-JSON-safe value (Decimal, datetime, Enum)
    with ``null`` and let the test pass. That defeated the test's purpose —
    the generated ``get_status()`` could emit raw Decimals and still "round
    trip" under the default encoder. Now the call is bare ``json.dumps(status)``
    so a non-serialisable value raises ``TypeError`` and fails the test.
    """
    import json

    dumped = json.dumps(status)
    # Round-trip sanity: re-parsing the string gives a dict with the same keys.
    reparsed = json.loads(dumped)
    assert set(reparsed.keys()) == set(status.keys())


def test_get_status_blank_template_returns_canonical_trio_only() -> None:
    cls, _ = _scaffold_and_get_class(StrategyTemplate.BLANK)
    inst = _bare_instance(cls)

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert set(status.keys()) == {"strategy", "chain", "wallet"}
    _assert_json_serializable(status)


def test_get_status_lending_loop_exposes_state_and_health_fields() -> None:
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.LENDING_LOOP)
    LendingLoopState = ns["LendingLoopState"]

    inst = _bare_instance(cls)
    inst._loop_state = LendingLoopState.BORROWED
    inst._loop_count = 2
    inst._current_leverage = Decimal("1.85")
    inst.target_leverage = Decimal("2.0")
    inst.collateral_token = "WETH"
    inst.borrow_token = "USDC"
    # Use real Decimals in the snapshot (not pre-stringified strings) so the
    # generated get_status()'s ``_safe`` helper is actually exercised. Before
    # CodeRabbit's audit the templates dropped snapshot values in raw, which
    # would have crashed ``json.dumps(status)`` on this fixture. The strings
    # in the asserts below are what ``_safe`` should convert Decimals into.
    inst._last_position_snapshot = {
        "health_factor": Decimal("1.72"),
        "supply_usd": Decimal("5000.00"),
        "debt_usd": Decimal("2000.00"),
        "ltv": Decimal("0.40"),
    }

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "borrowed"  # StrEnum .value, not enum repr
    assert status["loop_count"] == 2
    assert status["current_leverage"] == "1.85"  # Decimal -> str
    assert status["target_leverage"] == "2.0"
    assert status["health_factor"] == "1.72"
    assert status["supply_usd"] == "5000.00"
    assert status["debt_usd"] == "2000.00"
    assert status["ltv"] == "0.40"
    assert status["collateral_token"] == "WETH"
    assert status["borrow_token"] == "USDC"
    _assert_json_serializable(status)


def test_get_status_lending_loop_defaults_position_fields_to_none() -> None:
    """Without a position snapshot, health_factor / supply_usd / debt_usd / ltv are None."""
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.LENDING_LOOP)
    LendingLoopState = ns["LendingLoopState"]

    inst = _bare_instance(cls)
    inst._loop_state = LendingLoopState.IDLE
    inst._loop_count = 0
    inst._current_leverage = Decimal("1.0")
    inst.target_leverage = Decimal("2.0")
    inst.collateral_token = "WETH"
    inst.borrow_token = "USDC"

    status = inst.get_status()
    assert status["state"] == "idle"
    assert status["health_factor"] is None
    assert status["supply_usd"] is None
    assert status["debt_usd"] is None
    assert status["ltv"] is None


def test_get_status_basis_trade_exposes_legs_and_delta() -> None:
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.BASIS_TRADE)
    BasisTradeState = ns["BasisTradeState"]

    inst = _bare_instance(cls)
    inst._trade_state = BasisTradeState.HEDGED
    inst.base_token = "WETH"
    inst.quote_token = "USDC"
    inst.perp_market = "ETH/USD"
    inst.spot_size_usd = Decimal("10000")
    inst.hedge_ratio = Decimal("1.0")
    inst._last_position_snapshot = {
        "spot_leg_value_usd": Decimal("10050.12"),
        "perp_leg_value_usd": Decimal("-10010.00"),
        "funding_pnl_usd": Decimal("42.50"),
        "net_delta": Decimal("0.0034"),
    }

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "hedged"
    assert status["base_token"] == "WETH"
    assert status["quote_token"] == "USDC"
    assert status["perp_market"] == "ETH/USD"
    assert status["spot_size_usd"] == "10000"
    assert status["hedge_ratio"] == "1.0"
    assert status["spot_leg_value_usd"] == "10050.12"
    assert status["perp_leg_value_usd"] == "-10010.00"
    assert status["funding_pnl_usd"] == "42.50"
    assert status["net_delta"] == "0.0034"
    _assert_json_serializable(status)


def test_get_status_vault_yield_exposes_shares_and_apr() -> None:
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.VAULT_YIELD)
    VaultYieldState = ns["VaultYieldState"]

    inst = _bare_instance(cls)
    inst._state = VaultYieldState.DEPOSITED
    inst.vault_address = "0xbeef000000000000000000000000000000000000"
    inst.deposit_token = "USDC"
    inst.deposit_amount = Decimal("1000")
    inst._last_position_snapshot = {
        "vault_shares": Decimal("998.12"),
        "current_yield_apr": Decimal("0.0543"),
        "deposited_usd": Decimal("1000.00"),
    }

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "deposited"
    assert status["vault_address"] == "0xbeef000000000000000000000000000000000000"
    assert status["deposit_token"] == "USDC"
    assert status["deposit_amount"] == "1000"
    assert status["vault_shares"] == "998.12"
    assert status["current_yield_apr"] == "0.0543"
    assert status["deposited_usd"] == "1000.00"
    _assert_json_serializable(status)


def test_get_status_perps_exposes_direction_and_entry() -> None:
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.PERPS)
    PerpsState = ns["PerpsState"]

    inst = _bare_instance(cls)
    inst._position_state = PerpsState.OPEN
    inst.direction = "LONG"
    inst._position_direction = "LONG"
    inst._is_long = True
    inst._position_is_long = True
    inst.perp_market = "ETH/USD"
    inst.collateral_token = "USDC"
    inst.position_size_usd = Decimal("1000")
    inst._entry_price = Decimal("2000")
    inst.leverage = Decimal("5")
    inst._last_position_snapshot = {
        "pnl_usd": Decimal("12.34"),
        "liq_price": Decimal("1600"),
    }

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "open"
    assert status["direction"] == "LONG"
    assert status["perp_market"] == "ETH/USD"
    assert status["collateral_token"] == "USDC"
    assert status["position_size_usd"] == "1000"
    assert status["entry_price"] == "2000"
    assert status["leverage"] == "5"
    assert status["pnl_usd"] == "12.34"
    assert status["liq_price"] == "1600"
    _assert_json_serializable(status)


def test_get_status_perps_idle_state_has_none_entry_price() -> None:
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.PERPS)
    PerpsState = ns["PerpsState"]

    inst = _bare_instance(cls)
    inst._position_state = PerpsState.IDLE
    inst.direction = "SHORT"
    inst._position_direction = None
    inst._is_long = False
    inst._position_is_long = None
    inst.perp_market = "ETH/USD"
    inst.collateral_token = "USDC"
    inst.position_size_usd = Decimal("500")
    inst._entry_price = None
    inst.leverage = Decimal("3")

    status = inst.get_status()
    assert status["state"] == "idle"
    assert status["direction"] == "SHORT"  # Falls back to self.direction
    assert status["entry_price"] is None
    assert status["pnl_usd"] is None
    assert status["liq_price"] is None


def test_get_status_staking_exposes_staked_amount_and_rewards() -> None:
    from decimal import Decimal

    cls, ns = _scaffold_and_get_class(StrategyTemplate.STAKING, chain=SupportedChain.ETHEREUM)
    StakingState = ns["StakingState"]

    inst = _bare_instance(cls)
    inst._chain = "ethereum"
    inst._stake_state = StakingState.STAKED
    inst.stake_token = "ETH"
    inst.staking_protocol = "lido"
    inst._staked_amount = Decimal("1.5")
    # Exercise the _safe helper's datetime branch: pass a real datetime for
    # unbonding_end_ts (the pre-audit version pre-stringified it, which never
    # hit the isoformat path and would have silently broken any strategy that
    # passed a real datetime).
    from datetime import UTC as _UTC
    from datetime import datetime as _dt

    inst._last_position_snapshot = {
        "rewards_usd": Decimal("3.21"),
        "unbonding_end_ts": _dt(2026, 5, 1, 0, 0, 0, tzinfo=_UTC),
    }

    status = inst.get_status()
    _assert_base_status_shape(status, expected_chain="ethereum")
    assert status["state"] == "staked"
    assert status["stake_token"] == "ETH"
    assert status["staking_protocol"] == "lido"
    assert status["staked_amount"] == "1.5"
    assert status["rewards_usd"] == "3.21"
    assert status["unbonding_end_ts"] == "2026-05-01T00:00:00+00:00"
    _assert_json_serializable(status)


def test_get_status_ta_swap_exposes_signal_state() -> None:
    cls, _ = _scaffold_and_get_class(StrategyTemplate.TA_SWAP)

    inst = _bare_instance(cls)
    inst._holding_base = True
    inst.base_token = "WETH"
    inst.quote_token = "USDC"
    inst._indicator = "rsi"
    # Exercise the _safe helper's Enum branch for last_signal: pass a real
    # Enum member so the generated get_status() has to normalise it via
    # ``getattr(v, 'value', str(v))``.
    from datetime import UTC as _UTC
    from datetime import datetime as _dt
    from enum import Enum as _Enum

    class _Signal(_Enum):
        BUY = "buy"

    inst._last_position_snapshot = {
        "last_signal": _Signal.BUY,
        "last_trade_ts": _dt(2026, 4, 19, 12, 0, 0, tzinfo=_UTC),
    }

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "holding_base"
    assert status["holding_base"] is True
    assert status["base_token"] == "WETH"
    assert status["quote_token"] == "USDC"
    assert status["indicator"] == "rsi"
    assert status["last_signal"] == "buy"
    assert status["last_trade_ts"] == "2026-04-19T12:00:00+00:00"
    _assert_json_serializable(status)


def test_get_status_ta_swap_holding_quote_state() -> None:
    """When not holding base, ``state`` flips to 'holding_quote'."""
    cls, _ = _scaffold_and_get_class(StrategyTemplate.TA_SWAP)

    inst = _bare_instance(cls)
    inst._holding_base = False
    inst.base_token = "WETH"
    inst.quote_token = "USDC"
    inst._indicator = "bollinger"

    status = inst.get_status()
    assert status["state"] == "holding_quote"
    assert status["holding_base"] is False


def test_get_status_dynamic_lp_exposes_position_and_tick_range() -> None:
    from decimal import Decimal

    cls, _ = _scaffold_and_get_class(StrategyTemplate.DYNAMIC_LP)

    inst = _bare_instance(cls)
    inst._position_id = "12345"
    inst._range_lower = Decimal("1900")
    inst._range_upper = Decimal("2100")
    inst.pool = "WETH/USDC/3000"
    inst._last_position_snapshot = {
        "in_range": True,
        "fees_earned_usd": Decimal("5.55"),
    }

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "open"
    assert status["position_id"] == "12345"
    assert status["tick_range"] == ["1900", "2100"]
    assert status["pool"] == "WETH/USDC/3000"
    assert status["in_range"] is True
    assert status["fees_earned_usd"] == "5.55"
    _assert_json_serializable(status)


def test_get_status_dynamic_lp_no_position_returns_idle() -> None:
    cls, _ = _scaffold_and_get_class(StrategyTemplate.DYNAMIC_LP)

    inst = _bare_instance(cls)
    inst._position_id = None
    inst._range_lower = None
    inst._range_upper = None
    inst.pool = "WETH/USDC/3000"

    status = inst.get_status()
    assert status["state"] == "idle"
    assert status["position_id"] is None
    assert status["tick_range"] is None
    assert status["in_range"] is None
    assert status["fees_earned_usd"] is None


def test_get_status_multi_step_exposes_position_and_tick_range() -> None:
    from decimal import Decimal

    cls, _ = _scaffold_and_get_class(StrategyTemplate.MULTI_STEP)

    inst = _bare_instance(cls)
    inst._position_id = "77"
    inst._range_lower = Decimal("1800")
    inst._range_upper = Decimal("2200")
    inst.pool = "WETH/USDC/3000"

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["state"] == "open"
    assert status["position_id"] == "77"
    assert status["tick_range"] == ["1800", "2200"]
    assert status["pool"] == "WETH/USDC/3000"
    _assert_json_serializable(status)


def test_get_status_copy_trader_exposes_open_trades_count() -> None:
    cls, _ = _scaffold_and_get_class(StrategyTemplate.COPY_TRADER)

    inst = _bare_instance(cls)
    inst._open_trades = [{"intent_type": "SWAP"}, {"intent_type": "LP_OPEN"}]
    inst.action_types = ["SWAP", "LP_OPEN"]

    status = inst.get_status()
    _assert_base_status_shape(status)
    assert status["open_trades_count"] == 2
    assert status["action_types"] == ["SWAP", "LP_OPEN"]
    _assert_json_serializable(status)


# ---------------------------------------------------------------------------
# Cross-template invariants
# ---------------------------------------------------------------------------


# Templates whose init requires heavier wiring (gateway/services) for a full
# ``__init__``; we always bypass ``__init__`` via ``_bare_instance`` here, so
# every template in this list works.
_ALL_TEMPLATES_FOR_STATUS = list(StrategyTemplate)


@pytest.mark.parametrize("template", _ALL_TEMPLATES_FOR_STATUS, ids=lambda t: t.value)
def test_get_status_always_returns_canonical_trio(template: StrategyTemplate) -> None:
    """Every template must keep the canonical ``{strategy, chain, wallet}`` trio."""
    # Staking template only supports Ethereum; every other template works on Arbitrum.
    chain = SupportedChain.ETHEREUM if template == StrategyTemplate.STAKING else SupportedChain.ARBITRUM
    expected_chain = "ethereum" if template == StrategyTemplate.STAKING else "arbitrum"
    cls, _ = _scaffold_and_get_class(template, chain=chain)
    inst = _bare_instance(cls)
    inst._chain = expected_chain
    # Seed the minimum attributes that get_status reads, regardless of template.
    _seed_minimum_status_attrs(inst, template)

    status = inst.get_status()
    assert isinstance(status, dict)
    assert status["strategy"] == "status_check"
    assert status["chain"] == expected_chain
    assert status["wallet"] is not None


@pytest.mark.parametrize("template", _ALL_TEMPLATES_FOR_STATUS, ids=lambda t: t.value)
def test_get_status_is_json_serializable(template: StrategyTemplate) -> None:
    """Every template's get_status() output must round-trip through json.dumps."""
    import json

    chain = SupportedChain.ETHEREUM if template == StrategyTemplate.STAKING else SupportedChain.ARBITRUM
    cls, _ = _scaffold_and_get_class(template, chain=chain)
    inst = _bare_instance(cls)
    _seed_minimum_status_attrs(inst, template)
    status = inst.get_status()

    # No custom encoder: the generator must produce JSON-safe values out of the box.
    json.dumps(status)


def _seed_minimum_status_attrs(inst, template: StrategyTemplate) -> None:
    """Set the minimum instance attributes that each template's get_status() reads.

    We bypass ``__init__`` in these tests so we must mirror the subset of
    attributes assigned by ``_get_template_init_params`` that the emitted
    ``get_status()`` reads.
    """
    from decimal import Decimal

    if template == StrategyTemplate.LENDING_LOOP:
        inst._loop_state = "idle"
        inst._loop_count = 0
        inst._current_leverage = Decimal("1.0")
        inst.target_leverage = Decimal("2.0")
        inst.collateral_token = "WETH"
        inst.borrow_token = "USDC"
    elif template == StrategyTemplate.BASIS_TRADE:
        inst._trade_state = "idle"
        inst.base_token = "WETH"
        inst.quote_token = "USDC"
        inst.perp_market = "ETH/USD"
        inst.spot_size_usd = Decimal("10000")
        inst.hedge_ratio = Decimal("1.0")
    elif template == StrategyTemplate.VAULT_YIELD:
        inst._state = "idle"
        inst.vault_address = "0x0000000000000000000000000000000000000000"
        inst.deposit_token = "USDC"
        inst.deposit_amount = Decimal("1000")
    elif template == StrategyTemplate.PERPS:
        inst._position_state = "idle"
        inst.direction = "LONG"
        inst._position_direction = None
        inst.perp_market = "ETH/USD"
        inst.collateral_token = "USDC"
        inst.position_size_usd = Decimal("1000")
        inst._entry_price = None
        inst.leverage = Decimal("5")
    elif template == StrategyTemplate.STAKING:
        inst._stake_state = "idle"
        inst.stake_token = "ETH"
        inst.staking_protocol = "lido"
        inst._staked_amount = None
    elif template == StrategyTemplate.TA_SWAP:
        inst._holding_base = False
        inst.base_token = "WETH"
        inst.quote_token = "USDC"
        inst._indicator = "rsi"
    elif template in (StrategyTemplate.DYNAMIC_LP, StrategyTemplate.MULTI_STEP):
        inst._position_id = None
        inst._range_lower = None
        inst._range_upper = None
        inst.pool = "WETH/USDC/3000"
    elif template == StrategyTemplate.COPY_TRADER:
        inst._open_trades = []
        inst.action_types = []
    # BLANK needs nothing.
