"""Branch coverage for the ``almanak ax --natural`` handler.

Covers ``_handle_natural_language``: missing API key, interpretation error
mapping, chain-default injection, read vs write dispatch, dry-run
simulation, the safety gate, and execution error paths — with the LLM,
catalog, renderers and tool runner all faked.
"""

from types import SimpleNamespace

import click
import pytest

from almanak.framework.agent_tools.catalog import RiskTier
from almanak.framework.agent_tools.llm_client import LLMConfig, LLMConfigError
from almanak.framework.cli.ax import _handle_natural_language
from almanak.framework.cli.ax_natural import NaturalLanguageError


def _ctx(*, json_output=False, dry_run=False, yes=False, chain="ethereum"):
    ctx = click.Context(click.Command("ax"))
    ctx.obj = {
        "json_output": json_output,
        "dry_run": dry_run,
        "yes": yes,
        "chain": chain,
    }
    return ctx


class _Renders:
    """Capture calls to the ax_render functions."""

    def __init__(self):
        self.errors = []
        self.interpretations = []
        self.results = []
        self.simulations = []


@pytest.fixture
def renders(monkeypatch) -> _Renders:
    captured = _Renders()
    monkeypatch.setattr(
        "almanak.framework.cli.ax_render.render_error",
        lambda message, *, json_output=False: captured.errors.append(message),
    )
    monkeypatch.setattr(
        "almanak.framework.cli.ax_render.render_interpretation",
        lambda tool, arguments, *, json_output=False: captured.interpretations.append(
            (tool, dict(arguments))
        ),
    )
    monkeypatch.setattr(
        "almanak.framework.cli.ax_render.render_result",
        lambda response, *, json_output=False, title="Result": captured.results.append(response),
    )
    monkeypatch.setattr(
        "almanak.framework.cli.ax_render.render_simulation",
        lambda response, *, json_output=False: captured.simulations.append(response),
    )
    return captured


@pytest.fixture
def llm_key(monkeypatch):
    monkeypatch.setattr(
        LLMConfig, "from_env", classmethod(lambda cls: LLMConfig(api_key="sk-ant-test"))
    )


def _wire_interpretation(monkeypatch, tool_name="get_balance", arguments=None, error=None):
    async def _interpret(text, chain, config):
        if error is not None:
            raise error
        return SimpleNamespace(tool_name=tool_name, arguments=dict(arguments or {}))

    monkeypatch.setattr(
        "almanak.framework.cli.ax_natural.interpret_natural_language", _interpret
    )


def _wire_catalog(monkeypatch, risk_tier):
    tool_def = None if risk_tier is None else SimpleNamespace(risk_tier=risk_tier)
    catalog = SimpleNamespace(get=lambda name: tool_def)
    monkeypatch.setattr(
        "almanak.framework.agent_tools.catalog.get_default_catalog", lambda: catalog
    )


def _wire_run_tool(monkeypatch, *, status="success", error=None):
    calls = []

    def _run_tool(ctx, tool_name, arguments):
        calls.append((tool_name, dict(arguments)))
        if error is not None:
            raise error
        return SimpleNamespace(status=status)

    monkeypatch.setattr("almanak.framework.cli.ax._run_tool", _run_tool)
    return calls


def test_missing_api_key_exits(monkeypatch, renders):
    monkeypatch.setattr(LLMConfig, "from_env", classmethod(lambda cls: LLMConfig(api_key="")))
    with pytest.raises(SystemExit) as excinfo:
        _handle_natural_language(_ctx(), "swap 1 eth to usdc")
    assert excinfo.value.code == 1
    assert "AGENT_LLM_API_KEY" in renders.errors[0]


@pytest.mark.parametrize(
    "error",
    [LLMConfigError("bad config"), NaturalLanguageError("could not parse")],
    ids=["llm-config", "natural-language"],
)
def test_interpretation_errors_exit(monkeypatch, renders, llm_key, error):
    _wire_interpretation(monkeypatch, error=error)
    with pytest.raises(SystemExit):
        _handle_natural_language(_ctx(), "do something")
    assert renders.errors == [str(error)]


def test_unexpected_interpretation_error_exits(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, error=RuntimeError("api down"))
    with pytest.raises(SystemExit):
        _handle_natural_language(_ctx(), "do something")
    assert "Failed to interpret" in renders.errors[0]


def test_read_action_executes_and_renders(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "get_balance", {"token": "USDC"})
    _wire_catalog(monkeypatch, RiskTier.LOW)
    calls = _wire_run_tool(monkeypatch)
    _handle_natural_language(_ctx(), "what is my USDC balance")
    assert renders.interpretations == [("get_balance", {"token": "USDC"})]
    # Chain default injected because the LLM omitted it.
    assert calls == [("get_balance", {"token": "USDC", "chain": "ethereum"})]
    assert len(renders.results) == 1


def test_explicit_chain_not_overridden(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "get_balance", {"chain": "base"})
    _wire_catalog(monkeypatch, None)
    calls = _wire_run_tool(monkeypatch)
    _handle_natural_language(_ctx(), "balance on base")
    assert calls[0][1]["chain"] == "base"


def test_read_action_error_status_exits(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "get_balance", {})
    _wire_catalog(monkeypatch, RiskTier.LOW)
    _wire_run_tool(monkeypatch, status="error")
    with pytest.raises(SystemExit):
        _handle_natural_language(_ctx(), "balance")


def test_write_action_dry_run_simulates(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "swap_tokens", {"amount": "1"})
    _wire_catalog(monkeypatch, RiskTier.HIGH)
    calls = _wire_run_tool(monkeypatch)
    _handle_natural_language(_ctx(dry_run=True), "swap 1 eth")
    assert calls[0][1]["dry_run"] is True
    assert len(renders.simulations) == 1
    assert renders.results == []


def test_write_action_dry_run_error_exits(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "swap_tokens", {})
    _wire_catalog(monkeypatch, RiskTier.MEDIUM)
    _wire_run_tool(monkeypatch, status="error")
    with pytest.raises(SystemExit):
        _handle_natural_language(_ctx(dry_run=True), "swap")


def test_write_action_safety_gate_cancel(monkeypatch, renders, llm_key, capsys):
    _wire_interpretation(monkeypatch, "swap_tokens", {"amount": "1"})
    _wire_catalog(monkeypatch, RiskTier.HIGH)
    calls = _wire_run_tool(monkeypatch)
    monkeypatch.setattr(
        "almanak.framework.cli.ax_render.check_safety_gate",
        lambda *, dry_run, yes, action_description: False,
    )
    _handle_natural_language(_ctx(), "swap 1 eth")
    assert calls == []
    assert "Cancelled." in capsys.readouterr().out


def test_write_action_gate_approved_executes(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "swap_tokens", {"amount": "1"})
    _wire_catalog(monkeypatch, RiskTier.HIGH)
    calls = _wire_run_tool(monkeypatch)
    gate_args = {}

    def _gate(*, dry_run, yes, action_description):
        gate_args.update(dry_run=dry_run, yes=yes, action_description=action_description)
        return True

    monkeypatch.setattr("almanak.framework.cli.ax_render.check_safety_gate", _gate)
    _handle_natural_language(_ctx(yes=True), "swap 1 eth")
    assert len(calls) == 1
    assert gate_args["yes"] is True
    assert "swap_tokens" in gate_args["action_description"]
    assert "amount=1" in gate_args["action_description"]


def test_execution_exception_exits(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "get_balance", {})
    _wire_catalog(monkeypatch, None)
    _wire_run_tool(monkeypatch, error=RuntimeError("gateway unreachable"))
    with pytest.raises(SystemExit):
        _handle_natural_language(_ctx(), "balance")
    assert renders.errors == ["gateway unreachable"]


def test_click_exception_propagates(monkeypatch, renders, llm_key):
    _wire_interpretation(monkeypatch, "get_balance", {})
    _wire_catalog(monkeypatch, None)
    _wire_run_tool(monkeypatch, error=click.ClickException("usage problem"))
    with pytest.raises(click.ClickException, match="usage problem"):
        _handle_natural_language(_ctx(), "balance")
