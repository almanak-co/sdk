"""Tests for the gas-worthiness gate in scaffolded TA_SWAP strategies.

Verifies that ``almanak strat new --template ta_swap`` produces a strategy
whose ``decide()`` calls ``market.is_trade_worthwhile`` / reads
``min_trade_value_usd`` BEFORE returning a swap intent.
"""

import ast
import json
import tempfile
from pathlib import Path

from almanak.framework.cli.new_strategy import (
    StrategyTemplate,
    SupportedChain,
    generate_config_json,
    generate_strategy_file,
)


def _generate(chain: SupportedChain = SupportedChain.ARBITRUM) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        return generate_strategy_file(
            name="Gas Gate Test",
            template=StrategyTemplate.TA_SWAP,
            chain=chain,
            output_dir=Path(tmpdir),
        )


class TestScaffoldedTaSwapGasGate:
    """Scaffolded ta_swap strategy embeds the gas-worthiness gate in decide()."""

    def test_decide_references_is_trade_worthwhile(self) -> None:
        code = _generate()
        assert "is_trade_worthwhile" in code, (
            "decide() must call market.is_trade_worthwhile() before swapping"
        )

    def test_decide_references_min_trade_value_usd(self) -> None:
        code = _generate()
        assert "min_trade_value_usd" in code, (
            "decide() must check self.min_trade_value_usd absolute floor"
        )

    def test_decide_references_max_gas_ratio(self) -> None:
        code = _generate()
        assert "max_gas_ratio" in code, (
            "decide() must pass self.max_gas_ratio to is_trade_worthwhile()"
        )

    def test_decide_holds_on_gas_gate_failure(self) -> None:
        """Scaffolded code must return Intent.hold with a gas-cost reason."""
        code = _generate()
        assert "gas cost" in code.lower(), (
            "scaffold must emit a hold reason mentioning gas cost when gate fails"
        )

    def test_init_sets_min_trade_value_usd_default(self) -> None:
        """Scaffold __init__ must read min_trade_value_usd from config with a sane default."""
        code = _generate()
        tree = ast.parse(code)
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "__init__":
                # ast.unparse normalizes quoting; check for either style.
                body_src = ast.unparse(node)
                if "get_config('min_trade_value_usd'" in body_src or 'get_config("min_trade_value_usd"' in body_src:
                    found = True
                    break
        assert found, "__init__ must read min_trade_value_usd via get_config()"

    def test_init_sets_max_gas_ratio_default(self) -> None:
        code = _generate()
        tree = ast.parse(code)
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "__init__":
                body_src = ast.unparse(node)
                if "get_config('max_gas_ratio'" in body_src or 'get_config("max_gas_ratio"' in body_src:
                    found = True
                    break
        assert found, "__init__ must read max_gas_ratio via get_config()"

    def test_gate_appears_before_swap_intent_in_decide(self) -> None:
        """The gas-worthiness gate must appear textually BEFORE Intent.swap()
        inside the decide() method body (excluding the docstring) so authors
        can see/modify the check.
        """
        code = _generate()
        tree = ast.parse(code)
        decide_body_src: str | None = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "decide":
                # Skip the docstring (which also mentions Intent.swap in
                # example prose) and compare against the executable body.
                body = node.body
                if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                    body = body[1:]
                decide_body_src = "\n".join(ast.unparse(stmt) for stmt in body)
                break
        assert decide_body_src is not None, "decide() not found in scaffold"

        gate_idx = decide_body_src.find("is_trade_worthwhile")
        swap_idx = decide_body_src.find("Intent.swap")
        assert gate_idx != -1, "decide() must call is_trade_worthwhile()"
        assert swap_idx != -1, "decide() must contain Intent.swap()"
        assert gate_idx < swap_idx, (
            "Gate call must appear before the first Intent.swap() in decide() "
            "so strategy authors can see the check explicitly."
        )


class TestConfigJsonEmitsGateFields:
    """generate_config_json(TA_SWAP, ...) includes gate defaults."""

    def test_min_trade_value_usd_present(self) -> None:
        raw = generate_config_json(
            name="Gas Gate Test",
            template=StrategyTemplate.TA_SWAP,
            chain=SupportedChain.ARBITRUM,
        )
        cfg = json.loads(raw)
        assert cfg.get("min_trade_value_usd") == "10"

    def test_max_gas_ratio_present(self) -> None:
        raw = generate_config_json(
            name="Gas Gate Test",
            template=StrategyTemplate.TA_SWAP,
            chain=SupportedChain.ARBITRUM,
        )
        cfg = json.loads(raw)
        assert cfg.get("max_gas_ratio") == "0.05"
