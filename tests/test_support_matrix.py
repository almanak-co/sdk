import json

import pytest

from almanak.reports import support
from scripts.docs import author_actions
from scripts.docs import generate_support_matrix as matrix

SAMPLE = {
    "beta": [
        {
            "chain": "eip155:1",
            "modes": ["fork"],
            "actions": ["supply"],
            "account_profiles": ["eoa"],
            "restrictions": [],
            "historical": [],
            "variants": [],
        }
    ],
    "alpha": [
        {
            "chain": "eip155:43114",
            "modes": ["fork", "backtest"],
            "actions": ["swap_exact_input"],
            "account_profiles": ["eoa", "safe_roles"],
            "restrictions": ["verified pools"],
            "historical": [{"model": "m-v1", "mode": "economic", "actions": ["swap_exact_input"]}],
            "variants": ["v4"],
        }
    ],
}


def test_render_orders_connectors_and_fills_cells():
    text = matrix.render(SAMPLE)
    lines = text.splitlines()
    assert lines[0] == matrix.HEADER
    assert "| `alpha` |  | fork, backtest |" in text
    assert "| `beta` | fork |  |" in text
    assert text.index("## alpha") < text.index("## beta")
    assert "`m-v1` (economic)" in text
    assert "| `eip155:1` | fork | supply | eoa | none | none | none |" in text
    assert text.endswith("do not edit by hand.\n")


def test_render_refuses_empty_support():
    with pytest.raises(ValueError):
        matrix.render({})


def test_main_reads_json_input_and_the_installed_package(tmp_path):
    source = tmp_path / "support.json"
    source.write_text(json.dumps(SAMPLE))
    output = tmp_path / "support.md"
    assert matrix.main(["--input", str(source), "--output", str(output)]) == 0
    assert output.read_text().startswith(matrix.HEADER)
    assert author_actions.HEADER not in output.read_text()
    live = tmp_path / "live.md"
    assert matrix.main(["--output", str(live)]) == 0
    assert set(support()) <= {
        line[3:].strip() for line in live.read_text().splitlines() if line.startswith("## ")
    }
    assert author_actions.HEADER in live.read_text()


def test_render_appends_strategy_actions_before_the_footer():
    actions = {"beta": [("supply(name: str, amount: int)", "")], "alpha": [("swap(name)", "Swap.")]}
    text = matrix.render(SAMPLE, actions)
    assert text.index("## beta") < text.index(author_actions.HEADER)
    assert text.index("### alpha actions") < text.index("### beta actions")
    assert "- `swap(name)`: Swap." in text
    assert "- `supply(name: str, amount: int)`\n" in text
    assert "Strategy actions on `ctx.actions` for the `alpha` connector: `swap`." in text
    assert 'under "beta actions" in Strategy actions below.' in text
    assert text.endswith("and the connector action bindings; do not edit by hand.\n")


def test_live_actions_follow_each_connector_binding():
    actions = {key: [text for text, _ in rows] for key, rows in author_actions.collect().items()}
    assert set(support()) <= set(actions)
    assert "component(name)" in actions["composed"]
    assert "supply(name: str, amount: int)" in actions["aave_v3"]
    assert not any(text.startswith(("target(", "allocation(")) for text in actions["aave_v3"])
    assert "swap(name, *, exit=False, rebalance=False)" in actions["uniswap_v4"]
    assert not any(text.startswith("exit_") for text in actions["gmx_v2"])
    assert all(rows for rows in actions.values())


def test_live_actions_describe_their_effects():
    docs = {key: dict(rows) for key, rows in author_actions.collect().items()}
    exit_doc = docs["uniswap_v3"]["exit(name)"]
    assert exit_doc.startswith("Full exit/teardown: close owned liquidity")
    assert "funding asset" in exit_doc and "returns None" in exit_doc
    assert docs["composed"]["component(name)"].startswith("The `ctx.actions` of the component")
    assert "\n" not in exit_doc
    assert all(doc for rows in docs.values() for doc in rows.values())
