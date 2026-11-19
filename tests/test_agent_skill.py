"""The agent skill names only commands this CLI accepts."""

import re
import shlex
from pathlib import Path

import pytest

from almanak.cli_arguments import build_parser, parse_validated

SKILL = Path(__file__).parents[1] / "skills/almanak/SKILL.md"
PLACEHOLDERS = {
    "<address>": "0x" + "11" * 20,
    "<transport>": "--public-rpc publicnode",
    "<record>": "actions/kept",
    "<chain>": "eip155:43114",
    "<spoke>": "0x94e7a5dcbe816e498b89ab752661904e2f56c485",
    "<id>": "0",
    "<base units>": "1000",
    "<wei>": "1000",
    "<CAIP-19 asset>": "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
}


def commands():
    blocks = re.findall(r"```bash\n(.*?)```", SKILL.read_text(), flags=re.DOTALL)
    lines = [line.strip() for block in blocks for line in block.splitlines()]
    return [line for line in lines if line.startswith("almanak ")]


def filled(line):
    for placeholder, value in PLACEHOLDERS.items():
        line = line.replace(placeholder, value)
    assert "<" not in line, f"unfilled placeholder in: {line}"
    return shlex.split(line)[1:]


def test_the_skill_has_front_matter_and_commands():
    text = SKILL.read_text()
    assert re.match(r"---\nname: almanak\ndescription: .+\n---\n", text)
    assert len(commands()) >= 8


@pytest.mark.parametrize("line", commands())
def test_every_command_in_the_skill_parses(line):
    arguments = filled(line)
    if "--help" in arguments:
        with pytest.raises(SystemExit) as stopped:
            parse_validated(build_parser(), arguments)
        assert stopped.value.code == 0
        return
    parse_validated(build_parser(), arguments)


def test_the_skill_previews_before_it_executes():
    actions = [line for line in commands() if re.match(r"almanak ax (supply|withdraw) ", line)]
    assert {line.split()[2] for line in actions} == {"supply", "withdraw"}
    assert all("--execute" not in line for line in actions if "--help" not in line)


def refusal_messages():
    table = SKILL.read_text().split("## When something is refused")[1].split("\n## ")[0]
    rows = [line.split("|")[1] for line in table.splitlines() if line.startswith("| `")]
    return [message for row in rows for message in re.findall(r"`([^`]+)`", row)]


def string_literals():
    import ast

    literals = []
    for path in (SKILL.parents[2] / "src/almanak").rglob("*.py"):
        nodes = ast.walk(ast.parse(path.read_text()))
        literals.extend(
            node.value
            for node in nodes
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        )
    return literals


def test_every_refusal_the_skill_names_is_text_the_sdk_can_print():
    """Each entry is part of a string the SDK holds, not merely a name in its code.

    This does not prove which command prints it; the fork proof and the action tests
    cover the refusals an action raises.
    """
    literals = string_literals()
    messages = refusal_messages()
    assert len(messages) >= 9
    for message in messages:
        assert any(message in literal for literal in literals), message


def test_reads_and_previews_take_either_transport_and_recovery_takes_the_key():
    """A first run needs no key: only the commands that send anything name the env file."""
    for line in commands():
        if "--help" in line or line.startswith("almanak wallet "):
            continue
        if line.startswith("almanak ax recover"):
            assert "<transport>" not in line
        else:
            assert "<transport>" in line and "--env-file" not in line, line
        keyed = line.replace("<transport>", "--env-file .env")
        parse_validated(build_parser(), filled(keyed))
