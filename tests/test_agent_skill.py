"""The agent skill names only commands this CLI accepts."""

import re
import shlex
from pathlib import Path

import pytest

from almanak.cli_arguments import build_parser, parse_validated

SKILL = Path(__file__).parents[1] / "skills/almanak/SKILL.md"
PLACEHOLDERS = {
    "<address>": "0x" + "11" * 20,
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
