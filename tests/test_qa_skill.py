"""The live QA skill names only commands that exist, and gates the write level."""

import re
import shlex
from pathlib import Path

import pytest

from almanak.cli_arguments import build_parser, parse_validated
from scripts.live_qa import __main__ as entry
from scripts.live_qa import writes

SKILL = Path(__file__).parents[1] / "skills/almanak-qa/SKILL.md"
RUNNER = "uv run --locked python -m scripts.live_qa "
SPEND = f"{writes.SPEND_ENV}=1 "
PLACEHOLDERS = {
    "<market>": "ethereum-usdc",
    "<base units>": "100000",
    "<wallet directory>": "wallet",
    "<wei>": "20000000000",
    "<fee>": "10000000000000000",
    "<new directory>": "artifacts/live-writes",
    "<phrase>": "supply 100000 ethereum-usdc and withdraw",
    "<version>": "3.2.1",
    "<text>": "gmx_v2",
    "<path>": "record",
}


def commands():
    blocks = re.findall(r"```bash\n(.*?)```", SKILL.read_text(), flags=re.DOTALL)
    return [line.strip() for block in blocks for line in block.splitlines() if line.strip()]


def filled(line):
    for placeholder, value in PLACEHOLDERS.items():
        line = line.replace(placeholder, value)
    assert "<" not in line, f"unfilled placeholder in: {line}"
    return shlex.split(line)


def runner_arguments(line):
    return filled(line.removeprefix(SPEND).removeprefix(RUNNER))


def test_the_skill_has_front_matter_beside_the_almanak_defi_skill():
    front = r"---\nname: almanak-qa\ndescription: .+\nmetadata:\n  internal: true\n---\n"
    assert re.match(front, SKILL.read_text())
    assert (SKILL.parents[1] / "almanak-defi/SKILL.md").is_file()


@pytest.mark.parametrize("line", commands())
def test_every_command_in_the_skill_parses(line):
    if line.startswith("almanak "):
        parse_validated(build_parser(), filled(line)[1:])
    elif RUNNER in line:
        entry.build_parser().parse_args(runner_arguments(line))
    else:
        assert line.startswith("uv run --locked pytest tests/live --live-public"), line


def test_the_read_level_needs_no_flag_wallet_or_key():
    reads = [line for line in commands() if RUNNER + "reads" in line]
    assert len(reads) >= 3
    for line in reads:
        assert line.startswith(RUNNER), line
        assert "--wallet" not in line and "--env-file" not in line and "--execute" not in line


def test_the_write_level_previews_first_and_executes_only_with_the_phrase(tmp_path):
    lines = [line for line in commands() if RUNNER + "writes" in line]
    assert len(lines) == 2 and all(line.startswith(SPEND) for line in lines)
    preview, execute = (entry.build_parser().parse_args(runner_arguments(line)) for line in lines)
    assert not preview.execute and preview.confirm is None
    assert execute.execute and execute.confirm == PLACEHOLDERS["<phrase>"]
    for args in (preview, execute):
        request = writes.WriteRequest(
            market=args.market,
            amount=args.amount,
            # Under tmp_path, so a wallet directory in the checkout cannot change the outcome.
            wallet=tmp_path / args.wallet,
            max_gas_price_wei=args.max_gas_price_wei,
            max_transaction_fee=args.max_transaction_fee,
            public_rpc=args.public_rpc,
            confirm=args.confirm,
        )
        # Every gate but the wallet's own existence is met by the command as written.
        assert writes.gates(request, {writes.SPEND_ENV: "1"}) == [
            "--wallet holds no wallet.json from `almanak wallet create`"
        ]
        assert writes.confirmation_phrase(request) == PLACEHOLDERS["<phrase>"]


def test_the_sizes_the_skill_suggests_are_the_ones_the_check_enforces():
    text = SKILL.read_text()
    for name, market in writes.MARKETS.items():
        row = next(line for line in text.splitlines() if line.startswith(f"| `{name}` |"))
        assert f"`{market.suggested}`" in row and f"`{market.maximum}`" in row, name


def test_the_skill_repeats_the_cost_and_the_rules_it_must_keep():
    text = " ".join(SKILL.read_text().replace("\n> ", "\n").split())
    for statement in (
        "This spends real funds on mainnet.",
        "It must not run in CI, must not be scheduled, and must not be run often.",
        "Never start the write level on your own initiative",
        "Never create a wallet for this",
        "Never print, read, copy or move `keystores.json`, `keystore-password`, a private key",
        "Never send funds anywhere.",
        "Then ask them to confirm, and wait.",
        "Only after the user confirms",
        "A blocked case is not a pass",
        "Preview, show, wait for the confirmation, execute, report.",
    ):
        assert statement in text, statement
    assert text.lower().count("real funds") >= 6
    # The preview comes before the execution in the text an agent follows.
    assert text.index("### 2. Preview") < text.index("### 3. Show the preview and wait")
    assert text.index("### 3. Show the preview and wait") < text.index("### 4. Execute")


def test_shares_that_could_not_be_compared_are_reported_as_inconclusive():
    """Missing shares are not a measured zero, so they show neither a change nor none."""
    text = " ".join(SKILL.read_text().split())
    assert "shows no change" not in text
    assert "say the comparison is inconclusive" in text
    assert "the reports do not show whether the position changed" in text
    assert "could not confirm the position is empty" in text
