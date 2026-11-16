import argparse

from almanak.cli_arguments import build_parser
from scripts.docs import generate_cli_reference as cli_docs


def test_every_product_is_listed_with_a_page(tmp_path):
    rendered = cli_docs.pages(build_parser())
    names = cli_docs.write_pages(tmp_path, rendered)
    index = (tmp_path / "almanak.md").read_text()
    products = [name for name, _, _ in cli_docs.subcommands(build_parser())]
    assert products
    for product in products:
        assert f"`almanak {product}`" in index
    assert "almanak-lifecycle" in names
    assert "almanak-backtest" in names
    assert cli_docs.main(["--output", str(tmp_path)]) == 0
    assert cli_docs.main(["--output", str(tmp_path), "--check"]) == 0


def test_check_mode_reports_stale_pages(tmp_path):
    assert cli_docs.main(["--output", str(tmp_path), "--check"]) == 1
    assert cli_docs.main(["--output", str(tmp_path)]) == 0
    (tmp_path / "almanak-check.md").write_text("stale\n")
    assert cli_docs.main(["--output", str(tmp_path), "--check"]) == 1
    assert cli_docs.main(["--output", str(tmp_path)]) == 0
    (tmp_path / "almanak-v3-old.md").write_text("obsolete\n")
    assert cli_docs.main(["--output", str(tmp_path), "--check"]) == 1
    assert cli_docs.main(["--output", str(tmp_path)]) == 0
    assert not (tmp_path / "almanak-v3-old.md").exists()
    assert cli_docs.main(["--output", str(tmp_path), "--check"]) == 0


def test_exclusive_groups_and_nested_subcommands_render():
    rendered = cli_docs.pages(build_parser())
    check = "\n".join(rendered["almanak-check"])
    assert "At most one of `--fork`, `--production`." in check
    run = "\n".join(rendered["almanak-run"])
    assert "Exactly one of `--fork`, `--production`." in run
    backtest = "\n".join(rendered["almanak-backtest"])
    assert "## almanak backtest run" in backtest
    assert "## almanak backtest reproduce" in backtest
    lifecycle = "\n".join(rendered["almanak-lifecycle"])
    for name in cli_docs.LIFECYCLE:
        assert f"## almanak {name}" in lifecycle


def test_option_rows_hide_suppressed_and_describe_types():
    parser = argparse.ArgumentParser(prog="x")
    parser.add_argument("--secret", help=argparse.SUPPRESS)
    parser.add_argument("--count", type=int, default=3, required=False, help="how many")
    parser.add_argument("--mode", choices=("a", "b"))
    parser.add_argument("name")
    rows = cli_docs.option_rows(parser)
    assert [row[0] for row in rows] == ["`--count`", "`--mode`", "`name`"]
    assert rows[0][1] == "int; default `3`"
    assert rows[1][1] == "one of `a`, `b`"
    assert cli_docs.type_name(parser._actions[0]) == ""
