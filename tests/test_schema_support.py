"""Versioned SDK schemas are public CLI output and installed wheel data."""

import json
import posixpath
import subprocess
import sys
import zipfile

import pytest

from almanak import cli
from almanak.cli_arguments import build_parser


def external_refs(value):
    if isinstance(value, dict):
        for key, nested in value.items():
            if key == "$ref" and isinstance(nested, str) and not nested.startswith("#"):
                yield nested.split("#", 1)[0]
            else:
                yield from external_refs(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from external_refs(nested)


def assert_refs_are_bundled(document):
    files = document["files"]
    for source, schema in files.items():
        for reference in external_refs(schema):
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(source), reference))
            assert resolved in files, f"{source} references missing {resolved}"


def test_support_schema_parser_and_dispatch(capsys):
    args = build_parser().parse_args(["support", "--schema", "backtest", "--json"])
    assert cli.dispatch(args) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["root"] == "backtest.schema.json"
    assert document["files"]["backtest.schema.json"]["$schema"]


@pytest.mark.parametrize("filter_option", ["--connector", "--chain"])
def test_support_schema_refuses_support_filters(filter_option):
    value = "aave_v4" if filter_option == "--connector" else "eip155:1"
    args = build_parser().parse_args(
        ["support", "--schema", "strategy", filter_option, value, "--json"]
    )
    with pytest.raises(ValueError, match="cannot be combined"):
        cli.dispatch(args)


def test_built_wheel_contains_and_reads_authored_schemas(tmp_path):
    distribution = tmp_path / "dist"
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(distribution)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wheel = next(distribution.glob("*.whl"))
    extracted = tmp_path / "wheel"
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(extracted)
    command = (
        "import json,sys; sys.path.insert(0,sys.argv[1]); "
        "from almanak.schemas import bundle; "
        "print(json.dumps(bundle('strategy')))"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", command, str(extracted)],
        check=True,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    document = json.loads(result.stdout)
    assert document["root"] == "strategy.schema.json"
    assert document["files"]["strategy.schema.json"]["$schema"]
    assert "positions/aave_v4_debt.schema.json" in document["files"]
    assert_refs_are_bundled(document)
