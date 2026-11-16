"""Check installed SDK commands outside the checkout: schema kinds and reference strategies."""

import argparse
import json
import posixpath
import subprocess
import tempfile
from pathlib import Path

from almanak.references import is_published


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


def verify_bundle(document: dict, kind: str, schemas: Path) -> None:
    if document["root"] != f"{kind}.schema.json":
        raise ValueError("installed command returned the wrong schema root")
    files = document["files"]
    if document["root"] not in files or not files[document["root"]].get("$schema"):
        raise ValueError("installed command omitted the root JSON schema")
    for source, schema in files.items():
        for reference in external_refs(schema):
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(source), reference))
            if resolved not in files:
                raise ValueError(f"installed schema bundle is missing {resolved}")
    if kind != "dashboard":
        expected = {
            p.relative_to(schemas).as_posix(): json.loads(p.read_text())
            for p in schemas.rglob("*.schema.json")
        }
        if files != expected:
            raise ValueError("installed schema bundle differs from the release source")


def verify_references(document: dict, root: Path) -> None:
    """The installed listing must be the source's published references, read from the wheel."""
    source = root / "strategies"
    if Path(document["directory"]).resolve() == source.resolve():
        raise ValueError("installed command read references from the checkout, not the wheel")
    expected = {
        path.parent.name
        for path in source.glob("*/strategy.py")
        if (path.parent / "config.yaml").is_file() and is_published(path.parent.name)
    }
    listed = {entry["name"]: entry for entry in document["references"]}
    if set(listed) != expected:
        raise ValueError("installed reference listing differs from the release source")
    for name, entry in listed.items():
        for file in entry["files"]:
            if (Path(entry["path"]) / file).read_bytes() != (source / name / file).read_bytes():
                raise ValueError(f"installed reference file differs from source: {name}/{file}")


def smoke(python: Path, version: str, root: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="almanak-release-smoke-") as directory:
        subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                "import importlib.metadata,sys; "
                "assert importlib.metadata.version('almanak') == sys.argv[1]",
                version,
            ],
            cwd=directory,
            check=True,
        )
        command = [str(python.parent / "almanak")]
        subprocess.run([*command, "--help"], cwd=directory, check=True, capture_output=True)
        for kind in ("strategy", "backtest", "dashboard"):
            result = subprocess.run(
                [*command, "support", "--schema", kind, "--json"],
                cwd=directory,
                check=True,
                capture_output=True,
                text=True,
            )
            verify_bundle(json.loads(result.stdout), kind, root / "docs/schemas")
        result = subprocess.run(
            [*command, "support", "--references", "--json"],
            cwd=directory,
            check=True,
            capture_output=True,
            text=True,
        )
        verify_references(json.loads(result.stdout), root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    smoke(args.python.absolute(), args.expected_version, args.root.resolve())
    print(f"Installed SDK {args.expected_version} passed CLI, schema and reference smoke checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
