"""Resolve an RC tag only when it belongs to the checked-out SDK release line."""

import argparse
import re
import tomllib
from pathlib import Path


def candidate_version(tag: str, project_version: str) -> str:
    match = re.fullmatch(r"v?([0-9]+\.[0-9]+\.[0-9]+)-?rc([1-9][0-9]*)", tag)
    if match is None:
        raise ValueError("RC tag must be vX.Y.ZrcN or X.Y.Z-rcN with N greater than zero")
    base, number = match.groups()
    if base != project_version:
        raise ValueError(f"RC base {base} does not match project version {project_version}")
    return f"{base}rc{number}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    args = parser.parse_args(argv)
    project = tomllib.loads(args.project.read_text())["project"]["version"]
    print(candidate_version(args.tag, project))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
