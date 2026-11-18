"""Validate mkdocs.yml nav against the filesystem.

Every nav entry must exist under docs/, and every published page under docs/
must appear in the nav. Generated CLI pages are excluded from orphan detection
because `docs/cli/` is rebuilt as a whole; locale variants pair with their
English sibling through the i18n plugin.
"""

import argparse
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
EXCLUDE_DIRS = {"internal", "assets", "stylesheets", "overrides", "schemas", "cli"}
LOCALE_SUFFIX = re.compile(r"\.(zh|fr|es)\.md$")


def load_mkdocs(path):
    class Loader(yaml.SafeLoader):
        pass

    def ignore(loader, suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    Loader.add_multi_constructor("!", ignore)
    Loader.add_multi_constructor("tag:", ignore)
    return yaml.load(path.read_text(encoding="utf-8"), Loader=Loader)


def nav_files(nav):
    if isinstance(nav, list):
        return [entry for item in nav for entry in nav_files(item)]
    if isinstance(nav, dict):
        return [entry for value in nav.values() for entry in nav_files(value)]
    return [nav] if isinstance(nav, str) else []


def findings(docs_root, nav):
    problems = []
    local = set()
    for entry in nav_files(nav):
        if entry.startswith(("http://", "https://", "mailto:")):
            continue
        path_part = entry.split("#", 1)[0]
        if not path_part:
            continue
        local.add(path_part)
        if not (docs_root / path_part).exists():
            problems.append(f"nav entry refers to missing file: docs/{path_part}")
    for page in sorted(docs_root.rglob("*.md")):
        rel = page.relative_to(docs_root)
        if rel.parts[0] in EXCLUDE_DIRS or LOCALE_SUFFIX.search(page.name):
            continue
        if rel.as_posix() not in local:
            problems.append(f"orphan page (not in nav): docs/{rel.as_posix()}")
    return problems


def main(argv=None, repo_root=REPO_ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    mkdocs = repo_root / "mkdocs.yml"
    if not mkdocs.exists():
        print(f"ERROR: mkdocs.yml not found at {mkdocs}", file=sys.stderr)
        return 2
    nav = load_mkdocs(mkdocs).get("nav", [])
    problems = findings(repo_root / "docs", nav)
    if not problems:
        if not args.quiet:
            print(f"OK: {len(nav_files(nav))} nav entries resolve, no orphans under docs/")
        return 0
    print(f"DRIFT: {len(problems)} mkdocs nav finding(s):")
    for problem in problems:
        print(f"  - {problem}")
    print("Fix: add the nav entry or delete the orphan.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
