"""Validate that locale pages are partial translations of the English page.

A locale page may have fewer headings than the English page, but the heading
levels it has must appear in the same order. Heading text is not compared.
"""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCALES = ("zh", "fr", "es")
HEADING = re.compile(r"^(#{2,3})\s+\S", re.MULTILINE)


def signature(path):
    return tuple(len(m.group(1)) for m in HEADING.finditer(path.read_text(encoding="utf-8")))


def translated_pages(docs_root):
    pairs = []
    for page in sorted(docs_root.rglob("*.md")):
        if any(page.name.endswith(f".{locale}.md") for locale in LOCALES):
            continue
        locales = [
            page.with_name(f"{page.name[:-3]}.{locale}.md")
            for locale in LOCALES
            if page.with_name(f"{page.name[:-3]}.{locale}.md").exists()
        ]
        if locales:
            pairs.append((page, locales))
    return pairs


def is_subsequence(sub, full):
    index = 0
    for level in full:
        if index < len(sub) and sub[index] == level:
            index += 1
    return index == len(sub)


def findings(docs_root):
    pairs = translated_pages(docs_root)
    problems = []
    for english, locales in pairs:
        expected = signature(english)
        for locale in locales:
            actual = signature(locale)
            if not is_subsequence(actual, expected):
                problems.append(
                    f"{locale.relative_to(docs_root.parent)}: heading levels {actual} are not an "
                    f"ordered subsequence of {english.name} levels {expected}"
                )
    return len(pairs), problems


def main(argv=None, repo_root=REPO_ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    pairs, problems = findings(repo_root / "docs")
    if not problems:
        if not args.quiet:
            print(f"OK: i18n structure aligned across {pairs} translated page(s)")
        return 0
    print(f"DRIFT: {len(problems)} i18n structure finding(s):")
    for problem in problems:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
