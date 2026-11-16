"""Validate that every `::: dotted.path` mkdocstrings directive resolves.

An unresolved directive renders as an empty section on the site.
"""

import argparse
import importlib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EXCLUDE_DIRS = {"internal", "assets", "stylesheets", "overrides", "schemas"}
DIRECTIVE = re.compile(r"^[ \t]*:::\s+([A-Za-z_][\w.]*)\s*$", re.MULTILINE)


def doc_files(docs_root):
    return sorted(
        path
        for path in docs_root.rglob("*.md")
        if path.relative_to(docs_root).parts[0] not in EXCLUDE_DIRS
    )


def resolve(dotted):
    """Return (ok, error) for a module path or a module attribute path."""
    try:
        importlib.import_module(dotted)
        return True, None
    except ImportError:
        pass
    except Exception as error:
        return False, f"import error: {type(error).__name__}: {error}"
    parts = dotted.split(".")
    for split in range(len(parts) - 1, 0, -1):
        prefix = ".".join(parts[:split])
        try:
            obj = importlib.import_module(prefix)
        except ImportError:
            continue
        except Exception as error:
            return False, f"import error on {prefix!r}: {type(error).__name__}: {error}"
        for attribute in parts[split:]:
            if not hasattr(obj, attribute):
                return False, f"module {prefix!r} has no attribute {'.'.join(parts[split:])!r}"
            obj = getattr(obj, attribute)
        return True, None
    return False, f"could not import any prefix of {dotted!r}"


def findings(docs_root):
    problems = []
    total = 0
    for path in doc_files(docs_root):
        for match in DIRECTIVE.finditer(path.read_text(encoding="utf-8")):
            total += 1
            ok, error = resolve(match.group(1))
            if not ok:
                problems.append(
                    f"{path.relative_to(docs_root.parent)}: '::: {match.group(1)}' {error}"
                )
    return total, problems


def main(argv=None, repo_root=REPO_ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    total, problems = findings(repo_root / "docs")
    if not problems:
        if not args.quiet:
            print(f"OK: {total} mkdocstrings directive(s) resolved")
        return 0
    print(f"DRIFT: {len(problems)} mkdocstrings path(s) do not resolve:")
    for problem in problems:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
