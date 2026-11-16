"""Merge shard junit results into a .test_durations file for pytest-split.

Every shard writes test-results-<K>.xml with per-test wall times measured under
hosted CI conditions, which are the conditions the next run partitions for.
Merging those files keeps every collected test at its measured duration; without
a recorded entry pytest-split falls back to the suite average, which unbalances
the shards as the suite grows.

Usage: merge_test_durations.py OUTPUT INPUT [INPUT ...]
where each INPUT is a test-results XML file or a directory searched for
test-results-*.xml. A test recorded by several inputs keeps its longest time.
Tests absent from every input are dropped: they are not collected in the hosted
scope, so a recorded duration would never be used.
"""

import argparse
import json
import sys
from pathlib import Path

import defusedxml.ElementTree as DET


def junit_files(inputs):
    files = []
    for name in inputs:
        path = Path(name)
        if path.is_dir():
            files.extend(sorted(path.rglob("test-results-*.xml")))
        elif path.is_file():
            files.append(path)
        else:
            raise ValueError(f"junit input does not exist: {name}")
    if not files:
        raise ValueError("no test-results XML found in the inputs")
    return files


def test_id(testcase):
    # pytest mangles the node ID into a dotted classname (file path and class
    # segments joined by dots), which cannot be unambiguously split again. The
    # xunit1 `file` attribute carries the real path; class segments are the
    # classname suffix beyond the module. Without `file` (xunit2 output), fall
    # back to treating the whole classname as the module.
    name = testcase.get("name")
    classname = testcase.get("classname")
    if name is None or classname is None:
        raise ValueError("testcase without a name or classname attribute")
    path = testcase.get("file")
    if path is None:
        return classname.replace(".", "/") + ".py::" + name
    module = path.removesuffix(".py").replace("/", ".")
    prefix = module + "."
    classes = classname[len(prefix) :] if classname.startswith(prefix) else ""
    parts = [path]
    if classes:
        parts.append(classes.replace(".", "::"))
    parts.append(name)
    return "::".join(parts)


def merge(files):
    durations: dict[str, float] = {}
    for path in files:
        for testcase in DET.parse(path).getroot().iter("testcase"):
            key = test_id(testcase)
            durations[key] = max(durations.get(key, 0.0), float(testcase.get("time") or 0.0))
    return durations


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="durations file to write")
    parser.add_argument("inputs", nargs="+", help="junit XML files or directories")
    options = parser.parse_args(argv)
    durations = merge(junit_files(options.inputs))
    options.output.write_text(json.dumps(durations, indent=2) + "\n", encoding="utf-8")
    print(f"Recorded {len(durations)} test durations in {options.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
