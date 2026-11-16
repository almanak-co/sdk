"""Identify, sanitize and record one hosted cell QA job before its evidence is uploaded.

`identity` records the checked-out source the job ran: commit, tree and lock
hashes, the interpreter and the tool versions, computed exactly as the bench
computes them. `export` copies a pytest node's retained files through the bench's
evidence boundary. `scan` refuses a directory holding any provider key, including
inside gzip members, and reads the key values from the environment only, so no
secret reaches a process argument or the log. `record` writes the job record the
aggregate reads; a job that never writes one is reported as having no result.
"""

import argparse
import gzip
import json
import os
import sys
from pathlib import Path
from urllib.parse import quote

from scripts.qa_bench import identity as bench_identity
from tests.qa.inputs import write_json
from tests.qa.results import export_evidence, secrets_for
from tests.qa.source import tool_versions

# Shorter values would match unrelated bytes; the bench applies the same floor.
MINIMUM_SECRET = 6


def identity():
    value = bench_identity()
    return {
        "commit": value["commit"],
        "tree_sha256": value["tree_sha256"],
        "lock_sha256": value["lock_sha256"],
        "python": value["python"],
        "tools": tool_versions(),
    }


def secret_values(names, environ):
    values = {environ[name] for name in names if environ.get(name)}
    expanded = values | {quote(value, safe="") for value in values}
    return tuple(value.encode() for value in expanded if len(value) >= MINIMUM_SECRET)


def contents(path):
    data = path.read_bytes()
    yield data
    if path.name.endswith((".gz", ".tgz")):
        yield gzip.decompress(data)


def leaked(directory, secrets):
    """Relative paths whose name or bytes carry a secret; links are refused unread."""
    if not secrets:
        raise ValueError("no provider key values to scan for; refusing a vacuous scan")
    if not directory.is_dir():
        raise ValueError(f"{directory} is not a directory to scan")
    found = []
    for path in sorted(directory.rglob("*")):
        relative = str(path.relative_to(directory))
        if path.is_symlink() or any(s in relative.encode() for s in secrets):
            found.append(relative)
        elif path.is_file() and any(s in blob for blob in contents(path) for s in secrets):
            found.append(relative)
    return found


def export(source, destination, env_file):
    if not source.is_dir():
        return []
    return export_evidence(source, destination, secrets_for(env_file, source))


def record(entry, outcome, scan, omitted, before, after):
    return {
        "schema_version": 1,
        "entry": entry,
        "outcome": outcome,
        "scan": scan,
        "omitted": omitted,
        "identity_before": before,
        "identity_after": after,
    }


def read_json(path):
    return json.loads(path.read_text()) if path is not None and path.is_file() else None


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    identifying = commands.add_parser("identity")
    identifying.add_argument("--output", type=Path, required=True)
    exporting = commands.add_parser("export")
    exporting.add_argument("--source", type=Path, required=True)
    exporting.add_argument("--destination", type=Path, required=True)
    exporting.add_argument("--env-file", type=Path, required=True)
    exporting.add_argument("--omitted", type=Path, required=True)
    scanning = commands.add_parser("scan")
    scanning.add_argument("--directory", type=Path, required=True)
    scanning.add_argument("--secret-env", action="append", required=True)
    recording = commands.add_parser("record")
    recording.add_argument("--entry", required=True, help="the job's matrix entry as JSON")
    recording.add_argument("--outcome", required=True)
    recording.add_argument("--scan", choices=("passed", "refused"), required=True)
    recording.add_argument("--omitted", type=Path)
    recording.add_argument("--before", type=Path)
    recording.add_argument("--after", type=Path)
    recording.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def scan(args):
    found = leaked(args.directory, secret_values(args.secret_env, os.environ))
    for path in found:
        print(f"::error::provider key material in {path}", file=sys.stderr)
    return int(bool(found))


def main(argv):
    args = arguments(argv)
    if args.command == "identity":
        write_json(args.output, identity())
    elif args.command == "export":
        omitted = export(args.source, args.destination, args.env_file)
        previous = read_json(args.omitted) or []
        write_json(args.omitted, [*previous, *omitted])
    elif args.command == "scan":
        return scan(args)
    else:
        value = record(
            json.loads(args.entry),
            args.outcome,
            args.scan,
            read_json(args.omitted) or [],
            read_json(args.before),
            read_json(args.after),
        )
        write_json(args.output, value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
