"""Pin retained evidence copies for tests, and verify or restore the committed copies.

Tests that read retained captures through tests/retained_fixtures.py read copies committed
under tests/fixtures/evidence/. tests/fixtures/evidence.json pins each copy's SHA-256 by its
path relative to the evidence tree.

  pin NAME... --env-file .env  pin retained files and place their copies for commit, after
                               refusing any that carries a non-GitHub URL, a credential
                               assignment or an operator env value; also keeps a <digest>
                               copy in the local cache
  fetch                        verify every pinned copy, restoring a missing or changed one
                               from the cache or the local evidence tree; fails on any copy
                               no source has

The retained originals are only read.
"""

import argparse
import gzip
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path, PurePosixPath

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence"
FIXTURES = ROOT / "tests/fixtures"
CACHE = ROOT / ".fixture-cache"
# Public source repositories are the only URLs a copy may name, whatever the scheme and also
# when JSON escapes their slashes as \/. Credential markers are
# matched as assignments, so prose such as "no production authorization" and fields that
# only name an environment variable, such as "api_key_env", pass.
FORBIDDEN = re.compile(
    rb"(?i)\b[a-z][a-z0-9+.-]*:(?:\\?/){2}(?!github\.com\\?/)"
    rb"|(?:api[_-]?key|secret|password|private[_-]?key|(?:access|refresh|auth|session)[_-]?token)"
    rb"[\"']?\s*[:=]"
    rb"|bearer\s+\w|authorization[\"']?\s*:\s*[\"']?(?:bearer|basic)"
)


def main(argv=None) -> int:
    args = arguments(argv)
    return args.command(args)


def arguments(argv):
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--manifest", type=Path, default=FIXTURES / "evidence.json")
    shared.add_argument("--cache", type=Path, default=CACHE)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(required=True)
    pinning = commands.add_parser("pin", parents=[shared])
    pinning.add_argument("relative", nargs="+", help="paths relative to the evidence tree")
    pinning.add_argument("--env-file", type=existing_file, required=True)
    pinning.set_defaults(command=pin)
    commands.add_parser("fetch", parents=[shared]).set_defaults(command=fetch)
    return parser.parse_args(argv)


def pin(args) -> int:
    secrets = env_values(args.env_file)
    refused = [
        (name, reason)
        for name in args.relative
        if (reason := outside(name) or refusal(EVIDENCE / name, secrets)) is not None
    ]
    if refused:
        report(refused, "refused")
        return 1
    pinned = read_manifest(args.manifest)
    for name in args.relative:
        digest = sha256(EVIDENCE / name)
        place(EVIDENCE / name, args.cache / digest)
        place(EVIDENCE / name, FIXTURES / "evidence" / name)
        pinned[name] = digest
    args.manifest.write_text(json.dumps(dict(sorted(pinned.items())), indent=1) + "\n")
    print(json.dumps({"pinned": len(args.relative), "manifest": len(pinned)}))
    return 0


def fetch(args) -> int:
    if manifest_missing(args.manifest):
        return 1
    missing = []
    for name, digest in read_manifest(args.manifest).items():
        target = FIXTURES / "evidence" / name
        if reason := escaping(name) or destination(name):
            missing.append((name, reason))
        elif not (target.is_file() and sha256(target) == digest):
            source = source_for(name, digest, args.cache)
            if source is None:
                missing.append((name, f"no copy with digest {digest}"))
            else:
                place(source, target)
    if missing:
        report(missing, "missing")
        return 1
    return 0


def source_for(name: str, digest: str, cache: Path) -> Path | None:
    candidates = (cache / digest, EVIDENCE / name)
    return next((p for p in candidates if p.is_file() and sha256(p) == digest), None)


def existing_file(value: str) -> Path:
    # python-dotenv reads a missing file as empty, which would silently skip the value check.
    path = Path(value)
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"env file {value} does not exist")
    return path


def env_values(path: Path) -> list[bytes]:
    values = dotenv_values(path, interpolate=False).values()
    return [value.encode() for value in values if value is not None and len(value) >= 8]


def outside(name: str) -> str | None:
    """Names must stay inside the evidence tree, so reads and copies stay in theirs."""
    if reason := escaping(name):
        return reason
    if not (EVIDENCE / name).resolve().is_relative_to(EVIDENCE.resolve()):
        return "resolves outside the evidence tree"
    return destination(name)


def escaping(name: str) -> str | None:
    relative = PurePosixPath(name)
    if relative.is_absolute() or ".." in relative.parts:
        return "not a path inside the evidence tree"
    return None


def destination(name: str) -> str | None:
    copies = FIXTURES / "evidence"
    target = copies / name
    if any(p.is_symlink() for p in (target, *target.parents) if p.is_relative_to(copies)):
        return "copy destination passes through a symlink"
    return None


def refusal(path: Path, secrets: list[bytes]) -> str | None:
    if not path.is_file():
        return "not a retained file"
    raw = path.read_bytes()
    content = gzip.decompress(raw) if path.suffix == ".gz" else raw
    if match := FORBIDDEN.search(content):
        return f"carries {match.group().decode(errors='replace')!r}"
    if any(secret in content for secret in secrets):
        return "carries an operator env value"
    return None


def manifest_missing(path: Path) -> bool:
    # Only pin may start an empty manifest; fetching nothing must not pass.
    if path.is_file():
        return False
    report([(str(path), "the manifest does not exist")], "missing")
    return True


def read_manifest(path: Path) -> dict[str, str]:
    return json.loads(path.read_text()) if path.exists() else {}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def place(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def report(problems, verb: str) -> None:
    for name, reason in problems:
        print(f"{verb} {name}: {reason}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
