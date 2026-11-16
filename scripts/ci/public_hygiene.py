"""Refuse private material in the tree that is published to the public repository.

Scans every Git-tracked file outside ``PRIVATE_PATHS``, reading ``.gz`` and ``.xz`` files
decompressed, for credentials, internal infrastructure names, workstation paths, tracker
references, links into the private documents and, where a local denylist exists, denied wallet
addresses. A finding names
the file, line and rule only: the matched text is never printed, since it may be a secret.
"""

import argparse
import gzip
import lzma
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[2]

# Tracked for now but never exported to the public repository.
PRIVATE_PATHS = ("docs/internal/",)

# Files that state the rules themselves, and the ignore file, which names local-only
# paths under the private documents so they are never committed.
EXEMPT = ("scripts/ci/public_hygiene.py", "tests/test_public_hygiene.py", ".gitignore")

RULES: dict[str, bytes] = {
    "aws-access-key": rb"AKIA[0-9A-Z]{16}",
    "github-token": rb"gh[pousr]_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{40,}",
    "slack-token": rb"xox[baprs]-[A-Za-z0-9-]{10,}",
    "google-api-key": rb"AIza[0-9A-Za-z_-]{35}",
    "private-key-block": rb"-----BEGIN [A-Z ]*PRIVATE KEY",
    "coingecko-key": rb"CG-[A-Za-z0-9]{22,}",
    "pypi-token": rb"pypi-AgE[A-Za-z0-9_-]{20,}",
    # `test-key` is the placeholder the offline tests use.
    "provider-url-key": (
        rb"g\.alchemy\.com/v2/(?!test-key\b)[A-Za-z0-9_-]{8,}"
        rb"|infura\.io/v3/[a-f0-9]{16,}|quiknode\.pro/[a-f0-9]{16,}"
    ),
    "gcp-project": rb"starry-agency|almanak-production",
    "gcp-identity": rb"iam\.gserviceaccount\.com|workloadIdentityPools",
    "gcs-bucket": rb"gs://[a-z0-9]",
    "workstation-path": rb"/home/[a-z][a-z0-9_-]*/|/Users/[A-Za-z][A-Za-z0-9_-]*/",
    "tracker-reference": rb"linear\.app|\b(?:ALM|VIB)-[0-9]{3,}\b",
    "private-repository": rb"almanak-sdk-private|almanak-co/platform|almanak-co/sdk-v3",
    # The retained evidence directory is a local, ignored location and may be named.
    "private-document-link": (
        rb"docs/internal/(?!reports/evidence\b)"
        rb"|\binternal/(?:blueprints|reports)/(?!evidence\b)"
    ),
}

ADDRESS = re.compile(rb"0x[0-9a-fA-F]{40}(?![0-9a-fA-F])")

# Denied wallet addresses, one per line, in a file that is never committed: a published
# list, even hashed, would name the accounts it protects, since the set of on-chain
# addresses is small enough to hash exhaustively. Without the file no address is denied.
DENYLIST = Path(".local/public-hygiene-denylist.txt")


class Finding(NamedTuple):
    path: str
    line: int
    rule: str


def compiled(rules: dict[str, bytes]) -> re.Pattern[bytes]:
    return re.compile(
        b"|".join(
            b"(?P<%s>%s)" % (name.replace("-", "_").encode(), body) for name, body in rules.items()
        )
    )


def content(path: Path) -> bytes:
    """The file's bytes, decompressed when its suffix names a supported compression."""
    data = path.read_bytes()
    if path.suffix == ".gz":
        return gzip.decompress(data)
    if path.suffix == ".xz":
        return lzma.decompress(data)
    return data


def denied_addresses(path: Path) -> frozenset[bytes]:
    """Lowercase addresses from the local denylist; blank lines and `#` comments are skipped."""
    if not path.is_file():
        return frozenset()
    lines = (line.split("#")[0].strip().lower() for line in path.read_text().splitlines())
    return frozenset(line.encode() for line in lines if line)


def scan(
    name: str,
    data: bytes,
    pattern: re.Pattern[bytes],
    denied: frozenset[bytes] = frozenset(),
) -> Iterator[Finding]:
    for match in pattern.finditer(data):
        line = data.count(b"\n", 0, match.start()) + 1
        yield Finding(name, line, (match.lastgroup or "").replace("_", "-"))
    if not denied:
        return
    for match in ADDRESS.finditer(data):
        if match.group().lower() in denied:
            yield Finding(name, data.count(b"\n", 0, match.start()) + 1, "denied-address")


def published(names: Iterable[str]) -> list[str]:
    return [name for name in names if not name.startswith(PRIVATE_PATHS) and name not in EXEMPT]


def tracked(root: Path) -> list[str]:
    result = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True)
    return [name for name in result.stdout.decode().split("\0") if name]


def findings(root: Path, names: Iterable[str]) -> list[Finding]:
    pattern = compiled(RULES)
    denied = denied_addresses(root / DENYLIST)
    result: list[Finding] = []
    for name in names:
        path = root / name
        if path.is_symlink() or not path.is_file():
            continue
        result.extend(scan(name, content(path), pattern, denied))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths", nargs="*", help="tracked paths to scan; default is every published file"
    )
    args = parser.parse_args(argv)
    found = findings(ROOT, published(args.paths or tracked(ROOT)))
    for item in found:
        print(f"{item.path}:{item.line}: {item.rule}")
    if found:
        print(
            f"{len(found)} finding(s) in {len({item.path for item in found})} file(s)",
            file=sys.stderr,
        )
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
