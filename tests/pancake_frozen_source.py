"""Historical lifecycle captures retain their executed author source identity."""

import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
BASE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/pancakeswap-v3-bnb-public-native"


def frozen_sources(package, name):
    """The {path: sha256} source freeze a package seals, checked against its manifest."""
    manifest = json.loads((package / "manifest.json").read_text())
    entry = next(item for item in manifest["files"] if item["path"] == name)
    raw = (package / name).read_bytes()
    assert (len(raw), hashlib.sha256(raw).hexdigest()) == (entry["bytes"], entry["sha256"])
    return json.loads(gzip.decompress(raw))


def assert_frozen_reference(
    reference, definition, package=BASE, name="audit/qualified/source-freeze.json.gz"
):
    frozen = frozen_sources(package, name)
    assert definition.filename == reference.name
    assert definition.sha256 == frozen[reference.relative_to(ROOT).as_posix()]


def recaptured_reference(package):
    """Check a recaptured lifecycle against the source freeze sealed beside it."""

    def check(reference, definition):
        assert_frozen_reference(reference, definition, package, "source-freeze.json.gz")

    return check
