"""Placed evidence copies are exactly the files the manifest pins, with no extras.

The copies are committed; `make fixtures` verifies them.
"""

import hashlib

from scripts.evidence_fixtures import refusal
from tests.retained_fixtures import ROOT, manifest


def copies():
    return sorted(path for path in ROOT.rglob("*") if path.is_file())


def test_every_placed_copy_matches_its_pinned_digest():
    present = {
        path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in copies()
    }
    assert present == manifest()


def test_manifest_is_sorted_and_names_only_relative_paths():
    pinned = manifest()
    assert list(pinned) == sorted(pinned)
    assert all(not name.startswith("/") and ".." not in name.split("/") for name in pinned)


def test_no_placed_copy_carries_a_provider_url_or_credential():
    refused = {
        path.relative_to(ROOT).as_posix(): reason
        for path in copies()
        if (reason := refusal(path, [])) is not None
    }
    assert refused == {}
