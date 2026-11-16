"""Replacement evidence for native cases whose old fixture funds contained code."""

import json

from tests.qa.catalog import ROOT

QUALIFIED = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/plain-eoa-qualified"


def native_package(case, original):
    manifest = json.loads((QUALIFIED / "manifest.json").read_text())
    return QUALIFIED if any(row["case"] == case.id for row in manifest["cases"]) else original
