"""Adversarial contracts for reviewed Protocol QA capability scope."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture(scope="module")
def scope_module() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "quant-test" / "qa_protocol_scope.py"
    spec = importlib.util.spec_from_file_location("qa_protocol_scope_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_four_focus_protocols_have_reviewed_non_verdict_scope(scope_module: ModuleType) -> None:
    scope_module.validate_scope_reviews()
    assert set(scope_module.PROTOCOL_SCOPE_REVIEWS) == {"aave_v3", "uniswap_v3", "euler_v2", "gmx_v2"}
    for protocol in scope_module.PROTOCOL_SCOPE_REVIEWS:
        review = scope_module.reviewed_scope(protocol)
        assert review["review_id"].startswith(f"protocol-scope.{protocol}.")
        assert len(review["scope_sha256"]) == 64
        assert review["capabilities"]
        assert review["not_advertised"]
        assert all(capability["required"] is True for capability in review["capabilities"])
        assert all("verdict" not in capability for capability in review["capabilities"])


def test_scope_digest_changes_when_scientific_capability_changes(scope_module: ModuleType) -> None:
    review = copy.deepcopy(scope_module.PROTOCOL_SCOPE_REVIEWS["aave_v3"])
    original = scope_module.scope_digest(review)
    review["capabilities"][0]["claim"] = "weaker claim"
    assert scope_module.scope_digest(review) != original


@pytest.mark.parametrize("forbidden", ["verdict", "status", "pass"])
def test_scope_cannot_self_declare_an_evidence_outcome(scope_module: ModuleType, forbidden: str) -> None:
    reviews = copy.deepcopy(scope_module.PROTOCOL_SCOPE_REVIEWS)
    reviews["aave_v3"]["capabilities"][0][forbidden] = "PASS"
    with pytest.raises(ValueError, match="may not declare evidence outcomes"):
        scope_module.validate_scope_reviews(reviews)


def test_scope_rejects_capability_without_exact_proof_owner(scope_module: ModuleType) -> None:
    reviews = copy.deepcopy(scope_module.PROTOCOL_SCOPE_REVIEWS)
    del reviews["gmx_v2"]["capabilities"][0]["obligation_id"]
    with pytest.raises(ValueError, match="one required proof owner"):
        scope_module.validate_scope_reviews(reviews)
