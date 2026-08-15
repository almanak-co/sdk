"""Contract tests for the lifecycle evidence-reference integrity gate."""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

from almanak.connectors._connector_descriptor import ImportRef
from almanak.core.capability_obligations import EvidenceKind, EvidenceRef
from almanak.framework.capabilities.effective_matrix import build_effective_capability_matrix
from scripts.ci import check_lifecycle_capability_ratchet as ratchet
from scripts.ci.lifecycle_evidence_integrity import (
    EvidenceIntegrityError,
    validate_evidence_ref,
    validate_matrix_evidence,
)
from scripts.ci.production_claim_universe import build_production_core_execution_matrix


def _write(repo_root: Path, ref: str) -> None:
    path = repo_root / ref
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "def test_evidence():\n    pass\n" if path.suffix == ".py" else "evidence\n"
    path.write_text(content, encoding="utf-8")


@pytest.mark.parametrize(
    ("kind", "ref"),
    [
        (EvidenceKind.CONTRACT_TEST, "tests/unit/test_contract.py"),
        (EvidenceKind.INTENT_TEST, "tests/intents/base/test_swap.py"),
        (EvidenceKind.REAL_FORK, "tests/intents/base/test_real_fork.py"),
        (EvidenceKind.MANAGED_ANVIL, "tests/intents/base/test_managed_anvil.py"),
    ],
)
def test_repository_evidence_kinds_resolve_only_permitted_files(
    tmp_path: Path,
    kind: EvidenceKind,
    ref: str,
) -> None:
    _write(tmp_path, ref)

    validate_evidence_ref(EvidenceRef(kind, ref), repo_root=tmp_path)


@pytest.mark.parametrize(
    ("kind", "ref", "match"),
    [
        (EvidenceKind.CONTRACT_TEST, "todo", "must be under"),
        (EvidenceKind.INTENT_TEST, "tests/unit/test_wrong_surface.py", "must be under"),
        (EvidenceKind.REAL_FORK, "../outside.py", "canonical repository-relative"),
        (EvidenceKind.REAL_FORK, "/tmp/evidence.py", "canonical repository-relative"),
        (EvidenceKind.REAL_FORK, "tests/intents/missing.py", "does not exist"),
        (EvidenceKind.REAL_FORK, "tests/intents/report.txt", "suffixes"),
        (EvidenceKind.REAL_FORK, "tests/./intents/base/test_swap.py", "canonical"),
        (EvidenceKind.REAL_FORK, "tests//intents/base/test_swap.py", "canonical"),
        (EvidenceKind.REAL_FORK, "tests/intents/base/test_swap.py/", "canonical"),
    ],
)
def test_repository_evidence_rejects_placeholders_escape_mismatch_and_missing_files(
    tmp_path: Path,
    kind: EvidenceKind,
    ref: str,
    match: str,
) -> None:
    with pytest.raises(EvidenceIntegrityError, match=match):
        validate_evidence_ref(EvidenceRef(kind, ref), repo_root=tmp_path)


def test_repository_evidence_rejects_directory_and_symlink_escape(tmp_path: Path) -> None:
    directory_ref = "tests/intents/base/directory.py"
    (tmp_path / directory_ref).mkdir(parents=True)
    with pytest.raises(EvidenceIntegrityError, match="does not exist"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.REAL_FORK, directory_ref), repo_root=tmp_path)

    outside = tmp_path.parent / "outside.py"
    outside.write_text("outside\n", encoding="utf-8")
    link = tmp_path / "tests/intents/base/link.py"
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(outside)
    with pytest.raises(EvidenceIntegrityError, match="escapes the repository"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.REAL_FORK, str(link.relative_to(tmp_path))), repo_root=tmp_path)


def test_repository_evidence_rejects_symlink_into_a_different_typed_root(tmp_path: Path) -> None:
    target = tmp_path / "scripts/ci/test_generated.py"
    _write(tmp_path, str(target.relative_to(tmp_path)))
    link = tmp_path / "tests/intents/base/test_link.py"
    link.parent.mkdir(parents=True)
    link.symlink_to(target)

    with pytest.raises(EvidenceIntegrityError, match="resolves outside its permitted roots"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.REAL_FORK, str(link.relative_to(tmp_path))), repo_root=tmp_path)


def test_repository_evidence_rejects_symlink_to_a_different_suffix_in_the_same_root(tmp_path: Path) -> None:
    target = tmp_path / "tests/data/fixture.json"
    target.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")
    link = tmp_path / "tests/unit/test_evidence.py"
    link.parent.mkdir(parents=True)
    link.symlink_to(target)

    with pytest.raises(EvidenceIntegrityError, match="different file suffix"):
        validate_evidence_ref(
            EvidenceRef(EvidenceKind.CONTRACT_TEST, str(link.relative_to(tmp_path))), repo_root=tmp_path
        )


def test_repository_evidence_normalizes_symlink_resolution_errors(tmp_path: Path) -> None:
    link = tmp_path / "tests/unit/test_loop.py"
    link.parent.mkdir(parents=True)
    link.symlink_to(link)

    with pytest.raises(EvidenceIntegrityError, match="path cannot be resolved"):
        validate_evidence_ref(
            EvidenceRef(EvidenceKind.CONTRACT_TEST, str(link.relative_to(tmp_path))), repo_root=tmp_path
        )


def test_repository_evidence_requires_exact_path_casing(tmp_path: Path) -> None:
    _write(tmp_path, "tests/unit/test_case.py")

    with pytest.raises(EvidenceIntegrityError, match="exact casing"):
        validate_evidence_ref(
            EvidenceRef(EvidenceKind.CONTRACT_TEST, "tests/Unit/test_case.py"),
            repo_root=tmp_path,
        )


@pytest.mark.parametrize(
    "ref",
    [
        "tests/__init__.py",
        "tests/intents/helper.py",
        "tests/intents/test_empty.py",
    ],
)
def test_python_evidence_requires_a_real_test_module(tmp_path: Path, ref: str) -> None:
    path = tmp_path / ref
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# not a test\n", encoding="utf-8")

    with pytest.raises(EvidenceIntegrityError, match=r"test_\*\.py|contains no test function"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.CONTRACT_TEST, ref), repo_root=tmp_path)


@pytest.mark.parametrize(
    "content",
    [
        "def helper():\n    def test_nested():\n        pass\n",
        "class Helper:\n    def test_not_collected(self):\n        pass\n",
        "if False:\n    def test_unreachable():\n        pass\n",
    ],
)
def test_python_evidence_rejects_non_collectable_test_functions(tmp_path: Path, content: str) -> None:
    ref = "tests/unit/test_inert.py"
    path = tmp_path / ref
    path.parent.mkdir(parents=True)
    path.write_text(content, encoding="utf-8")

    with pytest.raises(EvidenceIntegrityError, match="contains no test function"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.CONTRACT_TEST, ref), repo_root=tmp_path)


def test_python_evidence_accepts_test_class_method(tmp_path: Path) -> None:
    ref = "tests/unit/test_contract.py"
    path = tmp_path / ref
    path.parent.mkdir(parents=True)
    path.write_text("class TestContract:\n    def test_evidence(self):\n        pass\n", encoding="utf-8")

    validate_evidence_ref(EvidenceRef(EvidenceKind.CONTRACT_TEST, ref), repo_root=tmp_path)


@pytest.mark.parametrize(
    "ref",
    [
        "docs/internal/reports/narrative.md",
        "scripts/ci/lifecycle-capability-baseline.json",
    ],
)
def test_generated_matrix_requires_an_explicit_artifact_validator(tmp_path: Path, ref: str) -> None:
    _write(tmp_path, ref)

    with pytest.raises(EvidenceIntegrityError, match="no approved artifact validator"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.GENERATED_MATRIX, ref), repo_root=tmp_path)


@pytest.mark.parametrize(
    "ref",
    [
        "http://example.com/evidence",
        "https:///missing-host",
        "https://user:secret@example.com/evidence",
        "tests/reports/hosted.json",
    ],
)
def test_hosted_contract_requires_explicit_credential_free_https_url(ref: str) -> None:
    with pytest.raises(EvidenceIntegrityError, match="HTTPS URL without credentials, query, or fragment"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.HOSTED_CONTRACT, ref))


def test_hosted_contract_normalizes_malformed_url_errors() -> None:
    with pytest.raises(EvidenceIntegrityError, match="URL is malformed"):
        validate_evidence_ref(EvidenceRef(EvidenceKind.HOSTED_CONTRACT, "https://[::1/evidence"))


@pytest.mark.parametrize(
    "ref",
    [
        "https://evidence.example/contracts/run-123",
        "https://evidence.example/contracts/run-123?token=secret",
        "https://evidence.example/contracts/run-123#mutable",
    ],
)
def test_hosted_contract_requires_an_explicit_artifact_validator(ref: str) -> None:
    match = "query, or fragment" if "?" in ref or "#" in ref else "no approved artifact validator"
    with pytest.raises(EvidenceIntegrityError, match=match):
        validate_evidence_ref(EvidenceRef(EvidenceKind.HOSTED_CONTRACT, ref))


def test_current_sealed_production_evidence_passes_the_central_policy() -> None:
    summary = validate_matrix_evidence(build_production_core_execution_matrix())

    assert summary.satisfied_obligations == 838
    assert summary.evidence_references == 869


def test_current_registered_matrix_evidence_passes_the_ci_policy() -> None:
    summary = validate_matrix_evidence(build_effective_capability_matrix())

    assert summary.satisfied_obligations == 838
    assert summary.evidence_references == 869


def test_matrix_evidence_validation_opens_no_network_or_provider_imports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matrix = build_production_core_execution_matrix()

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("evidence integrity must remain offline")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(ImportRef, "load", forbidden)

    assert validate_matrix_evidence(matrix).evidence_references == 869


def test_lifecycle_ratchet_entrypoint_runs_evidence_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def fail_from_integrity(_matrix: object) -> None:
        nonlocal called
        called = True
        raise EvidenceIntegrityError("sentinel invalid reference")

    monkeypatch.setattr(ratchet, "validate_matrix_evidence", fail_from_integrity)

    assert ratchet.main([]) == 1
    assert called
