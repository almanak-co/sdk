"""A self-consistent archive must still match the exact release source."""

import io
import json
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest
import yaml

from almanak import references
from scripts.build_platform_wheels import PLATFORMS, rebuild_record
from scripts.ci import release_version, smoke_sdk_distribution, verify_sdk_distribution

VERSION = "3.0.0rc2"
PREFIX = f"almanak-{VERSION}.dist-info/"
METADATA = (
    f"Name: almanak\nVersion: {VERSION}\nRequires-Python: >=3.12\n"
    "License-Expression: Apache-2.0\nLicense-File: LICENSE\n"
).encode()
LICENSE_TEXT = b"Apache License\n"


def write_wheel(path, members):
    rebuild_record(members, PREFIX.rstrip("/"))
    with zipfile.ZipFile(path, "w") as wheel:
        for name, data in members.items():
            info = zipfile.ZipInfo(name)
            info.external_attr = (0o100755 if "/bin/almanak-code-" in name else 0o100644) << 16
            wheel.writestr(info, data)


def write_sdist(path, members):
    with tarfile.open(path, "w:gz") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(f"almanak-{VERSION}/{name}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


@pytest.fixture
def release(tmp_path, monkeypatch):
    # The verifier and smoke check read the source's allowlist; "hidden" is not on it.
    monkeypatch.setattr(references, "PUBLISHED", frozenset({"demo"}))
    root = tmp_path / "source"
    root.mkdir()
    source = {
        "pyproject.toml": (
            f'[project]\nname = "almanak"\nversion = "{VERSION}"\nrequires-python = ">=3.12"\n'
            'license = "Apache-2.0"\nlicense-files = ["LICENSE"]\n'
        ).encode(),
        "LICENSE": LICENSE_TEXT,
        "src/almanak/__init__.py": b'"""SDK."""\n',
        "src/almanak/cli.py": b"def main(): return 0\n",
        "src/almanak/bin/.gitkeep": b"",
        "README.md": b"SDK release documentation\n",
        "docs/schemas/strategy.schema.json": b'{"$schema":"schema","$ref":"nested/a.schema.json"}',
        "docs/schemas/nested/a.schema.json": b'{"type":"object"}',
        "scripts/hatch_build.py": b'"""Build hook."""\n',
        "strategies/README.md": b"References\n",
        "strategies/demo/strategy.py": b"def decide(context): return None\n",
        "strategies/demo/config.yaml": b"markets: {}\n",
        "strategies/demo/backtest-economic.yaml": b"mode: economic\n",
        "strategies/demo/observed-actions.json": b"[]",
        "strategies/demo/config.production.yaml": b"markets: {}\n",
        "strategies/hidden/strategy.py": b"def decide(context): return None\n",
        "strategies/hidden/config.yaml": b"markets: {}\n",
        "strategies/hidden/README.md": b"# Hidden\n",
    }
    for name, data in source.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    dist = root / "dist"
    dist.mkdir()
    (dist / ".gitignore").write_text("*")
    expected = verify_sdk_distribution.package_files(root)
    for tag, binary in PLATFORMS.values():
        payload = b"binary-" + binary.encode()
        (root / "src/almanak/bin" / binary).write_bytes(payload)
        members = {
            **expected,
            f"almanak/bin/{binary}": payload,
            PREFIX + "METADATA": METADATA,
            PREFIX + "licenses/LICENSE": LICENSE_TEXT,
            PREFIX + "WHEEL": f"Wheel-Version: 1.0\nTag: {tag}\n".encode(),
            PREFIX + "entry_points.txt": b"[console_scripts]\nalmanak = almanak.cli:main\n",
        }
        write_wheel(dist / f"almanak-{VERSION}-{tag}.whl", members)
    source["PKG-INFO"] = METADATA
    write_sdist(dist / f"almanak-{VERSION}.tar.gz", source)
    return root, dist


def test_complete_release_is_bound_to_source_and_hashed(release, tmp_path):
    root, dist = release
    report = tmp_path / "manifest.json"
    assert (
        verify_sdk_distribution.main(
            ["--root", str(root), "--dist", str(dist), "--output", str(report)]
        )
        == 0
    )
    result = json.loads(report.read_text())
    assert result["version"] == VERSION
    assert len(result["sha256"]) == 6
    assert all(len(value) == 64 for value in result["sha256"].values())


@pytest.mark.parametrize(
    "member,value",
    [
        ("almanak/cli.py", b"def main(): return 99\n"),
        ("almanak/schemas/files/strategy.schema.json", b'{"type":"string"}'),
        ("almanak/unreviewed.py", b"unexpected source"),
        (PREFIX + "METADATA", b"Name: almanak\nVersion: 3.0.0\n"),
        (PREFIX + "METADATA", METADATA + b"Requires-Dist: unrelated>=1\n"),
        (PREFIX + "METADATA", METADATA.replace(b">=3.12", b">=3.11")),
        (PREFIX + "WHEEL", b"Wheel-Version: 1.0\nTag: py3-none-any\n"),
        (PREFIX + "entry_points.txt", b"[console_scripts]\nalmanak = unrelated:main\n"),
        (PREFIX + "unexpected.json", b"{}"),
        (PREFIX + "licenses/LICENSE", b"Proprietary\n"),
        (PREFIX + "METADATA", METADATA.replace(b"Apache-2.0", b"MIT")),
        (PREFIX + "METADATA", METADATA.replace(b"License-File: LICENSE\n", b"")),
    ],
)
def test_rehashed_stale_or_changed_wheel_is_refused(release, member, value):
    root, dist = release
    path = next(dist.glob("*.whl"))
    with zipfile.ZipFile(path) as wheel:
        members = {name: wheel.read(name) for name in wheel.namelist()}
    members[member] = value
    write_wheel(path, members)
    with pytest.raises(ValueError):
        verify_sdk_distribution.verify(root, dist)


def test_wheel_missing_nested_schema_is_refused(release):
    root, dist = release
    path = next(dist.glob("*.whl"))
    with zipfile.ZipFile(path) as wheel:
        members = {name: wheel.read(name) for name in wheel.namelist()}
    del members["almanak/schemas/files/nested/a.schema.json"]
    write_wheel(path, members)
    with pytest.raises(ValueError, match="layout"):
        verify_sdk_distribution.verify(root, dist)


def test_wheel_carries_only_reference_author_files(release):
    root, dist = release
    shipped = {
        name
        for name in verify_sdk_distribution.package_files(root)
        if name.startswith("almanak/references/")
    }
    assert shipped == {
        "almanak/references/files/demo/strategy.py",
        "almanak/references/files/demo/config.yaml",
        "almanak/references/files/demo/backtest-economic.yaml",
    }
    path = next(dist.glob("*.whl"))
    with zipfile.ZipFile(path) as wheel:
        members = {name: wheel.read(name) for name in wheel.namelist()}
    hidden = {**members, "almanak/references/files/hidden/config.yaml": b"markets: {}\n"}
    write_wheel(path, hidden)
    with pytest.raises(ValueError, match="layout"):
        verify_sdk_distribution.verify(root, dist)
    evidence = {**members, "almanak/references/files/demo/observed-actions.json": b"[]"}
    write_wheel(path, evidence)
    with pytest.raises(ValueError, match="layout"):
        verify_sdk_distribution.verify(root, dist)
    del members["almanak/references/files/demo/config.yaml"]
    write_wheel(path, members)
    with pytest.raises(ValueError, match="layout"):
        verify_sdk_distribution.verify(root, dist)


def test_sdist_must_carry_the_reference_build_inputs(release):
    root, dist = release
    path = dist / f"almanak-{VERSION}.tar.gz"
    members = verify_sdk_distribution.sdist_files(path, VERSION)
    for name in ("scripts/hatch_build.py", "strategies/demo/strategy.py"):
        partial = {key: value for key, value in members.items() if key != name}
        write_sdist(path, partial)
        with pytest.raises(ValueError, match="differ"):
            verify_sdk_distribution.verify_sdist(root, dist, VERSION)


def test_generated_llms_bundle_is_bound_to_source(release):
    root, dist = release
    bundle = b"v3 documentation bundle\n"
    (root / "src/almanak/llms").mkdir()
    (root / "src/almanak/llms/llms-full.txt").write_bytes(bundle)
    for path in dist.glob("*.whl"):
        with zipfile.ZipFile(path) as wheel:
            members = {name: wheel.read(name) for name in wheel.namelist()}
        members["almanak/llms/llms-full.txt"] = bundle
        write_wheel(path, members)
    verify_sdk_distribution.verify(root, dist)

    with zipfile.ZipFile(next(dist.glob("*.whl"))) as wheel:
        members = {name: wheel.read(name) for name in wheel.namelist()}
    members["almanak/llms/llms-full.txt"] = b"stale bundle"
    write_wheel(next(dist.glob("*.whl")), members)
    with pytest.raises(ValueError, match="differ"):
        verify_sdk_distribution.verify(root, dist)


@pytest.mark.parametrize(
    "member,value",
    [
        ("src/almanak/cli.py", b"changed source"),
        ("README.md", b"changed non-package source"),
        ("src/almanak/bin/almanak-code-linux-x86_64", b"unexpected binary"),
        ("gha-creds-example.json", b"credential"),
        ("PKG-INFO", b"Name: almanak\nVersion: 3.0.0\n"),
        ("PKG-INFO", METADATA + b"Requires-Dist: unrelated>=1\n"),
        ("PKG-INFO", METADATA.replace(b">=3.12", b">=3.11")),
    ],
)
def test_changed_or_contaminated_sdist_is_refused(release, member, value):
    root, dist = release
    path = dist / f"almanak-{VERSION}.tar.gz"
    members = verify_sdk_distribution.sdist_files(path, VERSION)
    members[member] = value
    write_sdist(path, members)
    with pytest.raises(ValueError):
        verify_sdk_distribution.verify(root, dist)


@pytest.mark.parametrize("binary", [binary for _, binary in PLATFORMS.values()])
@pytest.mark.parametrize("included", [False, True], ids=["excluded", "included"])
def test_sdist_excludes_tracked_binaries(release, binary, included):
    root, dist = release
    name = f"src/almanak/bin/{binary}"
    subprocess.run(["git", "add", name], cwd=root, check=True)
    path = dist / f"almanak-{VERSION}.tar.gz"
    if included:
        members = verify_sdk_distribution.sdist_files(path, VERSION)
        members[name] = (root / name).read_bytes()
        write_sdist(path, members)
        with pytest.raises(ValueError, match="sdist must not contain bundled platform binaries"):
            verify_sdk_distribution.verify_sdist(root, dist, VERSION)
    else:
        verify_sdk_distribution.verify_sdist(root, dist, VERSION)


def test_partial_release_cannot_be_published(release):
    root, dist = release
    next(dist.glob("*.whl")).unlink()
    with pytest.raises(ValueError, match="five platform wheels"):
        verify_sdk_distribution.verify(root, dist)


@pytest.mark.parametrize("tag", ["v3.0.0rc2", "3.0.0-rc2", "v3.0.0-rc2"])
def test_rc_tag_spellings_share_one_canonical_version(tag):
    assert release_version.candidate_version(tag, "3.0.0") == VERSION


@pytest.mark.parametrize(
    "tag", ["v3.0.0rc0", "v3.0.0rc02", "v3.0.0", "v3.1.0rc2", "v3.0.0rc2+local"]
)
def test_invalid_or_wrong_release_line_tag_is_refused(tag):
    with pytest.raises(ValueError):
        release_version.candidate_version(tag, "3.0.0")


def test_rc_guard_reads_the_project_release_line(release, capsys):
    root, _ = release
    project = root / "pyproject.toml"
    project.write_text('[project]\nversion = "3.0.0"\n')
    assert release_version.main(["v3.0.0rc2", "--project", str(project)]) == 0
    assert capsys.readouterr().out == "3.0.0rc2\n"


def test_installed_bundle_refuses_missing_references_and_stale_schemas(release):
    root, _ = release
    schemas = root / "docs/schemas"
    files = {
        p.relative_to(schemas).as_posix(): json.loads(p.read_text())
        for p in schemas.rglob("*.schema.json")
    }
    document = {"root": "strategy.schema.json", "files": files}
    smoke_sdk_distribution.verify_bundle(document, "strategy", schemas)
    files["nested/a.schema.json"] = {"type": "string"}
    with pytest.raises(ValueError, match="differs"):
        smoke_sdk_distribution.verify_bundle(document, "strategy", schemas)
    del files["nested/a.schema.json"]
    with pytest.raises(ValueError, match="missing"):
        smoke_sdk_distribution.verify_bundle(document, "strategy", schemas)


WORKFLOWS = Path(__file__).resolve().parents[1] / ".github/workflows"
# CI must not depend on Google Cloud for releases or for the gates that guard them.
GOOGLE_CLOUD = ("google-github-actions", "gcloud", "kubectl", "pkg.dev", "id-token")


def workflow(name):
    return yaml.load((WORKFLOWS / name).read_text(), Loader=yaml.BaseLoader)


def test_no_workflow_or_make_target_reaches_google_cloud():
    root = WORKFLOWS.parents[1]
    files = [*root.joinpath(".github").rglob("*.yml"), root / "Makefile"]
    found = {
        f"{path.relative_to(root)}: {marker}"
        for path in files
        for marker in GOOGLE_CLOUD
        if marker in path.read_text()
    }
    assert found == set()


def test_stable_release_uploads_its_verified_build_to_pypi():
    jobs = workflow("release_tag.yml")["jobs"]
    build = jobs["build_dist"]
    assert build["uses"].endswith("/template_build_dist.yml")
    gates = {"lint", "typecheck", "test_linux", "test_macos", "test_strat"}
    assert gates | {"test_pytest", "docs", "extract_version"} <= set(build["needs"])
    publish = jobs["publish_pypi"]
    assert publish["uses"].endswith("/template_publish_pypi.yml")
    assert set(publish["needs"]) == {"extract_version", "build_dist"}
    # PyPI is the only destination; nothing else uploads or deploys.
    assert [name for name in jobs if "publish" in name] == ["publish_pypi"]
    pypi = workflow("template_publish_pypi.yml")["jobs"]["publish"]["steps"]
    assert any(step.get("with", {}).get("name") == "sdk-dist" for step in pypi)
    builder = workflow("template_build_dist.yml")["jobs"]["build"]["steps"]
    assert any(step.get("with", {}).get("name") == "sdk-dist" for step in builder)


def test_release_candidates_build_and_publish_nowhere():
    jobs = workflow("release_candidate.yml")["jobs"]
    assert jobs["build_dist"]["uses"].endswith("/template_build_dist.yml")
    assert not [name for name in jobs if "publish" in name or "notify" in name]
    assert "template_publish_pypi" not in (WORKFLOWS / "release_candidate.yml").read_text()


def test_no_release_job_hands_off_to_another_repository():
    for name in ("release_tag.yml", "release_candidate.yml"):
        text = (WORKFLOWS / name).read_text()
        assert "dispatches" not in text and "notify" not in text


def test_installed_references_must_match_the_release_source(release, tmp_path):
    root, _ = release
    installed = tmp_path / "site-packages/almanak/references/files"
    (installed / "demo").mkdir(parents=True)
    for name in ("strategy.py", "config.yaml"):
        (installed / "demo" / name).write_bytes((root / "strategies/demo" / name).read_bytes())
    entry = {"name": "demo", "path": str(installed / "demo"), "files": ["config.yaml"]}
    document = {"directory": str(installed), "references": [entry]}
    smoke_sdk_distribution.verify_references(document, root)
    (installed / "demo/config.yaml").write_bytes(b"markets: {changed: {}}\n")
    with pytest.raises(ValueError, match="differs from source"):
        smoke_sdk_distribution.verify_references(document, root)
    with pytest.raises(ValueError, match="listing differs"):
        smoke_sdk_distribution.verify_references({**document, "references": []}, root)
    hidden = {"name": "hidden", "path": str(installed / "hidden"), "files": []}
    with pytest.raises(ValueError, match="listing differs"):
        smoke_sdk_distribution.verify_references({**document, "references": [entry, hidden]}, root)
    checkout = {**document, "directory": str(root / "strategies")}
    with pytest.raises(ValueError, match="checkout"):
        smoke_sdk_distribution.verify_references(checkout, root)
