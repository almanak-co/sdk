"""Verify the complete SDK release against source before Google authentication."""

import argparse
import hashlib
import json
import subprocess
import tarfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

from almanak.references import is_author_file, is_published
from scripts.build_platform_wheels import PLATFORMS
from scripts.ci.distribution_metadata import verify_project_metadata
from scripts.ci.wheel_verification import (
    read_entry_points,
    verify_members,
    verify_metadata_identity,
    verify_path,
    verify_record,
)

# Tracked trees the wheel carries: the package, its schemas and reference strategies.
PACKAGED_SOURCES = ("src/almanak", "docs/schemas", "strategies")


def tracked_files(root: Path, *paths: str) -> dict[str, bytes]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", *paths],
        cwd=root,
        check=True,
        capture_output=True,
    )
    names = result.stdout.decode().strip("\0").split("\0")
    found = {}
    for name in filter(None, names):
        path = root / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"release source is not a regular file: {name}")
        found[name] = path.read_bytes()
    return found


def wheel_name(name: str) -> str | None:
    """Where a tracked source file lands in the wheel, or None when it is not shipped."""
    if name.startswith("docs/schemas/"):
        return "almanak/schemas/files/" + name.removeprefix("docs/schemas/")
    if name.startswith("strategies/"):
        # The build hook ships only author files directly inside a published reference.
        parts = name.split("/")
        if len(parts) == 3 and is_published(parts[1]) and is_author_file(parts[2]):
            return f"almanak/references/files/{parts[1]}/{parts[2]}"
        return None
    return name.removeprefix("src/")


def package_files(root: Path) -> dict[str, bytes]:
    sources = tracked_files(root, *PACKAGED_SOURCES)
    expected = {}
    for name, data in sources.items():
        destination = wheel_name(name)
        if destination is not None:
            expected[destination] = data
    if "almanak/__init__.py" not in expected:
        raise ValueError("SDK release source is missing")
    return expected


def verify_inventory(dist: Path, version: str) -> list[Path]:
    expected = {f"almanak-{version}-{tag}.whl" for tag, _ in PLATFORMS.values()}
    expected.add(f"almanak-{version}.tar.gz")
    marker = dist / ".gitignore"
    if marker.exists():
        if marker.is_symlink() or not marker.is_file() or marker.read_bytes() != b"*":
            raise ValueError("unexpected distribution marker")
    artifacts = [path for path in dist.iterdir() if path.name != ".gitignore"]
    if {path.name for path in artifacts} != expected:
        raise ValueError("release must contain exactly five platform wheels and one sdist")
    if any(not path.is_file() or path.is_symlink() for path in artifacts):
        raise ValueError("release distributions must be regular files")
    return sorted(artifacts)


def license_files(root: Path, project: dict) -> dict[str, bytes]:
    patterns = project.get("license-files", [])
    names = sorted(
        {str(path.relative_to(root)) for pattern in patterns for path in root.glob(pattern)}
    )
    if not names:
        raise ValueError("project declares no license files")
    return {name: (root / name).read_bytes() for name in names}


def verify_license_metadata(metadata, project: dict, names: list[str]) -> None:
    if metadata.get_all("License-Expression") != [project["license"]]:
        raise ValueError("distribution License-Expression differs from the project")
    if sorted(metadata.get_all("License-File", [])) != sorted(names):
        raise ValueError("distribution License-File entries differ from the project")


def verify_payload(
    wheel: zipfile.ZipFile, names: set[str], expected: dict[str, bytes], prefix: str
) -> None:
    metadata = {prefix + name for name in ("METADATA", "WHEEL", "entry_points.txt", "RECORD")}
    if names != set(expected) | metadata:
        raise ValueError("wheel contains files outside the SDK release layout")
    for name, data in expected.items():
        if wheel.read(name) != data:
            raise ValueError(f"wheel source bytes differ from checkout: {name}")


def verify_wheel(root: Path, dist: Path, version: str, tag: str, binary: str) -> None:
    expected = package_files(root)
    name = f"almanak/bin/{binary}"
    expected[name] = (root / "src" / name).read_bytes()
    # Generated at publish time and gitignored like the binaries; when the
    # publish workflow has bundled it, the wheel must carry exactly these bytes.
    llms_bundle = root / "src" / "almanak" / "llms" / "llms-full.txt"
    if llms_bundle.exists():
        expected["almanak/llms/llms-full.txt"] = llms_bundle.read_bytes()
    prefix = f"almanak-{version}.dist-info/"
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    licenses = license_files(root, project)
    # PEP 639: hatchling copies each declared license file to dist-info/licenses/.
    for license_name, data in licenses.items():
        expected[f"{prefix}licenses/{license_name}"] = data
    with zipfile.ZipFile(dist / f"almanak-{version}-{tag}.whl") as wheel:
        names = verify_members(wheel)
        metadata = verify_metadata_identity(wheel, prefix, "almanak", version)
        verify_project_metadata(metadata, project)
        verify_license_metadata(metadata, project, list(licenses))
        tags = [
            line
            for line in wheel.read(prefix + "WHEEL").decode().splitlines()
            if line.startswith("Tag:")
        ]
        if tags != [f"Tag: {tag}"]:
            raise ValueError("wheel platform tag differs from its release filename")
        if not wheel.getinfo(name).external_attr >> 16 & 0o111:
            raise ValueError("bundled binary is not executable")
        if dict(read_entry_points(wheel, prefix)["console_scripts"]) != {
            "almanak": "almanak.cli:main"
        }:
            raise ValueError("SDK console entry point differs from the release contract")
        verify_payload(wheel, names, expected, prefix)
        verify_record(wheel, prefix, names)


def sdist_files(path: Path, version: str) -> dict[str, bytes]:
    prefix = f"almanak-{version}/"
    found = {}
    with tarfile.open(path) as archive:
        for member in archive:
            verify_path(member.name)
            if member.isdir():
                continue
            if not member.isfile() or not member.name.startswith(prefix):
                raise ValueError(f"invalid sdist member: {member.name}")
            name = member.name.removeprefix(prefix)
            if name in found:
                raise ValueError(f"duplicate sdist member: {name}")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError(f"unreadable sdist member: {name}")
            found[name] = stream.read()
    return found


def verify_sdist(root: Path, dist: Path, version: str) -> None:
    files = sdist_files(dist / f"almanak-{version}.tar.gz", version)
    binaries = {f"src/almanak/bin/{binary}" for _, binary in PLATFORMS.values()}
    # Tracking a bundled binary must not weaken the platform-neutral sdist contract.
    if set(files) & binaries:
        raise ValueError("sdist must not contain bundled platform binaries")
    source = tracked_files(root)
    if set(files) - set(source) - {"PKG-INFO"}:
        raise ValueError("sdist contains files outside the tracked release source")
    for name, data in files.items():
        if name != "PKG-INFO" and data != source[name]:
            raise ValueError(f"sdist source bytes differ from checkout: {name}")
    # A wheel built from the sdist runs the same hook, so its inputs must be present.
    required = tracked_files(root, *PACKAGED_SOURCES, "pyproject.toml", "scripts/hatch_build.py")
    for name in binaries:
        required.pop(name, None)
    for name, data in required.items():
        if files.get(name) != data:
            raise ValueError(f"sdist source bytes differ from checkout: {name}")
    metadata = BytesParser().parsebytes(files["PKG-INFO"])
    if metadata.get_all("Name") != ["almanak"] or metadata.get_all("Version") != [version]:
        raise ValueError("sdist name or version does not match the requested release")
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    verify_project_metadata(metadata, project)
    licenses = license_files(root, project)
    verify_license_metadata(metadata, project, list(licenses))
    for name, data in licenses.items():
        if files.get(name) != data:
            raise ValueError(f"sdist is missing license file: {name}")
    if tomllib.loads(files["pyproject.toml"].decode())["project"]["version"] != version:
        raise ValueError("sdist project version does not match the requested release")


def verify(root: Path, dist: Path) -> dict[str, object]:
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    artifacts = verify_inventory(dist, version)
    for tag, binary in PLATFORMS.values():
        verify_wheel(root, dist, version, tag, binary)
    verify_sdist(root, dist, version)
    hashes = {}
    for path in artifacts:
        with path.open("rb") as stream:
            hashes[path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"version": version, "sha256": hashes}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    parser.add_argument("--output", type=Path, default=Path("release-manifest.json"))
    args = parser.parse_args(argv)
    result = verify(args.root.resolve(), args.dist.resolve())
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Verified all six SDK distributions for {result['version']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
