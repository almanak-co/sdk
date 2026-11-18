import base64
import hashlib
import zipfile
from pathlib import Path

import pytest

from scripts import build_platform_wheels as builder

BINARY = "almanak/bin/almanak-code-linux-x86_64"


def synthetic_wheel(directory: Path) -> Path:
    """A minimal almanak-3.0.0 wheel carrying one executable member."""
    path = directory / "almanak-3.0.0-py3-none-any.whl"
    members = {
        "almanak/__init__.py": b"",
        BINARY: b"\x7fELF binary",
        "almanak-3.0.0.dist-info/METADATA": b"Name: almanak\n",
        "almanak-3.0.0.dist-info/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: hatchling\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        "almanak-3.0.0.dist-info/RECORD": b"almanak-3.0.0.dist-info/RECORD,,\n",
    }
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            info = zipfile.ZipInfo(name)
            info.external_attr = (0o755 if name == BINARY else 0o644) << 16
            archive.writestr(info, data)
    return path


def test_retag_rewrites_the_tag_and_the_record(tmp_path):
    output = tmp_path / "dist"
    result = builder.retag_wheel(
        synthetic_wheel(tmp_path), "py3-none-manylinux_2_17_x86_64", output
    )
    assert result.name == "almanak-3.0.0-py3-none-manylinux_2_17_x86_64.whl"

    with zipfile.ZipFile(result) as archive:
        wheel = archive.read("almanak-3.0.0.dist-info/WHEEL").decode()
        tags = [line for line in wheel.splitlines() if line.startswith("Tag:")]
        assert tags == ["Tag: py3-none-manylinux_2_17_x86_64"]
        assert "Generator: hatchling" in wheel

        rows = archive.read("almanak-3.0.0.dist-info/RECORD").decode().splitlines()
        assert "almanak-3.0.0.dist-info/RECORD,," in rows
        for row in rows:
            name, digest, size = row.split(",")
            if not digest:
                continue
            data = archive.read(name)
            expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
            assert digest == f"sha256={expected.decode()}"
            assert int(size) == len(data)


def test_retag_preserves_the_executable_bit(tmp_path):
    result = builder.retag_wheel(synthetic_wheel(tmp_path), "py3-none-win_amd64", tmp_path / "out")
    with zipfile.ZipFile(result) as archive:
        modes = {info.filename: info.external_attr >> 16 for info in archive.infolist()}
    assert modes[BINARY] & 0o111
    assert not modes["almanak-3.0.0.dist-info/METADATA"] & 0o111


def test_record_digests_are_urlsafe_and_unpadded():
    digest = builder.record_digest(b"\xfb\xff\xfe payload")
    assert "=" not in digest
    assert "+" not in digest
    assert "/" not in digest


def test_retag_rejects_a_wheel_without_wheel_metadata(tmp_path):
    path = tmp_path / "almanak-3.0.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("almanak/__init__.py", b"")
    with pytest.raises(SystemExit):
        builder.retag_wheel(path, "py3-none-win_amd64", tmp_path / "out")


def test_every_platform_maps_to_a_distinct_tag_and_binary():
    tags = [tag for tag, _ in builder.PLATFORMS.values()]
    binaries = [binary for _, binary in builder.PLATFORMS.values()]
    assert len(builder.PLATFORMS) == 5
    assert len(set(tags)) == len(tags)
    assert len(set(binaries)) == len(binaries)
    assert all(tag.startswith("py3-none-") for tag in tags)


def test_platform_binaries_match_the_runtime_resolver():
    """The wheel builder and the CLI must agree on every bundled filename."""
    from almanak import native

    expected = {
        "darwin-arm64": ("Darwin", "arm64"),
        "darwin-x86_64": ("Darwin", "x86_64"),
        "linux-arm64": ("Linux", "aarch64"),
        "linux-x86_64": ("Linux", "x86_64"),
        "windows-x86_64": ("Windows", "AMD64"),
    }
    for key, (system, machine) in expected.items():
        assert builder.PLATFORMS[key][1] == native.binary_name(system, machine)


def test_a_platform_without_a_downloaded_binary_fails_the_build(tmp_path, monkeypatch):
    """A partial binary set must not quietly become a partial wheel set."""
    monkeypatch.setattr(builder, "STASH_DIR", tmp_path / "stash")
    (tmp_path / "stash").mkdir()
    with pytest.raises(SystemExit) as failure:
        builder.build_one("linux-x86_64", tmp_path / "dist")
    assert "linux-x86_64" in str(failure.value)
    assert "almanak-code-linux-x86_64" in str(failure.value)


def test_a_stash_from_an_interrupted_build_is_never_overwritten(tmp_path, monkeypatch):
    """The stash can hold the only copy of the other platforms' binaries."""
    bin_dir, stash = tmp_path / "bin", tmp_path / "stash"
    bin_dir.mkdir()
    stash.mkdir()
    (stash / "almanak-code-linux-arm64").write_bytes(b"the only copy")
    monkeypatch.setattr(builder, "BIN_DIR", bin_dir)
    monkeypatch.setattr(builder, "STASH_DIR", stash)

    with pytest.raises(SystemExit):
        builder.stash_binaries()

    assert (stash / "almanak-code-linux-arm64").read_bytes() == b"the only copy"


def test_a_partial_binary_set_publishes_no_wheels(tmp_path, monkeypatch):
    """The whole run fails, and it leaves no wheels a publish target could upload."""
    bin_dir, dist = tmp_path / "bin", tmp_path / "dist"
    bin_dir.mkdir()
    (bin_dir / ".gitkeep").write_bytes(b"")
    present = [binary for _, binary in builder.PLATFORMS.values()][:-1]
    for binary in present:
        (bin_dir / binary).write_bytes(b"payload")
    monkeypatch.setattr(builder, "BIN_DIR", bin_dir)
    monkeypatch.setattr(builder, "STASH_DIR", tmp_path / "stash")
    monkeypatch.setattr(builder, "STAGING_DIR", tmp_path / "staging")
    monkeypatch.setattr(builder, "DIST_DIR", dist)
    monkeypatch.setattr(builder, "build_generic_wheel", lambda: synthetic_wheel(tmp_path))

    with pytest.raises(SystemExit):
        builder.build_platform_wheels(list(builder.PLATFORMS))

    assert list(dist.glob("*.whl")) == []
    assert sorted(p.name for p in bin_dir.iterdir()) == sorted([".gitkeep", *present])


def test_binaries_are_restored_after_a_failing_build(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / ".gitkeep").write_bytes(b"")
    for _, binary in builder.PLATFORMS.values():
        (bin_dir / binary).write_bytes(b"payload")
    before = sorted(path.name for path in bin_dir.iterdir())

    monkeypatch.setattr(builder, "BIN_DIR", bin_dir)
    monkeypatch.setattr(builder, "STASH_DIR", tmp_path / "stash")
    monkeypatch.setattr(builder, "STAGING_DIR", tmp_path / "staging")
    monkeypatch.setattr(builder, "DIST_DIR", tmp_path / "dist")

    def explode() -> Path:
        raise SystemExit("wheel build failed")

    monkeypatch.setattr(builder, "build_generic_wheel", explode)
    with pytest.raises(SystemExit):
        builder.build_platform_wheels(["linux-x86_64"])

    assert sorted(path.name for path in bin_dir.iterdir()) == before
    assert not (tmp_path / "stash").exists()
    assert not (tmp_path / "staging").exists()
