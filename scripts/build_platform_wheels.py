"""Build one wheel per platform, each carrying only its own Almanak Code binary.

A single wheel holding every binary would be several hundred megabytes. The
package is pure Python, so each wheel is built generically and then retagged:
hatchling has no notion of these platforms and would tag them all `py3-none-any`.

Usage:
    uv run python scripts/build_platform_wheels.py
    uv run python scripts/build_platform_wheels.py --platform linux-x86_64
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import zipfile
from base64 import urlsafe_b64encode
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BIN_DIR = ROOT / "src" / "almanak" / "bin"
DIST_DIR = ROOT / "dist"
STASH_DIR = ROOT / ".bin_stash"
STAGING_DIR = ROOT / ".wheel_staging"

# Platform key to (wheel tag, bundled binary name).
PLATFORMS: dict[str, tuple[str, str]] = {
    "darwin-arm64": ("py3-none-macosx_11_0_arm64", "almanak-code-darwin-arm64"),
    "darwin-x86_64": ("py3-none-macosx_11_0_x86_64", "almanak-code-darwin-x86_64"),
    "linux-arm64": ("py3-none-manylinux_2_17_aarch64", "almanak-code-linux-arm64"),
    "linux-x86_64": ("py3-none-manylinux_2_17_x86_64", "almanak-code-linux-x86_64"),
    "windows-x86_64": ("py3-none-win_amd64", "almanak-code-windows-x86_64.exe"),
}


def record_digest(data: bytes) -> str:
    """RECORD hashes are urlsafe base64 with the padding stripped."""
    return urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")


def read_members(wheel: Path) -> tuple[dict[str, bytes], dict[str, zipfile.ZipInfo]]:
    with zipfile.ZipFile(wheel) as archive:
        infos = {info.filename: info for info in archive.infolist()}
        contents = {name: archive.read(name) for name in infos}
    return contents, infos


def retag_metadata(contents: dict[str, bytes], dist_info: str, tag: str) -> None:
    key = f"{dist_info}/WHEEL"
    if key not in contents:
        raise SystemExit(f"{key} is missing from the built wheel")
    lines = contents[key].decode("utf-8").splitlines()
    retagged = [f"Tag: {tag}" if line.startswith("Tag:") else line for line in lines]
    contents[key] = "\n".join(retagged).encode("utf-8")


def rebuild_record(contents: dict[str, bytes], dist_info: str) -> None:
    key = f"{dist_info}/RECORD"
    rows = [
        f"{name},sha256={record_digest(data)},{len(data)}"
        for name, data in sorted(contents.items())
        if name != key
    ]
    rows.append(f"{key},,")
    contents[key] = "\n".join(rows).encode("utf-8")


def write_wheel(
    target: Path, contents: dict[str, bytes], infos: dict[str, zipfile.ZipInfo]
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(contents.items()):
            source = infos.get(name)
            if source is None:
                archive.writestr(name, data)
                continue
            # external_attr carries the Unix mode; the binary must stay executable.
            info = zipfile.ZipInfo(name, date_time=source.date_time)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = source.external_attr
            archive.writestr(info, data)


def retag_wheel(wheel: Path, tag: str, output_dir: Path) -> Path:
    name, version = wheel.stem.split("-")[:2]
    dist_info = f"{name}-{version}.dist-info"
    contents, infos = read_members(wheel)
    retag_metadata(contents, dist_info, tag)
    rebuild_record(contents, dist_info)
    target = output_dir / f"{name}-{version}-{tag}.whl"
    write_wheel(target, contents, infos)
    return target


def clean_bin_dir() -> None:
    for entry in BIN_DIR.iterdir():
        if entry.name != ".gitkeep":
            entry.unlink()


def stash_binaries() -> None:
    # A stash that outlived its run means an earlier build was killed before it
    # could restore, so it may hold the only copy of the other platforms'
    # binaries. Called before the try, so refusing here cannot reach the finally
    # and consume the stash it just declined to overwrite.
    if STASH_DIR.exists():
        raise SystemExit(
            f"{STASH_DIR} survives an interrupted build and may hold the only copy "
            f"of the other platforms' binaries. Move its contents back into "
            f"{BIN_DIR}, or delete it, before rebuilding."
        )
    STASH_DIR.mkdir()
    for entry in BIN_DIR.iterdir():
        if entry.name != ".gitkeep":
            shutil.copy2(entry, STASH_DIR / entry.name)


def restore_binaries() -> None:
    if not STASH_DIR.exists():
        return
    clean_bin_dir()
    for entry in STASH_DIR.iterdir():
        shutil.copy2(entry, BIN_DIR / entry.name)
        os.chmod(BIN_DIR / entry.name, 0o755)
    shutil.rmtree(STASH_DIR)


def place_binary(name: str) -> None:
    clean_bin_dir()
    shutil.copy2(STASH_DIR / name, BIN_DIR / name)
    os.chmod(BIN_DIR / name, 0o755)


def build_generic_wheel() -> Path:
    if STAGING_DIR.exists():
        shutil.rmtree(STAGING_DIR)
    result = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(STAGING_DIR)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"wheel build failed:\n{result.stderr}")
    wheels = list(STAGING_DIR.glob("*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one staged wheel, found {len(wheels)}")
    return wheels[0]


def build_one(platform: str, output_dir: Path) -> Path:
    tag, binary = PLATFORMS[platform]
    if not (STASH_DIR / binary).exists():
        raise SystemExit(
            f"{platform}: {binary} was not downloaded. Run 'make binaries' for the "
            f"full set, or pass --platform to build only what is present."
        )
    place_binary(binary)
    built = retag_wheel(build_generic_wheel(), tag, output_dir)
    print(f"  {platform} -> {built.name}")
    return built


def build_platform_wheels(platforms: list[str]) -> list[Path]:
    # The caller owns dist/; an sdist built before this runs must survive.
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    built: list[Path] = []
    stash_binaries()
    try:
        for platform in platforms:
            built.append(build_one(platform, DIST_DIR))
    except BaseException:
        # A failed build leaves no wheels behind, so a partial set cannot be
        # published by a later publish target run on its own.
        for wheel in built:
            wheel.unlink(missing_ok=True)
        raise
    finally:
        restore_binaries()
        if STAGING_DIR.exists():
            shutil.rmtree(STAGING_DIR)
    return built


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=sorted(PLATFORMS), help="build a single platform")
    arguments = parser.parse_args(argv)
    platforms = [arguments.platform] if arguments.platform else list(PLATFORMS)

    for wheel in build_platform_wheels(platforms):
        print(f"{wheel.name}  ({wheel.stat().st_size / (1024 * 1024):.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
