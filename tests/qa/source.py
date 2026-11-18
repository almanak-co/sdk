"""Capture exact local sources, including uncommitted implementation changes."""

import io
import subprocess
import tarfile

from tests.qa.catalog import ROOT
from tests.qa.results import digest


def reviewed_path(name):
    path = ROOT / name
    if path.is_symlink() or not path.resolve().is_relative_to(ROOT):
        raise ValueError("source snapshot refuses links outside the reviewed tree")
    return path


def source_bytes(name, expected, secrets):
    path = reviewed_path(name)
    data = path.read_bytes()
    if digest(path) != expected:
        raise ValueError("source changed during snapshot")
    if any(secret.encode() in data for secret in secrets):
        raise ValueError(f"source snapshot contains credential material: {name}")
    return data


def snapshot(identity, destination, secrets):
    with tarfile.open(destination, "w:gz") as archive:
        for name, expected in identity["files"].items():
            data = source_bytes(name, expected, secrets)
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(data))


def tool_versions():
    result = {}
    for name in ("uv", "anvil", "bwrap"):
        try:
            output = subprocess.run([name, "--version"], capture_output=True, text=True, timeout=10)
            result[name] = output.stdout.strip() if output.returncode == 0 else "unavailable"
        except (OSError, subprocess.TimeoutExpired):
            result[name] = "unavailable"
    return result


def changes(rows, previous):
    old = {r["id"]: r for r in previous.get("cases", [])}
    changed = []
    for row in rows:
        prior = old.get(row["id"])
        signature = (row["status"], row.get("category"))
        before = (prior["status"], prior.get("category")) if prior else None
        if signature != before:
            changed.append({"case": row["id"], "before": before, "after": signature})
    return changed
