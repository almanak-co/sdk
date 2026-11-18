"""Fork child scratch files honor explicit operator storage without sharing credentials."""

import json
import sys

from tests.fork.support import processes


def test_fork_process_preserves_explicit_scratch_directory_only(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setenv("TMPDIR", str(scratch))
    monkeypatch.setenv("ALCHEMY_API_KEY", "must-not-reach-fork-child")
    command = """
import json
import os
import tempfile
with tempfile.NamedTemporaryFile() as output:
    print(json.dumps({
        "temporary_file": output.name,
        "tmpdir": os.environ.get("TMPDIR"),
        "operator_key_present": "ALCHEMY_API_KEY" in os.environ,
        "bytecode_disabled": os.environ.get("PYTHONDONTWRITEBYTECODE"),
    }))
"""
    with processes(tmp_path) as launch:
        child = launch([sys.executable, "-c", command], ready=False)
        assert child.wait(timeout=20) == 0
    result = json.loads((tmp_path / "process-0.log").read_text())
    assert result["tmpdir"] == str(scratch)
    assert result["temporary_file"].startswith(str(scratch) + "/")
    assert result["operator_key_present"] is False
    assert result["bytecode_disabled"] == "1"
