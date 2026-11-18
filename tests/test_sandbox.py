import errno
import json
import os
import socket
import subprocess
import tempfile
from pathlib import Path

import pytest

from almanak import sandbox


def run_worker(code):
    result = subprocess.run(
        [*sandbox.worker_command(), "-c", code],
        cwd="/",
        env={"PATH": os.defpath, "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def operation_errno(operation):
    return run_worker(
        "import json, socket\nfrom pathlib import Path\n"
        f"try:\n    {operation}\n"
        "except OSError as exc:\n    print(json.dumps(exc.errno))\n"
        "else:\n    print(json.dumps(None))\n"
    )


def test_strategy_sandbox_denies_host_network():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        address = listener.getsockname()
        # Prove this endpoint is reachable from the host before testing isolation.
        with socket.create_connection(address, timeout=2):
            pass
        error = operation_errno(f"socket.create_connection({address!r}, timeout=2)")
    assert error in (errno.EACCES, errno.EPERM, errno.ECONNREFUSED, errno.ENETUNREACH)


@pytest.mark.parametrize("operation", ["read_text()", "write_text('changed')"])
def test_strategy_sandbox_denies_secret_access(tmp_path, operation):
    secret = tmp_path / "gateway-secret"
    secret.write_text("synthetic credential")
    error = operation_errno(f"Path({str(secret)!r}).{operation}")
    assert error in (errno.EACCES, errno.EPERM, errno.ENOENT, errno.EROFS)
    assert secret.read_text() == "synthetic credential"


def test_strategy_sandbox_cannot_modify_readable_source():
    with tempfile.NamedTemporaryFile(dir=Path(sandbox.__file__).parent, suffix=".probe") as probe:
        path = Path(probe.name)
        path.write_text("readable")
        assert (
            run_worker(f"import json; print(json.dumps(open({str(path)!r}).read()))") == "readable"
        )
        error = operation_errno(f"Path({str(path)!r}).write_text('changed')")
        assert error in (errno.EACCES, errno.EPERM, errno.EROFS)
        assert path.read_text() == "readable"


def test_strategy_sandbox_cannot_create_files(tmp_path):
    output = tmp_path / "worker-output"
    error = operation_errno(f"Path({str(output)!r}).write_text('write')")
    assert error in (errno.EACCES, errno.EPERM, errno.ENOENT, errno.EROFS)
    assert not output.exists()


def test_missing_linux_sandbox_refuses_execution(monkeypatch):
    monkeypatch.setattr(sandbox.sys, "platform", "linux")
    monkeypatch.setattr(sandbox.shutil, "which", lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match="requires bubblewrap"):
        sandbox.worker_command()


def test_linux_sandbox_exposes_interpreter_library_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(sandbox.sysconfig, "get_config_var", lambda name: str(tmp_path))
    monkeypatch.setattr(sandbox.shutil, "which", lambda *args, **kwargs: "/usr/bin/bwrap")
    command = sandbox.linux_command((tmp_path,))
    index = command.index("LD_LIBRARY_PATH")
    assert command[index - 1 : index + 2] == ["--setenv", "LD_LIBRARY_PATH", str(tmp_path)]


@pytest.mark.parametrize("configured", [None, "", "relative/lib"])
def test_linux_sandbox_rejects_invalid_interpreter_library_directory(monkeypatch, configured):
    monkeypatch.setattr(sandbox.sysconfig, "get_config_var", lambda name: configured)
    monkeypatch.setattr(sandbox.shutil, "which", lambda *args, **kwargs: "/usr/bin/bwrap")
    command = sandbox.linux_command(())
    assert "LD_LIBRARY_PATH" not in command


def test_unsupported_platform_refuses_execution(monkeypatch):
    monkeypatch.setattr(sandbox.sys, "platform", "unsupported")
    with pytest.raises(RuntimeError, match="requires macOS sandbox-exec or Linux bubblewrap"):
        sandbox.worker_command()


def test_key_symlink_into_readable_source_is_refused(tmp_path):
    key = tmp_path / "key"
    key.symlink_to(Path(sandbox.__file__).parent / "secret.json")
    with pytest.raises(ValueError, match="strategy-readable"):
        sandbox.worker_command(key_file=key)
