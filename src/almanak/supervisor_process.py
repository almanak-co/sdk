"""Process ownership evidence is checked before signals or reuse."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

# How long a live gateway may take to answer after it is spawned. One that exits is
# reported at once; this bounds only one that hangs. A rescheduled container starts
# beside other cold starts with little CPU, where a fixed 25 seconds was too short.
SERVICE_READY_SECONDS = 300


def timing(event, started, **fields):
    """One line in logs/supervisor.log, which hosts export, on how long a start step took."""
    seconds = round(time.monotonic() - started, 1)
    print(json.dumps({"event": event, "seconds": seconds, **fields}), flush=True)


def loopback_listener(port=0):
    """A loopback TCP socket bound to an ephemeral port, for a spawned service to adopt.

    The port stays reserved while this socket is open. Releasing a chosen port
    before the child binds it lets a concurrent process take it, and the
    readiness probe then reaches a service holding a different token. A service
    restarted at an endpoint other processes already name binds its recorded `port`.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", port))
    except BaseException:
        sock.close()
        raise
    return sock


def identity(pid):
    if sys.platform == "linux":
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            if stat[0] == "Z":
                return None
            return {
                "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "started": stat[19],
                "command": Path(f"/proc/{pid}/cmdline").read_bytes().hex(),
            }
        except (OSError, IndexError):
            return None
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    return (
        {"started": result.stdout.strip()}
        if result.returncode == 0 and result.stdout.strip()
        else None
    )


def running(record):
    return bool(record and record.get("identity") and identity(record["pid"]) == record["identity"])


def spawn(root, name, arguments, *, password=None, listener=None):
    with (root / "logs" / (name + ".log")).open("a") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "almanak.cli", *arguments],
            stdin=subprocess.PIPE if password is not None else subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            cwd="/",
            pass_fds=() if listener is None else (listener.fileno(),),
        )
    if password is not None:
        assert process.stdin is not None
        process.stdin.write((json.dumps(password) + "\n").encode())
        process.stdin.close()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        record = identity(process.pid)
        if record is not None and "almanak.cli" in (
            bytes.fromhex(record["command"]).decode(errors="replace")
            if "command" in record
            else record["started"]
        ):
            return {"pid": process.pid, "identity": record}
        time.sleep(0.01)
    raise RuntimeError(f"{name} did not start")


def stop(record, *, interrupt=False):
    if not running(record):
        return
    os.kill(record["pid"], signal.SIGINT if interrupt else signal.SIGTERM)
    deadline = time.monotonic() + 25
    while running(record) and time.monotonic() < deadline:
        time.sleep(0.05)
    if running(record):
        raise RuntimeError("process did not acknowledge stop; pending work remains recorded")
