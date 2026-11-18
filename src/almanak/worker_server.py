"""Sandboxed preload process; every evaluation executes in a fresh isolated child.

The parent only frames requests, imports trusted connector modules and forks. It
never evaluates strategy source.
Children cannot signal, inspect or modify the parent, create descendants, or retain
IPC objects between evaluations. The enclosing OS sandbox has no writable files.
"""

import ctypes
import io
import json
import os
import select
import signal
import sys
import time
import traceback
from importlib import import_module

# Every evaluation hydrates memory, and nearly every strategy imports the public
# author root, so each fork inherits them instead of importing them again.
import almanak.api  # noqa: F401
import almanak.dashboard  # noqa: F401
import almanak.memory_codec  # noqa: F401
from almanak.capability_worker import main as capabilities
from almanak.connectors.composition import connector_for
from almanak.strategy_worker import main as evaluate
from almanak.strategy_worker import prepared
from almanak.worker_policy import MAX_BYTES, MAX_FRAME_BYTES, WALL_SECONDS

STRATEGY = "almanak.strategy_worker"
CAPABILITY = "almanak.capability_worker"
# The trusted session names the single-evaluation module a request replaces; each
# entry applies the shared author limits itself before loading source.
ENTRYPOINTS = {STRATEGY: evaluate, CAPABILITY: capabilities}


def child(request, output):
    try:
        os.dup2(output, 1)
        os.dup2(output, 2)
        null = os.open("/dev/null", os.O_RDONLY)
        os.dup2(null, 0)
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.close_range(3, ctypes.c_uint(-1), 0):
            raise RuntimeError("child could not close inherited control descriptors")
        sys.stdin = io.StringIO(request["body"])
        sys.stdout = os.fdopen(1, "w", buffering=1, closefd=False)
        sys.stderr = os.fdopen(2, "w", buffering=1, closefd=False)
        module = request.get("module", STRATEGY)
        sys.argv = [module, *request["arguments"]]
        ENTRYPOINTS[module]()
        sys.stdout.flush()
        os._exit(0)
    except BaseException:
        traceback.print_exc()
        os._exit(1)


def read_output(descriptor, deadline):
    chunks: list[bytes] = []
    length = 0
    while True:
        if not select.select([descriptor], [], [], max(0, deadline - time.monotonic()))[0]:
            raise RuntimeError("strategy worker timed out")
        chunk = os.read(descriptor, 65536)
        if not chunk:
            return b"".join(chunks).decode()
        length += len(chunk)
        if length > MAX_BYTES:
            raise RuntimeError("strategy worker output exceeds its limit")
        chunks.append(chunk)


def wait_child(pid, deadline):
    while True:
        finished, status = os.waitpid(pid, os.WNOHANG)
        if finished:
            return status
        if time.monotonic() >= deadline:
            raise RuntimeError("strategy worker timed out after closing its output")
        time.sleep(0.01)


def collect(pid, descriptor):
    deadline = time.monotonic() + WALL_SECONDS
    try:
        output = read_output(descriptor, deadline)
        status = wait_child(pid, deadline)
        pid = 0
        if status:
            raise RuntimeError("strategy worker failed: " + output[-2000:])
        return {"output": output.strip()}
    except (ValueError, RuntimeError) as error:
        return {"error": str(error)}
    finally:
        os.close(descriptor)
        if pid:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)


def preload_dispatch():
    """Import the configuration types `connector_for` tells apart; selects no connector.

    A forked evaluation whose strategy asks for its connector otherwise imports them
    itself, which was most of what each evaluation of a long backtest cost. They are
    configuration models only: no other connector's implementation is imported.
    """
    try:
        connector_for(None)
    except ValueError:
        pass


def preload(request):
    # A child otherwise imports its connector package after fork, which costs
    # about half a second per evaluation and dominates long backtests. Importing
    # it here keeps it in the forked image; the child still resolves the
    # connector itself, so any failure here resurfaces there as the request's
    # error. The parent only accumulates connectors its requests name, which
    # never exceeds the image a composed strategy imports.
    try:
        arguments = request["arguments"]
        if request.get("module", STRATEGY) == CAPABILITY:
            # The codec factory comes from trusted connector wiring, never strategy JSON.
            import_module(arguments[0].split(":")[0])
            return
        prepared(arguments[0] if arguments else "", request["body"])
        # Strategy helpers ask the composition root for their connector, which imports
        # every configuration type it tells apart before it looks at the config.
        preload_dispatch()
    except Exception:
        pass


def dispatch(request):
    if request.get("module", STRATEGY) not in ENTRYPOINTS:
        return {"error": "preload worker request names an unknown module"}
    preload(request)
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:
        child(request, write)
    os.close(write)
    return collect(pid, read)


def main():
    print(json.dumps({"ready": True}), flush=True)
    while line := sys.stdin.readline(MAX_FRAME_BYTES + 1):
        if len(line) > MAX_FRAME_BYTES or not line.endswith("\n"):
            raise ValueError("worker request exceeds its framing limit")
        request = json.loads(line)
        if len(request["body"].encode()) > MAX_BYTES:
            raise ValueError("worker request exceeds its limit")
        response = dispatch(request)
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
