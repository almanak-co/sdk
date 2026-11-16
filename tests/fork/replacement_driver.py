"""Process-death controls around the real runtime and sandboxed reference strategy."""

import argparse
import json
import os
import signal
import time
from pathlib import Path

from almanak.connectors.composition import configuration_from_json
from almanak.runtime import Runtime
from almanak.strategy import StrategyDefinition


def is_replacement(pending):
    family = getattr(pending, "family", None)
    return family is not None and (len(family.candidates) > 1 or family.proposal is not None)


def pause_supply(stage, pending, path):
    if stage != "prepared" or pending.operation.kind != "supply" or path is None:
        return False
    path.touch()
    while path.exists():
        time.sleep(0.05)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--strategy", type=Path, required=True)
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--gateway-auth-file", type=Path, required=True)
    parser.add_argument("--pause-file", type=Path)
    parser.add_argument("--crash")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    paused = False

    def fault(stage, pending):
        nonlocal paused
        if not paused:
            paused = pause_supply(stage, pending, args.pause_file)
        if stage == args.crash and is_replacement(pending):
            os.kill(os.getpid(), signal.SIGKILL)

    runtime = Runtime(
        args.gateway_url,
        args.gateway_auth_file,
        create=None if args.resume else configuration_from_json(args.config.read_text()),
        strategy=None if args.resume else StrategyDefinition.read(args.strategy),
        fault=fault,
    )
    print(json.dumps({"event": "ready"}), flush=True)
    try:
        while not runtime.tick():
            time.sleep(0.05)
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
