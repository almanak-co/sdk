"""Kill the actual runtime around the commit of a bridge transfer's measured settlement."""

import argparse
import os
import signal
import time
from pathlib import Path

from almanak.connectors.across.settlement_types import Scan
from almanak.runtime import Runtime

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--gateway-url", required=True)
parser.add_argument("--gateway-auth-file", type=Path, required=True)
parser.add_argument("--crash", choices=("external_evidence", "external_commit"), required=True)
args = parser.parse_args()


def crash(stage, value):
    # Only the page that carries a settlement: an empty page commits nothing to lose.
    if stage == args.crash and isinstance(value, Scan) and value.outcome is not None:
        os.kill(os.getpid(), signal.SIGKILL)


runner = Runtime(args.gateway_url, args.gateway_auth_file, fault=crash)
try:
    while not runner.tick():
        time.sleep(0.05)
finally:
    runner.close()
