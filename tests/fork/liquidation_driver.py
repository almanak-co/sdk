"""Kill the actual runtime at unsolicited financial evidence and delivery boundaries."""

import argparse
import os
import signal
import time
from pathlib import Path

from almanak.connectors.gmx_v2.liquidation_types import LiquidationOutcome, Scan
from almanak.runtime import Runtime

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--gateway-url", required=True)
parser.add_argument("--gateway-auth-file", type=Path, required=True)
parser.add_argument(
    "--crash", choices=("external_evidence", "external_commit", "outcome"), required=True
)
args = parser.parse_args()


def crash(stage, value):
    selected = (isinstance(value, Scan) and value.outcome is not None) or isinstance(
        value, LiquidationOutcome
    )
    if stage == args.crash and selected:
        os.kill(os.getpid(), signal.SIGKILL)


runner = Runtime(args.gateway_url, args.gateway_auth_file, fault=crash)
try:
    while not runner.tick():
        time.sleep(0.05)
finally:
    runner.close()
