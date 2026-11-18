"""Retained QA checkpoints and run manifests read through today's author interface.

The QA packages were sealed before lending exits lost their repayment ceiling
and before Safe grants became scope-only (blueprint 27). Their allocations and
capabilities still carry the retired budgets; every other retained fact, including the
deposit maxima of recorded liquidity intents and operations, is read as captured. The
packages on disk are unchanged.
"""

import json

from tests.exchange_scope import without_exchange_budgets
from tests.lending_captures import current_capture


def current_value(value: dict) -> dict:
    """A retained checkpoint or manifest wire value in the current interface."""
    return without_exchange_budgets(current_capture(value))


def current_json(raw: bytes | str) -> str:
    return json.dumps(current_value(json.loads(raw)))
