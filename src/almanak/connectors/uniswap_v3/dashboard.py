"""Concrete V3 author parameter types bind shared retained-data dashboard values."""

from almanak.connectors import dex_dashboard as family
from almanak.connectors.dex_dashboard import inventory_dataset as inventory_dataset
from almanak.connectors.dex_dashboard import payer_values as payer_values
from almanak.connectors.dex_dashboard import position_values as position_values
from almanak.connectors.dex_dashboard import validate_projection as validate_projection
from almanak.connectors.uniswap_v3.models import Parameters


def project(state, *, captured_at=None):
    return family.project(state, parameter_type=Parameters, captured_at=captured_at)
