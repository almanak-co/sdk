"""Back-compat shim: ``fluid_vault.receipt_parser`` re-exports the vault receipt
parser from ``almanak.connectors._fluid_core.receipt_parser``.
"""

from almanak.connectors._fluid_core import receipt_parser as _impl
from almanak.connectors._fluid_core.receipt_parser import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    return getattr(_impl, name)
